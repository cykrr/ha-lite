//! MQTT plumbing: the link that handlers publish through, the outbound request
//! worker, and the event loop that routes inbound messages.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use rumqttc::{AsyncClient, ClientError, Event, Incoming, MqttOptions, QoS};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::app::{AppContext, LightView};
use crate::bthome::{bthome_bytes, process_bthome};
use crate::discovery::{handle_discovery, handle_tasmota_discovery};
use crate::lights::{handle_light_state, legacy_light_id, legacy_light_state, LightConfig};

/// One outbound MQTT request queued for the request worker.
pub(crate) enum MqttCommand {
    Subscribe(String, QoS),
    Unsubscribe(String),
    Publish(String, Vec<u8>, QoS, bool),
}

/// Everything that talks to the broker, and the connection state handlers need.
///
/// `AsyncClient` feeds a bounded channel that only `EventLoop::poll` drains.
/// Awaiting `subscribe`/`publish` from the task that is *running* `poll`
/// blocks that task, so a burst larger than the channel capacity (32) would
/// wedge the event loop forever. A reconnect re-arming enough lights is
/// enough to do it. So nothing here awaits the client: requests go to a
/// worker, and HTTP handlers use `try_publish`.
pub(crate) struct MqttLink {
    client: AsyncClient,
    requests: mpsc::UnboundedSender<MqttCommand>,
    /// Whether the event loop currently has a live broker session. `publish`
    /// only reports a closed request channel, not a dead connection, so a
    /// toggle consults this flag instead of assuming success while the broker
    /// is unreachable.
    connected: AtomicBool,
    /// State topics already asked for a snapshot on this MQTT connection, so
    /// the reconnect re-arm and the retained discovery replay don't both ask.
    requested_state: Mutex<HashSet<String>>,
}

impl MqttLink {
    pub(crate) fn new(client: AsyncClient) -> (Self, mpsc::UnboundedReceiver<MqttCommand>) {
        let (requests, request_rx) = mpsc::unbounded_channel();
        (
            Self {
                client,
                requests,
                connected: AtomicBool::new(false),
                requested_state: Mutex::new(HashSet::new()),
            },
            request_rx,
        )
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub(crate) fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
    }

    fn send(&self, request: MqttCommand) {
        // Only fails once the worker (and therefore the process) is going away.
        if self.requests.send(request).is_err() {
            warn!("MQTT request worker stopped; dropping request");
        }
    }

    pub(crate) fn subscribe(&self, topic: &str, qos: QoS) {
        self.send(MqttCommand::Subscribe(topic.to_string(), qos));
    }

    pub(crate) fn unsubscribe(&self, topic: &str) {
        self.send(MqttCommand::Unsubscribe(topic.to_string()));
    }

    pub(crate) fn publish(&self, topic: &str, payload: Vec<u8>, qos: QoS, retain: bool) {
        self.send(MqttCommand::Publish(
            topic.to_string(),
            payload,
            qos,
            retain,
        ));
    }

    /// Publish an empty payload to ask a device for its current state. OpenBeken
    /// uses the channel's `/get` topic for this, Tasmota uses
    /// `cmnd/<topic>/STATE`. Each topic is asked at most once per MQTT
    /// connection; the mark is cleared on reconnect.
    pub(crate) fn request_state(&self, topic: &str) {
        let first_request = self
            .requested_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(topic.to_string());
        if !first_request {
            return;
        }

        self.publish(topic, Vec::new(), QoS::AtLeastOnce, false);
    }

    pub(crate) fn reset_requested_state(&self) {
        self.requested_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    /// Publish without waiting for the event loop: if the request queue is full
    /// this fails now, so an HTTP request can't stall.
    pub(crate) fn try_publish<P: Into<Vec<u8>>>(
        &self,
        topic: &str,
        qos: QoS,
        retain: bool,
        payload: P,
    ) -> Result<(), ClientError> {
        self.client.try_publish(topic, qos, retain, payload)
    }
}

/// Build a link and spawn its request worker. The event-loop task must never
/// await the client, so the worker is the only place that drains requests.
pub(crate) fn connect(options: MqttOptions, capacity: usize) -> (MqttLink, rumqttc::EventLoop) {
    let (client, eventloop) = AsyncClient::new(options, capacity);
    let (link, requests) = MqttLink::new(client.clone());
    tokio::spawn(run_requests(client, requests));
    (link, eventloop)
}

/// Drains queued requests into the client, and therefore into the event loop.
///
/// This task is allowed to block when the client's bounded channel is full:
/// `poll()` keeps running and drains it. Doing the same awaits from the
/// event-loop task itself is what deadlocks, since nothing would drain the
/// channel while it waits.
async fn run_requests(client: AsyncClient, mut requests: mpsc::UnboundedReceiver<MqttCommand>) {
    while let Some(request) = requests.recv().await {
        match request {
            MqttCommand::Subscribe(topic, qos) => {
                if let Err(e) = client.subscribe(&topic, qos).await {
                    warn!(error = %e, %topic, "failed to subscribe");
                }
            }
            MqttCommand::Unsubscribe(topic) => {
                if let Err(e) = client.unsubscribe(&topic).await {
                    warn!(error = %e, %topic, "failed to unsubscribe");
                }
            }
            MqttCommand::Publish(topic, payload, qos, retain) => {
                if let Err(e) = client.publish(&topic, qos, retain, payload).await {
                    warn!(error = %e, %topic, "failed to publish");
                }
            }
        }
    }
    debug!("MQTT request worker stopped");
}

/// Consume the broker event loop, reconnecting with backoff, and dispatch
/// every inbound event. Runs until the process exits.
pub(crate) async fn run_event_loop(ctx: Arc<AppContext>, mut eventloop: rumqttc::EventLoop) {
    let mut backoff = Duration::from_secs(1);

    loop {
        match eventloop.poll().await {
            Ok(event) => {
                if handle_event(&ctx, event).await {
                    // A successful session earns a fresh retry delay.
                    backoff = Duration::from_secs(1);
                }
            }
            Err(e) => {
                ctx.mqtt.set_connected(false);
                warn!(
                    error = %e,
                    retry_in_secs = backoff.as_secs(),
                    "MQTT connection error; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Route one event from the broker. Kept separate from `run_event_loop` so the
/// dispatch rules can be exercised without a live connection. Returns whether
/// the event established a new session.
pub(crate) async fn handle_event(ctx: &Arc<AppContext>, event: Event) -> bool {
    match event {
        Event::Incoming(Incoming::ConnAck(_)) => {
            on_connected(ctx).await;
            true
        }
        Event::Incoming(Incoming::Publish(msg)) => {
            handle_publish(ctx, &msg.topic, &msg.payload).await;
            false
        }
        _ => false,
    }
}

/// Re-subscribe everything and re-arm state on a fresh session.
async fn on_connected(ctx: &Arc<AppContext>) {
    info!("connected to MQTT broker");
    ctx.mqtt.set_connected(true);
    ctx.mqtt.reset_requested_state();
    for (topic, qos) in [
        ("sensors/#", QoS::AtMostOnce),
        ("lights/+/state", QoS::AtLeastOnce),
        // Discovery topics can be `component/object/config` or
        // `component/node/object/config`, so listen to the whole tree.
        ("homeassistant/#", QoS::AtLeastOnce),
        ("tasmota/discovery/#", QoS::AtLeastOnce),
    ] {
        ctx.mqtt.subscribe(topic, qos);
    }

    // Re-arm state subscriptions and ask each known device for its current
    // value, since most of them only publish on change. Retained discovery
    // replay calls this again; the request set makes that a no-op instead of a
    // duplicate round trip. State topics are deduplicated because all channels
    // of a multi-relay device share the same RESULT/STATE topics.
    let configs: Vec<LightConfig> = {
        let reg = ctx.lights.read().await;
        reg.by_id.values().cloned().collect()
    };
    let state_topics: HashSet<&str> = configs
        .iter()
        .flat_map(|cfg| cfg.state_topics.iter().map(String::as_str))
        .collect();
    for topic in state_topics {
        ctx.mqtt.subscribe(topic, QoS::AtLeastOnce);
    }
    for cfg in &configs {
        if let Some(topic) = &cfg.request_topic {
            ctx.mqtt.request_state(topic);
        }
    }
}

/// Dispatch one broker message by topic. Discovery is handled first; any other
/// topic is either a sensor feed, a discovered light's state topic, or the
/// legacy `lights/<id>/state` convention.
pub(crate) async fn handle_publish(ctx: &Arc<AppContext>, topic: &str, payload: &[u8]) {
    if let Some(mac) = topic.strip_prefix("tasmota/discovery/") {
        if let Some(mac) = mac.strip_suffix("/config") {
            handle_tasmota_discovery(ctx, topic, mac, payload).await;
        }
        return;
    }

    if let Some(platform) = topic
        .strip_prefix("homeassistant/")
        .and_then(|rest| rest.split('/').next())
    {
        if topic.ends_with("/config") {
            handle_discovery(ctx, topic, platform, payload).await;
        }
        return;
    }

    if topic.starts_with("sensors/") {
        if topic.starts_with("sensors/bthome") {
            ctx.bthome_packets.fetch_add(1, Ordering::Relaxed);

            // Proxies usually send a hex string (e.g. "4002ca0903bf13"),
            // but accept raw BTHome bytes too.
            let bytes = bthome_bytes(payload);

            debug!(
                %topic,
                len = bytes.len(),
                hex = %hex::encode(&bytes),
                "BTHome broadcast received"
            );

            process_bthome(ctx, "MQTT", topic, &bytes).await;
        } else {
            debug!(
                %topic,
                payload = %String::from_utf8_lossy(payload),
                "unhandled sensor topic"
            );
        }
    } else if !handle_light_state(ctx, topic, payload).await {
        // Legacy convention: `lights/<id>/state` with ON/OFF payloads.
        // Discovered entities get first claim on the topic, and only a
        // single-segment id matches the `lights/+/state` subscription, so a
        // nested `stat_t` can't be mistaken for a legacy light.
        if let Some(id) = legacy_light_id(topic) {
            match legacy_light_state(payload) {
                Some(is_on) => {
                    let mut state = ctx.state.write().await;
                    state
                        .lights
                        .entry(id.to_string())
                        .or_insert_with(|| LightView {
                            name: id.to_string(),
                            on: is_on,
                        })
                        .on = is_on;
                }
                None => debug!(
                    %topic,
                    payload = %String::from_utf8_lossy(payload),
                    "ignoring malformed legacy light state"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::test_ctx;

    fn take_commands(requests: &mut mpsc::UnboundedReceiver<MqttCommand>) -> Vec<String> {
        let mut commands = Vec::new();
        while let Ok(request) = requests.try_recv() {
            commands.push(match request {
                MqttCommand::Subscribe(topic, _) => format!("subscribe {topic}"),
                MqttCommand::Unsubscribe(topic) => format!("unsubscribe {topic}"),
                MqttCommand::Publish(topic, payload, _, _) => {
                    format!("publish {topic} {}", String::from_utf8_lossy(&payload))
                }
            });
        }
        commands
    }

    #[tokio::test]
    async fn connack_resubscribes_and_marks_the_link_connected() {
        let (ctx, _eventloop, mut requests) = test_ctx(false, 4);

        let reconnected = handle_event(
            &ctx,
            Event::Incoming(Incoming::ConnAck(rumqttc::ConnAck {
                session_present: false,
                code: rumqttc::ConnectReturnCode::Success,
            })),
        )
        .await;

        assert!(reconnected, "a ConnAck establishes a new session");
        assert!(ctx.mqtt.is_connected());
        let commands = take_commands(&mut requests);
        assert!(commands.contains(&"subscribe sensors/#".to_string()), "{commands:?}");
        assert!(
            commands.contains(&"subscribe homeassistant/#".to_string()),
            "{commands:?}"
        );
    }

    #[tokio::test]
    async fn homeassistant_discovery_registers_and_subscribes() {
        let (ctx, _eventloop, mut requests) = test_ctx(false, 8);

        handle_publish(
            &ctx,
            "homeassistant/switch/desk/config",
            br#"{"name":"Desk","uniq_id":"desk","stat_t":"stat/desk/POWER","cmd_t":"cmnd/desk/POWER"}"#,
        )
        .await;

        assert_eq!(
            ctx.lights.read().await.by_id["desk"].command_topic,
            "cmnd/desk/POWER"
        );
        assert!(ctx.state.read().await.lights.contains_key("desk"));
        let commands = take_commands(&mut requests);
        assert!(
            commands.contains(&"subscribe stat/desk/POWER".to_string()),
            "{commands:?}"
        );
    }

    #[tokio::test]
    async fn tasmota_discovery_is_routed_from_its_topic() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 8);

        handle_publish(
            &ctx,
            "tasmota/discovery/AABBCCDDEEFF/config",
            br#"{"dn":"Shelf","t":"shelf","rl":[1],"state":["OFF","ON"]}"#,
        )
        .await;

        let reg = ctx.lights.read().await;
        assert!(reg.by_id.contains_key("tasmota_AABBCCDDEEFF_1"), "{:?}", reg.by_id.keys());
        assert_eq!(reg.by_id["tasmota_AABBCCDDEEFF_1"].command_topic, "cmnd/shelf/POWER");
    }

    #[tokio::test]
    async fn an_empty_discovery_config_removes_the_entity() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 8);
        let topic = "homeassistant/switch/desk/config";
        let config = br#"{"uniq_id":"desk","stat_t":"stat/desk/POWER","cmd_t":"cmnd/desk/POWER"}"#;

        handle_publish(&ctx, topic, config).await;
        assert!(ctx.state.read().await.lights.contains_key("desk"));

        handle_publish(&ctx, topic, b"").await;

        assert!(!ctx.state.read().await.lights.contains_key("desk"));
        assert!(ctx.lights.read().await.by_id.is_empty());
    }

    #[tokio::test]
    async fn a_discovered_topic_wins_over_the_legacy_convention() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 8);
        handle_publish(
            &ctx,
            "homeassistant/switch/desk/config",
            br#"{"uniq_id":"desk","stat_t":"lights/bedroom/state","cmd_t":"cmnd/desk/POWER"}"#,
        )
        .await;

        handle_publish(&ctx, "lights/bedroom/state", b"ON").await;

        let state = ctx.state.read().await;
        assert!(state.lights["desk"].on, "discovered entity got the update");
        assert!(
            !state.lights.contains_key("bedroom"),
            "legacy fallback must not create a second entity"
        );
    }

    #[tokio::test]
    async fn an_unowned_lights_topic_uses_the_legacy_fallback() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);

        handle_publish(&ctx, "lights/lamp/state", b"ON").await;

        assert!(ctx.state.read().await.lights["lamp"].on);
    }

    #[tokio::test]
    async fn request_state_is_sent_once_per_connection() {
        let (ctx, _eventloop, mut requests) = test_ctx(false, 4);

        ctx.mqtt.request_state("cmnd/desk/STATE");
        ctx.mqtt.request_state("cmnd/desk/STATE");
        let commands = take_commands(&mut requests);
        assert_eq!(commands.len(), 1, "{commands:?}");
        assert!(commands[0].starts_with("publish cmnd/desk/STATE"), "{commands:?}");

        // A reconnect clears the mark, so the new session asks again.
        ctx.mqtt.reset_requested_state();
        ctx.mqtt.request_state("cmnd/desk/STATE");
        assert_eq!(take_commands(&mut requests).len(), 1);
    }

    #[tokio::test]
    async fn sensor_bthome_payload_updates_state() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);

        handle_publish(&ctx, "sensors/bthome1", b"400084015302100b03b00f").await;

        let state = ctx.state.read().await;
        let sensor = &state.sensors["sensors/bthome1"];
        assert_eq!(sensor.temp_c, Some(28.32));
        assert_eq!(sensor.humidity, Some(40.16));
    }
}
