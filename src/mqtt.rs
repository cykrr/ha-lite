//! MQTT plumbing: the link that handlers publish through, the outbound request
//! worker, and the event loop that routes inbound messages.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
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
use crate::lights::{
    forget_lights, handle_light_state, legacy_light_id, legacy_light_state, LightConfig,
};

/// How often the watchdog samples the event-loop heartbeat.
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(20);
/// Consecutive samples without a heartbeat before the process gives up. The
/// event loop beats at least every reconnect backoff (30s) plus connect timeout
/// (5s), so five idle samples (100s) mean it is genuinely stuck.
const WATCHDOG_STALL_LIMIT: u32 = 5;

/// How long after a (re)connect the retained discovery replay is given to
/// finish before lights it did not mention are treated as deleted.
const DISCOVERY_SWEEP_DELAY: Duration = Duration::from_secs(30);

/// MQTT client id sent on CONNECT.
///
/// The broker keys sessions by this id, and this app connects with a clean
/// session, so two instances sharing an id evict each other every time either
/// one reconnects. Default to a per-host id; `MQTT_CLIENT_ID` pins it when a
/// deployment needs a stable name.
pub(crate) fn client_id() -> String {
    if let Ok(id) = std::env::var("MQTT_CLIENT_ID") {
        if !id.is_empty() {
            return id;
        }
    }

    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_default();

    derive_client_id(&host)
}

/// Build the default id from a hostname. MQTT 3.1.1 allows 23 bytes and only
/// alphanumerics plus `-`/`_`, so anything else becomes `-` and the result is
/// truncated.
fn derive_client_id(host: &str) -> String {
    let host = host.trim();
    if host.is_empty() {
        return "ha-lite".to_string();
    }

    let mut id = String::from("ha-lite-");
    id.extend(host.chars().map(|c| {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            c
        } else {
            '-'
        }
    }));
    // Every character is ASCII after the mapping, so this is a char boundary.
    id.truncate(23);
    id
}

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
    /// Incremented once per event-loop iteration. The watchdog compares it
    /// across samples to tell a live-but-disconnected loop from a wedged one:
    /// `connected` alone cannot distinguish the two.
    eventloop_polls: AtomicU64,
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
                eventloop_polls: AtomicU64::new(0),
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

    fn beat(&self) {
        self.eventloop_polls.fetch_add(1, Ordering::Relaxed);
    }

    fn polls(&self) -> u64 {
        self.eventloop_polls.load(Ordering::Relaxed)
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
        ctx.mqtt.beat();
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

/// Exit when the MQTT event loop stops making progress.
///
/// The event loop is a spawned task, so a panic in it (or a future that never
/// returns) leaves the process itself running: HTTP keeps answering `/api/state`
/// but every toggle fails with 502 until someone restarts it by hand. Exiting
/// hands recovery to the service manager instead (`Restart=on-failure`).
pub(crate) async fn watch_event_loop(ctx: Arc<AppContext>) {
    let mut previous = ctx.mqtt.polls();
    let mut stalled = 0u32;

    loop {
        tokio::time::sleep(WATCHDOG_INTERVAL).await;
        let current = ctx.mqtt.polls();
        let (next_stalled, give_up) = stall_check(previous, current, stalled);
        previous = current;
        stalled = next_stalled;

        if give_up {
            tracing::error!(
                stalled_secs = WATCHDOG_INTERVAL.as_secs() * WATCHDOG_STALL_LIMIT as u64,
                "MQTT event loop stopped making progress; exiting so the service manager restarts it"
            );
            std::process::exit(1);
        }
    }
}

/// Fold one heartbeat sample into the watchdog state. Returns the new run of
/// idle samples and whether that run is long enough to give up.
fn stall_check(previous: u64, current: u64, stalled: u32) -> (u32, bool) {
    if current == previous {
        let stalled = stalled.saturating_add(1);
        (stalled, stalled >= WATCHDOG_STALL_LIMIT)
    } else {
        (0, false)
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
    let session = ctx.lights.write().await.begin_session();
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

    // A config cleared while we were offline is never replayed, so nothing
    // would ever remove its light. Once the retained replay has had time to
    // arrive, drop whatever it did not mention.
    let ctx = ctx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(DISCOVERY_SWEEP_DELAY).await;
        sweep_stale_discovery(&ctx, session).await;
    });
}

/// Remove lights whose discovery config the broker did not replay on `session`.
/// Skipped when the link has dropped or a newer session has begun: the replay
/// is then incomplete, and sweeping would delete live lights.
async fn sweep_stale_discovery(ctx: &Arc<AppContext>, session: u64) {
    if !ctx.mqtt.is_connected() {
        return;
    }

    let swept = ctx.lights.write().await.sweep_unseen(session);
    let Some((removed, freed_topics)) = swept else {
        return;
    };
    if removed.is_empty() {
        return;
    }

    forget_lights(ctx, &removed, freed_topics).await;
    info!(ids = ?removed, "removed light(s) whose discovery config is gone from the broker");
}

/// Dispatch one broker message by topic. Discovery is handled first; any other
/// topic is either a sensor feed, a discovered light's state topic, or the
/// legacy `lights/<id>/state` convention.
pub(crate) async fn handle_publish(ctx: &Arc<AppContext>, topic: &str, payload: &[u8]) {
    debug!(%topic, len = payload.len(), "MQTT message");

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

    // Only claim the BTHome feed. Any other `sensors/...` topic falls through,
    // so a discovered light whose `stat_t` lives there still gets its state.
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
    async fn a_light_state_topic_under_sensors_is_not_swallowed() {
        let (ctx, _eventloop, _requests) = test_ctx(true, 8);
        handle_publish(
            &ctx,
            "homeassistant/switch/fan/config",
            br#"{"name":"Fan","uniq_id":"fan","stat_t":"sensors/fan/state","cmd_t":"cmnd/fan/POWER","pl_on":"ON","pl_off":"OFF"}"#,
        )
        .await;

        handle_publish(&ctx, "sensors/fan/state", b"ON").await;

        assert!(ctx.state.read().await.lights["fan"].on);
    }

    #[tokio::test]
    async fn a_light_deleted_while_offline_is_swept_after_reconnect() {
        let (ctx, _eventloop, _requests) = test_ctx(true, 16);
        let desk = br#"{"name":"Desk","uniq_id":"desk","stat_t":"stat/desk/POWER","cmd_t":"cmnd/desk/POWER"}"#;
        let lamp = br#"{"name":"Lamp","uniq_id":"lamp","stat_t":"stat/lamp/POWER","cmd_t":"cmnd/lamp/POWER"}"#;
        handle_publish(&ctx, "homeassistant/switch/desk/config", desk).await;
        handle_publish(&ctx, "homeassistant/switch/lamp/config", lamp).await;

        // Reconnect: the broker replays the desk config but the lamp's was
        // cleared while we were away, so nothing ever mentions it.
        let session = ctx.lights.write().await.begin_session();
        handle_publish(&ctx, "homeassistant/switch/desk/config", desk).await;
        sweep_stale_discovery(&ctx, session).await;

        assert!(ctx.lights.read().await.by_id.contains_key("desk"));
        assert!(!ctx.lights.read().await.by_id.contains_key("lamp"));
        assert!(ctx.state.read().await.lights.contains_key("desk"));
        assert!(!ctx.state.read().await.lights.contains_key("lamp"));
    }

    #[tokio::test]
    async fn a_stale_or_disconnected_sweep_removes_nothing() {
        let (ctx, _eventloop, _requests) = test_ctx(true, 16);
        handle_publish(
            &ctx,
            "homeassistant/switch/desk/config",
            br#"{"name":"Desk","uniq_id":"desk","stat_t":"stat/desk/POWER","cmd_t":"cmnd/desk/POWER"}"#,
        )
        .await;

        // A newer session has started, so the older sweep must not run.
        let old = ctx.lights.write().await.begin_session();
        ctx.lights.write().await.begin_session();
        sweep_stale_discovery(&ctx, old).await;
        assert!(ctx.lights.read().await.by_id.contains_key("desk"));

        // The link dropped mid-replay, so "unseen" is not trustworthy yet.
        let current = ctx.lights.write().await.begin_session();
        ctx.mqtt.set_connected(false);
        sweep_stale_discovery(&ctx, current).await;
        assert!(ctx.lights.read().await.by_id.contains_key("desk"));
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

    #[test]
    fn watchdog_needs_a_full_idle_window_before_giving_up() {
        let mut stalled = 0;
        for _ in 0..WATCHDOG_STALL_LIMIT - 1 {
            let (next, give_up) = stall_check(7, 7, stalled);
            stalled = next;
            assert!(!give_up, "must not exit before the window is full");
        }

        let (_, give_up) = stall_check(7, 7, stalled);
        assert!(give_up, "a full idle window means the loop is stuck");
    }

    #[test]
    fn a_heartbeat_resets_the_watchdog() {
        let (stalled, give_up) = stall_check(8, 9, WATCHDOG_STALL_LIMIT);
        assert_eq!(stalled, 0);
        assert!(!give_up);
    }

    #[tokio::test]
    async fn the_event_loop_heartbeat_is_observable() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);
        let before = ctx.mqtt.polls();

        ctx.mqtt.beat();

        assert_eq!(ctx.mqtt.polls(), before + 1);
    }

    #[test]
    fn client_id_defaults_to_a_hostname_suffix() {
        assert_eq!(derive_client_id("alarm\n"), "ha-lite-alarm");
        assert_eq!(derive_client_id(""), "ha-lite");
        assert_eq!(derive_client_id("a.b c"), "ha-lite-a-b-c");
    }

    #[test]
    fn client_id_fits_the_mqtt_limit() {
        let id = derive_client_id("a-very-long-hostname-that-keeps-going");

        assert_eq!(id.len(), 23);
        assert!(id.starts_with("ha-lite-"));
    }
}
