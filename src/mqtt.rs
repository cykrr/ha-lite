//! MQTT plumbing: the outbound request worker, the event loop driver, and the
//! inbound message routing.

use std::{collections::HashSet, sync::Arc, time::Duration};

use rumqttc::{AsyncClient, Event, Incoming, QoS};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::app::{AppContext, LightView, MqttCommand};
use crate::bthome::{
    bthome_bytes, bthome_temperature_humidity, decode_bthome, record_bthome, summarize_bthome,
};
use crate::discovery::{handle_discovery, handle_tasmota_discovery};
use crate::lights::{handle_light_state, legacy_light_id, legacy_light_state};

/// Drains queued requests into the client, and therefore into the event loop.
///
/// This task is allowed to block when the client's bounded channel is full:
/// `poll()` keeps running and drains it. Doing the same awaits from the
/// event-loop task itself is what deadlocks, since nothing would drain the
/// channel while it waits.
pub(crate) async fn run_mqtt_requests(
    client: AsyncClient,
    mut requests: mpsc::UnboundedReceiver<MqttCommand>,
) {
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

/// Publish an empty payload to ask a device for its current state. OpenBeken
/// uses the channel's `/get` topic for this, Tasmota uses `cmnd/<topic>/STATE`.
/// Each topic is asked at most once per MQTT connection; the mark is cleared
/// on reconnect, and publishing failures are logged by the request worker.
pub(crate) fn request_state(ctx: &AppContext, topic: &str) {
    let first_request = ctx
        .requested_state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(topic.to_string());
    if !first_request {
        return;
    }

    ctx.mqtt_publish(topic, Vec::new(), QoS::AtLeastOnce, false);
}

/// Consume the broker event loop, reconnecting with backoff, and dispatch
/// every inbound message. Runs until the process exits.
pub(crate) async fn run_event_loop(ctx: Arc<AppContext>, mut eventloop: rumqttc::EventLoop) {
    let mut backoff = Duration::from_secs(1);

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                info!("connected to MQTT broker");
                ctx.mqtt_connected
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                backoff = Duration::from_secs(1);
                ctx.requested_state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clear();
                for (topic, qos) in [
                    ("sensors/#", QoS::AtMostOnce),
                    ("lights/+/state", QoS::AtLeastOnce),
                    // Discovery topics can be `component/object/config` or
                    // `component/node/object/config`, so listen to the whole tree.
                    ("homeassistant/#", QoS::AtLeastOnce),
                    ("tasmota/discovery/#", QoS::AtLeastOnce),
                ] {
                    ctx.mqtt_subscribe(topic, qos);
                }

                // Re-arm state subscriptions and ask each known device for its
                // current value, since most of them only publish on change.
                // Retained discovery replay calls this again; the request set
                // makes that a no-op instead of a duplicate round trip. State
                // topics are deduplicated because all channels of a multi-relay
                // device share the same RESULT/STATE topics.
                let configs: Vec<crate::lights::LightConfig> = {
                    let reg = ctx.lights.read().await;
                    reg.by_id.values().cloned().collect()
                };
                let state_topics: HashSet<&str> = configs
                    .iter()
                    .flat_map(|cfg| cfg.state_topics.iter().map(String::as_str))
                    .collect();
                for topic in state_topics {
                    ctx.mqtt_subscribe(topic, QoS::AtLeastOnce);
                }
                for cfg in &configs {
                    if let Some(topic) = &cfg.request_topic {
                        request_state(&ctx, topic);
                    }
                }
            }
            Ok(Event::Incoming(Incoming::Publish(msg))) => {
                if let Some(mac) = msg.topic.strip_prefix("tasmota/discovery/") {
                    if let Some(mac) = mac.strip_suffix("/config") {
                        handle_tasmota_discovery(&ctx, &msg.topic, mac, &msg.payload).await;
                    }
                    continue;
                }

                if let Some(platform) = msg
                    .topic
                    .strip_prefix("homeassistant/")
                    .and_then(|rest| rest.split('/').next())
                {
                    if msg.topic.ends_with("/config") {
                        handle_discovery(&ctx, &msg.topic, platform, &msg.payload).await;
                    }
                    continue;
                }

                if msg.topic.starts_with("sensors/") {
                    if msg.topic.starts_with("sensors/bthome") {
                        ctx.bthome_packets
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        // Proxies usually send a hex string (e.g. "4002ca0903bf13"),
                        // but accept raw BTHome bytes too.
                        let bytes = bthome_bytes(&msg.payload);

                        debug!(
                            topic = %msg.topic,
                            len = bytes.len(),
                            hex = %hex::encode(&bytes),
                            "BTHome broadcast received"
                        );

                        match decode_bthome(&bytes) {
                            None => warn!(
                                topic = %msg.topic,
                                hex = %hex::encode(&bytes),
                                "BTHome payload is encrypted or not v2"
                            ),
                            Some(readings) => {
                                let (t, h) = bthome_temperature_humidity(&readings);
                                if readings.is_empty() {
                                    warn!(
                                        topic = %msg.topic,
                                        hex = %hex::encode(&bytes),
                                        "BTHome payload had no supported objects"
                                    );
                                } else {
                                    info!(
                                        topic = %msg.topic,
                                        objects = %summarize_bthome(&readings),
                                        temp_c = ?t,
                                        humidity = ?h,
                                        "BTHome update"
                                    );
                                }
                                if t.is_some() || h.is_some() {
                                    record_bthome(&ctx, &msg.topic, &readings).await;
                                }
                            }
                        }
                    } else {
                        debug!(
                            topic = %msg.topic,
                            payload = %String::from_utf8_lossy(&msg.payload),
                            "unhandled sensor topic"
                        );
                    }
                } else if !handle_light_state(&ctx, &msg.topic, &msg.payload).await {
                    // Legacy convention: `lights/<id>/state` with ON/OFF
                    // payloads. Discovered entities get first claim on the
                    // topic, and only a single-segment id matches the
                    // `lights/+/state` subscription, so a nested `stat_t`
                    // can't be mistaken for a legacy light.
                    if let Some(id) = legacy_light_id(&msg.topic) {
                        match legacy_light_state(&msg.payload) {
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
                                topic = %msg.topic,
                                payload = %String::from_utf8_lossy(&msg.payload),
                                "ignoring malformed legacy light state"
                            ),
                        }
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                ctx.mqtt_connected
                    .store(false, std::sync::atomic::Ordering::Relaxed);
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
