use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

#[derive(Clone, Serialize)]
struct LightView {
    name: String,
    on: bool,
}

/// Latest reading from one BTHome sensor.
#[derive(Clone, Serialize)]
struct SensorView {
    temp_c: Option<f64>,
    humidity: Option<f64>,
    /// Unix seconds of the last packet that carried a value.
    last_seen: u64,
}

#[derive(Default, Clone, Serialize)]
struct HomeState {
    /// Keyed by BLE address or MQTT topic, so two sensors in range each keep
    /// their own readings instead of fighting over one slot.
    sensors: BTreeMap<String, SensorView>,
    /// Most recently reported value across all sensors, kept for consumers
    /// that just want a single number.
    temp_c: Option<f64>,
    humidity: Option<f64>,
    /// Sorted like `sensors`, so `/api/state` is stable between polls and the
    /// UI order does not jitter.
    lights: BTreeMap<String, LightView>,
}

#[derive(Clone)]
struct LightConfig {
    state_topics: Vec<String>,
    command_topic: String,
    /// Topic to publish an empty payload to in order to ask for current state.
    request_topic: Option<String>,
    payload_on: String,
    payload_off: String,
    /// When set, state payloads are JSON and this key holds ON/OFF.
    state_key: Option<String>,
}

impl LightConfig {
    /// Resolve a state payload to `Some(on)` for this entity. Returns `None`
    /// when the payload belongs to another channel or an unknown value.
    fn state_is_on(&self, raw: &str, json: Option<&serde_json::Value>) -> Option<bool> {
        let value = match &self.state_key {
            Some(key) => json?.get(key)?.as_str()?,
            None => raw,
        };
        if value == self.payload_on {
            Some(true)
        } else if value == self.payload_off {
            Some(false)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct LightRegistry {
    by_id: HashMap<String, LightConfig>,
    /// Several entities can share a state topic: every channel of a
    /// multi-relay Tasmota device is driven by the same `.../RESULT` and
    /// `.../STATE` topics, each reading its own `POWERn` JSON key.
    by_state_topic: HashMap<String, HashSet<String>>,
    /// Discovery topic that announced each entity, so an empty retained config
    /// removes exactly the entities it created.
    by_discovery_topic: HashMap<String, HashSet<String>>,
}

/// Drop `id` from a topic index, removing the topic once nobody owns it.
/// Returns `true` when the topic became unused.
fn unindex(
    index: &mut HashMap<String, HashSet<String>>,
    topic: &str,
    id: &str,
) -> bool {
    let now_empty = match index.get_mut(topic) {
        Some(ids) => {
            ids.remove(id);
            ids.is_empty()
        }
        None => false,
    };
    if now_empty {
        index.remove(topic);
    }
    now_empty
}

/// What changed when an entity was registered.
struct Registration {
    is_new: bool,
    /// State topics that had no owner before, so they still need a SUBSCRIBE.
    new_topics: Vec<String>,
    /// State topics this entity dropped from its config and nobody else owns;
    /// the caller should UNSUBSCRIBE them.
    freed_topics: Vec<String>,
}

impl LightRegistry {
    /// Insert or replace an entity announced on `discovery_topic`.
    fn insert(
        &mut self,
        id: String,
        config: LightConfig,
        discovery_topic: &str,
    ) -> Registration {
        // Every indexed topic is already subscribed, so only unowned ones need
        // a SUBSCRIBE. Computed before the indexes below are updated.
        let new_topics: Vec<String> = config
            .state_topics
            .iter()
            .filter(|topic| !self.by_state_topic.contains_key(*topic))
            .cloned()
            .collect();

        let mut freed_topics = Vec::new();
        let previous_topics = self.by_id.get(&id).map(|c| c.state_topics.clone());
        if let Some(previous_topics) = previous_topics {
            for topic in previous_topics {
                if !config.state_topics.contains(&topic)
                    && unindex(&mut self.by_state_topic, &topic, &id)
                {
                    freed_topics.push(topic);
                }
            }
        }

        // The same entity may be re-announced on a different discovery topic
        // (HA node id changes), so forget the old announcement.
        let previous_discovery_topics: Vec<String> = self
            .by_discovery_topic
            .iter()
            .filter(|(_, ids)| ids.contains(&id))
            .map(|(topic, _)| topic.clone())
            .collect();
        for topic in previous_discovery_topics {
            unindex(&mut self.by_discovery_topic, &topic, &id);
        }

        for topic in &config.state_topics {
            self.by_state_topic
                .entry(topic.clone())
                .or_default()
                .insert(id.clone());
        }
        self.by_discovery_topic
            .entry(discovery_topic.to_string())
            .or_default()
            .insert(id.clone());

        Registration {
            is_new: self.by_id.insert(id, config).is_none(),
            new_topics,
            freed_topics,
        }
    }

    /// Remove entities by id, forgetting both indexes. Returns the ids that
    /// existed and any state topics that no longer have an owner.
    fn remove_ids(&mut self, ids: impl IntoIterator<Item = String>) -> (Vec<String>, Vec<String>) {
        let mut removed = Vec::new();
        let mut freed_topics = Vec::new();

        for id in ids {
            // An entity normally has exactly one discovery topic, but clear
            // every index entry rather than assuming that.
            let indexed_topics: Vec<String> = self
                .by_discovery_topic
                .iter()
                .filter(|(_, ids)| ids.contains(&id))
                .map(|(topic, _)| topic.clone())
                .collect();
            for topic in indexed_topics {
                unindex(&mut self.by_discovery_topic, &topic, &id);
            }

            let Some(config) = self.by_id.remove(&id) else {
                continue;
            };
            for topic in &config.state_topics {
                if unindex(&mut self.by_state_topic, topic, &id) {
                    freed_topics.push(topic.clone());
                }
            }
            removed.push(id);
        }

        (removed, freed_topics)
    }

    /// Remove every entity announced on this discovery topic.
    fn remove_by_discovery_topic(&mut self, discovery_topic: &str) -> (Vec<String>, Vec<String>) {
        let ids: Vec<String> = self
            .by_discovery_topic
            .get(discovery_topic)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default();
        self.remove_ids(ids)
    }

    /// Remove entities this discovery topic used to announce but no longer
    /// does. A Tasmota config can shrink (a relay is disabled) without the
    /// discovery topic itself being deleted.
    fn retain_discovery_topic(
        &mut self,
        discovery_topic: &str,
        keep: &HashSet<String>,
    ) -> (Vec<String>, Vec<String>) {
        let stale: Vec<String> = self
            .by_discovery_topic
            .get(discovery_topic)
            .map(|ids| {
                ids.iter()
                    .filter(|id| !keep.contains(*id))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        self.remove_ids(stale)
    }

    /// Every entity whose state may be carried by this topic.
    fn ids_for_topic(&self, topic: &str) -> Vec<String> {
        self.by_state_topic
            .get(topic)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }
}

struct AppContext {
    state: RwLock<HomeState>,
    lights: RwLock<LightRegistry>,
    bthome_packets: AtomicU64,
    /// State topics already asked for a snapshot on this MQTT connection, so
    /// the reconnect re-arm and the retained discovery replay don't both ask.
    requested_state: Mutex<HashSet<String>>,
    /// Used directly by HTTP handlers, which run outside the MQTT event loop.
    /// Never await this from inside `eventloop.poll()`; use [`Self::mqtt_request`].
    mqtt: AsyncClient,
    /// Outbound requests created while handling an MQTT event.
    ///
    /// `AsyncClient` feeds a bounded channel that only `EventLoop::poll` drains.
    /// Awaiting `subscribe`/`publish` from the task that is *running* `poll`
    /// blocks that task, so a burst larger than the channel capacity (32) would
    /// wedge the event loop forever. A reconnect re-arming enough lights is
    /// enough to do it. These commands are sent by a separate worker instead.
    mqtt_requests: mpsc::UnboundedSender<MqttCommand>,
}

/// One outbound MQTT request queued for the request worker.
enum MqttCommand {
    Subscribe(String, QoS),
    Unsubscribe(String),
    Publish(String, Vec<u8>, QoS, bool),
}

impl AppContext {
    fn mqtt_request(&self, request: MqttCommand) {
        // Only fails once the worker (and therefore the process) is going away.
        if self.mqtt_requests.send(request).is_err() {
            warn!("MQTT request worker stopped; dropping request");
        }
    }

    fn mqtt_subscribe(&self, topic: &str, qos: QoS) {
        self.mqtt_request(MqttCommand::Subscribe(topic.to_string(), qos));
    }

    fn mqtt_unsubscribe(&self, topic: &str) {
        self.mqtt_request(MqttCommand::Unsubscribe(topic.to_string()));
    }

    fn mqtt_publish(&self, topic: &str, payload: Vec<u8>, qos: QoS, retain: bool) {
        self.mqtt_request(MqttCommand::Publish(
            topic.to_string(),
            payload,
            qos,
            retain,
        ));
    }
}

/// Subset of a Home Assistant MQTT discovery payload that ha-lite understands.
#[derive(Deserialize)]
struct DiscoveryConfig {
    name: Option<String>,
    uniq_id: Option<String>,
    stat_t: Option<String>,
    cmd_t: Option<String>,
    pl_on: Option<String>,
    pl_off: Option<String>,
    #[serde(rename = "~")]
    tilde: Option<String>,
    dev: Option<DiscoveryDevice>,
}

#[derive(Deserialize)]
struct DiscoveryDevice {
    name: Option<String>,
}

/// Subset of the Tasmota integration discovery payload
/// (`tasmota/discovery/<MAC>/config`).
#[derive(Deserialize)]
struct TasmotaDiscovery {
    dn: Option<String>,
    #[serde(rename = "fn", default)]
    friendly_names: Vec<Option<String>>,
    ft: Option<String>,
    t: Option<String>,
    #[serde(default)]
    tp: Vec<String>,
    #[serde(default)]
    rl: Vec<i64>,
    so: Option<serde_json::Value>,
    #[serde(default)]
    state: Vec<String>,
}

#[derive(Clone, Copy)]
enum BthomeKind {
    U8,
    I8,
    U16,
    I16,
    U24,
    U32,
    I32,
}

impl BthomeKind {
    fn len(self) -> usize {
        match self {
            BthomeKind::U8 | BthomeKind::I8 => 1,
            BthomeKind::U16 | BthomeKind::I16 => 2,
            BthomeKind::U24 => 3,
            BthomeKind::U32 | BthomeKind::I32 => 4,
        }
    }

    fn read(self, b: &[u8]) -> f64 {
        match self {
            BthomeKind::U8 => b[0] as f64,
            BthomeKind::I8 => b[0] as i8 as f64,
            BthomeKind::U16 => u16::from_le_bytes([b[0], b[1]]) as f64,
            BthomeKind::I16 => i16::from_le_bytes([b[0], b[1]]) as f64,
            BthomeKind::U24 => (b[0] as u32 | (b[1] as u32) << 8 | (b[2] as u32) << 16) as f64,
            BthomeKind::U32 => u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as f64,
            BthomeKind::I32 => i32::from_le_bytes([b[0], b[1], b[2], b[3]]) as f64,
        }
    }
}

struct BthomeObject {
    id: u8,
    name: &'static str,
    kind: BthomeKind,
    factor: f64,
}

struct BthomeReading {
    name: &'static str,
    value: f64,
}

/// BTHome v2 object table (sensor + binary sensor + device info).
/// Variable-length objects (text/raw/events) are intentionally absent; the
/// decoder stops when it hits one, as the spec requires for unknown ids.
const BTHOME_OBJECTS: &[BthomeObject] = &[
    BthomeObject { id: 0x00, name: "packet_id", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x01, name: "battery", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x02, name: "temperature", kind: BthomeKind::I16, factor: 0.01 },
    BthomeObject { id: 0x03, name: "humidity", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x04, name: "pressure", kind: BthomeKind::U24, factor: 0.01 },
    BthomeObject { id: 0x05, name: "illuminance", kind: BthomeKind::U24, factor: 0.01 },
    BthomeObject { id: 0x06, name: "mass_kg", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x07, name: "mass_lb", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x08, name: "dewpoint", kind: BthomeKind::I16, factor: 0.01 },
    BthomeObject { id: 0x09, name: "count", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x0A, name: "energy", kind: BthomeKind::U24, factor: 0.001 },
    BthomeObject { id: 0x0B, name: "power", kind: BthomeKind::U24, factor: 0.01 },
    BthomeObject { id: 0x0C, name: "voltage", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x0D, name: "pm2_5", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x0E, name: "pm10", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x0F, name: "generic_boolean", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x10, name: "power_state", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x11, name: "opening", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x12, name: "co2", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x13, name: "tvoc", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x14, name: "moisture", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x15, name: "battery_low", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x16, name: "battery_charging", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x17, name: "carbon_monoxide", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x18, name: "cold", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x19, name: "connectivity", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1A, name: "door", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1B, name: "garage_door", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1C, name: "gas", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1D, name: "heat", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1E, name: "light", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1F, name: "lock", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x20, name: "moisture_state", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x21, name: "motion", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x22, name: "moving", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x23, name: "occupancy", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x24, name: "plug", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x25, name: "presence", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x26, name: "problem", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x27, name: "running", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x28, name: "safety", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x29, name: "smoke", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2A, name: "sound", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2B, name: "tamper", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2C, name: "vibration", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2D, name: "window", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2E, name: "humidity", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2F, name: "moisture", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x3D, name: "count", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x3E, name: "count", kind: BthomeKind::U32, factor: 1.0 },
    BthomeObject { id: 0x3F, name: "rotation", kind: BthomeKind::I16, factor: 0.1 },
    BthomeObject { id: 0x40, name: "distance_mm", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x41, name: "distance_m", kind: BthomeKind::U16, factor: 0.1 },
    BthomeObject { id: 0x42, name: "duration", kind: BthomeKind::U24, factor: 0.001 },
    BthomeObject { id: 0x43, name: "current", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x44, name: "speed", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x45, name: "temperature", kind: BthomeKind::I16, factor: 0.1 },
    BthomeObject { id: 0x46, name: "uv_index", kind: BthomeKind::U8, factor: 0.1 },
    BthomeObject { id: 0x47, name: "volume", kind: BthomeKind::U16, factor: 0.1 },
    BthomeObject { id: 0x48, name: "volume", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x49, name: "volume_flow_rate", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x4A, name: "voltage", kind: BthomeKind::U16, factor: 0.1 },
    BthomeObject { id: 0x4B, name: "gas", kind: BthomeKind::U24, factor: 0.001 },
    BthomeObject { id: 0x4C, name: "gas", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x4D, name: "energy", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x4E, name: "volume", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x4F, name: "water", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x50, name: "timestamp", kind: BthomeKind::U32, factor: 1.0 },
    BthomeObject { id: 0x51, name: "acceleration", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x52, name: "gyroscope", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x55, name: "volume_storage", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x56, name: "conductivity", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x57, name: "temperature", kind: BthomeKind::I8, factor: 1.0 },
    BthomeObject { id: 0x58, name: "temperature", kind: BthomeKind::I8, factor: 0.35 },
    BthomeObject { id: 0x59, name: "count", kind: BthomeKind::I8, factor: 1.0 },
    BthomeObject { id: 0x5A, name: "count", kind: BthomeKind::I16, factor: 1.0 },
    BthomeObject { id: 0x5B, name: "count", kind: BthomeKind::I32, factor: 1.0 },
    BthomeObject { id: 0x5C, name: "power", kind: BthomeKind::I32, factor: 0.01 },
    BthomeObject { id: 0x5D, name: "current", kind: BthomeKind::I16, factor: 0.001 },
    BthomeObject { id: 0x5E, name: "direction", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x5F, name: "precipitation", kind: BthomeKind::U16, factor: 0.1 },
    BthomeObject { id: 0x60, name: "channel", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x61, name: "rotational_speed", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x62, name: "speed", kind: BthomeKind::I32, factor: 0.000001 },
    BthomeObject { id: 0x63, name: "acceleration", kind: BthomeKind::I32, factor: 0.000001 },
    BthomeObject { id: 0x64, name: "light_level", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x65, name: "settings_revision", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0xF0, name: "device_type_id", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0xF1, name: "firmware_version", kind: BthomeKind::U32, factor: 1.0 },
    BthomeObject { id: 0xF2, name: "firmware_version", kind: BthomeKind::U24, factor: 1.0 },
];

/// Returns `None` when the payload isn't unencrypted BTHome v2.
fn decode_bthome(payload: &[u8]) -> Option<Vec<BthomeReading>> {
    if payload.len() < 2 || (payload[0] & 0xE0) != 0x40 {
        return None;
    }
    if payload[0] & 0x01 != 0 {
        return None; // Encrypted; needs a pre-shared key
    }

    let mut readings = Vec::new();
    let mut i = 1;
    while i < payload.len() {
        let id = payload[i];
        let spec = match BTHOME_OBJECTS.binary_search_by_key(&id, |o| o.id) {
            Ok(index) => &BTHOME_OBJECTS[index],
            Err(_) => {
                debug!(object_id = %format!("0x{id:02x}"), offset = i, "unknown BTHome object; stopping");
                break;
            }
        };

        let len = spec.kind.len();
        if i + 1 + len > payload.len() {
            debug!(object_id = %format!("0x{id:02x}"), "truncated BTHome object; stopping");
            break;
        }

        let raw = spec.kind.read(&payload[i + 1..i + 1 + len]);
        readings.push(BthomeReading {
            name: spec.name,
            value: raw * spec.factor,
        });
        i += 1 + len;
    }

    Some(readings)
}

fn summarize_bthome(readings: &[BthomeReading]) -> String {
    readings
        .iter()
        .map(|r| format!("{}={:.3}", r.name, r.value))
        .collect::<Vec<_>>()
        .join(" ")
}

/// First temperature and humidity in a packet. BTHome allows repeated object
/// ids (e.g. internal + external probe); the first one is the primary reading.
fn bthome_temperature_humidity(readings: &[BthomeReading]) -> (Option<f64>, Option<f64>) {
    let (mut temp, mut humidity) = (None, None);
    for reading in readings {
        match reading.name {
            "temperature" if temp.is_none() => temp = Some(round2(reading.value)),
            "humidity" if humidity.is_none() => humidity = Some(round2(reading.value)),
            _ => {}
        }
    }
    (temp, humidity)
}

/// Round to the 0.01 resolution BTHome uses for temperature/humidity, so
/// binary floats don't leak artifacts like 42.830000000000002 into JSON.
fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Record one sensor update under its source identity (BLE address or MQTT
/// topic). Metrics missing from this packet keep their previous value.
fn record_sensor(state: &mut HomeState, source: &str, t: Option<f64>, h: Option<f64>, now: u64) {
    if t.is_none() && h.is_none() {
        return;
    }

    let sensor = state
        .sensors
        .entry(source.to_string())
        .or_insert_with(|| SensorView {
            temp_c: None,
            humidity: None,
            last_seen: now,
        });
    if t.is_some() {
        sensor.temp_c = t;
    }
    if h.is_some() {
        sensor.humidity = h;
    }
    sensor.last_seen = now;

    if t.is_some() {
        state.temp_c = t;
    }
    if h.is_some() {
        state.humidity = h;
    }
}

async fn record_bthome(ctx: &Arc<AppContext>, source: &str, readings: &[BthomeReading]) {
    let (t, h) = bthome_temperature_humidity(readings);
    if t.is_none() && h.is_none() {
        return;
    }
    let mut state = ctx.state.write().await;
    record_sensor(&mut state, source, t, h, unix_secs());
}

/// Proxies usually send a hex string, sometimes with a trailing newline; raw
/// BTHome bytes are accepted too. Whitespace is ignored for hex text only, so
/// it can't corrupt a raw packet that happens to contain 0x20/0x0a.
fn bthome_bytes(payload: &[u8]) -> Vec<u8> {
    if let Ok(text) = std::str::from_utf8(payload) {
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        if !compact.is_empty() && compact.chars().all(|c| c.is_ascii_hexdigit()) {
            if let Ok(bytes) = hex::decode(&compact) {
                return bytes;
            }
        }
    }
    payload.to_vec()
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Expand HA discovery's `~` abbreviation against the device topic base.
fn expand_topic(base: Option<&str>, topic: &str) -> String {
    match base {
        Some(base) if topic == "~" => base.to_string(),
        Some(base) => match topic.strip_prefix("~/") {
            Some(rest) => format!("{base}/{rest}"),
            None => topic.to_string(),
        },
        None => topic.to_string(),
    }
}

/// Stop discovery on every adapter on the way out. BlueZ remembers an active
/// scan per client, so a clean exit avoids leaving a stale scan behind for the
/// next start. Best effort: this runs while shutting down, so errors are
/// ignored (a stopped or vanished adapter is fine).
async fn stop_ble_scans() {
    use btleplug::api::{Central as _, Manager as _};
    use btleplug::platform::Manager;

    let Ok(manager) = Manager::new().await else {
        return;
    };
    let Ok(adapters) = manager.adapters().await else {
        return;
    };
    for adapter in adapters {
        let _ = adapter.stop_scan().await;
    }
}

/// Scan the local Bluetooth adapter for BTHome advertisements and feed the
/// same state the MQTT sensor topic feeds. BTHome v2 service data uses the
/// 0xFCD2 UUID and starts with a 0x40-tagged byte, which `decode_bthome`
/// already understands.
///
/// Adapters come and go: a dongle can reset, `bluetoothd` can restart, and the
/// event stream then just ends. So this is a supervisor that re-enumerates
/// adapters and re-arms scanning with the same 1s -> 30s backoff as MQTT.
async fn ble_scanner(ctx: Arc<AppContext>) {
    const MIN_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    // Sessions shorter than this are treated as failures, so a scan that dies
    // right away backs off instead of spinning.
    const HEALTHY_AFTER: Duration = Duration::from_secs(10);

    let mut backoff = MIN_BACKOFF;
    let mut failures = 0u32;

    loop {
        let started = Instant::now();
        let outcome = run_ble_session(&ctx).await;
        let ran_for = started.elapsed();

        match &outcome {
            Ok(()) => warn!("BLE event stream ended; restarting scan"),
            Err(reason) if failures == 0 => {
                warn!(%reason, retry_in = ?backoff, "BLE scanning unavailable")
            }
            Err(reason) => debug!(%reason, "BLE still unavailable"),
        }

        let healthy = ran_for >= HEALTHY_AFTER;
        if healthy {
            // A healthy run earns a fresh 1s retry for whatever comes next.
            backoff = MIN_BACKOFF;
            failures = 0;
        } else {
            failures = failures.saturating_add(1);
        }

        sleep(backoff).await;

        // Only grow the delay after a failure. Doubling unconditionally would
        // make the first failure after a healthy session wait 2s, not 1s.
        if !healthy {
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }
}

/// One setup-and-run pass: enumerate adapters, start discovery, and consume
/// their event streams until the first one stops. Dropping the `JoinSet` aborts
/// any adapter still running, so the next pass starts from a clean slate.
async fn run_ble_session(ctx: &Arc<AppContext>) -> Result<(), String> {
    use btleplug::api::Manager as _;
    use btleplug::platform::Manager;
    use tokio::task::JoinSet;

    let manager = Manager::new()
        .await
        .map_err(|e| format!("BLE unavailable: {e}"))?;
    let adapters = manager
        .adapters()
        .await
        .map_err(|e| format!("could not list BLE adapters: {e}"))?;

    if adapters.is_empty() {
        return Err("no BLE adapters found".to_string());
    }

    let mut tasks = JoinSet::new();
    for adapter in adapters {
        let ctx = ctx.clone();
        tasks.spawn(scan_adapter(ctx, adapter));
    }

    // A failing adapter (an onboard chip disabled in firmware, say) must not
    // take a working adapter down with it, so keep waiting while any adapter
    // still runs. An event stream that ends is different: that adapter was
    // reset, so hand back to the supervisor to re-enumerate everything.
    let mut last_error = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(reason)) => {
                debug!(%reason, "BLE adapter unavailable; other adapters still running");
                last_error = Some(reason);
            }
            Err(join_error) => {
                let reason = format!("BLE scan task failed: {join_error}");
                debug!(%reason, "BLE adapter task failed; other adapters still running");
                last_error = Some(reason);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| "no BLE scan tasks started".to_string()))
}

/// Start discovery on one adapter and process its advertisements until the
/// stream ends. Returns `Err` when scanning cannot be started at all.
async fn scan_adapter(
    ctx: Arc<AppContext>,
    adapter: btleplug::platform::Adapter,
) -> Result<(), String> {
    use btleplug::api::bleuuid::uuid_from_u16;
    use btleplug::api::{Central, CentralEvent, Peripheral as _, ScanFilter};

    let bthome_uuid = uuid_from_u16(0xFCD2);

    // A previous pass may have left discovery running; clear it so this pass
    // gets a fresh event stream. Fails harmlessly when already stopped.
    let _ = adapter.stop_scan().await;

    adapter
        .start_scan(ScanFilter::default())
        .await
        .map_err(|e| format!("failed to start BLE scan: {e}"))?;

    let mut events = adapter
        .events()
        .await
        .map_err(|e| format!("failed to open BLE event stream: {e}"))?;

    info!("BLE scan started");

    while let Some(event) = events.next().await {
        let CentralEvent::ServiceDataAdvertisement { id, service_data } = event else {
            continue;
        };

        let address = adapter
            .peripheral(&id)
            .await
            .map(|p| p.address().to_string())
            .unwrap_or_else(|_| format!("{id:?}"));

        for (uuid, data) in service_data {
            if uuid != bthome_uuid {
                debug!(%address, %uuid, hex = %hex::encode(&data), "BLE service data");
                continue;
            }

            ctx.bthome_packets.fetch_add(1, Ordering::Relaxed);
            let hex_payload = hex::encode(&data);
            debug!(%address, hex = %hex_payload, "BLE BTHome advertisement");

            let Some(readings) = decode_bthome(&data) else {
                warn!(
                    %address,
                    hex = %hex_payload,
                    "BLE BTHome advertisement is encrypted or not v2"
                );
                continue;
            };

            let (t, h) = bthome_temperature_humidity(&readings);
            if readings.is_empty() {
                warn!(
                    %address,
                    hex = %hex_payload,
                    "BLE BTHome advertisement had no supported objects"
                );
                continue;
            }

            info!(
                %address,
                objects = %summarize_bthome(&readings),
                temp_c = ?t,
                humidity = ?h,
                "BLE BTHome update"
            );

            record_bthome(&ctx, &address, &readings).await;
        }
    }

    Ok(())
}

/// Drains queued requests into the client, and therefore into the event loop.
///
/// This task is allowed to block when the client's bounded channel is full:
/// `poll()` keeps running and drains it. Doing the same awaits from the
/// event-loop task itself is what deadlocks, since nothing would drain the
/// channel while it waits.
async fn run_mqtt_requests(
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

#[tokio::main]
async fn main() {
    // Load .env before the subscriber so RUST_LOG can be set there too.
    let _ = dotenv::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let host = env_or("MQTT_HOST", "127.0.0.1");
    let port: u16 = env_or("MQTT_PORT", "1883").parse().unwrap_or(1883);
    let user = std::env::var("MQTT_USER").unwrap_or_default();
    let password = std::env::var("MQTT_PASSWORD").unwrap_or_default();

    info!(%host, port, authenticated = !user.is_empty(), "connecting to MQTT broker");

    let mut mqttoptions = MqttOptions::new("ha-lite", &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(15));
    if !user.is_empty() {
        mqttoptions.set_credentials(user, password);
    }

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 32);
    // The event-loop task must never await `client`: its 32-slot request
    // channel is only drained by `poll()`, which is the task that would block.
    let (mqtt_requests, mqtt_request_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_mqtt_requests(client.clone(), mqtt_request_rx));
    let ctx = Arc::new(AppContext {
        state: RwLock::new(HomeState::default()),
        lights: RwLock::new(LightRegistry::default()),
        bthome_packets: AtomicU64::new(0),
        requested_state: Mutex::new(HashSet::new()),
        mqtt: client,
        mqtt_requests,
    });

    // An empty BTHome feed should be loud, not silent.
    info!("waiting for BTHome broadcasts (MQTT sensors/# and local BLE)");
    let watch_ctx = ctx.clone();
    tokio::spawn(async move {
        let mut waited_secs = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            if watch_ctx.bthome_packets.load(Ordering::Relaxed) > 0 {
                break;
            }
            waited_secs += 60;
            warn!(
                waited_secs,
                "no BTHome broadcasts received yet (MQTT sensors/# or local BLE)"
            );
        }
    });

    // Scan BLE directly for BTHome service data (UUID 0xFCD2).
    tokio::spawn(ble_scanner(ctx.clone()));

    // Background MQTT listener with automatic reconnect
    let bg_ctx = ctx.clone();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);

        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    info!("connected to MQTT broker");
                    backoff = Duration::from_secs(1);
                    bg_ctx
                        .requested_state
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
                        bg_ctx.mqtt_subscribe(topic, qos);
                    }

                    // Re-arm state subscriptions and ask each known device for its
                    // current value, since most of them only publish on change.
                    // Retained discovery replay calls this again; the request set
                    // makes that a no-op instead of a duplicate round trip. State
                    // topics are deduplicated because all channels of a multi-relay
                    // device share the same RESULT/STATE topics.
                    let configs: Vec<LightConfig> = {
                        let reg = bg_ctx.lights.read().await;
                        reg.by_id.values().cloned().collect()
                    };
                    let state_topics: HashSet<&str> = configs
                        .iter()
                        .flat_map(|cfg| cfg.state_topics.iter().map(String::as_str))
                        .collect();
                    for topic in state_topics {
                        bg_ctx.mqtt_subscribe(topic, QoS::AtLeastOnce);
                    }
                    for cfg in &configs {
                        if let Some(topic) = &cfg.request_topic {
                            request_state(&bg_ctx, topic);
                        }
                    }
                }
                Ok(Event::Incoming(Incoming::Publish(msg))) => {
                    if let Some(mac) = msg.topic.strip_prefix("tasmota/discovery/") {
                        if let Some(mac) = mac.strip_suffix("/config") {
                            handle_tasmota_discovery(&bg_ctx, &msg.topic, mac, &msg.payload).await;
                        }
                        continue;
                    }

                    if let Some(platform) = msg
                        .topic
                        .strip_prefix("homeassistant/")
                        .and_then(|rest| rest.split('/').next())
                    {
                        if msg.topic.ends_with("/config") {
                            handle_discovery(&bg_ctx, &msg.topic, platform, &msg.payload).await;
                        }
                        continue;
                    }

                    if msg.topic.starts_with("sensors/") {
                        if msg.topic.starts_with("sensors/bthome") {
                            bg_ctx.bthome_packets.fetch_add(1, Ordering::Relaxed);

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
                                        record_bthome(&bg_ctx, &msg.topic, &readings).await;
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
                    } else if let Some(id) = msg
                        .topic
                        .strip_prefix("lights/")
                        .and_then(|s| s.strip_suffix("/state"))
                    {
                        // Legacy convention: lights/<id>/state with ON/OFF payloads.
                        let is_on = msg.payload.as_ref() == b"ON";
                        let mut state = bg_ctx.state.write().await;
                        state
                            .lights
                            .entry(id.to_string())
                            .or_insert_with(|| LightView {
                                name: id.to_string(),
                                on: is_on,
                            })
                            .on = is_on;
                    } else {
                        handle_light_state(&bg_ctx, &msg.topic, &msg.payload).await;
                    }
                }
                Ok(_) => {}
                Err(e) => {
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
    });

    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/api/state", get(get_state))
        .route("/api/lights/:id/toggle", post(toggle_light))
        .with_state(ctx);

    // Bind on all interfaces by default; set BIND_ADDR=127.0.0.1:8082 to sit
    // behind a reverse proxy, or to run a second instance on another port.
    let bind_addr = env_or("BIND_ADDR", "0.0.0.0:8082");

    let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!(error = %e, %bind_addr, "failed to bind HTTP listener; exiting");
            return;
        }
    };
    info!(%bind_addr, "HTTP server listening");
    let shutdown = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "failed to listen for shutdown signal");
        }
    };
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
    {
        error!(error = %e, "HTTP server failed");
    }

    stop_ble_scans().await;
    info!("shutting down");
}

async fn handle_discovery(
    ctx: &Arc<AppContext>,
    discovery_topic: &str,
    platform: &str,
    payload: &[u8],
) {
    if platform != "switch" && platform != "light" {
        debug!(%platform, "discovery config for unsupported platform; ignoring");
        return;
    }

    if payload.is_empty() {
        // Empty retained config is Home Assistant's deletion signal.
        remove_discovery(ctx, discovery_topic).await;
        return;
    }

    let cfg: DiscoveryConfig = match serde_json::from_slice(payload) {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!(error = %e, %platform, "ignoring malformed discovery config");
            return;
        }
    };

    let Some(entity) = ha_entity(&cfg) else {
        debug!(%platform, "discovery config without stat_t/cmd_t; skipping");
        return;
    };

    // One discovery topic describes one entity. If a re-announcement changes
    // `uniq_id`, no deletion message will ever arrive for the old one, so drop
    // it here instead of leaving a dead switch in the UI. Register first: an
    // entity that keeps the same state topic should not be unsubscribed just
    // to be subscribed again.
    let announced = HashSet::from([entity.id.clone()]);
    register_light(ctx, entity.id, entity.name, entity.config, discovery_topic).await;

    let (removed, freed_topics) = {
        let mut reg = ctx.lights.write().await;
        reg.retain_discovery_topic(discovery_topic, &announced)
    };
    forget_lights(ctx, &removed, freed_topics).await;
    if !removed.is_empty() {
        info!(%discovery_topic, ids = ?removed, "replaced light(s)");
    }
}

struct HaEntity {
    id: String,
    name: String,
    config: LightConfig,
}

fn ha_entity(cfg: &DiscoveryConfig) -> Option<HaEntity> {
    let stat_t = cfg.stat_t.as_deref()?;
    let cmd_t = cfg.cmd_t.as_deref()?;

    let state_topic = expand_topic(cfg.tilde.as_deref(), stat_t);
    let command_topic = expand_topic(cfg.tilde.as_deref(), cmd_t);
    let id = cfg
        .uniq_id
        .clone()
        .unwrap_or_else(|| command_topic.replace('/', "_"));
    let name = match (
        cfg.dev.as_ref().and_then(|d| d.name.clone()),
        cfg.name.clone(),
    ) {
        (Some(dev), Some(part)) if !part.is_empty() && part != dev => format!("{dev} {part}"),
        (Some(dev), _) => dev,
        (None, Some(part)) if !part.is_empty() => part,
        _ => id.clone(),
    };

    Some(HaEntity {
        id,
        name,
        config: LightConfig {
            state_topics: vec![state_topic.clone()],
            command_topic,
            request_topic: infer_request_topic(&state_topic),
            payload_on: cfg.pl_on.clone().unwrap_or_else(|| "ON".to_string()),
            payload_off: cfg.pl_off.clone().unwrap_or_else(|| "OFF".to_string()),
            state_key: None,
        },
    })
}

/// Only publish a state request to topics a device actually treats as one.
/// OpenBeken advertises its `.../get` channel topic as `stat_t`, so an empty
/// message there returns the current value. Everything else (ESPHome,
/// Zigbee2MQTT, generic MQTT) uses `stat_t` purely for outbound state, where
/// an empty publish is meaningless and could clear a retained value.
fn infer_request_topic(state_topic: &str) -> Option<String> {
    state_topic
        .ends_with("/get")
        .then(|| state_topic.to_string())
}

/// Tasmota integration discovery. Modern Tasmota no longer supports the
/// `homeassistant/...` discovery topic; it publishes `rl` (relay/light/shutter
/// per channel) and derives all MQTT topics from `ft` (full topic), `tp`
/// (prefixes) and `t` (device topic).
async fn handle_tasmota_discovery(
    ctx: &Arc<AppContext>,
    discovery_topic: &str,
    mac: &str,
    payload: &[u8],
) {
    if payload.is_empty() {
        remove_discovery(ctx, discovery_topic).await;
        return;
    }

    let cfg: TasmotaDiscovery = match serde_json::from_slice(payload) {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!(error = %e, %mac, "ignoring malformed Tasmota discovery config");
            return;
        }
    };

    let Some(device_topic) = cfg.t.clone() else {
        debug!(%mac, "Tasmota discovery without topic; skipping");
        return;
    };

    let entities = tasmota_entities(mac, &cfg);
    let announced: HashSet<String> = entities.iter().map(|entity| entity.id.clone()).collect();

    if entities.is_empty() {
        debug!(%mac, %device_topic, "Tasmota device has no relay/light channels");
    } else {
        for entity in entities {
            info!(
                %mac,
                %device_topic,
                kind = entity.kind,
                id = %entity.id,
                "discovered Tasmota channel"
            );
            register_light(ctx, entity.id, entity.name, entity.config, discovery_topic).await;
        }
    }

    // A retained config can shrink without being deleted: relays get disabled
    // or the device becomes sensor-only. Evict whatever this topic used to
    // announce and no longer does, otherwise the UI keeps dead switches. This
    // runs after registration so a surviving channel keeps its shared state
    // topic subscribed without an unsubscribe/subscribe round trip.
    let (removed, freed_topics) = {
        let mut reg = ctx.lights.write().await;
        reg.retain_discovery_topic(discovery_topic, &announced)
    };
    forget_lights(ctx, &removed, freed_topics).await;
    if !removed.is_empty() {
        info!(%mac, %device_topic, ids = ?removed, "removed stale Tasmota channel(s)");
    }
}

struct TasmotaEntity {
    id: String,
    name: String,
    kind: &'static str,
    config: LightConfig,
}

fn tasmota_entities(mac: &str, cfg: &TasmotaDiscovery) -> Vec<TasmotaEntity> {
    let Some(device_topic) = cfg.t.as_deref() else {
        return Vec::new();
    };

    let full_topic = cfg.ft.as_deref().unwrap_or("%prefix%/%topic%/");
    let default_prefixes = ["cmnd", "stat", "tele"];
    let prefix = |i: usize| {
        cfg.tp
            .get(i)
            .map(String::as_str)
            .unwrap_or(default_prefixes[i])
    };

    // `mac` is taken raw from an MQTT topic, which is arbitrary UTF-8. Slice
    // on a character boundary so a malformed topic cannot panic the MQTT task;
    // six characters is the same result as six bytes for a real hex MAC.
    let short_id = mac
        .char_indices()
        .rev()
        .nth(5)
        .map(|(start, _)| &mac[start..])
        .unwrap_or(mac);
    let base = |prefix: &str| {
        full_topic
            .replace("%prefix%", prefix)
            .replace("%topic%", device_topic)
            .replace("%hostname%", device_topic)
            .replace("%id%", short_id)
    };
    let cmnd_base = base(prefix(0));
    let stat_base = base(prefix(1));
    let tele_base = base(prefix(2));
    let result_topic = format!("{stat_base}RESULT");
    let tele_state_topic = format!("{tele_base}STATE");
    let request_topic = format!("{cmnd_base}STATE");

    let force_light = cfg
        .so
        .as_ref()
        .and_then(|so| so.get("30"))
        .is_some_and(|v| v.as_i64() == Some(1) || v.as_str() == Some("1"));

    let channels: Vec<(usize, i64)> = cfg
        .rl
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, kind)| *kind == 1 || *kind == 2)
        .collect();
    let multiple = channels.len() > 1;

    // Tasmota's `state` is [off, on, toggle, hold].
    let payload_off = cfg.state.first().cloned().unwrap_or_else(|| "OFF".to_string());
    let payload_on = cfg.state.get(1).cloned().unwrap_or_else(|| "ON".to_string());

    channels
        .into_iter()
        .map(|(idx, kind)| {
            let key = if multiple {
                format!("POWER{}", idx + 1)
            } else {
                "POWER".to_string()
            };

            let name = cfg
                .friendly_names
                .get(idx)
                .and_then(|n| n.clone())
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| {
                    let device = cfg.dn.as_deref().unwrap_or(device_topic);
                    if multiple {
                        format!("{device} {}", idx + 1)
                    } else {
                        device.to_string()
                    }
                });

            TasmotaEntity {
                id: format!("tasmota_{mac}_{}", idx + 1),
                name,
                kind: if kind == 2 || force_light { "light" } else { "switch" },
                config: LightConfig {
                    state_topics: vec![result_topic.clone(), tele_state_topic.clone()],
                    command_topic: format!("{cmnd_base}{key}"),
                    request_topic: Some(request_topic.clone()),
                    payload_on: payload_on.clone(),
                    payload_off: payload_off.clone(),
                    state_key: Some(key),
                },
            }
        })
        .collect()
}

async fn register_light(
    ctx: &Arc<AppContext>,
    id: String,
    name: String,
    config: LightConfig,
    discovery_topic: &str,
) {
    let registration = {
        let mut reg = ctx.lights.write().await;
        reg.insert(id.clone(), config.clone(), discovery_topic)
    };

    {
        let mut state = ctx.state.write().await;
        let view = state.lights.entry(id.clone()).or_insert_with(|| LightView {
            name: name.clone(),
            on: false,
        });
        view.name = name;
    }

    if registration.is_new {
        info!(%id, topics = ?config.state_topics, "discovered light");
    }

    // A topic shared by several channels is subscribed once, and one that
    // already has an owner is already subscribed.
    for topic in &registration.new_topics {
        ctx.mqtt_subscribe(topic, QoS::AtLeastOnce);
    }
    for topic in &registration.freed_topics {
        ctx.mqtt_unsubscribe(topic);
    }
    if let Some(topic) = &config.request_topic {
        request_state(ctx, topic);
    }
}

/// Drop entities from the UI and stop listening to state topics that lost
/// their last owner.
async fn forget_lights(ctx: &AppContext, removed: &[String], freed_topics: Vec<String>) {
    if removed.is_empty() {
        return;
    }

    {
        let mut state = ctx.state.write().await;
        for id in removed {
            state.lights.remove(id);
        }
    }

    for topic in freed_topics {
        ctx.mqtt_unsubscribe(&topic);
    }
}

/// Remove every entity announced by a retained discovery config that has been
/// cleared (empty payload), the standard "device deleted" signal.
async fn remove_discovery(ctx: &Arc<AppContext>, discovery_topic: &str) {
    let (removed, freed_topics) = {
        let mut reg = ctx.lights.write().await;
        reg.remove_by_discovery_topic(discovery_topic)
    };

    if removed.is_empty() {
        debug!(%discovery_topic, "removal for unknown discovery topic");
        return;
    }

    forget_lights(ctx, &removed, freed_topics).await;
    info!(%discovery_topic, ids = ?removed, "removed light(s)");
}

/// Publish an empty payload to ask a device for its current state. OpenBeken
/// uses the channel's `/get` topic for this, Tasmota uses `cmnd/<topic>/STATE`.
/// Each topic is asked at most once per MQTT connection; the mark is cleared
/// on reconnect, and publishing failures are logged by the request worker.
fn request_state(ctx: &AppContext, topic: &str) {
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

async fn handle_light_state(ctx: &Arc<AppContext>, topic: &str, payload: &[u8]) {
    let configs: Vec<(String, LightConfig)> = {
        let reg = ctx.lights.read().await;
        reg.ids_for_topic(topic)
            .into_iter()
            .filter_map(|id| reg.by_id.get(&id).map(|cfg| (id, cfg.clone())))
            .collect()
    };

    if configs.is_empty() {
        return;
    }

    let raw = String::from_utf8_lossy(payload);
    let raw = raw.trim();
    if raw.is_empty() {
        // Our own empty state request echoed back by the broker.
        return;
    }

    // Parse once when any entity on this topic expects JSON (Tasmota).
    let json = if configs.iter().any(|(_, cfg)| cfg.state_key.is_some()) {
        match serde_json::from_str::<serde_json::Value>(raw) {
            Ok(json) => Some(json),
            Err(e) => {
                debug!(error = %e, topic, payload = raw, "light state is not JSON");
                return;
            }
        }
    } else {
        None
    };

    let mut state = ctx.state.write().await;
    for (id, cfg) in configs {
        // A multi-channel device reports all keys at once, so a payload that
        // doesn't mention this channel simply leaves it untouched.
        let Some(is_on) = cfg.state_is_on(raw, json.as_ref()) else {
            debug!(
                topic,
                key = ?cfg.state_key,
                payload = raw,
                "state payload does not apply to entity"
            );
            continue;
        };

        if let Some(light) = state.lights.get_mut(&id) {
            if light.on != is_on {
                info!(%id, on = is_on, "light state changed");
            }
            light.on = is_on;
        }
    }
}

async fn get_state(State(ctx): State<Arc<AppContext>>) -> Json<HomeState> {
    Json(ctx.state.read().await.clone())
}

async fn toggle_light(Path(id): Path<String>, State(ctx): State<Arc<AppContext>>) -> StatusCode {
    let cfg = ctx.lights.read().await.by_id.get(&id).cloned();
    let discovered = cfg.is_some();

    // Fall back to the legacy `lights/<id>/set` convention.
    let (topic, payload_on, payload_off) = match cfg {
        Some(cfg) => (cfg.command_topic, cfg.payload_on, cfg.payload_off),
        None => (
            format!("lights/{id}/set"),
            "ON".to_string(),
            "OFF".to_string(),
        ),
    };

    // Decide the target *and* record it under one write lock. Two rapid
    // requests therefore alternate ON/OFF instead of both reading the same
    // stale value and publishing the same command. The same lock rejects ids
    // the app has never seen, so a stale tab or typo can't create a ghost
    // switch in the UI.
    let flip = {
        let mut state = ctx.state.write().await;
        if discovered || state.lights.contains_key(&id) {
            let name = state
                .lights
                .get(&id)
                .map(|light| light.name.clone())
                .unwrap_or_else(|| id.clone());
            Some(flip_light(
                &mut state,
                &id,
                &name,
                &payload_on,
                &payload_off,
            ))
        } else {
            None
        }
    };
    let Some((optimistic_on, payload)) = flip else {
        debug!(%id, "ignoring toggle for unknown light");
        return StatusCode::NOT_FOUND;
    };

    // Safe to await here: HTTP handlers run outside the MQTT event-loop task,
    // so the loop is free to drain this request. Keeping it direct (rather
    // than queueing) is what lets us report a publish failure and undo the
    // optimistic flip.
    match ctx
        .mqtt
        .publish(&topic, QoS::AtLeastOnce, false, payload.clone())
        .await
    {
        Ok(_) => {
            debug!(%id, %payload, %topic, "sent light command");
            StatusCode::NO_CONTENT
        }
        Err(e) => {
            // The command never left, so undo the optimistic flip unless a
            // real state update arrived in the meantime.
            revert_flip(&mut *ctx.state.write().await, &id, optimistic_on);
            warn!(error = %e, %id, "failed to publish light command");
            StatusCode::BAD_GATEWAY
        }
    }
}

/// Flip a light inside a single mutable borrow and return the new state plus
/// the command payload. Keeping the read and the write together is what makes
/// concurrent toggles serialize instead of racing.
fn flip_light(
    state: &mut HomeState,
    id: &str,
    name: &str,
    payload_on: &str,
    payload_off: &str,
) -> (bool, String) {
    let light = state
        .lights
        .entry(id.to_string())
        .or_insert_with(|| LightView {
            name: name.to_string(),
            on: false,
        });
    light.on = !light.on;
    let payload = if light.on { payload_on } else { payload_off };
    (light.on, payload.to_string())
}

/// Undo an optimistic flip after a failed publish, but only while the light
/// still holds our value: a real state update may have superseded it.
fn revert_flip(state: &mut HomeState, id: &str, expected_on: bool) {
    if let Some(light) = state.lights.get_mut(id) {
        if light.on == expected_on {
            light.on = !expected_on;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(hex_payload: &str) -> Vec<BthomeReading> {
        decode_bthome(&hex::decode(hex_payload).unwrap()).unwrap()
    }

    #[test]
    fn decodes_temperature_humidity_packet() {
        // 00 pid, 01 battery 83, 02 temp 28.32, 03 humidity 40.16
        let readings = decode("400084015302100b03b00f");
        let (t, h) = bthome_temperature_humidity(&readings);
        assert!((t.unwrap() - 28.32).abs() < 0.001);
        assert!((h.unwrap() - 40.16).abs() < 0.001);
    }

    #[test]
    fn decodes_contact_packet() {
        // 00 pid, 0C voltage 2.867V, 10 power off, 11 opening open
        let readings = decode("4000810c330b10001101");
        let summary = summarize_bthome(&readings);
        assert!(summary.contains("voltage=2.867"), "{summary}");
        assert!(summary.contains("power_state=0.000"), "{summary}");
        assert!(summary.contains("opening=1.000"), "{summary}");
        let (t, h) = bthome_temperature_humidity(&readings);
        assert_eq!(t, None);
        assert_eq!(h, None);
    }

    #[test]
    fn rejects_encrypted_packet() {
        assert!(decode_bthome(&hex::decode("4100").unwrap()).is_none());
    }

    #[test]
    fn bthome_object_table_is_sorted_for_binary_search() {
        assert!(
            BTHOME_OBJECTS.windows(2).all(|pair| pair[0].id < pair[1].id),
            "BTHOME_OBJECTS must stay strictly ascending by id"
        );
    }

    #[test]
    fn keeps_first_of_duplicate_temperature_objects() {
        // 00 pid, 02 temp 28.32, 02 temp 20.00, 03 humidity 40.16
        let readings = decode("40008402100b02d00703b00f");
        let (t, h) = bthome_temperature_humidity(&readings);
        assert_eq!(t, Some(28.32));
        assert_eq!(h, Some(40.16));
    }

    #[test]
    fn two_sensors_keep_their_own_readings() {
        let mut state = HomeState::default();

        record_sensor(&mut state, "A4:C1:38:19:C6:B0", Some(27.5), Some(42.8), 100);
        record_sensor(&mut state, "AA:BB:CC:DD:EE:FF", Some(19.0), Some(55.0), 101);

        assert_eq!(state.sensors.len(), 2);
        assert_eq!(state.sensors["A4:C1:38:19:C6:B0"].temp_c, Some(27.5));
        assert_eq!(state.sensors["A4:C1:38:19:C6:B0"].humidity, Some(42.8));
        assert_eq!(state.sensors["AA:BB:CC:DD:EE:FF"].temp_c, Some(19.0));
        assert_eq!(state.sensors["AA:BB:CC:DD:EE:FF"].humidity, Some(55.0));
        // The single-value fields mirror the most recent report, not a blend.
        assert_eq!(state.temp_c, Some(19.0));
        assert_eq!(state.humidity, Some(55.0));
    }

    #[test]
    fn a_packet_without_temperature_keeps_the_previous_one() {
        let mut state = HomeState::default();

        record_sensor(&mut state, "sensor", Some(27.5), Some(42.8), 100);
        record_sensor(&mut state, "sensor", None, Some(43.1), 200);

        assert_eq!(state.sensors["sensor"].temp_c, Some(27.5));
        assert_eq!(state.sensors["sensor"].humidity, Some(43.1));
        assert_eq!(state.sensors["sensor"].last_seen, 200);
    }

    #[test]
    fn contact_only_packets_do_not_create_a_sensor() {
        let mut state = HomeState::default();

        record_sensor(&mut state, "sensor", None, None, 100);

        assert!(state.sensors.is_empty());
    }

    #[test]
    fn state_json_exposes_per_sensor_readings() {
        let mut state = HomeState::default();
        record_sensor(&mut state, "A4:C1:38:19:C6:B0", Some(27.5), Some(42.8), 100);

        let json = serde_json::to_string(&state).unwrap();

        assert!(
            json.contains(
                r#""sensors":{"A4:C1:38:19:C6:B0":{"temp_c":27.5,"humidity":42.8,"last_seen":100}}"#
            ),
            "{json}"
        );
    }

    #[test]
    fn light_json_order_is_stable() {
        let mut state = HomeState::default();
        for id in ["zulu", "alpha", "mike"] {
            state.lights.insert(
                id.to_string(),
                LightView {
                    name: id.to_string(),
                    on: false,
                },
            );
        }

        let json = serde_json::to_string(&state).unwrap();
        let alpha = json.find("\"alpha\"").unwrap();
        let mike = json.find("\"mike\"").unwrap();
        let zulu = json.find("\"zulu\"").unwrap();

        assert!(alpha < mike && mike < zulu, "{json}");
    }

    #[test]
    fn mqtt_hex_with_trailing_newline_decodes() {
        // A proxy that appends a newline must not be read as ASCII bytes.
        let bytes = bthome_bytes(b"400084015302100b03b00f\r\n");
        assert_eq!(bytes[0], 0x40);

        let readings = decode_bthome(&bytes).unwrap();
        let (t, h) = bthome_temperature_humidity(&readings);
        assert_eq!(t, Some(28.32));
        assert_eq!(h, Some(40.16));
    }

    #[test]
    fn raw_bthome_bytes_are_not_mistaken_for_hex_text() {
        let raw = [0x40, 0x00, 0xcb, 0x0c, 0x2b, 0x0b, 0x10, 0x00, 0x11, 0x01];
        assert_eq!(bthome_bytes(&raw), raw.to_vec());
    }

    #[test]
    fn derives_tasmota_entities_from_real_payload() {
        let payload = r#"{
            "ip":"192.168.100.16","dn":"Tasmota",
            "fn":["Tasmota",null,null,null,null,null,null,null],
            "hn":"tasmota-73673C-1852","mac":"48551973673C","md":"Sonoff Basic",
            "ty":0,"if":0,"ofln":"Offline","onln":"Online",
            "state":["OFF","ON","TOGGLE","HOLD"],"sw":"13.2.0",
            "t":"tasmota_73673C","ft":"%prefix%/%topic%/","tp":["cmnd","stat","tele"],
            "rl":[1,0,0,0,0,0,0,0]
        }"#;

        let cfg: TasmotaDiscovery = serde_json::from_str(payload).unwrap();
        let entities = tasmota_entities("48551973673C", &cfg);

        assert_eq!(entities.len(), 1);
        let entity = &entities[0];
        assert_eq!(entity.id, "tasmota_48551973673C_1");
        assert_eq!(entity.name, "Tasmota");
        assert_eq!(entity.kind, "switch");
        assert_eq!(entity.config.command_topic, "cmnd/tasmota_73673C/POWER");
        assert_eq!(
            entity.config.state_topics,
            vec!["stat/tasmota_73673C/RESULT", "tele/tasmota_73673C/STATE"]
        );
        assert_eq!(entity.config.payload_on, "ON");
        assert_eq!(entity.config.payload_off, "OFF");
        assert_eq!(entity.config.state_key.as_deref(), Some("POWER"));
        assert_eq!(
            entity.config.request_topic.as_deref(),
            Some("cmnd/tasmota_73673C/STATE")
        );
    }

    #[test]
    fn non_ascii_tasmota_mac_does_not_panic() {
        // Topics are arbitrary UTF-8. In `a😀bcdef` the byte at `len - 6`
        // falls inside the emoji, so byte slicing used to panic and take the
        // MQTT task down with it.
        let payload = r#"{
            "dn":"Weird","fn":[],"t":"dev","ft":"%prefix%/%topic%/%id%/",
            "tp":["cmnd","stat","tele"],"rl":[1]
        }"#;

        let cfg: TasmotaDiscovery = serde_json::from_str(payload).unwrap();
        let entities = tasmota_entities("a😀bcdef", &cfg);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].config.command_topic, "cmnd/dev/😀bcdef/POWER");
    }

    #[test]
    fn openbeken_discovery_requests_state_via_its_get_topic() {
        // Real retained config from the OpenBeken relay: `stat_t` is `~/1/get`.
        let payload = r#"{
            "dev":{"ids":["OpenBL602_4CA5EA8B"],"name":"obl4CA5EA8B","sw":"1.18.258"},
            "name":"1","~":"obl4CA5EA8B","pl_on":"1","pl_off":"0",
            "uniq_id":"OpenBL602_4CA5EA8B_relay_1","qos":1,
            "stat_t":"~/1/get","cmd_t":"~/1/set"
        }"#;

        let cfg: DiscoveryConfig = serde_json::from_str(payload).unwrap();
        let entity = ha_entity(&cfg).unwrap();

        assert_eq!(entity.id, "OpenBL602_4CA5EA8B_relay_1");
        assert_eq!(entity.config.state_topics, vec!["obl4CA5EA8B/1/get"]);
        assert_eq!(entity.config.command_topic, "obl4CA5EA8B/1/set");
        assert_eq!(
            entity.config.request_topic.as_deref(),
            Some("obl4CA5EA8B/1/get")
        );
        assert_eq!(entity.config.payload_on, "1");
        assert_eq!(entity.config.payload_off, "0");
    }

    #[test]
    fn generic_discovery_never_requests_state_on_stat_t() {
        for stat_t in [
            "esphome/desk/light/state",
            "zigbee2mqtt/desk",
            "home/kitchen/relay/state",
        ] {
            let payload = format!(
                r#"{{"name":"Desk","uniq_id":"desk",
                     "stat_t":"{stat_t}","cmd_t":"some/command"}}"#
            );
            let cfg: DiscoveryConfig = serde_json::from_str(&payload).unwrap();
            let entity = ha_entity(&cfg).unwrap();

            assert_eq!(entity.config.state_topics, vec![stat_t]);
            assert_eq!(
                entity.config.request_topic, None,
                "must not publish to stat_t {stat_t}"
            );
        }
    }

    fn light_config(state_topics: &[&str], state_key: Option<&str>) -> LightConfig {
        LightConfig {
            state_topics: state_topics.iter().map(|t| t.to_string()).collect(),
            command_topic: "cmnd/test/POWER1".to_string(),
            request_topic: None,
            payload_on: "ON".to_string(),
            payload_off: "OFF".to_string(),
            state_key: state_key.map(str::to_string),
        }
    }

    #[test]
    fn registry_keeps_all_entities_sharing_a_state_topic() {
        let mut reg = LightRegistry::default();
        reg.insert(
            "tasmota_MAC_1".to_string(),
            light_config(&["stat/t/RESULT"], Some("POWER1")),
            "tasmota/discovery/MAC/config",
        );
        reg.insert(
            "tasmota_MAC_2".to_string(),
            light_config(&["stat/t/RESULT"], Some("POWER2")),
            "tasmota/discovery/MAC/config",
        );

        let mut ids = reg.ids_for_topic("stat/t/RESULT");
        ids.sort();
        assert_eq!(ids, vec!["tasmota_MAC_1", "tasmota_MAC_2"]);
    }

    #[test]
    fn only_the_first_owner_of_a_topic_needs_a_subscribe() {
        let mut reg = LightRegistry::default();

        let first = reg.insert(
            "tasmota_MAC_1".to_string(),
            light_config(&["stat/t/RESULT", "tele/t/STATE"], Some("POWER1")),
            "tasmota/discovery/MAC/config",
        );
        assert!(first.is_new);
        assert_eq!(first.new_topics, vec!["stat/t/RESULT", "tele/t/STATE"]);

        let second = reg.insert(
            "tasmota_MAC_2".to_string(),
            light_config(&["stat/t/RESULT", "tele/t/STATE"], Some("POWER2")),
            "tasmota/discovery/MAC/config",
        );
        assert!(second.is_new);
        assert!(second.new_topics.is_empty(), "topics already subscribed");

        // Re-announcing an existing entity subscribes nothing either.
        let again = reg.insert(
            "tasmota_MAC_2".to_string(),
            light_config(&["stat/t/RESULT", "tele/t/STATE"], Some("POWER2")),
            "tasmota/discovery/MAC/config",
        );
        assert!(!again.is_new);
        assert!(again.new_topics.is_empty());
    }

    #[test]
    fn changing_a_state_topic_frees_the_old_one() {
        let mut reg = LightRegistry::default();
        reg.insert(
            "lamp".to_string(),
            light_config(&["stat/old"], None),
            "homeassistant/switch/lamp/config",
        );

        let change = reg.insert(
            "lamp".to_string(),
            light_config(&["stat/new"], None),
            "homeassistant/switch/lamp/config",
        );

        assert_eq!(change.freed_topics, vec!["stat/old"]);
        assert_eq!(change.new_topics, vec!["stat/new"]);
        assert!(reg.ids_for_topic("stat/old").is_empty());
    }

    #[test]
    fn shrinking_a_tasmota_config_evicts_vanished_channels() {
        let mut reg = LightRegistry::default();
        let topic = "tasmota/discovery/MAC/config";
        for n in 1..=2 {
            let key = format!("POWER{n}");
            reg.insert(
                format!("tasmota_MAC_{n}"),
                light_config(&["stat/t/RESULT"], Some(key.as_str())),
                topic,
            );
        }

        let keep = HashSet::from(["tasmota_MAC_1".to_string()]);
        let (removed, freed_topics) = reg.retain_discovery_topic(topic, &keep);

        assert_eq!(removed, vec!["tasmota_MAC_2"]);
        assert!(freed_topics.is_empty(), "channel 1 still owns the topic");
        assert_eq!(reg.ids_for_topic("stat/t/RESULT"), vec!["tasmota_MAC_1"]);
        assert!(!reg.by_id.contains_key("tasmota_MAC_2"));
    }

    #[test]
    fn dropping_the_last_channel_frees_the_shared_topic() {
        let mut reg = LightRegistry::default();
        let topic = "tasmota/discovery/MAC/config";
        for n in 1..=2 {
            let key = format!("POWER{n}");
            reg.insert(
                format!("tasmota_MAC_{n}"),
                light_config(&["stat/t/RESULT"], Some(key.as_str())),
                topic,
            );
        }

        let (mut removed, freed_topics) = reg.retain_discovery_topic(topic, &HashSet::new());

        removed.sort();
        assert_eq!(removed, vec!["tasmota_MAC_1", "tasmota_MAC_2"]);
        assert_eq!(freed_topics, vec!["stat/t/RESULT"]);
        assert!(reg.by_state_topic.is_empty());
    }

    #[test]
    fn re_registering_one_entity_keeps_shared_topic_index() {
        let mut reg = LightRegistry::default();
        reg.insert(
            "tasmota_MAC_1".to_string(),
            light_config(&["stat/t/RESULT"], Some("POWER1")),
            "tasmota/discovery/MAC/config",
        );
        reg.insert(
            "tasmota_MAC_2".to_string(),
            light_config(&["stat/t/RESULT"], Some("POWER2")),
            "tasmota/discovery/MAC/config",
        );

        // Channel 1 re-announces with a different topic; channel 2 must keep
        // the shared one and channel 1's old entry must be cleaned up.
        reg.insert(
            "tasmota_MAC_1".to_string(),
            light_config(&["stat/t/OTHER"], Some("POWER1")),
            "tasmota/discovery/MAC/config",
        );

        assert_eq!(reg.ids_for_topic("stat/t/RESULT"), vec!["tasmota_MAC_2"]);
        assert_eq!(reg.ids_for_topic("stat/t/OTHER"), vec!["tasmota_MAC_1"]);
    }

    #[test]
    fn removing_discovery_topic_drops_all_of_its_channels() {
        let mut reg = LightRegistry::default();
        reg.insert(
            "tasmota_MAC_1".to_string(),
            light_config(&["stat/t/RESULT"], Some("POWER1")),
            "tasmota/discovery/MAC/config",
        );
        reg.insert(
            "tasmota_MAC_2".to_string(),
            light_config(&["stat/t/RESULT"], Some("POWER2")),
            "tasmota/discovery/MAC/config",
        );
        reg.insert(
            "other".to_string(),
            light_config(&["stat/other/state"], None),
            "homeassistant/switch/other/config",
        );

        let (mut removed, mut freed) =
            reg.remove_by_discovery_topic("tasmota/discovery/MAC/config");
        removed.sort();
        freed.sort();

        assert_eq!(removed, vec!["tasmota_MAC_1", "tasmota_MAC_2"]);
        assert_eq!(freed, vec!["stat/t/RESULT"]);
        assert!(reg.by_id.contains_key("other"));
        assert_eq!(reg.ids_for_topic("stat/other/state"), vec!["other"]);
        assert!(reg.ids_for_topic("stat/t/RESULT").is_empty());
    }

    #[test]
    fn removing_unknown_discovery_topic_is_a_no_op() {
        let mut reg = LightRegistry::default();
        reg.insert(
            "other".to_string(),
            light_config(&["stat/other/state"], None),
            "homeassistant/switch/other/config",
        );

        let (removed, freed) = reg.remove_by_discovery_topic("homeassistant/switch/gone/config");
        assert!(removed.is_empty());
        assert!(freed.is_empty());
        assert_eq!(reg.by_id.len(), 1);

        // The still-announced entity is untouched.
        let (removed, _) = reg.remove_by_discovery_topic("homeassistant/switch/other/config");
        assert_eq!(removed, vec!["other"]);
        assert!(reg.by_id.is_empty());
    }

    #[test]
    fn multi_channel_tasmota_entities_share_topics_with_distinct_keys() {
        let payload = r#"{
            "dn":"Tasmota","fn":["Left","Right",null,null],
            "mac":"AABBCCDDEEFF","state":["OFF","ON","TOGGLE","HOLD"],
            "t":"tasmota_test","ft":"%prefix%/%topic%/","tp":["cmnd","stat","tele"],
            "rl":[1,1,0,0]
        }"#;

        let cfg: TasmotaDiscovery = serde_json::from_str(payload).unwrap();
        let entities = tasmota_entities("AABBCCDDEEFF", &cfg);
        assert_eq!(entities.len(), 2);

        assert_eq!(entities[0].name, "Left");
        assert_eq!(entities[0].config.command_topic, "cmnd/tasmota_test/POWER1");
        assert_eq!(entities[0].config.state_key.as_deref(), Some("POWER1"));
        assert_eq!(entities[1].name, "Right");
        assert_eq!(entities[1].config.command_topic, "cmnd/tasmota_test/POWER2");
        assert_eq!(entities[1].config.state_key.as_deref(), Some("POWER2"));
        assert_eq!(
            entities[0].config.state_topics,
            entities[1].config.state_topics
        );

        // Both channels of one RESULT/STATE payload must be updated.
        let json: serde_json::Value =
            serde_json::from_str(r#"{"POWER1":"ON","POWER2":"OFF"}"#).unwrap();
        for entity in &entities {
            let json = Some(&json);
            let on = entity
                .config
                .state_is_on(r#"{"POWER1":"ON","POWER2":"OFF"}"#, json)
                .unwrap();
            assert_eq!(on, entity.config.state_key.as_deref() == Some("POWER1"));
        }
    }

    #[test]
    fn rapid_toggles_alternate_payloads() {
        let mut state = HomeState::default();

        assert_eq!(
            flip_light(&mut state, "lamp", "lamp", "ON", "OFF"),
            (true, "ON".to_string())
        );
        assert_eq!(
            flip_light(&mut state, "lamp", "lamp", "ON", "OFF"),
            (false, "OFF".to_string())
        );
        assert_eq!(
            flip_light(&mut state, "lamp", "lamp", "1", "0"),
            (true, "1".to_string())
        );
        assert!(state.lights["lamp"].on);
    }

    #[test]
    fn failed_publish_reverts_optimistic_flip() {
        let mut state = HomeState::default();

        let (optimistic_on, _) = flip_light(&mut state, "lamp", "lamp", "ON", "OFF");
        assert!(optimistic_on);
        revert_flip(&mut state, "lamp", optimistic_on);

        assert!(!state.lights["lamp"].on);
    }

    #[test]
    fn a_new_light_view_uses_the_assigned_name() {
        let mut state = HomeState::default();

        flip_light(&mut state, "tasmota_x_1", "Tasmota Left", "ON", "OFF");

        assert_eq!(state.lights["tasmota_x_1"].name, "Tasmota Left");
    }

    #[test]
    fn revert_does_not_clobber_a_newer_state_update() {
        let mut state = HomeState::default();

        let (optimistic_on, _) = flip_light(&mut state, "lamp", "lamp", "ON", "OFF");
        assert!(optimistic_on);

        // The device reported OFF between our flip and the failed publish.
        state.lights.get_mut("lamp").unwrap().on = false;
        revert_flip(&mut state, "lamp", optimistic_on);

        assert!(!state.lights["lamp"].on, "authoritative state must win");
    }
}
