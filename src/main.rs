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
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

#[derive(Clone, Serialize)]
struct LightView {
    name: String,
    on: bool,
}

#[derive(Default, Clone, Serialize)]
struct HomeState {
    temp_c: Option<f32>,
    humidity: Option<f32>,
    lights: HashMap<String, LightView>,
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

impl LightRegistry {
    /// Insert or replace an entity announced on `discovery_topic`; returns
    /// `true` when it is new.
    fn insert(&mut self, id: String, config: LightConfig, discovery_topic: &str) -> bool {
        let previous_topics = self.by_id.get(&id).map(|c| c.state_topics.clone());
        if let Some(previous_topics) = previous_topics {
            for topic in previous_topics {
                if !config.state_topics.contains(&topic) {
                    unindex(&mut self.by_state_topic, &topic, &id);
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

        self.by_id.insert(id, config).is_none()
    }

    /// Remove every entity announced on this discovery topic. Returns the
    /// removed ids and any state topics that no longer have an owner.
    fn remove_by_discovery_topic(&mut self, discovery_topic: &str) -> (Vec<String>, Vec<String>) {
        let Some(ids) = self.by_discovery_topic.remove(discovery_topic) else {
            return (Vec::new(), Vec::new());
        };

        let mut removed = Vec::new();
        let mut freed_topics = Vec::new();
        for id in ids {
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
    mqtt: AsyncClient,
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
        let Some(spec) = BTHOME_OBJECTS.iter().find(|o| o.id == id) else {
            debug!(object_id = %format!("0x{id:02x}"), offset = i, "unknown BTHome object; stopping");
            break;
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

fn bthome_temperature_humidity(readings: &[BthomeReading]) -> (Option<f32>, Option<f32>) {
    let (mut temp, mut humidity) = (None, None);
    for reading in readings {
        match reading.name {
            "temperature" if temp.is_none() => temp = Some(reading.value as f32),
            "humidity" if humidity.is_none() => humidity = Some(reading.value as f32),
            _ => {}
        }
    }
    (temp, humidity)
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

/// Scan the local Bluetooth adapter for BTHome advertisements and feed the
/// same state the MQTT sensor topic feeds. BTHome v2 service data uses the
/// 0xFCD2 UUID and starts with a 0x40-tagged byte, which `parse_bthome_v2`
/// already understands.
async fn ble_scanner(ctx: Arc<AppContext>) {
    use btleplug::api::bleuuid::uuid_from_u16;
    use btleplug::api::{Central, CentralEvent, Manager as _, Peripheral as _, ScanFilter};
    use btleplug::platform::Manager;

    let bthome_uuid = uuid_from_u16(0xFCD2);

    let manager = match Manager::new().await {
        Ok(manager) => manager,
        Err(e) => {
            warn!(error = %e, "BLE unavailable; BLE scanning disabled");
            return;
        }
    };

    let adapters = match manager.adapters().await {
        Ok(adapters) => adapters,
        Err(e) => {
            warn!(error = %e, "could not list BLE adapters");
            return;
        }
    };

    if adapters.is_empty() {
        warn!("no BLE adapters found; BLE scanning disabled");
        return;
    }

    for adapter in adapters {
        if let Err(e) = adapter.start_scan(ScanFilter::default()).await {
            warn!(error = %e, "failed to start BLE scan");
            continue;
        }

        let events = match adapter.events().await {
            Ok(events) => events,
            Err(e) => {
                warn!(error = %e, "failed to open BLE event stream");
                continue;
            }
        };

        info!("BLE scan started");

        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut events = events;
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

                    if t.is_some() || h.is_some() {
                        let mut state = ctx.state.write().await;
                        if t.is_some() { state.temp_c = t; }
                        if h.is_some() { state.humidity = h; }
                    }
                }
            }
        });
    }
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
    let ctx = Arc::new(AppContext {
        state: RwLock::new(HomeState::default()),
        lights: RwLock::new(LightRegistry::default()),
        bthome_packets: AtomicU64::new(0),
        requested_state: Mutex::new(HashSet::new()),
        mqtt: client.clone(),
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
                        .expect("state request set poisoned")
                        .clear();
                    for (topic, qos) in [
                        ("sensors/#", QoS::AtMostOnce),
                        ("lights/+/state", QoS::AtLeastOnce),
                        // Discovery topics can be `component/object/config` or
                        // `component/node/object/config`, so listen to the whole tree.
                        ("homeassistant/#", QoS::AtLeastOnce),
                        ("tasmota/discovery/#", QoS::AtLeastOnce),
                    ] {
                        if let Err(e) = bg_ctx.mqtt.subscribe(topic, qos).await {
                            warn!(error = %e, topic, "failed to subscribe");
                        }
                    }

                    // Re-arm state subscriptions and ask each known device for its
                    // current value, since most of them only publish on change.
                    // Retained discovery replay calls this again; the request set
                    // makes that a no-op instead of a duplicate round trip.
                    let configs: Vec<LightConfig> = {
                        let reg = bg_ctx.lights.read().await;
                        reg.by_id.values().cloned().collect()
                    };
                    for cfg in &configs {
                        for topic in &cfg.state_topics {
                            if let Err(e) = bg_ctx.mqtt.subscribe(topic, QoS::AtLeastOnce).await {
                                warn!(error = %e, %topic, "failed to subscribe to light state");
                            }
                        }
                        if let Some(topic) = &cfg.request_topic {
                            request_state(&bg_ctx, topic).await;
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
                            let bytes = hex::decode(&msg.payload)
                                .unwrap_or_else(|_| msg.payload.to_vec());

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
                                        let mut state = bg_ctx.state.write().await;
                                        if t.is_some() { state.temp_c = t; }
                                        if h.is_some() { state.humidity = h; }
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

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8082").await.unwrap();
    info!("HTTP server listening on 0.0.0.0:8082");
    axum::serve(listener, app).await.unwrap();
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

    register_light(ctx, entity.id, entity.name, entity.config, discovery_topic).await;
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
    if entities.is_empty() {
        debug!(%mac, %device_topic, "Tasmota device has no relay/light channels");
        return;
    }

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

    let short_id = &mac[mac.len().saturating_sub(6)..];
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
    let is_new = {
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

    if is_new {
        info!(%id, topics = ?config.state_topics, "discovered light");
    }

    for topic in &config.state_topics {
        if let Err(e) = ctx.mqtt.subscribe(topic, QoS::AtLeastOnce).await {
            warn!(error = %e, %topic, "failed to subscribe to light state");
        }
    }
    if let Some(topic) = &config.request_topic {
        request_state(ctx, topic).await;
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

    {
        let mut state = ctx.state.write().await;
        for id in &removed {
            state.lights.remove(id);
        }
    }

    for topic in freed_topics {
        if let Err(e) = ctx.mqtt.unsubscribe(&topic).await {
            warn!(error = %e, %topic, "failed to unsubscribe from removed state topic");
        }
    }

    info!(%discovery_topic, ids = ?removed, "removed light(s)");
}

/// Publish an empty payload to ask a device for its current state. OpenBeken
/// uses the channel's `/get` topic for this, Tasmota uses `cmnd/<topic>/STATE`.
/// Each topic is asked at most once per MQTT connection; a failed publish clears
/// the mark so a later call can retry.
async fn request_state(ctx: &Arc<AppContext>, topic: &str) {
    let first_request = ctx
        .requested_state
        .lock()
        .expect("state request set poisoned")
        .insert(topic.to_string());
    if !first_request {
        return;
    }

    if let Err(e) = ctx
        .mqtt
        .publish(topic, QoS::AtLeastOnce, false, Vec::<u8>::new())
        .await
    {
        warn!(error = %e, %topic, "failed to request light state");
        ctx.requested_state
            .lock()
            .expect("state request set poisoned")
            .remove(topic);
    }
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
            flip_light(&mut state, "lamp", "ON", "OFF"),
            (true, "ON".to_string())
        );
        assert_eq!(
            flip_light(&mut state, "lamp", "ON", "OFF"),
            (false, "OFF".to_string())
        );
        assert_eq!(
            flip_light(&mut state, "lamp", "1", "0"),
            (true, "1".to_string())
        );
        assert!(state.lights["lamp"].on);
    }

    #[test]
    fn failed_publish_reverts_optimistic_flip() {
        let mut state = HomeState::default();

        let (optimistic_on, _) = flip_light(&mut state, "lamp", "ON", "OFF");
        assert!(optimistic_on);
        revert_flip(&mut state, "lamp", optimistic_on);

        assert!(!state.lights["lamp"].on);
    }

    #[test]
    fn revert_does_not_clobber_a_newer_state_update() {
        let mut state = HomeState::default();

        let (optimistic_on, _) = flip_light(&mut state, "lamp", "ON", "OFF");
        assert!(optimistic_on);

        // The device reported OFF between our flip and the failed publish.
        state.lights.get_mut("lamp").unwrap().on = false;
        revert_flip(&mut state, "lamp", optimistic_on);

        assert!(!state.lights["lamp"].on, "authoritative state must win");
    }
}

async fn get_state(State(ctx): State<Arc<AppContext>>) -> Json<HomeState> {
    Json(ctx.state.read().await.clone())
}

async fn toggle_light(Path(id): Path<String>, State(ctx): State<Arc<AppContext>>) -> StatusCode {
    let cfg = ctx.lights.read().await.by_id.get(&id).cloned();

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
    // stale value and publishing the same command.
    let (optimistic_on, payload) = {
        let mut state = ctx.state.write().await;
        flip_light(&mut state, &id, &payload_on, &payload_off)
    };

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
    payload_on: &str,
    payload_off: &str,
) -> (bool, String) {
    let light = state
        .lights
        .entry(id.to_string())
        .or_insert_with(|| LightView {
            name: id.to_string(),
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
