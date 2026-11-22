//! Shared application state: the values every handler reads and writes.
//!
//! `HomeState` is what `/api/state` serves; `AppContext` is the handle the BLE,
//! MQTT and HTTP sides share. Keeping them in one module makes the state a
//! handler may touch visible in a single place.

use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::sync::Arc;

use rumqttc::{AsyncClient, QoS};
use serde::Serialize;
use tokio::sync::{mpsc, RwLock};

use crate::lights::LightRegistry;

#[derive(Clone, Serialize)]
pub(crate) struct LightView {
    pub(crate) name: String,
    pub(crate) on: bool,
}

/// Latest reading from one BTHome sensor.
#[derive(Clone, Serialize)]
pub(crate) struct SensorView {
    pub(crate) temp_c: Option<f64>,
    pub(crate) humidity: Option<f64>,
    /// Unix seconds of the last packet that carried a value.
    pub(crate) last_seen: u64,
}

#[derive(Default, Clone, Serialize)]
pub(crate) struct HomeState {
    /// Keyed by BLE address or MQTT topic, so two sensors in range each keep
    /// their own readings instead of fighting over one slot.
    pub(crate) sensors: BTreeMap<String, SensorView>,
    /// Most recently reported value across all sensors, kept for consumers
    /// that just want a single number.
    pub(crate) temp_c: Option<f64>,
    pub(crate) humidity: Option<f64>,
    /// Sorted like `sensors`, so `/api/state` is stable between polls and the
    /// UI order does not jitter.
    pub(crate) lights: BTreeMap<String, LightView>,
}

pub(crate) struct AppContext {
    pub(crate) state: RwLock<HomeState>,
    pub(crate) lights: RwLock<LightRegistry>,
    pub(crate) bthome_packets: AtomicU64,
    /// State topics already asked for a snapshot on this MQTT connection, so
    /// the reconnect re-arm and the retained discovery replay don't both ask.
    pub(crate) requested_state: Mutex<HashSet<String>>,
    /// Whether the event loop currently has a live broker session. `publish`
    /// only reports a closed request channel, not a dead connection, so a
    /// toggle consults this flag instead of assuming success while the broker
    /// is unreachable.
    pub(crate) mqtt_connected: AtomicBool,
    /// Only touched by HTTP handlers, which run outside the MQTT event loop.
    /// It's used with [`AsyncClient::try_publish`] so a full request queue
    /// can't stall a request. Never touch this from inside `eventloop.poll()`;
    /// use the request queue instead.
    pub(crate) mqtt: AsyncClient,
    /// Outbound requests created while handling an MQTT event.
    ///
    /// `AsyncClient` feeds a bounded channel that only `EventLoop::poll` drains.
    /// Awaiting `subscribe`/`publish` from the task that is *running* `poll`
    /// blocks that task, so a burst larger than the channel capacity (32) would
    /// wedge the event loop forever. A reconnect re-arming enough lights is
    /// enough to do it. These commands are sent by a separate worker instead.
    pub(crate) mqtt_requests: mpsc::UnboundedSender<MqttCommand>,
}

/// One outbound MQTT request queued for the request worker.
pub(crate) enum MqttCommand {
    Subscribe(String, QoS),
    Unsubscribe(String),
    Publish(String, Vec<u8>, QoS, bool),
}

impl AppContext {
    pub(crate) fn mqtt_request(&self, request: MqttCommand) {
        // Only fails once the worker (and therefore the process) is going away.
        if self.mqtt_requests.send(request).is_err() {
            tracing::warn!("MQTT request worker stopped; dropping request");
        }
    }

    pub(crate) fn mqtt_subscribe(&self, topic: &str, qos: QoS) {
        self.mqtt_request(MqttCommand::Subscribe(topic.to_string(), qos));
    }

    pub(crate) fn mqtt_unsubscribe(&self, topic: &str) {
        self.mqtt_request(MqttCommand::Unsubscribe(topic.to_string()));
    }

    pub(crate) fn mqtt_publish(&self, topic: &str, payload: Vec<u8>, qos: QoS, retain: bool) {
        self.mqtt_request(MqttCommand::Publish(
            topic.to_string(),
            payload,
            qos,
            retain,
        ));
    }
}

/// Record one sensor update under its source identity (BLE address or MQTT
/// topic). Metrics missing from this packet keep their previous value.
pub(crate) fn record_sensor(
    state: &mut HomeState,
    source: &str,
    t: Option<f64>,
    h: Option<f64>,
    now: u64,
) {
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

pub(crate) fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// An `AppContext` whose MQTT client is connected to nothing, so a handler's
/// publish path can be exercised without a broker. The returned `EventLoop`
/// owns the client's request receiver: dropping it makes every publish fail
/// for the wrong reason, so tests keep it alive. The returned request receiver
/// lets tests observe queued subscribe/publish commands.
#[cfg(test)]
pub(crate) fn test_ctx(
    connected: bool,
    capacity: usize,
) -> (
    Arc<AppContext>,
    rumqttc::EventLoop,
    mpsc::UnboundedReceiver<MqttCommand>,
) {
    let (client, eventloop) =
        AsyncClient::new(rumqttc::MqttOptions::new("test", "127.0.0.1", 1), capacity);
    let (requests, request_rx) = mpsc::unbounded_channel();
    let ctx = Arc::new(AppContext {
        state: RwLock::new(HomeState::default()),
        lights: RwLock::new(LightRegistry::default()),
        bthome_packets: AtomicU64::new(0),
        requested_state: Mutex::new(HashSet::new()),
        mqtt_connected: AtomicBool::new(connected),
        mqtt: client,
        mqtt_requests: requests,
    });
    (ctx, eventloop, request_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
