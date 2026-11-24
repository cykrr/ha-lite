//! Shared application state: the values every handler reads and writes.
//!
//! `HomeState` is what `/api/state` serves; `AppContext` is the handle the BLE,
//! MQTT and HTTP sides share. Keeping them in one module makes the state a
//! handler may touch visible in a single place.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use tokio::{
    sync::{Notify, RwLock},
    task::{AbortHandle, JoinHandle},
};
use tracing::error;

use crate::lights::LightRegistry;
use crate::mqtt::MqttLink;

#[cfg(test)]
use tokio::sync::mpsc;

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
    pub(crate) mqtt: MqttLink,
    pub(crate) shutdown: Shutdown,
}

/// A way for background tasks to ask `main` to shut down cleanly and exit with
/// a failure status, so the service manager restarts the process. Going through
/// `main` (instead of `process::exit`) lets the BLE scans be stopped first.
#[derive(Default)]
pub(crate) struct Shutdown {
    notify: Notify,
    failed: AtomicBool,
}

impl Shutdown {
    /// Ask for a shutdown because something is broken. Safe to call from any
    /// task and more than once.
    pub(crate) fn fail(&self) {
        self.failed.store(true, Ordering::Relaxed);
        // Stores a permit when nobody is waiting yet, so a request made before
        // `requested` is polled is not lost.
        self.notify.notify_one();
    }

    /// Whether the shutdown was requested through `fail`.
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    /// Resolves once `fail` has been called.
    pub(crate) async fn requested(&self) {
        self.notify.notified().await;
    }
}

/// Watch a task that is meant to run for the life of the process. If it panics
/// or returns, the process would otherwise keep serving HTTP with that part
/// dead, so request a shutdown instead. Aborting the task is not a failure.
///
/// Returns a handle to abort the task and the monitor to await, which finishes
/// once the task is gone.
pub(crate) fn supervise(
    name: &'static str,
    task: JoinHandle<()>,
    ctx: Arc<AppContext>,
) -> (AbortHandle, JoinHandle<()>) {
    let abort = task.abort_handle();
    let monitor = tokio::spawn(async move {
        match task.await {
            Err(e) if e.is_cancelled() => {}
            Err(e) => {
                error!(task = name, error = %e, "task panicked; shutting down so the service manager restarts it");
                ctx.shutdown.fail();
            }
            Ok(()) => {
                error!(task = name, "task ended unexpectedly; shutting down so the service manager restarts it");
                ctx.shutdown.fail();
            }
        }
    });
    (abort, monitor)
}

/// Most distinct sensor sources kept in `HomeState::sensors`.
const MAX_SENSORS: usize = 64;

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

    // Anything in radio range can advertise BTHome, so cap the map: once it is
    // full, a new source displaces the least recently seen one. Real sensors
    // re-announce every few seconds and reappear on their next packet.
    if state.sensors.len() >= MAX_SENSORS && !state.sensors.contains_key(source) {
        let stalest = state
            .sensors
            .iter()
            .min_by_key(|(_, sensor)| sensor.last_seen)
            .map(|(key, _)| key.clone());
        if let Some(key) = stalest {
            state.sensors.remove(&key);
        }
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
    mpsc::UnboundedReceiver<crate::mqtt::MqttCommand>,
) {
    let (client, eventloop) =
        rumqttc::AsyncClient::new(rumqttc::MqttOptions::new("test", "127.0.0.1", 1), capacity);
    let (mqtt, request_rx) = MqttLink::new(client);
    mqtt.set_connected(connected);
    let ctx = Arc::new(AppContext {
        state: RwLock::new(HomeState::default()),
        lights: RwLock::new(LightRegistry::default()),
        bthome_packets: AtomicU64::new(0),
        mqtt,
        shutdown: Shutdown::default(),
    });
    (ctx, eventloop, request_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_shutdown_requested_before_anyone_waits_is_not_lost() {
        let shutdown = Shutdown::default();
        assert!(!shutdown.failed());

        shutdown.fail();

        assert!(shutdown.failed());
        tokio::time::timeout(std::time::Duration::from_secs(1), shutdown.requested())
            .await
            .expect("the stored request resolves immediately");
    }

    #[tokio::test]
    async fn a_panicking_task_requests_a_shutdown() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);
        let task = tokio::spawn(async { panic!("boom") });

        let (_abort, monitor) = supervise("test task", task, ctx.clone());
        monitor.await.unwrap();

        assert!(ctx.shutdown.failed());
    }

    #[tokio::test]
    async fn aborting_a_supervised_task_is_not_a_failure() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);
        let task = tokio::spawn(std::future::pending::<()>());

        let (abort, monitor) = supervise("test task", task, ctx.clone());
        abort.abort();
        monitor.await.unwrap();

        assert!(!ctx.shutdown.failed());
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
    fn sensor_map_is_capped_and_drops_the_stalest_source() {
        let mut state = HomeState::default();

        for i in 0..MAX_SENSORS {
            record_sensor(&mut state, &format!("s{i}"), Some(20.0), None, 100 + i as u64);
        }
        // Refresh the oldest entry, so the next-oldest becomes the stalest.
        record_sensor(&mut state, "s0", Some(21.0), None, 1000);
        record_sensor(&mut state, "new", Some(22.0), None, 1001);

        assert_eq!(state.sensors.len(), MAX_SENSORS);
        assert!(state.sensors.contains_key("s0"));
        assert!(state.sensors.contains_key("new"));
        assert!(!state.sensors.contains_key("s1"));
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
