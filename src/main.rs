//! ha-lite: a small Home Assistant MQTT bridge.
//!
//! Reads BTHome broadcasts (over MQTT or a local Bluetooth adapter), tracks
//! discovered lights/switches, and serves both as JSON on `/api/state`.

mod app;
mod ble;
mod bthome;
mod discovery;
mod http;
mod lights;
mod mqtt;

use std::{
    process::ExitCode,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    response::Html,
    routing::{get, post},
    Router,
};
use rumqttc::MqttOptions;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use app::{AppContext, HomeState, Shutdown};
use lights::LightRegistry;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// How long a shutdown requested by a failing task may take before the process
/// is ended anyway. Graceful HTTP shutdown waits for open connections, and a
/// browser holding a keep-alive connection could stall it indefinitely.
const FAILURE_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Resolve when the process should shut down: on a termination signal, or when
/// a task that must stay alive (MQTT, BLE) has died.
///
/// A task failure ends with a non-zero exit so `Restart=on-failure` brings the
/// service back, but only after `main` has stopped the BLE scans. If that
/// cleanup stalls, the process is ended anyway after a grace period.
async fn shutdown_signal(ctx: Arc<AppContext>) {
    tokio::select! {
        _ = termination_signal() => {}
        _ = ctx.shutdown.requested() => {
            tokio::spawn(async {
                tokio::time::sleep(FAILURE_SHUTDOWN_GRACE).await;
                error!("graceful shutdown did not finish in time; exiting");
                std::process::exit(1);
            });
        }
    }
}

/// Wait for a termination signal. Service managers and containers stop a
/// process with SIGTERM, which `ctrl_c()` alone does not catch, and the default
/// SIGTERM action ends the process before any cleanup can run. Where the
/// platform has it, race SIGTERM against Ctrl-C so both paths shut down
/// gracefully.
async fn termination_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            // Keep the other signal (if any) alive rather than resolving early.
            warn!(error = %e, "failed to listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let terminate = async {
            match signal(SignalKind::terminate()) {
                Ok(mut sig) => {
                    sig.recv().await;
                }
                Err(e) => {
                    warn!(error = %e, "failed to listen for SIGTERM");
                    std::future::pending::<()>().await;
                }
            }
        };

        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}

#[tokio::main]
async fn main() -> ExitCode {
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
    let client_id = mqtt::client_id();

    info!(
        %host,
        port,
        %client_id,
        authenticated = !user.is_empty(),
        "connecting to MQTT broker"
    );

    let mut mqttoptions = MqttOptions::new(client_id, &host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(15));
    if !user.is_empty() {
        mqttoptions.set_credentials(user, password);
    }

    let (mqtt, eventloop) = mqtt::connect(mqttoptions, 32);
    let ctx = Arc::new(AppContext {
        state: RwLock::new(HomeState::default()),
        lights: RwLock::new(LightRegistry::default()),
        bthome_packets: AtomicU64::new(0),
        mqtt,
        shutdown: Shutdown::default(),
    });

    // An empty BTHome feed should be loud, not silent.
    info!("waiting for BTHome broadcasts (MQTT sensors/# and local BLE)");
    let watch_ctx = ctx.clone();
    tokio::spawn(async move {
        // A box with only lights never gets a broadcast, so back the warning
        // off (1m, 2m, 4m ... capped at 1h) instead of nagging every minute.
        let mut waited_secs = 0u64;
        let mut interval_secs = 60u64;
        loop {
            tokio::time::sleep(Duration::from_secs(interval_secs)).await;
            if watch_ctx.bthome_packets.load(Ordering::Relaxed) > 0 {
                break;
            }
            waited_secs += interval_secs;
            warn!(
                waited_secs,
                "no BTHome broadcasts received yet (MQTT sensors/# or local BLE)"
            );
            interval_secs = (interval_secs * 2).min(3600);
        }
    });

    // Scan BLE directly for BTHome service data (UUID 0xFCD2).
    // Both background tasks are supervised: if one panics, the process would
    // otherwise keep answering HTTP with that part dead, so a panic requests a
    // shutdown instead. Only these tasks are covered; a panic while serving one
    // HTTP request still only drops that connection.
    let (ble_abort, ble_monitor) = app::supervise(
        "BLE scanner",
        tokio::spawn(ble::ble_scanner(ctx.clone())),
        ctx.clone(),
    );

    // Background MQTT listener with automatic reconnect.
    app::supervise(
        "MQTT event loop",
        tokio::spawn(mqtt::run_event_loop(ctx.clone(), eventloop)),
        ctx.clone(),
    );

    // The listener can also hang without panicking; catch that too, rather
    // than serving 502s forever from a half-dead process.
    tokio::spawn(mqtt::watch_event_loop(ctx.clone()));

    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/api/state", get(http::get_state))
        .route("/api/lights/:id/toggle", post(http::toggle_light))
        .with_state(ctx.clone());

    // Bind on all interfaces by default; set HA_LITE_HOST=127.0.0.1 to sit
    // behind a reverse proxy, or HA_LITE_PORT to run a second instance.
    let listen_host = env_or("HA_LITE_HOST", "0.0.0.0");
    let listen_port: u16 = env_or("HA_LITE_PORT", "8082").parse().unwrap_or(8082);
    let bind_addr = format!("{listen_host}:{listen_port}");

    let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!(error = %e, %bind_addr, "failed to bind HTTP listener; exiting");
            return ExitCode::FAILURE;
        }
    };
    info!(%bind_addr, "HTTP server listening");
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(ctx.clone()))
        .await
    {
        error!(error = %e, "HTTP server failed");
    }

    // Stop the supervisor before clearing scans, otherwise a restart could
    // re-arm discovery in the window between the two. Awaiting the monitor
    // guarantees the task is gone and no longer holds an adapter.
    ble_abort.abort();
    let _ = ble_monitor.await;
    ble::stop_ble_scans().await;
    info!("shutting down");

    // A task failure must not look like a clean stop, or
    // `Restart=on-failure` would leave the service down.
    if ctx.shutdown.failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
