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

use app::{AppContext, HomeState};
use lights::LightRegistry;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Wait for a termination signal. Service managers and containers stop a
/// process with SIGTERM, which `ctrl_c()` alone does not catch, and the default
/// SIGTERM action ends the process before any cleanup can run. Where the
/// platform has it, race SIGTERM against Ctrl-C so both paths shut down
/// gracefully.
async fn shutdown_signal() {
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

    let (mqtt, eventloop) = mqtt::connect(mqttoptions, 32);
    let ctx = Arc::new(AppContext {
        state: RwLock::new(HomeState::default()),
        lights: RwLock::new(LightRegistry::default()),
        bthome_packets: AtomicU64::new(0),
        mqtt,
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
    let ble_handle = tokio::spawn(ble::ble_scanner(ctx.clone()));

    // Background MQTT listener with automatic reconnect.
    tokio::spawn(mqtt::run_event_loop(ctx.clone(), eventloop));

    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/api/state", get(http::get_state))
        .route("/api/lights/:id/toggle", post(http::toggle_light))
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
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!(error = %e, "HTTP server failed");
    }

    // Stop the supervisor before clearing scans, otherwise a restart could
    // re-arm discovery in the window between the two. Awaiting the aborted
    // handle guarantees the task is gone and no longer holds an adapter.
    ble_handle.abort();
    let _ = ble_handle.await;
    ble::stop_ble_scans().await;
    info!("shutting down");
}
