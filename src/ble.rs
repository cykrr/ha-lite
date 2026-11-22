//! Local Bluetooth scanning for BTHome advertisements.
//!
//! BLE hardware is unreliable, so this is a supervisor around a one-shot
//! session: adapters are re-enumerated and scanning re-armed with backoff.

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

use crate::app::AppContext;
use crate::bthome::{bthome_temperature_humidity, decode_bthome, record_bthome, summarize_bthome};

/// Stop discovery on every adapter on the way out. BlueZ remembers an active
/// scan per client, so a clean exit avoids leaving a stale scan behind for the
/// next start. Best effort: this runs while shutting down, so errors are
/// ignored (a stopped or vanished adapter is fine).
pub(crate) async fn stop_ble_scans() {
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
pub(crate) async fn ble_scanner(ctx: Arc<AppContext>) {
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

            ctx.bthome_packets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
