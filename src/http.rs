//! HTTP surface: serve the UI, expose state, and accept light commands.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use tracing::{debug, warn};

use crate::app::AppContext;
use crate::lights::{flip_light, revert_flip};

/// A non-simple header that HTML forms cannot set. Requiring it makes a
/// cross-origin POST fail its CORS preflight, so a random web page on the LAN
/// cannot toggle lights just by submitting a form to this server.
const TOGGLE_HEADER: &str = "x-requested-with";
const TOGGLE_HEADER_VALUE: &str = "ha-lite";

pub(crate) async fn get_state(State(ctx): State<Arc<AppContext>>) -> Json<crate::app::HomeState> {
    Json(ctx.state.read().await.clone())
}

pub(crate) async fn toggle_light(
    Path(id): Path<String>,
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
) -> StatusCode {
    if headers
        .get(TOGGLE_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(TOGGLE_HEADER_VALUE)
    {
        debug!(%id, "rejecting toggle without the ha-lite request header");
        return StatusCode::FORBIDDEN;
    }

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

    // Decide the target *and* record it under one write lock, and look the
    // light up without creating it: `forget_lights` may have removed it since
    // the registry read above, and an `entry().or_insert_with()` would then
    // resurrect it with a fallback name. Two rapid requests therefore
    // alternate ON/OFF instead of both reading the same stale value.
    let (optimistic_on, payload) = {
        let mut state = ctx.state.write().await;
        let Some(light) = state.lights.get_mut(&id) else {
            debug!(%id, "ignoring toggle for unknown light");
            return StatusCode::NOT_FOUND;
        };

        // `publish` only reports a closed request channel, not a dead broker:
        // it would keep returning Ok while disconnected and then block once
        // the 32-slot queue filled. Check the session flag before flipping, so
        // an offline broker fails fast and no optimistic state is written.
        if !ctx.mqtt.is_connected() {
            warn!(%id, "MQTT broker is not connected; rejecting light command");
            return StatusCode::BAD_GATEWAY;
        }

        flip_light(light, &payload_on, &payload_off)
    };

    // `try_publish` never waits for the event loop: if the 32-slot request
    // queue is full it fails now, so the request can't stall and the optimistic
    // flip can be undone before the client sees a response.
    match ctx
        .mqtt
        .try_publish(&topic, rumqttc::QoS::AtLeastOnce, false, payload.clone())
    {
        Ok(()) => {
            debug!(%id, %payload, %topic, "sent light command");
            StatusCode::NO_CONTENT
        }
        Err(e) => {
            // The command never left, so undo the optimistic flip unless a
            // real state update arrived in the meantime.
            revert_flip(&mut *ctx.state.write().await, &id, optimistic_on);
            warn!(error = %e, %id, "failed to queue light command");
            StatusCode::BAD_GATEWAY
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{test_ctx, LightView};
    use axum::http::HeaderValue;

    fn toggle_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-requested-with", HeaderValue::from_static("ha-lite"));
        headers
    }

    #[tokio::test]
    async fn a_toggle_without_the_custom_header_is_forbidden() {
        let (ctx, _eventloop, _requests) = test_ctx(true, 4);
        ctx.state.write().await.lights.insert(
            "lamp".to_string(),
            LightView {
                name: "Lamp".to_string(),
                on: false,
            },
        );

        let status = toggle_light(
            Path("lamp".to_string()),
            State(ctx.clone()),
            HeaderMap::new(),
        )
        .await;

        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(!ctx.state.read().await.lights["lamp"].on);
    }

    #[tokio::test]
    async fn offline_toggle_is_rejected_without_flipping() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);
        ctx.state.write().await.lights.insert(
            "lamp".to_string(),
            LightView {
                name: "Lamp".to_string(),
                on: false,
            },
        );

        let status = toggle_light(
            Path("lamp".to_string()),
            State(ctx.clone()),
            toggle_headers(),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(!ctx.state.read().await.lights["lamp"].on);
    }

    #[tokio::test]
    async fn unknown_light_is_404_even_while_offline() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);

        let status = toggle_light(
            Path("ghost".to_string()),
            State(ctx.clone()),
            toggle_headers(),
        )
        .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(ctx.state.read().await.lights.is_empty());
    }

    #[tokio::test]
    async fn a_full_request_queue_reverts_the_toggle_instead_of_blocking() {
        let (ctx, _eventloop, _requests) = test_ctx(true, 1);
        ctx.state.write().await.lights.insert(
            "lamp".to_string(),
            LightView {
                name: "Lamp".to_string(),
                on: false,
            },
        );
        // Occupy the only slot. A connected broker with a stalled event loop
        // looks like this to the handler.
        ctx.mqtt
            .try_publish(
                "unrelated/topic",
                rumqttc::QoS::AtMostOnce,
                false,
                b"x".to_vec(),
            )
            .expect("first publish fits");

        let status = toggle_light(
            Path("lamp".to_string()),
            State(ctx.clone()),
            toggle_headers(),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(
            !ctx.state.read().await.lights["lamp"].on,
            "flip must be undone"
        );
    }
}
