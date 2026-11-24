//! Discovery payload parsing: Home Assistant MQTT discovery and the Tasmota
//! integration format. Both turn a retained config into `LightConfig` entities
//! and hand them to [`crate::lights`].

use std::collections::HashSet;
use std::sync::Arc;

use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::app::AppContext;
use crate::lights::{forget_lights, register_light, LightConfig};

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

struct HaEntity {
    id: String,
    name: String,
    config: LightConfig,
}

struct TasmotaEntity {
    id: String,
    name: String,
    kind: &'static str,
    config: LightConfig,
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

pub(crate) async fn handle_discovery(
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
pub(crate) async fn handle_tasmota_discovery(
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
    // `rl` is indexed by power slot, and slots held by lights or shutters are
    // filtered out above. A lone relay in a later slot is still `POWER<n>`,
    // so number by slot whenever any channel sits past the first one.
    let multiple = channels.len() > 1 || channels.iter().any(|(idx, _)| *idx > 0);

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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_lone_relay_in_a_later_slot_keeps_its_slot_number() {
        let payload = r#"{
            "dn":"Dev","fn":[null,null],"t":"tasmota_test","ft":"%prefix%/%topic%/",
            "tp":["cmnd","stat","tele"],"rl":[0,1,0,0]
        }"#;

        let cfg: TasmotaDiscovery = serde_json::from_str(payload).unwrap();
        let entities = tasmota_entities("AABBCCDDEEFF", &cfg);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].id, "tasmota_AABBCCDDEEFF_2");
        assert_eq!(entities[0].config.command_topic, "cmnd/tasmota_test/POWER2");
        assert_eq!(entities[0].config.state_key.as_deref(), Some("POWER2"));
    }
}
