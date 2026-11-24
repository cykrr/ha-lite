//! Discovered light/switch entities: their configuration, the topic indexes
//! that keep subscriptions correct, and the state/command handling.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use rumqttc::QoS;
use tracing::{debug, info};

use crate::app::{AppContext, HomeState, LightView};

#[derive(Clone)]
pub(crate) struct LightConfig {
    pub(crate) state_topics: Vec<String>,
    pub(crate) command_topic: String,
    /// Topic to publish an empty payload to in order to ask for current state.
    pub(crate) request_topic: Option<String>,
    pub(crate) payload_on: String,
    pub(crate) payload_off: String,
    /// When set, state payloads are JSON and this key holds ON/OFF.
    pub(crate) state_key: Option<String>,
}

impl LightConfig {
    /// Resolve a state payload to `Some(on)` for this entity. Returns `None`
    /// when the payload belongs to another channel or an unknown value.
    pub(crate) fn state_is_on(&self, raw: &str, json: Option<&serde_json::Value>) -> Option<bool> {
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

/// An entity and the discovery topic that announced it. Keeping them in one
/// value means the two can't disagree about which topic owns an entity.
struct Entry {
    config: LightConfig,
    discovery_topic: String,
}

#[derive(Default)]
pub(crate) struct LightRegistry {
    by_id: HashMap<String, Entry>,
    /// Several entities can share a state topic: every channel of a
    /// multi-relay Tasmota device is driven by the same `.../RESULT` and
    /// `.../STATE` topics, each reading its own `POWERn` JSON key.
    by_state_topic: HashMap<String, HashSet<String>>,
    /// Discovery topic that announced each entity, so an empty retained config
    /// removes exactly the entities it created.
    by_discovery_topic: HashMap<String, HashSet<String>>,
    /// Counts MQTT sessions, so a delayed sweep can tell it belongs to an
    /// older connection and must not run.
    session: u64,
    /// Discovery topics (re)announced on the current session. The broker replays
    /// every retained config after a reconnect, so anything still missing once
    /// the replay settles was deleted while we were offline.
    seen_discovery: HashSet<String>,
}

/// Drop `id` from a topic index, removing the topic once nobody owns it.
/// Returns `true` when the topic became unused.
fn unindex(index: &mut HashMap<String, HashSet<String>>, topic: &str, id: &str) -> bool {
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
pub(crate) struct Registration {
    pub(crate) is_new: bool,
    /// State topics that had no owner before, so they still need a SUBSCRIBE.
    pub(crate) new_topics: Vec<String>,
    /// State topics this entity dropped from its config and nobody else owns;
    /// the caller should UNSUBSCRIBE them.
    pub(crate) freed_topics: Vec<String>,
}

impl LightRegistry {
    /// Insert or replace an entity announced on `discovery_topic`.
    pub(crate) fn insert(
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
        let previous_topics = self.by_id.get(&id).map(|e| e.config.state_topics.clone());
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
        if let Some(entry) = self.by_id.get(&id) {
            unindex(&mut self.by_discovery_topic, &entry.discovery_topic, &id);
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
        self.seen_discovery.insert(discovery_topic.to_string());

        let entry = Entry {
            config,
            discovery_topic: discovery_topic.to_string(),
        };
        Registration {
            is_new: self.by_id.insert(id, entry).is_none(),
            new_topics,
            freed_topics,
        }
    }

    pub(crate) fn get(&self, id: &str) -> Option<&LightConfig> {
        self.by_id.get(id).map(|entry| &entry.config)
    }

    pub(crate) fn configs(&self) -> impl Iterator<Item = &LightConfig> {
        self.by_id.values().map(|entry| &entry.config)
    }

    /// Remove entities by id, forgetting both indexes. Returns the ids that
    /// existed and any state topics that no longer have an owner.
    fn remove_ids(&mut self, ids: impl IntoIterator<Item = String>) -> (Vec<String>, Vec<String>) {
        let mut removed = Vec::new();
        let mut freed_topics = Vec::new();

        for id in ids {
            let Some(entry) = self.by_id.remove(&id) else {
                continue;
            };
            unindex(&mut self.by_discovery_topic, &entry.discovery_topic, &id);
            for topic in &entry.config.state_topics {
                if unindex(&mut self.by_state_topic, topic, &id) {
                    freed_topics.push(topic.clone());
                }
            }
            removed.push(id);
        }

        (removed, freed_topics)
    }

    /// Remove every entity announced on this discovery topic.
    pub(crate) fn remove_by_discovery_topic(
        &mut self,
        discovery_topic: &str,
    ) -> (Vec<String>, Vec<String>) {
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
    pub(crate) fn retain_discovery_topic(
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

    /// Start tracking a new MQTT session and return its number. Discovery that
    /// the broker replays from now on counts as seen.
    pub(crate) fn begin_session(&mut self) -> u64 {
        self.session += 1;
        self.seen_discovery.clear();
        self.session
    }

    /// Remove entities whose discovery topic was not re-announced on `session`.
    /// Returns `None` when a newer session has started since, which makes the
    /// caller's view of "seen" stale.
    pub(crate) fn sweep_unseen(
        &mut self,
        session: u64,
    ) -> Option<(Vec<String>, Vec<String>)> {
        if session != self.session {
            return None;
        }

        let stale: Vec<String> = self
            .by_discovery_topic
            .iter()
            .filter(|(topic, _)| !self.seen_discovery.contains(*topic))
            .flat_map(|(_, ids)| ids.iter().cloned())
            .collect();
        Some(self.remove_ids(stale))
    }

    /// Every entity whose state may be carried by this topic.
    pub(crate) fn ids_for_topic(&self, topic: &str) -> Vec<String> {
        self.by_state_topic
            .get(topic)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }
}

pub(crate) async fn register_light(
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
        ctx.mqtt.subscribe(topic, QoS::AtLeastOnce);
    }
    for topic in &registration.freed_topics {
        ctx.mqtt.unsubscribe(topic);
    }
    if let Some(topic) = &config.request_topic {
        ctx.mqtt.request_state(topic);
    }
}

/// Drop entities from the UI and stop listening to state topics that lost
/// their last owner.
pub(crate) async fn forget_lights(
    ctx: &AppContext,
    removed: &[String],
    freed_topics: Vec<String>,
) {
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
        ctx.mqtt.unsubscribe(&topic);
    }
}

/// Apply a state payload to every discovered entity that owns `topic`.
/// Returns whether any entity claimed the topic, so the caller can fall back
/// to the legacy convention only for unowned topics.
pub(crate) async fn handle_light_state(ctx: &Arc<AppContext>, topic: &str, payload: &[u8]) -> bool {
    let configs: Vec<(String, LightConfig)> = {
        let reg = ctx.lights.read().await;
        reg.ids_for_topic(topic)
            .into_iter()
            .filter_map(|id| reg.get(&id).map(|cfg| (id, cfg.clone())))
            .collect()
    };

    if configs.is_empty() {
        return false;
    }

    let raw = String::from_utf8_lossy(payload);
    let raw = raw.trim();
    if raw.is_empty() {
        // Our own empty state request echoed back by the broker.
        return true;
    }

    // Parse once when any entity on this topic expects JSON (Tasmota).
    let json = if configs.iter().any(|(_, cfg)| cfg.state_key.is_some()) {
        match serde_json::from_str::<serde_json::Value>(raw) {
            Ok(json) => Some(json),
            Err(e) => {
                // Keyed entities skip a non-JSON payload (no JSON to read), but
                // a raw entity sharing the topic must still see it.
                debug!(error = %e, topic, payload = raw, "light state is not JSON");
                None
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

    true
}

/// Match the legacy `lights/<id>/state` convention. Only a single-segment id
/// matches the `lights/+/state` subscription, so nested paths belong to a
/// discovered entity's `stat_t`.
pub(crate) fn legacy_light_id(topic: &str) -> Option<&str> {
    topic
        .strip_prefix("lights/")
        .and_then(|rest| rest.strip_suffix("/state"))
        .filter(|id| !id.is_empty() && !id.contains('/'))
}

/// Legacy lights use exactly `ON` and `OFF`; anything else is malformed and
/// must not be read as OFF.
pub(crate) fn legacy_light_state(payload: &[u8]) -> Option<bool> {
    match payload {
        b"ON" => Some(true),
        b"OFF" => Some(false),
        _ => None,
    }
}

/// What a client asked a light to do. `Set` names the target state, so
/// repeating it (a retry, a double tap, two clients) converges instead of
/// flipping back and forth; `Toggle` flips whatever state is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Command {
    Toggle,
    Set(bool),
}

/// The optimistic change a command made, kept so a failed publish can undo it.
pub(crate) struct Applied {
    pub(crate) previous: bool,
    pub(crate) on: bool,
    pub(crate) payload: String,
}

/// Apply a command to a light that the caller has already looked up and return
/// the change plus the payload to publish. Taking the view rather than the
/// whole state means a command can never insert an entity, so it can't
/// resurrect one that discovery removed concurrently.
pub(crate) fn apply_command(
    light: &mut LightView,
    command: Command,
    payload_on: &str,
    payload_off: &str,
) -> Applied {
    let previous = light.on;
    light.on = match command {
        Command::Toggle => !previous,
        Command::Set(on) => on,
    };
    let payload = if light.on { payload_on } else { payload_off };
    Applied {
        previous,
        on: light.on,
        payload: payload.to_string(),
    }
}

/// Undo an optimistic change after a failed publish, but only while the light
/// still holds our value: a real state update may have superseded it.
pub(crate) fn revert_command(state: &mut HomeState, id: &str, applied: &Applied) {
    if let Some(light) = state.lights.get_mut(id) {
        if light.on == applied.on {
            light.on = applied.previous;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::test_ctx;

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
    fn legacy_light_id_only_matches_one_segment() {
        assert_eq!(legacy_light_id("lights/lamp/state"), Some("lamp"));
        assert_eq!(legacy_light_id("lights/bedroom/ceiling/state"), None);
        assert_eq!(legacy_light_id("lights//state"), None);
        assert_eq!(legacy_light_id("lights/lamp/set"), None);
        assert_eq!(legacy_light_id("sensors/lamp/state"), None);
    }

    #[test]
    fn legacy_light_state_requires_explicit_on_or_off() {
        assert_eq!(legacy_light_state(b"ON"), Some(true));
        assert_eq!(legacy_light_state(b"OFF"), Some(false));
        assert_eq!(legacy_light_state(b""), None);
        assert_eq!(legacy_light_state(b" ON "), None);
        assert_eq!(legacy_light_state(b"on"), None);
    }

    #[tokio::test]
    async fn a_discovered_entity_owns_its_nested_legacy_looking_topic() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);
        ctx.lights.write().await.insert(
            "desk".to_string(),
            light_config(&["lights/bedroom/ceiling/state"], None),
            "homeassistant/light/desk/config",
        );
        ctx.state.write().await.lights.insert(
            "desk".to_string(),
            LightView {
                name: "Desk".to_string(),
                on: false,
            },
        );

        let owned = handle_light_state(&ctx, "lights/bedroom/ceiling/state", b"ON").await;

        assert!(owned, "a registered entity claims the topic");
        assert!(ctx.state.read().await.lights["desk"].on);
    }

    #[tokio::test]
    async fn a_raw_entity_still_sees_a_non_json_payload_on_a_shared_topic() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);
        {
            let mut reg = ctx.lights.write().await;
            reg.insert(
                "json".to_string(),
                light_config(&["shared/state"], Some("POWER1")),
                "homeassistant/switch/json/config",
            );
            reg.insert(
                "raw".to_string(),
                light_config(&["shared/state"], None),
                "homeassistant/switch/raw/config",
            );
        }
        for id in ["json", "raw"] {
            ctx.state.write().await.lights.insert(
                id.to_string(),
                LightView {
                    name: id.to_string(),
                    on: false,
                },
            );
        }

        handle_light_state(&ctx, "shared/state", b"ON").await;

        let state = ctx.state.read().await;
        assert!(state.lights["raw"].on, "the raw entity must apply the payload");
        assert!(!state.lights["json"].on, "the keyed entity has no JSON to read");
    }

    #[tokio::test]
    async fn an_unregistered_topic_is_not_owned_by_any_entity() {
        let (ctx, _eventloop, _requests) = test_ctx(false, 4);

        let owned = handle_light_state(&ctx, "lights/lamp/state", b"ON").await;

        assert!(!owned, "the legacy fallback must handle this topic");
        assert!(ctx.state.read().await.lights.is_empty());
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
        assert!(reg.get("tasmota_MAC_2").is_none());
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
        assert!(reg.get("other").is_some());
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
        assert_eq!(reg.configs().count(), 1);

        // The still-announced entity is untouched.
        let (removed, _) = reg.remove_by_discovery_topic("homeassistant/switch/other/config");
        assert_eq!(removed, vec!["other"]);
        assert!(reg.configs().next().is_none());
    }

    #[test]
    fn re_announcing_an_entity_on_a_new_topic_moves_its_ownership() {
        let mut reg = LightRegistry::default();
        reg.insert(
            "desk".to_string(),
            light_config(&["stat/desk"], None),
            "homeassistant/switch/old/config",
        );
        reg.insert(
            "desk".to_string(),
            light_config(&["stat/desk"], None),
            "homeassistant/switch/new/config",
        );

        let (removed, _) = reg.remove_by_discovery_topic("homeassistant/switch/old/config");
        assert!(removed.is_empty(), "the old topic no longer owns the entity");
        assert!(reg.get("desk").is_some());

        let (removed, freed) = reg.remove_by_discovery_topic("homeassistant/switch/new/config");
        assert_eq!(removed, vec!["desk"]);
        assert_eq!(freed, vec!["stat/desk"]);
        assert!(reg.get("desk").is_none());
    }

    #[test]
    fn rapid_toggles_alternate_payloads() {
        let mut light = LightView {
            name: "lamp".to_string(),
            on: false,
        };

        let first = apply_command(&mut light, Command::Toggle, "ON", "OFF");
        assert_eq!((first.on, first.payload.as_str()), (true, "ON"));
        let second = apply_command(&mut light, Command::Toggle, "ON", "OFF");
        assert_eq!((second.on, second.payload.as_str()), (false, "OFF"));
        let third = apply_command(&mut light, Command::Toggle, "1", "0");
        assert_eq!((third.on, third.payload.as_str()), (true, "1"));
        assert!(light.on);
    }

    #[test]
    fn setting_an_explicit_state_is_idempotent() {
        let mut light = LightView {
            name: "lamp".to_string(),
            on: false,
        };

        for _ in 0..3 {
            let applied = apply_command(&mut light, Command::Set(true), "ON", "OFF");
            assert_eq!((applied.on, applied.payload.as_str()), (true, "ON"));
        }
        assert!(light.on);

        let applied = apply_command(&mut light, Command::Set(false), "ON", "OFF");
        assert_eq!((applied.on, applied.payload.as_str()), (false, "OFF"));
        assert!(!light.on);
    }

    #[test]
    fn reverting_a_no_op_set_keeps_the_light_on() {
        let mut state = HomeState::default();
        state.lights.insert(
            "lamp".to_string(),
            LightView {
                name: "lamp".to_string(),
                on: true,
            },
        );

        let applied = apply_command(
            state.lights.get_mut("lamp").unwrap(),
            Command::Set(true),
            "ON",
            "OFF",
        );
        revert_command(&mut state, "lamp", &applied);

        assert!(state.lights["lamp"].on, "previous state was already on");
    }

    #[test]
    fn failed_publish_reverts_optimistic_flip() {
        let mut state = HomeState::default();
        state.lights.insert(
            "lamp".to_string(),
            LightView {
                name: "lamp".to_string(),
                on: false,
            },
        );

        let applied = apply_command(
            state.lights.get_mut("lamp").unwrap(),
            Command::Toggle,
            "ON",
            "OFF",
        );
        assert!(applied.on);
        revert_command(&mut state, "lamp", &applied);

        assert!(!state.lights["lamp"].on);
    }

    #[test]
    fn toggling_preserves_the_registered_name() {
        let mut light = LightView {
            name: "Tasmota Left".to_string(),
            on: false,
        };

        apply_command(&mut light, Command::Toggle, "ON", "OFF");

        assert_eq!(light.name, "Tasmota Left");
    }

    #[test]
    fn revert_does_not_clobber_a_newer_state_update() {
        let mut state = HomeState::default();
        state.lights.insert(
            "lamp".to_string(),
            LightView {
                name: "lamp".to_string(),
                on: false,
            },
        );

        let applied = apply_command(
            state.lights.get_mut("lamp").unwrap(),
            Command::Toggle,
            "ON",
            "OFF",
        );
        assert!(applied.on);

        // The device reported OFF between our flip and the failed publish.
        state.lights.get_mut("lamp").unwrap().on = false;
        revert_command(&mut state, "lamp", &applied);

        assert!(!state.lights["lamp"].on, "authoritative state must win");
    }
}
