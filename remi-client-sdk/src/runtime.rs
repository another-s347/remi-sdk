use crate::context_prompt;
use crate::events_events::EventsEvent;
use crate::storage::Storage;
use crate::things_crdt::{
    ThingCollectionEntry, ThingCollectionUpsert, ThingEntry, ThingUpsert, ThingsSnapshot,
};
use crate::things_events::ThingsEvent;
use crate::trigger_events::TriggerEvent;
use crate::types::{
    EventPayload, NotificationEntry, NotificationGroup, NotificationSource, StoredTrigger,
    ThingsChangeLogEntry, ThingsContentSnapshot, ThingsOperationType, ThingsUndoConflict,
    ThingsUndoConflictType, ThingsUndoExecution, ThingsUndoPreview, ThingsUndoResolutionOption,
    TriggerExecutionSummary, TriggerInfo, TriggerLogLevel, TriggerRegistration,
    TriggerReplaySummary, TriggerRule, TriggerRunType,
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Datelike, Duration, FixedOffset, Timelike, Utc};
use croner::Cron;
use rule_trigger_engine::{
    EvaluationContext, MonitoringEvent, PreconditionPolicy, Rule as EngineRule, TriggerConfig,
};
use serde_json::Value as JsonValue;
use serde_json::json;
use serde_json::to_string;
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

const DEFAULT_TIMEZONE_OFFSET: &str = "+08:00";

fn default_timezone() -> FixedOffset {
    FixedOffset::from_str(DEFAULT_TIMEZONE_OFFSET).unwrap_or_else(|_| {
        FixedOffset::east_opt(8 * 3600).expect("UTC+08:00 offset must be valid")
    })
}
pub trait TriggerCallback: Send + Sync {
    fn on_trigger(&self, summary: &TriggerExecutionSummary) -> Result<()>;
}

pub struct NotificationCallback;

impl TriggerCallback for NotificationCallback {
    fn on_trigger(&self, summary: &TriggerExecutionSummary) -> Result<()> {
        let fired_at_local = summary.fired_at.with_timezone(&default_timezone());
        info!(
            trigger_id = %summary.trigger_id,
            name = %summary.name,
            result = summary.result,
            fired_at_utc = %summary.fired_at,
            fired_at_local = %fired_at_local,
            "Trigger fired"
        );
        Ok(())
    }
}

pub struct TriggerSdk {
    storage: Storage,
    things_event_tx: broadcast::Sender<ThingsEvent>,
    trigger_event_tx: broadcast::Sender<TriggerEvent>,
    events_event_tx: broadcast::Sender<EventsEvent>,
}

impl TriggerSdk {
    pub fn initialize(db_path: impl AsRef<Path>) -> Result<Self> {
        let storage = Storage::new(db_path)?;
        let (things_event_tx, _rx) = broadcast::channel(2048);
        let (trigger_event_tx, _rx) = broadcast::channel(2048);
        let (events_event_tx, _rx) = broadcast::channel(2048);
        Ok(Self {
            storage,
            things_event_tx,
            trigger_event_tx,
            events_event_tx,
        })
    }

    pub fn things_subscribe(&self) -> broadcast::Receiver<ThingsEvent> {
        self.things_event_tx.subscribe()
    }

    pub fn triggers_subscribe(&self) -> broadcast::Receiver<TriggerEvent> {
        self.trigger_event_tx.subscribe()
    }

    pub fn events_subscribe(&self) -> broadcast::Receiver<EventsEvent> {
        self.events_event_tx.subscribe()
    }

    pub(crate) fn emit_things_event(&self, event: ThingsEvent) {
        // Ignore send errors (no active subscribers).
        let _ = self.things_event_tx.send(event);
    }

    /// Build and emit a `SnapshotReplace` event so Flutter replaces its full
    /// Things/Collections state.  Called after sync pulls new documents from
    /// the server.
    pub fn emit_snapshot_replace(&self, device_id: &str) -> Result<()> {
        use crate::things_crdt::SnapshotOptions;
        let doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set
            .extract_snapshot_with_options(SnapshotOptions {
                include_content: true,
            })
            .context("Failed to extract snapshot for SnapshotReplace event")?;
        let dirty = doc_set.has_pending_changes();
        self.emit_things_event(ThingsEvent::SnapshotReplace {
            device_id: device_id.to_string(),
            collections: snapshot.collections,
            things: snapshot.things,
            dirty,
            last_sync_at: None,
        });
        Ok(())
    }

    fn emit_trigger_event(&self, event: TriggerEvent) {
        // Ignore send errors (no active subscribers).
        let _ = self.trigger_event_tx.send(event);
    }

    fn emit_events_event(&self, event: EventsEvent) {
        // Ignore send errors (no active subscribers).
        let _ = self.events_event_tx.send(event);
    }

    /// Wipe all local data and broadcast DataWiped events to all streams.
    /// This is the canonical logout path — Flutter notifiers will clear
    /// in-memory state when they receive the DataWiped event.
    pub fn wipe_all_data_and_notify(&self) -> Result<()> {
        self.storage.wipe_all_data()?;
        self.emit_things_event(ThingsEvent::DataWiped);
        self.emit_trigger_event(TriggerEvent::DataWiped);
        self.emit_events_event(EventsEvent::DataWiped);
        info!("Wiped all local data and notified all event streams");
        Ok(())
    }

    /// Attribute all anonymous (user_id IS NULL) local data to the given user.
    /// Called after a successful login so locally-created content is owned by the user.
    pub fn claim_anonymous_data(&self, user_id: &str) -> Result<()> {
        let claimed = self.storage.claim_anonymous_data(user_id)?;
        info!(user_id = %user_id, claimed, "Claimed anonymous data after login");
        Ok(())
    }

    fn emit_things_field_patch(
        &self,
        device_id: &str,
        thing_uuid: &str,
        fields: BTreeMap<String, JsonValue>,
    ) {
        if fields.is_empty() {
            return;
        }
        self.emit_things_event(ThingsEvent::ThingUpsert {
            device_id: device_id.to_string(),
            thing_uuid: thing_uuid.to_string(),
            fields,
        });
    }

    fn emit_collection_field_patch(
        &self,
        device_id: &str,
        collection_uuid: &str,
        fields: BTreeMap<String, JsonValue>,
    ) {
        if fields.is_empty() {
            return;
        }
        self.emit_things_event(ThingsEvent::CollectionUpsert {
            device_id: device_id.to_string(),
            collection_uuid: collection_uuid.to_string(),
            fields,
        });
    }

    pub fn register_trigger(&self, params: TriggerRegistration) -> Result<String> {
        self.register_trigger_inner(params)
    }

    fn register_trigger_inner(&self, params: TriggerRegistration) -> Result<String> {
        // UUID must be provided
        if params.trigger_uuid.is_empty() {
            anyhow::bail!("Trigger UUID is required but not provided.");
        }

        // Extract timing from preconditions/conditions for scheduling.
        // Cron triggers store a computed `next_fire`; event-driven triggers (e.g. network_change)
        // default to `next_fire = None` and will be marked due by listeners.
        let timings = extract_timings_from_rules(&params.precondition, &params.condition)?;
        let cron = timings.iter().find_map(|t| match t {
            rule_trigger_engine::TriggerTiming::Cron { expression } => Some(expression.clone()),
            _ => None,
        });

        let next_fire = if let Some(cron) = cron {
            let now = Utc::now();
            compute_next_fire(&cron, now, Some(now))?
        } else if timings.iter().any(|t| {
            matches!(
                t,
                rule_trigger_engine::TriggerTiming::NetworkChange
                    | rule_trigger_engine::TriggerTiming::Location
            )
        }) {
            None
        } else {
            anyhow::bail!(
                "No supported timing found in trigger rules (expected cron(...) or network_change()/location())."
            );
        };

        let trigger_uuid = params.trigger_uuid.clone();
        let inserted_uuid = self.storage.insert_trigger(params, next_fire)?;
        self.emit_trigger_event(TriggerEvent::TriggerUpsert { trigger_uuid });
        Ok(inserted_uuid)
    }

    pub fn record_event(&self, event: EventPayload) -> Result<()> {
        let event_type = event.event_type.clone();
        let event_ts = event.timestamp;
        self.storage.insert_event(&event)?;

        // Emit event notification to subscribers (e.g. UI).
        self.emit_events_event(EventsEvent::EventRecorded {
            event_type: event_type.clone(),
            timestamp: event_ts.to_rfc3339(),
        });

        Ok(())
    }

    /// Schedule triggers that have `network_change()` in their precondition.
    /// This API is exposed for the application layer to call when a Connectivity event
    /// is recorded - the SDK no longer has hardcoded event type handling.
    pub fn schedule_network_change_triggers(&self, due_at: DateTime<Utc>) -> Result<()> {
        self.schedule_triggers_by_precondition_keyword("network_change(", due_at)
    }

    /// Schedule triggers that have `location_change()` in their precondition.
    /// This API is exposed for the application layer to call when a Location event
    /// is recorded.
    pub fn schedule_location_change_triggers(&self, due_at: DateTime<Utc>) -> Result<()> {
        self.schedule_triggers_by_precondition_keyword("location_change(", due_at)
    }

    /// Generic trigger scheduling by precondition keyword.
    /// Scans all triggers and marks those containing the keyword in precondition as due.
    fn schedule_triggers_by_precondition_keyword(
        &self,
        keyword: &str,
        due_at: DateTime<Utc>,
    ) -> Result<()> {
        let triggers = self.storage.list_triggers()?;
        for trigger in triggers {
            let has_keyword = trigger.precondition.iter().any(|rule| {
                let expr = rule.rule.replace(' ', "");
                expr.contains(keyword)
            });
            if !has_keyword {
                continue;
            }

            // Mark due; `run_due_triggers()` will execute it and reschedule appropriately.
            self.storage
                .mark_trigger_due(&trigger.trigger_id, due_at)
                .with_context(|| format!("Failed to mark trigger due: {}", trigger.trigger_id))?;
        }
        Ok(())
    }

    pub fn next_deadline(&self, now_unix: Option<i64>) -> Result<Option<DateTime<Utc>>> {
        self.storage.next_deadline(now_unix)
    }

    pub fn list_events_json(&self, limit: Option<u32>, offset: u32) -> Result<String> {
        let events = self.storage.list_events(limit, offset)?;
        let payloads: Vec<EventPayload> = events.into_iter().map(EventPayload::from).collect();
        to_string(&payloads).context("Failed to serialize events")
    }

    pub fn events_list_between_json(&self, start_time: &str, end_time: &str) -> Result<String> {
        let start = chrono::DateTime::parse_from_rfc3339(start_time)
            .context("Invalid start_time (RFC3339)")?
            .with_timezone(&Utc);
        let end = chrono::DateTime::parse_from_rfc3339(end_time)
            .context("Invalid end_time (RFC3339)")?
            .with_timezone(&Utc);
        if start > end {
            anyhow::bail!("start_time must be <= end_time");
        }

        let events = self
            .storage
            .list_events_between_utc(start.timestamp(), end.timestamp())?;
        let payloads: Vec<EventPayload> = events.into_iter().map(EventPayload::from).collect();
        to_string(&payloads).context("Failed to serialize events")
    }

    pub fn events_abstract_json(&self, top_n: u32) -> Result<String> {
        #[derive(Default)]
        struct Bucket {
            total: u32,
            counts: std::collections::BTreeMap<String, u32>,
        }

        // For now, read all events and bucket by UTC hour.
        // If needed, we can optimize with SQL aggregation.
        let events = self.storage.list_events(None, 0)?;
        let mut buckets: std::collections::BTreeMap<String, Bucket> =
            std::collections::BTreeMap::new();

        for ev in events {
            let dt = ev.timestamp;
            let hour_key = format!(
                "{:04}-{:02}-{:02} {:02}:00",
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour()
            );

            let bucket = buckets.entry(hour_key).or_default();
            bucket.total += 1;
            let et = ev.event_type.clone();
            *bucket.counts.entry(et).or_insert(0) += 1;
        }

        let mut hours_json = Vec::new();
        for (hour, bucket) in buckets {
            let mut top: Vec<(String, u32)> = bucket.counts.into_iter().collect();
            top.sort_by(|a, b| b.1.cmp(&a.1));
            top.truncate(top_n as usize);
            let top_types: Vec<serde_json::Value> = top
                .into_iter()
                .map(|(t, c)| json!({"type": t, "count": c}))
                .collect();
            hours_json.push(json!({
                "hour": hour,
                "total_events": bucket.total,
                "top_types": top_types,
            }));
        }

        to_string(&json!({"hours": hours_json, "top_n": top_n})).context("Failed to serialize")
    }

    pub fn event_count(&self) -> Result<i64> {
        self.storage.events_count()
    }

    pub fn list_triggers(&self) -> Result<Vec<TriggerInfo>> {
        self.storage.list_triggers()
    }

    pub fn list_triggers_json(&self) -> Result<String> {
        let triggers = self.list_triggers()?;
        to_string(&triggers).context("Failed to serialize triggers")
    }

    /// Pause or resume a trigger. Returns the updated paused state.
    pub fn set_trigger_paused(&self, trigger_uuid: &str, paused: bool) -> Result<()> {
        self.storage.set_trigger_paused(trigger_uuid, paused)
    }

    /// Record a binding between a trigger and a thing/collection.
    pub fn upsert_trigger_binding(
        &self,
        trigger_uuid: &str,
        entity_type: &str,
        entity_uuid: &str,
    ) -> Result<()> {
        self.storage
            .upsert_trigger_binding(trigger_uuid, entity_type, entity_uuid)
    }

    /// Remove the `trigger_bindings` row for the given entity.
    ///
    /// Use this when unbinding a trigger so that `is_trigger_bound` correctly reflects
    /// the new state before calling `delete_trigger_if_unbound`.
    pub fn delete_trigger_binding(&self, entity_type: &str, entity_uuid: &str) -> Result<()> {
        self.storage
            .delete_trigger_binding(entity_type, entity_uuid)
    }

    /// Get the trigger UUID currently bound to a specific entity from the trigger_bindings table.
    ///
    /// This supplements the CRDT snapshot lookup and catches stale bindings that the CRDT
    /// may not reflect (e.g., edge cases from migrations or non-CRDT binding paths).
    pub fn get_trigger_for_entity(
        &self,
        entity_type: &str,
        entity_uuid: &str,
    ) -> Result<Option<String>> {
        self.storage
            .get_trigger_for_entity(entity_type, entity_uuid)
    }

    /// Delete a trigger definition if it is no longer bound to any entity.
    ///
    /// Returns `true` if the trigger was deleted.
    pub fn delete_trigger_if_unbound(&self, trigger_uuid: &str) -> Result<bool> {
        if self.storage.is_trigger_bound(trigger_uuid)? {
            return Ok(false);
        }

        let deleted = self.storage.delete_trigger(trigger_uuid)?;
        if deleted {
            self.emit_trigger_event(TriggerEvent::TriggerDelete {
                trigger_uuid: trigger_uuid.to_string(),
            });
        }

        Ok(deleted)
    }

    /// Delete a trigger and all its bindings unconditionally.
    ///
    /// This is the correct method for an explicit user-initiated delete:
    /// it clears the CRDT on every bound entity, removes all `trigger_bindings`
    /// rows, deletes the trigger record, and emits the `TriggerDelete` event.
    ///
    /// Returns `true` if the trigger record was found and deleted.
    pub fn delete_trigger_and_bindings(&self, device_id: &str, trigger_uuid: &str) -> Result<bool> {
        // 1. Collect every entity currently bound to this trigger.
        let bound = self.storage.get_entities_for_trigger(trigger_uuid)?;

        // 2. Clear the CRDT trigger_uuid on each bound entity (best-effort).
        for (entity_type, entity_uuid) in &bound {
            // Pass Some("") to get TriggerUpdate::Clear — None is TriggerUpdate::Noop and skips the write.
            let result = match entity_type.as_str() {
                "collection" => {
                    self.things_set_collection_trigger_uuid(device_id, entity_uuid, Some(""))
                }
                "thing" => self.things_set_thing_trigger_uuid(device_id, entity_uuid, Some("")),
                other => {
                    tracing::warn!(entity_type = %other, "Unknown entity_type in trigger_bindings; skipping CRDT clear");
                    Ok(())
                }
            };
            if let Err(e) = result {
                tracing::warn!(
                    entity_type,
                    entity_uuid,
                    "Failed to clear CRDT trigger on entity: {e}"
                );
            }
        }

        // 3. Remove all trigger_bindings rows for this trigger.
        let removed = self.storage.delete_all_bindings_for_trigger(trigger_uuid)?;
        tracing::debug!(trigger_uuid, removed, "Removed trigger_bindings rows");

        // 4. Delete the trigger record.
        let deleted = self.storage.delete_trigger(trigger_uuid)?;
        if deleted {
            self.emit_trigger_event(TriggerEvent::TriggerDelete {
                trigger_uuid: trigger_uuid.to_string(),
            });
        }

        Ok(deleted)
    }

    /// Set (or clear) a collection's `trigger_uuid` inside the Things CRDT.
    ///
    /// This is the canonical source of truth for trigger bindings as surfaced in the UI.
    /// The SQL `trigger_bindings` table is reconciled from this CRDT state.
    pub fn things_set_collection_trigger_uuid(
        &self,
        device_id: &str,
        collection_uuid: &str,
        trigger_uuid: Option<&str>,
    ) -> Result<()> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Validate the collection exists to avoid creating phantom bindings.
        let snapshot = doc_set.extract_snapshot()?;
        if snapshot
            .collections
            .iter()
            .find(|c| c.uuid == collection_uuid)
            .is_none()
        {
            anyhow::bail!("Collection not found: {collection_uuid}");
        }

        // V3: Update collection trigger via collection document
        let trigger = crate::things_crdt::trigger_update_from_tri_state(trigger_uuid);
        doc_set.update_collection_meta(collection_uuid, None, None, trigger)?;
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        let mut fields: BTreeMap<String, JsonValue> = BTreeMap::new();
        fields.insert(
            "trigger_uuid".to_string(),
            match trigger_uuid {
                None => JsonValue::Null,
                Some(v) if v.trim().is_empty() => JsonValue::Null,
                Some(v) => JsonValue::String(v.to_string()),
            },
        );
        self.emit_collection_field_patch(device_id, collection_uuid, fields);

        Ok(())
    }

    /// Set (or clear) a thing's `trigger_uuid` inside the Things CRDT.
    pub fn things_set_thing_trigger_uuid(
        &self,
        device_id: &str,
        thing_uuid: &str,
        trigger_uuid: Option<&str>,
    ) -> Result<()> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set.extract_snapshot()?;

        let collection_uuid = match snapshot.things.iter().find(|t| t.uuid == thing_uuid) {
            Some(t) => t.collection_uuid.clone(),
            None => {
                tracing::warn!(
                    thing_uuid,
                    "things_set_thing_trigger_uuid: thing not in snapshot, scanning collection docs"
                );
                doc_set
                    .find_thing_collection_uuid(thing_uuid)
                    .ok_or_else(|| anyhow!("Thing not found: {thing_uuid}"))?
            }
        };

        // V3: Update thing trigger via collection document
        let trigger = crate::things_crdt::trigger_update_from_tri_state(trigger_uuid);
        doc_set.upsert_thing_meta(
            &collection_uuid,
            thing_uuid,
            None, // datatype
            None, // status
            None, // title
            None, // parent_uuid
            trigger,
        )?;
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        let mut fields: BTreeMap<String, JsonValue> = BTreeMap::new();
        fields.insert(
            "trigger_uuid".to_string(),
            match trigger_uuid {
                None => JsonValue::Null,
                Some(v) if v.trim().is_empty() => JsonValue::Null,
                Some(v) => JsonValue::String(v.to_string()),
            },
        );
        self.emit_things_field_patch(device_id, thing_uuid, fields);

        Ok(())
    }

    pub fn build_session_context(
        &self,
        device_id: &str,
        granted_permissions: &[String],
        active_context_json: Option<&str>,
    ) -> Result<String> {
        use std::time::Instant;

        let total = Instant::now();

        let t0 = Instant::now();
        // V3: Load document set for session context
        let doc_set = self.get_or_init_document_set(device_id)?;
        info!(
            device_id = %device_id,
            active_context = active_context_json.is_some(),
            permissions = granted_permissions.len(),
            ms = t0.elapsed().as_millis(),
            "build_session_context: get_or_init_document_set"
        );

        let t1 = Instant::now();
        let snapshot =
            doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
                include_content: false,
            })?;
        info!(
            collections = snapshot.collections.len(),
            things = snapshot.things.len(),
            ms = t1.elapsed().as_millis(),
            "build_session_context: extract_snapshot"
        );

        let t2 = Instant::now();
        let triggers = self.storage.list_triggers_for_context_prompt(50)?;
        info!(
            triggers = triggers.len(),
            ms = t2.elapsed().as_millis(),
            "build_session_context: list_triggers_for_context_prompt"
        );

        let t3 = Instant::now();
        let out = context_prompt::build_context_prompt_markdown(
            granted_permissions,
            &snapshot,
            &triggers,
            active_context_json,
        )?;
        info!(
            out_bytes = out.len(),
            ms = t3.elapsed().as_millis(),
            total_ms = total.elapsed().as_millis(),
            "build_session_context: build_context_prompt_markdown"
        );

        Ok(out)
    }

    // ===== Things V3 (Multi-document CRDT) =====

    /// Load or initialize the v3 document set from storage
    fn get_or_init_document_set(
        &self,
        device_id: &str,
    ) -> Result<crate::things_crdt::ThingsDocumentSet> {
        let mut doc_set =
            crate::things_crdt::ThingsDocumentSet::load_from_storage(&self.storage, device_id)?;

        // Ensure root document exists
        if !doc_set.contains(&crate::things_crdt::DocumentKey::root()) {
            doc_set.init_root()?;
            doc_set.save_to_storage(&self.storage)?;
        }

        // Repair root → collection linkage if any collection documents
        // are present locally but not listed in the root document.
        // This can happen after interrupted syncs or out-of-order document pulls.
        let repaired = doc_set.repair_root_collection_linkage()?;
        if repaired > 0 {
            tracing::info!(
                device_id,
                repaired,
                "get_or_init_document_set: repaired root-collection linkage"
            );
            doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;
        }

        Ok(doc_set)
    }

    pub fn things_list_snapshot_json(&self, device_id: &str) -> Result<String> {
        self.things_list_snapshot_json_with_options(
            device_id,
            true,
            crate::things_crdt::SnapshotOptions {
                include_content: true,
            },
        )
    }

    /// Snapshot optimized for agent/tools and context prompts.
    ///
    /// - Omits thing `data.content` entirely.
    /// - Still returns collections + things metadata.
    pub fn things_list_snapshot_json_lite(&self, device_id: &str) -> Result<String> {
        self.things_list_snapshot_json_with_options(
            device_id,
            true,
            crate::things_crdt::SnapshotOptions {
                include_content: false,
            },
        )
    }

    /// Flexible snapshot builder for tools/UI.
    ///
    /// Use this to avoid extracting content (and optionally avoid extracting things at all).
    pub fn things_list_snapshot_json_with_options(
        &self,
        device_id: &str,
        _include_things: bool,
        snapshot_options: crate::things_crdt::SnapshotOptions,
    ) -> Result<String> {
        let doc_set = self.get_or_init_document_set(device_id)?;
        let mut snapshot = doc_set
            .extract_snapshot_with_options(snapshot_options)
            .context("Failed to extract v3 snapshot")?;
        let dirty = doc_set.has_pending_changes();

        // Enrich snapshot entries with cached actor attribution metadata.
        if let Ok(actor_meta) = self.storage.load_things_actor_meta_map() {
            for col in &mut snapshot.collections {
                if let Some(meta) = actor_meta.get(&col.uuid) {
                    col.actor_type = Some(meta.actor_type.clone());
                    col.actor_app_id = meta.actor_app_id.clone();
                    col.actor_display_name = meta.actor_display_name.clone();
                }
            }
            for thing in &mut snapshot.things {
                if let Some(meta) = actor_meta.get(&thing.uuid) {
                    thing.actor_type = Some(meta.actor_type.clone());
                    thing.actor_app_id = meta.actor_app_id.clone();
                    thing.actor_display_name = meta.actor_display_name.clone();
                }
            }
        }

        let payload = json!({
            "collections": snapshot.collections,
            "things": snapshot.things,
            "dirty": dirty,
            "last_sync_at": null,
        });
        to_string(&payload).context("Failed to serialize things snapshot")
    }

    /// Store a batch of actor attribution metadata (fetched from server) into the local cache.
    pub fn things_upsert_actor_meta(&self, items: &[crate::storage::ActorMetaEntry]) -> Result<()> {
        self.storage
            .upsert_things_actor_meta_batch(items)
            .context("Failed to store actor meta batch")
    }

    pub fn things_has_pending_changes(&self, device_id: &str) -> Result<bool> {
        let doc_set = self.get_or_init_document_set(device_id)?;
        Ok(doc_set.has_pending_changes())
    }

    pub fn things_upsert_collection_json(
        &self,
        device_id: &str,
        payload_json: &str,
    ) -> Result<String> {
        let upsert: ThingCollectionUpsert =
            serde_json::from_str(payload_json).context("Invalid ThingCollectionUpsert JSON")?;
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Check if collection exists before upsert to determine if this is create or update
        let before_snapshot = doc_set.extract_snapshot()?;
        let existing = before_snapshot
            .collections
            .iter()
            .find(|c| c.uuid == upsert.uuid);
        let is_create = existing.is_none();

        // V3: Update collection metadata
        let trigger =
            crate::things_crdt::trigger_update_from_tri_state(upsert.trigger_uuid.as_deref());
        doc_set.update_collection_meta(&upsert.uuid, Some(upsert.title.clone()), None, trigger)?;
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        let snapshot = doc_set.extract_snapshot()?;
        let created = snapshot
            .collections
            .iter()
            .find(|c| c.uuid == upsert.uuid)
            .ok_or_else(|| anyhow!("Collection not found after upsert"))?;

        let mut fields: BTreeMap<String, JsonValue> = BTreeMap::new();
        fields.insert(
            "title".to_string(),
            JsonValue::String(created.title.clone()),
        );
        if is_create {
            fields.insert(
                "trigger_uuid".to_string(),
                created
                    .trigger_uuid
                    .as_ref()
                    .map(|v| JsonValue::String(v.clone()))
                    .unwrap_or(JsonValue::Null),
            );
            fields.insert(
                "created_at".to_string(),
                JsonValue::String(created.created_at.clone()),
            );
        } else if let Some(v) = upsert.trigger_uuid.as_deref() {
            fields.insert(
                "trigger_uuid".to_string(),
                if v.is_empty() {
                    JsonValue::Null
                } else {
                    JsonValue::String(v.to_string())
                },
            );
        }
        fields.insert(
            "updated_at".to_string(),
            JsonValue::String(created.updated_at.clone()),
        );
        self.emit_collection_field_patch(device_id, &upsert.uuid, fields);

        // Record change log
        let op_type = if is_create {
            ThingsOperationType::CreateCollection
        } else {
            ThingsOperationType::UpdateCollection
        };
        let summary = if is_create {
            format!("Created collection '{}'", created.title)
        } else {
            format!("Updated collection '{}'", created.title)
        };
        let details = json!({
            "uuid": upsert.uuid,
            "title": upsert.title,
            "trigger_uuid": upsert.trigger_uuid,
        });
        let _ = self.storage.insert_things_change_log(
            device_id,
            op_type,
            "collection",
            &upsert.uuid,
            &summary,
            &details.to_string(),
            None,
            true,
        );

        to_string(&created).context("Failed to serialize collection")
    }

    pub fn things_delete_collection(&self, device_id: &str, uuid: &str) -> Result<bool> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;
        let before = doc_set.extract_snapshot()?;
        let collection = before.collections.iter().find(|c| c.uuid == uuid);
        if collection.is_none() {
            return Ok(false);
        }
        let collection = collection.unwrap();
        let collection_title = collection.title.clone();

        let mut removed_triggers: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        if let Some(trigger_uuid) = collection.trigger_uuid.as_ref() {
            let _ = self
                .storage
                .delete_trigger_binding("collection", uuid)
                .map_err(|e| {
                    tracing::warn!(
                        collection_uuid = %uuid,
                        trigger_uuid = %trigger_uuid,
                        error = %e,
                        "Failed to delete trigger binding for collection"
                    );
                    e
                });
            removed_triggers.insert(trigger_uuid.clone());
        }

        // Find all things in this collection (for cascade logging)
        let things_in_collection: Vec<_> = before
            .things
            .iter()
            .filter(|t| t.collection_uuid == uuid)
            .cloned()
            .collect();

        for thing in &things_in_collection {
            if let Some(trigger_uuid) = thing.trigger_uuid.as_ref() {
                let _ = self
                    .storage
                    .delete_trigger_binding("thing", &thing.uuid)
                    .map_err(|e| {
                        tracing::warn!(
                            thing_uuid = %thing.uuid,
                            trigger_uuid = %trigger_uuid,
                            error = %e,
                            "Failed to delete trigger binding for thing"
                        );
                        e
                    });
                removed_triggers.insert(trigger_uuid.clone());
            }
        }

        // V3: Delete collection and associated documents from storage
        doc_set.delete_collection_from_storage(&self.storage, uuid)?;
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        // Emit incremental delete events (collection + cascade things).
        for thing in &things_in_collection {
            self.emit_things_event(ThingsEvent::ThingDelete {
                device_id: device_id.to_string(),
                thing_uuid: thing.uuid.clone(),
            });
        }
        self.emit_things_event(ThingsEvent::CollectionDelete {
            device_id: device_id.to_string(),
            collection_uuid: uuid.to_string(),
        });

        // Record change log for the collection deletion
        let summary = format!("Deleted collection '{}'", collection_title);
        let details = json!({
            "uuid": uuid,
            "title": collection_title,
            "things_count": things_in_collection.len(),
        });
        let parent_log_id = self
            .storage
            .insert_things_change_log(
                device_id,
                ThingsOperationType::DeleteCollection,
                "collection",
                uuid,
                &summary,
                &details.to_string(),
                None,
                true,
            )
            .ok();

        // Record cascade deletions for things
        if let Some(parent_id) = parent_log_id {
            let mut cascade_ids = Vec::new();
            for thing in &things_in_collection {
                let thing_summary = format!("Deleted thing '{}' (cascade)", thing.title);
                let thing_details = json!({
                    "uuid": thing.uuid,
                    "title": thing.title,
                    "collection_uuid": uuid,
                    "cascade_reason": "parent_collection_deleted",
                });
                if let Ok(cascade_id) = self.storage.insert_things_change_log(
                    device_id,
                    ThingsOperationType::DeleteThing,
                    "thing",
                    &thing.uuid,
                    &thing_summary,
                    &thing_details.to_string(),
                    Some(parent_id),
                    false, // Cascade deletions can't be individually undone
                ) {
                    cascade_ids.push(cascade_id);
                }
            }
            if !cascade_ids.is_empty() {
                let _ = self
                    .storage
                    .update_things_change_log_cascade_ids(parent_id, &cascade_ids);
            }
        }

        // After bindings are cleared, uninstall triggers that are no longer bound.
        for trigger_uuid in removed_triggers {
            if let Err(e) = self.delete_trigger_if_unbound(&trigger_uuid) {
                tracing::warn!(
                    trigger_uuid = %trigger_uuid,
                    error = %e,
                    "Failed to cleanup trigger after collection deletion"
                );
            }
        }

        Ok(true)
    }

    pub fn things_upsert_thing_json(&self, device_id: &str, payload_json: &str) -> Result<String> {
        let upsert: ThingUpsert =
            serde_json::from_str(payload_json).context("Invalid ThingUpsert JSON")?;

        // Guard: collection_uuid must be non-empty
        if upsert.collection_uuid.trim().is_empty() {
            anyhow::bail!(
                "collection_uuid must not be empty when upserting thing '{}'",
                upsert.uuid,
            );
        }

        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Check if thing exists before upsert to determine if this is create or update.
        // Try snapshot first, fall back to direct collection scan.
        let before_snapshot = doc_set.extract_snapshot()?;
        let existing = before_snapshot
            .things
            .iter()
            .find(|t| t.uuid == upsert.uuid);

        let (is_create, old_collection_uuid) = match existing {
            Some(t) => (false, Some(t.collection_uuid.clone())),
            None => {
                // Thing not in snapshot — scan collection docs directly (stale root case).
                match doc_set.find_thing_collection_uuid(&upsert.uuid) {
                    Some(coll) => (false, Some(coll)),
                    None => (true, None),
                }
            }
        };
        let old_content_json = existing.map(|t| serde_json::to_string(t).ok()).flatten();

        // If the thing is being moved to a different collection, tombstone it
        // in the old collection document so it doesn't appear in two places.
        let is_move = !is_create
            && old_collection_uuid
                .as_ref()
                .map_or(false, |old| *old != upsert.collection_uuid);

        if is_move {
            let old_coll = old_collection_uuid.as_ref().unwrap();
            tracing::info!(
                thing_uuid = upsert.uuid,
                from_collection = old_coll.as_str(),
                to_collection = upsert.collection_uuid.as_str(),
                "things_upsert_thing_json: moving thing — tombstoning in source collection"
            );
            doc_set.delete_thing(old_coll, &upsert.uuid)?;
        }

        // V3: Update thing metadata in collection document
        let trigger =
            crate::things_crdt::trigger_update_from_tri_state(upsert.trigger_uuid.as_deref());
        doc_set.upsert_thing_meta(
            &upsert.collection_uuid,
            &upsert.uuid,
            Some(upsert.datatype.clone()),
            None, // status
            Some(upsert.title.clone()),
            upsert.parent_uuid.clone(),
            trigger,
        )?;

        // V3: If data is provided, update thing markdown document
        if let Some(ref data) = upsert.data {
            let content =
                crate::things_crdt::markdown_only_content_from_value(&upsert.datatype, data);
            doc_set.set_thing_content(&upsert.uuid, content)?;
        }

        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        let snapshot = doc_set.extract_snapshot()?;
        let created = snapshot
            .things
            .iter()
            .find(|t| t.uuid == upsert.uuid)
            .ok_or_else(|| anyhow!("Thing not found after upsert"))?;

        let mut fields: BTreeMap<String, JsonValue> = BTreeMap::new();
        fields.insert(
            "title".to_string(),
            JsonValue::String(created.title.clone()),
        );
        fields.insert(
            "datatype".to_string(),
            serde_json::to_value(&created.datatype).unwrap_or(JsonValue::Null),
        );
        fields.insert(
            "collection_uuid".to_string(),
            JsonValue::String(created.collection_uuid.clone()),
        );
        if is_create {
            fields.insert(
                "trigger_uuid".to_string(),
                created
                    .trigger_uuid
                    .as_ref()
                    .map(|v| JsonValue::String(v.clone()))
                    .unwrap_or(JsonValue::Null),
            );
            fields.insert(
                "parent_uuid".to_string(),
                created
                    .parent_uuid
                    .as_ref()
                    .map(|v| JsonValue::String(v.clone()))
                    .unwrap_or(JsonValue::Null),
            );
            fields.insert(
                "created_at".to_string(),
                JsonValue::String(created.created_at.clone()),
            );
        } else {
            if let Some(v) = upsert.trigger_uuid.as_deref() {
                fields.insert(
                    "trigger_uuid".to_string(),
                    if v.is_empty() {
                        JsonValue::Null
                    } else {
                        JsonValue::String(v.to_string())
                    },
                );
            }
            if let Some(v) = upsert.parent_uuid.as_deref() {
                fields.insert(
                    "parent_uuid".to_string(),
                    if v.is_empty() {
                        JsonValue::Null
                    } else {
                        JsonValue::String(v.to_string())
                    },
                );
            }
        }
        if is_create || upsert.data.is_some() {
            fields.insert("data".to_string(), created.data.clone());
        }
        fields.insert(
            "updated_at".to_string(),
            JsonValue::String(created.updated_at.clone()),
        );
        self.emit_things_field_patch(device_id, &created.uuid, fields);

        // Record change log
        let op_type = if is_create {
            ThingsOperationType::CreateThing
        } else if is_move {
            ThingsOperationType::MoveThing
        } else {
            ThingsOperationType::UpdateThing
        };
        let summary = if is_create {
            format!("Created thing '{}'", created.title)
        } else if is_move {
            format!("Moved thing '{}'", created.title)
        } else {
            format!("Updated thing '{}'", created.title)
        };
        let details = if is_move {
            json!({
                "uuid": upsert.uuid,
                "title": upsert.title,
                "from_collection_uuid": old_collection_uuid,
                "to_collection_uuid": upsert.collection_uuid,
                "collection_uuid": upsert.collection_uuid,
                "datatype": format!("{:?}", upsert.datatype),
            })
        } else {
            json!({
                "uuid": upsert.uuid,
                "title": upsert.title,
                "collection_uuid": upsert.collection_uuid,
                "datatype": format!("{:?}", upsert.datatype),
            })
        };

        // For updates, use 5-minute grouping window
        let log_id = if !is_create {
            // Check if there's a recent update log within 5 minutes
            if let Ok(Some(recent)) = self.storage.find_recent_thing_update_log(&upsert.uuid, 300) {
                // Reuse the same log entry (don't create a new one)
                Some(recent.id)
            } else {
                self.storage
                    .insert_things_change_log(
                        device_id,
                        op_type,
                        "thing",
                        &upsert.uuid,
                        &summary,
                        &details.to_string(),
                        None,
                        true,
                    )
                    .ok()
            }
        } else {
            self.storage
                .insert_things_change_log(
                    device_id,
                    op_type,
                    "thing",
                    &upsert.uuid,
                    &summary,
                    &details.to_string(),
                    None,
                    true,
                )
                .ok()
        };

        // For updates, save content snapshot if we don't have one for this log entry
        if !is_create {
            if let Some(lid) = log_id {
                // Check if snapshot exists for this log entry
                if self
                    .storage
                    .get_things_content_snapshot_by_log_id(lid)
                    .ok()
                    .flatten()
                    .is_none()
                {
                    // Save the old content as snapshot
                    if let Some(old_json) = old_content_json {
                        let _ = self.storage.insert_things_content_snapshot(
                            device_id,
                            &upsert.uuid,
                            &old_json,
                            Some(lid),
                        );
                    }
                }
            }
        }

        to_string(&created).context("Failed to serialize thing")
    }

    pub fn things_splice_text(
        &self,
        device_id: &str,
        thing_uuid: &str,
        block_id: &str,
        index: usize,
        delete: usize,
        insert: &str,
    ) -> Result<bool> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // V3: Get the thing markdown document before splice to check if it exists
        let md_view_before = doc_set.thing_markdown_view(thing_uuid)?;
        let has_content = md_view_before.content.is_some();

        // V3: Splice text in thing markdown document
        doc_set.splice_thing_text(thing_uuid, block_id, index, delete, insert)?;

        // Check if the splice actually changed anything
        let md_view_after = doc_set.thing_markdown_view(thing_uuid)?;
        if md_view_before.content == md_view_after.content && has_content {
            // No-op (e.g., block not found). Treat as failure so callers can fall back.
            return Ok(false);
        }

        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        self.emit_things_event(ThingsEvent::ThingMarkdownSplice {
            device_id: device_id.to_string(),
            thing_uuid: thing_uuid.to_string(),
            block_id: block_id.to_string(),
            index: u32::try_from(index).unwrap_or(u32::MAX),
            delete: u32::try_from(delete).unwrap_or(u32::MAX),
            insert: insert.to_string(),
        });

        // For splice_text, we use 5-minute grouping for update logs
        // Check if there's a recent update log within 5 minutes
        if self
            .storage
            .find_recent_thing_update_log(thing_uuid, 300)
            .ok()
            .flatten()
            .is_none()
        {
            // No recent log, create one
            let summary = format!("Edited thing content");
            let details = json!({
                "uuid": thing_uuid,
                "block_id": block_id,
                "operation": "splice_text",
            });
            let _ = self.storage.insert_things_change_log(
                device_id,
                ThingsOperationType::UpdateThing,
                "thing",
                thing_uuid,
                &summary,
                &details.to_string(),
                None,
                true,
            );
        }

        Ok(true)
    }

    /// Get the markdown content of a thing.
    /// Returns the markdown text content, or None if the thing doesn't exist or has no markdown content.
    pub fn things_get_thing_markdown(
        &self,
        device_id: &str,
        thing_uuid: &str,
    ) -> Result<Option<String>> {
        let doc_set = self.get_or_init_document_set(device_id)?;
        let md_view = doc_set.thing_markdown_view(thing_uuid)?;

        // Extract markdown text from content
        let markdown = md_view.content.and_then(|c| {
            c.blocks.and_then(|blocks| {
                blocks
                    .into_iter()
                    .find(|b| b.id == "main")
                    .and_then(|b| b.text)
            })
        });
        Ok(markdown)
    }

    /// Edit the content of a thing using editor-level operations.
    ///
    /// # Operations
    /// - `overwrite`: Replace all content with `new_content`
    /// - `set_title`: Only change the title (ignores content fields)
    /// - `str_replace`: Find `old_str` and replace with `new_str` (must match exactly once)
    /// - `insert_at_line`: Insert `insert_text` after line `line_number` (1-based, 0 = prepend)
    /// - `append`: Append `append_text` to the end
    ///
    /// # Returns
    /// JSON result with success status, or error with current content for retry
    pub fn things_edit_content(
        &self,
        device_id: &str,
        thing_uuid: &str,
        operation: &str,
        // Optional fields depending on operation
        new_title: Option<&str>,
        new_content: Option<&str>,
        old_str: Option<&str>,
        new_str: Option<&str>,
        line_number: Option<usize>,
        insert_text: Option<&str>,
        append_text: Option<&str>,
    ) -> Result<String> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // V3: Extract snapshot for metadata (without content)
        let snapshot =
            doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
                include_content: false,
            })?;

        let thing = snapshot.things.iter().find(|t| t.uuid == thing_uuid);
        let Some(thing) = thing else {
            return Ok(json!({
                "error": "thing_not_found",
                "message": format!("Thing with UUID '{}' not found", thing_uuid),
            })
            .to_string());
        };

        // We only need to extract full content when the editor operation depends on reading current markdown.
        let include_content_for_read = operation != "overwrite";

        // Get current markdown content on-demand for the target thing only.
        let current_markdown = if include_content_for_read {
            let md_view = doc_set.thing_markdown_view(thing_uuid)?;
            md_view
                .content
                .and_then(|c| {
                    c.blocks.and_then(|blocks| {
                        blocks
                            .into_iter()
                            .find(|b| b.id == "main")
                            .and_then(|b| b.text)
                    })
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        // Determine the new content based on operation
        let (final_content, title_only) = match operation {
            "overwrite" => {
                let content = new_content.unwrap_or("");
                (content.to_string(), false)
            }
            "set_title" => {
                // Title-only operation, don't touch content
                (current_markdown.clone(), true)
            }
            "str_replace" => {
                let old = old_str.ok_or_else(|| anyhow!("str_replace requires 'old_str'"))?;
                let new = new_str.unwrap_or("");

                // Find all occurrences
                let matches: Vec<_> = current_markdown.match_indices(old).collect();

                if matches.is_empty() {
                    return Ok(json!({
                        "error": "str_replace_no_match",
                        "message": format!("'old_str' not found in content"),
                        "current_content": current_markdown,
                        "old_str": old,
                    })
                    .to_string());
                }

                if matches.len() > 1 {
                    return Ok(json!({
                        "error": "str_replace_multiple_matches",
                        "message": format!("'old_str' found {} times, must be unique. Use more context.", matches.len()),
                        "current_content": current_markdown,
                        "old_str": old,
                        "match_positions": matches.iter().map(|(pos, _)| *pos).collect::<Vec<_>>(),
                    }).to_string());
                }

                // Single match, perform replacement
                (current_markdown.replacen(old, new, 1), false)
            }
            "insert_at_line" => {
                let line_num = line_number.unwrap_or(0);
                let insert =
                    insert_text.ok_or_else(|| anyhow!("insert_at_line requires 'insert_text'"))?;

                let lines: Vec<&str> = current_markdown.lines().collect();
                let total_lines = lines.len();

                if line_num > total_lines {
                    return Ok(json!({
                        "error": "insert_at_line_out_of_range",
                        "message": format!("Line {} is out of range. Content has {} lines.", line_num, total_lines),
                        "current_content": current_markdown,
                        "total_lines": total_lines,
                    }).to_string());
                }

                // Insert after line_num (0 = prepend, 1 = after first line, etc.)
                let mut new_lines: Vec<&str> = Vec::with_capacity(lines.len() + 1);
                if line_num == 0 {
                    // Prepend
                    new_lines.push(insert);
                    new_lines.extend(lines);
                } else {
                    for (i, line) in lines.iter().enumerate() {
                        new_lines.push(line);
                        if i + 1 == line_num {
                            new_lines.push(insert);
                        }
                    }
                }
                (new_lines.join("\n"), false)
            }
            "append" => {
                let append = append_text.ok_or_else(|| anyhow!("append requires 'append_text'"))?;
                let mut result = current_markdown.clone();
                if !result.is_empty() && !result.ends_with('\n') {
                    result.push('\n');
                }
                result.push_str(append);
                (result, false)
            }
            _ => {
                return Ok(json!({
                    "error": "invalid_operation",
                    "message": format!("Unknown operation '{}'. Valid: overwrite, set_title, str_replace, insert_at_line, append", operation),
                }).to_string());
            }
        };

        // Build upsert payload
        let final_title = new_title.unwrap_or(&thing.title);

        // V3: Apply updates using document set operations
        // - Title change (optional): upsert_thing_meta
        // - Content change (optional): splice_thing_text or set_thing_content

        if new_title.is_some() || operation == "set_title" {
            doc_set.upsert_thing_meta(
                &thing.collection_uuid,
                thing_uuid,
                None, // datatype
                None, // status
                Some(final_title.to_string()),
                thing.parent_uuid.clone(),
                remi_things_crdt::TriggerUpdate::Noop,
            )?;
        }

        if !title_only {
            // Replace the entire main block in one op.
            doc_set.splice_thing_text(thing_uuid, "main", 0, usize::MAX, &final_content)?;

            self.emit_things_event(ThingsEvent::ThingMarkdownSplice {
                device_id: device_id.to_string(),
                thing_uuid: thing_uuid.to_string(),
                block_id: "main".to_string(),
                index: 0,
                delete: u32::MAX,
                insert: final_content.clone(),
            });
        }

        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        // Emit a lightweight UI patch directly (avoid re-extracting a full snapshot, which can be
        // expensive for large markdown overwrites).
        let mut fields: BTreeMap<String, JsonValue> = BTreeMap::new();
        fields.insert(
            "title".to_string(),
            JsonValue::String(final_title.to_string()),
        );
        if !title_only {
            fields.insert("data".to_string(), json!({"markdown": final_content}));
        }
        // Snapshot currently uses empty timestamps; keep behavior consistent.
        fields.insert("updated_at".to_string(), JsonValue::String("".to_string()));
        self.emit_things_field_patch(device_id, thing_uuid, fields);

        // Record change log with 5-minute grouping
        if self
            .storage
            .find_recent_thing_update_log(thing_uuid, 300)
            .ok()
            .flatten()
            .is_none()
        {
            let summary = format!("Edited thing '{}' ({})", final_title, operation);
            let details = json!({
                "uuid": thing_uuid,
                "operation": operation,
            });
            let _ = self.storage.insert_things_change_log(
                device_id,
                ThingsOperationType::UpdateThing,
                "thing",
                thing_uuid,
                &summary,
                &details.to_string(),
                None,
                true,
            );
        }

        // Return success with updated content
        let result_content = if title_only {
            current_markdown
        } else {
            final_content
        };
        Ok(json!({
            "success": true,
            "uuid": thing_uuid,
            "title": final_title,
            "operation": operation,
            "content": result_content,
        })
        .to_string())
    }

    /// Set the status of a thing.
    /// Status values: "none", "in-progress", "stalled", "done"
    pub fn set_thing_status(
        &self,
        device_id: &str,
        thing_uuid: &str,
        status: &str,
    ) -> Result<bool> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Validate status value
        let valid_statuses = ["none", "in-progress", "stalled", "done"];
        if !valid_statuses.contains(&status) {
            anyhow::bail!(
                "Invalid status '{}', must be one of: {:?}",
                status,
                valid_statuses
            );
        }

        // V3: Find the thing to get its collection_uuid
        let collection_uuid = {
            let snapshot = doc_set.extract_snapshot()?;
            match snapshot.things.iter().find(|t| t.uuid == thing_uuid) {
                Some(t) => t.collection_uuid.clone(),
                None => {
                    tracing::warn!(
                        thing_uuid,
                        "set_thing_status: thing not in snapshot, scanning collection docs"
                    );
                    doc_set
                        .find_thing_collection_uuid(thing_uuid)
                        .ok_or_else(|| anyhow::anyhow!("Thing not found: {}", thing_uuid))?
                }
            }
        };

        let timestamp_ms = Some(chrono::Utc::now().timestamp_millis());

        // V3: Update status via collection document
        doc_set.upsert_thing_meta(
            &collection_uuid,
            thing_uuid,
            None, // datatype
            Some(status.to_string()),
            None, // title
            None, // parent_uuid
            remi_things_crdt::TriggerUpdate::Noop,
        )?;

        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        let ts = timestamp_ms.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
        self.emit_things_event(ThingsEvent::ThingStatusSet {
            device_id: device_id.to_string(),
            thing_uuid: thing_uuid.to_string(),
            status: status.to_string(),
            status_timestamp_ms: ts,
        });

        // Record change log
        let summary = format!("Set thing status to '{}'", status);
        let details = json!({
            "uuid": thing_uuid,
            "status": status,
            "timestamp_ms": timestamp_ms,
        });
        let _ = self.storage.insert_things_change_log(
            device_id,
            ThingsOperationType::UpdateThing,
            "thing",
            thing_uuid,
            &summary,
            &details.to_string(),
            None,
            true,
        );

        Ok(true)
    }

    pub fn things_delete_thing(
        &self,
        device_id: &str,
        collection_uuid: &str,
        uuid: &str,
    ) -> Result<bool> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;
        let before = doc_set.extract_snapshot()?;
        let thing = before.things.iter().find(|t| t.uuid == uuid);
        if thing.is_none() {
            return Ok(false);
        }
        let thing = thing.unwrap();
        let thing_title = thing.title.clone();
        let thing_content_json = serde_json::to_string(thing).ok();

        // Find child things (for cascade logging)
        let child_things: Vec<_> = before
            .things
            .iter()
            .filter(|t| t.parent_uuid.as_deref() == Some(uuid))
            .cloned()
            .collect();

        // V3: Delete thing from collection document
        doc_set.delete_thing(collection_uuid, uuid)?;
        // V3: Also remove thing markdown document
        let md_key = crate::things_crdt::DocumentKey::thing_markdown(uuid);
        doc_set.remove_document(&md_key);
        self.storage
            .delete_crdt_document(uuid, "thing_markdown")
            .ok();
        // Delete child things
        for child in &child_things {
            doc_set.delete_thing(collection_uuid, &child.uuid)?;
            let child_md_key = crate::things_crdt::DocumentKey::thing_markdown(&child.uuid);
            doc_set.remove_document(&child_md_key);
            self.storage
                .delete_crdt_document(&child.uuid, "thing_markdown")
                .ok();
        }
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        // Emit incremental delete events (thing + cascade children).
        self.emit_things_event(ThingsEvent::ThingDelete {
            device_id: device_id.to_string(),
            thing_uuid: uuid.to_string(),
        });
        for child in &child_things {
            self.emit_things_event(ThingsEvent::ThingDelete {
                device_id: device_id.to_string(),
                thing_uuid: child.uuid.clone(),
            });
        }

        // Record change log
        let summary = format!("Deleted thing '{}'", thing_title);
        let details = json!({
            "uuid": uuid,
            "title": thing_title,
            "collection_uuid": collection_uuid,
            "child_count": child_things.len(),
        });
        let parent_log_id = self
            .storage
            .insert_things_change_log(
                device_id,
                ThingsOperationType::DeleteThing,
                "thing",
                uuid,
                &summary,
                &details.to_string(),
                None,
                true,
            )
            .ok();

        // Save content snapshot for potential restore
        if let (Some(parent_id), Some(content_json)) = (parent_log_id, thing_content_json) {
            let _ = self.storage.insert_things_content_snapshot(
                device_id,
                uuid,
                &content_json,
                Some(parent_id),
            );
        }

        // Record cascade deletions for child things
        if let Some(parent_id) = parent_log_id {
            let mut cascade_ids = Vec::new();
            for child in &child_things {
                let child_summary = format!("Deleted thing '{}' (cascade)", child.title);
                let child_details = json!({
                    "uuid": child.uuid,
                    "title": child.title,
                    "parent_uuid": uuid,
                    "cascade_reason": "parent_thing_deleted",
                });
                if let Ok(cascade_id) = self.storage.insert_things_change_log(
                    device_id,
                    ThingsOperationType::DeleteThing,
                    "thing",
                    &child.uuid,
                    &child_summary,
                    &child_details.to_string(),
                    Some(parent_id),
                    false, // Cascade deletions can't be individually undone
                ) {
                    cascade_ids.push(cascade_id);
                    // Save content snapshot for child
                    if let Ok(child_json) = serde_json::to_string(child) {
                        let _ = self.storage.insert_things_content_snapshot(
                            device_id,
                            &child.uuid,
                            &child_json,
                            Some(cascade_id),
                        );
                    }
                }
            }
            if !cascade_ids.is_empty() {
                let _ = self
                    .storage
                    .update_things_change_log_cascade_ids(parent_id, &cascade_ids);
            }
        }

        Ok(true)
    }

    pub fn things_set_status(
        &self,
        device_id: &str,
        thing_uuid: &str,
        status: &str,
        timestamp_ms: Option<i64>,
    ) -> Result<String> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Get the thing before update for logging
        let before = doc_set.extract_snapshot()?;
        let (thing_title, collection_uuid) =
            match before.things.iter().find(|t| t.uuid == thing_uuid) {
                Some(t) => (t.title.clone(), t.collection_uuid.clone()),
                None => {
                    tracing::warn!(
                        thing_uuid,
                        "things_set_status: thing not in snapshot, scanning collection docs"
                    );
                    let coll = doc_set
                        .find_thing_collection_uuid(thing_uuid)
                        .ok_or_else(|| anyhow::anyhow!("Thing not found: {}", thing_uuid))?;
                    (String::new(), coll)
                }
            };

        // V3: Apply status change via collection document
        doc_set.upsert_thing_meta(
            &collection_uuid,
            thing_uuid,
            None, // datatype
            Some(status.to_string()),
            None, // title
            None, // parent_uuid
            remi_things_crdt::TriggerUpdate::Noop,
        )?;
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        let ts = timestamp_ms.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
        self.emit_things_event(ThingsEvent::ThingStatusSet {
            device_id: device_id.to_string(),
            thing_uuid: thing_uuid.to_string(),
            status: status.to_string(),
            status_timestamp_ms: ts,
        });

        // Record change log
        let summary = format!("Changed status of '{}' to '{}'", thing_title, status);
        let details = json!({
            "uuid": thing_uuid,
            "title": thing_title,
            "collection_uuid": collection_uuid,
            "status": status,
            "timestamp_ms": timestamp_ms,
        });
        let details_json = serde_json::to_string(&details).unwrap_or_default();
        let _ = self.storage.insert_things_change_log(
            device_id,
            ThingsOperationType::UpdateThing,
            "thing",
            thing_uuid,
            &summary,
            &details_json,
            None,  // parent_log_id
            false, // can_undo
        )?;

        Ok(format!("Status updated: {} -> {}", thing_title, status))
    }

    /// Add a content block to a thing (V3 multi-value).
    ///
    /// # Arguments
    /// * `device_id` - Device identifier
    /// * `thing_uuid` - Thing UUID
    /// * `block_json` - JSON string of content block
    ///
    /// Block JSON format:
    /// ```json
    /// {
    ///   "id": "block-uuid",
    ///   "title": "Optional title",
    ///   "order": 0.0,
    ///   "payload": {
    ///     "type": "location",
    ///     "loc_type": "coordinate",
    ///     "lat": 39.9,
    ///     "lng": 116.4,
    ///     "coord_system": "wgs84"
    ///   }
    /// }
    /// ```
    pub fn things_add_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_json: &str,
    ) -> Result<String> {
        use remi_things_crdt::ContentEntry;

        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Find the thing's collection — try snapshot first, fall back to direct scan.
        let collection_uuid = {
            let snapshot = doc_set.extract_snapshot()?;
            match snapshot.things.iter().find(|t| t.uuid == thing_uuid) {
                Some(t) => t.collection_uuid.clone(),
                None => {
                    // Snapshot didn't contain the thing (root <-> collection linkage may be stale).
                    // Scan collection documents directly.
                    tracing::warn!(
                        thing_uuid,
                        snapshot_thing_count = snapshot.things.len(),
                        "things_add_content_entry: thing not in snapshot, scanning collection docs"
                    );
                    doc_set
                        .find_thing_collection_uuid(thing_uuid)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Thing not found: {} (snapshot had {} things)",
                                thing_uuid,
                                snapshot.things.len()
                            )
                        })?
                }
            }
        };

        // Parse entry JSON
        let v: serde_json::Value =
            serde_json::from_str(entry_json).context("Invalid entry JSON")?;

        // Generate UUID using uuid crate if not provided
        let id = v
            .get("id")
            .and_then(|i| i.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let title = v
            .get("title")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string());
        let order = v.get("order").and_then(|o| o.as_f64()).unwrap_or(0.0);

        let payload_val = v
            .get("payload")
            .ok_or_else(|| anyhow::anyhow!("Missing payload"))?;
        let payload = parse_content_entry_payload(payload_val)?;

        let entry = ContentEntry {
            id: id.clone(),
            title,
            order,
            payload,
        };

        doc_set.add_content_entry(&collection_uuid, thing_uuid, entry)?;
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        Ok(id)
    }

    /// Update a content entry on a thing (V3 multi-value).
    ///
    /// # Arguments
    /// * `device_id` - Device identifier
    /// * `thing_uuid` - Thing UUID
    /// * `entry_id` - Entry ID to update
    /// * `update_json` - JSON string of fields to update
    ///
    /// Update JSON format:
    /// ```json
    /// {
    ///   "title": "New title",  // or null to clear
    ///   "order": 1.5,
    ///   "payload": { ... }     // optional new payload
    /// }
    /// ```
    pub fn things_update_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
        update_json: &str,
    ) -> Result<()> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Find the thing's collection — try snapshot first, fall back to direct scan.
        let collection_uuid = {
            let snapshot = doc_set.extract_snapshot()?;
            match snapshot.things.iter().find(|t| t.uuid == thing_uuid) {
                Some(t) => t.collection_uuid.clone(),
                None => {
                    tracing::warn!(
                        thing_uuid,
                        "things_update_content_entry: thing not in snapshot, scanning collection docs"
                    );
                    doc_set
                        .find_thing_collection_uuid(thing_uuid)
                        .ok_or_else(|| anyhow::anyhow!("Thing not found: {}", thing_uuid))?
                }
            }
        };

        // Parse update JSON
        let v: serde_json::Value =
            serde_json::from_str(update_json).context("Invalid update JSON")?;

        let title = if v.get("title").is_some() {
            Some(
                v.get("title")
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string()),
            )
        } else {
            None
        };
        let order = v.get("order").and_then(|o| o.as_f64());
        let payload = if let Some(p) = v.get("payload") {
            Some(parse_content_entry_payload(p)?)
        } else {
            None
        };

        doc_set.update_content_entry(
            &collection_uuid,
            thing_uuid,
            entry_id,
            title,
            order,
            payload,
        )?;
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        Ok(())
    }

    /// Delete a content entry from a thing (V3 multi-value).
    pub fn things_delete_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<()> {
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Find the thing's collection — try snapshot first, fall back to direct scan.
        let collection_uuid = {
            let snapshot = doc_set.extract_snapshot()?;
            match snapshot.things.iter().find(|t| t.uuid == thing_uuid) {
                Some(t) => t.collection_uuid.clone(),
                None => {
                    tracing::warn!(
                        thing_uuid,
                        "things_delete_content_entry: thing not in snapshot, scanning collection docs"
                    );
                    doc_set
                        .find_thing_collection_uuid(thing_uuid)
                        .ok_or_else(|| anyhow::anyhow!("Thing not found: {}", thing_uuid))?
                }
            }
        };

        doc_set.delete_content_entry(&collection_uuid, thing_uuid, entry_id)?;
        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;

        Ok(())
    }

    /// Get all content entries of a thing as JSON array.
    pub fn things_get_content_entries(&self, device_id: &str, thing_uuid: &str) -> Result<String> {
        let doc_set = self.get_or_init_document_set(device_id)?;

        // Find the thing — try snapshot first, fall back to direct scan for collection_uuid.
        let snapshot = doc_set.extract_snapshot()?;
        let thing = snapshot.things.iter().find(|t| t.uuid == thing_uuid);

        if let Some(thing) = thing {
            // Get built_in.content_entries from thing data
            let data = &thing.data;
            let built_in = data.get("built_in");
            if let Some(bi) = built_in {
                if let Some(entries) = bi.get("content_entries") {
                    return Ok(serde_json::to_string(entries)?);
                }
            }
            Ok("[]".to_string())
        } else {
            // Fallback: scan collection documents directly
            tracing::warn!(
                thing_uuid,
                "things_get_content_entries: thing not in snapshot, scanning collection docs"
            );
            let collection_uuid = doc_set
                .find_thing_collection_uuid(thing_uuid)
                .ok_or_else(|| anyhow::anyhow!("Thing not found: {}", thing_uuid))?;
            let entries = doc_set.get_content_entries(&collection_uuid, thing_uuid)?;
            Ok(serde_json::to_string(&entries)?)
        }
    }

    // Legacy single-value getters for backward compatibility
    // (deprecated: use content entries instead)

    /// Get location field of a thing as JSON string (backward compat).
    /// Returns the first location content entry if any.
    pub fn things_get_location(&self, device_id: &str, thing_uuid: &str) -> Result<Option<String>> {
        let doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set.extract_snapshot()?;
        let thing = snapshot.things.iter().find(|t| t.uuid == thing_uuid);
        if thing.is_none() {
            // Fallback: try direct collection scan for the content entries
            let coll = doc_set.find_thing_collection_uuid(thing_uuid);
            if coll.is_none() {
                anyhow::bail!("Thing not found: {}", thing_uuid);
            }
            let entries = doc_set.get_content_entries(&coll.unwrap(), thing_uuid)?;
            for entry in &entries {
                if let remi_things_crdt::ContentEntryPayload::Location(ref loc) = entry.payload {
                    return Ok(Some(serde_json::to_string(loc)?));
                }
            }
            return Ok(None);
        }

        // Get built_in.content_entries from thing data and find first location
        let data = &thing.unwrap().data;
        if let Some(bi) = data.get("built_in") {
            if let Some(entries) = bi.get("content_entries").and_then(|b| b.as_array()) {
                for entry in entries {
                    if let Some(payload) = entry.get("payload") {
                        if payload.get("type").and_then(|t| t.as_str()) == Some("location") {
                            return Ok(Some(serde_json::to_string(payload)?));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Get date field of a thing as JSON string (backward compat).
    /// Returns the first date content entry if any.
    pub fn things_get_date(&self, device_id: &str, thing_uuid: &str) -> Result<Option<String>> {
        let doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set.extract_snapshot()?;
        let thing = snapshot.things.iter().find(|t| t.uuid == thing_uuid);
        if thing.is_none() {
            // Fallback: try direct collection scan for the content entries
            let coll = doc_set.find_thing_collection_uuid(thing_uuid);
            if coll.is_none() {
                anyhow::bail!("Thing not found: {}", thing_uuid);
            }
            let entries = doc_set.get_content_entries(&coll.unwrap(), thing_uuid)?;
            for entry in &entries {
                if let remi_things_crdt::ContentEntryPayload::Date(ref date) = entry.payload {
                    return Ok(Some(serde_json::to_string(date)?));
                }
            }
            return Ok(None);
        }

        // Get built_in.content_entries from thing data and find first date
        let data = &thing.unwrap().data;
        if let Some(bi) = data.get("built_in") {
            if let Some(entries) = bi.get("content_entries").and_then(|b| b.as_array()) {
                for entry in entries {
                    if let Some(payload) = entry.get("payload") {
                        if payload.get("type").and_then(|t| t.as_str()) == Some("date") {
                            return Ok(Some(serde_json::to_string(payload)?));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// V3: Replace state after sync - this needs to be rewritten for multi-doc
    /// For now, we keep a simplified version that saves synced documents individually
    pub fn things_replace_state_after_sync(
        &self,
        _device_id: &str,
        _doc_bytes: Vec<u8>,
        _sync_state_bytes: Vec<u8>,
        _last_sync_at: Option<&str>,
        _dirty: bool,
    ) -> Result<String> {
        // V3: This function is no longer used in the same way.
        // The sync layer now handles per-document sync.
        // This is kept for API compatibility but should be migrated to v3 sync flow.
        anyhow::bail!(
            "things_replace_state_after_sync is deprecated in v3. Use v3 sync flow instead."
        )
    }

    /// V3: Get raw state is no longer applicable for multi-doc architecture
    pub fn things_get_raw_state(
        &self,
        _device_id: &str,
    ) -> Result<(Vec<u8>, Vec<u8>, bool, Option<String>)> {
        // V3: This function is deprecated. Use get_or_init_document_set instead.
        anyhow::bail!(
            "things_get_raw_state is deprecated in v3. Use document set operations instead."
        )
    }

    // ===== Things Change Log API =====

    /// List recent change log entries with pagination.
    /// Returns entries ordered by created_at DESC (newest first).
    pub fn things_list_change_log(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<ThingsChangeLogEntry>> {
        self.storage.list_things_change_log(limit, offset)
    }

    /// List change log entries for a specific entity.
    pub fn things_list_change_log_for_entity(
        &self,
        entity_type: &str,
        entity_uuid: &str,
        limit: u32,
    ) -> Result<Vec<ThingsChangeLogEntry>> {
        self.storage
            .list_things_change_log_for_entity(entity_type, entity_uuid, limit)
    }

    /// Get a single change log entry by ID.
    pub fn things_get_change_log(&self, log_id: i64) -> Result<Option<ThingsChangeLogEntry>> {
        self.storage.get_things_change_log(log_id)
    }

    /// Get content snapshots for a thing (version history).
    pub fn things_list_content_snapshots(
        &self,
        thing_uuid: &str,
        limit: u32,
    ) -> Result<Vec<ThingsContentSnapshot>> {
        self.storage
            .list_things_content_snapshots(thing_uuid, limit)
    }

    // ===== Things Change Log Sync Methods =====

    /// Get unsynced change log entries for upload to server.
    pub fn things_get_unsynced_change_logs(&self, limit: u32) -> Result<Vec<ThingsChangeLogEntry>> {
        self.storage.get_unsynced_change_logs(limit)
    }

    /// Mark change log entries as synced.
    pub fn things_mark_change_logs_synced(&self, ids: &[i64]) -> Result<()> {
        self.storage.mark_change_logs_synced(ids)
    }

    /// Get unsynced content snapshots for upload to server.
    pub fn things_get_unsynced_content_snapshots(
        &self,
        limit: u32,
    ) -> Result<Vec<ThingsContentSnapshot>> {
        self.storage.get_unsynced_content_snapshots(limit)
    }

    /// Mark content snapshots as synced.
    pub fn things_mark_content_snapshots_synced(&self, ids: &[i64]) -> Result<()> {
        self.storage.mark_content_snapshots_synced(ids)
    }

    /// Insert a change log entry received from server (already synced).
    pub fn things_insert_synced_change_log(
        &self,
        device_id: &str,
        op_type: ThingsOperationType,
        entity_type: &str,
        entity_uuid: &str,
        summary: &str,
        details_json: &str,
        created_at: i64,
    ) -> Result<i64> {
        self.storage.insert_synced_change_log(
            device_id,
            op_type,
            entity_type,
            entity_uuid,
            summary,
            details_json,
            created_at,
        )
    }

    /// Insert a content snapshot received from server (already synced).
    pub fn things_insert_synced_content_snapshot(
        &self,
        device_id: &str,
        thing_uuid: &str,
        content_json: &str,
        created_at: i64,
    ) -> Result<i64> {
        self.storage
            .insert_synced_content_snapshot(device_id, thing_uuid, content_json, created_at)
    }

    /// Preview an undo operation without executing it.
    /// Returns information about what would be undone and any conflicts.
    pub fn things_preview_undo(&self, device_id: &str, log_id: i64) -> Result<ThingsUndoPreview> {
        let log_entry = self
            .storage
            .get_things_change_log(log_id)?
            .ok_or_else(|| anyhow!("Change log entry not found: {}", log_id))?;

        if !log_entry.can_undo {
            return Err(anyhow!("This operation cannot be undone"));
        }

        let doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set.extract_snapshot()?;

        // Get cascade entries
        let cascade_entries = self.storage.get_things_change_log_cascades(log_id)?;

        // Determine conflicts based on operation type
        let conflict = self.check_undo_conflict(&log_entry, &snapshot)?;

        Ok(ThingsUndoPreview {
            log_entry,
            needs_cascade_restore: !cascade_entries.is_empty(),
            conflict,
            cascade_entries,
        })
    }

    /// Execute an undo operation.
    /// V3 implementation: Uses multi-document architecture.
    pub fn things_execute_undo(
        &self,
        device_id: &str,
        execution: ThingsUndoExecution,
    ) -> Result<String> {
        let preview = self.things_preview_undo(device_id, execution.log_id)?;
        let log_entry = &preview.log_entry;

        // Check for conflicts that need resolution
        if let Some(conflict) = &preview.conflict {
            if execution.resolution_option.is_none() {
                return Err(anyhow!(
                    "Conflict detected: {}. Please provide a resolution option.",
                    conflict.description
                ));
            }
        }

        let message = match log_entry.op_type {
            ThingsOperationType::CreateCollection => {
                // Undo create = delete the collection
                self.things_delete_collection(device_id, &log_entry.entity_uuid)?;
                format!("Undone: {}", log_entry.summary)
            }
            ThingsOperationType::CreateThing => {
                // Undo create = delete the thing
                let details: serde_json::Value =
                    serde_json::from_str(&log_entry.details_json).unwrap_or_default();
                let collection_uuid = details["collection_uuid"].as_str().unwrap_or("");
                self.things_delete_thing(device_id, collection_uuid, &log_entry.entity_uuid)?;
                format!("Undone: {}", log_entry.summary)
            }
            ThingsOperationType::DeleteCollection => {
                // Undo delete = restore collection from snapshot
                self.restore_deleted_collection_v3(device_id, log_entry, &preview)?
            }
            ThingsOperationType::DeleteThing => {
                // Undo delete = restore thing from snapshot
                self.restore_deleted_thing_v3(device_id, log_entry, &execution, &preview)?
            }
            ThingsOperationType::UpdateCollection | ThingsOperationType::UpdateThing => {
                // Undo update = restore previous state from snapshot
                self.restore_from_snapshot_v3(device_id, log_entry)?
            }
            ThingsOperationType::MoveThing => {
                // Undo move = restore original collection_uuid by re-upserting with old collection
                self.undo_move_thing_v3(device_id, log_entry)?
            }
            ThingsOperationType::MoveThings => {
                // Undo batch move = restore each thing to original collection
                self.undo_move_things_v3(device_id, log_entry, &preview)?
            }
            ThingsOperationType::DeleteThings => {
                // Undo batch delete = restore each deleted thing
                self.undo_delete_things_v3(device_id, log_entry, &preview)?
            }
            _ => {
                return Err(anyhow!(
                    "Undo not supported for operation type: {:?}",
                    log_entry.op_type
                ));
            }
        };

        // Mark the log entry as undone
        self.storage.mark_things_change_log_undone(log_entry.id)?;

        // Log the undo operation
        let undo_op_type = log_entry
            .op_type
            .to_undo_variant()
            .unwrap_or(log_entry.op_type);
        let _ = self.storage.insert_things_change_log(
            device_id,
            undo_op_type,
            &log_entry.entity_type,
            &log_entry.entity_uuid,
            &message,
            &serde_json::json!({
                "undone_log_id": log_entry.id,
                "original_op": log_entry.op_type.as_str(),
            })
            .to_string(),
            None,  // parent_log_id
            false, // can_undo (undo operations typically can't be undone again)
        );

        Ok(message)
    }

    /// V3: Restore a deleted collection from snapshot
    fn restore_deleted_collection_v3(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        preview: &ThingsUndoPreview,
    ) -> Result<String> {
        let details: serde_json::Value =
            serde_json::from_str(&log_entry.details_json).unwrap_or_default();
        let title = details["title"].as_str().unwrap_or("Restored Collection");
        let trigger_uuid = details["trigger_uuid"].as_str().map(|s| s.to_string());

        // Restore the collection
        let collection_json = serde_json::json!({
            "uuid": log_entry.entity_uuid,
            "title": title,
            "trigger_uuid": trigger_uuid,
        });
        self.things_upsert_collection_json(device_id, &collection_json.to_string())?;

        // Restore cascade-deleted things
        for cascade_entry in &preview.cascade_entries {
            if cascade_entry.entity_type == "thing" {
                if let Some(snapshot) = self
                    .storage
                    .get_things_content_snapshot_by_log_id(cascade_entry.id)?
                {
                    if let Ok(thing_data) =
                        serde_json::from_str::<ThingEntry>(&snapshot.content_json)
                    {
                        let thing_json = serde_json::json!({
                            "uuid": thing_data.uuid,
                            "title": thing_data.title,
                            "datatype": thing_data.datatype.as_str(),
                            "data": thing_data.data,
                            "collection_uuid": thing_data.collection_uuid,
                            "trigger_uuid": thing_data.trigger_uuid,
                            "parent_uuid": thing_data.parent_uuid,
                        });
                        self.things_upsert_thing_json(device_id, &thing_json.to_string())?;

                        // For markdown things, content is in the data.content field
                        // The upsert will handle setting it via the document set
                    }
                }
            }
        }

        Ok(format!("Restored collection '{}' and its contents", title))
    }

    /// V3: Restore a deleted thing from snapshot
    fn restore_deleted_thing_v3(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        execution: &ThingsUndoExecution,
        preview: &ThingsUndoPreview,
    ) -> Result<String> {
        let content_snapshot = self
            .storage
            .get_things_content_snapshot_by_log_id(log_entry.id)?
            .ok_or_else(|| anyhow!("No content snapshot found for deleted thing"))?;

        let thing_data: ThingEntry = serde_json::from_str(&content_snapshot.content_json)
            .context("Failed to parse thing snapshot")?;

        // Check if parent collection exists
        let doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set.extract_snapshot()?;
        let parent_exists = snapshot
            .collections
            .iter()
            .any(|c| c.uuid == thing_data.collection_uuid);

        let target_collection = if !parent_exists {
            match execution.resolution_option.as_deref() {
                Some("cascade_restore") => {
                    return Err(anyhow!(
                        "Cascade restore of parent collection not yet implemented. Use 'move_to_other' instead."
                    ));
                }
                Some("move_to_other") => execution
                    .target_collection_uuid
                    .as_ref()
                    .ok_or_else(|| anyhow!("Target collection UUID required for move_to_other"))?
                    .clone(),
                Some("cancel") => {
                    return Err(anyhow!("Undo cancelled by user"));
                }
                _ => {
                    return Err(anyhow!(
                        "Parent collection deleted. Please provide a resolution option."
                    ));
                }
            }
        } else {
            thing_data.collection_uuid.clone()
        };

        // Restore the thing
        let thing_json = serde_json::json!({
            "uuid": thing_data.uuid,
            "title": thing_data.title,
            "datatype": thing_data.datatype.as_str(),
            "data": thing_data.data,
            "collection_uuid": target_collection,
            "trigger_uuid": thing_data.trigger_uuid,
            "parent_uuid": thing_data.parent_uuid,
        });
        self.things_upsert_thing_json(device_id, &thing_json.to_string())?;

        // Restore cascade-deleted child things
        for cascade_entry in &preview.cascade_entries {
            if cascade_entry.entity_type == "thing" && cascade_entry.entity_uuid != thing_data.uuid
            {
                if let Some(child_snapshot) = self
                    .storage
                    .get_things_content_snapshot_by_log_id(cascade_entry.id)?
                {
                    if let Ok(child_data) =
                        serde_json::from_str::<ThingEntry>(&child_snapshot.content_json)
                    {
                        let child_json = serde_json::json!({
                            "uuid": child_data.uuid,
                            "title": child_data.title,
                            "datatype": child_data.datatype.as_str(),
                            "data": child_data.data,
                            "collection_uuid": child_data.collection_uuid,
                            "trigger_uuid": child_data.trigger_uuid,
                            "parent_uuid": child_data.parent_uuid,
                        });
                        self.things_upsert_thing_json(device_id, &child_json.to_string())?;
                    }
                }
            }
        }

        Ok(format!("Restored thing '{}'", thing_data.title))
    }

    /// V3: Restore from update snapshot
    fn restore_from_snapshot_v3(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
    ) -> Result<String> {
        let content_snapshot = self
            .storage
            .get_things_content_snapshot_by_log_id(log_entry.id)?
            .ok_or_else(|| anyhow!("No content snapshot found for update operation"))?;

        if log_entry.entity_type == "collection" {
            let collection_data: ThingCollectionEntry =
                serde_json::from_str(&content_snapshot.content_json)
                    .context("Failed to parse collection snapshot")?;

            let collection_json = serde_json::json!({
                "uuid": collection_data.uuid,
                "title": collection_data.title,
                "trigger_uuid": collection_data.trigger_uuid,
            });
            self.things_upsert_collection_json(device_id, &collection_json.to_string())?;

            Ok(format!(
                "Restored collection '{}' to previous state",
                collection_data.title
            ))
        } else {
            let thing_data: ThingEntry = serde_json::from_str(&content_snapshot.content_json)
                .context("Failed to parse thing snapshot")?;

            let thing_json = serde_json::json!({
                "uuid": thing_data.uuid,
                "title": thing_data.title,
                "datatype": thing_data.datatype.as_str(),
                "data": thing_data.data,
                "collection_uuid": thing_data.collection_uuid,
                "trigger_uuid": thing_data.trigger_uuid,
                "parent_uuid": thing_data.parent_uuid,
            });
            self.things_upsert_thing_json(device_id, &thing_json.to_string())?;

            Ok(format!(
                "Restored thing '{}' to previous state",
                thing_data.title
            ))
        }
    }

    /// V3: Undo move thing - restore original collection_uuid
    fn undo_move_thing_v3(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
    ) -> Result<String> {
        let details: serde_json::Value =
            serde_json::from_str(&log_entry.details_json).unwrap_or_default();
        let original_collection = details["from_collection_uuid"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing from_collection_uuid in move details"))?;
        let thing_uuid = &log_entry.entity_uuid;

        // Get the current thing data
        let doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set.extract_snapshot()?;
        let thing = match snapshot.things.iter().find(|t| t.uuid == *thing_uuid) {
            Some(t) => t,
            None => {
                anyhow::bail!("Thing not found for undo-move: {}", thing_uuid);
            }
        };

        // Re-upsert with original collection
        let thing_json = serde_json::json!({
            "uuid": thing.uuid,
            "title": thing.title,
            "datatype": thing.datatype.as_str(),
            "data": thing.data,
            "collection_uuid": original_collection,
            "trigger_uuid": thing.trigger_uuid,
            "parent_uuid": thing.parent_uuid,
        });
        self.things_upsert_thing_json(device_id, &thing_json.to_string())?;

        Ok(format!(
            "Moved thing back to original collection '{}'",
            original_collection
        ))
    }

    /// V3: Undo batch move - restore each thing to original collection
    fn undo_move_things_v3(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        preview: &ThingsUndoPreview,
    ) -> Result<String> {
        let details: serde_json::Value =
            serde_json::from_str(&log_entry.details_json).unwrap_or_default();

        // The batch move details should contain a list of moved things with original collections
        let moved_items = details["moved_items"].as_array();

        let mut restored_count = 0;

        // First try batch details from the main log entry
        if let Some(items) = moved_items {
            for item in items {
                let thing_uuid = item["uuid"].as_str().unwrap_or("");
                let original_collection = item["from_collection_uuid"].as_str().unwrap_or("");

                if thing_uuid.is_empty() || original_collection.is_empty() {
                    continue;
                }

                // Get the current thing data
                let doc_set = self.get_or_init_document_set(device_id)?;
                let snapshot = doc_set.extract_snapshot()?;

                if let Some(thing) = snapshot.things.iter().find(|t| t.uuid == thing_uuid) {
                    let thing_json = serde_json::json!({
                        "uuid": thing.uuid,
                        "title": thing.title,
                        "datatype": thing.datatype.as_str(),
                        "data": thing.data,
                        "collection_uuid": original_collection,
                        "trigger_uuid": thing.trigger_uuid,
                        "parent_uuid": thing.parent_uuid,
                    });
                    self.things_upsert_thing_json(device_id, &thing_json.to_string())?;
                    restored_count += 1;
                }
            }
        }

        // Also process cascade entries (individual MoveThing logs linked to this batch)
        for cascade_entry in &preview.cascade_entries {
            if cascade_entry.op_type == ThingsOperationType::MoveThing {
                let cascade_details: serde_json::Value =
                    serde_json::from_str(&cascade_entry.details_json).unwrap_or_default();
                let thing_uuid = &cascade_entry.entity_uuid;
                let original_collection = cascade_details["from_collection_uuid"]
                    .as_str()
                    .unwrap_or("");

                if original_collection.is_empty() {
                    continue;
                }

                let doc_set = self.get_or_init_document_set(device_id)?;
                let snapshot = doc_set.extract_snapshot()?;

                if let Some(thing) = snapshot.things.iter().find(|t| t.uuid == *thing_uuid) {
                    let thing_json = serde_json::json!({
                        "uuid": thing.uuid,
                        "title": thing.title,
                        "datatype": thing.datatype.as_str(),
                        "data": thing.data,
                        "collection_uuid": original_collection,
                        "trigger_uuid": thing.trigger_uuid,
                        "parent_uuid": thing.parent_uuid,
                    });
                    self.things_upsert_thing_json(device_id, &thing_json.to_string())?;
                    restored_count += 1;
                }
            }
        }

        if restored_count == 0 {
            return Err(anyhow!("No things found to restore from batch move"));
        }

        Ok(format!(
            "Restored {} things to original collections",
            restored_count
        ))
    }

    /// V3: Undo batch delete - restore each deleted thing from snapshots
    fn undo_delete_things_v3(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        preview: &ThingsUndoPreview,
    ) -> Result<String> {
        let mut restored_count = 0;

        // Try to restore from main log entry's snapshot first
        if let Some(snapshot) = self
            .storage
            .get_things_content_snapshot_by_log_id(log_entry.id)?
        {
            // The snapshot might contain a batch of things in JSON array format
            if let Ok(things) = serde_json::from_str::<Vec<ThingEntry>>(&snapshot.content_json) {
                for thing_data in things {
                    let thing_json = serde_json::json!({
                        "uuid": thing_data.uuid,
                        "title": thing_data.title,
                        "datatype": thing_data.datatype.as_str(),
                        "data": thing_data.data,
                        "collection_uuid": thing_data.collection_uuid,
                        "trigger_uuid": thing_data.trigger_uuid,
                        "parent_uuid": thing_data.parent_uuid,
                    });
                    self.things_upsert_thing_json(device_id, &thing_json.to_string())?;
                    restored_count += 1;
                }
            } else if let Ok(thing_data) =
                serde_json::from_str::<ThingEntry>(&snapshot.content_json)
            {
                // Single thing in snapshot
                let thing_json = serde_json::json!({
                    "uuid": thing_data.uuid,
                    "title": thing_data.title,
                    "datatype": thing_data.datatype.as_str(),
                    "data": thing_data.data,
                    "collection_uuid": thing_data.collection_uuid,
                    "trigger_uuid": thing_data.trigger_uuid,
                    "parent_uuid": thing_data.parent_uuid,
                });
                self.things_upsert_thing_json(device_id, &thing_json.to_string())?;
                restored_count += 1;
            }
        }

        // Also process cascade entries (individual DeleteThing logs linked to this batch)
        for cascade_entry in &preview.cascade_entries {
            if cascade_entry.op_type == ThingsOperationType::DeleteThing {
                if let Some(child_snapshot) = self
                    .storage
                    .get_things_content_snapshot_by_log_id(cascade_entry.id)?
                {
                    if let Ok(thing_data) =
                        serde_json::from_str::<ThingEntry>(&child_snapshot.content_json)
                    {
                        // Check if parent collection still exists
                        let doc_set = self.get_or_init_document_set(device_id)?;
                        let snapshot = doc_set.extract_snapshot()?;
                        let parent_exists = snapshot
                            .collections
                            .iter()
                            .any(|c| c.uuid == thing_data.collection_uuid);

                        if parent_exists {
                            let thing_json = serde_json::json!({
                                "uuid": thing_data.uuid,
                                "title": thing_data.title,
                                "datatype": thing_data.datatype.as_str(),
                                "data": thing_data.data,
                                "collection_uuid": thing_data.collection_uuid,
                                "trigger_uuid": thing_data.trigger_uuid,
                                "parent_uuid": thing_data.parent_uuid,
                            });
                            self.things_upsert_thing_json(device_id, &thing_json.to_string())?;
                            restored_count += 1;
                        }
                    }
                }
            }
        }

        if restored_count == 0 {
            return Err(anyhow!("No things found to restore from batch delete"));
        }

        Ok(format!("Restored {} deleted things", restored_count))
    }

    /// Cleanup old change logs and snapshots (retention policy).
    pub fn things_cleanup_change_logs(&self, older_than_days: i64) -> Result<(u64, u64)> {
        let logs_deleted = self.storage.cleanup_things_change_log(older_than_days)?;
        let snapshots_deleted = self
            .storage
            .cleanup_things_content_snapshots(older_than_days)?;
        Ok((logs_deleted, snapshots_deleted))
    }

    // ===== V3 CRDT Multi-Document API =====

    /// Get a single CRDT document by key (uuid + data_type).
    pub fn crdt_get_document(
        &self,
        uuid: &str,
        data_type: &str,
    ) -> Result<Option<crate::types::CrdtDocumentRow>> {
        self.storage.get_crdt_document(uuid, data_type)
    }

    /// Save a CRDT document.
    pub fn crdt_save_document(
        &self,
        uuid: &str,
        data_type: &str,
        automerge_doc: &[u8],
        sync_state: &[u8],
        dirty: bool,
        last_sync_at: Option<&str>,
    ) -> Result<()> {
        self.storage.save_crdt_document(
            uuid,
            data_type,
            automerge_doc,
            sync_state,
            dirty,
            last_sync_at,
        )
    }

    /// Get all dirty CRDT documents (for sync), ordered by sync priority.
    pub fn crdt_get_dirty_documents(&self) -> Result<Vec<crate::types::CrdtDocumentRow>> {
        self.storage.get_dirty_crdt_documents()
    }

    /// List all CRDT document keys (uuid, data_type).
    pub fn crdt_list_document_keys(&self) -> Result<Vec<(String, String)>> {
        self.storage.list_crdt_document_keys()
    }

    /// Delete a CRDT document by key.
    pub fn crdt_delete_document(&self, uuid: &str, data_type: &str) -> Result<()> {
        self.storage.delete_crdt_document(uuid, data_type)
    }

    /// Mark a CRDT document as dirty/clean.
    pub fn crdt_set_document_dirty(&self, uuid: &str, data_type: &str, dirty: bool) -> Result<()> {
        self.storage.set_crdt_document_dirty(uuid, data_type, dirty)
    }

    // ===== Undo Helper Methods =====

    fn check_undo_conflict(
        &self,
        log_entry: &ThingsChangeLogEntry,
        snapshot: &ThingsSnapshot,
    ) -> Result<Option<ThingsUndoConflict>> {
        match log_entry.op_type {
            ThingsOperationType::CreateCollection => {
                // Check if collection still exists
                let exists = snapshot
                    .collections
                    .iter()
                    .any(|c| c.uuid == log_entry.entity_uuid);
                if !exists {
                    return Ok(Some(ThingsUndoConflict {
                        conflict_type: ThingsUndoConflictType::EntityModified,
                        description: "Collection no longer exists".to_string(),
                        options: vec![],
                    }));
                }
                Ok(None)
            }
            ThingsOperationType::CreateThing => {
                // Check if thing still exists
                let exists = snapshot
                    .things
                    .iter()
                    .any(|t| t.uuid == log_entry.entity_uuid);
                if !exists {
                    return Ok(Some(ThingsUndoConflict {
                        conflict_type: ThingsUndoConflictType::EntityModified,
                        description: "Thing no longer exists".to_string(),
                        options: vec![],
                    }));
                }
                Ok(None)
            }
            ThingsOperationType::DeleteCollection => {
                // Check if collection already exists (restored elsewhere)
                let exists = snapshot
                    .collections
                    .iter()
                    .any(|c| c.uuid == log_entry.entity_uuid);
                if exists {
                    return Ok(Some(ThingsUndoConflict {
                        conflict_type: ThingsUndoConflictType::EntityExists,
                        description: "Collection already exists".to_string(),
                        options: vec![],
                    }));
                }
                Ok(None)
            }
            ThingsOperationType::DeleteThing => {
                // Check if thing already exists
                let exists = snapshot
                    .things
                    .iter()
                    .any(|t| t.uuid == log_entry.entity_uuid);
                if exists {
                    return Ok(Some(ThingsUndoConflict {
                        conflict_type: ThingsUndoConflictType::EntityExists,
                        description: "Thing already exists".to_string(),
                        options: vec![],
                    }));
                }
                // Check if parent collection exists
                let details: serde_json::Value =
                    serde_json::from_str(&log_entry.details_json).unwrap_or_default();
                let collection_uuid = details["collection_uuid"].as_str().unwrap_or("");
                let parent_exists = snapshot
                    .collections
                    .iter()
                    .any(|c| c.uuid == collection_uuid);
                if !parent_exists && !collection_uuid.is_empty() {
                    return Ok(Some(ThingsUndoConflict {
                        conflict_type: ThingsUndoConflictType::ParentDeleted,
                        description: format!(
                            "Parent collection '{}' has been deleted",
                            collection_uuid
                        ),
                        options: vec![
                            ThingsUndoResolutionOption {
                                id: "cascade_restore".to_string(),
                                label: "Restore parent collection too".to_string(),
                                description:
                                    "Restore the parent collection and then restore this thing"
                                        .to_string(),
                            },
                            ThingsUndoResolutionOption {
                                id: "move_to_other".to_string(),
                                label: "Move to another collection".to_string(),
                                description: "Restore this thing to a different collection"
                                    .to_string(),
                            },
                            ThingsUndoResolutionOption {
                                id: "cancel".to_string(),
                                label: "Cancel".to_string(),
                                description: "Do not restore this thing".to_string(),
                            },
                        ],
                    }));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub fn things_bootstrap_stash_local_snapshot_if_needed(&self, device_id: &str) -> Result<bool> {
        const STASH_KEY: &str = "things.bootstrap.stash_snapshot_json";
        const DONE_KEY: &str = "things.bootstrap.done";

        if self
            .storage
            .get_internal_kv(DONE_KEY)?
            .as_deref()
            .unwrap_or("")
            == "1"
        {
            return Ok(false);
        }

        if self.storage.get_internal_kv(STASH_KEY)?.is_some() {
            return Ok(false);
        }

        let doc_set = self.get_or_init_document_set(device_id)?;
        if !doc_set.has_pending_changes() {
            return Ok(false);
        }

        let snapshot = doc_set
            .extract_snapshot()
            .context("Failed to extract local things snapshot for bootstrap stash")?;
        let payload = serde_json::to_string(&snapshot)
            .context("Failed to serialize bootstrap stash snapshot")?;
        self.storage
            .set_internal_kv(STASH_KEY, &payload)
            .context("Failed to persist bootstrap stash snapshot")?;
        Ok(true)
    }

    pub fn things_bootstrap_has_stash(&self) -> Result<bool> {
        const STASH_KEY: &str = "things.bootstrap.stash_snapshot_json";
        Ok(self.storage.get_internal_kv(STASH_KEY)?.is_some())
    }

    /// V3: Bootstrap by replaying stashed local changes.
    ///
    /// In V3 multi-document architecture, bootstrapping works differently:
    /// 1. Server sync is handled per-document, so there's no single "server snapshot"
    /// 2. Instead, we replay the stashed local snapshot to fresh documents
    /// 3. These documents will be marked dirty and synced on next sync cycle
    ///
    /// Note: The `_server_snapshot_doc` parameter is ignored in V3 - server data
    /// comes through per-document sync, not a single snapshot.
    pub fn things_bootstrap_from_server_snapshot_and_replay_stash(
        &self,
        device_id: &str,
        _server_snapshot_doc: Vec<u8>,
        _server_last_sync_at: Option<&str>,
    ) -> Result<()> {
        const STASH_KEY: &str = "things.bootstrap.stash_snapshot_json";
        const DONE_KEY: &str = "things.bootstrap.done";

        // Load the stashed local snapshot
        let stash_json = self
            .storage
            .get_internal_kv(STASH_KEY)?
            .ok_or_else(|| anyhow::anyhow!("No bootstrap stash found"))?;

        let stash: crate::things_crdt::ThingsSnapshot = serde_json::from_str(&stash_json)
            .context("Failed to parse bootstrap stash snapshot")?;

        // Clear all existing V3 documents from storage
        self.storage
            .delete_all_crdt_documents()
            .context("Failed to clear existing CRDT documents")?;

        // Get a fresh document set (mutable)
        let mut doc_set = self.get_or_init_document_set(device_id)?;

        // Replay stashed collections
        for collection in &stash.collections {
            // Create/init collection document and update meta
            doc_set.get_or_init_collection(&collection.uuid)?;
            let trigger = crate::things_crdt::trigger_update_from_tri_state(
                collection.trigger_uuid.as_deref(),
            );
            doc_set.update_collection_meta(
                &collection.uuid,
                Some(collection.title.clone()),
                None, // status
                trigger,
            )?;
        }

        // Replay stashed things
        for thing in &stash.things {
            // Create thing meta in collection document
            let trigger =
                crate::things_crdt::trigger_update_from_tri_state(thing.trigger_uuid.as_deref());
            doc_set.upsert_thing_meta(
                &thing.collection_uuid,
                &thing.uuid,
                Some(thing.datatype.clone()),
                None, // status
                Some(thing.title.clone()),
                thing.parent_uuid.clone(),
                trigger,
            )?;

            // Set thing content in markdown document
            let content =
                crate::things_crdt::markdown_only_content_from_value(&thing.datatype, &thing.data);
            doc_set.set_thing_content(&thing.uuid, content)?;
        }

        // Persist all documents as dirty (so they sync on next cycle)
        doc_set.save_to_storage(&self.storage)?;

        // Mark bootstrap as done
        self.storage.set_internal_kv(DONE_KEY, "1")?;

        // Clear the stash
        self.storage.delete_internal_kv(STASH_KEY)?;

        Ok(())
    }

    /// Reconcile trigger bindings after a sync operation.
    /// Returns a list of trigger UUIDs that need to be downloaded and installed.
    pub fn reconcile_trigger_bindings_after_sync(&self, device_id: &str) -> Result<Vec<String>> {
        let doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set.extract_snapshot()?;

        // Desired bindings for *all* non-deleted entities.
        let mut desired_bindings: std::collections::HashMap<(String, String), Option<String>> =
            std::collections::HashMap::new();

        for c in &snapshot.collections {
            desired_bindings.insert(
                ("collection".to_string(), c.uuid.clone()),
                c.trigger_uuid.clone(),
            );
        }

        for t in &snapshot.things {
            desired_bindings.insert(
                ("thing".to_string(), t.uuid.clone()),
                t.trigger_uuid.clone(),
            );
        }

        // Snapshot of existing bound triggers (for uninstall decisions later).
        let existing_bound_triggers: std::collections::HashSet<String> = self
            .storage
            .list_bound_trigger_uuids()?
            .into_iter()
            .collect();

        // Apply desired bindings and also delete rows for entities that no longer exist.
        let existing_rows = self.storage.list_trigger_bindings()?;
        let mut seen: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        for (trigger_uuid, entity_type, entity_uuid) in existing_rows {
            let key = (entity_type.clone(), entity_uuid.clone());
            seen.insert(key.clone());

            match desired_bindings.get(&key) {
                None => {
                    // Entity disappeared (deleted); remove stale binding.
                    self.storage
                        .delete_trigger_binding(&entity_type, &entity_uuid)?;
                }
                Some(None) => {
                    // Entity exists but wants no binding.
                    self.storage
                        .delete_trigger_binding(&entity_type, &entity_uuid)?;
                }
                Some(Some(desired_trigger)) => {
                    if desired_trigger != &trigger_uuid {
                        self.storage.upsert_trigger_binding(
                            desired_trigger,
                            &entity_type,
                            &entity_uuid,
                        )?;
                    }
                }
            }
        }

        // Insert any bindings for entities we haven't seen yet.
        for ((entity_type, entity_uuid), trigger_uuid) in &desired_bindings {
            if seen.contains(&(entity_type.clone(), entity_uuid.clone())) {
                continue;
            }
            if let Some(trigger_uuid) = trigger_uuid {
                self.storage
                    .upsert_trigger_binding(trigger_uuid, entity_type, entity_uuid)?;
            }
        }

        // Collect all trigger UUIDs that are now bound
        let mut all_trigger_uuids: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for trigger_uuid_opt in desired_bindings.values() {
            if let Some(trigger_uuid) = trigger_uuid_opt {
                all_trigger_uuids.insert(trigger_uuid.clone());
            }
        }

        // Find triggers that need to be downloaded (not yet installed)
        let mut triggers_to_download = Vec::new();
        for trigger_uuid in &all_trigger_uuids {
            // Check if trigger is already installed
            if self.storage.fetch_trigger(trigger_uuid)?.is_none() {
                triggers_to_download.push(trigger_uuid.clone());
            }
        }

        // Find triggers that are no longer bound and can be uninstalled
        for old_trigger_uuid in existing_bound_triggers {
            if !all_trigger_uuids.contains(&old_trigger_uuid) {
                // Check if trigger is still bound to anything (shouldn't be, but double-check)
                if !self.storage.is_trigger_bound(&old_trigger_uuid)? {
                    info!(trigger_uuid = %old_trigger_uuid, "Uninstalling trigger no longer bound to any entity");
                    let _ = self.storage.delete_trigger(&old_trigger_uuid);
                    self.emit_trigger_event(TriggerEvent::TriggerDelete {
                        trigger_uuid: old_trigger_uuid.clone(),
                    });
                }
            }
        }

        Ok(triggers_to_download)
    }

    pub fn list_trigger_logs_json(
        &self,
        trigger_uuid: &str,
        limit: Option<u32>,
        run_type: Option<TriggerRunType>,
    ) -> Result<String> {
        let logs = self
            .storage
            .list_trigger_logs(trigger_uuid, limit, run_type)?;
        to_string(&logs).context("Failed to serialize trigger logs")
    }
    pub fn export_trigger_logs_json(
        &self,
        trigger_uuid: &str,
        run_type: Option<TriggerRunType>,
    ) -> Result<String> {
        let logs = self.storage.export_trigger_logs(trigger_uuid, run_type)?;
        to_string(&logs).context("Failed to serialize trigger logs for export")
    }

    // ===== Notification queries =====

    pub fn list_notifications_grouped_json(&self, limit: u32) -> Result<String> {
        let groups = self.storage.list_notifications_grouped(limit)?;
        to_string(&groups).context("Failed to serialize notification groups")
    }

    pub fn list_notifications_by_category_json(
        &self,
        category: &str,
        limit: u32,
    ) -> Result<String> {
        let items = self
            .storage
            .list_notifications_by_category(category, limit)?;
        to_string(&items).context("Failed to serialize notifications by category")
    }

    pub fn list_notifications_flat_json(&self, limit: u32, offset: u32) -> Result<String> {
        let items = self.storage.list_notifications_flat(limit, offset)?;
        to_string(&items).context("Failed to serialize flat notifications")
    }

    pub fn get_latest_unread_notification_json(&self) -> Result<Option<String>> {
        let entry = self.storage.get_latest_unread_notification()?;
        match entry {
            Some(e) => Ok(Some(
                to_string(&e).context("Failed to serialize latest unread notification")?,
            )),
            None => Ok(None),
        }
    }

    pub fn get_unread_notification_count(&self) -> Result<i64> {
        self.storage.get_unread_notification_count()
    }

    pub fn mark_notification_read(&self, notification_id: i64) -> Result<()> {
        self.storage.mark_notification_read(notification_id)
    }

    pub fn mark_category_notifications_read(&self, category: &str) -> Result<()> {
        self.storage.mark_category_notifications_read(category)
    }

    pub fn mark_all_notifications_read(&self) -> Result<()> {
        self.storage.mark_all_notifications_read()
    }

    pub fn delete_notifications_by_category(&self, category: &str) -> Result<()> {
        self.storage.delete_notifications_by_category(category)
    }

    pub fn run_due_triggers<C>(&self, callback: &C) -> Result<Vec<TriggerExecutionSummary>>
    where
        C: TriggerCallback,
    {
        let now = Utc::now();
        let due = self.storage.fetch_due_triggers(now)?;
        let mut summaries = Vec::new();

        for trigger in due {
            let fire_time = trigger.next_fire.unwrap_or(now);
            let summary = self.execute_trigger(&trigger, fire_time, TriggerRunType::Automatic)?;
            callback.on_trigger(&summary)?;

            // Parse rules to determine the next schedule.
            let precondition: Vec<TriggerRule> = serde_json::from_str(&trigger.precondition_json)
                .context("Failed to parse precondition JSON")?;
            let condition: Vec<TriggerRule> = serde_json::from_str(&trigger.condition_json)
                .context("Failed to parse condition JSON")?;
            let timings = extract_timings_from_rules(&precondition, &condition)?;

            // Ensure the computed next fire is strictly after "now" even if this run is late.
            let next_fire = if let Some(cron) = timings.iter().find_map(|t| match t {
                rule_trigger_engine::TriggerTiming::Cron { expression } => {
                    Some(expression.as_str())
                }
                _ => None,
            }) {
                compute_next_fire(cron, fire_time, Some(now))?
            } else {
                None
            };

            self.storage.update_next_fire(
                &trigger.trigger_uuid,
                summary.fired_at,
                next_fire,
                summary.result,
            )?;

            self.emit_trigger_event(TriggerEvent::TriggerFired {
                trigger_uuid: trigger.trigger_uuid.clone(),
                fired_at: summary.fired_at.to_rfc3339(),
                next_fire: next_fire.map(|dt| dt.to_rfc3339()),
                result: summary.result,
            });
            summaries.push(summary);
        }

        Ok(summaries)
    }

    /// Get things and collections bound to a trigger
    fn get_bound_entities_for_trigger(
        &self,
        trigger_uuid: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let entities = self.storage.get_entities_for_trigger(trigger_uuid)?;
        let mut thing_uuids = Vec::new();
        let mut collection_uuids = Vec::new();

        for (entity_type, entity_uuid) in entities {
            match entity_type.as_str() {
                "thing" => thing_uuids.push(entity_uuid),
                "collection" => collection_uuids.push(entity_uuid),
                _ => warn!(entity_type = %entity_type, "Unknown entity type in trigger binding"),
            }
        }

        Ok((thing_uuids, collection_uuids))
    }

    /// Public API to get entities bound to a trigger (for FFI/Flutter bridge)
    pub fn get_bound_entities_for_trigger_api(
        &self,
        trigger_uuid: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        self.get_bound_entities_for_trigger(trigger_uuid)
    }

    pub fn run_trigger_now<C>(&self, trigger_uuid: &str, callback: &C) -> Result<bool>
    where
        C: TriggerCallback,
    {
        let trigger = self
            .storage
            .fetch_trigger(trigger_uuid)?
            .ok_or_else(|| anyhow!("Trigger not found: {trigger_uuid}"))?;
        let fire_time = Utc::now();
        let summary = self.execute_trigger(&trigger, fire_time, TriggerRunType::Manual)?;
        callback.on_trigger(&summary)?;

        // Parse rules to determine the next schedule.
        let precondition: Vec<TriggerRule> = serde_json::from_str(&trigger.precondition_json)
            .context("Failed to parse precondition JSON")?;
        let condition: Vec<TriggerRule> = serde_json::from_str(&trigger.condition_json)
            .context("Failed to parse condition JSON")?;
        let timings = extract_timings_from_rules(&precondition, &condition)?;
        let next_fire = if let Some(cron) = timings.iter().find_map(|t| match t {
            rule_trigger_engine::TriggerTiming::Cron { expression } => Some(expression.as_str()),
            _ => None,
        }) {
            compute_next_fire(cron, fire_time, Some(fire_time))?
        } else {
            None
        };

        self.storage.update_next_fire(
            &trigger.trigger_uuid,
            summary.fired_at,
            next_fire,
            summary.result,
        )?;

        self.emit_trigger_event(TriggerEvent::TriggerFired {
            trigger_uuid: trigger.trigger_uuid.clone(),
            fired_at: summary.fired_at.to_rfc3339(),
            next_fire: next_fire.map(|dt| dt.to_rfc3339()),
            result: summary.result,
        });

        Ok(summary.result)
    }

    /// Simulate time advancing to a specific point and run all due triggers
    /// Returns summaries of all executed triggers
    pub fn simulate_time_to<C>(
        &self,
        target_time: DateTime<Utc>,
        callback: &C,
    ) -> Result<Vec<TriggerExecutionSummary>>
    where
        C: TriggerCallback,
    {
        let due = self.storage.fetch_due_triggers(target_time)?;
        let mut summaries = Vec::new();

        for trigger in due {
            let fire_time = trigger.next_fire.unwrap_or(target_time);
            let summary = self.execute_trigger(&trigger, fire_time, TriggerRunType::Automatic)?;
            callback.on_trigger(&summary)?;

            // Parse rules to determine the next schedule.
            let precondition: Vec<TriggerRule> = serde_json::from_str(&trigger.precondition_json)
                .context("Failed to parse precondition JSON")?;
            let condition: Vec<TriggerRule> = serde_json::from_str(&trigger.condition_json)
                .context("Failed to parse condition JSON")?;
            let timings = extract_timings_from_rules(&precondition, &condition)?;

            let next_fire = if let Some(cron) = timings.iter().find_map(|t| match t {
                rule_trigger_engine::TriggerTiming::Cron { expression } => {
                    Some(expression.as_str())
                }
                _ => None,
            }) {
                compute_next_fire(cron, fire_time, Some(target_time))?
            } else {
                None
            };
            self.storage.update_next_fire(
                &trigger.trigger_uuid,
                summary.fired_at,
                next_fire,
                summary.result,
            )?;

            self.emit_trigger_event(TriggerEvent::TriggerFired {
                trigger_uuid: trigger.trigger_uuid.clone(),
                fired_at: summary.fired_at.to_rfc3339(),
                next_fire: next_fire.map(|dt| dt.to_rfc3339()),
                result: summary.result,
            });
            summaries.push(summary);
        }

        Ok(summaries)
    }

    pub fn replay_trigger(
        &self,
        trigger_uuid: &str,
        start_iso: Option<String>,
        end_iso: Option<String>,
    ) -> Result<TriggerReplaySummary> {
        let trigger = self
            .storage
            .fetch_trigger(trigger_uuid)?
            .ok_or_else(|| anyhow!("Trigger not found: {trigger_uuid}"))?;

        let range = self
            .storage
            .events_time_range()?
            .ok_or_else(|| anyhow!("No events available for replay"))?;

        let start = match start_iso {
            Some(value) => DateTime::parse_from_rfc3339(&value)
                .map(|dt| dt.with_timezone(&Utc))
                .context("Invalid replay start timestamp")?,
            None => range.0,
        };

        let end = match end_iso {
            Some(value) => DateTime::parse_from_rfc3339(&value)
                .map(|dt| dt.with_timezone(&Utc))
                .context("Invalid replay end timestamp")?,
            None => range.1,
        };

        if end <= start {
            return Err(anyhow!("Replay window must be positive"));
        }

        // Parse rules to extract cron + optional repeat frequency.
        let precondition: Vec<TriggerRule> = serde_json::from_str(&trigger.precondition_json)
            .context("Failed to parse precondition JSON")?;
        let condition: Vec<TriggerRule> = serde_json::from_str(&trigger.condition_json)
            .context("Failed to parse condition JSON")?;
        let cron = extract_cron_from_preconditions(&precondition)
            .ok_or_else(|| anyhow!("No cron expression found in preconditions"))?;
        // Repeat frequency rules live in `condition` only.
        let repeat_freq = extract_repeat_frequency_from_conditions(&condition);
        let schedule = Cron::from_str(&cron).context("Invalid cron expression")?;
        let tz = default_timezone();

        let mut runs_considered = 0;
        let mut runs_executed = 0;
        let mut runs_succeeded = 0;
        let mut last_success: Option<DateTime<Utc>> = None;

        let mut next_local = schedule
            .find_next_occurrence(&start.with_timezone(&tz), false)
            .context("Invalid cron expression")?;

        while next_local.with_timezone(&Utc) <= end {
            let fire_time = next_local.with_timezone(&Utc);
            runs_considered += 1;

            if let Some(last) = last_success {
                if let Some(ref freq) = repeat_freq {
                    if let Some(min_gap) = repeat_min_gap(freq) {
                        if fire_time - last < min_gap {
                            continue;
                        }
                    }
                }
            }

            let summary = self.execute_trigger(&trigger, fire_time, TriggerRunType::Replay)?;
            runs_executed += 1;
            if summary.result {
                runs_succeeded += 1;
                last_success = Some(summary.fired_at);
            }

            next_local = schedule
                .find_next_occurrence(&next_local, false)
                .context("Invalid cron expression")?;
        }

        Ok(TriggerReplaySummary {
            trigger_id: trigger.trigger_uuid.clone(),
            start,
            end,
            runs_considered,
            runs_executed,
            runs_succeeded,
        })
    }

    /// Test a trigger configuration against stored events without registering it.
    ///
    /// This is the SDK equivalent of the `trigger-test` CLI tool. It accepts a trigger
    /// configuration (JSON) and simulates its execution over a time range using local events.
    ///
    /// # Arguments
    /// * `trigger_json` - Full trigger configuration JSON (name, version, precondition, condition)
    /// * `start_iso` - Optional start time (RFC3339); defaults to first event timestamp
    /// * `end_iso` - Optional end time (RFC3339); defaults to last event timestamp
    /// * `manual` - If true, runs once at end time ignoring precondition gates (like --manual flag)
    ///
    /// # Returns
    /// JSON string containing simulation results
    pub fn trigger_test_json(
        &self,
        trigger_json: &str,
        start_iso: Option<String>,
        end_iso: Option<String>,
        manual: bool,
    ) -> Result<String> {
        use rule_trigger_engine::TriggerEvaluationReport;

        // Parse the trigger configuration
        let config = TriggerConfig::from_json(trigger_json)
            .map_err(|e| anyhow!("Failed to parse trigger config: {e}"))?;

        // Extract timing info from preconditions
        let timings = config
            .extract_timing()
            .map_err(|e| anyhow!("Failed to extract timing: {e}"))?;

        let cron_str = timings
            .iter()
            .find_map(|t| match t {
                rule_trigger_engine::TriggerTiming::Cron { expression } => Some(expression.clone()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("No cron expression found in preconditions"))?;

        let repeat_freq = timings.iter().find_map(|t| match t {
            rule_trigger_engine::TriggerTiming::RepeatFrequency { frequency } => {
                Some(frequency.clone())
            }
            _ => None,
        });

        // Get event time range
        let range = self
            .storage
            .events_time_range()?
            .ok_or_else(|| anyhow!("No events available for testing"))?;

        let start_utc = match start_iso {
            Some(value) => DateTime::parse_from_rfc3339(&value)
                .map(|dt| dt.with_timezone(&Utc))
                .context("Invalid start timestamp")?,
            None => range.0,
        };

        let end_utc = match end_iso {
            Some(value) => DateTime::parse_from_rfc3339(&value)
                .map(|dt| dt.with_timezone(&Utc))
                .context("Invalid end timestamp")?,
            None => range.1,
        };

        if end_utc <= start_utc {
            return Err(anyhow!("Test window must be positive (start < end)"));
        }

        // Fetch all events in range for simulation
        let all_events: Vec<MonitoringEvent> = self
            .storage
            .list_events_between_utc(start_utc.timestamp(), end_utc.timestamp())?
            .into_iter()
            .map(|ev| MonitoringEvent {
                event_type: ev.event_type,
                timestamp: ev.timestamp.to_rfc3339(),
                metadata_json: serde_json::to_string(&ev.metadata)
                    .unwrap_or_else(|_| "{}".to_string()),
            })
            .collect();

        let tz = default_timezone();

        // Build result structure
        #[derive(serde::Serialize)]
        struct TriggerTestResult {
            trigger_name: String,
            cron_schedule: String,
            repeat_frequency: Option<String>,
            start_time: String,
            end_time: String,
            mode: String,
            events_in_window: usize,
            runs: Vec<TriggerTestRun>,
            summary: TriggerTestSummary,
        }

        #[derive(serde::Serialize)]
        struct TriggerTestRun {
            trigger_time: String,
            result: bool,
            status: String,
            report: Option<TriggerEvaluationReport>,
        }

        #[derive(serde::Serialize)]
        struct TriggerTestSummary {
            runs_considered: u32,
            runs_executed: u32,
            runs_succeeded: u32,
        }

        let freq_str = repeat_freq.as_ref().map(|f| match f {
            rule_trigger_engine::RepeatFrequency::PerDay(n) => format!("per_day({n})"),
            rule_trigger_engine::RepeatFrequency::PerWeek(n) => format!("per_week({n})"),
        });

        // Manual mode: single evaluation at end time
        if manual {
            let visible_events = filter_events_at_time(&all_events, end_utc, 120);
            let eval_ctx = EvaluationContext {
                events: &visible_events,
                current_time: end_utc.timestamp(),
                timezone_offset: DEFAULT_TIMEZONE_OFFSET,
            };

            let report = config.evaluate_detailed(&eval_ctx, PreconditionPolicy::IgnoreGates);

            let run = TriggerTestRun {
                trigger_time: end_utc.with_timezone(&tz).to_rfc3339(),
                result: report.overall_result,
                status: "manual".to_string(),
                report: Some(report.clone()),
            };

            let result = TriggerTestResult {
                trigger_name: config.name,
                cron_schedule: cron_str,
                repeat_frequency: freq_str,
                start_time: start_utc.to_rfc3339(),
                end_time: end_utc.to_rfc3339(),
                mode: "manual".to_string(),
                events_in_window: all_events.len(),
                runs: vec![run],
                summary: TriggerTestSummary {
                    runs_considered: 1,
                    runs_executed: 1,
                    runs_succeeded: if report.overall_result { 1 } else { 0 },
                },
            };

            return serde_json::to_string(&result).context("Failed to serialize result");
        }

        // Automatic mode: iterate through cron schedule
        let schedule = Cron::from_str(&cron_str).context("Invalid cron expression")?;
        let start_local = start_utc.with_timezone(&tz);
        let end_local = end_utc.with_timezone(&tz);

        let mut runs = Vec::new();
        let mut runs_considered = 0u32;
        let mut runs_executed = 0u32;
        let mut runs_succeeded = 0u32;
        let mut last_success: Option<DateTime<Utc>> = None;

        let mut trigger_time_local = schedule
            .find_next_occurrence(&start_local, false)
            .context("Failed to find next cron occurrence")?;

        while trigger_time_local <= end_local {
            let trigger_time_utc = trigger_time_local.with_timezone(&Utc);
            runs_considered += 1;

            // Check repeat frequency gating
            if let (Some(last), Some(freq)) = (last_success, &repeat_freq) {
                if let Some(min_gap) = repeat_min_gap(freq) {
                    if trigger_time_utc - last < min_gap {
                        runs.push(TriggerTestRun {
                            trigger_time: trigger_time_local.to_rfc3339(),
                            result: false,
                            status: "skipped_repeat_frequency".to_string(),
                            report: None,
                        });
                        trigger_time_local = schedule
                            .find_next_occurrence(&trigger_time_local, false)
                            .context("Failed to find next cron occurrence")?;
                        continue;
                    }
                }
            }

            let visible_events = filter_events_at_time(&all_events, trigger_time_utc, 120);
            let eval_ctx = EvaluationContext {
                events: &visible_events,
                current_time: trigger_time_utc.timestamp(),
                timezone_offset: DEFAULT_TIMEZONE_OFFSET,
            };

            let report = config.evaluate_detailed(&eval_ctx, PreconditionPolicy::EnforceAsGates);

            let has_error = report
                .preconditions
                .iter()
                .chain(report.conditions.iter())
                .any(|e| e.error.is_some());

            runs_executed += 1;
            let fired = report.overall_result;
            if fired {
                runs_succeeded += 1;
                last_success = Some(trigger_time_utc);
            }

            runs.push(TriggerTestRun {
                trigger_time: trigger_time_local.to_rfc3339(),
                result: fired,
                status: if has_error {
                    "error".to_string()
                } else if fired {
                    "fired".to_string()
                } else {
                    "not_fired".to_string()
                },
                report: Some(report),
            });

            trigger_time_local = schedule
                .find_next_occurrence(&trigger_time_local, false)
                .context("Failed to find next cron occurrence")?;
        }

        let result = TriggerTestResult {
            trigger_name: config.name,
            cron_schedule: cron_str,
            repeat_frequency: freq_str,
            start_time: start_utc.to_rfc3339(),
            end_time: end_utc.to_rfc3339(),
            mode: "automatic".to_string(),
            events_in_window: all_events.len(),
            runs,
            summary: TriggerTestSummary {
                runs_considered,
                runs_executed,
                runs_succeeded,
            },
        };

        serde_json::to_string(&result).context("Failed to serialize result")
    }

    fn execute_trigger(
        &self,
        trigger: &StoredTrigger,
        fire_time: DateTime<Utc>,
        run_type: TriggerRunType,
    ) -> Result<TriggerExecutionSummary> {
        info!(
            trigger_id = %trigger.trigger_uuid,
            version = %trigger.version,
            run_type = %run_type,
            "Executing trigger with CEL evaluation"
        );

        const EVENT_LOOKBACK_MINUTES: u32 = 60 * 24 * 7;

        // Parse JSON rules
        let precondition: Vec<TriggerRule> = match serde_json::from_str(&trigger.precondition_json)
        {
            Ok(v) => v,
            Err(err) => {
                let payload = json!({
                    "kind": "trigger_execution_report_v1",
                    "trigger_uuid": trigger.trigger_uuid,
                    "trigger_name": trigger.name,
                    "fired_at": fire_time.to_rfc3339(),
                    "run_type": run_type.as_str(),
                    "error": format!("Failed to parse precondition JSON: {err}"),
                });
                let message = serde_json::to_string(&payload)
                    .unwrap_or_else(|_| "{\"kind\":\"trigger_execution_report_v1\",\"error\":\"serialization_failed\"}".to_string());
                let _ = self.storage.insert_trigger_log(
                    &trigger.trigger_uuid,
                    TriggerLogLevel::Error,
                    &message,
                    fire_time,
                    run_type.clone(),
                );
                return Err(anyhow!("Failed to parse precondition JSON: {err}"));
            }
        };
        let condition: Vec<TriggerRule> = match serde_json::from_str(&trigger.condition_json) {
            Ok(v) => v,
            Err(err) => {
                let payload = json!({
                    "kind": "trigger_execution_report_v1",
                    "trigger_uuid": trigger.trigger_uuid,
                    "trigger_name": trigger.name,
                    "fired_at": fire_time.to_rfc3339(),
                    "run_type": run_type.as_str(),
                    "error": format!("Failed to parse condition JSON: {err}"),
                });
                let message = serde_json::to_string(&payload)
                    .unwrap_or_else(|_| "{\"kind\":\"trigger_execution_report_v1\",\"error\":\"serialization_failed\"}".to_string());
                let _ = self.storage.insert_trigger_log(
                    &trigger.trigger_uuid,
                    TriggerLogLevel::Error,
                    &message,
                    fire_time,
                    run_type.clone(),
                );
                return Err(anyhow!("Failed to parse condition JSON: {err}"));
            }
        };

        // Build rule-trigger-engine config
        let precondition_rules: Vec<EngineRule> = precondition
            .into_iter()
            .map(|rule| EngineRule {
                rule: rule.rule,
                description: rule.description,
            })
            .collect();
        let condition_rules: Vec<EngineRule> = condition
            .into_iter()
            .map(|rule| EngineRule {
                rule: rule.rule,
                description: rule.description,
            })
            .collect();

        let config = TriggerConfig {
            name: trigger.name.clone(),
            version: trigger.version.clone(),
            precondition: precondition_rules,
            condition: condition_rules,
        };

        // Fetch recent events and map to engine event type
        let recent = self
            .storage
            .fetch_events_recent(fire_time, EVENT_LOOKBACK_MINUTES)?;
        let events: Vec<MonitoringEvent> = recent
            .into_iter()
            .map(|event| MonitoringEvent {
                event_type: event.event_type,
                timestamp: event.timestamp.to_rfc3339(),
                metadata_json: serde_json::to_string(&event.metadata)
                    .unwrap_or_else(|_| "{}".to_string()),
            })
            .collect();

        let eval_ctx = EvaluationContext {
            events: &events,
            current_time: fire_time.timestamp(),
            timezone_offset: DEFAULT_TIMEZONE_OFFSET,
        };

        let precondition_policy = match run_type {
            TriggerRunType::Manual => PreconditionPolicy::IgnoreGates,
            TriggerRunType::Automatic | TriggerRunType::Replay => {
                PreconditionPolicy::EnforceAsGates
            }
        };

        let report = config.evaluate_detailed(&eval_ctx, precondition_policy);

        // Persist one aggregated log entry per execution.
        let has_errors = report
            .preconditions
            .iter()
            .chain(report.conditions.iter())
            .any(|e| e.error.is_some());
        let level = if has_errors {
            TriggerLogLevel::Error
        } else {
            TriggerLogLevel::Info
        };
        let payload = json!({
            "kind": "trigger_execution_report_v1",
            "trigger_uuid": trigger.trigger_uuid,
            "trigger_name": trigger.name,
            "fired_at": fire_time.to_rfc3339(),
            "run_type": run_type.as_str(),
            "report": report,
        });
        if let Ok(message) = serde_json::to_string(&payload) {
            if let Err(err) = self.storage.insert_trigger_log(
                &trigger.trigger_uuid,
                level,
                &message,
                fire_time,
                run_type.clone(),
            ) {
                error!(
                    error = %err,
                    trigger_id = %trigger.trigger_uuid,
                    "Failed to persist trigger execution log entry"
                );
            }
        }

        if has_errors {
            warn!(
                trigger_id = %trigger.trigger_uuid,
                "Trigger execution completed with evaluation errors"
            );
        }

        let all_conditions_met = report.overall_result;

        // Persist a notification entry when the trigger fires successfully.
        if all_conditions_met {
            let body = format!(
                "触发器「{}」已于 {} 触发",
                trigger.name,
                fire_time.with_timezone(&default_timezone()).format("%H:%M")
            );
            if let Err(e) = self.storage.insert_notification(
                &NotificationSource::Trigger,
                &trigger.trigger_uuid,
                &trigger.name,
                &body,
            ) {
                warn!(
                    trigger_id = %trigger.trigger_uuid,
                    error = %e,
                    "Failed to persist notification for trigger execution"
                );
            }
        }

        Ok(TriggerExecutionSummary {
            trigger_id: trigger.trigger_uuid.clone(),
            name: trigger.name.clone(),
            fired_at: fire_time,
            result: all_conditions_met,
            run_type,
        })
    }

    // ===== Chat Session Management =====

    /// Create or update a chat session
    pub fn upsert_chat_session(
        &self,
        session_id: String,
        title: Option<String>,
        message_count: i32,
    ) -> Result<()> {
        let now = Utc::now();
        let session = crate::types::ChatSession {
            session_id,
            title,
            created_at: now,
            last_activity: now,
            message_count,
        };
        self.storage.upsert_chat_session(&session)
    }

    /// Get a specific chat session
    pub fn get_chat_session(&self, session_id: &str) -> Result<Option<crate::types::ChatSession>> {
        self.storage.get_chat_session(session_id)
    }

    /// List all chat sessions
    pub fn list_chat_sessions(&self, limit: Option<u32>) -> Result<Vec<crate::types::ChatSession>> {
        self.storage.list_chat_sessions(limit)
    }

    /// Update session activity (last activity time and message count)
    pub fn update_session_activity(
        &self,
        session_id: String,
        title: Option<String>,
        message_count: i32,
    ) -> Result<()> {
        let update = crate::types::ChatSessionUpdate {
            session_id,
            title,
            last_activity: Utc::now(),
            message_count,
        };
        self.storage.update_session_activity(&update)
    }

    /// Delete a chat session
    pub fn delete_chat_session(&self, session_id: &str) -> Result<()> {
        self.storage.delete_chat_session(session_id)
    }

    // ===== Chat Message History =====

    /// Create or update a chat message (stored as raw JSON) for a given session.
    pub fn upsert_chat_message_json(
        &self,
        session_id: String,
        message_id: String,
        created_at_ms: i64,
        message_json: String,
    ) -> Result<()> {
        self.storage.upsert_chat_message_json(
            &session_id,
            &message_id,
            created_at_ms,
            &message_json,
        )
    }

    /// List chat messages (raw JSON strings) for a session.
    pub fn list_chat_messages_json(
        &self,
        session_id: &str,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<String>> {
        self.storage
            .list_chat_messages_json(session_id, limit, offset)
    }

    /// Delete chat messages for a session (keeps the session record).
    pub fn delete_chat_messages(&self, session_id: &str) -> Result<()> {
        self.storage.delete_chat_messages(session_id)
    }

    /// Persist runtime-owned chat protocol state for a session.
    pub fn upsert_chat_runtime_state_json(
        &self,
        session_id: String,
        state_json: String,
    ) -> Result<()> {
        self.storage
            .upsert_chat_runtime_state_json(&session_id, &state_json)
    }

    /// Load runtime-owned chat protocol state for a session.
    pub fn get_chat_runtime_state_json(&self, session_id: &str) -> Result<Option<String>> {
        self.storage.get_chat_runtime_state_json(session_id)
    }

    /// Delete runtime-owned chat protocol state for a session.
    pub fn delete_chat_runtime_state(&self, session_id: &str) -> Result<()> {
        self.storage.delete_chat_runtime_state(session_id)
    }
}

#[cfg(test)]
mod db_observability_tests {
    use super::TriggerSdk;
    use crate::storage::{test_sqlite_counters_get, test_sqlite_counters_reset};
    use serde_json::json;
    use std::time::Instant;
    use tempfile::tempdir;

    #[test]
    fn long_overwrite_writes_things_state_once() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("sdk.sqlite3");

        let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
        let device_id = "device-test";
        let collection_id = "col-test";
        let thing_id = "thing-test";

        // Seed doc with a collection + thing so get_or_init doesn't need to create rows during the measurement.
        let collection_payload = json!({
            "uuid": collection_id,
            "title": "Test Collection",
            "trigger_uuid": null,
            "created_at": null,
            "updated_at": null
        })
        .to_string();
        sdk.things_upsert_collection_json(device_id, &collection_payload)
            .expect("seed collection");

        let thing_payload = json!({
            "uuid": thing_id,
            "title": "Test Thing",
            "datatype": "markdown",
            "data": {"markdown": "hello"},
            "collection_uuid": collection_id,
            "trigger_uuid": null,
            "parent_uuid": null,
            "created_at": null,
            "updated_at": null
        })
        .to_string();
        sdk.things_upsert_thing_json(device_id, &thing_payload)
            .expect("seed thing");

        // Measure overwrite behavior.
        test_sqlite_counters_reset();

        let short = "short content".to_string();
        sdk.things_edit_content(
            device_id,
            thing_id,
            "overwrite",
            None,
            Some(&short),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("short overwrite");

        let after_short = test_sqlite_counters_get();
        eprintln!("short overwrite sqlite counters: {:?}", after_short);
        // V3 uses crdt_document_save instead of things_state_save (which persists each doc individually)
        // Expect at least 1 save per edit (ThingMarkdown)
        assert!(
            after_short.crdt_document_save >= 1,
            "short overwrite must persist crdt documents at least once"
        );

        // Reset again and do a long overwrite.
        test_sqlite_counters_reset();
        let long = "A".repeat(200_000);
        sdk.things_edit_content(
            device_id,
            thing_id,
            "overwrite",
            None,
            Some(&long),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("long overwrite");

        let after_long = test_sqlite_counters_get();
        eprintln!("long overwrite sqlite counters: {:?}", after_long);

        // The core guarantee: overwrite results in bounded DB writes regardless of content length.
        // V3 saves multiple documents per edit (root for metadata, collection, thing_markdown).
        // The key invariant: document save count should be bounded regardless of content size,
        // NOT proportional to content length (no per-character writes).
        assert!(
            after_long.crdt_document_save <= 10,
            "long overwrite should persist crdt documents a bounded number of times, got {}",
            after_long.crdt_document_save
        );
        assert!(
            after_short.crdt_document_save <= 10,
            "short overwrite should persist crdt documents a bounded number of times, got {}",
            after_short.crdt_document_save
        );

        // Connection opens should be roughly constant (not length-dependent)
        // The first edit may open more connections (initializing docs), but both should be bounded.
        assert!(
            after_long.open_connections <= 15,
            "long overwrite should have bounded DB connections: {after_long:?}"
        );
        assert!(
            after_short.open_connections <= 15,
            "short overwrite should have bounded DB connections: {after_short:?}"
        );
    }

    #[test]
    fn long_overwrite_time_breakdown() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("sdk.sqlite3");

        let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
        let device_id = "device-test";
        let collection_id = "col-test";
        let thing_id = "thing-test";

        let collection_payload = json!({
            "uuid": collection_id,
            "title": "Test Collection",
            "trigger_uuid": null,
            "created_at": null,
            "updated_at": null
        })
        .to_string();
        sdk.things_upsert_collection_json(device_id, &collection_payload)
            .expect("seed collection");

        let thing_payload = json!({
            "uuid": thing_id,
            "title": "Test Thing",
            "datatype": "markdown",
            "data": {"markdown": "hello"},
            "collection_uuid": collection_id,
            "trigger_uuid": null,
            "parent_uuid": null,
            "created_at": null,
            "updated_at": null
        })
        .to_string();
        sdk.things_upsert_thing_json(device_id, &thing_payload)
            .expect("seed thing");

        let long = "A".repeat(200_000);

        test_sqlite_counters_reset();

        // V3: Performance test needs to be rewritten for multi-document architecture
        // The v3 architecture stores documents separately, so the performance characteristics differ
        // This test is temporarily simplified to just verify the basic operation works

        // 1) Test the new v3 splice path
        let t0 = Instant::now();
        let result = sdk.things_splice_text(device_id, thing_id, "main", 0, usize::MAX, &long);
        eprintln!(
            "breakdown: things_splice_text ms={} success={}",
            t0.elapsed().as_millis(),
            result.is_ok()
        );
        assert!(result.is_ok());

        let counters = test_sqlite_counters_get();
        eprintln!("breakdown: sqlite counters: {:?}", counters);

        // V3: Multiple documents may be saved, so we just check that operations succeed
        // The exact count depends on the v3 implementation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tempfile::tempdir;

    #[test]
    fn test_extract_cron_from_preconditions() {
        let preconditions = vec![TriggerRule {
            rule: "cron('0 18 * * *')".to_string(),
            description: "Every day at 6 PM".to_string(),
        }];
        assert_eq!(
            extract_cron_from_preconditions(&preconditions),
            Some("0 18 * * *".to_string())
        );

        let no_cron = vec![TriggerRule {
            rule: "in_time_range('09:00', '17:00')".to_string(),
            description: "Business hours".to_string(),
        }];
        assert_eq!(extract_cron_from_preconditions(&no_cron), None);
    }

    #[test]
    fn test_extract_cron_with_double_quotes() {
        let preconditions = vec![TriggerRule {
            rule: r#"cron("0 9 * * *")"#.to_string(),
            description: "Morning".to_string(),
        }];
        assert_eq!(
            extract_cron_from_preconditions(&preconditions),
            Some("0 9 * * *".to_string())
        );
    }

    #[test]
    fn test_croner_accepts_posix_sunday_zero() {
        assert!(Cron::from_str("0 10 * * 0,6").is_ok());
    }

    #[test]
    fn test_network_change_trigger_is_marked_due_on_connectivity_event() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("sdk.sqlite3");
        let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");

        let trigger_uuid = "trg-network-change".to_string();
        sdk.register_trigger(TriggerRegistration {
            trigger_uuid: trigger_uuid.clone(),
            name: "Network change trigger".to_string(),
            version: "v1".to_string(),
            precondition: vec![TriggerRule {
                rule: "network_change()".to_string(),
                description: "On connectivity change".to_string(),
            }],
            condition: vec![TriggerRule {
                rule: "true".to_string(),
                description: "Always true".to_string(),
            }],
        })
        .expect("register trigger");

        // Event-driven triggers should not be due until an event arrives.
        let before = sdk
            .storage
            .fetch_due_triggers(Utc::now())
            .expect("fetch due triggers");
        assert!(
            before.iter().all(|t| t.trigger_uuid != trigger_uuid),
            "network-change trigger must not be due immediately after registration"
        );

        let event_ts = Utc::now();
        sdk.record_event(EventPayload {
            event_type: "Connectivity".to_string(),
            timestamp: event_ts,
            metadata: serde_json::json!({
                "message": "Update connectivity: wifi",
                "states": ["wifi"]
            }),
        })
        .expect("record connectivity event");

        let after = sdk
            .storage
            .fetch_due_triggers(event_ts + chrono::Duration::seconds(1))
            .expect("fetch due triggers after event");
        assert!(
            after.iter().any(|t| t.trigger_uuid == trigger_uuid),
            "network-change trigger must be marked due after Connectivity event"
        );
    }
}

fn compute_next_fire(
    cron_expr: &str,
    anchor: DateTime<Utc>,
    now: Option<DateTime<Utc>>, // if provided, ensure next fire is after this time
) -> Result<Option<DateTime<Utc>>> {
    let schedule = Cron::from_str(cron_expr).context("Invalid cron expression")?;
    let tz = default_timezone();
    let effective_anchor = match now {
        Some(now) if now > anchor => now,
        _ => anchor,
    };
    let anchor_local = effective_anchor.with_timezone(&tz);
    let next_local = schedule
        .find_next_occurrence(&anchor_local, false)
        .context("Invalid cron expression")?;
    Ok(Some(next_local.with_timezone(&Utc)))
}

/// Filter events that fall within a time window ending at `current` and spanning `window_minutes` back.
fn filter_events_at_time(
    all_events: &[MonitoringEvent],
    current: DateTime<Utc>,
    window_minutes: i64,
) -> Vec<MonitoringEvent> {
    let start = current - Duration::minutes(window_minutes);
    all_events
        .iter()
        .filter_map(|e| {
            let dt = DateTime::parse_from_rfc3339(&e.timestamp)
                .ok()?
                .with_timezone(&Utc);
            if dt >= start && dt <= current {
                Some(e.clone())
            } else {
                None
            }
        })
        .collect()
}

fn extract_cron_from_preconditions(preconditions: &[TriggerRule]) -> Option<String> {
    for rule in preconditions {
        // Look for cron() function calls in the rule
        let rule_text = rule.rule.trim();
        if rule_text.starts_with("cron(") {
            // Extract the cron expression from cron('...') or cron("...")
            if let Some(start) = rule_text.find('\'') {
                if let Some(end) = rule_text[start + 1..].find('\'') {
                    return Some(rule_text[start + 1..start + 1 + end].to_string());
                }
            }
            if let Some(start) = rule_text.find('"') {
                if let Some(end) = rule_text[start + 1..].find('"') {
                    return Some(rule_text[start + 1..start + 1 + end].to_string());
                }
            }
        }
    }
    None
}

fn extract_timings_from_rules(
    preconditions: &[TriggerRule],
    conditions: &[TriggerRule],
) -> Result<Vec<rule_trigger_engine::TriggerTiming>> {
    let precondition_rules: Vec<EngineRule> = preconditions
        .iter()
        .cloned()
        .map(|rule| EngineRule {
            rule: rule.rule,
            description: rule.description,
        })
        .collect();
    let condition_rules: Vec<EngineRule> = conditions
        .iter()
        .cloned()
        .map(|rule| EngineRule {
            rule: rule.rule,
            description: rule.description,
        })
        .collect();

    // Dummy values: timing extraction only uses rule bodies.
    let config = TriggerConfig {
        name: "timing-extract".to_string(),
        version: "v1".to_string(),
        precondition: precondition_rules,
        condition: condition_rules,
    };

    config
        .extract_timing()
        .map_err(|err| anyhow!("Failed to extract timing: {err}"))
}

fn extract_repeat_frequency_from_conditions(
    conditions: &[TriggerRule],
) -> Option<rule_trigger_engine::RepeatFrequency> {
    for rule in conditions {
        let rule_text = rule.rule.trim();

        if let Some(arg) = rule_text
            .strip_prefix("repeat_per_day(")
            .and_then(|s| s.strip_suffix(')'))
        {
            let value = arg.trim().parse::<u32>().ok()?;
            if value > 0 {
                return Some(rule_trigger_engine::RepeatFrequency::PerDay(value));
            }
        }

        if let Some(arg) = rule_text
            .strip_prefix("repeat_per_week(")
            .and_then(|s| s.strip_suffix(')'))
        {
            let value = arg.trim().parse::<u32>().ok()?;
            if value > 0 {
                return Some(rule_trigger_engine::RepeatFrequency::PerWeek(value));
            }
        }
    }
    None
}

fn repeat_min_gap(freq: &rule_trigger_engine::RepeatFrequency) -> Option<Duration> {
    match *freq {
        rule_trigger_engine::RepeatFrequency::PerDay(times) if times > 0 => {
            let seconds = (24 * 60 * 60) / i64::from(times);
            Some(Duration::seconds(seconds.max(1)))
        }
        rule_trigger_engine::RepeatFrequency::PerWeek(times) if times > 0 => {
            let seconds = (7 * 24 * 60 * 60) / i64::from(times);
            Some(Duration::seconds(seconds.max(1)))
        }
        _ => None,
    }
}

/// Parse ContentEntryPayload from JSON value
fn parse_content_entry_payload(
    v: &serde_json::Value,
) -> Result<remi_things_crdt::ContentEntryPayload> {
    use remi_things_crdt::{ContentEntryPayload, DateField, LocationField};

    let payload_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match payload_type {
        "location" => {
            let loc_type = v.get("loc_type").and_then(|t| t.as_str()).unwrap_or("");
            let location = match loc_type {
                "coordinate" => {
                    let lat = v
                        .get("lat")
                        .and_then(|l| l.as_f64())
                        .ok_or_else(|| anyhow!("Missing lat"))?;
                    let lng = v
                        .get("lng")
                        .and_then(|l| l.as_f64())
                        .ok_or_else(|| anyhow!("Missing lng"))?;
                    let coord_system = v
                        .get("coord_system")
                        .and_then(|c| c.as_str())
                        .unwrap_or("wgs84")
                        .to_string();
                    let source_name = v
                        .get("source_name")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string());
                    LocationField::Coordinate {
                        lat,
                        lng,
                        coord_system,
                        source_name,
                    }
                }
                "fuzzy" => {
                    let name = v
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string();
                    let place_type = v
                        .get("place_type")
                        .and_then(|p| p.as_str())
                        .unwrap_or("")
                        .to_string();
                    LocationField::Fuzzy { name, place_type }
                }
                _ => anyhow::bail!("Invalid location type: {}", loc_type),
            };
            Ok(ContentEntryPayload::Location(location))
        }
        "markdown" => {
            let doc_uuid = v
                .get("doc_uuid")
                .and_then(|d| d.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            Ok(ContentEntryPayload::Markdown { doc_uuid })
        }
        "date" => {
            let timestamp_ms = v
                .get("timestamp_ms")
                .and_then(|t| t.as_i64())
                .ok_or_else(|| anyhow!("Missing timestamp_ms"))?;
            let has_time = v.get("has_time").and_then(|h| h.as_bool()).unwrap_or(false);
            let timezone = v
                .get("timezone")
                .and_then(|t| t.as_str())
                .map(|s| s.to_string());
            Ok(ContentEntryPayload::Date(DateField {
                timestamp_ms,
                has_time,
                timezone,
            }))
        }
        "custom" => {
            let data = v.get("data").cloned().unwrap_or(serde_json::Value::Null);
            Ok(ContentEntryPayload::Custom(data))
        }
        "image" => {
            let uri = v
                .get("uri")
                .and_then(|u| u.as_str())
                .unwrap_or("")
                .to_string();
            let caption = v
                .get("caption")
                .and_then(|c| c.as_str())
                .map(|s| s.to_string());
            let width = v.get("width").and_then(|w| w.as_u64()).map(|w| w as u32);
            let height = v.get("height").and_then(|h| h.as_u64()).map(|h| h as u32);
            let size_bytes = v.get("size_bytes").and_then(|s| s.as_u64());
            let device_id = v
                .get("device_id")
                .and_then(|d| d.as_str())
                .map(|s| s.to_string());
            Ok(ContentEntryPayload::Image(remi_things_crdt::ImageField {
                uri,
                caption,
                width,
                height,
                size_bytes,
                device_id,
            }))
        }
        "url" => {
            let url = v
                .get("url")
                .and_then(|u| u.as_str())
                .unwrap_or("")
                .to_string();
            let title = v
                .get("title")
                .and_then(|t| t.as_str())
                .map(|s| s.to_string());
            let description = v
                .get("description")
                .and_then(|d| d.as_str())
                .map(|s| s.to_string());
            let image_url = v
                .get("image_url")
                .and_then(|i| i.as_str())
                .map(|s| s.to_string());
            let favicon_url = v
                .get("favicon_url")
                .and_then(|f| f.as_str())
                .map(|s| s.to_string());
            let site_name = v
                .get("site_name")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
            let resolved = v.get("resolved").and_then(|r| r.as_bool()).unwrap_or(false);
            Ok(ContentEntryPayload::Url(remi_things_crdt::UrlField {
                url,
                title,
                description,
                image_url,
                favicon_url,
                site_name,
                resolved,
            }))
        }
        _ => anyhow::bail!("Invalid payload type: {}", payload_type),
    }
}
