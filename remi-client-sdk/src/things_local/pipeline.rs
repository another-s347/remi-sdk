use super::*;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum MutationSource {
    LocalCommand { actor: Option<String> },
    RemoteSync { sync_run_id: String },
    Maintenance,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DirtyPolicy {
    MarkDirty,
    MarkClean,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeLogPolicy {
    RecordUserVisible,
    RecordSyncSummary,
    Suppress,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsMutationContext {
    pub device_id: String,
    pub source: MutationSource,
    pub dirty_policy: DirtyPolicy,
    pub change_log_policy: ChangeLogPolicy,
}

impl ThingsMutationContext {
    pub fn local_command(device_id: &str) -> Self {
        Self {
            device_id: device_id.to_string(),
            source: MutationSource::LocalCommand { actor: None },
            dirty_policy: DirtyPolicy::MarkDirty,
            change_log_policy: ChangeLogPolicy::RecordUserVisible,
        }
    }

    pub fn remote_sync(device_id: &str, sync_run_id: impl Into<String>) -> Self {
        Self {
            device_id: device_id.to_string(),
            source: MutationSource::RemoteSync {
                sync_run_id: sync_run_id.into(),
            },
            dirty_policy: DirtyPolicy::MarkClean,
            change_log_policy: ChangeLogPolicy::Suppress,
        }
    }

    pub fn maintenance(device_id: &str, dirty_policy: DirtyPolicy) -> Self {
        Self {
            device_id: device_id.to_string(),
            source: MutationSource::Maintenance,
            dirty_policy,
            change_log_policy: ChangeLogPolicy::Suppress,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ThingsMutationResult<T> {
    pub value: T,
    pub events: Vec<ThingsDocumentEvent>,
    pub dirty_documents: Vec<DocumentKey>,
    pub change_log_ids: Vec<i64>,
    pub event_range: Option<(i64, i64)>,
}
pub struct ThingsMutationPipeline<'a> {
    storage: &'a Storage,
    things_event_tx: Option<&'a broadcast::Sender<ThingsEvent>>,
}

fn merge_incoming_document_state(
    key: &DocumentKey,
    existing: Option<&DocumentState>,
    incoming: DocumentState,
    clear_dirty: bool,
) -> Result<DocumentState> {
    let Some(existing) = existing else {
        return Ok(DocumentState {
            dirty: if clear_dirty { false } else { incoming.dirty },
            ..incoming
        });
    };

    if existing.automerge_doc.is_empty() {
        let last_sync_at = incoming
            .last_sync_at
            .clone()
            .or_else(|| existing.last_sync_at.clone());
        return Ok(DocumentState {
            automerge_doc: incoming.automerge_doc,
            sync_state: incoming.sync_state,
            dirty: if clear_dirty {
                false
            } else {
                existing.dirty || incoming.dirty
            },
            last_sync_at,
        });
    }

    if incoming.automerge_doc.is_empty() {
        return Ok(DocumentState {
            dirty: if clear_dirty { false } else { existing.dirty },
            last_sync_at: incoming
                .last_sync_at
                .or_else(|| existing.last_sync_at.clone()),
            sync_state: if incoming.sync_state.is_empty() {
                existing.sync_state.clone()
            } else {
                incoming.sync_state
            },
            automerge_doc: existing.automerge_doc.clone(),
        });
    }

    let mut merged = automerge::AutoCommit::load(&existing.automerge_doc)
        .with_context(|| format!("Failed to load existing Things CRDT document {key:?}"))?;
    let mut incoming_doc = automerge::AutoCommit::load(&incoming.automerge_doc)
        .with_context(|| format!("Failed to load incoming Things CRDT document {key:?}"))?;
    merged
        .merge(&mut incoming_doc)
        .with_context(|| format!("Failed to merge incoming Things CRDT document {key:?}"))?;

    let sync_state = if incoming.sync_state.is_empty() {
        existing.sync_state.clone()
    } else {
        incoming.sync_state
    };
    let last_sync_at = incoming
        .last_sync_at
        .or_else(|| existing.last_sync_at.clone());
    let dirty = if clear_dirty {
        false
    } else {
        existing.dirty || incoming.dirty
    };

    Ok(DocumentState {
        automerge_doc: merged.save(),
        sync_state,
        dirty,
        last_sync_at,
    })
}

impl<'a> ThingsMutationPipeline<'a> {
    pub fn new(storage: &'a Storage) -> Self {
        Self {
            storage,
            things_event_tx: None,
        }
    }

    pub fn with_broadcaster(
        storage: &'a Storage,
        things_event_tx: &'a broadcast::Sender<ThingsEvent>,
    ) -> Self {
        Self {
            storage,
            things_event_tx: Some(things_event_tx),
        }
    }

    pub(crate) fn commit_local_documents<T>(
        &self,
        context: &ThingsMutationContext,
        doc_set: &mut ThingsDocumentSet,
        events: Vec<ThingsDocumentEvent>,
        value: T,
    ) -> Result<ThingsMutationResult<T>> {
        self.commit_local_documents_with_deletions(context, doc_set, Vec::new(), events, value)
    }

    pub(crate) fn commit_local_documents_with_deletions<T>(
        &self,
        context: &ThingsMutationContext,
        doc_set: &mut ThingsDocumentSet,
        deleted_documents: Vec<DocumentKey>,
        events: Vec<ThingsDocumentEvent>,
        value: T,
    ) -> Result<ThingsMutationResult<T>> {
        let dirty_documents = doc_set.dirty_document_keys_public();
        let persistence = DocumentPersistence::new(self.storage);
        match context.dirty_policy {
            DirtyPolicy::MarkDirty => {
                persistence
                    .save_dirty_documents_with_compaction(
                        doc_set,
                        remi_things_crdt::DEFAULT_COMPACTION_THRESHOLD,
                    )
                    .context("Failed to save dirty Things CRDT documents")?;
            }
            DirtyPolicy::MarkClean => {
                persistence
                    .save_document_set_mark_clean(doc_set)
                    .context("Failed to save clean Things CRDT documents")?;
            }
        }
        for key in &deleted_documents {
            self.storage
                .delete_crdt_document(&key.uuid, key.data_type_str())
                .with_context(|| format!("Failed to delete Things CRDT document {:?}", key))?;
        }

        let event_ids = self.persist_document_events(context, &events)?;
        let mut change_log_ids = Vec::new();
        if let Some(change_log_id) =
            self.record_sync_summary_change_log(context, &events, &event_ids)?
        {
            change_log_ids.push(change_log_id);
        }
        self.broadcast_document_events(&context.device_id, &events);
        Ok(ThingsMutationResult {
            value,
            events,
            dirty_documents,
            change_log_ids,
            event_range: event_range(&event_ids),
        })
    }

    #[cfg(test)]
    pub(crate) fn apply_remote_document(
        &self,
        context: &ThingsMutationContext,
        key: DocumentKey,
        state: DocumentState,
    ) -> Result<ThingsMutationResult<()>> {
        let result = self.apply_remote_documents(context, vec![(key, state)])?;
        Ok(ThingsMutationResult {
            value: (),
            events: result.events,
            dirty_documents: result.dirty_documents,
            change_log_ids: result.change_log_ids,
            event_range: result.event_range,
        })
    }

    pub(crate) fn apply_remote_documents(
        &self,
        context: &ThingsMutationContext,
        documents: Vec<(DocumentKey, DocumentState)>,
    ) -> Result<ThingsMutationResult<usize>> {
        if documents.is_empty() {
            return Ok(ThingsMutationResult {
                value: 0,
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        }

        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_document_set(&context.device_id)
            .context("Failed to load Things document set before remote apply")?;
        let before_snapshot = doc_set.extract_snapshot().ok();
        let mut applied_keys = Vec::with_capacity(documents.len());
        for (key, state) in documents {
            let last_sync_at = state.last_sync_at.clone();
            let existing = doc_set.get(&key);
            let preserve_dirty = existing.map_or(false, |state| state.dirty);
            let merged_state =
                merge_incoming_document_state(&key, existing, state, !preserve_dirty)?;
            doc_set.set(key.clone(), merged_state);
            applied_keys.push((key, last_sync_at, !preserve_dirty));
        }
        let after_snapshot = doc_set.extract_snapshot().ok();

        let mut events = match (before_snapshot.as_ref(), after_snapshot.as_ref()) {
            (Some(before), Some(after)) => diff_snapshot_document_events(before, after),
            _ => Vec::new(),
        };
        if events.is_empty() {
            events.extend(
                applied_keys.iter().map(|(key, _, _)| {
                    document_event_for_key(key, ThingsDocumentChangeKind::Updated)
                }),
            );
        }
        let persistence = DocumentPersistence::new(self.storage);
        for (key, last_sync_at, mark_clean) in &applied_keys {
            persistence
                .save_document_state(&mut doc_set, key, *mark_clean, last_sync_at.as_deref())
                .with_context(|| {
                    format!("Failed to save remote-applied Things CRDT document clean: {key:?}")
                })?;
        }
        let dirty_documents = applied_keys
            .iter()
            .filter(|(_, _, mark_clean)| !*mark_clean)
            .map(|(key, _, _)| key.clone())
            .collect();
        let event_ids = self.persist_document_events(context, &events)?;
        let mut change_log_ids = Vec::new();
        if let Some(change_log_id) =
            self.record_sync_summary_change_log(context, &events, &event_ids)?
        {
            change_log_ids.push(change_log_id);
        }
        self.broadcast_document_events(&context.device_id, &events);

        Ok(ThingsMutationResult {
            value: applied_keys.len(),
            events,
            dirty_documents,
            change_log_ids,
            event_range: event_range(&event_ids),
        })
    }

    pub(crate) fn save_synced_document_clean(
        &self,
        context: &ThingsMutationContext,
        key: DocumentKey,
        automerge_doc: Vec<u8>,
        sync_state: Vec<u8>,
        last_sync_at: Option<&str>,
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_document_set(&context.device_id)
            .context("Failed to load Things document set before saving synced document clean")?;
        let before_snapshot = doc_set.extract_snapshot().ok();
        let state = DocumentState {
            automerge_doc,
            sync_state,
            dirty: false,
            last_sync_at: last_sync_at.map(ToString::to_string),
        };
        let merged_state = merge_incoming_document_state(&key, doc_set.get(&key), state, true)?;
        doc_set.set(key.clone(), merged_state);
        let after_snapshot = doc_set.extract_snapshot().ok();
        let events = match (before_snapshot.as_ref(), after_snapshot.as_ref()) {
            (Some(before), Some(after)) => diff_snapshot_document_events(before, after),
            _ => Vec::new(),
        };
        DocumentPersistence::new(self.storage)
            .save_document_state(&mut doc_set, &key, true, last_sync_at)
            .context("Failed to save synced Things CRDT document clean")?;
        let event_ids = self.persist_document_events(context, &events)?;
        let mut change_log_ids = Vec::new();
        if let Some(change_log_id) =
            self.record_sync_summary_change_log(context, &events, &event_ids)?
        {
            change_log_ids.push(change_log_id);
        }
        self.broadcast_document_events(&context.device_id, &events);

        Ok(ThingsMutationResult {
            value: (),
            events,
            dirty_documents: Vec::new(),
            change_log_ids,
            event_range: event_range(&event_ids),
        })
    }

    pub(crate) fn delete_raw_document(
        &self,
        context: &ThingsMutationContext,
        key: DocumentKey,
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_document_set(&context.device_id)
            .context("Failed to load Things document set before raw document delete")?;
        doc_set.remove_document(&key);
        let event = document_event_for_key(&key, ThingsDocumentChangeKind::Deleted);
        self.commit_local_documents_with_deletions(
            context,
            &mut doc_set,
            vec![key],
            vec![event],
            (),
        )
    }

    pub(crate) fn emit_snapshot_replaced(
        &self,
        context: &ThingsMutationContext,
        event: ThingsEvent,
    ) -> Result<ThingsMutationResult<()>> {
        if !matches!(event, ThingsEvent::SnapshotReplaced { .. }) {
            anyhow::bail!("emit_snapshot_replaced requires a SnapshotReplaced event");
        }

        let source_json = serde_json::to_string(&context.source)?;
        let sync_run_id = match &context.source {
            MutationSource::RemoteSync { sync_run_id } => Some(sync_run_id.as_str()),
            _ => None,
        };
        let payload = serde_json::to_string(&event)?;
        let event_id = self.storage.insert_things_local_event(
            &context.device_id,
            &source_json,
            "snapshot",
            remi_things_crdt::ROOT_DOC_UUID,
            "snapshot_replaced",
            &payload,
            sync_run_id,
        )?;

        if let Some(things_event_tx) = self.things_event_tx {
            let _ = things_event_tx.send(event);
        }

        Ok(ThingsMutationResult {
            value: (),
            events: Vec::new(),
            dirty_documents: Vec::new(),
            change_log_ids: Vec::new(),
            event_range: Some((event_id, event_id)),
        })
    }

    pub(crate) fn emit_data_wiped(
        &self,
        context: &ThingsMutationContext,
    ) -> Result<ThingsMutationResult<()>> {
        let event = ThingsEvent::DataWiped;
        let source_json = serde_json::to_string(&context.source)?;
        let payload = serde_json::to_string(&event)?;
        let event_id = self.storage.insert_things_local_event(
            &context.device_id,
            &source_json,
            "data",
            "all",
            "data_wiped",
            &payload,
            None,
        )?;

        if let Some(things_event_tx) = self.things_event_tx {
            let _ = things_event_tx.send(event);
        }

        Ok(ThingsMutationResult {
            value: (),
            events: Vec::new(),
            dirty_documents: Vec::new(),
            change_log_ids: Vec::new(),
            event_range: Some((event_id, event_id)),
        })
    }

    fn persist_document_events(
        &self,
        context: &ThingsMutationContext,
        events: &[ThingsDocumentEvent],
    ) -> Result<Vec<i64>> {
        let source_json = serde_json::to_string(&context.source)?;
        let sync_run_id = match &context.source {
            MutationSource::RemoteSync { sync_run_id } => Some(sync_run_id.as_str()),
            _ => None,
        };

        let mut ids = Vec::with_capacity(events.len());
        for event in events {
            let payload = serde_json::to_string(event)?;
            ids.push(self.storage.insert_things_local_event(
                &context.device_id,
                &source_json,
                entity_type(event),
                entity_uuid(event),
                change_kind(event),
                &payload,
                sync_run_id,
            )?);
        }
        Ok(ids)
    }

    fn record_sync_summary_change_log(
        &self,
        context: &ThingsMutationContext,
        events: &[ThingsDocumentEvent],
        event_ids: &[i64],
    ) -> Result<Option<i64>> {
        if context.change_log_policy != ChangeLogPolicy::RecordSyncSummary || events.is_empty() {
            return Ok(None);
        }

        let sync_run_id = match &context.source {
            MutationSource::RemoteSync { sync_run_id } => Some(sync_run_id.as_str()),
            _ => None,
        };
        let entity_uuid = sync_run_id.unwrap_or("maintenance");
        let event_range = event_range(event_ids);
        let details = json!({
            "source": context.source,
            "event_ids": event_ids,
            "event_range": event_range,
            "events": events,
            "sync_run_id": sync_run_id,
        });
        let summary = match sync_run_id {
            Some(sync_run_id) => format!(
                "Applied {} Things sync event(s) from sync run {}",
                events.len(),
                sync_run_id
            ),
            None => format!("Applied {} Things maintenance event(s)", events.len()),
        };

        let change_log_id = self.storage.insert_things_change_log(
            &context.device_id,
            ThingsOperationType::SyncApplied,
            "sync",
            entity_uuid,
            &summary,
            &details.to_string(),
            None,
            false,
        )?;
        self.storage.mark_change_logs_synced(&[change_log_id])?;
        Ok(Some(change_log_id))
    }

    fn broadcast_document_events(&self, device_id: &str, events: &[ThingsDocumentEvent]) {
        let Some(things_event_tx) = self.things_event_tx else {
            return;
        };
        for event in events {
            let _ = things_event_tx.send(event.clone().into_event(device_id));
        }
    }
}
