use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::broadcast;

pub use crate::things_crdt::FieldPatch;

use crate::storage::Storage;
use crate::things_crdt::{
    ARCHIVED_AT_ATTR_KEY, ARCHIVED_FROM_COLLECTION_UUID_ATTR_KEY, COLLECTION_APP_ID_ATTR_KEY,
    COLLECTION_TYPE_ATTR_KEY, CollectionType, ContentEntry, ContentEntryPayload,
    ContentEntryUpdate, DocumentKey, DocumentPersistence, DocumentState, JsonObjectField,
    ThingCollectionEntry, ThingCollectionUpsert, ThingDatatype, ThingEntry, ThingStatus,
    ThingUpsert, ThingsDocumentSet, ThingsMutationEvent, ThingsSnapshot, format_domain_datetime,
};
use crate::things_events::{
    ThingsDocumentChangeKind, ThingsDocumentEvent, ThingsDocumentKind, ThingsEvent,
};
use crate::types::{
    EntityActionBinding, ThingsChangeLogEntry, ThingsContentSnapshot, ThingsOperationType,
    ThingsUndoConflict, ThingsUndoConflictType, ThingsUndoExecution, ThingsUndoPreview,
    ThingsUndoResolutionOption,
};

const ENTITY_ACTION_BINDINGS_ATTR_KEY: &str = "action_bindings";
const COLLECTION_CARD_JSX_ATTR_KEY: &str = "card_jsx";
const ENTRY_REFERENCE_SCHEME: &str = "remi-entry://";
pub const SYSTEM_DEFAULT_COLLECTION_ID: &str = "00000000-0000-0000-0000-000000000000";
pub const SYSTEM_TRASH_COLLECTION_ID: &str = "00000000-0000-0000-0000-000000000001";

mod bindings;
mod bootstrap;
mod changelog;
mod collections;
mod content;
mod markdown;

mod pipeline;
pub use pipeline::{
    ChangeLogPolicy, DirtyPolicy, MutationSource, ThingsMutationContext, ThingsMutationPipeline,
    ThingsMutationResult,
};
mod things;
mod undo;

#[derive(Debug, Clone, Default)]
pub struct ThingsDeleteCollectionOutcome {
    pub deleted: bool,
}

pub type ThingsLocalEvent = ThingsMutationEvent;

fn attrs_object(attrs: Option<Value>) -> serde_json::Map<String, Value> {
    match attrs {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    }
}

fn collection_metadata_attrs(
    existing_attrs: Option<Value>,
    collection_type: CollectionType,
    app_id: Option<&str>,
    archived_at: Option<&str>,
) -> Value {
    let mut map = attrs_object(existing_attrs);
    map.insert(
        COLLECTION_TYPE_ATTR_KEY.to_string(),
        Value::String(collection_type.as_str().to_string()),
    );
    match app_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => {
            map.insert(
                COLLECTION_APP_ID_ATTR_KEY.to_string(),
                Value::String(value.to_string()),
            );
        }
        None => {
            map.remove(COLLECTION_APP_ID_ATTR_KEY);
        }
    }
    match archived_at.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => {
            map.insert(
                ARCHIVED_AT_ATTR_KEY.to_string(),
                Value::String(value.to_string()),
            );
        }
        None => {
            map.remove(ARCHIVED_AT_ATTR_KEY);
        }
    }
    Value::Object(map)
}

fn thing_archive_attrs(
    existing_attrs: Option<Value>,
    archived_at: Option<&str>,
    archived_from_collection_uuid: Option<&str>,
) -> Value {
    let mut map = attrs_object(existing_attrs);
    match archived_at.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => {
            map.insert(
                ARCHIVED_AT_ATTR_KEY.to_string(),
                Value::String(value.to_string()),
            );
        }
        None => {
            map.remove(ARCHIVED_AT_ATTR_KEY);
        }
    }
    match archived_from_collection_uuid
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => {
            map.insert(
                ARCHIVED_FROM_COLLECTION_UUID_ATTR_KEY.to_string(),
                Value::String(value.to_string()),
            );
        }
        None => {
            map.remove(ARCHIVED_FROM_COLLECTION_UUID_ATTR_KEY);
        }
    }
    Value::Object(map)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapStashedDocument {
    pub uuid: String,
    pub data_type: String,
    pub automerge_doc_base64: String,
}

pub struct ThingsLocalService<'a> {
    storage: &'a Storage,
    pipeline: ThingsMutationPipeline<'a>,
}

impl<'a> ThingsLocalService<'a> {
    pub fn new(storage: &'a Storage) -> Self {
        Self {
            storage,
            pipeline: ThingsMutationPipeline::new(storage),
        }
    }

    pub fn with_broadcaster(
        storage: &'a Storage,
        things_event_tx: &'a broadcast::Sender<ThingsEvent>,
    ) -> Self {
        Self {
            storage,
            pipeline: ThingsMutationPipeline::with_broadcaster(storage, things_event_tx),
        }
    }

    #[cfg(test)]
    fn pipeline(&self) -> &ThingsMutationPipeline<'a> {
        &self.pipeline
    }

    fn load_document_set(&self, device_id: &str) -> Result<ThingsDocumentSet> {
        DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .with_context(|| format!("Failed to load Things document set for device {device_id}"))
    }

    pub fn app_collection_uuid(app_id: &str) -> Result<String> {
        let app_id = app_id.trim();
        if app_id.is_empty() {
            anyhow::bail!("app_id must not be empty");
        }
        Ok(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!("remi:things:app:{app_id}").as_bytes(),
        )
        .to_string())
    }

    fn ensure_system_collection_in_doc_set(
        &self,
        doc_set: &mut ThingsDocumentSet,
        uuid: &str,
        title: &str,
        collection_type: CollectionType,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let snapshot =
            doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
                include_content: false,
            })?;
        let mut events = Vec::new();
        let existing = snapshot
            .collections
            .iter()
            .find(|collection| collection.uuid == uuid);
        let needs_update = existing.map_or(true, |collection| {
            collection.title != title
                || collection.collection_type != collection_type
                || collection.archived_at.is_some()
                || collection.app_id.is_some()
        });
        if !needs_update {
            return Ok(events);
        }
        let existing_attrs = doc_set
            .collection_view(uuid)
            .ok()
            .and_then(|view| view.meta.attrs);
        events.extend(doc_set.update_collection_meta_with_timestamps(
            uuid,
            Some(title.to_string()),
            None,
            None,
            None,
        )?);
        events.extend(doc_set.update_collection_attrs(
            uuid,
            Some(collection_metadata_attrs(
                existing_attrs,
                collection_type,
                None,
                None,
            )),
        )?);
        Ok(events)
    }

    fn ensure_default_collection_in_doc_set(
        &self,
        doc_set: &mut ThingsDocumentSet,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        self.ensure_system_collection_in_doc_set(
            doc_set,
            SYSTEM_DEFAULT_COLLECTION_ID,
            "Drafts",
            CollectionType::Default,
        )
    }

    fn ensure_trash_collection_in_doc_set(
        &self,
        doc_set: &mut ThingsDocumentSet,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        self.ensure_system_collection_in_doc_set(
            doc_set,
            SYSTEM_TRASH_COLLECTION_ID,
            "Trash",
            CollectionType::Trash,
        )
    }

    fn ensure_system_collections_in_doc_set(
        &self,
        doc_set: &mut ThingsDocumentSet,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let mut events = self.ensure_default_collection_in_doc_set(doc_set)?;
        events.extend(self.ensure_trash_collection_in_doc_set(doc_set)?);
        Ok(events)
    }

    fn ensure_target_system_collection_in_doc_set(
        &self,
        doc_set: &mut ThingsDocumentSet,
        collection_uuid: &str,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        match collection_uuid {
            SYSTEM_DEFAULT_COLLECTION_ID => self.ensure_default_collection_in_doc_set(doc_set),
            SYSTEM_TRASH_COLLECTION_ID => self.ensure_trash_collection_in_doc_set(doc_set),
            _ => Ok(Vec::new()),
        }
    }

    pub fn ensure_app_collection(
        &self,
        device_id: &str,
        app_id: &str,
        title: Option<String>,
    ) -> Result<ThingsMutationResult<ThingCollectionEntry>> {
        let app_id = app_id.trim();
        let collection_uuid = Self::app_collection_uuid(app_id)?;
        let title = title.unwrap_or_else(|| app_id.to_string());
        let context = ThingsMutationContext::local_command(device_id);
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before app collection ensure")?;
        let mut events = Vec::new();
        let existing_attrs = doc_set
            .collection_view(&collection_uuid)
            .ok()
            .and_then(|view| view.meta.attrs);
        events.extend(doc_set.update_collection_meta_with_timestamps(
            &collection_uuid,
            Some(title),
            None,
            None,
            None,
        )?);
        events.extend(doc_set.update_collection_attrs(
            &collection_uuid,
            Some(collection_metadata_attrs(
                existing_attrs,
                CollectionType::App,
                Some(app_id),
                None,
            )),
        )?);
        let snapshot = doc_set.extract_snapshot()?;
        let value = snapshot
            .collections
            .into_iter()
            .find(|collection| collection.uuid == collection_uuid)
            .ok_or_else(|| anyhow!("App collection not found after ensure"))?;
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, value)
    }

    pub fn snapshot(&self, device_id: &str) -> Result<crate::things_crdt::ThingsSnapshotState> {
        self.snapshot_with_options(
            device_id,
            crate::things_crdt::SnapshotOptions {
                include_content: true,
            },
        )
    }

    pub fn snapshot_lite(
        &self,
        device_id: &str,
    ) -> Result<crate::things_crdt::ThingsSnapshotState> {
        self.snapshot_with_options(
            device_id,
            crate::things_crdt::SnapshotOptions {
                include_content: false,
            },
        )
    }

    pub fn snapshot_with_options(
        &self,
        device_id: &str,
        snapshot_options: crate::things_crdt::SnapshotOptions,
    ) -> Result<crate::things_crdt::ThingsSnapshotState> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_document_set(device_id)
            .with_context(|| {
                format!("Failed to load Things document set for device {device_id}")
            })?;
        let dirty = doc_set.has_pending_changes();
        if !doc_set.has_root_document() {
            doc_set.init_root()?;
        }
        self.ensure_system_collections_in_doc_set(&mut doc_set)?;
        let mut snapshot = doc_set
            .extract_snapshot_with_options(snapshot_options)
            .context("Failed to extract Things snapshot")?;

        if let Ok(actor_meta) = self.storage.load_things_actor_meta_map() {
            for collection in &mut snapshot.collections {
                if let Some(meta) = actor_meta.get(&collection.uuid) {
                    collection.actor_type = Some(meta.actor_type.clone());
                    collection.actor_app_id = meta.actor_app_id.clone();
                    collection.actor_display_name = meta.actor_display_name.clone();
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

        Ok(crate::things_crdt::ThingsSnapshotState {
            collections: snapshot.collections,
            things: snapshot.things,
            dirty,
            last_sync_at: None,
        })
    }

    pub fn has_pending_changes(&self, device_id: &str) -> Result<bool> {
        Ok(self.load_document_set(device_id)?.has_pending_changes())
    }

    pub fn list_collections(&self, device_id: &str) -> Result<Vec<ThingCollectionEntry>> {
        Ok(self.snapshot_lite(device_id)?.collections)
    }

    pub fn get_collection(
        &self,
        device_id: &str,
        collection_uuid: &str,
    ) -> Result<Option<ThingCollectionEntry>> {
        Ok(self
            .list_collections(device_id)?
            .into_iter()
            .find(|collection| collection.uuid == collection_uuid))
    }

    pub fn list_things(&self, device_id: &str, include_content: bool) -> Result<Vec<ThingEntry>> {
        Ok(self
            .snapshot_with_options(
                device_id,
                crate::things_crdt::SnapshotOptions { include_content },
            )?
            .things)
    }

    pub fn get_thing(
        &self,
        device_id: &str,
        thing_uuid: &str,
        include_content: bool,
    ) -> Result<Option<ThingEntry>> {
        Ok(self
            .list_things(device_id, include_content)?
            .into_iter()
            .find(|thing| thing.uuid == thing_uuid))
    }

    pub fn get_thing_markdown(&self, device_id: &str, thing_uuid: &str) -> Result<Option<String>> {
        let doc_set = self.load_document_set(device_id)?;
        let snapshot =
            doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
                include_content: false,
            })?;
        if !snapshot.things.iter().any(|thing| thing.uuid == thing_uuid) {
            return Ok(None);
        }
        doc_set.get_thing_markdown_text(thing_uuid)
    }

    pub fn tree_data(&self, device_id: &str) -> Result<crate::things_crdt::ThingsTreeData> {
        self.load_document_set(device_id)?.extract_tree_data()
    }

    pub fn render_thing_content_markdown(
        &self,
        device_id: &str,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Result<String> {
        let doc_set = self.load_document_set(device_id)?;
        let collection = doc_set.collection_view(collection_uuid)?;
        let thing = collection
            .things
            .iter()
            .find(|thing| {
                thing.id == thing_uuid
                    && !thing
                        .tombstone
                        .as_ref()
                        .map(|tombstone| tombstone.deleted)
                        .unwrap_or(false)
            })
            .ok_or_else(|| anyhow!("Thing not found: {}", thing_uuid))?;

        let markdown = doc_set
            .get_thing_markdown_text(thing_uuid)?
            .unwrap_or_default();
        Ok(rewrite_embedded_entry_references(
            &markdown,
            collection_uuid,
            thing_uuid,
            &thing.built_in.content_entries,
        ))
    }
}

fn resolve_thing_collection_uuid(doc_set: &ThingsDocumentSet, thing_uuid: &str) -> Result<String> {
    let snapshot = doc_set.extract_snapshot()?;
    match snapshot
        .things
        .iter()
        .find(|thing| thing.uuid == thing_uuid)
    {
        Some(thing) => Ok(thing.collection_uuid.clone()),
        None => doc_set
            .find_thing_collection_uuid(thing_uuid)
            .ok_or_else(|| anyhow!("Thing not found: {}", thing_uuid)),
    }
}

fn no_event_result<T>(value: T) -> ThingsMutationResult<T> {
    ThingsMutationResult {
        value,
        events: Vec::new(),
        dirty_documents: Vec::new(),
        change_log_ids: Vec::new(),
        event_range: None,
    }
}

fn check_undo_conflict(
    log_entry: &ThingsChangeLogEntry,
    snapshot: &ThingsSnapshot,
) -> Result<Option<ThingsUndoConflict>> {
    match log_entry.op_type {
        ThingsOperationType::CreateCollection => {
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

            let details: Value = serde_json::from_str(&log_entry.details_json).unwrap_or_default();
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
                            description: "Restore this thing to a different collection".to_string(),
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

fn rewrite_embedded_entry_references(
    markdown: &str,
    collection_uuid: &str,
    thing_uuid: &str,
    entries: &[ContentEntry],
) -> String {
    let mut rendered = markdown.to_string();
    let id_to_index = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.id.as_str(), index))
        .collect::<std::collections::HashMap<_, _>>();

    for (entry_id, index) in id_to_index {
        let full_path =
            format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{index}");
        let label = match entries[index].payload {
            ContentEntryPayload::Image(_) => "IMG",
            _ => "内容",
        };
        let target = format!("{ENTRY_REFERENCE_SCHEME}{entry_id}");

        rendered =
            replace_entry_reference_target(&rendered, &target, &format!("[{label}]({full_path})"));
    }

    rendered
}

fn replace_entry_reference_target(markdown: &str, target: &str, replacement: &str) -> String {
    let mut next = markdown.replace(&format!("![]({target})"), replacement);
    next = next.replace(&format!("[remi-entry]({target})"), replacement);
    next = next.replace(&format!("<{}>", target), replacement);
    next
}

fn replay_missing_snapshot_into_document_set(
    doc_set: &mut ThingsDocumentSet,
    snapshot: &ThingsSnapshot,
) -> Result<Vec<ThingsDocumentEvent>> {
    let current = doc_set.extract_snapshot()?;
    let current_collections: std::collections::HashSet<_> = current
        .collections
        .iter()
        .map(|collection| collection.uuid.as_str())
        .collect();
    let current_things: std::collections::HashSet<_> = current
        .things
        .iter()
        .map(|thing| thing.uuid.as_str())
        .collect();

    let mut events = Vec::new();
    for collection in &snapshot.collections {
        if current_collections.contains(collection.uuid.as_str()) {
            continue;
        }

        doc_set.get_or_init_collection(&collection.uuid)?;
        events.extend(doc_set.update_collection_meta_with_timestamps(
            &collection.uuid,
            Some(collection.title.clone()),
            None,
            Some(format_domain_datetime(collection.created_at)),
            Some(format_domain_datetime(collection.updated_at)),
        )?);
        let existing_attrs = doc_set
            .collection_view(&collection.uuid)
            .ok()
            .and_then(|view| view.meta.attrs);
        let archived_at = collection.archived_at.map(format_domain_datetime);
        events.extend(doc_set.update_collection_attrs(
            &collection.uuid,
            Some(collection_metadata_attrs(
                existing_attrs,
                collection.collection_type,
                collection.app_id.as_deref(),
                archived_at.as_deref(),
            )),
        )?);
    }

    for thing in &snapshot.things {
        if current_things.contains(thing.uuid.as_str()) {
            continue;
        }
        if thing_is_known_in_collection(doc_set, &thing.collection_uuid, &thing.uuid)? {
            continue;
        }

        events.extend(replay_snapshot_thing_into_document_set(doc_set, thing)?);
    }

    Ok(events)
}

fn thing_is_known_in_collection(
    doc_set: &ThingsDocumentSet,
    collection_uuid: &str,
    thing_uuid: &str,
) -> Result<bool> {
    let Ok(view) = doc_set.collection_view(collection_uuid) else {
        return Ok(false);
    };

    Ok(view.things.iter().any(|thing| thing.id == thing_uuid))
}

fn replay_snapshot_thing_into_document_set(
    doc_set: &mut ThingsDocumentSet,
    thing: &ThingEntry,
) -> Result<Vec<ThingsDocumentEvent>> {
    let content_registry = crate::things_crdt::ContentTypeRegistry::new();
    let (markdown, content_entries) = content_registry.extract_thing_snapshot_parts(&thing.data)?;

    let mut events = doc_set.upsert_thing_meta_with_timestamps(
        &thing.collection_uuid,
        &thing.uuid,
        Some(thing.datatype.clone()),
        Some(thing.status.as_str().to_string()),
        Some(thing.title.clone()),
        thing.parent_uuid.clone(),
        Some(format_domain_datetime(thing.created_at)),
        Some(format_domain_datetime(thing.updated_at)),
    )?;
    let existing_attrs = doc_set
        .collection_view(&thing.collection_uuid)
        .ok()
        .and_then(|view| {
            view.things
                .into_iter()
                .find(|item| item.id == thing.uuid)
                .and_then(|item| item.attrs)
        });
    let archived_at = thing.archived_at.map(format_domain_datetime);
    events.extend(doc_set.update_thing_attrs(
        &thing.collection_uuid,
        &thing.uuid,
        Some(thing_archive_attrs(
            existing_attrs,
            archived_at.as_deref(),
            thing.archived_from_collection_uuid.as_deref(),
        )),
    )?);

    if let Some(markdown) = markdown {
        events.extend(doc_set.set_thing_markdown_text(&thing.uuid, &markdown)?);
    }

    for entry in content_entries {
        events.extend(doc_set.add_content_entry(&thing.collection_uuid, &thing.uuid, entry)?);
    }

    Ok(events)
}

fn event_range(ids: &[i64]) -> Option<(i64, i64)> {
    let first = ids.iter().min().copied()?;
    let last = ids.iter().max().copied()?;
    Some((first, last))
}

fn merge_event_ranges(left: Option<(i64, i64)>, right: Option<(i64, i64)>) -> Option<(i64, i64)> {
    match (left, right) {
        (Some((left_start, left_end)), Some((right_start, right_end))) => {
            Some((left_start.min(right_start), left_end.max(right_end)))
        }
        (Some(range), None) | (None, Some(range)) => Some(range),
        (None, None) => None,
    }
}

fn entity_type(event: &ThingsDocumentEvent) -> &'static str {
    match event.document_kind {
        ThingsDocumentKind::Root => "snapshot",
        ThingsDocumentKind::Collection => "collection",
        ThingsDocumentKind::Thing | ThingsDocumentKind::ThingMarkdown => "thing",
        ThingsDocumentKind::ContentEntry => "content_entry",
    }
}

fn entity_uuid(event: &ThingsDocumentEvent) -> &str {
    event
        .entry_id
        .as_deref()
        .or(event.thing_uuid.as_deref())
        .or(event.collection_uuid.as_deref())
        .unwrap_or(event.document_uuid.as_str())
}

fn change_kind(event: &ThingsDocumentEvent) -> &'static str {
    match event.change_kind {
        ThingsDocumentChangeKind::Created => "created",
        ThingsDocumentChangeKind::Updated => "updated",
        ThingsDocumentChangeKind::Deleted => "deleted",
    }
}

pub fn document_event_for_key(
    key: &DocumentKey,
    change_kind: ThingsDocumentChangeKind,
) -> ThingsDocumentEvent {
    match key.data_type {
        remi_things_crdt::CrdtDataType::Root => ThingsDocumentEvent::root(change_kind),
        remi_things_crdt::CrdtDataType::Collection => {
            ThingsDocumentEvent::collection(change_kind, &key.uuid)
        }
        remi_things_crdt::CrdtDataType::ThingMarkdown => {
            ThingsDocumentEvent::thing_markdown(change_kind, None, &key.uuid)
        }
    }
}

fn diff_snapshot_document_events(
    before: &ThingsSnapshot,
    after: &ThingsSnapshot,
) -> Vec<ThingsDocumentEvent> {
    let before_collections = before
        .collections
        .iter()
        .map(|collection| (collection.uuid.as_str(), collection))
        .collect::<HashMap<_, _>>();
    let after_collections = after
        .collections
        .iter()
        .map(|collection| (collection.uuid.as_str(), collection))
        .collect::<HashMap<_, _>>();
    let before_things = before
        .things
        .iter()
        .map(|thing| (thing.uuid.as_str(), thing))
        .collect::<HashMap<_, _>>();
    let after_things = after
        .things
        .iter()
        .map(|thing| (thing.uuid.as_str(), thing))
        .collect::<HashMap<_, _>>();

    let mut events = Vec::new();

    for collection in &after.collections {
        match before_collections.get(collection.uuid.as_str()) {
            None => events.push(ThingsDocumentEvent::collection(
                ThingsDocumentChangeKind::Created,
                &collection.uuid,
            )),
            Some(previous) if *previous != collection => {
                events.push(ThingsDocumentEvent::collection(
                    ThingsDocumentChangeKind::Updated,
                    &collection.uuid,
                ));
            }
            _ => {}
        }
    }
    for collection in &before.collections {
        if !after_collections.contains_key(collection.uuid.as_str()) {
            events.push(ThingsDocumentEvent::collection(
                ThingsDocumentChangeKind::Deleted,
                &collection.uuid,
            ));
        }
    }

    for thing in &after.things {
        match before_things.get(thing.uuid.as_str()) {
            None => events.push(ThingsDocumentEvent::thing(
                ThingsDocumentChangeKind::Created,
                &thing.collection_uuid,
                &thing.uuid,
            )),
            Some(previous) if *previous != thing => {
                events.push(ThingsDocumentEvent::thing(
                    ThingsDocumentChangeKind::Updated,
                    &thing.collection_uuid,
                    &thing.uuid,
                ));
            }
            _ => {}
        }
    }
    for thing in &before.things {
        if !after_things.contains_key(thing.uuid.as_str()) {
            events.push(ThingsDocumentEvent::thing(
                ThingsDocumentChangeKind::Deleted,
                &thing.collection_uuid,
                &thing.uuid,
            ));
        }
    }

    events
}

pub fn sync_applied_payload(range: Option<(i64, i64)>) -> Value {
    json!({
        "type": "sync_applied",
        "event_range": range,
        "created_at": Utc::now().to_rfc3339(),
    })
}

#[cfg(test)]
mod tests;
