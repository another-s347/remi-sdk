use anyhow::{Context, Result};
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, ObjId, ObjType, ReadDoc, ScalarValue, Value as AmValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

use remi_things_crdt::{
    CURRENT_SCHEMA_VERSION, CrdtDataType, Content, Op, Schema, ThingDatatype, TriggerUpdate,
    ROOT_DOC_UUID,
    // V3 types
    CollectionOp, ThingMarkdownOp,
    RootView, CollectionDocView, ThingMarkdownView,
    apply_collection_op, apply_thing_markdown_op,
    extract_root_view, extract_collection_doc_view, extract_thing_markdown_view,
    // V3 compaction
    needs_compaction, compact_root_doc, compact_collection_doc, compact_thing_markdown_doc,
    DEFAULT_COMPACTION_THRESHOLD,
    // V3 built-in fields (multi-value)
    ContentEntry, ContentEntryPayload, ThingBuiltInFieldsUpdate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotOptions {
    /// If false, omit thing `data.content` from the snapshot (and avoid extracting content when paired with ExtractOptions).
    pub include_content: bool,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        Self {
            include_content: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThingCollectionEntry {
    pub uuid: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_uuid: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// "user" or "application" — populated from server-side actor metadata cache
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_type: Option<String>,
    /// App ID when actor_type is "application"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_app_id: Option<String>,
    /// Resolved display name for the app, if available
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingCollectionUpsert {
    pub uuid: String,
    pub title: String,
    /// Tri-state semantics:
    /// - key omitted / null => no change
    /// - empty string => clear
    /// - UUID string => set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_uuid: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThingEntry {
    pub uuid: String,
    pub title: String,
    pub datatype: ThingDatatype,
    pub data: Value,
    pub collection_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_uuid: Option<String>,
    #[serde(default)]
    pub parent_uuid: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// Status of the thing: "none", "in-progress", "stalled", "done"
    #[serde(default)]
    pub status: String,
    /// Timestamp when status was last changed (ms since epoch)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_timestamp_ms: Option<i64>,
    /// "user" or "application" — populated from server-side actor metadata cache
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_type: Option<String>,
    /// App ID when actor_type is "application"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_app_id: Option<String>,
    /// Resolved display name for the app, if available
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_display_name: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingUpsert {
    pub uuid: String,
    pub title: String,
    pub datatype: ThingDatatype,
    /// Optional to support CRDT-typed markdown edits where text is updated via SpliceText ops.
    /// When omitted/null, we won't rewrite `content`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    pub collection_uuid: String,
    /// Tri-state semantics:
    /// - key omitted / null => no change
    /// - empty string => clear
    /// - UUID string => set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_uuid: Option<String>,
    #[serde(default)]
    pub parent_uuid: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsSnapshot {
    pub collections: Vec<ThingCollectionEntry>,
    pub things: Vec<ThingEntry>,
}

pub fn init_empty_doc(device_id: &str) -> Result<Vec<u8>> {
    let mut doc = AutoCommit::new();
    Schema::ensure_root(&mut doc, device_id, 0)?;
    Ok(doc.save())
}

pub fn upsert_collection(
    doc_bytes: &[u8],
    device_id: &str,
    upsert: ThingCollectionUpsert,
) -> Result<Vec<u8>> {
    let trigger = trigger_update_from_tri_state(upsert.trigger_uuid.as_deref());
    remi_things_crdt::apply_op(
        doc_bytes,
        device_id,
        Op::UpsertCollection {
            id: upsert.uuid,
            title: Some(upsert.title),
            status: None,
            trigger,
        },
    )
    .context("Failed to apply v2 UpsertCollection")
}

pub fn delete_collection(
    doc_bytes: &[u8],
    device_id: &str,
    collection_uuid: &str,
) -> Result<Vec<u8>> {
    remi_things_crdt::apply_op(
        doc_bytes,
        device_id,
        Op::DeleteCollection {
            id: collection_uuid.to_string(),
        },
    )
    .context("Failed to apply v2 DeleteCollection")
}

pub fn upsert_thing(doc_bytes: &[u8], device_id: &str, upsert: ThingUpsert) -> Result<Vec<u8>> {
    let trigger = trigger_update_from_tri_state(upsert.trigger_uuid.as_deref());

    let content = upsert.data.as_ref().map(|payload| {
        let original_datatype = upsert.datatype.clone();
        markdown_only_content_from_value(&original_datatype, payload)
    });
    remi_things_crdt::apply_op(
        doc_bytes,
        device_id,
        Op::UpsertThing {
            id: upsert.uuid,
            collection_id: upsert.collection_uuid,
            datatype: Some(ThingDatatype::Markdown),
            status: None,
            status_timestamp_ms: None,
            title: Some(upsert.title),
            parent_id: upsert
                .parent_uuid
                .as_ref()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            trigger,
            content,
        },
    )
    .context("Failed to apply v2 UpsertThing")
}

pub fn splice_text(
    doc_bytes: &[u8],
    device_id: &str,
    thing_id: &str,
    block_id: &str,
    index: usize,
    delete: usize,
    insert: &str,
) -> Result<Vec<u8>> {
    remi_things_crdt::apply_op(
        doc_bytes,
        device_id,
        Op::SpliceText {
            thing_id: thing_id.to_string(),
            block_id: block_id.to_string(),
            index,
            delete,
            insert: insert.to_string(),
        },
    )
    .context("Failed to apply v2 SpliceText")
}

pub fn markdown_only_content_from_value(
    original_datatype: &ThingDatatype,
    payload: &Value,
) -> Content {
    if original_datatype.is_markdownish() {
        let text = match payload {
            Value::String(s) => s.clone(),
            Value::Object(map) => map
                .get("markdown")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| payload.to_string()),
            _ => payload.to_string(),
        };
        return Content::Markdown {
            blocks: vec![remi_things_crdt::Block {
                id: "main".to_string(),
                r#type: "markdown".to_string(),
                attrs_json: None,
                text: Some(text),
            }],
        };
    }

    let attrs = json!({
        "embed_kind": original_datatype.as_str(),
        "payload": payload,
    })
    .to_string();

    Content::Markdown {
        blocks: vec![remi_things_crdt::Block {
            id: "main".to_string(),
            r#type: original_datatype.to_string(),
            attrs_json: Some(attrs),
            text: None,
        }],
    }
}

pub fn delete_thing(
    doc_bytes: &[u8],
    device_id: &str,
    collection_uuid: &str,
    thing_uuid: &str,
) -> Result<Vec<u8>> {
    let _ = collection_uuid; // v2 delete is global by id.
    remi_things_crdt::apply_op(
        doc_bytes,
        device_id,
        Op::DeleteThing {
            id: thing_uuid.to_string(),
        },
    )
    .context("Failed to apply v2 DeleteThing")
}

pub fn set_thing_status(
    doc_bytes: &[u8],
    device_id: &str,
    thing_uuid: &str,
    status: &str,
    timestamp_ms: Option<i64>,
) -> Result<Vec<u8>> {
    remi_things_crdt::apply_op(
        doc_bytes,
        device_id,
        Op::SetThingStatus {
            id: thing_uuid.to_string(),
            status: status.to_string(),
            timestamp_ms,
        },
    )
    .context("Failed to apply v2 SetThingStatus")
}

pub fn extract_snapshot(doc_bytes: &[u8]) -> Result<ThingsSnapshot> {
    extract_snapshot_with_options(doc_bytes, SnapshotOptions::default())
}

pub fn extract_snapshot_with_options(
    doc_bytes: &[u8],
    options: SnapshotOptions,
) -> Result<ThingsSnapshot> {
    let extract_opts = remi_things_crdt::ExtractOptions {
        include_things: true,
        include_content: options.include_content,
    };
    let (view, _scale) = remi_things_crdt::extract_view_with_options_and_scale(doc_bytes, extract_opts)
        .context("Failed to extract v2 view")?;
    snapshot_from_view_with_options(&view, options)
}

pub fn snapshot_from_view(view: &remi_things_crdt::view::View) -> Result<ThingsSnapshot> {
    snapshot_from_view_with_options(view, SnapshotOptions::default())
}

pub fn snapshot_from_view_with_options(
    view: &remi_things_crdt::view::View,
    options: SnapshotOptions,
) -> Result<ThingsSnapshot> {
    let mut collections: Vec<ThingCollectionEntry> = Vec::new();
    for c in &view.collections {
        let deleted = c.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false);
        if deleted {
            continue;
        }
        let trigger_uuid = desired_trigger_uuid(deleted, &c.trigger);
        collections.push(ThingCollectionEntry {
            uuid: c.id.clone(),
            title: c.title.clone(),
            trigger_uuid,
            created_at: "".to_string(),
            updated_at: "".to_string(),
            actor_type: None,
            actor_app_id: None,
            actor_display_name: None,
        });
    }

    let mut things: Vec<ThingEntry> = Vec::new();
    for t in &view.things {
        let deleted = t.tombstone.as_ref().map(|x| x.deleted).unwrap_or(false);
        if deleted {
            continue;
        }
        let trigger_uuid = desired_trigger_uuid(deleted, &t.trigger);
        things.push(ThingEntry {
            uuid: t.id.clone(),
            title: t.title.clone().unwrap_or_default(),
            datatype: t.datatype.clone(),
            data: thing_data_from_view_with_options(t, options),
            collection_uuid: t.collection_id.clone(),
            trigger_uuid,
            parent_uuid: t.parent_id.clone(),
            created_at: "".to_string(),
            updated_at: "".to_string(),
            status: t.status.as_storage_str().to_string(),
            status_timestamp_ms: t.status.timestamp_ms(),
            actor_type: None,
            actor_app_id: None,
            actor_display_name: None,
        });
    }

    Ok(ThingsSnapshot { collections, things })
}

/// Check if document is a supported single-document format (v2 or v3 unified)
pub fn is_v2_doc(doc_bytes: &[u8]) -> bool {
    if doc_bytes.is_empty() {
        return true;
    }

    let Ok(doc) = AutoCommit::load(doc_bytes) else {
        return false;
    };

    match doc.get(automerge::ROOT, Schema::KEY_SCHEMA_VERSION) {
        Ok(Some((AmValue::Scalar(sv), _))) => match sv.as_ref() {
            // Support both legacy v2 and current v3 unified docs
            automerge::ScalarValue::Int(i) => (*i as u32) >= 2 && (*i as u32) <= CURRENT_SCHEMA_VERSION,
            _ => false,
        },
        _ => false,
    }
}

pub fn desired_trigger_uuid(
    tombstone_deleted: bool,
    trigger: &Option<remi_things_crdt::view::TriggerBinding>,
) -> Option<String> {
    if tombstone_deleted {
        return None;
    }
    let Some(t) = trigger.as_ref() else {
        return None;
    };
    if t.state != "some" {
        return None;
    }
    t.uuid
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn thing_data_from_view(view: &remi_things_crdt::view::ThingView) -> Value {
    thing_data_from_view_with_options(view, SnapshotOptions::default())
}

pub fn thing_data_from_view_with_options(
    view: &remi_things_crdt::view::ThingView,
    options: SnapshotOptions,
) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("status".to_string(), json!(view.status));
    obj.insert("datatype".to_string(), json!(view.datatype));
    obj.insert("attrs".to_string(), json!(view.attrs));
    if options.include_content {
        obj.insert("content".to_string(), json!(view.content));
    }
    Value::Object(obj)
}

pub fn trigger_update_from_tri_state(raw: Option<&str>) -> TriggerUpdate {
    match raw {
        None => TriggerUpdate::Noop,
        Some(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                TriggerUpdate::Clear
            } else {
                TriggerUpdate::Set(trimmed.to_string())
            }
        }
    }
}

// ============================================================================
// V3 Multi-Document Architecture
// ============================================================================

/// Document key: (uuid, data_type)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DocumentKey {
    pub uuid: String,
    pub data_type: CrdtDataType,
}

impl DocumentKey {
    pub fn root() -> Self {
        Self {
            uuid: ROOT_DOC_UUID.to_string(),
            data_type: CrdtDataType::Root,
        }
    }

    pub fn collection(uuid: &str) -> Self {
        Self {
            uuid: uuid.to_string(),
            data_type: CrdtDataType::Collection,
        }
    }

    pub fn thing_markdown(uuid: &str) -> Self {
        Self {
            uuid: uuid.to_string(),
            data_type: CrdtDataType::ThingMarkdown,
        }
    }

    pub fn data_type_str(&self) -> &'static str {
        self.data_type.as_str()
    }
}

/// In-memory document with sync state
#[derive(Debug, Clone)]
pub struct DocumentState {
    pub automerge_doc: Vec<u8>,
    pub sync_state: Vec<u8>,
    pub dirty: bool,
    pub last_sync_at: Option<String>,
}

impl DocumentState {
    pub fn new_empty() -> Self {
        Self {
            automerge_doc: Vec::new(),
            sync_state: Vec::new(),
            dirty: false,
            last_sync_at: None,
        }
    }
}

/// Manages the set of CRDT documents (Root, Collections, ThingMarkdown)
#[derive(Debug, Clone)]
pub struct ThingsDocumentSet {
    device_id: String,
    documents: HashMap<DocumentKey, DocumentState>,
}

impl ThingsDocumentSet {
    /// Create a new empty document set
    pub fn new(device_id: &str) -> Self {
        Self {
            device_id: device_id.to_string(),
            documents: HashMap::new(),
        }
    }

    /// Initialize with a root document
    pub fn init_root(&mut self) -> Result<()> {
        let key = DocumentKey::root();
        if self.documents.contains_key(&key) {
            return Ok(());
        }

        let doc_bytes = Schema::init_root_doc(&self.device_id)?;
        self.documents.insert(
            key,
            DocumentState {
                automerge_doc: doc_bytes,
                sync_state: Vec::new(),
                dirty: true,
                last_sync_at: None,
            },
        );
        Ok(())
    }

    /// Get or create the root document
    pub fn get_or_init_root(&mut self) -> Result<&DocumentState> {
        self.init_root()?;
        Ok(self.documents.get(&DocumentKey::root()).unwrap())
    }

    /// Get a document by key
    pub fn get(&self, key: &DocumentKey) -> Option<&DocumentState> {
        self.documents.get(key)
    }

    /// Get a document mutably
    pub fn get_mut(&mut self, key: &DocumentKey) -> Option<&mut DocumentState> {
        self.documents.get_mut(key)
    }

    /// Insert or update a document
    pub fn set(&mut self, key: DocumentKey, state: DocumentState) {
        self.documents.insert(key, state);
    }

    /// Check if a document exists
    pub fn contains(&self, key: &DocumentKey) -> bool {
        self.documents.contains_key(key)
    }

    /// Get all document keys
    pub fn keys(&self) -> impl Iterator<Item = &DocumentKey> {
        self.documents.keys()
    }

    /// Get all dirty documents, ordered by sync priority
    pub fn dirty_documents(&self) -> Vec<(&DocumentKey, &DocumentState)> {
        let mut dirty: Vec<_> = self
            .documents
            .iter()
            .filter(|(_, state)| state.dirty)
            .collect();
        dirty.sort_by_key(|(key, _)| key.data_type.sync_priority());
        dirty
    }

    /// Mark a document as clean (synced)
    pub fn mark_clean(&mut self, key: &DocumentKey) {
        if let Some(state) = self.documents.get_mut(key) {
            state.dirty = false;
        }
    }

    /// Mark a document as dirty
    pub fn mark_dirty(&mut self, key: &DocumentKey) {
        if let Some(state) = self.documents.get_mut(key) {
            state.dirty = true;
        }
    }

    /// Ensure every locally-loaded collection document is listed in the root
    /// document's `collection_uuids`.  This repairs the root → collection
    /// linkage that can break when sync is interrupted or documents arrive
    /// out of order.
    ///
    /// Returns the number of collections that were re-linked to root.
    pub fn repair_root_collection_linkage(&mut self) -> Result<usize> {
        let root_view = self.root_view()?;
        let root_uuids: std::collections::HashSet<&str> =
            root_view.collection_uuids.iter().map(|s| s.as_str()).collect();

        let orphaned_colls: Vec<String> = self
            .documents
            .keys()
            .filter(|k| k.data_type == CrdtDataType::Collection && !root_uuids.contains(k.uuid.as_str()))
            .filter_map(|k| {
                match self.collection_view(&k.uuid) {
                    Ok(view)
                        if !view
                            .meta
                            .tombstone
                            .as_ref()
                            .map(|t| t.deleted)
                            .unwrap_or(false) =>
                    {
                        Some(k.uuid.clone())
                    }
                    Ok(_) => {
                        tracing::debug!(
                            collection_uuid = k.uuid.as_str(),
                            "repair_root_collection_linkage: skipping tombstoned collection"
                        );
                        None
                    }
                    Err(err) => {
                        tracing::warn!(
                            collection_uuid = k.uuid.as_str(),
                            error = %err,
                            "repair_root_collection_linkage: failed to inspect collection, skipping relink"
                        );
                        None
                    }
                }
            })
            .collect();

        if orphaned_colls.is_empty() {
            return Ok(0);
        }

        for coll_uuid in &orphaned_colls {
            tracing::warn!(
                collection_uuid = coll_uuid.as_str(),
                "repair_root_collection_linkage: re-linking orphaned collection to root"
            );
            self.add_collection(coll_uuid)?;
        }

        Ok(orphaned_colls.len())
    }

    /// Return collection UUIDs that are still live and reachable from the root document.
    pub fn live_collection_uuids_from_root(&self) -> Result<HashSet<String>> {
        let root_view = self.root_view()?;
        let mut live = HashSet::new();

        for coll_uuid in root_view.collection_uuids {
            let key = DocumentKey::collection(&coll_uuid);
            match self.documents.get(&key) {
                Some(_) => {
                    let deleted = self
                        .collection_view(&coll_uuid)?
                        .meta
                        .tombstone
                        .as_ref()
                        .map(|t| t.deleted)
                        .unwrap_or(false);
                    if !deleted {
                        live.insert(coll_uuid);
                    }
                }
                None => {
                    // Root still references it; keep it reachable so Phase 2 can pull it.
                    live.insert(coll_uuid);
                }
            }
        }

        Ok(live)
    }

    /// Return thing UUIDs that are still reachable through live collections.
    pub fn live_thing_uuids_from_root(&self) -> Result<HashSet<String>> {
        let live_collections = self.live_collection_uuids_from_root()?;
        let mut live_things = HashSet::new();

        for coll_uuid in &live_collections {
            let key = DocumentKey::collection(coll_uuid);
            let Some(state) = self.documents.get(&key) else {
                continue;
            };

            let view = extract_collection_doc_view(&state.automerge_doc, coll_uuid)?;
            if view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false)
            {
                continue;
            }

            for thing in &view.things {
                if !thing
                    .tombstone
                    .as_ref()
                    .map(|t| t.deleted)
                    .unwrap_or(false)
                {
                    live_things.insert(thing.id.clone());
                }
            }
        }

        Ok(live_things)
    }

    // ===== V3 Compaction =====

    /// Try to compact a document if it exceeds the size threshold.
    /// Returns true if compaction was performed.
    pub fn maybe_compact(&mut self, key: &DocumentKey) -> Result<bool> {
        self.maybe_compact_with_threshold(key, DEFAULT_COMPACTION_THRESHOLD)
    }

    /// Try to compact a document if it exceeds the specified threshold.
    /// Returns true if compaction was performed.
    pub fn maybe_compact_with_threshold(&mut self, key: &DocumentKey, threshold: usize) -> Result<bool> {
        let Some(state) = self.documents.get(key) else {
            return Ok(false);
        };

        if !needs_compaction(&state.automerge_doc, threshold) {
            return Ok(false);
        }

        // Perform compaction based on document type
        let compacted = match key.data_type {
            CrdtDataType::Root => {
                compact_root_doc(&state.automerge_doc, &self.device_id)
                    .context("Failed to compact root document")?
            }
            CrdtDataType::Collection => {
                compact_collection_doc(&state.automerge_doc, &key.uuid, &self.device_id)
                    .context("Failed to compact collection document")?
            }
            CrdtDataType::ThingMarkdown => {
                compact_thing_markdown_doc(&state.automerge_doc, &key.uuid, &self.device_id)
                    .context("Failed to compact thing markdown document")?
            }
        };

        // Update the document in-place
        if let Some(state) = self.documents.get_mut(key) {
            state.automerge_doc = compacted;
            state.dirty = true;
        }

        Ok(true)
    }

    /// Compact all documents that exceed the threshold.
    /// Returns the number of documents compacted.
    pub fn compact_all(&mut self) -> Result<usize> {
        self.compact_all_with_threshold(DEFAULT_COMPACTION_THRESHOLD)
    }

    /// Compact all documents that exceed the specified threshold.
    /// Returns the number of documents compacted.
    pub fn compact_all_with_threshold(&mut self, threshold: usize) -> Result<usize> {
        // Collect keys that need compaction
        let keys_to_compact: Vec<DocumentKey> = self
            .documents
            .iter()
            .filter(|(_, state)| needs_compaction(&state.automerge_doc, threshold))
            .map(|(key, _)| key.clone())
            .collect();

        let mut count = 0;
        for key in keys_to_compact {
            if self.maybe_compact_with_threshold(&key, threshold)? {
                count += 1;
            }
        }

        Ok(count)
    }

    // ===== Root Operations =====

    fn root_collection_list_ids(doc: &AutoCommit) -> Result<Vec<ObjId>> {
        Ok(doc
            .get_all(automerge::ROOT, Schema::KEY_COLLECTION_UUIDS)?
            .into_iter()
            .filter_map(|(value, obj_id)| match value {
                AmValue::Object(ObjType::List) => Some(obj_id),
                _ => None,
            })
            .collect())
    }

    fn root_list_contains_collection(
        doc: &AutoCommit,
        list_obj: &ObjId,
        collection_uuid: &str,
    ) -> Result<bool> {
        for index in 0..doc.length(list_obj) {
            if let Some((AmValue::Scalar(value), _)) = doc.get(list_obj, index)? {
                if let ScalarValue::Str(existing_uuid) = value.as_ref() {
                    if existing_uuid == collection_uuid {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    fn remove_collection_from_root_lists(
        doc: &mut AutoCommit,
        collection_uuid: &str,
    ) -> Result<()> {
        for list_obj in Self::root_collection_list_ids(doc)? {
            let mut indexes_to_delete = Vec::new();

            for index in 0..doc.length(&list_obj) {
                if let Some((AmValue::Scalar(value), _)) = doc.get(&list_obj, index)? {
                    if let ScalarValue::Str(existing_uuid) = value.as_ref() {
                        if existing_uuid == collection_uuid {
                            indexes_to_delete.push(index);
                        }
                    }
                }
            }

            for index in indexes_to_delete.into_iter().rev() {
                doc.delete(&list_obj, index)
                    .context("Failed to remove collection uuid from root list")?;
            }
        }

        Ok(())
    }

    fn update_root_collection_membership(
        &self,
        doc_bytes: &[u8],
        collection_uuid: &str,
        should_exist: bool,
    ) -> Result<Vec<u8>> {
        let mut doc = if doc_bytes.is_empty() {
            let init_bytes = Schema::init_root_doc(&self.device_id)?;
            AutoCommit::load(&init_bytes).context("Failed to load init root doc")?
        } else {
            AutoCommit::load(doc_bytes).context("Failed to load root doc")?
        };
        doc.set_actor(ActorId::from(self.device_id.as_bytes().to_vec()));

        let list_objs = Self::root_collection_list_ids(&doc)?;

        if should_exist {
            let mut already_present = false;
            for list_obj in &list_objs {
                if Self::root_list_contains_collection(&doc, list_obj, collection_uuid)? {
                    already_present = true;
                    break;
                }
            }

            if !already_present {
                let target_list = if let Some(existing) = list_objs.first() {
                    existing.clone()
                } else {
                    doc.put_object(automerge::ROOT, Schema::KEY_COLLECTION_UUIDS, ObjType::List)
                        .context("Failed to create collection_uuids list")?
                };
                doc.insert(&target_list, doc.length(&target_list), collection_uuid.to_string())
                    .context("Failed to add collection uuid to root list")?;
            }
        } else {
            Self::remove_collection_from_root_lists(&mut doc, collection_uuid)?;
        }

        Ok(doc.save())
    }

    /// Add a collection to the root document
    pub fn add_collection(&mut self, collection_uuid: &str) -> Result<()> {
        self.init_root()?;
        let key = DocumentKey::root();
        let current_doc = self.documents.get(&key).unwrap().automerge_doc.clone();
        let updated_doc =
            self.update_root_collection_membership(&current_doc, collection_uuid, true)?;
        let state = self.documents.get_mut(&key).unwrap();

        state.automerge_doc = updated_doc;
        state.dirty = true;
        Ok(())
    }

    /// Remove a collection from the root document
    pub fn remove_collection(&mut self, collection_uuid: &str) -> Result<()> {
        self.init_root()?;
        let key = DocumentKey::root();
        let current_doc = self.documents.get(&key).unwrap().automerge_doc.clone();
        let updated_doc =
            self.update_root_collection_membership(&current_doc, collection_uuid, false)?;
        let state = self.documents.get_mut(&key).unwrap();

        state.automerge_doc = updated_doc;
        state.dirty = true;
        Ok(())
    }

    /// Get root view
    pub fn root_view(&self) -> Result<RootView> {
        let key = DocumentKey::root();
        match self.documents.get(&key) {
            Some(state) => extract_root_view(&state.automerge_doc),
            None => Ok(RootView {
                schema_version: CURRENT_SCHEMA_VERSION,
                epoch: 0,
                collection_uuids: Vec::new(),
            }),
        }
    }

    // ===== Collection Operations =====

    /// Get or create a collection document
    pub fn get_or_init_collection(&mut self, collection_uuid: &str) -> Result<&DocumentState> {
        let key = DocumentKey::collection(collection_uuid);
        if !self.documents.contains_key(&key) {
            let doc_bytes = Schema::init_collection_doc(&self.device_id, collection_uuid)?;
            self.documents.insert(
                key.clone(),
                DocumentState {
                    automerge_doc: doc_bytes,
                    sync_state: Vec::new(),
                    dirty: true,
                    last_sync_at: None,
                },
            );
            // Also add to root
            self.add_collection(collection_uuid)?;
        }
        Ok(self.documents.get(&key).unwrap())
    }

    /// Update collection metadata
    pub fn update_collection_meta(
        &mut self,
        collection_uuid: &str,
        title: Option<String>,
        status: Option<String>,
        trigger: TriggerUpdate,
    ) -> Result<()> {
        self.get_or_init_collection(collection_uuid)?;
        let key = DocumentKey::collection(collection_uuid);
        let state = self.documents.get_mut(&key).unwrap();

        state.automerge_doc = apply_collection_op(
            &state.automerge_doc,
            &self.device_id,
            collection_uuid,
            CollectionOp::UpdateMeta {
                title,
                status,
                trigger,
                attrs_json: None,
            },
        )?;
        state.dirty = true;
        Ok(())
    }

    /// Upsert a thing in a collection
    pub fn upsert_thing_meta(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        datatype: Option<ThingDatatype>,
        status: Option<String>,
        title: Option<String>,
        parent_uuid: Option<String>,
        trigger: TriggerUpdate,
    ) -> Result<()> {
        self.get_or_init_collection(collection_uuid)?;
        let key = DocumentKey::collection(collection_uuid);
        let state = self.documents.get_mut(&key).unwrap();

        state.automerge_doc = apply_collection_op(
            &state.automerge_doc,
            &self.device_id,
            collection_uuid,
            CollectionOp::UpsertThingMeta {
                thing_id: thing_uuid.to_string(),
                datatype,
                status,
                status_timestamp_ms: None,
                title,
                parent_id: parent_uuid,
                trigger,
                built_in: None,
                attrs_json: None,
            },
        )?;
        state.dirty = true;
        Ok(())
    }

    /// Delete a collection by tombstoning its collection document and removing
    /// the root reference. Child things become unreachable through the deleted
    /// collection and are pruned from snapshots without deleting their docs.
    pub fn delete_collection(&mut self, collection_uuid: &str) -> Result<()> {
        self.get_or_init_collection(collection_uuid)?;

        let key = DocumentKey::collection(collection_uuid);
        let state = self.documents.get_mut(&key).unwrap();

        state.automerge_doc = apply_collection_op(
            &state.automerge_doc,
            &self.device_id,
            collection_uuid,
            CollectionOp::Delete,
        )?;
        state.dirty = true;

        self.remove_collection(collection_uuid)?;
        Ok(())
    }

    /// Delete a thing from a collection
    pub fn delete_thing(&mut self, collection_uuid: &str, thing_uuid: &str) -> Result<()> {
        let key = DocumentKey::collection(collection_uuid);
        if let Some(state) = self.documents.get_mut(&key) {
            state.automerge_doc = apply_collection_op(
                &state.automerge_doc,
                &self.device_id,
                collection_uuid,
                CollectionOp::DeleteThing {
                    thing_id: thing_uuid.to_string(),
                },
            )?;
            state.dirty = true;
        }
        Ok(())
    }

    /// Add a content entry to a thing (V3 multi-value)
    pub fn add_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry: ContentEntry,
    ) -> Result<()> {
        self.get_or_init_collection(collection_uuid)?;
        let key = DocumentKey::collection(collection_uuid);
        let state = self.documents.get_mut(&key).unwrap();

        let built_in = ThingBuiltInFieldsUpdate {
            add_entries: vec![entry],
            ..Default::default()
        };

        state.automerge_doc = apply_collection_op(
            &state.automerge_doc,
            &self.device_id,
            collection_uuid,
            CollectionOp::UpsertThingMeta {
                thing_id: thing_uuid.to_string(),
                datatype: None,
                status: None,
                status_timestamp_ms: None,
                title: None,
                parent_id: None,
                trigger: TriggerUpdate::Noop,
                built_in: Some(built_in),
                attrs_json: None,
            },
        )?;
        state.dirty = true;
        Ok(())
    }

    /// Update a content entry on a thing (V3 multi-value)
    pub fn update_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry_id: &str,
        title: Option<Option<String>>,
        order: Option<f64>,
        payload: Option<ContentEntryPayload>,
    ) -> Result<()> {
        self.get_or_init_collection(collection_uuid)?;
        let key = DocumentKey::collection(collection_uuid);
        let state = self.documents.get_mut(&key).unwrap();

        let entry_update = remi_things_crdt::ContentEntryUpdate {
            id: entry_id.to_string(),
            title,
            order,
            payload,
        };

        let built_in = ThingBuiltInFieldsUpdate {
            update_entries: vec![entry_update],
            ..Default::default()
        };

        state.automerge_doc = apply_collection_op(
            &state.automerge_doc,
            &self.device_id,
            collection_uuid,
            CollectionOp::UpsertThingMeta {
                thing_id: thing_uuid.to_string(),
                datatype: None,
                status: None,
                status_timestamp_ms: None,
                title: None,
                parent_id: None,
                trigger: TriggerUpdate::Noop,
                built_in: Some(built_in),
                attrs_json: None,
            },
        )?;
        state.dirty = true;
        Ok(())
    }

    /// Delete a content entry from a thing (V3 multi-value)
    pub fn delete_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<()> {
        self.get_or_init_collection(collection_uuid)?;
        let key = DocumentKey::collection(collection_uuid);
        let state = self.documents.get_mut(&key).unwrap();

        let built_in = ThingBuiltInFieldsUpdate {
            delete_entry_ids: vec![entry_id.to_string()],
            ..Default::default()
        };

        state.automerge_doc = apply_collection_op(
            &state.automerge_doc,
            &self.device_id,
            collection_uuid,
            CollectionOp::UpsertThingMeta {
                thing_id: thing_uuid.to_string(),
                datatype: None,
                status: None,
                status_timestamp_ms: None,
                title: None,
                parent_id: None,
                trigger: TriggerUpdate::Noop,
                built_in: Some(built_in),
                attrs_json: None,
            },
        )?;
        state.dirty = true;
        Ok(())
    }

    /// Get content entries for a thing
    pub fn get_content_entries(
        &self,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Result<Vec<ContentEntry>> {
        let key = DocumentKey::collection(collection_uuid);
        let state = self.documents.get(&key)
            .context("Collection not found")?;

        let view = extract_collection_doc_view(&state.automerge_doc, collection_uuid)?;
        let thing = view.things.iter()
            .find(|t| t.id == thing_uuid)
            .context("Thing not found")?;

        Ok(thing.built_in.content_entries.clone())
    }

    /// Find which collection a thing belongs to by scanning all collection documents.
    ///
    /// This is more robust than `extract_snapshot()` because it does **not** depend on
    /// the root document listing the collection. It's useful when the root <-> collection
    /// linkage might be stale (e.g. after sync or migration).
    pub fn find_thing_collection_uuid(&self, thing_uuid: &str) -> Option<String> {
        for (key, state) in &self.documents {
            if key.data_type != CrdtDataType::Collection {
                continue;
            }
            // Try to extract the collection view; skip broken documents.
            let view = match extract_collection_doc_view(&state.automerge_doc, &key.uuid) {
                Ok(v) => v,
                Err(_) => continue,
            };
            for thing in &view.things {
                if thing.id == thing_uuid {
                    // Also accept tombstoned things — the caller may want to
                    // revive or operate on a soft-deleted thing.
                    return Some(key.uuid.clone());
                }
            }
        }
        None
    }

    /// Get collection view
    pub fn collection_view(&self, collection_uuid: &str) -> Result<CollectionDocView> {
        let key = DocumentKey::collection(collection_uuid);
        match self.documents.get(&key) {
            Some(state) => extract_collection_doc_view(&state.automerge_doc, collection_uuid),
            None => Ok(CollectionDocView {
                schema_version: CURRENT_SCHEMA_VERSION,
                meta: remi_things_crdt::CollectionMetaView {
                    id: collection_uuid.to_string(),
                    title: String::new(),
                    status: "active".to_string(),
                    edit_clock: remi_things_crdt::view::EditClock::zero(),
                    tombstone: None,
                    trigger: None,
                    attrs: None,
                },
                things: Vec::new(),
            }),
        }
    }

    // ===== ThingMarkdown Operations =====

    /// Get or create a thing markdown document
    pub fn get_or_init_thing_markdown(&mut self, thing_uuid: &str) -> Result<&DocumentState> {
        let key = DocumentKey::thing_markdown(thing_uuid);
        if !self.documents.contains_key(&key) {
            let doc_bytes = Schema::init_thing_markdown_doc(&self.device_id, thing_uuid)?;
            self.documents.insert(
                key.clone(),
                DocumentState {
                    automerge_doc: doc_bytes,
                    sync_state: Vec::new(),
                    dirty: true,
                    last_sync_at: None,
                },
            );
        }
        Ok(self.documents.get(&key).unwrap())
    }

    /// Set content on a thing markdown document
    pub fn set_thing_content(&mut self, thing_uuid: &str, content: Content) -> Result<()> {
        self.get_or_init_thing_markdown(thing_uuid)?;
        let key = DocumentKey::thing_markdown(thing_uuid);
        let state = self.documents.get_mut(&key).unwrap();

        state.automerge_doc = apply_thing_markdown_op(
            &state.automerge_doc,
            &self.device_id,
            thing_uuid,
            ThingMarkdownOp::SetContent { content },
        )?;
        state.dirty = true;
        Ok(())
    }

    /// Splice text in a thing markdown block
    pub fn splice_thing_text(
        &mut self,
        thing_uuid: &str,
        block_id: &str,
        index: usize,
        delete: usize,
        insert: &str,
    ) -> Result<()> {
        self.get_or_init_thing_markdown(thing_uuid)?;
        let key = DocumentKey::thing_markdown(thing_uuid);
        let state = self.documents.get_mut(&key).unwrap();

        state.automerge_doc = apply_thing_markdown_op(
            &state.automerge_doc,
            &self.device_id,
            thing_uuid,
            ThingMarkdownOp::SpliceText {
                block_id: block_id.to_string(),
                index,
                delete,
                insert: insert.to_string(),
            },
        )?;
        state.dirty = true;
        Ok(())
    }

    /// Get thing markdown view
    pub fn thing_markdown_view(&self, thing_uuid: &str) -> Result<ThingMarkdownView> {
        let key = DocumentKey::thing_markdown(thing_uuid);
        match self.documents.get(&key) {
            Some(state) => extract_thing_markdown_view(&state.automerge_doc, thing_uuid),
            None => Ok(ThingMarkdownView {
                schema_version: CURRENT_SCHEMA_VERSION,
                thing_uuid: thing_uuid.to_string(),
                content: None,
            }),
        }
    }

    // ===== Snapshot Generation =====

    /// Extract a full snapshot from all documents
    pub fn extract_snapshot(&self) -> Result<ThingsSnapshot> {
        self.extract_snapshot_with_options(SnapshotOptions::default())
    }

    /// Extract a snapshot with options
    pub fn extract_snapshot_with_options(&self, options: SnapshotOptions) -> Result<ThingsSnapshot> {
        let root = self.root_view()?;
        let mut collections = Vec::new();
        let mut things = Vec::new();

        for coll_uuid in &root.collection_uuids {
            let coll_view = self.collection_view(coll_uuid)?;

            // Skip deleted collections
            let deleted = coll_view.meta.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false);
            if deleted {
                continue;
            }

            let trigger_uuid = desired_trigger_uuid(deleted, &coll_view.meta.trigger);
            collections.push(ThingCollectionEntry {
                uuid: coll_view.meta.id.clone(),
                title: coll_view.meta.title.clone(),
                trigger_uuid,
                created_at: String::new(),
                updated_at: String::new(),
                actor_type: None,
                actor_app_id: None,
                actor_display_name: None,
            });

            // Process things in this collection
            for thing_meta in &coll_view.things {
                let thing_deleted = thing_meta.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false);
                if thing_deleted {
                    continue;
                }

                let thing_trigger = desired_trigger_uuid(thing_deleted, &thing_meta.trigger);

                // Get content if requested
                let data = if options.include_content {
                    let md_view = self.thing_markdown_view(&thing_meta.id)?;
                    thing_data_from_thing_meta_and_content(thing_meta, md_view.content.as_ref())
                } else {
                    thing_data_from_thing_meta_no_content(thing_meta)
                };

                things.push(ThingEntry {
                    uuid: thing_meta.id.clone(),
                    title: thing_meta.title.clone().unwrap_or_default(),
                    datatype: thing_meta.datatype.clone(),
                    data,
                    collection_uuid: coll_uuid.clone(),
                    trigger_uuid: thing_trigger,
                    parent_uuid: thing_meta.parent_id.clone(),
                    created_at: String::new(),
                    updated_at: String::new(),
                    status: thing_meta.status.as_storage_str().to_string(),
                    status_timestamp_ms: thing_meta.status.timestamp_ms(),
                    actor_type: None,
                    actor_app_id: None,
                    actor_display_name: None,
                });
            }
        }

        Ok(ThingsSnapshot { collections, things })
    }
}

fn thing_data_from_thing_meta_and_content(
    meta: &remi_things_crdt::ThingMetaView,
    content: Option<&remi_things_crdt::view::ContentView>,
) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("status".to_string(), json!(meta.status));
    obj.insert("datatype".to_string(), json!(meta.datatype));
    obj.insert("attrs".to_string(), json!(meta.attrs));
    obj.insert("content".to_string(), json!(content));
    // Include built-in fields (location, date, etc.)
    obj.insert("built_in".to_string(), json!(meta.built_in));
    Value::Object(obj)
}

fn thing_data_from_thing_meta_no_content(meta: &remi_things_crdt::ThingMetaView) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("status".to_string(), json!(meta.status));
    obj.insert("datatype".to_string(), json!(meta.datatype));
    obj.insert("attrs".to_string(), json!(meta.attrs));
    // Include built-in fields (location, date, etc.)
    obj.insert("built_in".to_string(), json!(meta.built_in));
    Value::Object(obj)
}

#[cfg(test)]
mod tests_v3 {
    use super::*;

    #[test]
    fn test_document_set_basic() {
        let mut docs = ThingsDocumentSet::new("test-device");
        docs.init_root().unwrap();

        // Add a collection
        docs.get_or_init_collection("coll-1").unwrap();
        let root = docs.root_view().unwrap();
        assert!(root.collection_uuids.contains(&"coll-1".to_string()));

        // Add a thing
        docs.upsert_thing_meta(
            "coll-1",
            "thing-1",
            Some(ThingDatatype::Markdown),
            Some("none".to_string()),
            Some("My Task".to_string()),
            None,
            TriggerUpdate::Noop,
        )
        .unwrap();

        let coll = docs.collection_view("coll-1").unwrap();
        assert_eq!(coll.things.len(), 1);
        assert_eq!(coll.things[0].id, "thing-1");
    }

    #[test]
    fn test_document_set_snapshot() {
        let mut docs = ThingsDocumentSet::new("test-device");
        docs.get_or_init_collection("coll-1").unwrap();
        docs.update_collection_meta("coll-1", Some("My Collection".to_string()), None, TriggerUpdate::Noop).unwrap();
        docs.upsert_thing_meta(
            "coll-1",
            "thing-1",
            Some(ThingDatatype::Markdown),
            Some("none".to_string()),
            Some("Task 1".to_string()),
            None,
            TriggerUpdate::Noop,
        )
        .unwrap();

        let snapshot = docs.extract_snapshot_with_options(SnapshotOptions { include_content: false }).unwrap();
        assert_eq!(snapshot.collections.len(), 1);
        assert_eq!(snapshot.collections[0].title, "My Collection");
        assert_eq!(snapshot.things.len(), 1);
        assert_eq!(snapshot.things[0].title, "Task 1");
    }

    #[test]
    fn test_deleted_collection_is_not_relinked_to_root() {
        let mut docs = ThingsDocumentSet::new("test-device");
        docs.get_or_init_collection("coll-1").unwrap();
        docs.update_collection_meta(
            "coll-1",
            Some("My Collection".to_string()),
            None,
            TriggerUpdate::Noop,
        )
        .unwrap();
        docs.upsert_thing_meta(
            "coll-1",
            "thing-1",
            Some(ThingDatatype::Markdown),
            Some("none".to_string()),
            Some("Task 1".to_string()),
            None,
            TriggerUpdate::Noop,
        )
        .unwrap();
        docs.set_thing_content(
            "thing-1",
            Content::Markdown {
                blocks: vec![remi_things_crdt::Block {
                    id: "main".to_string(),
                    r#type: "markdown".to_string(),
                    attrs_json: None,
                    text: Some("hello".to_string()),
                }],
            },
        )
        .unwrap();

        docs.delete_collection("coll-1").unwrap();

        let root = docs.root_view().unwrap();
        assert!(!root.collection_uuids.contains(&"coll-1".to_string()));

        let coll = docs.collection_view("coll-1").unwrap();
        assert!(
            coll.meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false)
        );
        assert!(docs.get(&DocumentKey::collection("coll-1")).is_some());
        assert!(docs.get(&DocumentKey::thing_markdown("thing-1")).is_some());

        let repaired = docs.repair_root_collection_linkage().unwrap();
        assert_eq!(repaired, 0);

        let snapshot = docs
            .extract_snapshot_with_options(SnapshotOptions {
                include_content: false,
            })
            .unwrap();
        assert!(snapshot.collections.is_empty());
        assert!(snapshot.things.is_empty());
    }

    #[test]
    fn test_live_reachability_skips_deleted_entities() {
        let mut docs = ThingsDocumentSet::new("test-device");
        docs.get_or_init_collection("coll-1").unwrap();
        docs.upsert_thing_meta(
            "coll-1",
            "thing-1",
            Some(ThingDatatype::Markdown),
            Some("none".to_string()),
            Some("Task 1".to_string()),
            None,
            TriggerUpdate::Noop,
        )
        .unwrap();
        docs.set_thing_content(
            "thing-1",
            Content::Markdown {
                blocks: vec![remi_things_crdt::Block {
                    id: "main".to_string(),
                    r#type: "markdown".to_string(),
                    attrs_json: None,
                    text: Some("hello".to_string()),
                }],
            },
        )
        .unwrap();

        let live_collections = docs.live_collection_uuids_from_root().unwrap();
        let live_things = docs.live_thing_uuids_from_root().unwrap();
        assert!(live_collections.contains("coll-1"));
        assert!(live_things.contains("thing-1"));

        docs.delete_thing("coll-1", "thing-1").unwrap();
        let live_things = docs.live_thing_uuids_from_root().unwrap();
        assert!(!live_things.contains("thing-1"));

        docs.delete_collection("coll-1").unwrap();
        let live_collections = docs.live_collection_uuids_from_root().unwrap();
        let live_things = docs.live_thing_uuids_from_root().unwrap();
        assert!(!live_collections.contains("coll-1"));
        assert!(!live_things.contains("thing-1"));
    }
}

// ============================================================================
// Storage Integration Helpers
// ============================================================================

impl ThingsDocumentSet {
    /// Load all documents from storage into a ThingsDocumentSet
    pub fn load_from_storage(storage: &crate::storage::Storage, device_id: &str) -> Result<Self> {
        let mut doc_set = Self::new(device_id);

        let keys = storage
            .list_crdt_document_keys()
            .context("Failed to list CRDT document keys")?;

        for (uuid, data_type_str) in keys {
            let data_type: CrdtDataType = match data_type_str.as_str() {
                "root" => CrdtDataType::Root,
                "collection" => CrdtDataType::Collection,
                "thing_markdown" => CrdtDataType::ThingMarkdown,
                _ => continue,
            };

            if let Some(row) = storage
                .get_crdt_document(&uuid, &data_type_str)
                .context("Failed to get CRDT document")?
            {
                let key = DocumentKey {
                    uuid: uuid.clone(),
                    data_type,
                };
                doc_set.set(
                    key,
                    DocumentState {
                        automerge_doc: row.automerge_doc,
                        sync_state: row.sync_state,
                        dirty: row.dirty,
                        last_sync_at: row.last_sync_at,
                    },
                );
            }
        }

        Ok(doc_set)
    }

    /// Save all documents to storage
    pub fn save_to_storage(&self, storage: &crate::storage::Storage) -> Result<()> {
        for (key, state) in &self.documents {
            storage
                .save_crdt_document(
                    &key.uuid,
                    key.data_type_str(),
                    &state.automerge_doc,
                    &state.sync_state,
                    state.dirty,
                    state.last_sync_at.as_deref(),
                )
                .with_context(|| format!("Failed to save CRDT document {:?}", key))?;
        }
        Ok(())
    }

    /// Save only modified documents to storage
    pub fn save_dirty_to_storage(&self, storage: &crate::storage::Storage) -> Result<usize> {
        let dirty = self.dirty_documents();
        let count = dirty.len();

        for (key, state) in dirty {
            storage
                .save_crdt_document(
                    &key.uuid,
                    key.data_type_str(),
                    &state.automerge_doc,
                    &state.sync_state,
                    state.dirty,
                    state.last_sync_at.as_deref(),
                )
                .with_context(|| format!("Failed to save dirty CRDT document {:?}", key))?;
        }

        Ok(count)
    }

    /// Save only modified documents to storage, compacting large documents first.
    /// Returns (documents_saved, documents_compacted).
    pub fn save_dirty_to_storage_with_compaction(
        &mut self,
        storage: &crate::storage::Storage,
    ) -> Result<(usize, usize)> {
        self.save_dirty_to_storage_with_compaction_threshold(storage, DEFAULT_COMPACTION_THRESHOLD)
    }

    /// Save only modified documents to storage, compacting documents that exceed the threshold.
    /// Returns (documents_saved, documents_compacted).
    pub fn save_dirty_to_storage_with_compaction_threshold(
        &mut self,
        storage: &crate::storage::Storage,
        threshold: usize,
    ) -> Result<(usize, usize)> {
        // First, collect dirty document keys
        let dirty_keys: Vec<DocumentKey> = self
            .documents
            .iter()
            .filter(|(_, state)| state.dirty)
            .map(|(key, _)| key.clone())
            .collect();

        let count = dirty_keys.len();
        let mut compacted = 0;

        // Compact and save each dirty document
        for key in &dirty_keys {
            // Try to compact if needed
            if self.maybe_compact_with_threshold(key, threshold)? {
                compacted += 1;
            }

            // Save the document
            if let Some(state) = self.documents.get(&key) {
                storage
                    .save_crdt_document(
                        &key.uuid,
                        key.data_type_str(),
                        &state.automerge_doc,
                        &state.sync_state,
                        state.dirty,
                        state.last_sync_at.as_deref(),
                    )
                    .with_context(|| format!("Failed to save dirty CRDT document {:?}", key))?;
            }
        }

        Ok((count, compacted))
    }

    /// Save a single document to storage
    pub fn save_document_to_storage(
        &mut self,
        storage: &crate::storage::Storage,
        key: &DocumentKey,
        mark_clean: bool,
        last_sync_at: Option<&str>,
    ) -> Result<()> {
        if let Some(state) = self.documents.get_mut(key) {
            let dirty = if mark_clean { false } else { state.dirty };
            storage
                .save_crdt_document(
                    &key.uuid,
                    key.data_type_str(),
                    &state.automerge_doc,
                    &state.sync_state,
                    dirty,
                    last_sync_at.or(state.last_sync_at.as_deref()),
                )
                .with_context(|| format!("Failed to save CRDT document {:?}", key))?;
            if mark_clean {
                state.dirty = false;
            }
            if let Some(last_sync_at) = last_sync_at {
                state.last_sync_at = Some(last_sync_at.to_string());
            }
        }
        Ok(())
    }

    /// Check if any document has pending changes
    pub fn has_pending_changes(&self) -> bool {
        self.documents.values().any(|s| s.dirty)
    }

    /// Delete a collection and its associated things from storage
    pub fn delete_collection_from_storage(
        &mut self,
        storage: &crate::storage::Storage,
        collection_uuid: &str,
    ) -> Result<()> {
        // Get things in this collection before deleting
        let coll_view = self.collection_view(collection_uuid)?;
        let thing_uuids: Vec<String> = coll_view.things.iter().map(|t| t.id.clone()).collect();

        // Delete thing markdown documents
        for thing_uuid in &thing_uuids {
            let key = DocumentKey::thing_markdown(thing_uuid);
            self.documents.remove(&key);
            storage.delete_crdt_document(thing_uuid, "thing_markdown").ok();
        }

        // Delete collection document
        let key = DocumentKey::collection(collection_uuid);
        self.documents.remove(&key);
        storage.delete_crdt_document(collection_uuid, "collection")?;

        // Remove from root
        self.remove_collection(collection_uuid)?;

        Ok(())
    }

    /// Get the device ID for this document set
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// Make documents field accessible for deletion
    pub fn remove_document(&mut self, key: &DocumentKey) -> Option<DocumentState> {
        self.documents.remove(key)
    }
}
