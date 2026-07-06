use anyhow::{Context, Result};
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, ObjId, ObjType, ReadDoc, ScalarValue, Value as AmValue};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

use crate::things_events::{ThingsDocumentChangeKind, ThingsDocumentEvent};

pub use remi_things_crdt::{
    CollectionId, CollectionType, ContentEntry, ContentEntryId, ContentEntryKind,
    ContentEntryPayload, ContentEntryUpdate, DateField, FieldPatch, ImageField, JsonObjectField,
    LocationField, Thing, ThingCollection, ThingCollectionEntry, ThingCollectionUpsert,
    ThingDatatype, ThingEntry, ThingId, ThingStatus, ThingUpsert, ThingsChangeLogEntry,
    ThingsContentSnapshot, ThingsMutationEvent, ThingsOperationType, ThingsSnapshot,
    ThingsSnapshotState, ThingsSyncSummary, ThingsUndoConflict, ThingsUndoConflictType,
    ThingsUndoExecution, ThingsUndoPreview, ThingsUndoResolutionOption, UrlField,
    format_domain_datetime, parse_domain_datetime, parse_domain_datetime_or_unix_epoch,
    parse_optional_domain_datetime,
};

use remi_things_crdt::{
    CURRENT_SCHEMA_VERSION,
    CollectionDocView,
    // V3 types
    CollectionOp,
    Content,
    CrdtDataType,
    ROOT_DOC_UUID,
    Schema,
    // V3 built-in fields (multi-value)
    ThingBuiltInFieldsUpdate,
    ThingContentView,
    ThingMarkdownOp,
    ThingMarkdownView,
    apply_collection_op,
    apply_thing_markdown_op,
    compact_collection_doc,
    compact_root_doc,
    compact_thing_content_doc,
    extract_collection_doc_view,
    extract_thing_content_view,
    extract_thing_markdown_view,
    // V3 compaction
    needs_compaction,
};

mod timestamps;
use timestamps::{
    extract_collection_card_jsx, extract_entity_timestamps, merge_entity_timestamps_into_attrs,
    resolve_entity_timestamps, strip_internal_timestamp_attrs,
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

#[derive(Debug, Clone)]
pub struct TreeCollectionData {
    pub uuid: String,
    pub title: String,
}

#[derive(Debug, Clone)]
pub struct TreeThingData {
    pub uuid: String,
    pub title: String,
    pub status: String,
    pub collection_uuid: String,
    pub parent_uuid: Option<String>,
    pub entries: Vec<ContentEntry>,
}

#[derive(Debug, Clone)]
pub struct ThingsTreeData {
    pub collections: Vec<TreeCollectionData>,
    pub things: Vec<TreeThingData>,
}

mod content_registry;
pub use content_registry::ContentTypeRegistry;

pub const COLLECTION_TYPE_ATTR_KEY: &str = "collection_type";
pub const COLLECTION_APP_ID_ATTR_KEY: &str = "app_id";
pub const ARCHIVED_AT_ATTR_KEY: &str = "archived_at";
pub const ARCHIVED_FROM_COLLECTION_UUID_ATTR_KEY: &str = "archived_from_collection_uuid";

fn normalize_entity_attrs_value(attrs: Option<Value>) -> Option<Value> {
    match attrs {
        Some(Value::Object(map)) if map.is_empty() => None,
        Some(value) => Some(value),
        None => None,
    }
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

mod document_store;
pub use document_store::DocumentKey;
pub(crate) use document_store::DocumentState;
use document_store::{DocumentStoreMut, DocumentStoreView};

mod reader;
use reader::ThingsDomainReader;

mod writer;
use writer::ThingsDomainWriter;

/// Manages the set of CRDT documents (Root, Collections, ThingMarkdown)
#[derive(Debug, Clone)]
pub(crate) struct ThingsDocumentSet {
    device_id: String,
    documents: HashMap<DocumentKey, DocumentState>,
}

impl ThingsDocumentSet {
    fn store_view(&self) -> DocumentStoreView<'_> {
        DocumentStoreView::new(&self.documents)
    }

    fn store_mut(&mut self) -> DocumentStoreMut<'_> {
        DocumentStoreMut::new(&self.device_id, &mut self.documents)
    }

    fn domain_reader(&self) -> ThingsDomainReader<'_> {
        ThingsDomainReader::new(self.store_view())
    }

    fn domain_writer(&mut self) -> ThingsDomainWriter<'_> {
        ThingsDomainWriter::new(&self.device_id, &mut self.documents)
    }

    /// Create a new empty document set
    pub(crate) fn new(device_id: &str) -> Self {
        Self {
            device_id: device_id.to_string(),
            documents: HashMap::new(),
        }
    }

    /// Initialize with a root document
    pub(crate) fn init_root(&mut self) -> Result<()> {
        self.domain_writer().init_root()
    }

    pub(crate) fn has_root_document(&self) -> bool {
        self.documents.contains_key(&DocumentKey::root())
    }

    /// Get a document by key
    pub(crate) fn get(&self, key: &DocumentKey) -> Option<&DocumentState> {
        self.documents.get(key)
    }

    /// Insert or update a document
    pub(crate) fn set(&mut self, key: DocumentKey, state: DocumentState) {
        self.store_mut().set(key, state);
    }

    /// Get all dirty documents, ordered by sync priority
    fn dirty_documents(&self) -> Vec<(&DocumentKey, &DocumentState)> {
        let mut dirty: Vec<_> = self
            .documents
            .iter()
            .filter(|(_, state)| state.dirty)
            .collect();
        dirty.sort_by_key(|(key, _)| key.data_type.sync_priority());
        dirty
    }

    pub(crate) fn dirty_document_keys_public(&self) -> Vec<DocumentKey> {
        self.dirty_documents()
            .into_iter()
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Return collection UUIDs that are still live according to collection documents.
    pub(crate) fn active_collection_uuids(&self) -> Result<HashSet<String>> {
        self.domain_reader().active_collection_uuids()
    }

    /// Return thing UUIDs that are still reachable through live collection documents.
    pub(crate) fn active_thing_uuids(&self) -> Result<HashSet<String>> {
        self.domain_reader().active_thing_uuids()
    }

    pub(crate) fn active_content_document_uuids(&self) -> Result<HashSet<String>> {
        self.domain_reader().active_content_document_uuids()
    }

    // ===== V3 Compaction =====

    /// Try to compact a document if it exceeds the specified threshold.
    /// Returns true if compaction was performed.
    pub(crate) fn maybe_compact_with_threshold(
        &mut self,
        key: &DocumentKey,
        threshold: usize,
    ) -> Result<bool> {
        self.store_mut()
            .maybe_compact_with_threshold(key, threshold)
    }

    // ===== Collection Operations =====

    #[cfg(test)]
    pub(crate) fn update_collection_meta(
        &mut self,
        collection_uuid: &str,
        title: Option<String>,
        status: Option<String>,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        self.update_collection_meta_with_timestamps(collection_uuid, title, status, None, None)
    }

    /// Get or create a collection document
    pub(crate) fn get_or_init_collection(
        &mut self,
        collection_uuid: &str,
    ) -> Result<&DocumentState> {
        self.domain_writer()
            .get_or_init_collection(collection_uuid)?;
        let key = DocumentKey::collection(collection_uuid);
        Ok(self.documents.get(&key).unwrap())
    }

    pub(crate) fn update_collection_meta_with_timestamps(
        &mut self,
        collection_uuid: &str,
        title: Option<String>,
        status: Option<String>,
        created_at: Option<String>,
        updated_at: Option<String>,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let existed = self.collection_is_live(collection_uuid)?;
        let existing_attrs = if existed {
            Some(self.collection_view(collection_uuid)?.meta.attrs)
        } else {
            None
        };
        let timestamps = resolve_entity_timestamps(
            existing_attrs.as_ref().and_then(|attrs| attrs.as_ref()),
            created_at,
            updated_at,
            existed,
        )?;
        let attrs_json = merge_entity_timestamps_into_attrs(
            existing_attrs.as_ref().and_then(|attrs| attrs.as_ref()),
            timestamps.created_at,
            timestamps.updated_at,
        )?;
        self.domain_writer().update_collection_meta(
            collection_uuid,
            title,
            status,
            Some(attrs_json),
        )?;

        let mut events = Vec::new();
        if !existed {
            events.push(ThingsDocumentEvent::root(ThingsDocumentChangeKind::Updated));
        }
        events.push(ThingsDocumentEvent::collection(
            if existed {
                ThingsDocumentChangeKind::Updated
            } else {
                ThingsDocumentChangeKind::Created
            },
            collection_uuid,
        ));
        Ok(events)
    }

    pub(crate) fn update_collection_attrs(
        &mut self,
        collection_uuid: &str,
        attrs: Option<Value>,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let existed = self.collection_is_live(collection_uuid)?;
        if !existed {
            anyhow::bail!("Collection not found: {collection_uuid}");
        }

        let existing_attrs = self.collection_view(collection_uuid)?.meta.attrs;
        let timestamps = resolve_entity_timestamps(existing_attrs.as_ref(), None, None, true)?;
        let normalized_attrs = normalize_entity_attrs_value(attrs);
        let attrs_json = merge_entity_timestamps_into_attrs(
            normalized_attrs.as_ref(),
            timestamps.created_at,
            timestamps.updated_at,
        )?;

        self.domain_writer().update_collection_meta(
            collection_uuid,
            None,
            None,
            Some(attrs_json),
        )?;

        Ok(vec![ThingsDocumentEvent::collection(
            ThingsDocumentChangeKind::Updated,
            collection_uuid,
        )])
    }

    /// Upsert a thing in a collection
    pub(crate) fn upsert_thing_meta(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        datatype: Option<ThingDatatype>,
        status: Option<String>,
        title: Option<String>,
        parent_uuid: Option<String>,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        self.upsert_thing_meta_with_timestamps(
            collection_uuid,
            thing_uuid,
            datatype,
            status,
            title,
            parent_uuid,
            None,
            None,
        )
    }

    pub(crate) fn upsert_thing_meta_with_timestamps(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        datatype: Option<ThingDatatype>,
        status: Option<String>,
        title: Option<String>,
        parent_uuid: Option<String>,
        created_at: Option<String>,
        updated_at: Option<String>,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let existed = self.thing_is_live_in_collection(collection_uuid, thing_uuid)?;
        let existing_attrs = if existed {
            self.collection_view(collection_uuid)?
                .things
                .into_iter()
                .find(|thing| thing.id == thing_uuid)
                .and_then(|thing| thing.attrs)
        } else {
            None
        };
        let timestamps =
            resolve_entity_timestamps(existing_attrs.as_ref(), created_at, updated_at, existed)?;
        let attrs_json = merge_entity_timestamps_into_attrs(
            existing_attrs.as_ref(),
            timestamps.created_at,
            timestamps.updated_at,
        )?;
        self.domain_writer().upsert_thing_meta(
            collection_uuid,
            thing_uuid,
            datatype,
            status,
            title,
            parent_uuid,
            Some(attrs_json),
        )?;

        Ok(vec![ThingsDocumentEvent::thing(
            if existed {
                ThingsDocumentChangeKind::Updated
            } else {
                ThingsDocumentChangeKind::Created
            },
            collection_uuid,
            thing_uuid,
        )])
    }

    pub(crate) fn update_thing_attrs(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        attrs: Option<Value>,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let existed = self.thing_is_live_in_collection(collection_uuid, thing_uuid)?;
        if !existed {
            anyhow::bail!("Thing not found: {thing_uuid}");
        }

        let existing_attrs = self
            .collection_view(collection_uuid)?
            .things
            .into_iter()
            .find(|thing| thing.id == thing_uuid)
            .and_then(|thing| thing.attrs);
        let timestamps = resolve_entity_timestamps(existing_attrs.as_ref(), None, None, true)?;
        let normalized_attrs = normalize_entity_attrs_value(attrs);
        let attrs_json = merge_entity_timestamps_into_attrs(
            normalized_attrs.as_ref(),
            timestamps.created_at,
            timestamps.updated_at,
        )?;

        self.domain_writer().upsert_thing_meta(
            collection_uuid,
            thing_uuid,
            None,
            None,
            None,
            None,
            Some(attrs_json),
        )?;

        Ok(vec![ThingsDocumentEvent::thing(
            ThingsDocumentChangeKind::Updated,
            collection_uuid,
            thing_uuid,
        )])
    }

    /// Delete a thing from a collection
    pub(crate) fn delete_thing(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        if !self.thing_is_live_in_collection(collection_uuid, thing_uuid)? {
            return Ok(Vec::new());
        }

        self.domain_writer()
            .delete_thing(collection_uuid, thing_uuid)?;
        Ok(vec![ThingsDocumentEvent::thing(
            ThingsDocumentChangeKind::Deleted,
            collection_uuid,
            thing_uuid,
        )])
    }

    /// Add a content entry to a thing (V3 multi-value)
    pub(crate) fn add_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry: ContentEntry,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let entry_id = entry.id.clone();
        let existed = self.content_entry_exists(collection_uuid, thing_uuid, &entry_id)?;
        self.domain_writer()
            .add_content_entry(collection_uuid, thing_uuid, entry)?;
        Ok(vec![ThingsDocumentEvent::content_entry(
            if existed {
                ThingsDocumentChangeKind::Updated
            } else {
                ThingsDocumentChangeKind::Created
            },
            collection_uuid,
            thing_uuid,
            &entry_id,
        )])
    }

    /// Update a content entry on a thing (V3 multi-value)
    pub(crate) fn update_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry_id: &str,
        title: Option<Option<String>>,
        order: Option<f64>,
        payload: Option<ContentEntryPayload>,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let existed = self.content_entry_exists(collection_uuid, thing_uuid, entry_id)?;
        self.domain_writer().update_content_entry(
            collection_uuid,
            thing_uuid,
            entry_id,
            title,
            order,
            payload,
        )?;
        Ok(vec![ThingsDocumentEvent::content_entry(
            if existed {
                ThingsDocumentChangeKind::Updated
            } else {
                ThingsDocumentChangeKind::Created
            },
            collection_uuid,
            thing_uuid,
            entry_id,
        )])
    }

    /// Delete a content entry from a thing (V3 multi-value)
    pub(crate) fn delete_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        if !self.content_entry_exists(collection_uuid, thing_uuid, entry_id)? {
            return Ok(Vec::new());
        }

        self.domain_writer()
            .delete_content_entry(collection_uuid, thing_uuid, entry_id)?;
        Ok(vec![ThingsDocumentEvent::content_entry(
            ThingsDocumentChangeKind::Deleted,
            collection_uuid,
            thing_uuid,
            entry_id,
        )])
    }

    /// Get content entries for a thing
    pub(crate) fn get_content_entries(
        &self,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Result<Vec<ContentEntry>> {
        self.domain_reader()
            .get_content_entries(collection_uuid, thing_uuid)
    }

    /// Find which collection a thing belongs to by scanning all collection documents.
    ///
    /// This is more robust than `extract_snapshot()` because it does **not** depend on
    /// the root document listing the collection. It's useful when the root <-> collection
    /// linkage might be stale (e.g. after sync or migration).
    pub(crate) fn find_thing_collection_uuid(&self, thing_uuid: &str) -> Option<String> {
        self.domain_reader().find_thing_collection_uuid(thing_uuid)
    }

    /// Get collection view
    pub(crate) fn collection_view(&self, collection_uuid: &str) -> Result<CollectionDocView> {
        self.domain_reader().collection_view(collection_uuid)
    }

    fn collection_is_live(&self, collection_uuid: &str) -> Result<bool> {
        let key = DocumentKey::collection(collection_uuid);
        let Some(state) = self.documents.get(&key) else {
            return Ok(false);
        };
        let view = extract_collection_doc_view(&state.automerge_doc, collection_uuid)?;
        Ok(!view
            .meta
            .tombstone
            .as_ref()
            .map(|t| t.deleted)
            .unwrap_or(false))
    }

    fn thing_is_live_in_collection(&self, collection_uuid: &str, thing_uuid: &str) -> Result<bool> {
        let view = self.collection_view(collection_uuid)?;
        if view
            .meta
            .tombstone
            .as_ref()
            .map(|t| t.deleted)
            .unwrap_or(false)
        {
            return Ok(false);
        }

        Ok(view.things.iter().any(|thing| {
            thing.id == thing_uuid && !thing.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false)
        }))
    }

    fn content_entry_exists(
        &self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<bool> {
        Ok(self
            .get_content_entries(collection_uuid, thing_uuid)?
            .iter()
            .any(|entry| entry.id == entry_id))
    }

    fn thing_markdown_document_exists(&self, thing_uuid: &str) -> bool {
        self.documents
            .contains_key(&DocumentKey::thing_content(thing_uuid))
    }

    // ===== ThingMarkdown Operations =====

    /// Set content on a thing markdown document from a typed payload.
    pub(crate) fn set_thing_content_from_payload(
        &mut self,
        thing_uuid: &str,
        datatype: &ThingDatatype,
        payload: &Value,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let content = ContentTypeRegistry::new().markdown_content_from_value(datatype, payload);
        let existed = self.thing_markdown_document_exists(thing_uuid);
        self.domain_writer()
            .set_thing_content(thing_uuid, content)?;
        Ok(vec![ThingsDocumentEvent::thing_markdown(
            if existed {
                ThingsDocumentChangeKind::Updated
            } else {
                ThingsDocumentChangeKind::Created
            },
            self.find_thing_collection_uuid(thing_uuid).as_deref(),
            thing_uuid,
        )])
    }

    /// Reconcile built-in content entries from a snapshot-style payload.
    ///
    /// If the payload does not include `built_in.content_entries`, this is a no-op.
    pub(crate) fn sync_content_entries_from_snapshot_payload(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        payload: &Value,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        let Some(entries_value) = payload
            .get("built_in")
            .and_then(|built_in| built_in.get("content_entries"))
        else {
            return Ok(Vec::new());
        };

        if !entries_value.is_array() {
            anyhow::bail!("built_in.content_entries must be an array when present");
        }

        let desired_entries =
            ContentTypeRegistry::new().extract_content_entries_from_snapshot_data(payload)?;
        let existing_entries = self.get_content_entries(collection_uuid, thing_uuid)?;
        let existing_by_id = existing_entries
            .iter()
            .map(|entry| (entry.id.clone(), entry.clone()))
            .collect::<HashMap<_, _>>();
        let desired_ids = desired_entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<HashSet<_>>();

        let mut events = Vec::new();

        for existing_entry in &existing_entries {
            if !desired_ids.contains(&existing_entry.id) {
                events.extend(self.delete_content_entry(
                    collection_uuid,
                    thing_uuid,
                    &existing_entry.id,
                )?);
            }
        }

        for desired_entry in desired_entries {
            match existing_by_id.get(&desired_entry.id) {
                Some(existing_entry) => {
                    let title = (existing_entry.title != desired_entry.title)
                        .then_some(desired_entry.title.clone());
                    let order = (existing_entry.order != desired_entry.order)
                        .then_some(desired_entry.order);
                    let payload = (existing_entry.payload != desired_entry.payload)
                        .then_some(desired_entry.payload.clone());

                    if title.is_some() || order.is_some() || payload.is_some() {
                        events.extend(self.update_content_entry(
                            collection_uuid,
                            thing_uuid,
                            &desired_entry.id,
                            title,
                            order,
                            payload,
                        )?);
                    }
                }
                None => {
                    events.extend(self.add_content_entry(
                        collection_uuid,
                        thing_uuid,
                        desired_entry,
                    )?);
                }
            }
        }

        Ok(events)
    }

    /// Set plain markdown text on a thing using the default markdown payload shape.
    pub(crate) fn set_thing_markdown_text(
        &mut self,
        thing_uuid: &str,
        text: &str,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        self.set_thing_content_from_payload(
            thing_uuid,
            &ThingDatatype::Markdown,
            &json!({ "markdown": text }),
        )
    }

    /// Splice text and report whether the markdown content actually changed.
    pub(crate) fn try_splice_thing_text(
        &mut self,
        thing_uuid: &str,
        block_id: &str,
        index: usize,
        delete: usize,
        insert: &str,
    ) -> Result<Option<Vec<ThingsDocumentEvent>>> {
        let before = self.domain_reader().thing_markdown_view(thing_uuid)?;
        let had_content = before.content.is_some();
        let existed = self.thing_markdown_document_exists(thing_uuid);

        let missing_primary_block = block_id == "main"
            && before
                .content
                .as_ref()
                .and_then(|content| content.blocks.as_ref())
                .map(|blocks| !blocks.iter().any(|block| block.id == "main"))
                .unwrap_or(true);

        if missing_primary_block {
            if index == 0 && (delete == 0 || delete == usize::MAX) {
                return Ok(Some(self.replace_thing_markdown_text(thing_uuid, insert)?));
            }

            return Ok(None);
        }

        self.domain_writer()
            .splice_thing_text(thing_uuid, block_id, index, delete, insert)?;

        let after = self.domain_reader().thing_markdown_view(thing_uuid)?;
        if before.content != after.content || !had_content {
            Ok(Some(vec![ThingsDocumentEvent::thing_markdown(
                if existed {
                    ThingsDocumentChangeKind::Updated
                } else {
                    ThingsDocumentChangeKind::Created
                },
                self.find_thing_collection_uuid(thing_uuid).as_deref(),
                thing_uuid,
            )]))
        } else {
            Ok(None)
        }
    }

    /// Replace the entire primary markdown block for a thing.
    pub(crate) fn replace_thing_markdown_text(
        &mut self,
        thing_uuid: &str,
        text: &str,
    ) -> Result<Vec<ThingsDocumentEvent>> {
        self.set_thing_markdown_text(thing_uuid, text)
    }

    /// Get plain markdown text from the primary markdown block, if present.
    pub(crate) fn get_thing_markdown_text(&self, thing_uuid: &str) -> Result<Option<String>> {
        let md_view = self.domain_reader().thing_markdown_view(thing_uuid)?;
        Ok(md_view.content.and_then(|content| {
            content.blocks.and_then(|blocks| {
                blocks
                    .into_iter()
                    .find(|block| block.id == "main")
                    .and_then(|block| block.text)
            })
        }))
    }

    pub(crate) fn extract_tree_data(&self) -> Result<ThingsTreeData> {
        let mut collections = Vec::new();
        let mut things = Vec::new();

        let mut collection_views = Vec::new();
        for (key, state) in self.documents.iter() {
            if key.data_type != CrdtDataType::Collection {
                continue;
            }

            let view = extract_collection_doc_view(&state.automerge_doc, &key.uuid)?;
            let deleted = view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false);
            if deleted {
                continue;
            }

            collection_views.push((key.uuid.clone(), view));
        }

        collection_views.sort_by(|(left, _), (right, _)| left.cmp(right));

        for (collection_uuid, coll_view) in collection_views {
            let deleted = coll_view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false);
            if deleted {
                continue;
            }

            collections.push(TreeCollectionData {
                uuid: collection_uuid.clone(),
                title: coll_view.meta.title.clone(),
            });

            for thing_meta in coll_view.things {
                let thing_deleted = thing_meta
                    .tombstone
                    .as_ref()
                    .map(|t| t.deleted)
                    .unwrap_or(false);
                if thing_deleted {
                    continue;
                }

                things.push(TreeThingData {
                    uuid: thing_meta.id.clone(),
                    title: thing_meta.title.clone().unwrap_or_default(),
                    status: thing_meta.status.as_storage_str().to_string(),
                    collection_uuid: collection_uuid.clone(),
                    parent_uuid: thing_meta.parent_id.clone(),
                    entries: thing_meta.built_in.content_entries.clone(),
                });
            }
        }

        Ok(ThingsTreeData {
            collections,
            things,
        })
    }

    // ===== Snapshot Generation =====

    pub(crate) fn set_thing_json_content(
        &mut self,
        document_uuid: &str,
        thing_uuid: &str,
        content_type: &str,
        value: &Value,
    ) -> Result<()> {
        let payload_json = serde_json::to_string(value)
            .context("Failed to serialize thing content JSON payload")?;
        self.domain_writer().set_thing_content_document(
            document_uuid,
            thing_uuid,
            content_type,
            Content::Opaque {
                kind: content_type.to_string(),
                payload_json,
            },
        )
    }

    pub(crate) fn get_thing_json_content(
        &self,
        document_uuid: &str,
        thing_uuid: &str,
    ) -> Result<Option<Value>> {
        let Some(view) = self
            .domain_reader()
            .thing_content_view(document_uuid, thing_uuid)?
        else {
            return Ok(None);
        };

        Ok(view.content.and_then(|content| content.payload))
    }

    /// Extract a full snapshot from all documents
    pub(crate) fn extract_snapshot(&self) -> Result<ThingsSnapshot> {
        self.domain_reader().extract_snapshot()
    }

    /// Extract a snapshot with options
    pub(crate) fn extract_snapshot_with_options(
        &self,
        options: SnapshotOptions,
    ) -> Result<ThingsSnapshot> {
        self.domain_reader().extract_snapshot_with_options(options)
    }
}

mod persistence;
pub(crate) use persistence::DocumentPersistence;

#[cfg(test)]
mod tests;
