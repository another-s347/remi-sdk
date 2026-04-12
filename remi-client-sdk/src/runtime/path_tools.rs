use super::TriggerSdk;
use crate::things_crdt::{
    ContentEntry, ContentEntryPayload, ContentEntryUpdate, ImageField, ThingCollectionUpsert,
    ThingDatatype, ThingUpsert,
};
use crate::types::{
    TriggerRegistration, TriggerRule, VirtualFsNodeKind, VirtualFsProfileResult,
    VirtualFsProfileStep, VirtualFsReadResult,
};
use anyhow::{Context, Result, anyhow};
use serde_json::{Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

const ROOT_PATH: &str = "/";
const TRIGGER_PREVIEW_LIMIT: usize = 5;
const ENTRY_REFERENCE_SCHEME: &str = "remi-entry://";

#[derive(Debug, Clone, PartialEq, Eq)]
enum VirtualPath {
    Root,
    TriggerRoot,
    TriggerDir { trigger_uuid: String },
    TriggerName { trigger_uuid: String },
    TriggerRule { trigger_uuid: String },
    CollectionRoot,
    CollectionDir { collection_uuid: String },
    CollectionName { collection_uuid: String },
    CollectionTriggerUuid { collection_uuid: String },
    CollectionThingsDir { collection_uuid: String },
    ThingDir { collection_uuid: String, thing_uuid: String },
    ThingName { collection_uuid: String, thing_uuid: String },
    ThingTriggerUuid { collection_uuid: String, thing_uuid: String },
    ThingStatus { collection_uuid: String, thing_uuid: String },
    ThingContent { collection_uuid: String, thing_uuid: String },
    ThingEntry { collection_uuid: String, thing_uuid: String, index: usize },
    ThingEntryData { collection_uuid: String, thing_uuid: String, index: usize },
    ThingEntrySchema { collection_uuid: String, thing_uuid: String, index: usize },
    ThingChildrenDir { collection_uuid: String, thing_uuid: String },
}

pub(crate) enum VirtualFsCatResult {
    Text(VirtualFsReadResult),
    Image {
        uri: String,
    },
}

#[derive(Debug, Clone)]
struct TreeNode {
    label: String,
    children: Vec<TreeNode>,
}

struct TreeIndex<'a> {
    collections_by_uuid: HashMap<&'a str, &'a crate::things_crdt::TreeCollectionData>,
    things_by_key: HashMap<(&'a str, &'a str), &'a crate::things_crdt::TreeThingData>,
    child_things: HashMap<(&'a str, Option<&'a str>), Vec<&'a crate::things_crdt::TreeThingData>>,
    parents_with_children: HashSet<(&'a str, &'a str)>,
}

impl<'a> TreeIndex<'a> {
    fn build(tree_data: &'a crate::things_crdt::ThingsTreeData) -> Self {
        let mut collections_by_uuid = HashMap::with_capacity(tree_data.collections.len());
        for collection in &tree_data.collections {
            collections_by_uuid.insert(collection.uuid.as_str(), collection);
        }

        let mut things_by_key = HashMap::with_capacity(tree_data.things.len());
        let mut child_things: HashMap<
            (&'a str, Option<&'a str>),
            Vec<&'a crate::things_crdt::TreeThingData>,
        > = HashMap::new();
        let mut parents_with_children = HashSet::new();

        for thing in &tree_data.things {
            let collection_uuid = thing.collection_uuid.as_str();
            let parent_uuid = thing.parent_uuid.as_deref();
            things_by_key.insert((collection_uuid, thing.uuid.as_str()), thing);
            child_things
                .entry((collection_uuid, parent_uuid))
                .or_default()
                .push(thing);

            if let Some(parent_uuid) = parent_uuid {
                parents_with_children.insert((collection_uuid, parent_uuid));
            }
        }

        Self {
            collections_by_uuid,
            things_by_key,
            child_things,
            parents_with_children,
        }
    }

    fn collection(&self, collection_uuid: &str) -> Option<&'a crate::things_crdt::TreeCollectionData> {
        self.collections_by_uuid.get(collection_uuid).copied()
    }

    fn thing(
        &self,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Option<&'a crate::things_crdt::TreeThingData> {
        self.things_by_key.get(&(collection_uuid, thing_uuid)).copied()
    }

    fn child_things(
        &self,
        collection_uuid: &str,
        parent_uuid: Option<&str>,
    ) -> Vec<&'a crate::things_crdt::TreeThingData> {
        self.child_things
            .get(&(collection_uuid, parent_uuid))
            .cloned()
            .unwrap_or_default()
    }

    fn has_children(&self, collection_uuid: &str, thing_uuid: &str) -> bool {
        self.parents_with_children.contains(&(collection_uuid, thing_uuid))
    }
}

impl TreeNode {
    fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            children: Vec::new(),
        }
    }

    fn with_children(label: impl Into<String>, children: Vec<TreeNode>) -> Self {
        Self {
            label: label.into(),
            children,
        }
    }
}

impl TriggerSdk {
    pub fn ls_virtual_path(&self, device_id: &str, path: Option<&str>) -> Result<String> {
        self.tree_virtual_path(device_id, path)
    }

    pub fn tree_virtual_path(&self, device_id: &str, path: Option<&str>) -> Result<String> {
        let path = normalize_path(path.unwrap_or(ROOT_PATH))?;
        let parsed = parse_virtual_path(&path)?;
        let node = self.build_tree_node(device_id, &parsed)?;
        Ok(render_tree(&node))
    }

    pub fn profile_tree_virtual_path(
        &self,
        device_id: &str,
        path: Option<&str>,
    ) -> Result<VirtualFsProfileResult> {
        let total_started = Instant::now();
        let mut steps = Vec::new();

        let parse_started = Instant::now();
        let path = normalize_path(path.unwrap_or(ROOT_PATH))?;
        let parsed = parse_virtual_path(&path)?;
        push_profile_step(&mut steps, "normalize_parse", parse_started.elapsed());

        let triggers_started = Instant::now();
        let triggers = self.list_triggers()?;
        push_profile_step(&mut steps, "list_triggers", triggers_started.elapsed());

        let load_started = Instant::now();
        let doc_set = self.get_or_init_document_set(device_id)?;
        push_profile_step(&mut steps, "load_document_set", load_started.elapsed());

        let tree_data_started = Instant::now();
        let tree_data = doc_set.extract_tree_data()?;
        push_profile_step(&mut steps, "extract_tree_data", tree_data_started.elapsed());

        let render_started = Instant::now();
        let node = self.build_tree_node_from_tree_data(&parsed, &triggers, &tree_data)?;
        let rendered = render_tree(&node);
        push_profile_step(&mut steps, "render_tree", render_started.elapsed());

        Ok(VirtualFsProfileResult {
            operation: "tree_virtual_path".to_string(),
            path,
            total_ms: total_started.elapsed().as_millis() as u64,
            output_bytes: rendered.len(),
            steps,
        })
    }

    pub(crate) fn cat_virtual_path(&self, device_id: &str, path: &str) -> Result<VirtualFsCatResult> {
        let path = normalize_path(path)?;
        let parsed = parse_virtual_path(&path)?;

        if let VirtualPath::ThingEntry { thing_uuid, index, .. } = &parsed {
            let entry = self.content_entry_by_index(device_id, thing_uuid, *index)?;
            if let ContentEntryPayload::Image(image) = entry.payload {
                return Ok(VirtualFsCatResult::Image {
                    uri: image.uri,
                });
            }
        }

        Ok(VirtualFsCatResult::Text(self.read_virtual_path(device_id, &path)?))
    }

    pub fn read_virtual_path(&self, device_id: &str, path: &str) -> Result<VirtualFsReadResult> {
        let path = normalize_path(path)?;
        let parsed = parse_virtual_path(&path)?;
        let read = self.read_virtual_path_inner(device_id, &parsed)?;
        Ok(VirtualFsReadResult {
            path,
            kind: VirtualFsNodeKind::File,
            content: read,
        })
    }

    pub fn profile_read_virtual_path(
        &self,
        device_id: &str,
        path: &str,
    ) -> Result<VirtualFsProfileResult> {
        let total_started = Instant::now();
        let mut steps = Vec::new();

        let parse_started = Instant::now();
        let path = normalize_path(path)?;
        let parsed = parse_virtual_path(&path)?;
        push_profile_step(&mut steps, "normalize_parse", parse_started.elapsed());

        let output = match &parsed {
            VirtualPath::ThingContent {
                collection_uuid,
                thing_uuid,
            } => {
                let load_started = Instant::now();
                let doc_set = self.get_or_init_document_set(device_id)?;
                push_profile_step(&mut steps, "load_document_set", load_started.elapsed());

                let markdown_started = Instant::now();
                let rendered = self.render_thing_content_from_doc_set(&doc_set, collection_uuid, thing_uuid)?;
                push_profile_step(&mut steps, "render_content_markdown", markdown_started.elapsed());
                rendered
            }
            _ => {
                let read_started = Instant::now();
                let result = self.read_virtual_path(device_id, &path)?;
                push_profile_step(&mut steps, "read_virtual_path_total", read_started.elapsed());
                result.content
            }
        };

        Ok(VirtualFsProfileResult {
            operation: "read_virtual_path".to_string(),
            path,
            total_ms: total_started.elapsed().as_millis() as u64,
            output_bytes: output.len(),
            steps,
        })
    }

    pub fn edit_virtual_path(
        &self,
        device_id: &str,
        path: &str,
        operation: &str,
        value: Option<&JsonValue>,
        old_str: Option<&str>,
        new_str: Option<&str>,
        line_number: Option<usize>,
    ) -> Result<JsonValue> {
        let path = normalize_path(path)?;
        let parsed = parse_virtual_path(&path)?;
        let operation = normalize_operation(operation);

        let result = match parsed {
            VirtualPath::TriggerName { ref trigger_uuid } => {
                let new_name = value
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| friendly_anyhow(&path, "invalid_value", "Editing trigger name requires a string value."))?;
                self.update_trigger_name(trigger_uuid, new_name)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated trigger name for '{}'", trigger_uuid),
                    "value": new_name,
                })
            }
            VirtualPath::TriggerRule { ref trigger_uuid } => {
                let rule = parse_rule_json(&path, value)?;
                self.update_trigger_rule(trigger_uuid, &rule)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated trigger rule for '{}'", trigger_uuid),
                    "value": rule,
                })
            }
            VirtualPath::CollectionName { ref collection_uuid } => {
                let title = value
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| friendly_anyhow(&path, "invalid_value", "Editing collection name requires a string value."))?;
                self.rename_collection(device_id, collection_uuid, title)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated collection name for '{}'", collection_uuid),
                    "value": title,
                })
            }
            VirtualPath::CollectionTriggerUuid { ref collection_uuid } => {
                let trigger_uuid = value
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| friendly_anyhow(&path, "invalid_value", "Editing collection trigger requires a string value. Use an empty string to clear the binding."))?;
                self.things_set_collection_trigger_uuid(
                    device_id,
                    collection_uuid,
                    if trigger_uuid.trim().is_empty() { Some("") } else { Some(trigger_uuid) },
                )?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated collection trigger binding for '{}'", collection_uuid),
                    "value": trigger_uuid,
                })
            }
            VirtualPath::ThingName { ref thing_uuid, .. } => {
                let title = value
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| friendly_anyhow(&path, "invalid_value", "Editing thing name requires a string value."))?;
                self.rename_thing(device_id, thing_uuid, title)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated thing name for '{}'", thing_uuid),
                    "value": title,
                })
            }
            VirtualPath::ThingTriggerUuid { ref thing_uuid, .. } => {
                let trigger_uuid = value
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| friendly_anyhow(&path, "invalid_value", "Editing thing trigger requires a string value. Use an empty string to clear the binding."))?;
                self.things_set_thing_trigger_uuid(
                    device_id,
                    thing_uuid,
                    if trigger_uuid.trim().is_empty() { Some("") } else { Some(trigger_uuid) },
                )?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated thing trigger binding for '{}'", thing_uuid),
                    "value": trigger_uuid,
                })
            }
            VirtualPath::ThingStatus { ref thing_uuid, .. } => {
                let status = value
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| friendly_anyhow(&path, "invalid_value", "Editing thing status requires a string value."))?;
                self.set_thing_status(device_id, thing_uuid, status)
                    .with_context(|| format!("Failed to update thing status for {thing_uuid}"))?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated thing status for '{}'", thing_uuid),
                    "value": status,
                })
            }
            VirtualPath::ThingContent { ref thing_uuid, .. } => {
                self.edit_thing_content_path(
                    device_id,
                    &path,
                    thing_uuid,
                    operation,
                    value,
                    old_str,
                    new_str,
                    line_number,
                )?
            }
            VirtualPath::ThingEntry {
                ref thing_uuid,
                index,
                ..
            } => {
                let entry_value = value.ok_or_else(|| {
                    friendly_anyhow(
                        &path,
                        "invalid_value",
                        "Editing an entry requires an object value with optional title, order, and payload.",
                    )
                })?;
                self.edit_thing_entry_path(device_id, &path, thing_uuid, index, entry_value)?
            }
            VirtualPath::ThingEntryData {
                ref thing_uuid,
                index,
                ..
            } => self.edit_thing_entry_data_path(device_id, &path, thing_uuid, index, operation, value)?,
            VirtualPath::ThingEntrySchema {
                ref thing_uuid,
                index,
                ..
            } => self.edit_thing_entry_schema_path(device_id, &path, thing_uuid, index, operation, value)?,
            VirtualPath::Root
            | VirtualPath::TriggerRoot
            | VirtualPath::TriggerDir { .. }
            | VirtualPath::CollectionRoot
            | VirtualPath::CollectionDir { .. }
            | VirtualPath::CollectionThingsDir { .. }
            | VirtualPath::ThingDir { .. }
            | VirtualPath::ThingChildrenDir { .. }
            => {
                return Err(friendly_anyhow(
                    &path,
                    "is_directory",
                    "The target path is a directory. Use tree_tool for listing or target a file node such as name, status, content.md, entries.{idx}, entries.{idx}.data.json, entries.{idx}.schema.json, or rule.json.",
                ));
            }
        };

        Ok(result)
    }

    pub fn delete_virtual_path(&self, device_id: &str, path: &str) -> Result<JsonValue> {
        let path = normalize_path(path)?;
        let parsed = parse_virtual_path(&path)?;

        let result = match parsed {
            VirtualPath::TriggerDir { ref trigger_uuid } => {
                let deleted = self.delete_trigger_and_bindings(device_id, trigger_uuid)?;
                json!({
                    "ok": deleted,
                    "path": path,
                    "message": if deleted {
                        format!("Deleted trigger '{}'", trigger_uuid)
                    } else {
                        format!("Trigger '{}' was already absent", trigger_uuid)
                    },
                })
            }
            VirtualPath::CollectionDir { ref collection_uuid } => {
                let deleted = self.things_delete_collection(device_id, collection_uuid)?;
                json!({
                    "ok": deleted,
                    "path": path,
                    "message": if deleted {
                        format!("Deleted collection '{}'", collection_uuid)
                    } else {
                        format!("Collection '{}' was already absent", collection_uuid)
                    },
                })
            }
            VirtualPath::ThingDir {
                ref collection_uuid,
                ref thing_uuid,
            } => {
                let deleted = self.things_delete_thing(device_id, collection_uuid, thing_uuid)?;
                json!({
                    "ok": deleted,
                    "path": path,
                    "message": if deleted {
                        format!("Deleted thing '{}'", thing_uuid)
                    } else {
                        format!("Thing '{}' was already absent", thing_uuid)
                    },
                })
            }
            VirtualPath::ThingEntry {
                ref thing_uuid,
                index,
                ..
            } => {
                let entry = self.content_entry_by_index(device_id, thing_uuid, index).with_context(|| {
                    format!("Failed to resolve content entry at '{}'", path)
                })?;
                self.things_delete_content_entry(device_id, thing_uuid, &entry.id)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Deleted content entry {} from thing '{}'", index, thing_uuid),
                    "deleted_entry_id": entry.id,
                })
            }
            VirtualPath::ThingEntryData { .. } | VirtualPath::ThingEntrySchema { .. } => {
                return Err(friendly_anyhow(
                    &path,
                    "delete_unsupported",
                    "Delete the entry shell path entries.{idx} to remove a json_object entry and its associated schema/data documents.",
                ));
            }
            VirtualPath::Root
            | VirtualPath::TriggerRoot
            | VirtualPath::TriggerName { .. }
            | VirtualPath::TriggerRule { .. }
            | VirtualPath::CollectionRoot
            | VirtualPath::CollectionName { .. }
            | VirtualPath::CollectionTriggerUuid { .. }
            | VirtualPath::CollectionThingsDir { .. }
            | VirtualPath::ThingName { .. }
            | VirtualPath::ThingTriggerUuid { .. }
            | VirtualPath::ThingStatus { .. }
            | VirtualPath::ThingContent { .. }
            | VirtualPath::ThingChildrenDir { .. } => {
                return Err(friendly_anyhow(
                    &path,
                    "delete_unsupported",
                    "Delete only supports entity directories (/trigger/{uuid}, /collection/{uuid}, /collection/{collection_uuid}/things/{thing_uuid}) or entry files (/entries.{idx}).",
                ));
            }
        };

        Ok(result)
    }

    pub fn move_virtual_path(
        &self,
        device_id: &str,
        from_path: &str,
        to_path: &str,
    ) -> Result<JsonValue> {
        let from_path = normalize_path(from_path)?;
        let to_path = normalize_path(to_path)?;
        let from = parse_virtual_path(&from_path)?;
        let to = parse_virtual_path(&to_path)?;

        let (source_collection_uuid, thing_uuid) = match from {
            VirtualPath::ThingDir {
                collection_uuid,
                thing_uuid,
            } => (collection_uuid, thing_uuid),
            VirtualPath::TriggerDir { .. } | VirtualPath::TriggerName { .. } | VirtualPath::TriggerRule { .. } => {
                return Err(friendly_anyhow(
                    &from_path,
                    "move_unsupported",
                    "Trigger paths do not support move. Edit name or rule.json, or delete and recreate the trigger binding instead.",
                ));
            }
            VirtualPath::CollectionDir { .. } | VirtualPath::CollectionName { .. } => {
                return Err(friendly_anyhow(
                    &from_path,
                    "move_unsupported",
                    "Collection paths do not support move.",
                ));
            }
            _ => {
                return Err(friendly_anyhow(
                    &from_path,
                    "invalid_source",
                    "Move source must be a thing directory path like /collection/{collection_uuid}/things/{thing_uuid}.",
                ));
            }
        };

        let (target_collection_uuid, target_parent_uuid) = match to {
            VirtualPath::CollectionThingsDir { collection_uuid } => (collection_uuid, None),
            VirtualPath::ThingChildrenDir {
                collection_uuid,
                thing_uuid,
            } => (collection_uuid, Some(thing_uuid)),
            _ => {
                return Err(friendly_anyhow(
                    &to_path,
                    "invalid_destination",
                    "Move destination must be a things directory path like /collection/{collection_uuid}/things or /collection/{collection_uuid}/things/{thing_uuid}/things.",
                ));
            }
        };

        let mut doc_set = self.get_or_init_document_set(device_id)?;
        let snapshot = doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
            include_content: false,
        })?;
        let thing = snapshot
            .things
            .iter()
            .find(|item| item.uuid == thing_uuid)
            .cloned()
            .ok_or_else(|| friendly_anyhow(&from_path, "thing_not_found", &format!("Thing '{}' was not found.", thing_uuid)))?;
        let entries = if source_collection_uuid != target_collection_uuid {
            doc_set.get_content_entries(&source_collection_uuid, &thing_uuid).unwrap_or_default()
        } else {
            Vec::new()
        };

        let mut events = Vec::new();
        if source_collection_uuid != target_collection_uuid {
            events.extend(doc_set.delete_thing(&source_collection_uuid, &thing_uuid)?);
        }

        events.extend(doc_set.upsert_thing_meta(
            &target_collection_uuid,
            &thing_uuid,
            Some(thing.datatype.clone()),
            Some(thing.status.clone()),
            Some(thing.title.clone()),
            target_parent_uuid.clone(),
            crate::things_crdt::trigger_update_from_tri_state(thing.trigger_uuid.as_deref()),
        )?);

        for entry in entries {
            events.extend(doc_set.add_content_entry(&target_collection_uuid, &thing_uuid, entry)?);
        }

        doc_set.save_dirty_to_storage_with_compaction(&self.storage)?;
        self.emit_document_events(device_id, events);

        Ok(json!({
            "ok": true,
            "from_path": from_path,
            "to_path": to_path,
            "message": format!(
                "Moved thing '{}' from collection '{}' to collection '{}'{}",
                thing_uuid,
                source_collection_uuid,
                target_collection_uuid,
                target_parent_uuid
                    .as_ref()
                    .map(|value| format!(" under parent '{}'", value))
                    .unwrap_or_default(),
            ),
            "thing_uuid": thing_uuid,
            "target_collection_uuid": target_collection_uuid,
            "target_parent_uuid": target_parent_uuid,
        }))
    }

    pub fn create_virtual_path(
        &self,
        device_id: &str,
        parent_path: &str,
        kind: &str,
        title: Option<&str>,
        content: Option<&str>,
        source_uri: Option<&str>,
        bind_path: Option<&str>,
        uuid: Option<&str>,
    ) -> Result<JsonValue> {
        let parent_path = normalize_path(parent_path)?;
        let parent = parse_virtual_path(&parent_path)?;
        let kind = kind.trim().to_ascii_lowercase();
        let bind_path = bind_path
            .map(normalize_path)
            .transpose()?;

        if bind_path.is_some() {
            return Err(friendly_anyhow(
                bind_path.as_deref().unwrap_or(ROOT_PATH),
                "bind_path_unsupported",
                "bind_path is no longer supported by create_tool. Use create_trigger or create_timer_trigger for trigger creation and binding.",
            ));
        }

        if kind != "image" && source_uri.is_some() {
            return Err(friendly_anyhow(
                source_uri.unwrap_or_default(),
                "source_uri_unsupported",
                "source_uri is only supported when create_tool type is 'image'.",
            ));
        }

        let (created_path, created_uuid, extra) = match (kind.as_str(), parent) {
            ("collection", VirtualPath::Root | VirtualPath::CollectionRoot) => {
                let collection_uuid = uuid
                    .filter(|value| !value.trim().is_empty())
                    .map(ToString::to_string)
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                self.things_upsert_collection(
                    device_id,
                    ThingCollectionUpsert {
                        uuid: collection_uuid.clone(),
                        title: title.unwrap_or("New Collection").to_string(),
                        trigger_uuid: None,
                        created_at: None,
                        updated_at: None,
                    },
                )?;
                (
                    format!("/collection/{collection_uuid}"),
                    collection_uuid,
                    JsonValue::Null,
                )
            }
            (
                "thing",
                VirtualPath::CollectionThingsDir { collection_uuid },
            ) => {
                let thing_uuid = uuid
                    .filter(|value| !value.trim().is_empty())
                    .map(ToString::to_string)
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                self.things_upsert_thing(
                    device_id,
                    ThingUpsert {
                        uuid: thing_uuid.clone(),
                        title: title.unwrap_or("New Thing").to_string(),
                        datatype: ThingDatatype::Markdown,
                        data: Some(json!({ "markdown": content.unwrap_or("") })),
                        collection_uuid: collection_uuid.clone(),
                        trigger_uuid: None,
                        parent_uuid: None,
                        created_at: None,
                        updated_at: None,
                    },
                )?;
                (
                    format!("/collection/{collection_uuid}/things/{thing_uuid}"),
                    thing_uuid,
                    JsonValue::Null,
                )
            }
            (
                "thing",
                VirtualPath::ThingChildrenDir {
                    collection_uuid,
                    thing_uuid: parent_uuid,
                },
            ) => {
                let thing_uuid = uuid
                    .filter(|value| !value.trim().is_empty())
                    .map(ToString::to_string)
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                self.things_upsert_thing(
                    device_id,
                    ThingUpsert {
                        uuid: thing_uuid.clone(),
                        title: title.unwrap_or("New Thing").to_string(),
                        datatype: ThingDatatype::Markdown,
                        data: Some(json!({ "markdown": content.unwrap_or("") })),
                        collection_uuid: collection_uuid.clone(),
                        trigger_uuid: None,
                        parent_uuid: Some(parent_uuid.clone()),
                        created_at: None,
                        updated_at: None,
                    },
                )?;
                (
                    format!("/collection/{collection_uuid}/things/{thing_uuid}"),
                    thing_uuid,
                    JsonValue::Null,
                )
            }
            (
                "image",
                VirtualPath::ThingDir {
                    collection_uuid,
                    thing_uuid,
                },
            ) => {
                let source_uri = source_uri
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        friendly_anyhow(
                            &parent_path,
                            "missing_source_uri",
                            "Creating an image entry requires source_uri to be a remi:// URI from the current chat attachments.",
                        )
                    })?;
                if !source_uri.starts_with("remi://") {
                    return Err(friendly_anyhow(
                        source_uri,
                        "invalid_source_uri",
                        "Image source_uri must be a remi:// URI.",
                    ));
                }

                let before_entries = self.things_get_content_entries(device_id, &thing_uuid)?;
                let entry_id = uuid
                    .filter(|value| !value.trim().is_empty())
                    .map(ToString::to_string)
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                let order = before_entries
                    .iter()
                    .map(|entry| entry.order)
                    .fold(-1.0_f64, f64::max)
                    + 1.0;

                self.things_add_content_entry(
                    device_id,
                    &thing_uuid,
                    ContentEntry {
                        id: entry_id.clone(),
                        title: title.map(ToString::to_string),
                        order,
                        payload: ContentEntryPayload::Image(ImageField::new(source_uri.to_string())),
                    },
                )?;

                let after_entries = self.things_get_content_entries(device_id, &thing_uuid)?;
                let entry_index = after_entries
                    .iter()
                    .position(|entry| entry.id == entry_id)
                    .ok_or_else(|| anyhow!("Created image entry '{}' was not found after insertion", entry_id))?;
                let entry_path = format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{entry_index}");

                (
                    entry_path,
                    entry_id,
                    json!({
                        "source_uri": source_uri,
                        "thing_uuid": thing_uuid,
                        "collection_uuid": collection_uuid,
                    }),
                )
            }
            (
                "json_object",
                VirtualPath::ThingDir {
                    collection_uuid,
                    thing_uuid,
                },
            ) => {
                let initial_data = match content.map(str::trim).filter(|value| !value.is_empty()) {
                    Some(raw) => serde_json::from_str::<JsonValue>(raw).map_err(|error| {
                        friendly_anyhow(
                            &parent_path,
                            "invalid_content",
                            &format!("json_object content must be valid JSON: {error}"),
                        )
                    })?,
                    None => json!({}),
                };

                let entry_id = self.things_add_json_object_content_entry(
                    device_id,
                    &thing_uuid,
                    title,
                    Some(&initial_data),
                    None,
                )?;

                let after_entries = self.things_get_content_entries(device_id, &thing_uuid)?;
                let entry_index = after_entries
                    .iter()
                    .position(|entry| entry.id == entry_id)
                    .ok_or_else(|| anyhow!("Created json_object entry '{}' was not found after insertion", entry_id))?;
                let entry_path = format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{entry_index}");

                tracing::info!(
                    device_id,
                    thing_uuid,
                    collection_uuid,
                    entry_id,
                    entry_index,
                    entry_path,
                    has_initial_content = content.is_some(),
                    title = title.unwrap_or(""),
                    "create_tool created json_object entry"
                );

                (
                    entry_path,
                    entry_id,
                    json!({
                        "thing_uuid": thing_uuid,
                        "collection_uuid": collection_uuid,
                    }),
                )
            }
            ("collection", _) => {
                return Err(friendly_anyhow(
                    &parent_path,
                    "invalid_parent",
                    "Collections can only be created under '/' or '/collection'.",
                ));
            }
            ("thing", _) => {
                return Err(friendly_anyhow(
                    &parent_path,
                    "invalid_parent",
                    "Things can only be created under a things directory such as '/collection/{collection_uuid}/things' or '/collection/{collection_uuid}/things/{thing_uuid}/things'.",
                ));
            }
            ("image", _) => {
                return Err(friendly_anyhow(
                    &parent_path,
                    "invalid_parent",
                    "Images can only be created under a thing directory such as '/collection/{collection_uuid}/things/{thing_uuid}'.",
                ));
            }
            ("json_object", _) => {
                return Err(friendly_anyhow(
                    &parent_path,
                    "invalid_parent",
                    "json_object entries can only be created under a thing directory such as '/collection/{collection_uuid}/things/{thing_uuid}'.",
                ));
            }
            _ => {
                return Err(friendly_anyhow(
                    &parent_path,
                    "invalid_type",
                    "create_tool type must be 'collection', 'thing', 'image', or 'json_object'. Use create_trigger or create_timer_trigger for triggers.",
                ));
            }
        };

        let mut response = json!({
            "ok": true,
            "type": kind,
            "uuid": created_uuid,
            "path": created_path,
        });
        if let JsonValue::Object(object) = &mut response {
            if let JsonValue::Object(extra) = extra {
                object.extend(extra);
            }
        }
        Ok(response)
    }

    fn build_tree_node(&self, device_id: &str, path: &VirtualPath) -> Result<TreeNode> {
        let triggers = self.list_triggers()?;
        let doc_set = self.get_or_init_document_set(device_id)?;
        let tree_data = doc_set.extract_tree_data()?;

        self.build_tree_node_from_tree_data(path, &triggers, &tree_data)
    }

    fn build_tree_node_from_tree_data(
        &self,
        path: &VirtualPath,
        triggers: &[crate::types::TriggerInfo],
        tree_data: &crate::things_crdt::ThingsTreeData,
    ) -> Result<TreeNode> {
        let index = TreeIndex::build(tree_data);

        match path {
            VirtualPath::Root => {
                let trigger_children = render_trigger_listing(triggers, true);
                let collection_children = render_collection_listing(tree_data, &index, None, true);
                Ok(TreeNode::with_children(
                    ROOT_PATH,
                    vec![
                        TreeNode::with_children("trigger/", trigger_children),
                        TreeNode::with_children("collection/", collection_children),
                    ],
                ))
            }
            VirtualPath::TriggerRoot => Ok(TreeNode::with_children(
                "/trigger/",
                render_trigger_listing(triggers, false),
            )),
            VirtualPath::TriggerDir { trigger_uuid } => {
                let trigger = self.fetch_trigger_or_err(trigger_uuid, "/trigger")?;
                Ok(TreeNode::with_children(
                    format!("/trigger/{trigger_uuid}/"),
                    vec![
                        TreeNode::new(format!("name [value=\"{}\"]", trigger.name)),
                        TreeNode::new("rule.json"),
                    ],
                ))
            }
            VirtualPath::CollectionRoot => Ok(TreeNode::with_children(
                "/collection/",
                render_collection_listing(tree_data, &index, None, true),
            )),
            VirtualPath::CollectionDir { collection_uuid } => Ok(TreeNode::with_children(
                format!("/collection/{collection_uuid}/"),
                collection_dir_children(tree_data, &index, collection_uuid)?,
            )),
            VirtualPath::CollectionThingsDir { collection_uuid } => Ok(TreeNode::with_children(
                format!("/collection/{collection_uuid}/things/"),
                render_collection_listing(tree_data, &index, Some(collection_uuid.as_str()), true),
            )),
            VirtualPath::ThingDir {
                collection_uuid,
                thing_uuid,
            } => Ok(TreeNode::with_children(
                format!("/collection/{collection_uuid}/things/{thing_uuid}/"),
                thing_dir_children(tree_data, &index, collection_uuid, thing_uuid)?,
            )),
            VirtualPath::ThingChildrenDir {
                collection_uuid,
                thing_uuid,
            } => {
                let children = index.child_things(collection_uuid, Some(thing_uuid));
                Ok(TreeNode::with_children(
                    format!("/collection/{collection_uuid}/things/{thing_uuid}/things/"),
                    render_thing_nodes(&children, &index),
                ))
            }
            _ => Err(friendly_anyhow(
                &display_path(path),
                "tree_unsupported",
                "tree_tool expects a directory path such as /, /trigger, /collection, /collection/{collection_uuid}, or /collection/{collection_uuid}/things/{thing_uuid}.",
            )),
        }
    }

    fn read_virtual_path_inner(&self, device_id: &str, path: &VirtualPath) -> Result<String> {
        match path {
            VirtualPath::TriggerName { trigger_uuid } => Ok(self.fetch_trigger_or_err(trigger_uuid, "/trigger")?.name),
            VirtualPath::TriggerRule { trigger_uuid } => {
                let trigger = self.fetch_trigger_or_err(trigger_uuid, "/trigger")?;
                let rule = json!({
                    "version": trigger.version,
                    "precondition": parse_rules(&trigger.precondition_json)?,
                    "condition": parse_rules(&trigger.condition_json)?,
                });
                serde_json::to_string_pretty(&rule).context("Failed to serialize trigger rule.json")
            }
            VirtualPath::CollectionName { collection_uuid } => {
                let snapshot = self.things_list_snapshot_lite(device_id)?;
                let collection = snapshot
                    .collections
                    .into_iter()
                    .find(|item| item.uuid == *collection_uuid)
                    .ok_or_else(|| friendly_anyhow(&display_path(path), "collection_not_found", &format!("Collection '{}' was not found.", collection_uuid)))?;
                Ok(collection.title)
            }
            VirtualPath::CollectionTriggerUuid { collection_uuid } => {
                let snapshot = self.things_list_snapshot_lite(device_id)?;
                let collection = snapshot
                    .collections
                    .into_iter()
                    .find(|item| item.uuid == *collection_uuid)
                    .ok_or_else(|| friendly_anyhow(&display_path(path), "collection_not_found", &format!("Collection '{}' was not found.", collection_uuid)))?;
                Ok(collection.trigger_uuid.unwrap_or_default())
            }
            VirtualPath::ThingName { thing_uuid, .. } => {
                let snapshot = self.things_list_snapshot_lite(device_id)?;
                let thing = snapshot
                    .things
                    .into_iter()
                    .find(|item| item.uuid == *thing_uuid)
                    .ok_or_else(|| friendly_anyhow(&display_path(path), "thing_not_found", &format!("Thing '{}' was not found.", thing_uuid)))?;
                Ok(thing.title)
            }
            VirtualPath::ThingTriggerUuid { thing_uuid, .. } => {
                let snapshot = self.things_list_snapshot_lite(device_id)?;
                let thing = snapshot
                    .things
                    .into_iter()
                    .find(|item| item.uuid == *thing_uuid)
                    .ok_or_else(|| friendly_anyhow(&display_path(path), "thing_not_found", &format!("Thing '{}' was not found.", thing_uuid)))?;
                Ok(thing.trigger_uuid.unwrap_or_default())
            }
            VirtualPath::ThingStatus { thing_uuid, .. } => {
                let snapshot = self.things_list_snapshot_lite(device_id)?;
                let thing = snapshot
                    .things
                    .into_iter()
                    .find(|item| item.uuid == *thing_uuid)
                    .ok_or_else(|| friendly_anyhow(&display_path(path), "thing_not_found", &format!("Thing '{}' was not found.", thing_uuid)))?;
                Ok(thing.status)
            }
            VirtualPath::ThingContent {
                collection_uuid,
                thing_uuid,
            } => self.render_thing_content_markdown(device_id, collection_uuid, thing_uuid),
            VirtualPath::ThingEntry {
                thing_uuid,
                index,
                ..
            } => {
                let entry = self.content_entry_by_index(device_id, thing_uuid, *index)?;
                serde_json::to_string_pretty(&entry).context("Failed to serialize content entry")
            }
            VirtualPath::ThingEntryData { thing_uuid, index, .. } => {
                let entry = self.content_entry_by_index(device_id, thing_uuid, *index)?;
                let data = self
                    .things_get_json_object_entry_data(device_id, thing_uuid, &entry.id)?
                    .unwrap_or_else(|| json!({}));
                serde_json::to_string_pretty(&data).context("Failed to serialize json_object data")
            }
            VirtualPath::ThingEntrySchema { thing_uuid, index, .. } => {
                let entry = self.content_entry_by_index(device_id, thing_uuid, *index)?;
                let schema = self
                    .things_get_json_object_entry_schema(device_id, thing_uuid, &entry.id)?
                    .unwrap_or(JsonValue::Null);
                serde_json::to_string_pretty(&schema).context("Failed to serialize json_object schema")
            }
            _ => Err(friendly_anyhow(
                &display_path(path),
                "read_unsupported",
                "cat_tool only supports file nodes such as name, trigger, status, content.md, entries.{idx}, entries.{idx}.data.json, entries.{idx}.schema.json, and rule.json.",
            )),
        }
    }

    fn edit_thing_content_path(
        &self,
        device_id: &str,
        path: &str,
        thing_uuid: &str,
        operation: &str,
        value: Option<&JsonValue>,
        old_str: Option<&str>,
        new_str: Option<&str>,
        line_number: Option<usize>,
    ) -> Result<JsonValue> {
        let operation = match operation {
            "overwrite" | "append" | "str_replace" | "insert_at_line" => operation,
            other => {
                return Err(friendly_anyhow(
                    path,
                    "invalid_operation",
                    &format!(
                        "Unsupported content.md edit operation '{}'. Valid operations are overwrite, append, str_replace, and insert_at_line.",
                        other
                    ),
                ));
            }
        };

        let result = self.things_edit_content(
            device_id,
            thing_uuid,
            operation,
            None,
            value.and_then(JsonValue::as_str),
            old_str,
            new_str,
            line_number,
            value.and_then(JsonValue::as_str),
            value.and_then(JsonValue::as_str),
        )?;

        serde_json::from_str(&result).context("Failed to decode content.md edit result")
    }

    fn edit_thing_entry_path(
        &self,
        device_id: &str,
        path: &str,
        thing_uuid: &str,
        index: usize,
        entry_value: &JsonValue,
    ) -> Result<JsonValue> {
        let current = self.content_entry_by_index(device_id, thing_uuid, index)?;
        let object = entry_value.as_object().ok_or_else(|| {
            friendly_anyhow(
                path,
                "invalid_value",
                "entries.{idx} overwrite requires an object value.",
            )
        })?;

        let title = if object.contains_key("title") {
            Some(object.get("title").and_then(JsonValue::as_str).map(|value| value.to_string()))
        } else {
            None
        };
        let order = object.get("order").and_then(JsonValue::as_f64);
        let payload = if object.contains_key("payload") {
            let registry = crate::things_crdt::ContentTypeRegistry::new();
            Some(
                registry
                    .parse_content_entry_payload(object.get("payload").expect("payload exists"))
                    .with_context(|| format!("Invalid payload for '{}'", path))?,
            )
        } else {
            None
        };

        self.things_update_content_entry(
            device_id,
            thing_uuid,
            ContentEntryUpdate {
                id: current.id.clone(),
                title,
                order,
                payload,
            },
        )?;

        let updated = self.content_entry_by_index(device_id, thing_uuid, index)?;
        Ok(json!({
            "ok": true,
            "path": path,
            "message": format!("Updated content entry {} for thing '{}'", index, thing_uuid),
            "entry": updated,
        }))
    }

    fn edit_thing_entry_data_path(
        &self,
        device_id: &str,
        path: &str,
        thing_uuid: &str,
        index: usize,
        operation: &str,
        value: Option<&JsonValue>,
    ) -> Result<JsonValue> {
        if operation != "overwrite" {
            return Err(friendly_anyhow(
                path,
                "invalid_operation",
                "entries.{idx}.data.json only supports overwrite.",
            ));
        }

        let entry = self.content_entry_by_index(device_id, thing_uuid, index)?;
        let data = value.ok_or_else(|| {
            friendly_anyhow(
                path,
                "invalid_value",
                "entries.{idx}.data.json overwrite requires a JSON value.",
            )
        })?;
        self.things_set_json_object_entry_data(device_id, thing_uuid, &entry.id, data)?;
        let updated = self
            .things_get_json_object_entry_data(device_id, thing_uuid, &entry.id)?
            .unwrap_or_else(|| json!({}));
        Ok(json!({
            "ok": true,
            "path": path,
            "message": format!("Updated json_object data for entry {} on thing '{}'", index, thing_uuid),
            "value": updated,
        }))
    }

    fn edit_thing_entry_schema_path(
        &self,
        device_id: &str,
        path: &str,
        thing_uuid: &str,
        index: usize,
        operation: &str,
        value: Option<&JsonValue>,
    ) -> Result<JsonValue> {
        if operation != "overwrite" {
            return Err(friendly_anyhow(
                path,
                "invalid_operation",
                "entries.{idx}.schema.json only supports overwrite.",
            ));
        }

        let entry = self.content_entry_by_index(device_id, thing_uuid, index)?;
        self.things_set_json_object_entry_schema(device_id, thing_uuid, &entry.id, value)?;
        let updated = self
            .things_get_json_object_entry_schema(device_id, thing_uuid, &entry.id)?
            .unwrap_or(JsonValue::Null);
        Ok(json!({
            "ok": true,
            "path": path,
            "message": format!("Updated json_object schema for entry {} on thing '{}'", index, thing_uuid),
            "value": updated,
        }))
    }

    fn render_thing_content_markdown(
        &self,
        device_id: &str,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Result<String> {
        let doc_set = self.get_or_init_document_set(device_id)?;
        self.render_thing_content_from_doc_set(&doc_set, collection_uuid, thing_uuid)
    }

    fn render_thing_content_from_doc_set(
        &self,
        doc_set: &crate::things_crdt::ThingsDocumentSet,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Result<String> {
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

        let markdown = doc_set.get_thing_markdown_text(thing_uuid)?.unwrap_or_default();
        Ok(rewrite_embedded_entry_references(
            &markdown,
            collection_uuid,
            thing_uuid,
            &thing.built_in.content_entries,
        ))
    }

    fn rename_collection(&self, device_id: &str, collection_uuid: &str, title: &str) -> Result<()> {
        let snapshot = self.things_list_snapshot_lite(device_id)?;
        let collection = snapshot
            .collections
            .iter()
            .find(|item| item.uuid == collection_uuid)
            .ok_or_else(|| friendly_anyhow(
                &format!("/collection/{collection_uuid}/name"),
                "collection_not_found",
                &format!("Collection '{}' was not found.", collection_uuid),
            ))?;

        self.things_upsert_collection(
            device_id,
            crate::things_crdt::ThingCollectionUpsert {
                uuid: collection_uuid.to_string(),
                title: title.to_string(),
                trigger_uuid: collection.trigger_uuid.clone(),
                created_at: None,
                updated_at: None,
            },
        )?;
        Ok(())
    }

    fn rename_thing(&self, device_id: &str, thing_uuid: &str, title: &str) -> Result<()> {
        let snapshot = self.things_list_snapshot_lite(device_id)?;
        let thing = snapshot
            .things
            .iter()
            .find(|item| item.uuid == thing_uuid)
            .cloned()
            .ok_or_else(|| friendly_anyhow(
                &format!("/thing/{thing_uuid}/name"),
                "thing_not_found",
                &format!("Thing '{}' was not found.", thing_uuid),
            ))?;

        self.things_upsert_thing(
            device_id,
            ThingUpsert {
                uuid: thing.uuid,
                title: title.to_string(),
                datatype: thing.datatype,
                data: None,
                collection_uuid: thing.collection_uuid,
                trigger_uuid: thing.trigger_uuid,
                parent_uuid: thing.parent_uuid,
                created_at: None,
                updated_at: None,
            },
        )?;
        Ok(())
    }

    fn content_entry_by_index(
        &self,
        device_id: &str,
        thing_uuid: &str,
        index: usize,
    ) -> Result<crate::things_crdt::ContentEntry> {
        let entries = self.things_get_content_entries(device_id, thing_uuid)?;
        entries.into_iter().nth(index).ok_or_else(|| {
            friendly_anyhow(
                &format!("entries.{index}"),
                "entry_index_out_of_range",
                &format!("entries.{index} is out of range for thing '{}'.", thing_uuid),
            )
        })
    }

    fn fetch_trigger_or_err(&self, trigger_uuid: &str, path: &str) -> Result<crate::types::StoredTrigger> {
        self.storage
            .fetch_trigger(trigger_uuid)?
            .ok_or_else(|| {
                friendly_anyhow(
                    path,
                    "trigger_not_found",
                    &format!("Trigger '{}' was not found.", trigger_uuid),
                )
            })
    }

    fn update_trigger_name(&self, trigger_uuid: &str, name: &str) -> Result<()> {
        let existing = self.fetch_trigger_or_err(trigger_uuid, &format!("/trigger/{trigger_uuid}/name"))?;
        let registration = TriggerRegistration {
            trigger_uuid: existing.trigger_uuid.clone(),
            name: name.to_string(),
            version: existing.version.clone(),
            precondition: parse_rules(&existing.precondition_json)?,
            condition: parse_rules(&existing.condition_json)?,
        };
        self.register_trigger(registration)?;
        Ok(())
    }

    fn update_trigger_rule(&self, trigger_uuid: &str, rule: &JsonValue) -> Result<()> {
        let existing = self.fetch_trigger_or_err(trigger_uuid, &format!("/trigger/{trigger_uuid}/rule.json"))?;
        let registration = TriggerRegistration {
            trigger_uuid: existing.trigger_uuid.clone(),
            name: existing.name,
            version: rule
                .get("version")
                .and_then(JsonValue::as_str)
                .unwrap_or(existing.version.as_str())
                .to_string(),
            precondition: parse_rules_value(rule.get("precondition"))?,
            condition: parse_rules_value(rule.get("condition"))?,
        };
        self.register_trigger(registration)?;
        Ok(())
    }
}

fn push_profile_step(
    steps: &mut Vec<VirtualFsProfileStep>,
    name: &str,
    elapsed: std::time::Duration,
) {
    steps.push(VirtualFsProfileStep {
        name: name.to_string(),
        elapsed_ms: elapsed.as_millis() as u64,
    });
}

fn normalize_path(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(friendly_anyhow(ROOT_PATH, "invalid_path", "Path must not be empty."));
    }
    if !trimmed.starts_with('/') {
        return Err(friendly_anyhow(trimmed, "invalid_path", "Path must start with '/'."));
    }

    let normalized_segments = trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    if normalized_segments.is_empty() {
        return Ok(ROOT_PATH.to_string());
    }

    Ok(format!("/{}", normalized_segments.join("/")))
}

fn parse_virtual_path(path: &str) -> Result<VirtualPath> {
    if path == ROOT_PATH {
        return Ok(VirtualPath::Root);
    }

    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    match segments.as_slice() {
        ["trigger"] => Ok(VirtualPath::TriggerRoot),
        ["trigger", trigger_uuid] => Ok(VirtualPath::TriggerDir {
            trigger_uuid: (*trigger_uuid).to_string(),
        }),
        ["trigger", trigger_uuid, "name"] => Ok(VirtualPath::TriggerName {
            trigger_uuid: (*trigger_uuid).to_string(),
        }),
        ["trigger", trigger_uuid, "rule.json"] => Ok(VirtualPath::TriggerRule {
            trigger_uuid: (*trigger_uuid).to_string(),
        }),
        ["collection"] => Ok(VirtualPath::CollectionRoot),
        ["collection", collection_uuid] => Ok(VirtualPath::CollectionDir {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "name"] => Ok(VirtualPath::CollectionName {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "trigger"]
        | ["collection", collection_uuid, "trigger_uuid"] => Ok(VirtualPath::CollectionTriggerUuid {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things"] => Ok(VirtualPath::CollectionThingsDir {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things", thing_uuid] => Ok(VirtualPath::ThingDir {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things", thing_uuid, "name"] => Ok(VirtualPath::ThingName {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things", thing_uuid, "trigger"]
        | ["collection", collection_uuid, "things", thing_uuid, "trigger_uuid"] => Ok(VirtualPath::ThingTriggerUuid {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things", thing_uuid, "status"] => Ok(VirtualPath::ThingStatus {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things", thing_uuid, "content.md"] => Ok(VirtualPath::ThingContent {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things", thing_uuid, entry_segment]
            if entry_segment.starts_with("entries.") && entry_segment.ends_with(".data.json") =>
        {
            let index = entry_segment[8..entry_segment.len() - ".data.json".len()]
                .parse::<usize>()
                .map_err(|_| {
                    friendly_anyhow(path, "invalid_entry_index", "entries.{idx}.data.json must use a non-negative integer index.")
                })?;
            Ok(VirtualPath::ThingEntryData {
                collection_uuid: (*collection_uuid).to_string(),
                thing_uuid: (*thing_uuid).to_string(),
                index,
            })
        }
        ["collection", collection_uuid, "things", thing_uuid, entry_segment]
            if entry_segment.starts_with("entries.") && entry_segment.ends_with(".schema.json") =>
        {
            let index = entry_segment[8..entry_segment.len() - ".schema.json".len()]
                .parse::<usize>()
                .map_err(|_| {
                    friendly_anyhow(path, "invalid_entry_index", "entries.{idx}.schema.json must use a non-negative integer index.")
                })?;
            Ok(VirtualPath::ThingEntrySchema {
                collection_uuid: (*collection_uuid).to_string(),
                thing_uuid: (*thing_uuid).to_string(),
                index,
            })
        }
        ["collection", collection_uuid, "things", thing_uuid, entry_segment] if entry_segment.starts_with("entries.") => {
            let index = entry_segment[8..].parse::<usize>().map_err(|_| {
                friendly_anyhow(path, "invalid_entry_index", "entries.{idx} must end with a non-negative integer index.")
            })?;
            Ok(VirtualPath::ThingEntry {
                collection_uuid: (*collection_uuid).to_string(),
                thing_uuid: (*thing_uuid).to_string(),
                index,
            })
        }
        ["collection", collection_uuid, "things", thing_uuid, "things"] => Ok(VirtualPath::ThingChildrenDir {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        _ => Err(friendly_anyhow(
            path,
            "invalid_path",
            "Unsupported path. Expected /trigger/... or /collection/... according to the virtual filesystem contract.",
        )),
    }
}

fn render_trigger_listing(triggers: &[crate::types::TriggerInfo], limit_preview: bool) -> Vec<TreeNode> {
    let mut nodes = triggers
        .iter()
        .take(if limit_preview { TRIGGER_PREVIEW_LIMIT } else { triggers.len() })
        .map(|trigger| TreeNode::new(format!("{}/", trigger.trigger_id)))
        .collect::<Vec<_>>();

    if limit_preview && triggers.len() > TRIGGER_PREVIEW_LIMIT {
        nodes.push(TreeNode::new(format!("Has {} More", triggers.len() - TRIGGER_PREVIEW_LIMIT)));
    }

    if !limit_preview {
        for (node, trigger) in nodes.iter_mut().zip(triggers.iter()) {
            node.children = vec![
                TreeNode::new(format!("name [value=\"{}\"]", trigger.name)),
                TreeNode::new("rule.json"),
            ];
        }
    }

    nodes
}

fn render_collection_listing(
    tree_data: &crate::things_crdt::ThingsTreeData,
    index: &TreeIndex<'_>,
    collection_filter: Option<&str>,
    root_only: bool,
) -> Vec<TreeNode> {
    tree_data
        .collections
        .iter()
        .filter(|collection| collection_filter.is_none_or(|value| value == collection.uuid))
        .map(|collection| {
            let mut children = vec![
                TreeNode::new(format!("name [value=\"{}\"]", collection.title)),
                TreeNode::new(format!("trigger [value=\"{}\"]", collection.trigger_uuid.clone().unwrap_or_default())),
            ];
            let child_things = index.child_things(&collection.uuid, None);
            let thing_nodes = render_thing_nodes(&child_things, index);
            if root_only || !thing_nodes.is_empty() {
                children.push(TreeNode::with_children("things/", thing_nodes));
            }
            TreeNode::with_children(
                format!("{}/ [name=\"{}\"]", collection.uuid, collection.title),
                children,
            )
        })
        .collect()
}

fn collection_dir_children(
    tree_data: &crate::things_crdt::ThingsTreeData,
    index: &TreeIndex<'_>,
    collection_uuid: &str,
) -> Result<Vec<TreeNode>> {
    let collection = index
        .collection(collection_uuid)
        .ok_or_else(|| friendly_anyhow(
            &format!("/collection/{collection_uuid}"),
            "collection_not_found",
            &format!("Collection '{}' was not found.", collection_uuid),
        ))?;

    let root_things = index.child_things(collection_uuid, None);

    Ok(vec![
        TreeNode::new(format!("name [value=\"{}\"]", collection.title)),
        TreeNode::new(format!("trigger [value=\"{}\"]", collection.trigger_uuid.clone().unwrap_or_default())),
        TreeNode::with_children(
            "things/",
            render_thing_nodes(&root_things, index)
                .into_iter()
                .map(|mut node| {
                    if node.children.is_empty() {
                        if let Some(thing_uuid) = extract_uuid_from_dir_label(&node.label) {
                            if let Ok(children) = thing_dir_children(tree_data, index, collection_uuid, &thing_uuid) {
                                node.children = children;
                            }
                        }
                    }
                    node
                })
                .collect(),
        ),
    ])
}

fn thing_dir_children(
    _tree_data: &crate::things_crdt::ThingsTreeData,
    index: &TreeIndex<'_>,
    collection_uuid: &str,
    thing_uuid: &str,
) -> Result<Vec<TreeNode>> {
    let thing = index
        .thing(collection_uuid, thing_uuid)
        .ok_or_else(|| friendly_anyhow(
            &format!("/collection/{collection_uuid}/things/{thing_uuid}"),
            "thing_not_found",
            &format!("Thing '{}' was not found in collection '{}'.", thing_uuid, collection_uuid),
        ))?;

    let mut children = vec![
        TreeNode::new(format!("name [value=\"{}\"]", thing.title)),
        TreeNode::new(format!("trigger [value=\"{}\"]", thing.trigger_uuid.clone().unwrap_or_default())),
        thing_status_node(&thing.status),
        TreeNode::new("content.md"),
    ];

    children.extend(
        thing
            .entries
            .iter()
            .enumerate()
            .flat_map(|(index, entry)| {
                let mut nodes = vec![TreeNode::new(format!("entries.{}", index))];
                if matches!(entry.payload, ContentEntryPayload::JsonObject(_)) {
                    nodes.push(TreeNode::new(format!("entries.{}.data.json", index)));
                    nodes.push(TreeNode::new(format!("entries.{}.schema.json", index)));
                }
                nodes
            }),
    );

    let thing_children = index.child_things(collection_uuid, Some(thing_uuid));
    if !thing_children.is_empty() {
        children.push(TreeNode::with_children(
            "things/",
            render_thing_nodes(&thing_children, index),
        ));
    }

    Ok(children)
}

fn render_thing_nodes(
    things: &[&crate::things_crdt::TreeThingData],
    index: &TreeIndex<'_>,
) -> Vec<TreeNode> {
    things
        .iter()
        .map(|thing| {
            let mut children = vec![
                TreeNode::new(format!(
                    "trigger [value=\"{}\"]",
                    thing.trigger_uuid.clone().unwrap_or_default()
                )),
                thing_status_node(&thing.status),
            ];
            let has_children = index.has_children(&thing.collection_uuid, &thing.uuid);
            if has_children {
                children.push(TreeNode::new("things/"));
            }
            TreeNode::with_children(
                format!("{}/ [name=\"{}\", status=\"{}\"]", thing.uuid, thing.title, thing.status),
                children,
            )
        })
        .collect()
}

fn thing_status_node(status: &str) -> TreeNode {
    TreeNode::new(format!("status [value=\"{}\"]", status))
}

fn render_tree(root: &TreeNode) -> String {
    let mut lines = vec![root.label.clone()];
    for (index, child) in root.children.iter().enumerate() {
        let is_last = index + 1 == root.children.len();
        render_tree_child(child, "", is_last, &mut lines);
    }
    lines.join("\n")
}

fn render_tree_child(node: &TreeNode, prefix: &str, is_last: bool, lines: &mut Vec<String>) {
    let branch = if is_last { "`-- " } else { "|-- " };
    lines.push(format!("{}{}{}", prefix, branch, node.label));
    let child_prefix = if is_last {
        format!("{}    ", prefix)
    } else {
        format!("{}|   ", prefix)
    };

    for (index, child) in node.children.iter().enumerate() {
        render_tree_child(child, &child_prefix, index + 1 == node.children.len(), lines);
    }
}

fn parse_rule_json(path: &str, value: Option<&JsonValue>) -> Result<JsonValue> {
    let Some(value) = value else {
        return Err(friendly_anyhow(
            path,
            "invalid_value",
            "Editing rule.json requires an object value or a JSON string.",
        ));
    };

    match value {
        JsonValue::Object(_) => Ok(value.clone()),
        JsonValue::String(text) => serde_json::from_str::<JsonValue>(text).map_err(|error| {
            friendly_anyhow(
                path,
                "invalid_rule_json",
                &format!("rule.json must be valid JSON: {error}"),
            )
        }),
        _ => Err(friendly_anyhow(
            path,
            "invalid_value",
            "rule.json requires a JSON object or string.",
        )),
    }
}

fn parse_rules(raw: &str) -> Result<Vec<TriggerRule>> {
    serde_json::from_str(raw).context("Failed to decode stored trigger rules")
}

fn parse_rules_value(value: Option<&JsonValue>) -> Result<Vec<TriggerRule>> {
    let value = value.ok_or_else(|| anyhow!("Missing trigger rule section"))?;
    serde_json::from_value(value.clone()).context("Failed to decode trigger rules")
}

fn normalize_operation(operation: &str) -> &str {
    let trimmed = operation.trim();
    if trimmed.is_empty() {
        "overwrite"
    } else {
        trimmed
    }
}

fn display_path(path: &VirtualPath) -> String {
    match path {
        VirtualPath::Root => ROOT_PATH.to_string(),
        VirtualPath::TriggerRoot => "/trigger".to_string(),
        VirtualPath::TriggerDir { trigger_uuid } => format!("/trigger/{trigger_uuid}"),
        VirtualPath::TriggerName { trigger_uuid } => format!("/trigger/{trigger_uuid}/name"),
        VirtualPath::TriggerRule { trigger_uuid } => format!("/trigger/{trigger_uuid}/rule.json"),
        VirtualPath::CollectionRoot => "/collection".to_string(),
        VirtualPath::CollectionDir { collection_uuid } => format!("/collection/{collection_uuid}"),
        VirtualPath::CollectionName { collection_uuid } => format!("/collection/{collection_uuid}/name"),
        VirtualPath::CollectionTriggerUuid { collection_uuid } => format!("/collection/{collection_uuid}/trigger"),
        VirtualPath::CollectionThingsDir { collection_uuid } => format!("/collection/{collection_uuid}/things"),
        VirtualPath::ThingDir { collection_uuid, thing_uuid } => format!("/collection/{collection_uuid}/things/{thing_uuid}"),
        VirtualPath::ThingName { collection_uuid, thing_uuid } => format!("/collection/{collection_uuid}/things/{thing_uuid}/name"),
        VirtualPath::ThingTriggerUuid { collection_uuid, thing_uuid } => format!("/collection/{collection_uuid}/things/{thing_uuid}/trigger"),
        VirtualPath::ThingStatus { collection_uuid, thing_uuid } => format!("/collection/{collection_uuid}/things/{thing_uuid}/status"),
        VirtualPath::ThingContent { collection_uuid, thing_uuid } => format!("/collection/{collection_uuid}/things/{thing_uuid}/content.md"),
        VirtualPath::ThingEntry { collection_uuid, thing_uuid, index } => format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{index}"),
        VirtualPath::ThingEntryData { collection_uuid, thing_uuid, index } => format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{index}.data.json"),
        VirtualPath::ThingEntrySchema { collection_uuid, thing_uuid, index } => format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{index}.schema.json"),
        VirtualPath::ThingChildrenDir { collection_uuid, thing_uuid } => format!("/collection/{collection_uuid}/things/{thing_uuid}/things"),
    }
}

fn extract_uuid_from_dir_label(label: &str) -> Option<String> {
    label.split('/').next().map(|value| value.trim().to_string())
}

fn rewrite_embedded_entry_references(
    markdown: &str,
    collection_uuid: &str,
    thing_uuid: &str,
    entries: &[crate::things_crdt::ContentEntry],
) -> String {
    let mut rendered = markdown.to_string();
    let id_to_index = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.id.as_str(), index))
        .collect::<std::collections::HashMap<_, _>>();

    for (entry_id, index) in id_to_index {
        let full_path = format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{index}");
        let label = match entries[index].payload {
            ContentEntryPayload::Image(_) => "IMG",
            _ => "内容",
        };
        let target = format!("{ENTRY_REFERENCE_SCHEME}{entry_id}");

        rendered = replace_entry_reference_target(&rendered, &target, &format!("[{label}]({full_path})"));
    }

    rendered
}

fn replace_entry_reference_target(markdown: &str, target: &str, replacement: &str) -> String {
    let mut next = markdown.replace(&format!("![]({target})"), replacement);
    next = next.replace(&format!("[remi-entry]({target})"), replacement);
    next = next.replace(&format!("<{}>", target), replacement);
    next
}

fn friendly_anyhow(path: &str, code: &str, message: &str) -> anyhow::Error {
    anyhow!(json!({
        "error": code,
        "path": path,
        "message": message,
    })
    .to_string())
}
