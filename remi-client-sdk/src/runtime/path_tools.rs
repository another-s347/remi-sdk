use super::RemiSdk;
use crate::things_crdt::{
    ContentEntry, ContentEntryPayload, ContentEntryUpdate, ImageField, ThingCollectionUpsert,
    ThingDatatype, ThingUpsert,
};
use crate::types::{
    EntityActionBinding, VirtualFsNodeKind, VirtualFsProfileResult, VirtualFsProfileStep,
    VirtualFsReadResult,
};
use anyhow::{Context, Result, anyhow};
use serde_json::{Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

mod helpers;
use helpers::{
    collection_dir_children, display_path, friendly_anyhow, normalize_operation, normalize_path,
    parse_action_args_json, parse_entity_action_bindings_value, parse_virtual_path,
    push_profile_step, render_action_listing, render_collection_listing, render_thing_nodes,
    render_tree, thing_dir_children,
};

const ROOT_PATH: &str = "/";
const ACTION_PREVIEW_LIMIT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
enum VirtualPath {
    Root,
    ActionRoot,
    ActionDir {
        action_uuid: String,
    },
    ActionName {
        action_uuid: String,
    },
    ActionMetadata {
        action_uuid: String,
    },
    ActionInputSchema {
        action_uuid: String,
    },
    ActionOutputSchema {
        action_uuid: String,
    },
    ActionScript {
        action_uuid: String,
    },
    ActionLatestInvocation {
        action_uuid: String,
    },
    CollectionRoot,
    CollectionDir {
        collection_uuid: String,
    },
    CollectionName {
        collection_uuid: String,
    },
    CollectionCardJsx {
        collection_uuid: String,
    },
    CollectionActions {
        collection_uuid: String,
    },
    CollectionThingsDir {
        collection_uuid: String,
    },
    ThingDir {
        collection_uuid: String,
        thing_uuid: String,
    },
    ThingName {
        collection_uuid: String,
        thing_uuid: String,
    },
    ThingActions {
        collection_uuid: String,
        thing_uuid: String,
    },
    ThingStatus {
        collection_uuid: String,
        thing_uuid: String,
    },
    ThingContent {
        collection_uuid: String,
        thing_uuid: String,
    },
    ThingEntry {
        collection_uuid: String,
        thing_uuid: String,
        index: usize,
    },
    ThingEntryData {
        collection_uuid: String,
        thing_uuid: String,
        index: usize,
    },
    ThingEntrySchema {
        collection_uuid: String,
        thing_uuid: String,
        index: usize,
    },
    ThingChildrenDir {
        collection_uuid: String,
        thing_uuid: String,
    },
}

pub enum VirtualFsCatResult {
    Text(VirtualFsReadResult),
    Image { uri: String },
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

    fn collection(
        &self,
        collection_uuid: &str,
    ) -> Option<&'a crate::things_crdt::TreeCollectionData> {
        self.collections_by_uuid.get(collection_uuid).copied()
    }

    fn thing(
        &self,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Option<&'a crate::things_crdt::TreeThingData> {
        self.things_by_key
            .get(&(collection_uuid, thing_uuid))
            .copied()
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
        self.parents_with_children
            .contains(&(collection_uuid, thing_uuid))
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

impl RemiSdk {
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

        let actions_started = Instant::now();
        let actions = self.list_actions()?;
        push_profile_step(&mut steps, "list_actions", actions_started.elapsed());

        let tree_data_started = Instant::now();
        let tree_data = self.things_local_service().tree_data(device_id)?;
        push_profile_step(&mut steps, "extract_tree_data", tree_data_started.elapsed());

        let render_started = Instant::now();
        let node = self.build_tree_node_from_tree_data(&parsed, &actions, &tree_data)?;
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

    pub fn cat_virtual_path(&self, device_id: &str, path: &str) -> Result<VirtualFsCatResult> {
        let path = normalize_path(path)?;
        let parsed = parse_virtual_path(&path)?;

        if let VirtualPath::ThingEntry {
            thing_uuid, index, ..
        } = &parsed
        {
            let entry = self.content_entry_by_index(device_id, thing_uuid, *index)?;
            if let ContentEntryPayload::Image(image) = entry.payload {
                return Ok(VirtualFsCatResult::Image { uri: image.uri });
            }
        }

        Ok(VirtualFsCatResult::Text(
            self.read_virtual_path(device_id, &path)?,
        ))
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
                let markdown_started = Instant::now();
                let rendered = self.things_local_service().render_thing_content_markdown(
                    device_id,
                    collection_uuid,
                    thing_uuid,
                )?;
                push_profile_step(
                    &mut steps,
                    "render_content_markdown",
                    markdown_started.elapsed(),
                );
                rendered
            }
            _ => {
                let read_started = Instant::now();
                let result = self.read_virtual_path(device_id, &path)?;
                push_profile_step(
                    &mut steps,
                    "read_virtual_path_total",
                    read_started.elapsed(),
                );
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
            VirtualPath::ActionRoot
            | VirtualPath::ActionDir { .. }
            | VirtualPath::ActionName { .. }
            | VirtualPath::ActionMetadata { .. }
            | VirtualPath::ActionInputSchema { .. }
            | VirtualPath::ActionOutputSchema { .. }
            | VirtualPath::ActionScript { .. }
            | VirtualPath::ActionLatestInvocation { .. } => {
                return Err(friendly_anyhow(
                    &path,
                    "read_only_action_path",
                    "The /action subtree is read-only in v1.",
                ));
            }
            VirtualPath::CollectionActions {
                ref collection_uuid,
            } => {
                let bindings = parse_entity_action_bindings_value(&path, value)?;
                self.things_set_collection_action_bindings(device_id, collection_uuid, &bindings)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated collection action bindings for '{}'", collection_uuid),
                    "value": bindings,
                })
            }
            VirtualPath::CollectionCardJsx {
                ref collection_uuid,
            } => {
                let card_jsx = value
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| friendly_anyhow(&path, "invalid_value", "Editing collection card.jsx requires a string value. Use an empty string to clear the template."))?;
                self.things_set_collection_card_jsx(device_id, collection_uuid, Some(card_jsx))?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated collection card JSX for '{}'", collection_uuid),
                    "value": card_jsx,
                })
            }
            VirtualPath::CollectionName {
                ref collection_uuid,
            } => {
                let title = value.and_then(JsonValue::as_str).ok_or_else(|| {
                    friendly_anyhow(
                        &path,
                        "invalid_value",
                        "Editing collection name requires a string value.",
                    )
                })?;
                self.rename_collection(device_id, collection_uuid, title)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated collection name for '{}'", collection_uuid),
                    "value": title,
                })
            }
            VirtualPath::ThingName { ref thing_uuid, .. } => {
                let title = value.and_then(JsonValue::as_str).ok_or_else(|| {
                    friendly_anyhow(
                        &path,
                        "invalid_value",
                        "Editing thing name requires a string value.",
                    )
                })?;
                self.rename_thing(device_id, thing_uuid, title)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated thing name for '{}'", thing_uuid),
                    "value": title,
                })
            }
            VirtualPath::ThingActions { ref thing_uuid, .. } => {
                let bindings = parse_entity_action_bindings_value(&path, value)?;
                self.things_set_thing_action_bindings(device_id, thing_uuid, &bindings)?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated thing action bindings for '{}'", thing_uuid),
                    "value": bindings,
                })
            }
            VirtualPath::ThingStatus { ref thing_uuid, .. } => {
                let status = value.and_then(JsonValue::as_str).ok_or_else(|| {
                    friendly_anyhow(
                        &path,
                        "invalid_value",
                        "Editing thing status requires a string value.",
                    )
                })?;
                self.set_thing_status(device_id, thing_uuid, status)
                    .with_context(|| format!("Failed to update thing status for {thing_uuid}"))?;
                json!({
                    "ok": true,
                    "path": path,
                    "message": format!("Updated thing status for '{}'", thing_uuid),
                    "value": status,
                })
            }
            VirtualPath::ThingContent { ref thing_uuid, .. } => self.edit_thing_content_path(
                device_id,
                &path,
                thing_uuid,
                operation,
                value,
                old_str,
                new_str,
                line_number,
            )?,
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
            } => self.edit_thing_entry_data_path(
                device_id, &path, thing_uuid, index, operation, value,
            )?,
            VirtualPath::ThingEntrySchema {
                ref thing_uuid,
                index,
                ..
            } => self.edit_thing_entry_schema_path(
                device_id, &path, thing_uuid, index, operation, value,
            )?,
            VirtualPath::Root
            | VirtualPath::CollectionRoot
            | VirtualPath::CollectionDir { .. }
            | VirtualPath::CollectionThingsDir { .. }
            | VirtualPath::ThingDir { .. }
            | VirtualPath::ThingChildrenDir { .. } => {
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
            VirtualPath::ActionRoot
            | VirtualPath::ActionDir { .. }
            | VirtualPath::ActionName { .. }
            | VirtualPath::ActionMetadata { .. }
            | VirtualPath::ActionInputSchema { .. }
            | VirtualPath::ActionOutputSchema { .. }
            | VirtualPath::ActionScript { .. }
            | VirtualPath::ActionLatestInvocation { .. }
            | VirtualPath::CollectionActions { .. }
            | VirtualPath::ThingActions { .. } => {
                return Err(friendly_anyhow(
                    &path,
                    "read_only_action_path",
                    "The /action subtree is read-only in v1.",
                ));
            }
            VirtualPath::CollectionDir {
                ref collection_uuid,
            } => {
                let before = self.things_get_collection(device_id, collection_uuid)?;
                let deleted = self.things_delete_collection(device_id, collection_uuid)?;
                let message = if deleted {
                    format!("Deleted collection '{}'", collection_uuid)
                } else if before
                    .as_ref()
                    .is_some_and(|collection| collection.archived_at.is_some())
                {
                    format!("Collection '{}' was already archived", collection_uuid)
                } else if before
                    .as_ref()
                    .is_some_and(|collection| collection.is_system_collection())
                {
                    format!(
                        "Collection '{}' is a protected system collection",
                        collection_uuid
                    )
                } else {
                    format!("Collection '{}' was already absent", collection_uuid)
                };
                json!({
                    "ok": deleted,
                    "path": path,
                    "message": message,
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
                let entry = self
                    .content_entry_by_index(device_id, thing_uuid, index)
                    .with_context(|| format!("Failed to resolve content entry at '{}'", path))?;
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
            | VirtualPath::CollectionRoot
            | VirtualPath::CollectionName { .. }
            | VirtualPath::CollectionCardJsx { .. }
            | VirtualPath::CollectionThingsDir { .. }
            | VirtualPath::ThingName { .. }
            | VirtualPath::ThingStatus { .. }
            | VirtualPath::ThingContent { .. }
            | VirtualPath::ThingChildrenDir { .. } => {
                return Err(friendly_anyhow(
                    &path,
                    "delete_unsupported",
                    "Delete only supports entity directories (/collection/{uuid}, /collection/{collection_uuid}/things/{thing_uuid}) or entry files (/entries.{idx}).",
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

        let snapshot = self.things_list_snapshot_lite(device_id)?;
        snapshot
            .things
            .iter()
            .any(|item| item.uuid == thing_uuid)
            .then_some(())
            .ok_or_else(|| {
                friendly_anyhow(
                    &from_path,
                    "thing_not_found",
                    &format!("Thing '{}' was not found.", thing_uuid),
                )
            })?;
        self.things_local_service().move_thing(
            device_id,
            &thing_uuid,
            &target_collection_uuid,
            target_parent_uuid.clone(),
        )?;

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
        action_uuid: Option<&str>,
        title: Option<&str>,
        content: Option<&str>,
        source_uri: Option<&str>,
        bind_path: Option<&str>,
        uuid: Option<&str>,
    ) -> Result<JsonValue> {
        let parent_path = normalize_path(parent_path)?;
        let parent = parse_virtual_path(&parent_path)?;
        let kind = kind.trim().to_ascii_lowercase();
        let _bind_path = bind_path.map(normalize_path).transpose()?;

        if kind != "image" && source_uri.is_some() {
            return Err(friendly_anyhow(
                source_uri.unwrap_or_default(),
                "source_uri_unsupported",
                "source_uri is only supported when create_tool type is 'image'.",
            ));
        }

        let normalized_action_uuid = action_uuid
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);

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
                        collection_type: Default::default(),
                        app_id: None,
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
            ("thing", VirtualPath::CollectionThingsDir { collection_uuid }) => {
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
            ("action_binding", VirtualPath::CollectionDir { collection_uuid }) => {
                let action_uuid = normalized_action_uuid.clone().ok_or_else(|| {
                    friendly_anyhow(
                        &parent_path,
                        "missing_action_uuid",
                        "Creating an action binding requires action_uuid.",
                    )
                })?;
                let mut bindings =
                    self.list_collection_action_bindings(device_id, &collection_uuid)?;
                if bindings
                    .iter()
                    .any(|binding| binding.action_uuid == action_uuid)
                {
                    return Err(friendly_anyhow(
                        &parent_path,
                        "duplicate_action_binding",
                        "Collection already has this action binding. Use edit_path_tool on actions.json to update it.",
                    ));
                }
                let binding = EntityActionBinding {
                    action_uuid: action_uuid.clone(),
                    label_override: title.map(ToString::to_string),
                    args_json: parse_action_args_json(&parent_path, content)?,
                };
                bindings.push(binding.clone());
                self.things_set_collection_action_bindings(device_id, &collection_uuid, &bindings)?;
                (
                    format!("/collection/{collection_uuid}/actions.json"),
                    action_uuid.clone(),
                    json!({
                        "collection_uuid": collection_uuid,
                        "binding": binding,
                    }),
                )
            }
            (
                "action_binding",
                VirtualPath::ThingDir {
                    collection_uuid,
                    thing_uuid,
                },
            ) => {
                let action_uuid = normalized_action_uuid.clone().ok_or_else(|| {
                    friendly_anyhow(
                        &parent_path,
                        "missing_action_uuid",
                        "Creating an action binding requires action_uuid.",
                    )
                })?;
                let mut bindings = self.list_thing_action_bindings(device_id, &thing_uuid)?;
                if bindings
                    .iter()
                    .any(|binding| binding.action_uuid == action_uuid)
                {
                    return Err(friendly_anyhow(
                        &parent_path,
                        "duplicate_action_binding",
                        "Thing already has this action binding. Use edit_path_tool on actions.json to update it.",
                    ));
                }
                let binding = EntityActionBinding {
                    action_uuid: action_uuid.clone(),
                    label_override: title.map(ToString::to_string),
                    args_json: parse_action_args_json(&parent_path, content)?,
                };
                bindings.push(binding.clone());
                self.things_set_thing_action_bindings(device_id, &thing_uuid, &bindings)?;
                (
                    format!("/collection/{collection_uuid}/things/{thing_uuid}/actions.json"),
                    action_uuid.clone(),
                    json!({
                        "collection_uuid": collection_uuid,
                        "thing_uuid": thing_uuid,
                        "binding": binding,
                    }),
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
                        payload: ContentEntryPayload::Image(ImageField::new(
                            source_uri.to_string(),
                        )),
                    },
                )?;

                let after_entries = self.things_get_content_entries(device_id, &thing_uuid)?;
                let entry_index = after_entries
                    .iter()
                    .position(|entry| entry.id == entry_id)
                    .ok_or_else(|| {
                        anyhow!(
                            "Created image entry '{}' was not found after insertion",
                            entry_id
                        )
                    })?;
                let entry_path = format!(
                    "/collection/{collection_uuid}/things/{thing_uuid}/entries.{entry_index}"
                );

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
                    .ok_or_else(|| {
                        anyhow!(
                            "Created json_object entry '{}' was not found after insertion",
                            entry_id
                        )
                    })?;
                let entry_path = format!(
                    "/collection/{collection_uuid}/things/{thing_uuid}/entries.{entry_index}"
                );

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
            ("action_binding", _) => {
                return Err(friendly_anyhow(
                    &parent_path,
                    "invalid_parent",
                    "action_binding can only be created under '/collection/{collection_uuid}' or '/collection/{collection_uuid}/things/{thing_uuid}'.",
                ));
            }
            _ => {
                return Err(friendly_anyhow(
                    &parent_path,
                    "invalid_type",
                    "create_tool type must be 'collection', 'thing', 'image', 'json_object', or 'action_binding'.",
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
        let actions = self.list_actions()?;
        let tree_data = self.things_local_service().tree_data(device_id)?;

        self.build_tree_node_from_tree_data(path, &actions, &tree_data)
    }

    fn build_tree_node_from_tree_data(
        &self,
        path: &VirtualPath,
        actions: &[crate::types::ActionDefinition],
        tree_data: &crate::things_crdt::ThingsTreeData,
    ) -> Result<TreeNode> {
        let index = TreeIndex::build(tree_data);

        match path {
            VirtualPath::Root => {
                let collection_children = render_collection_listing(tree_data, &index, None, true);
                Ok(TreeNode::with_children(
                    ROOT_PATH,
                    vec![
                        TreeNode::with_children("action/", render_action_listing(actions, true)),
                        TreeNode::with_children("collection/", collection_children),
                    ],
                ))
            }
            VirtualPath::ActionRoot => Ok(TreeNode::with_children(
                "/action/",
                render_action_listing(actions, false),
            )),
            VirtualPath::ActionDir { action_uuid } => {
                let action = self.fetch_action_or_err(action_uuid, "/action")?;
                Ok(TreeNode::with_children(
                    format!("/action/{action_uuid}/"),
                    vec![
                        TreeNode::new(format!("name [value=\"{}\"]", action.title)),
                        TreeNode::new("metadata.json"),
                        TreeNode::new("input.schema.json"),
                        TreeNode::new("output.schema.json"),
                        TreeNode::new("script.js"),
                        TreeNode::new("latest-invocation.json"),
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
                "tree_tool expects a directory path such as /, /action, /collection, /collection/{collection_uuid}, or /collection/{collection_uuid}/things/{thing_uuid}.",
            )),
        }
    }

    fn read_virtual_path_inner(&self, device_id: &str, path: &VirtualPath) -> Result<String> {
        match path {
            VirtualPath::ActionName { action_uuid } => {
                let action = self.fetch_action_or_err(action_uuid, &display_path(path))?;
                Ok(action.title)
            }
            VirtualPath::ActionMetadata { action_uuid } => {
                let action = self.fetch_action_or_err(action_uuid, &display_path(path))?;
                let mut metadata = match action.metadata_json {
                    JsonValue::Object(object) => object,
                    other => {
                        let mut object = serde_json::Map::new();
                        object.insert("metadata".to_string(), other);
                        object
                    }
                };
                metadata.insert("action_uuid".to_string(), json!(action.action_uuid));
                metadata.insert("name".to_string(), json!(action.name));
                metadata.insert("title".to_string(), json!(action.title));
                metadata.insert("description".to_string(), json!(action.description));
                metadata.insert("version".to_string(), json!(action.version));
                metadata.insert("category".to_string(), json!(action.category));
                metadata.insert("enabled".to_string(), json!(action.enabled));
                serde_json::to_string_pretty(&metadata)
                    .context("Failed to serialize action metadata")
            }
            VirtualPath::ActionInputSchema { action_uuid } => {
                let action = self.fetch_action_or_err(action_uuid, &display_path(path))?;
                serde_json::to_string_pretty(&action.input_schema_json)
                    .context("Failed to serialize action input schema")
            }
            VirtualPath::ActionOutputSchema { action_uuid } => {
                let action = self.fetch_action_or_err(action_uuid, &display_path(path))?;
                serde_json::to_string_pretty(&action.output_schema_json.unwrap_or(JsonValue::Null))
                    .context("Failed to serialize action output schema")
            }
            VirtualPath::ActionScript { action_uuid } => {
                let action = self.fetch_action_or_err(action_uuid, &display_path(path))?;
                Ok(action.script_source)
            }
            VirtualPath::ActionLatestInvocation { action_uuid } => self
                .latest_action_invocation_json(action_uuid)?
                .map(Ok)
                .unwrap_or_else(|| {
                    serde_json::to_string_pretty(&JsonValue::Null)
                        .context("Failed to serialize empty action invocation")
                }),
            VirtualPath::CollectionActions { collection_uuid } => {
                let bindings =
                    self.resolve_collection_action_bindings(device_id, collection_uuid)?;
                serde_json::to_string_pretty(&bindings)
                    .context("Failed to serialize collection action bindings")
            }
            VirtualPath::CollectionCardJsx { collection_uuid } => Ok(self
                .get_collection_card_jsx(device_id, collection_uuid)?
                .unwrap_or_default()),
            VirtualPath::CollectionName { collection_uuid } => {
                let snapshot = self.things_list_snapshot_lite(device_id)?;
                let collection = snapshot
                    .collections
                    .into_iter()
                    .find(|item| item.uuid == *collection_uuid)
                    .ok_or_else(|| {
                        friendly_anyhow(
                            &display_path(path),
                            "collection_not_found",
                            &format!("Collection '{}' was not found.", collection_uuid),
                        )
                    })?;
                Ok(collection.title)
            }
            VirtualPath::ThingName { thing_uuid, .. } => {
                let snapshot = self.things_list_snapshot_lite(device_id)?;
                let thing = snapshot
                    .things
                    .into_iter()
                    .find(|item| item.uuid == *thing_uuid)
                    .ok_or_else(|| {
                        friendly_anyhow(
                            &display_path(path),
                            "thing_not_found",
                            &format!("Thing '{}' was not found.", thing_uuid),
                        )
                    })?;
                Ok(thing.title)
            }
            VirtualPath::ThingActions { thing_uuid, .. } => {
                let bindings = self.resolve_thing_action_bindings(device_id, thing_uuid)?;
                serde_json::to_string_pretty(&bindings)
                    .context("Failed to serialize thing action bindings")
            }
            VirtualPath::ThingStatus { thing_uuid, .. } => {
                let snapshot = self.things_list_snapshot_lite(device_id)?;
                let thing = snapshot
                    .things
                    .into_iter()
                    .find(|item| item.uuid == *thing_uuid)
                    .ok_or_else(|| {
                        friendly_anyhow(
                            &display_path(path),
                            "thing_not_found",
                            &format!("Thing '{}' was not found.", thing_uuid),
                        )
                    })?;
                Ok(thing.status)
            }
            VirtualPath::ThingContent {
                collection_uuid,
                thing_uuid,
            } => self.render_thing_content_markdown(device_id, collection_uuid, thing_uuid),
            VirtualPath::ThingEntry {
                thing_uuid, index, ..
            } => {
                let entry = self.content_entry_by_index(device_id, thing_uuid, *index)?;
                serde_json::to_string_pretty(&entry).context("Failed to serialize content entry")
            }
            VirtualPath::ThingEntryData {
                thing_uuid, index, ..
            } => {
                let entry = self.content_entry_by_index(device_id, thing_uuid, *index)?;
                let data = self
                    .things_get_json_object_entry_data(device_id, thing_uuid, &entry.id)?
                    .unwrap_or_else(|| json!({}));
                serde_json::to_string_pretty(&data).context("Failed to serialize json_object data")
            }
            VirtualPath::ThingEntrySchema {
                thing_uuid, index, ..
            } => {
                let entry = self.content_entry_by_index(device_id, thing_uuid, *index)?;
                let schema = self
                    .things_get_json_object_entry_schema(device_id, thing_uuid, &entry.id)?
                    .unwrap_or(JsonValue::Null);
                serde_json::to_string_pretty(&schema)
                    .context("Failed to serialize json_object schema")
            }
            _ => Err(friendly_anyhow(
                &display_path(path),
                "read_unsupported",
                "cat_tool only supports file nodes such as name, card.jsx, status, content.md, entries.{idx}, entries.{idx}.data.json, entries.{idx}.schema.json, metadata.json, input.schema.json, output.schema.json, script.js, and latest-invocation.json.",
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
            Some(
                object
                    .get("title")
                    .and_then(JsonValue::as_str)
                    .map(|value| value.to_string()),
            )
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
        self.things_local_service().render_thing_content_markdown(
            device_id,
            collection_uuid,
            thing_uuid,
        )
    }

    fn rename_collection(&self, device_id: &str, collection_uuid: &str, title: &str) -> Result<()> {
        let snapshot = self.things_list_snapshot_lite(device_id)?;
        let collection = snapshot
            .collections
            .iter()
            .find(|item| item.uuid == collection_uuid)
            .ok_or_else(|| {
                friendly_anyhow(
                    &format!("/collection/{collection_uuid}/name"),
                    "collection_not_found",
                    &format!("Collection '{}' was not found.", collection_uuid),
                )
            })?;

        self.things_upsert_collection(
            device_id,
            crate::things_crdt::ThingCollectionUpsert {
                uuid: collection_uuid.to_string(),
                title: title.to_string(),
                collection_type: collection.collection_type,
                app_id: collection.app_id.clone(),
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
            .ok_or_else(|| {
                friendly_anyhow(
                    &format!("/thing/{thing_uuid}/name"),
                    "thing_not_found",
                    &format!("Thing '{}' was not found.", thing_uuid),
                )
            })?;

        self.things_upsert_thing(
            device_id,
            ThingUpsert {
                uuid: thing.uuid,
                title: title.to_string(),
                datatype: thing.datatype,
                data: None,
                collection_uuid: thing.collection_uuid,
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
                &format!(
                    "entries.{index} is out of range for thing '{}'.",
                    thing_uuid
                ),
            )
        })
    }

    fn fetch_action_or_err(
        &self,
        action_uuid: &str,
        path: &str,
    ) -> Result<crate::types::ActionDefinition> {
        self.storage.fetch_action(action_uuid)?.ok_or_else(|| {
            friendly_anyhow(
                path,
                "action_not_found",
                &format!("Action '{}' was not found.", action_uuid),
            )
        })
    }
}
