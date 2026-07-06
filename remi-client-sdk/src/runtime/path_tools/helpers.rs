use super::*;

pub(super) fn push_profile_step(
    steps: &mut Vec<VirtualFsProfileStep>,
    name: &str,
    elapsed: std::time::Duration,
) {
    steps.push(VirtualFsProfileStep {
        name: name.to_string(),
        elapsed_ms: elapsed.as_millis() as u64,
    });
}

pub(super) fn normalize_path(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(friendly_anyhow(
            ROOT_PATH,
            "invalid_path",
            "Path must not be empty.",
        ));
    }
    if !trimmed.starts_with('/') {
        return Err(friendly_anyhow(
            trimmed,
            "invalid_path",
            "Path must start with '/'.",
        ));
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

pub(super) fn parse_virtual_path(path: &str) -> Result<VirtualPath> {
    if path == ROOT_PATH {
        return Ok(VirtualPath::Root);
    }

    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    match segments.as_slice() {
        ["action"] => Ok(VirtualPath::ActionRoot),
        ["action", action_uuid] => Ok(VirtualPath::ActionDir {
            action_uuid: (*action_uuid).to_string(),
        }),
        ["action", action_uuid, "name"] => Ok(VirtualPath::ActionName {
            action_uuid: (*action_uuid).to_string(),
        }),
        ["action", action_uuid, "metadata.json"] => Ok(VirtualPath::ActionMetadata {
            action_uuid: (*action_uuid).to_string(),
        }),
        ["action", action_uuid, "input.schema.json"] => Ok(VirtualPath::ActionInputSchema {
            action_uuid: (*action_uuid).to_string(),
        }),
        ["action", action_uuid, "output.schema.json"] => Ok(VirtualPath::ActionOutputSchema {
            action_uuid: (*action_uuid).to_string(),
        }),
        ["action", action_uuid, "script.js"] => Ok(VirtualPath::ActionScript {
            action_uuid: (*action_uuid).to_string(),
        }),
        ["action", action_uuid, "latest-invocation.json"] => {
            Ok(VirtualPath::ActionLatestInvocation {
                action_uuid: (*action_uuid).to_string(),
            })
        }
        ["collection"] => Ok(VirtualPath::CollectionRoot),
        ["collection", collection_uuid] => Ok(VirtualPath::CollectionDir {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "name"] => Ok(VirtualPath::CollectionName {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "card.jsx"] => Ok(VirtualPath::CollectionCardJsx {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "actions.json"] => Ok(VirtualPath::CollectionActions {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things"] => Ok(VirtualPath::CollectionThingsDir {
            collection_uuid: (*collection_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things", thing_uuid] => Ok(VirtualPath::ThingDir {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        ["collection", collection_uuid, "things", thing_uuid, "name"] => {
            Ok(VirtualPath::ThingName {
                collection_uuid: (*collection_uuid).to_string(),
                thing_uuid: (*thing_uuid).to_string(),
            })
        }
        [
            "collection",
            collection_uuid,
            "things",
            thing_uuid,
            "actions.json",
        ] => Ok(VirtualPath::ThingActions {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        [
            "collection",
            collection_uuid,
            "things",
            thing_uuid,
            "status",
        ] => Ok(VirtualPath::ThingStatus {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        [
            "collection",
            collection_uuid,
            "things",
            thing_uuid,
            "content.md",
        ] => Ok(VirtualPath::ThingContent {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        [
            "collection",
            collection_uuid,
            "things",
            thing_uuid,
            entry_segment,
        ] if entry_segment.starts_with("entries.") && entry_segment.ends_with(".data.json") => {
            let index = entry_segment[8..entry_segment.len() - ".data.json".len()]
                .parse::<usize>()
                .map_err(|_| {
                    friendly_anyhow(
                        path,
                        "invalid_entry_index",
                        "entries.{idx}.data.json must use a non-negative integer index.",
                    )
                })?;
            Ok(VirtualPath::ThingEntryData {
                collection_uuid: (*collection_uuid).to_string(),
                thing_uuid: (*thing_uuid).to_string(),
                index,
            })
        }
        [
            "collection",
            collection_uuid,
            "things",
            thing_uuid,
            entry_segment,
        ] if entry_segment.starts_with("entries.") && entry_segment.ends_with(".schema.json") => {
            let index = entry_segment[8..entry_segment.len() - ".schema.json".len()]
                .parse::<usize>()
                .map_err(|_| {
                    friendly_anyhow(
                        path,
                        "invalid_entry_index",
                        "entries.{idx}.schema.json must use a non-negative integer index.",
                    )
                })?;
            Ok(VirtualPath::ThingEntrySchema {
                collection_uuid: (*collection_uuid).to_string(),
                thing_uuid: (*thing_uuid).to_string(),
                index,
            })
        }
        [
            "collection",
            collection_uuid,
            "things",
            thing_uuid,
            entry_segment,
        ] if entry_segment.starts_with("entries.") => {
            let index = entry_segment[8..].parse::<usize>().map_err(|_| {
                friendly_anyhow(
                    path,
                    "invalid_entry_index",
                    "entries.{idx} must end with a non-negative integer index.",
                )
            })?;
            Ok(VirtualPath::ThingEntry {
                collection_uuid: (*collection_uuid).to_string(),
                thing_uuid: (*thing_uuid).to_string(),
                index,
            })
        }
        [
            "collection",
            collection_uuid,
            "things",
            thing_uuid,
            "things",
        ] => Ok(VirtualPath::ThingChildrenDir {
            collection_uuid: (*collection_uuid).to_string(),
            thing_uuid: (*thing_uuid).to_string(),
        }),
        _ => Err(friendly_anyhow(
            path,
            "invalid_path",
            "Unsupported path. Expected /action/... or /collection/... according to the virtual filesystem contract.",
        )),
    }
}

pub(super) fn render_action_listing(
    actions: &[crate::types::ActionDefinition],
    limit_preview: bool,
) -> Vec<TreeNode> {
    let mut nodes = actions
        .iter()
        .take(if limit_preview {
            ACTION_PREVIEW_LIMIT
        } else {
            actions.len()
        })
        .map(|action| {
            TreeNode::new(format!(
                "{}/ [title=\"{}\"]",
                action.action_uuid, action.title
            ))
        })
        .collect::<Vec<_>>();

    if limit_preview && actions.len() > ACTION_PREVIEW_LIMIT {
        nodes.push(TreeNode::new(format!(
            "Has {} More",
            actions.len() - ACTION_PREVIEW_LIMIT
        )));
    }

    if !limit_preview {
        for (node, action) in nodes.iter_mut().zip(actions.iter()) {
            node.children = vec![
                TreeNode::new(format!("name [value=\"{}\"]", action.title)),
                TreeNode::new("metadata.json"),
                TreeNode::new("input.schema.json"),
                TreeNode::new("output.schema.json"),
                TreeNode::new("script.js"),
                TreeNode::new("latest-invocation.json"),
            ];
        }
    }

    nodes
}

pub(super) fn render_collection_listing(
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
                TreeNode::new("card.jsx"),
                TreeNode::new("actions.json"),
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

pub(super) fn collection_dir_children(
    tree_data: &crate::things_crdt::ThingsTreeData,
    index: &TreeIndex<'_>,
    collection_uuid: &str,
) -> Result<Vec<TreeNode>> {
    let collection = index.collection(collection_uuid).ok_or_else(|| {
        friendly_anyhow(
            &format!("/collection/{collection_uuid}"),
            "collection_not_found",
            &format!("Collection '{}' was not found.", collection_uuid),
        )
    })?;

    let root_things = index.child_things(collection_uuid, None);

    Ok(vec![
        TreeNode::new(format!("name [value=\"{}\"]", collection.title)),
        TreeNode::new("card.jsx"),
        TreeNode::new("actions.json"),
        TreeNode::with_children(
            "things/",
            render_thing_nodes(&root_things, index)
                .into_iter()
                .map(|mut node| {
                    if node.children.is_empty() {
                        if let Some(thing_uuid) = extract_uuid_from_dir_label(&node.label) {
                            if let Ok(children) =
                                thing_dir_children(tree_data, index, collection_uuid, &thing_uuid)
                            {
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

pub(super) fn thing_dir_children(
    _tree_data: &crate::things_crdt::ThingsTreeData,
    index: &TreeIndex<'_>,
    collection_uuid: &str,
    thing_uuid: &str,
) -> Result<Vec<TreeNode>> {
    let thing = index.thing(collection_uuid, thing_uuid).ok_or_else(|| {
        friendly_anyhow(
            &format!("/collection/{collection_uuid}/things/{thing_uuid}"),
            "thing_not_found",
            &format!(
                "Thing '{}' was not found in collection '{}'.",
                thing_uuid, collection_uuid
            ),
        )
    })?;

    let mut children = vec![
        TreeNode::new(format!("name [value=\"{}\"]", thing.title)),
        TreeNode::new("actions.json"),
        thing_status_node(&thing.status),
        TreeNode::new("content.md"),
    ];

    children.extend(thing.entries.iter().enumerate().flat_map(|(index, entry)| {
        let mut nodes = vec![TreeNode::new(format!("entries.{}", index))];
        if matches!(entry.payload, ContentEntryPayload::JsonObject(_)) {
            nodes.push(TreeNode::new(format!("entries.{}.data.json", index)));
            nodes.push(TreeNode::new(format!("entries.{}.schema.json", index)));
        }
        nodes
    }));

    let thing_children = index.child_things(collection_uuid, Some(thing_uuid));
    if !thing_children.is_empty() {
        children.push(TreeNode::with_children(
            "things/",
            render_thing_nodes(&thing_children, index),
        ));
    }

    Ok(children)
}

pub(super) fn render_thing_nodes(
    things: &[&crate::things_crdt::TreeThingData],
    index: &TreeIndex<'_>,
) -> Vec<TreeNode> {
    things
        .iter()
        .map(|thing| {
            let mut children = vec![
                TreeNode::new("actions.json"),
                thing_status_node(&thing.status),
            ];
            let has_children = index.has_children(&thing.collection_uuid, &thing.uuid);
            if has_children {
                children.push(TreeNode::new("things/"));
            }
            TreeNode::with_children(
                format!(
                    "{}/ [name=\"{}\", status=\"{}\"]",
                    thing.uuid, thing.title, thing.status
                ),
                children,
            )
        })
        .collect()
}

pub(super) fn thing_status_node(status: &str) -> TreeNode {
    TreeNode::new(format!("status [value=\"{}\"]", status))
}

pub(super) fn render_tree(root: &TreeNode) -> String {
    let mut lines = vec![root.label.clone()];
    for (index, child) in root.children.iter().enumerate() {
        let is_last = index + 1 == root.children.len();
        render_tree_child(child, "", is_last, &mut lines);
    }
    lines.join("\n")
}

pub(super) fn render_tree_child(
    node: &TreeNode,
    prefix: &str,
    is_last: bool,
    lines: &mut Vec<String>,
) {
    let branch = if is_last { "`-- " } else { "|-- " };
    lines.push(format!("{}{}{}", prefix, branch, node.label));
    let child_prefix = if is_last {
        format!("{}    ", prefix)
    } else {
        format!("{}|   ", prefix)
    };

    for (index, child) in node.children.iter().enumerate() {
        render_tree_child(
            child,
            &child_prefix,
            index + 1 == node.children.len(),
            lines,
        );
    }
}

pub(super) fn parse_action_args_json(path: &str, content: Option<&str>) -> Result<JsonValue> {
    match content.map(str::trim).filter(|value| !value.is_empty()) {
        Some(raw) => serde_json::from_str::<JsonValue>(raw).map_err(|error| {
            friendly_anyhow(
                path,
                "invalid_content",
                &format!("Action binding content must be valid JSON: {error}"),
            )
        }),
        None => Ok(JsonValue::Object(Default::default())),
    }
}

pub(super) fn parse_entity_action_bindings_value(
    path: &str,
    value: Option<&JsonValue>,
) -> Result<Vec<EntityActionBinding>> {
    let Some(value) = value else {
        return Err(friendly_anyhow(
            path,
            "invalid_value",
            "actions.json requires an array, an object, or null.",
        ));
    };

    if value.is_null() {
        return Ok(Vec::new());
    }

    if let Some(text) = value.as_str() {
        if text.trim().is_empty() {
            return Ok(Vec::new());
        }
        let parsed = serde_json::from_str::<JsonValue>(text).map_err(|error| {
            friendly_anyhow(
                path,
                "invalid_value",
                &format!("actions.json string value must be valid JSON: {error}"),
            )
        })?;
        return parse_entity_action_bindings_value(path, Some(&parsed));
    }

    if let Some(array) = value.as_array() {
        return serde_json::from_value::<Vec<EntityActionBinding>>(JsonValue::Array(array.clone()))
            .map_err(|error| {
                friendly_anyhow(
                    path,
                    "invalid_value",
                    &format!("actions.json must be an array of action bindings: {error}"),
                )
            });
    }

    serde_json::from_value::<EntityActionBinding>(value.clone())
        .map(|binding| vec![binding])
        .map_err(|error| {
            friendly_anyhow(
                path,
                "invalid_value",
                &format!("actions.json must be an action binding object or array: {error}"),
            )
        })
}

pub(super) fn normalize_operation(operation: &str) -> &str {
    let trimmed = operation.trim();
    if trimmed.is_empty() {
        "overwrite"
    } else {
        trimmed
    }
}

pub(super) fn display_path(path: &VirtualPath) -> String {
    match path {
        VirtualPath::Root => ROOT_PATH.to_string(),
        VirtualPath::ActionRoot => "/action".to_string(),
        VirtualPath::ActionDir { action_uuid } => format!("/action/{action_uuid}"),
        VirtualPath::ActionName { action_uuid } => format!("/action/{action_uuid}/name"),
        VirtualPath::ActionMetadata { action_uuid } => {
            format!("/action/{action_uuid}/metadata.json")
        }
        VirtualPath::ActionInputSchema { action_uuid } => {
            format!("/action/{action_uuid}/input.schema.json")
        }
        VirtualPath::ActionOutputSchema { action_uuid } => {
            format!("/action/{action_uuid}/output.schema.json")
        }
        VirtualPath::ActionScript { action_uuid } => format!("/action/{action_uuid}/script.js"),
        VirtualPath::ActionLatestInvocation { action_uuid } => {
            format!("/action/{action_uuid}/latest-invocation.json")
        }
        VirtualPath::CollectionRoot => "/collection".to_string(),
        VirtualPath::CollectionDir { collection_uuid } => format!("/collection/{collection_uuid}"),
        VirtualPath::CollectionName { collection_uuid } => {
            format!("/collection/{collection_uuid}/name")
        }
        VirtualPath::CollectionCardJsx { collection_uuid } => {
            format!("/collection/{collection_uuid}/card.jsx")
        }
        VirtualPath::CollectionActions { collection_uuid } => {
            format!("/collection/{collection_uuid}/actions.json")
        }
        VirtualPath::CollectionThingsDir { collection_uuid } => {
            format!("/collection/{collection_uuid}/things")
        }
        VirtualPath::ThingDir {
            collection_uuid,
            thing_uuid,
        } => format!("/collection/{collection_uuid}/things/{thing_uuid}"),
        VirtualPath::ThingName {
            collection_uuid,
            thing_uuid,
        } => format!("/collection/{collection_uuid}/things/{thing_uuid}/name"),
        VirtualPath::ThingActions {
            collection_uuid,
            thing_uuid,
        } => format!("/collection/{collection_uuid}/things/{thing_uuid}/actions.json"),
        VirtualPath::ThingStatus {
            collection_uuid,
            thing_uuid,
        } => format!("/collection/{collection_uuid}/things/{thing_uuid}/status"),
        VirtualPath::ThingContent {
            collection_uuid,
            thing_uuid,
        } => format!("/collection/{collection_uuid}/things/{thing_uuid}/content.md"),
        VirtualPath::ThingEntry {
            collection_uuid,
            thing_uuid,
            index,
        } => format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{index}"),
        VirtualPath::ThingEntryData {
            collection_uuid,
            thing_uuid,
            index,
        } => format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{index}.data.json"),
        VirtualPath::ThingEntrySchema {
            collection_uuid,
            thing_uuid,
            index,
        } => {
            format!("/collection/{collection_uuid}/things/{thing_uuid}/entries.{index}.schema.json")
        }
        VirtualPath::ThingChildrenDir {
            collection_uuid,
            thing_uuid,
        } => format!("/collection/{collection_uuid}/things/{thing_uuid}/things"),
    }
}

pub(super) fn extract_uuid_from_dir_label(label: &str) -> Option<String> {
    label
        .split('/')
        .next()
        .map(|value| value.trim().to_string())
}

pub(super) fn friendly_anyhow(path: &str, code: &str, message: &str) -> anyhow::Error {
    anyhow!(
        json!({
            "error": code,
            "path": path,
            "message": message,
        })
        .to_string()
    )
}
