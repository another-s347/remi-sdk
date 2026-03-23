use prost_types::{Struct as ProstStruct, Value as ProstValue};
use serde_json::{Value as JsonValue, json};

const LOCAL_TOOL_NAMES: &[&str] = &["describe_skill", "handoff_to_deep_agent"];
const MANAGED_EXTERNAL_TOOL_NAMES: &[&str] = &[
    "create_trigger_simple",
    "delete_trigger",
    "test_trigger",
    "list_triggers_tool",
    "list_things_tool",
    "get_things_tool",
    "add_things_tool",
    "edit_things_tool",
    "remove_things_tool",
    "move_things_tool",
    "resolve_uri",
    "retrieve_events",
    "abstract_events",
    "create_trigger",
];

pub(crate) fn chat_start_extra_tools(user_state: Option<&JsonValue>) -> Vec<ProstStruct> {
    tool_definitions_for_user_state(user_state)
        .into_iter()
        .map(json_to_prost_struct)
        .collect()
}

pub(crate) fn normalize_resume_state_tool_definitions(mut state: JsonValue) -> JsonValue {
    let Some(state_obj) = state.as_object_mut() else {
        return state;
    };

    let mut definitions = tool_definitions_for_user_state(state_obj.get("user_state"));
    let preserved_extra_defs = state_obj
        .get("tool_definitions")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter(|definition| {
            tool_name(definition).is_none_or(|name| {
                !LOCAL_TOOL_NAMES.contains(&name) && !MANAGED_EXTERNAL_TOOL_NAMES.contains(&name)
            })
        })
        .cloned()
        .collect::<Vec<_>>();

    definitions.extend(preserved_extra_defs);
    state_obj.insert(
        "tool_definitions".to_string(),
        JsonValue::Array(dedupe_tool_definitions(definitions)),
    );

    state
}

fn deep_mode(user_state: Option<&JsonValue>) -> bool {
    user_state
        .and_then(|value| value.get("agent_mode"))
        .and_then(JsonValue::as_str)
        .is_some_and(|mode| mode == "deep")
}

fn tool_definitions_for_user_state(user_state: Option<&JsonValue>) -> Vec<JsonValue> {
    if deep_mode(user_state) {
        deep_tool_definitions()
    } else {
        light_tool_definitions()
    }
}

fn dedupe_tool_definitions(definitions: Vec<JsonValue>) -> Vec<JsonValue> {
    let mut seen = std::collections::HashSet::new();
    let mut deduped = Vec::with_capacity(definitions.len());

    for definition in definitions {
        let Some(name) = tool_name(&definition) else {
            continue;
        };

        if seen.insert(name.to_string()) {
            deduped.push(definition);
        }
    }

    deduped
}

fn tool_name(definition: &JsonValue) -> Option<&str> {
    definition
        .get("function")
        .and_then(|function| function.get("name"))
        .and_then(JsonValue::as_str)
}

fn json_to_prost_struct(value: JsonValue) -> ProstStruct {
    let JsonValue::Object(map) = value else {
        return ProstStruct::default();
    };

    let fields = map
        .into_iter()
        .map(|(key, value)| (key, json_to_prost_value(value)))
        .collect();

    ProstStruct { fields }
}

fn json_to_prost_value(value: JsonValue) -> ProstValue {
    use prost_types::value::Kind;

    let kind = match value {
        JsonValue::Null => Kind::NullValue(0),
        JsonValue::Bool(value) => Kind::BoolValue(value),
        JsonValue::Number(value) => Kind::NumberValue(value.as_f64().unwrap_or(0.0)),
        JsonValue::String(value) => Kind::StringValue(value),
        JsonValue::Array(values) => Kind::ListValue(prost_types::ListValue {
            values: values.into_iter().map(json_to_prost_value).collect(),
        }),
        JsonValue::Object(_) => Kind::StructValue(json_to_prost_struct(value)),
    };

    ProstValue { kind: Some(kind) }
}

fn tool(name: &str, description: &str, parameters: JsonValue) -> JsonValue {
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters
        }
    })
}

fn obj(props: JsonValue, required: &[&str]) -> JsonValue {
    json!({
        "type": "object",
        "properties": props,
        "required": required
    })
}

fn str_prop(description: &str) -> JsonValue {
    json!({ "type": "string", "description": description })
}

fn bool_prop(description: &str) -> JsonValue {
    json!({ "type": "boolean", "description": description })
}

fn int_prop(description: &str) -> JsonValue {
    json!({ "type": "integer", "description": description })
}

fn nullable_str(description: &str) -> JsonValue {
    json!({ "type": ["string", "null"], "description": description })
}

fn resolve_uri() -> JsonValue {
    tool(
        "resolve_uri",
        "Fetch a URL or remi:// URI and return structured metadata or image content. Supports http/https URLs and remi://file URIs.",
        obj(
            json!({
                "uri": str_prop("URL or URI to resolve. Accepts http/https URLs and remi://file/<path>?type=<mime> URIs.")
            }),
            &["uri"],
        ),
    )
}

fn list_things_tool() -> JsonValue {
    tool(
        "list_things_tool",
        "List Things and collections stored on the device.",
        obj(
            json!({
                "entity_type": {
                    "type": ["string", "null"],
                    "description": "Filter: 'all', 'thing', or 'collection'. Defaults to 'all'.",
                    "enum": ["all", "thing", "collection", null]
                },
                "include_content": bool_prop("Whether to include full markdown content. Defaults to false.")
            }),
            &[],
        ),
    )
}

fn get_things_tool() -> JsonValue {
    tool(
        "get_things_tool",
        "Fetch the full details of a single Thing by UUID.",
        obj(
            json!({ "uuid": str_prop("UUID of the Thing to fetch.") }),
            &["uuid"],
        ),
    )
}

fn add_things_tool() -> JsonValue {
    tool(
        "add_things_tool",
        "Create a new Thing inside a collection.",
        obj(
            json!({
                "title": str_prop("Title of the new Thing."),
                "collection_uuid": str_prop("UUID of the parent collection."),
                "content": str_prop("Initial markdown content."),
                "parent_uuid": nullable_str("UUID of a parent Thing for nesting."),
                "uuid": nullable_str("Optional client-generated UUID.")
            }),
            &["title", "collection_uuid"],
        ),
    )
}

fn edit_things_tool() -> JsonValue {
    tool(
        "edit_things_tool",
        "Edit an existing Thing.",
        obj(
            json!({
                "uuid": str_prop("UUID of the Thing to edit."),
                "edit": {
                    "type": "object",
                    "description": "Edit operation.",
                    "properties": {
                        "operation": {
                            "type": "string",
                            "enum": ["overwrite", "set_title", "str_replace", "insert_at_line", "append"]
                        }
                    },
                    "required": ["operation"]
                }
            }),
            &["uuid", "edit"],
        ),
    )
}

fn remove_things_tool() -> JsonValue {
    tool(
        "remove_things_tool",
        "Permanently delete a Thing or collection by UUID.",
        obj(
            json!({ "uuid": str_prop("UUID of the entity to delete.") }),
            &["uuid"],
        ),
    )
}

fn move_things_tool() -> JsonValue {
    tool(
        "move_things_tool",
        "Move a Thing to a different collection.",
        obj(
            json!({
                "uuid": str_prop("UUID of the Thing to move."),
                "new_collection_uuid": str_prop("Target collection UUID."),
                "new_parent_uuid": nullable_str("New parent Thing UUID, or null.")
            }),
            &["uuid", "new_collection_uuid"],
        ),
    )
}

fn create_trigger_simple() -> JsonValue {
    tool(
        "create_trigger_simple",
        "Create a simple cron-based trigger for a Thing or collection.",
        obj(
            json!({
                "name": str_prop("Display name for the trigger."),
                "cron": str_prop("POSIX 5-field cron expression in +08:00."),
                "condition": nullable_str("Optional CEL boolean condition expression."),
                "bind_uuid": str_prop("UUID of the Thing or collection to bind this trigger to."),
                "bind_type": {
                    "type": "string",
                    "description": "Type of binding entity.",
                    "enum": ["thing", "collection"]
                },
                "user_request": nullable_str("The original user request, for reference.")
            }),
            &["name", "cron", "bind_uuid", "bind_type"],
        ),
    )
}

fn delete_trigger() -> JsonValue {
    tool(
        "delete_trigger",
        "Delete a trigger by UUID.",
        obj(
            json!({ "trigger_uuid": str_prop("UUID of the trigger to delete.") }),
            &["trigger_uuid"],
        ),
    )
}

fn test_trigger() -> JsonValue {
    tool(
        "test_trigger",
        "Validate a trigger JSON configuration using the CEL engine.",
        obj(
            json!({
                "trigger": str_prop("Complete trigger configuration as JSON string.")
            }),
            &["trigger"],
        ),
    )
}

fn list_triggers_tool() -> JsonValue {
    tool(
        "list_triggers_tool",
        "List all triggers registered on the device.",
        obj(
            json!({
                "search_query": nullable_str("Optional filter substring on trigger name."),
                "limit": int_prop("Max results (default 50)."),
                "offset": int_prop("Pagination offset (default 0).")
            }),
            &[],
        ),
    )
}

fn retrieve_events() -> JsonValue {
    tool(
        "retrieve_events",
        "Retrieve telemetry events within an explicit time window.",
        obj(
            json!({
                "start_time": str_prop("ISO-8601 start timestamp."),
                "end_time": str_prop("ISO-8601 end timestamp.")
            }),
            &["start_time", "end_time"],
        ),
    )
}

fn abstract_events() -> JsonValue {
    tool(
        "abstract_events",
        "Create an hourly abstract of telemetry events with totals and top event types.",
        obj(
            json!({
                "top_n": int_prop("Number of top event types per hour to include (default 3).")
            }),
            &[],
        ),
    )
}

fn create_trigger_full() -> JsonValue {
    tool(
        "create_trigger",
        "Create or update a rule-based trigger using JSON configuration with CEL expressions.",
        obj(
            json!({
                "trigger": str_prop("Complete trigger config as JSON string."),
                "bind_uuid": str_prop("UUID of the Thing or collection to bind to."),
                "bind_type": {
                    "type": "string",
                    "enum": ["thing", "collection"],
                    "description": "Type of binding entity."
                },
                "user_request": nullable_str("The original user request."),
                "event_analysis": nullable_str("Analysis of user's recent events."),
                "trigger_uuid": nullable_str("Existing trigger UUID to update, omit for create.")
            }),
            &["trigger", "bind_uuid", "bind_type"],
        ),
    )
}

fn light_tool_definitions() -> Vec<JsonValue> {
    vec![
        create_trigger_simple(),
        delete_trigger(),
        test_trigger(),
        list_triggers_tool(),
        list_things_tool(),
        get_things_tool(),
        add_things_tool(),
        edit_things_tool(),
        remove_things_tool(),
        move_things_tool(),
        resolve_uri(),
    ]
}

fn deep_tool_definitions() -> Vec<JsonValue> {
    vec![
        create_trigger_simple(),
        delete_trigger(),
        test_trigger(),
        list_triggers_tool(),
        list_things_tool(),
        get_things_tool(),
        add_things_tool(),
        edit_things_tool(),
        remove_things_tool(),
        move_things_tool(),
        resolve_uri(),
        retrieve_events(),
        abstract_events(),
        create_trigger_full(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tool_names(definitions: &[JsonValue]) -> Vec<String> {
        definitions
            .iter()
            .filter_map(tool_name)
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn light_mode_omits_deep_only_tools() {
        let names = tool_names(&light_tool_definitions());
        assert!(names.iter().any(|name| name == "create_trigger_simple"));
        assert!(!names.iter().any(|name| name == "retrieve_events"));
        assert!(!names.iter().any(|name| name == "create_trigger"));
    }

    #[test]
    fn normalize_resume_state_swaps_managed_tool_set_by_mode() {
        let mut state = json!({
            "user_state": { "agent_mode": "deep" },
            "tool_definitions": light_tool_definitions(),
        });
        state["tool_definitions"]
            .as_array_mut()
            .expect("tool_definitions should be an array")
            .push(json!({
                "type": "function",
                "function": {
                    "name": "custom_external_tool",
                    "description": "custom",
                    "parameters": {"type": "object", "properties": {}, "required": []}
                }
            }));

        let normalized = normalize_resume_state_tool_definitions(state);
        let names = tool_names(
            normalized["tool_definitions"]
                .as_array()
                .expect("normalized tool_definitions should be an array"),
        );

        assert!(names.iter().any(|name| name == "retrieve_events"));
        assert!(names.iter().any(|name| name == "create_trigger"));
        assert!(names.iter().any(|name| name == "custom_external_tool"));
        assert_eq!(
            names.iter().filter(|name| name.as_str() == "create_trigger_simple").count(),
            1
        );
    }
}