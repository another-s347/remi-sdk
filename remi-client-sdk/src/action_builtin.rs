use serde_json::json;

use crate::types::ActionDefinition;

pub(crate) fn builtin_actions() -> Vec<ActionDefinition> {
    vec![ActionDefinition {
        action_uuid: "builtin.echo_json".to_string(),
        name: "echo_json".to_string(),
        title: "Echo JSON".to_string(),
        description: "Return the resolved action payload for verification and smoke testing."
            .to_string(),
        version: "v1".to_string(),
        category: "utility".to_string(),
        enabled: true,
        metadata_json: json!({
            "builtin": true,
            "supports_manual": true,
            "summary": "Returns the action input, source, and context as structured JSON."
        }),
        script_source: r#"
console.log(`running ${action.uuid}`);
return {
    ok: true,
    action,
    source,
    args,
    context,
};
"#
        .trim()
        .to_string(),
        input_schema_json: json!({
            "type": "object",
            "properties": {
                "args": { "type": ["object", "array", "string", "number", "boolean", "null"] },
                "source": {
                    "type": "object",
                    "properties": {
                        "kind": { "type": "string" },
                        "entity_type": { "type": ["string", "null"] },
                        "entity_uuid": { "type": ["string", "null"] }
                    },
                    "required": ["kind"]
                },
                "context": { "type": "object" }
            },
            "required": ["args", "source", "context"]
        }),
        output_schema_json: Some(json!({
            "type": "object",
            "properties": {
                "ok": { "type": "boolean" },
                "action": { "type": "object" },
                "source": { "type": "object" },
                "args": {},
                "context": { "type": "object" }
            },
            "required": ["ok", "action", "source", "args", "context"]
        })),
    }]
}
