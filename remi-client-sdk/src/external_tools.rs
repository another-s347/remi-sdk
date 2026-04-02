use serde_json::{Value as JsonValue, json};

use crate::InterruptHandler;
use crate::chat_types::{
    InterruptAction, PendingInterrupt, PendingToolCall, PendingToolExecutionState,
    RichHandlerResult, ToolExecutionOutcome,
};
use crate::interrupt_handler::{InterruptHandlerRegistry, extract_interrupt_type};

#[derive(Debug, Clone)]
pub struct ExternalToolCallRequest {
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments: JsonValue,
}

#[derive(Debug, Clone, Default)]
pub struct ExternalToolExecutionPlan {
    pub resolved_results: Vec<ToolExecutionOutcome>,
    pub pending_calls: Vec<PendingToolCall>,
    pub first_pending_interrupt: Option<PendingInterrupt>,
    pub things_changed: bool,
    pub trigger_scheduler_sync_needed: bool,
}

#[derive(Clone, Default)]
pub struct ExternalToolExecutor {
    registry: InterruptHandlerRegistry,
}

impl ExternalToolExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_registry(registry: InterruptHandlerRegistry) -> Self {
        Self { registry }
    }

    pub fn register<H: InterruptHandler + 'static>(
        &mut self,
        interrupt_type: impl Into<String>,
        handler: H,
    ) {
        self.registry.register(interrupt_type, handler);
    }

    pub fn with_handler<H: InterruptHandler + 'static>(
        mut self,
        interrupt_type: impl Into<String>,
        handler: H,
    ) -> Self {
        self.registry.register(interrupt_type, handler);
        self
    }

    pub fn has_handler(&self, interrupt_type: &str) -> bool {
        self.registry.has_handler(interrupt_type)
    }

    pub async fn resolve_calls(
        &self,
        calls: impl IntoIterator<Item = ExternalToolCallRequest>,
    ) -> ExternalToolExecutionPlan {
        let mut plan = ExternalToolExecutionPlan::default();

        for tool_call in calls {
            let display_data = tool_call_display_payload(&tool_call.tool_name, &tool_call.arguments);
            let pending_call = PendingToolCall {
                tool_call_id: tool_call.tool_call_id.clone(),
                tool_name: tool_call.tool_name.clone(),
                arguments: tool_call.arguments.clone(),
                display_data: display_data.clone(),
            };

            match self.registry.process(&tool_call.tool_call_id, &display_data).await {
                InterruptAction::AutoResume(values) => {
                    let resume_value = values
                        .get(&tool_call.tool_call_id)
                        .cloned()
                        .or_else(|| values.values().next().cloned())
                        .unwrap_or_else(|| {
                            RichHandlerResult::Json(json!({
                                "error": "handler did not return a resume value"
                            }))
                        });
                    let outcome = raw_resume_value_to_outcome(&pending_call, resume_value);
                    if extract_interrupt_type(&display_data) == "trigger_rule_published"
                        && outcome.error.is_none()
                    {
                        plan.trigger_scheduler_sync_needed = true;
                    }
                    plan.things_changed = true;
                    plan.resolved_results.push(outcome);
                }
                InterruptAction::WaitForUser { pending } => {
                    let display_data = pending
                        .iter()
                        .find(|item| item.interrupt_id == tool_call.tool_call_id)
                        .map(|item| item.display_data.clone())
                        .unwrap_or(display_data);
                    plan.pending_calls.push(PendingToolCall {
                        display_data,
                        ..pending_call
                    });
                }
                InterruptAction::Skip => {
                    plan.pending_calls.push(pending_call);
                }
            }
        }

        plan.first_pending_interrupt = plan.pending_calls.first().map(|call| PendingInterrupt {
            interrupt_id: call.tool_call_id.clone(),
            interrupt_type: {
                let extracted = extract_interrupt_type(&call.display_data);
                if extracted.trim().is_empty() {
                    call.tool_name.clone()
                } else {
                    extracted
                }
            },
            display_data: call.display_data.clone(),
        });

        plan
    }
}

pub fn manual_resume_outcomes(
    pending: &PendingToolExecutionState,
    resume_value: JsonValue,
) -> Result<Vec<ToolExecutionOutcome>, String> {
    if pending.pending_calls.is_empty() {
        return Ok(Vec::new());
    }

    let mapping = resume_value.as_object().cloned();
    let mut outcomes = Vec::with_capacity(pending.pending_calls.len());

    for call in &pending.pending_calls {
        let raw = mapping
            .as_ref()
            .and_then(|map| map.get(&call.tool_call_id).cloned())
            .or_else(|| {
                if pending.pending_calls.len() == 1 {
                    Some(resume_value.clone())
                } else {
                    None
                }
            })
            .ok_or_else(|| format!("Missing resume value for tool call {}", call.tool_call_id))?;

        outcomes.push(raw_resume_value_to_outcome(call, RichHandlerResult::Json(raw)));
    }

    Ok(outcomes)
}

fn tool_call_display_payload(tool_name: &str, arguments: &JsonValue) -> JsonValue {
    match tool_name {
        "ls_tool" => merge_tool_payload("virtual_fs_ls_request", arguments),
        "cat_tool" => merge_tool_payload("virtual_fs_cat_request", arguments),
        "create_tool" => merge_tool_payload("virtual_fs_create_request", arguments),
        "tree_tool" => merge_tool_payload("virtual_fs_tree_request", arguments),
        "read_path_tool" => merge_tool_payload("virtual_fs_cat_request", arguments),
        "add_things_tool" => merge_tool_payload("things_thing_added", arguments),
        "edit_path_tool" => merge_tool_payload("virtual_fs_edit_request", arguments),
        "delete_path_tool" => merge_tool_payload("virtual_fs_delete_request", arguments),
        "move_path_tool" => merge_tool_payload("virtual_fs_move_request", arguments),
        "create_trigger" | "create_trigger_simple" => {
            let mut payload = merge_tool_payload("trigger_rule_published", arguments);
            payload["version"] = json!(if arguments.get("trigger_uuid").is_some() { 2 } else { 1 });
            payload
        }
        "delete_trigger" => json!({
            "type": "external_tool_call",
            "tool_name": "delete_trigger",
            "arguments": arguments,
            "message": "delete_trigger currently needs bind_uuid and bind_type for legacy auto-handlers",
        }),
        "test_trigger" => json!({
            "type": "trigger_test_request",
            "trigger_json": arguments.get("trigger").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
            "start_iso": JsonValue::Null,
            "end_iso": JsonValue::Null,
            "manual": false,
        }),
        "retrieve_events" => json!({
            "type": "events_retrieve_request",
            "start_time": arguments.get("start_time").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
            "end_time": arguments.get("end_time").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
        }),
        "abstract_events" => json!({
            "type": "events_abstract_request",
            "top_n": arguments.get("top_n").cloned().unwrap_or_else(|| json!(3)),
        }),
        "resolve_uri" => json!({
            "type": "resolve_uri",
            "uri": arguments.get("uri").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
        }),
        _ => json!({
            "type": "external_tool_call",
            "tool_name": tool_name,
            "arguments": arguments,
        }),
    }
}

fn merge_tool_payload(interrupt_type: &str, arguments: &JsonValue) -> JsonValue {
    let mut payload = serde_json::Map::new();
    payload.insert("type".to_string(), JsonValue::String(interrupt_type.to_string()));

    if let Some(object) = arguments.as_object() {
        let mut object = object.clone();
        if interrupt_type == "virtual_fs_create_request" {
            if !object.contains_key("type_name") {
                if let Some(kind) = object.remove("type") {
                    object.insert("type_name".to_string(), kind);
                }
            } else {
                object.remove("type");
            }
        }
        payload.extend(object);
    } else {
        payload.insert("arguments".to_string(), arguments.clone());
    }

    JsonValue::Object(payload)
}

fn raw_resume_value_to_outcome(
    call: &PendingToolCall,
    raw: RichHandlerResult,
) -> ToolExecutionOutcome {
    match raw {
        RichHandlerResult::Image(img) => ToolExecutionOutcome {
            tool_call_id: call.tool_call_id.clone(),
            tool_name: call.tool_name.clone(),
            result: None,
            result_parts: Some(vec![img]),
            error: None,
        },
        RichHandlerResult::Json(json_val) => {
            let error = json_val
                .get("error")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string())
                .or_else(|| json_val.get("error").map(|value| value.to_string()));

            match error {
                Some(error) => ToolExecutionOutcome {
                    tool_call_id: call.tool_call_id.clone(),
                    tool_name: call.tool_name.clone(),
                    result: None,
                    result_parts: None,
                    error: Some(error),
                },
                None => ToolExecutionOutcome {
                    tool_call_id: call.tool_call_id.clone(),
                    tool_name: call.tool_name.clone(),
                    result: Some(match json_val {
                        JsonValue::String(text) => text,
                        other => serde_json::to_string(&other).unwrap_or_default(),
                    }),
                    result_parts: None,
                    error: None,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::{Value as JsonValue, json};

    use super::{
        ExternalToolCallRequest, ExternalToolExecutor, manual_resume_outcomes,
        tool_call_display_payload,
    };
    use crate::chat_types::{PendingToolCall, PendingToolExecutionState};
    use crate::interrupt_handler::InterruptHandler;

    struct EchoHandler;

    #[async_trait]
    impl InterruptHandler for EchoHandler {
        async fn handle(
            &self,
            _interrupt_id: &str,
            payload: &JsonValue,
        ) -> Result<JsonValue, String> {
            Ok(payload.clone())
        }
    }

    #[tokio::test]
    async fn resolve_calls_auto_resumes_registered_tools() {
        let mut executor = ExternalToolExecutor::new();
        executor.register("resolve_uri", EchoHandler);

        let plan = executor.resolve_calls([ExternalToolCallRequest {
            tool_call_id: "resolve_uri:0".to_string(),
            tool_name: "resolve_uri".to_string(),
            arguments: json!({ "uri": "https://example.com" }),
        }]).await;

        assert!(plan.pending_calls.is_empty());
        assert_eq!(plan.resolved_results.len(), 1);
        assert_eq!(
            plan.resolved_results[0].result.as_deref(),
            Some("{\"type\":\"resolve_uri\",\"uri\":\"https://example.com\"}")
        );
    }

    #[test]
    fn manual_resume_outcomes_support_single_pending_call() {
        let pending = PendingToolExecutionState {
            state: json!({ "turn": 1 }),
            completed_results: Vec::new(),
            resolved_results: Vec::new(),
            pending_calls: vec![PendingToolCall {
                tool_call_id: "tool:0".to_string(),
                tool_name: "resolve_uri".to_string(),
                arguments: json!({ "uri": "https://example.com" }),
                display_data: json!({ "type": "resolve_uri" }),
            }],
        };

        let outcomes = manual_resume_outcomes(&pending, json!({ "title": "Example" }))
            .expect("single pending call should accept direct value");

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].result.as_deref(), Some("{\"title\":\"Example\"}"));
    }

    #[test]
    fn read_path_tool_payload_is_aliased_to_cat_handler_shape() {
        let payload = tool_call_display_payload("read_path_tool", &json!({ "path": "/collection/c1/things/t1/content.md" }));

        assert_eq!(payload.get("type").and_then(JsonValue::as_str), Some("virtual_fs_cat_request"));
        assert_eq!(payload.get("path").and_then(JsonValue::as_str), Some("/collection/c1/things/t1/content.md"));
        assert!(payload.get("arguments").is_none());
    }

    #[test]
    fn cat_tool_payload_matches_registered_handler_shape() {
        let payload = tool_call_display_payload("cat_tool", &json!({ "path": "/collection/c1/things/t1/entries.1" }));

        assert_eq!(payload.get("type").and_then(JsonValue::as_str), Some("virtual_fs_cat_request"));
        assert_eq!(payload.get("path").and_then(JsonValue::as_str), Some("/collection/c1/things/t1/entries.1"));
        assert!(payload.get("arguments").is_none());
    }

    #[test]
    fn create_tool_payload_matches_registered_handler_shape() {
        let payload = tool_call_display_payload(
            "create_tool",
            &json!({ "parent_path": "/collection/c1/things", "type_name": "thing" }),
        );

        assert_eq!(payload.get("type").and_then(JsonValue::as_str), Some("virtual_fs_create_request"));
        assert_eq!(payload.get("parent_path").and_then(JsonValue::as_str), Some("/collection/c1/things"));
        assert_eq!(payload.get("type_name").and_then(JsonValue::as_str), Some("thing"));
    }

    #[test]
    fn create_tool_payload_drops_legacy_type_when_type_name_is_present() {
        let payload = tool_call_display_payload(
            "create_tool",
            &json!({
                "parent_path": "/trigger",
                "type_name": "trigger",
                "type": "trigger"
            }),
        );

        assert_eq!(payload.get("type").and_then(JsonValue::as_str), Some("virtual_fs_create_request"));
        assert_eq!(payload.get("type_name").and_then(JsonValue::as_str), Some("trigger"));
        assert!(payload.get("type").and_then(JsonValue::as_str) != Some("trigger"));
    }

    #[test]
    fn create_tool_payload_renames_legacy_type_argument() {
        let payload = tool_call_display_payload(
            "create_tool",
            &json!({ "parent_path": "/collection/c1/things", "type": "thing" }),
        );

        assert_eq!(payload.get("type").and_then(JsonValue::as_str), Some("virtual_fs_create_request"));
        assert_eq!(payload.get("type_name").and_then(JsonValue::as_str), Some("thing"));
    }

    #[tokio::test]
    async fn resolve_calls_auto_resumes_read_path_via_cat_handler() {
        let mut executor = ExternalToolExecutor::new();
        executor.register("virtual_fs_cat_request", EchoHandler);

        let plan = executor.resolve_calls([ExternalToolCallRequest {
            tool_call_id: "read_path_tool:0".to_string(),
            tool_name: "read_path_tool".to_string(),
            arguments: json!({ "path": "/trigger/t1/rule.json" }),
        }]).await;

        assert!(plan.pending_calls.is_empty());
        assert_eq!(plan.resolved_results.len(), 1);
        assert_eq!(
            plan.resolved_results[0].result.as_deref(),
            Some("{\"path\":\"/trigger/t1/rule.json\",\"type\":\"virtual_fs_cat_request\"}")
        );
    }

    #[tokio::test]
    async fn resolve_calls_auto_resumes_ls_when_handler_registered() {
        let mut executor = ExternalToolExecutor::new();
        executor.register("virtual_fs_ls_request", EchoHandler);

        let plan = executor.resolve_calls([ExternalToolCallRequest {
            tool_call_id: "ls_tool:0".to_string(),
            tool_name: "ls_tool".to_string(),
            arguments: json!({ "path": "/collection/c1" }),
        }]).await;

        assert!(plan.pending_calls.is_empty());
        assert_eq!(plan.resolved_results.len(), 1);
        assert_eq!(
            plan.resolved_results[0].result.as_deref(),
            Some("{\"path\":\"/collection/c1\",\"type\":\"virtual_fs_ls_request\"}")
        );
    }
}