//! Chat runtime with actor-based state management.
//!
//! The runtime owns chat execution, survives UI lifecycle changes,
//! handles interrupts automatically, and supports manual resume.

use crate::TriggerSdk;
use crate::chat_client::proto as chat_proto;
use crate::chat_client::{ChatClient, ChatStreamEvent, chat_stream_event};
use crate::chat_types::*;
use crate::interrupt_handler::InterruptHandlerRegistry;
use crate::local_wasm::{self, ChatEventStream};
use crate::remi_uri::{RemiUri, mime_from_extension};
use base64::Engine as _;
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_stream::StreamExt;

fn extract_fatal_error_from_event(event: &ChatStreamEvent) -> Option<String> {
    match event.event.as_ref()? {
        chat_stream_event::Event::Error(err) => Some(if err.message.trim().is_empty() {
            "Agent error".to_string()
        } else {
            err.message.clone()
        }),
        _ => None,
    }
}

#[derive(Clone, Default)]
enum ChatExecutionBackend {
    #[default]
    RemoteSharedTransport,
    LocalWasm(Arc<remi_agentloop_wasm::WasmAgentWithHttp>),
}

// ═══════════════════════════════════════════════════════════════════════════════
// Session State
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Default)]
struct SessionState {
    messages: Vec<CachedMessage>,
    index: HashMap<String, usize>,
    version: u64,
    protocol_state: ChatProtocolSessionState,
    protocol_loaded: bool,
    /// Accumulated content by message ID (for streaming)
    assistant_content: HashMap<String, String>,
    /// Accumulated thinking by message ID
    assistant_thinking: HashMap<String, String>,

    /// Accumulated tool call args by tool_call_id (chunked, indexed)
    tool_call_args: HashMap<String, BTreeMap<i32, String>>,
    /// Best-known tool name by tool_call_id
    tool_call_name: HashMap<String, String>,
    /// Accumulated tool result content by tool_call_id
    tool_result_content: HashMap<String, String>,
}

impl SessionState {
    fn upsert(&mut self, mut msg: CachedMessage) {
        msg.refresh_ui_elements();
        let id = msg.id.clone();
        if let Some(&idx) = self.index.get(&id) {
            self.messages[idx] = msg;
        } else {
            let idx = self.messages.len();
            self.messages.push(msg);
            self.index.insert(id, idx);
        }
        self.version += 1;
    }

    fn get_messages(&self) -> Vec<CachedMessage> {
        self.messages.clone()
    }
}

#[derive(Default)]
struct ProtocolTurnState {
    pending_user: Option<ProtocolHistoryMessage>,
    assistant_content: String,
    assistant_reasoning: Option<String>,
    tool_calls: Vec<ProtocolToolCallDraft>,
    tool_messages: Vec<ProtocolHistoryMessage>,
    committed: bool,
}

#[derive(Clone)]
struct ProtocolToolCallDraft {
    id: String,
    tool_name: String,
    arguments_json: String,
}

impl ProtocolTurnState {
    fn for_user(message: ProtocolHistoryMessage) -> Self {
        Self {
            pending_user: Some(message),
            ..Self::default()
        }
    }

    fn record_delta(&mut self, delta: &str) {
        self.assistant_content.push_str(delta);
    }

    fn record_reasoning(&mut self, thinking: &str) {
        self.assistant_reasoning = Some(thinking.to_string());
    }

    fn record_tool_call_start(&mut self, id: &str, tool_name: &str) {
        if self.tool_calls.iter().all(|call| call.id != id) {
            self.tool_calls.push(ProtocolToolCallDraft {
                id: id.to_string(),
                tool_name: tool_name.to_string(),
                arguments_json: String::new(),
            });
        }
    }

    fn record_tool_call_delta(&mut self, id: &str, delta: &str) {
        if let Some(call) = self.tool_calls.iter_mut().find(|call| call.id == id) {
            call.arguments_json.push_str(delta);
            return;
        }

        self.tool_calls.push(ProtocolToolCallDraft {
            id: id.to_string(),
            tool_name: "unknown".to_string(),
            arguments_json: delta.to_string(),
        });
    }

    fn assistant_message(&self, assistant_id: &str) -> Option<ProtocolHistoryMessage> {
        let tool_calls = self
            .tool_calls
            .iter()
            .map(|call| ProtocolToolCall {
                id: call.id.clone(),
                tool_name: call.tool_name.clone(),
                arguments: parse_tool_arguments_json(&call.arguments_json),
            })
            .collect::<Vec<_>>();

        if self.assistant_content.trim().is_empty()
            && tool_calls.is_empty()
            && self.assistant_reasoning.is_none()
        {
            return None;
        }

        Some(ProtocolHistoryMessage {
            id: assistant_id.to_string(),
            role: "assistant".to_string(),
            content: JsonValue::String(self.assistant_content.clone()),
            tool_calls,
            tool_call_id: None,
            reasoning_content: self.assistant_reasoning.clone(),
        })
    }

    fn record_tool_result_message(&mut self, outcome: &ToolExecutionOutcome) {
        if self
            .tool_messages
            .iter()
            .any(|message| message.tool_call_id.as_deref() == Some(outcome.tool_call_id.as_str()))
        {
            return;
        }

        self.tool_messages
            .push(outcome_to_protocol_tool_message(outcome));
    }

    fn commit_completed(&mut self, session: &mut SessionState, assistant_id: &str) {
        if self.committed {
            return;
        }

        if let Some(user) = self.pending_user.take() {
            session.protocol_state.history.push(user);
        }
        if let Some(assistant) = self.assistant_message(assistant_id) {
            session.protocol_state.history.push(assistant);
        }
        session
            .protocol_state
            .history
            .extend(self.tool_messages.drain(..));
        session.protocol_state.pending_tool_execution = None;
        self.committed = true;
    }

    fn commit_need_tool_execution(
        &mut self,
        session: &mut SessionState,
        assistant_id: &str,
        pending: PendingToolExecutionState,
    ) {
        if self.committed {
            session.protocol_state.pending_tool_execution = Some(pending);
            return;
        }

        if let Some(user) = self.pending_user.take() {
            session.protocol_state.history.push(user);
        }
        if let Some(assistant) = self.assistant_message(assistant_id) {
            session.protocol_state.history.push(assistant);
        }
        session
            .protocol_state
            .history
            .extend(self.tool_messages.drain(..));
        session.protocol_state.pending_tool_execution = Some(pending);
        self.committed = true;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Actor Commands
// ═══════════════════════════════════════════════════════════════════════════════

enum Command {
    /// Initialize/reconfigure the runtime
    Init {
        access_token: String,
        config: ChatRuntimeConfig,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// Update access token
    UpdateToken {
        access_token: String,
        reply: oneshot::Sender<()>,
    },

    /// Send a message and start streaming
    SendMessage {
        session_id: String,
        message: String,
        system_prompt: Option<String>,
        references: Option<JsonValue>,
        attachments: Option<JsonValue>,
        user_msg_id: Option<String>,
        assistant_msg_id: Option<String>,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// Resume after WaitingForUser
    Resume {
        session_id: String,
        resume_value: JsonValue,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// Cancel current run
    Cancel { reply: oneshot::Sender<()> },

    /// Get current status
    GetStatus {
        reply: oneshot::Sender<ChatRunStatus>,
    },

    /// Get messages for a session
    GetMessages {
        session_id: String,
        reply: oneshot::Sender<Vec<CachedMessage>>,
    },

    /// Get cache version for a session
    GetCacheVersion {
        session_id: String,
        reply: oneshot::Sender<u64>,
    },

    /// Initialize cache from existing messages
    InitCache {
        session_id: String,
        messages: Vec<CachedMessage>,
        reply: oneshot::Sender<()>,
    },

    /// Clear session cache
    ClearCache {
        session_id: String,
        reply: oneshot::Sender<()>,
    },

    /// Register interrupt handler registry
    SetHandlerRegistry {
        registry: InterruptHandlerRegistry,
        reply: oneshot::Sender<()>,
    },

    /// Subscribe to events
    Subscribe {
        tx: mpsc::Sender<ChatRuntimeEvent>,
        reply: oneshot::Sender<()>,
    },
}

// ═══════════════════════════════════════════════════════════════════════════════
// Actor State
// ═══════════════════════════════════════════════════════════════════════════════

struct ActorState {
    // Config
    access_token: Option<String>,
    config: ChatRuntimeConfig,
    backend: ChatExecutionBackend,

    // Run state
    state: ChatRunState,
    active_session_id: Option<String>,
    last_error: Option<String>,
    pending_interrupt: Option<PendingInterrupt>,

    // Sessions
    sessions: HashMap<String, SessionState>,

    // Interrupt handling
    handler_registry: InterruptHandlerRegistry,

    // Event subscribers
    subscribers: Vec<mpsc::Sender<ChatRuntimeEvent>>,

    // Cancel channel for current stream
    cancel_tx: Option<mpsc::Sender<()>>,

    // SDK reference for DB persistence
    sdk: Option<Arc<TriggerSdk>>,
}

impl ActorState {
    fn new() -> Self {
        Self {
            access_token: None,
            config: ChatRuntimeConfig::default(),
            backend: ChatExecutionBackend::default(),
            state: ChatRunState::Idle,
            active_session_id: None,
            last_error: None,
            pending_interrupt: None,
            sessions: HashMap::new(),
            handler_registry: InterruptHandlerRegistry::new(),
            subscribers: Vec::new(),
            cancel_tx: None,
            sdk: None,
        }
    }

    fn get_or_create_session(&mut self, session_id: &str) -> &mut SessionState {
        self.sessions.entry(session_id.to_string()).or_default()
    }

    fn emit_event(&mut self, event: ChatRuntimeEvent) {
        self.subscribers
            .retain(|tx| tx.try_send(event.clone()).is_ok());
    }

    fn set_state(&mut self, new_state: ChatRunState) {
        self.state = new_state;
        self.emit_event(ChatRuntimeEvent::StatusChanged {
            state: self.state,
            session_id: self.active_session_id.clone(),
            error: self.last_error.clone(),
        });
    }

    fn status(&self) -> ChatRunStatus {
        ChatRunStatus {
            state: self.state,
            session_id: self.active_session_id.clone(),
            error_message: self.last_error.clone(),
            pending_interrupt: self.pending_interrupt.clone(),
        }
    }
}

fn build_execution_backend(config: &ChatRuntimeConfig) -> Result<ChatExecutionBackend, String> {
    match &config.backend {
        ChatRuntimeBackend::RemoteServer => Ok(ChatExecutionBackend::RemoteSharedTransport),
        ChatRuntimeBackend::LocalWasm(local) => {
            local_wasm::load_agent(&local.source).map(ChatExecutionBackend::LocalWasm)
        }
    }
}

async fn open_chat_stream(
    backend: &ChatExecutionBackend,
    access_token: &str,
    config: &ChatRuntimeConfig,
    session_id: String,
    start_input: Option<chat_proto::ChatStartInput>,
    resume_input: Option<chat_proto::ChatResumeInput>,
) -> Result<ChatEventStream, String> {
    match backend {
        ChatExecutionBackend::RemoteSharedTransport => {
            let current_access_token = crate::auth::auth_get_access_token()
                .await
                .unwrap_or_else(|| access_token.to_string());
            tracing::info!(session_id = %session_id, "[ChatRuntime] creating ChatClient via shared transport");
            let mut client = ChatClient::new_with_shared_transport(current_access_token)
                .await
                .map_err(|e| {
                    tracing::error!(session_id = %session_id, error = %e, "[ChatRuntime] failed to create ChatClient");
                    format!("Failed to create chat client: {}", e)
                })?;
            tracing::info!(session_id = %session_id, "[ChatRuntime] ChatClient created, starting remote stream");

            let stream = client
                .chat_streaming_multimodal(session_id, start_input, resume_input)
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, "[ChatRuntime] failed to start remote stream");
                    format!("Failed to start stream: {}", e)
                })?
                .map(|item| item.map_err(|error| error.to_string()));

            let stream: ChatEventStream = Box::pin(stream);
            Ok(stream)
        }
        ChatExecutionBackend::LocalWasm(agent) => {
            tracing::info!(session_id = %session_id, "[ChatRuntime] starting local WASM chat stream");
            local_wasm::start_stream(agent.clone(), config, &session_id, start_input, resume_input)
                .await
        }
    }
}

async fn ensure_protocol_state_loaded(
    state: &Arc<RwLock<ActorState>>,
    session_id: &str,
) -> Result<(), String> {
    let (needs_load, sdk) = {
        let s = state.read().await;
        let needs_load = s
            .sessions
            .get(session_id)
            .map(|session| !session.protocol_loaded)
            .unwrap_or(true);
        (needs_load, s.sdk.clone())
    };

    if !needs_load {
        return Ok(());
    }

    let stored = match sdk {
        Some(sdk) => sdk
            .get_chat_runtime_state_json(session_id)
            .map_err(|e| format!("Failed to load chat runtime state: {}", e))?,
        None => None,
    };

    let loaded_state = match stored {
        Some(json) => serde_json::from_str::<ChatProtocolSessionState>(&json)
            .map_err(|e| format!("Failed to parse chat runtime state: {}", e))?,
        None => ChatProtocolSessionState::default(),
    };

    let mut s = state.write().await;
    let session = s.get_or_create_session(session_id);
    if !session.protocol_loaded {
        session.protocol_state = loaded_state;
        session.protocol_loaded = true;
    }

    Ok(())
}

fn content_to_proto_parts(content: &JsonValue) -> (String, Vec<chat_proto::ChatContentPart>) {
    match content {
        JsonValue::String(text) => (text.clone(), vec![]),
        JsonValue::Array(parts) => {
            let mut text_content = String::new();
            let mut proto_parts = Vec::new();

            for part in parts {
                let Some(part_type) = part.get("type").and_then(|value| value.as_str()) else {
                    continue;
                };

                match part_type {
                    "text" => {
                        let text = part
                            .get("text")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string();
                        text_content.push_str(&text);
                        proto_parts.push(chat_proto::ChatContentPart {
                            r#type: "text".to_string(),
                            value: Some(chat_proto::chat_content_part::Value::Text(text)),
                        });
                    }
                    "image_url" => {
                        let image = part.get("image_url").cloned().unwrap_or(JsonValue::Null);
                        let url = image
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string();
                        let detail = image
                            .get("detail")
                            .and_then(|value| value.as_str())
                            .unwrap_or("auto")
                            .to_string();
                        proto_parts.push(chat_proto::ChatContentPart {
                            r#type: "image_url".to_string(),
                            value: Some(chat_proto::chat_content_part::Value::ImageUrl(
                                chat_proto::ChatImageContent { url, detail, data: Vec::new(), media_type: String::new() },
                            )),
                        });
                    }
                    "resource_uri" => {
                        let uri = part
                            .get("uri")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string();
                        let resource_type = part
                            .get("resource_type")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string();
                        proto_parts.push(chat_proto::ChatContentPart {
                            r#type: "resource_uri".to_string(),
                            value: Some(chat_proto::chat_content_part::Value::ResourceUri(
                                chat_proto::ChatResourceContent { uri, resource_type },
                            )),
                        });
                    }
                    _ => {}
                }
            }

            (text_content, proto_parts)
        }
        _ => (String::new(), vec![]),
    }
}

fn protocol_history_to_proto(message: &ProtocolHistoryMessage) -> chat_proto::ChatHistoryMessage {
    let (content, content_parts) = content_to_proto_parts(&message.content);

    chat_proto::ChatHistoryMessage {
        id: message.id.clone(),
        role: message.role.clone(),
        content,
        content_parts,
        tool_calls: message
            .tool_calls
            .iter()
            .map(|tool_call| chat_proto::ChatToolCall {
                id: tool_call.id.clone(),
                tool_name: tool_call.tool_name.clone(),
                arguments: match &tool_call.arguments {
                    JsonValue::Object(_) => Some(json_value_to_prost_struct(tool_call.arguments.clone())),
                    _ => None,
                },
            })
            .collect(),
        tool_call_id: message.tool_call_id.clone().unwrap_or_default(),
        reasoning_content: message.reasoning_content.clone().unwrap_or_default(),
    }
}

fn tool_outcome_to_proto(outcome: &ToolExecutionOutcome) -> Option<chat_proto::ChatToolCallOutcome> {
    let outcome_value = if let Some(parts) = &outcome.result_parts {
        // Multimodal: encode each image part into proto ChatContentPart
        let proto_parts = parts
            .iter()
            .map(|img| chat_proto::ChatContentPart {
                r#type: "image".to_string(),
                value: Some(chat_proto::chat_content_part::Value::ImageUrl(
                    chat_proto::ChatImageContent {
                        url: String::new(),
                        detail: String::new(),
                        data: img.data.clone(),
                        media_type: img.media_type.clone(),
                    },
                )),
            })
            .collect();
        Some(chat_proto::chat_tool_call_outcome::Outcome::Parts(
            chat_proto::ChatToolCallResultParts { parts: proto_parts },
        ))
    } else if let Some(result) = &outcome.result {
        Some(chat_proto::chat_tool_call_outcome::Outcome::Result(result.clone()))
    } else {
        outcome
            .error
            .as_ref()
            .map(|error| chat_proto::chat_tool_call_outcome::Outcome::Error(error.clone()))
    }?;

    Some(chat_proto::ChatToolCallOutcome {
        tool_call_id: outcome.tool_call_id.clone(),
        tool_name: outcome.tool_name.clone(),
        outcome: Some(outcome_value),
    })
}

fn proto_tool_outcome_to_runtime(outcome: &chat_proto::ChatToolCallOutcome) -> Option<ToolExecutionOutcome> {
    let outcome_value = outcome.outcome.as_ref()?;
    match outcome_value {
        chat_proto::chat_tool_call_outcome::Outcome::Result(result) => Some(ToolExecutionOutcome {
            tool_call_id: outcome.tool_call_id.clone(),
            tool_name: outcome.tool_name.clone(),
            result: Some(result.clone()),
            result_parts: None,
            error: None,
        }),
        chat_proto::chat_tool_call_outcome::Outcome::Parts(parts_msg) => {
            let images = parts_msg
                .parts
                .iter()
                .filter_map(|part| match &part.value {
                    Some(chat_proto::chat_content_part::Value::ImageUrl(img))
                        if !img.data.is_empty() =>
                    {
                        Some(crate::chat_types::ToolImagePart {
                            media_type: img.media_type.clone(),
                            data: img.data.clone(),
                        })
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            Some(ToolExecutionOutcome {
                tool_call_id: outcome.tool_call_id.clone(),
                tool_name: outcome.tool_name.clone(),
                result: None,
                result_parts: if images.is_empty() { None } else { Some(images) },
                error: None,
            })
        }
        chat_proto::chat_tool_call_outcome::Outcome::Error(error) => Some(ToolExecutionOutcome {
            tool_call_id: outcome.tool_call_id.clone(),
            tool_name: outcome.tool_name.clone(),
            result: None,
            result_parts: None,
            error: Some(error.clone()),
        }),
    }
}

fn parse_tool_arguments_json(arguments_json: &str) -> JsonValue {
    if arguments_json.trim().is_empty() {
        return json!({});
    }

    serde_json::from_str(arguments_json)
        .unwrap_or_else(|_| JsonValue::String(arguments_json.to_string()))
}

fn tool_call_display_payload(tool_name: &str, arguments: &JsonValue) -> JsonValue {
    match tool_name {
        "list_things_tool" => json!({
            "type": "things_list_snapshot_request",
            "entity_type": arguments.get("entity_type").cloned().unwrap_or(JsonValue::Null),
            "include_content": arguments.get("include_content").cloned().unwrap_or_else(|| JsonValue::Bool(false)),
        }),
        "get_things_tool" => json!({
            "type": "things_get_thing_markdown_request",
            "uuid": arguments.get("uuid").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
        }),
        "add_things_tool" => json!({
            "type": "things_thing_added",
            "thing": {
                "uuid": arguments.get("uuid").cloned().unwrap_or_else(|| JsonValue::String(uuid::Uuid::new_v4().to_string())),
                "title": arguments.get("title").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
                "datatype": "markdown",
                "data_json": if let Some(content) = arguments.get("content").and_then(|v| v.as_str()) {
                    if content.trim().is_empty() { JsonValue::String("{}".to_string()) } else { JsonValue::String(json!({"markdown": content}).to_string()) }
                } else {
                    JsonValue::String("{}".to_string())
                },
                "parent_uuid": arguments.get("parent_uuid").cloned().unwrap_or(JsonValue::Null),
                "collection_uuid": arguments.get("collection_uuid").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
                "created_at": "",
                "updated_at": "",
            }
        }),
        "edit_things_tool" => {
            let edit = arguments.get("edit").cloned().unwrap_or_else(|| json!({}));
            json!({
                "type": "things_thing_content_edit",
                "uuid": arguments.get("uuid").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
                "operation": edit.get("operation").cloned().unwrap_or_else(|| JsonValue::String("replace_all".to_string())),
                "new_title": edit.get("new_title").cloned().unwrap_or(JsonValue::Null),
                "new_content": edit.get("new_content").cloned().unwrap_or(JsonValue::Null),
                "old_str": edit.get("old_str").cloned().unwrap_or(JsonValue::Null),
                "new_str": edit.get("new_str").cloned().unwrap_or(JsonValue::Null),
                "line_number": edit.get("line_number").cloned().unwrap_or(JsonValue::Null),
                "insert_text": edit.get("insert_text").cloned().unwrap_or(JsonValue::Null),
                "append_text": edit.get("append_text").cloned().unwrap_or(JsonValue::Null),
            })
        }
        "remove_things_tool" => json!({
            "type": "things_thing_removed",
            "uuid": arguments.get("uuid").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
            "collection_uuid": "",
            "message": "",
        }),
        "move_things_tool" => json!({
            "type": "things_thing_moved",
            "uuid": arguments.get("uuid").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
            "to_collection_uuid": arguments.get("new_collection_uuid").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
            "to_parent_uuid": arguments.get("new_parent_uuid").cloned().unwrap_or(JsonValue::Null),
        }),
        "create_trigger_simple" => json!({
            "type": "trigger_rule_published",
            "trigger_uuid": uuid::Uuid::new_v4().to_string(),
            "name": arguments.get("name").cloned().unwrap_or_else(|| JsonValue::String("trigger".to_string())),
            "rule_config_json": {
                "name": arguments.get("name").cloned().unwrap_or_else(|| JsonValue::String("trigger".to_string())),
                "precondition": [{
                    "rule": format!("cron('{}')", arguments.get("cron").and_then(|v| v.as_str()).unwrap_or_default()),
                    "description": format!("Cron schedule: {}", arguments.get("cron").and_then(|v| v.as_str()).unwrap_or_default()),
                }],
                "condition": arguments
                    .get("condition")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.trim().is_empty())
                    .map(|condition| vec![json!({
                        "rule": condition,
                        "description": "User-specified condition",
                    })])
                    .unwrap_or_default(),
            },
            "user_request": arguments.get("user_request").cloned().unwrap_or(JsonValue::Null),
            "event_analysis": JsonValue::Null,
            "bind_uuid": arguments.get("bind_uuid").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
            "bind_type": arguments.get("bind_type").cloned().unwrap_or_else(|| JsonValue::String("thing".to_string())),
            "version": 1,
        }),
        "create_trigger" => json!({
            "type": "trigger_rule_published",
            "trigger_uuid": arguments.get("trigger_uuid").cloned().unwrap_or_else(|| JsonValue::String(uuid::Uuid::new_v4().to_string())),
            "name": arguments.get("trigger").cloned().unwrap_or_else(|| JsonValue::String("trigger".to_string())),
            "rule_config_json": arguments.get("trigger").cloned().unwrap_or_else(|| JsonValue::String("{}".to_string())),
            "user_request": arguments.get("user_request").cloned().unwrap_or(JsonValue::Null),
            "event_analysis": arguments.get("event_analysis").cloned().unwrap_or(JsonValue::Null),
            "bind_uuid": arguments.get("bind_uuid").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
            "bind_type": arguments.get("bind_type").cloned().unwrap_or_else(|| JsonValue::String("thing".to_string())),
            "version": if arguments.get("trigger_uuid").is_some() { 2 } else { 1 },
        }),
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
        "list_triggers_tool" => json!({
            "type": "triggers_list_request",
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

fn raw_resume_value_to_outcome(call: &PendingToolCall, raw: RichHandlerResult) -> ToolExecutionOutcome {
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

fn build_manual_resume_outcomes(
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

fn outcome_to_protocol_tool_message(outcome: &ToolExecutionOutcome) -> ProtocolHistoryMessage {
    // Build content: for multimodal (image parts) use a JSON array of content parts
    // so that content_to_proto_parts() can later convert them for history replay.
    let content = if let Some(parts) = &outcome.result_parts {
        let parts_json: Vec<JsonValue> = parts
            .iter()
            .map(|img| {
                let b64 = base64::engine::general_purpose::STANDARD.encode(&img.data);
                json!({
                    "type": "image_url",
                    "image_url": {
                        "url": format!("data:{};base64,{}", img.media_type, b64)
                    }
                })
            })
            .collect();
        JsonValue::Array(parts_json)
    } else {
        JsonValue::String(
            outcome
                .result
                .clone()
                .or_else(|| outcome.error.clone())
                .unwrap_or_default(),
        )
    };

    ProtocolHistoryMessage {
        id: format!("tool:{}", outcome.tool_call_id),
        role: "tool".to_string(),
        content,
        tool_calls: Vec::new(),
        tool_call_id: Some(outcome.tool_call_id.clone()),
        reasoning_content: None,
    }
}

fn cached_message_storage_json(session_id: &str, message: &CachedMessage) -> String {
    let mut value = serde_json::to_value(message).unwrap_or_else(|_| json!({}));
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "session_id".to_string(),
            JsonValue::String(session_id.to_string()),
        );
        let created_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(message.timestamp_ms)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();
        obj.insert("created_at".to_string(), JsonValue::String(created_at));
        obj.insert(
            "ui_elements".to_string(),
            serde_json::to_value(&message.ui_elements).unwrap_or(JsonValue::Array(Vec::new())),
        );
    }
    value.to_string()
}

// ═══════════════════════════════════════════════════════════════════════════════
// ChatRuntime Handle
// ═══════════════════════════════════════════════════════════════════════════════

static STATUS_VERSION: AtomicU64 = AtomicU64::new(0);

/// Handle to the chat runtime actor
#[derive(Clone)]
pub struct ChatRuntime {
    cmd_tx: mpsc::Sender<Command>,
}

impl ChatRuntime {
    /// Start the chat runtime actor
    pub fn start(sdk: Arc<TriggerSdk>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        tokio::spawn(run_actor(cmd_rx, sdk));
        Self { cmd_tx }
    }

    /// Initialize the runtime with configuration
    pub async fn init(
        &self,
        access_token: String,
        config: ChatRuntimeConfig,
    ) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Init {
                access_token,
                config,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "Runtime stopped")?;
        reply_rx.await.map_err(|_| "Runtime stopped")?
    }

    /// Update access token
    pub async fn update_token(&self, access_token: String) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::UpdateToken {
                access_token,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "Runtime stopped")?;
        reply_rx.await.map_err(|_| "Runtime stopped")?;
        Ok(())
    }

    /// Send a message and start streaming
    pub async fn send_message(
        &self,
        session_id: String,
        message: String,
        system_prompt: Option<String>,
        references: Option<JsonValue>,
        attachments: Option<JsonValue>,
        user_msg_id: Option<String>,
        assistant_msg_id: Option<String>,
    ) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SendMessage {
                session_id,
                message,
                system_prompt,
                references,
                attachments,
                user_msg_id,
                assistant_msg_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "Runtime stopped")?;
        reply_rx.await.map_err(|_| "Runtime stopped")?
    }

    /// Resume after waiting for user
    pub async fn resume(&self, session_id: String, resume_value: JsonValue) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Resume {
                session_id,
                resume_value,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "Runtime stopped")?;
        reply_rx.await.map_err(|_| "Runtime stopped")?
    }

    /// Cancel current run
    pub async fn cancel(&self) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Cancel { reply: reply_tx })
            .await
            .map_err(|_| "Runtime stopped")?;
        reply_rx.await.map_err(|_| "Runtime stopped")?;
        Ok(())
    }

    /// Get current status
    pub async fn get_status(&self) -> ChatRunStatus {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::GetStatus { reply: reply_tx })
            .await
            .is_err()
        {
            return ChatRunStatus::default();
        }
        reply_rx.await.unwrap_or_default()
    }

    /// Get status version (for polling-based change detection)
    pub fn get_status_version() -> u64 {
        STATUS_VERSION.load(Ordering::SeqCst)
    }

    /// Get cached messages for a session
    pub async fn get_messages(&self, session_id: &str) -> Vec<CachedMessage> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::GetMessages {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return Vec::new();
        }
        reply_rx.await.unwrap_or_default()
    }

    /// Get cache version for a session
    pub async fn get_cache_version(&self, session_id: &str) -> u64 {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::GetCacheVersion {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return 0;
        }
        reply_rx.await.unwrap_or(0)
    }

    /// Initialize cache from existing messages
    pub async fn init_cache(&self, session_id: &str, messages: Vec<CachedMessage>) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::InitCache {
                session_id: session_id.to_string(),
                messages,
                reply: reply_tx,
            })
            .await
            .is_ok()
        {
            let _ = reply_rx.await;
        }
    }

    /// Clear session cache
    pub async fn clear_cache(&self, session_id: &str) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::ClearCache {
                session_id: session_id.to_string(),
                reply: reply_tx,
            })
            .await
            .is_ok()
        {
            let _ = reply_rx.await;
        }
    }

    /// Set interrupt handler registry
    pub async fn set_handler_registry(&self, registry: InterruptHandlerRegistry) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::SetHandlerRegistry {
                registry,
                reply: reply_tx,
            })
            .await
            .is_ok()
        {
            let _ = reply_rx.await;
        }
    }

    /// Subscribe to runtime events
    pub async fn subscribe(&self) -> mpsc::Receiver<ChatRuntimeEvent> {
        let (tx, rx) = mpsc::channel(64);
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::Subscribe {
                tx,
                reply: reply_tx,
            })
            .await
            .is_ok()
        {
            let _ = reply_rx.await;
        }
        rx
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Actor Loop
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_actor(mut cmd_rx: mpsc::Receiver<Command>, sdk: Arc<TriggerSdk>) {
    let state = Arc::new(RwLock::new(ActorState::new()));
    state.write().await.sdk = Some(sdk);

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            Command::Init {
                access_token,
                config,
                reply,
            } => {
                let backend = match build_execution_backend(&config) {
                    Ok(backend) => backend,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        continue;
                    }
                };
                let mut s = state.write().await;
                s.access_token = Some(access_token);
                s.config = config;
                s.backend = backend;
                let _ = reply.send(Ok(()));
            }

            Command::UpdateToken {
                access_token,
                reply,
            } => {
                state.write().await.access_token = Some(access_token);
                let _ = reply.send(());
            }

            Command::SendMessage {
                session_id,
                message,
                system_prompt,
                references,
                attachments,
                user_msg_id,
                assistant_msg_id,
                reply,
            } => {
                let result = handle_send_message(
                    state.clone(),
                    session_id,
                    message,
                    system_prompt,
                    references,
                    attachments,
                    user_msg_id,
                    assistant_msg_id,
                )
                .await;
                let _ = reply.send(result);
            }

            Command::Resume {
                session_id,
                resume_value,
                reply,
            } => {
                let result = handle_resume(state.clone(), session_id, resume_value).await;
                let _ = reply.send(result);
            }

            Command::Cancel { reply } => {
                let mut s = state.write().await;
                if let Some(tx) = s.cancel_tx.take() {
                    let _ = tx.send(()).await;
                }
                s.active_session_id = None;
                s.state = ChatRunState::Idle;
                s.pending_interrupt = None;
                STATUS_VERSION.fetch_add(1, Ordering::SeqCst);
                let _ = reply.send(());
            }

            Command::GetStatus { reply } => {
                let s = state.read().await;
                let _ = reply.send(s.status());
            }

            Command::GetMessages { session_id, reply } => {
                let s = state.read().await;
                let msgs = s
                    .sessions
                    .get(&session_id)
                    .map(|sess| sess.get_messages())
                    .unwrap_or_default();
                let _ = reply.send(msgs);
            }

            Command::GetCacheVersion { session_id, reply } => {
                let s = state.read().await;
                let v = s
                    .sessions
                    .get(&session_id)
                    .map(|sess| sess.version)
                    .unwrap_or(0);
                let _ = reply.send(v);
            }

            Command::InitCache {
                session_id,
                messages,
                reply,
            } => {
                let mut s = state.write().await;
                let sess = s.get_or_create_session(&session_id);
                sess.messages.clear();
                sess.index.clear();
                for mut msg in messages {
                    msg.refresh_ui_elements();
                    let id = msg.id.clone();
                    let idx = sess.messages.len();
                    sess.messages.push(msg);
                    sess.index.insert(id, idx);
                }
                sess.version += 1;
                let _ = reply.send(());
            }

            Command::ClearCache { session_id, reply } => {
                state.write().await.sessions.remove(&session_id);
                let _ = reply.send(());
            }

            Command::SetHandlerRegistry { registry, reply } => {
                state.write().await.handler_registry = registry;
                let _ = reply.send(());
            }

            Command::Subscribe { tx, reply } => {
                state.write().await.subscribers.push(tx);
                let _ = reply.send(());
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Multimodal Message Builder
// ═══════════════════════════════════════════════════════════════════════════════

/// Uploaded image info for building resource_uri content parts.
struct UploadedImage {
    /// remi:// URI pointing to the uploaded image
    remi_uri: String,
}

/// Upload chat image attachments to cloud storage and return remi:// URIs.
///
/// For each image attachment that has a local `path`, reads the file bytes,
/// uploads via the global ProfileClient with scenario="chat", and constructs
/// a `remi://remote/...` URI. Images that already have a `url` starting with
/// `remi://` are passed through as-is.
///
/// Returns an error if any image fails to read or upload — the caller should
/// abort the chat send so users are not silently losing their images.
async fn upload_chat_image_attachments(
    attachments: &Option<JsonValue>,
) -> Result<Vec<UploadedImage>, String> {
    let Some(JsonValue::Array(arr)) = attachments else {
        return Ok(vec![]);
    };

    let mut uploaded = Vec::new();

    for item in arr {
        let type_field = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if type_field != "image" {
            continue;
        }

        // If attachment already has a remi:// URI, use it directly
        if let Some(url) = item.get("url").and_then(|v| v.as_str()) {
            if url.starts_with("remi://") {
                uploaded.push(UploadedImage {
                    remi_uri: url.to_string(),
                });
                continue;
            }
        }

        // Get local file path
        let path = match item.get("path").and_then(|v| v.as_str()) {
            Some(p) if !p.trim().is_empty() => p,
            _ => continue,
        };

        // Read file bytes
        let bytes = match tokio::fs::read(path).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(path = %path, error = %e, "Failed to read image file for chat upload");
                return Err(format!("Failed to read image file: {}", e));
            }
        };

        // Determine extension
        let ext = path.rsplit('.').next().unwrap_or("jpg").to_lowercase();

        // Upload via ProfileClient with scenario="chat"
        match crate::profile::media_upload(bytes, ext.clone(), Some("chat".to_string())).await {
            Ok(public_url) => {
                // Construct remi://remote URI
                let mime = crate::remi_uri::mime_from_extension(&ext);
                let remi = crate::remi_uri::RemiUri::from_remote(&public_url, &mime);
                uploaded.push(UploadedImage {
                    remi_uri: remi.to_uri_string(),
                });
                tracing::info!(
                    path = %path,
                    remi_uri = %remi.to_uri_string(),
                    "Chat image uploaded successfully"
                );
            }
            Err(e) => {
                tracing::error!(path = %path, error = %e, "Failed to upload chat image");
                return Err(format!("Failed to upload image: {}", e));
            }
        }
    }

    Ok(uploaded)
}

async fn prepare_chat_image_attachments(
    config: &ChatRuntimeConfig,
    attachments: &Option<JsonValue>,
) -> Result<Vec<UploadedImage>, String> {
    match &config.backend {
        ChatRuntimeBackend::RemoteServer => upload_chat_image_attachments(attachments).await,
        ChatRuntimeBackend::LocalWasm(_) => prepare_local_chat_image_attachments(&config.device_id, attachments),
    }
}

fn prepare_local_chat_image_attachments(
    device_id: &str,
    attachments: &Option<JsonValue>,
) -> Result<Vec<UploadedImage>, String> {
    let Some(JsonValue::Array(arr)) = attachments else {
        return Ok(vec![]);
    };

    let mut images = Vec::new();

    for item in arr {
        let type_field = item.get("type").and_then(|value| value.as_str()).unwrap_or("");
        if type_field != "image" {
            continue;
        }

        if let Some(url) = item.get("url").and_then(|value| value.as_str()) {
            if !url.trim().is_empty() {
                images.push(UploadedImage {
                    remi_uri: url.to_string(),
                });
                continue;
            }
        }

        let Some(path) = item.get("path").and_then(|value| value.as_str()) else {
            continue;
        };
        if path.trim().is_empty() {
            continue;
        }

        let mime = item
            .get("mime")
            .or_else(|| item.get("mime_type"))
            .or_else(|| item.get("contentType"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .unwrap_or_else(|| {
                let ext = path.rsplit('.').next().unwrap_or("jpg");
                mime_from_extension(ext).to_string()
            });

        let remi = RemiUri::from_local_file(path, &mime, device_id);
        images.push(UploadedImage {
            remi_uri: remi.to_uri_string(),
        });
    }

    Ok(images)
}

/// Build the current user input from text + uploaded image URIs.
///
/// When `image_uris` is non-empty, the message is constructed with
/// `content_parts` containing text and `resource_uri` parts. The server will
/// resolve these URIs before forwarding to the model.
fn build_current_input_message(message: &str, image_uris: &[UploadedImage]) -> chat_proto::ChatInputMessage {
    if image_uris.is_empty() {
        return chat_proto::ChatInputMessage {
            role: "user".to_string(),
            content: message.to_string(),
            content_parts: vec![],
        };
    }

    let mut parts = Vec::new();

    if !message.trim().is_empty() {
        parts.push(chat_proto::ChatContentPart {
            r#type: "text".to_string(),
            value: Some(chat_proto::chat_content_part::Value::Text(
                message.to_string(),
            )),
        });
    }

    for img in image_uris {
        parts.push(chat_proto::ChatContentPart {
            r#type: "resource_uri".to_string(),
            value: Some(chat_proto::chat_content_part::Value::ResourceUri(
                chat_proto::ChatResourceContent {
                    uri: img.remi_uri.clone(),
                    resource_type: "image".to_string(),
                },
            )),
        });
    }

    chat_proto::ChatInputMessage {
        role: "user".to_string(),
        content: message.to_string(),
        content_parts: parts,
    }
}

fn input_message_to_protocol_history(
    id: String,
    input: &chat_proto::ChatInputMessage,
) -> ProtocolHistoryMessage {
    let content = if input.content_parts.is_empty() {
        JsonValue::String(input.content.clone())
    } else {
        JsonValue::Array(
            input.content_parts
                .iter()
                .map(|part| match part.value.as_ref() {
                    Some(chat_proto::chat_content_part::Value::Text(text)) => json!({
                        "type": "text",
                        "text": text,
                    }),
                    Some(chat_proto::chat_content_part::Value::ImageUrl(image)) => json!({
                        "type": "image_url",
                        "image_url": {
                            "url": image.url,
                            "detail": image.detail,
                        },
                    }),
                    Some(chat_proto::chat_content_part::Value::ResourceUri(resource)) => json!({
                        "type": "resource_uri",
                        "uri": resource.uri,
                        "resource_type": resource.resource_type,
                    }),
                    None => json!({
                        "type": part.r#type,
                    }),
                })
                .collect(),
        )
    };

    ProtocolHistoryMessage {
        id,
        role: if input.role.trim().is_empty() {
            "user".to_string()
        } else {
            input.role.clone()
        },
        content,
        tool_calls: Vec::new(),
        tool_call_id: None,
        reasoning_content: None,
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Send Message Handler
// ═══════════════════════════════════════════════════════════════════════════════

async fn handle_send_message(
    state: Arc<RwLock<ActorState>>,
    session_id: String,
    message: String,
    system_prompt: Option<String>,
    references: Option<JsonValue>,
    attachments: Option<JsonValue>,
    user_msg_id: Option<String>,
    assistant_msg_id: Option<String>,
) -> Result<(), String> {
    tracing::info!(
        session_id = %session_id,
        msg_len = message.len(),
        "[ChatRuntime] handle_send_message enter"
    );
    // Get config
    let (access_token, config, backend, sdk) = {
        let s = state.read().await;
        (
            s.access_token.clone().ok_or("Not initialized")?,
            s.config.clone(),
            s.backend.clone(),
            s.sdk.clone(),
        )
    };

    ensure_protocol_state_loaded(&state, &session_id).await?;

    // Cancel any existing run
    {
        let mut s = state.write().await;
        if let Some(tx) = s.cancel_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    // Generate message IDs
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let user_id = user_msg_id.unwrap_or_else(|| format!("user_{}", now_ms));
    let assistant_id = assistant_msg_id.unwrap_or_else(|| format!("assistant_{}", now_ms));

    // Insert user message into cache.
    // NOTE: We intentionally do NOT pre-insert an assistant placeholder here.
    // The model may emit multiple assistant messages (different message IDs),
    // and pre-creating a single placeholder causes UI to look like it only
    // ever updates one bubble.
    let cache_version = {
        let mut s = state.write().await;
        let version = {
            let sess = s.get_or_create_session(&session_id);
            let mut m = CachedMessage::user(user_id.clone(), message.clone(), now_ms);
            m.references = references.clone();
            m.attachments = attachments.clone();
            sess.upsert(m);
            sess.version
        };
        s.active_session_id = Some(session_id.clone());
        s.state = ChatRunState::Running;
        s.last_error = None;
        s.pending_interrupt = None;
        STATUS_VERSION.fetch_add(1, Ordering::SeqCst);
        s.emit_event(ChatRuntimeEvent::CacheUpdated {
            session_id: session_id.clone(),
            version,
        });
        version
    };
    let _ = cache_version; // silence unused warning

    // Persist to DB
    if let Some(ref sdk) = sdk {
        let mut user_message = CachedMessage::user(user_id.clone(), message.clone(), now_ms);
        user_message.references = references.clone();
        user_message.attachments = attachments.clone();
        user_message.refresh_ui_elements();
        let _ = sdk.upsert_chat_message_json(
            session_id.clone(),
            user_id.clone(),
            now_ms,
            cached_message_storage_json(&session_id, &user_message),
        );

        // Ensure the session appears in history immediately (even while streaming).
        let title = {
            let content = message.trim();
            if content.len() > 50 {
                // Walk back from byte 47 to the nearest char boundary so we never
                // panic on multi-byte UTF-8 characters (e.g. CJK: 3 bytes each).
                let mut cut = 47.min(content.len());
                while !content.is_char_boundary(cut) {
                    cut -= 1;
                }
                Some(format!("{}...", &content[..cut]))
            } else if content.is_empty() {
                None
            } else {
                Some(content.to_string())
            }
        };
        let _ = sdk.upsert_chat_session(session_id.clone(), title, 1);
    }

    // Build the current input (with multimodal content_parts if images are attached)
    // Step 1: Upload any local images to cloud storage and get remi:// URIs.
    let image_uris = match prepare_chat_image_attachments(&config, &attachments).await {
        Ok(uris) => uris,
        Err(e) => {
            let error_msg = format!("Image upload failed: {}", e);
            tracing::error!(error = %error_msg, "Aborting chat send due to image upload failure");
            let mut s = state.write().await;
            s.state = ChatRunState::Idle;
            s.last_error = Some(error_msg.clone());
            s.emit_event(ChatRuntimeEvent::StatusChanged {
                state: ChatRunState::Idle,
                session_id: Some(session_id.clone()),
                error: Some(error_msg.clone()),
            });
            return Err(error_msg);
        }
    };
    let current_input = build_current_input_message(&message, &image_uris);
    let pending_user = input_message_to_protocol_history(user_id.clone(), &current_input);
    let start_input = {
        let mut s = state.write().await;
        let session = s.get_or_create_session(&session_id);
        if session.protocol_state.history.is_empty() {
            if let Some(sys) = system_prompt.as_ref().filter(|value| !value.trim().is_empty()) {
                session.protocol_state.history.push(ProtocolHistoryMessage {
                    id: format!("system_{}", now_ms),
                    role: "system".to_string(),
                    content: JsonValue::String(sys.clone()),
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                    reasoning_content: None,
                });
            }
        }

        chat_proto::ChatStartInput {
            history: session
                .protocol_state
                .history
                .iter()
                .map(protocol_history_to_proto)
                .collect(),
            current: Some(current_input.clone()),
            metadata: None,
        }
    };

    // Create cancel channel
    let (cancel_tx, cancel_rx) = mpsc::channel::<()>(1);
    state.write().await.cancel_tx = Some(cancel_tx);

    // Spawn stream processing task
    let state_clone = state.clone();
    let session_id_clone = session_id.clone();
    let assistant_id_clone = assistant_id.clone();

    tracing::info!(session_id = %session_id, "[ChatRuntime] spawning run_stream_loop");
    tokio::spawn(async move {
        tracing::info!(session_id = %session_id_clone, "[ChatRuntime] run_stream_loop task started");
        let result = run_stream_loop(
            state_clone.clone(),
            access_token,
            config,
            backend,
            session_id_clone.clone(),
            assistant_id_clone.clone(),
            ProtocolTurnState::for_user(pending_user),
            Some(start_input),
            None,
            cancel_rx,
        )
        .await;

        tracing::info!(
            session_id = %session_id_clone,
            ok = result.is_ok(),
            "[ChatRuntime] run_stream_loop finished"
        );

        // Update state based on result
        let mut should_persist = false;
        {
            let mut s = state_clone.write().await;
            if s.active_session_id.as_ref() == Some(&session_id_clone) {
                match result {
                    Ok(StreamResult::Completed) => {
                        s.active_session_id = None;
                        s.state = ChatRunState::Idle;
                        s.last_error = None;
                        s.pending_interrupt = None;
                        // Emit StatusChanged event so subscribers know chat is done
                        s.emit_event(ChatRuntimeEvent::StatusChanged {
                            state: ChatRunState::Idle,
                            session_id: Some(session_id_clone.clone()),
                            error: None,
                        });
                        should_persist = true;
                    }
                    Ok(StreamResult::WaitingForUser(pending)) => {
                        s.state = ChatRunState::WaitingForUser;
                        s.pending_interrupt = Some(pending.clone());
                        s.emit_event(ChatRuntimeEvent::InterruptPending {
                            session_id: session_id_clone.clone(),
                            interrupt_id: pending.interrupt_id,
                            interrupt_type: pending.interrupt_type,
                            display_data: pending.display_data,
                        });
                        should_persist = true;
                    }
                    Err(e) => {
                        s.active_session_id = None;
                        s.state = ChatRunState::Error;
                        s.last_error = Some(e.clone());
                        s.pending_interrupt = None;

                        // Emit StatusChanged so Flutter can stop waiting and show the error.
                        s.emit_event(ChatRuntimeEvent::StatusChanged {
                            state: ChatRunState::Error,
                            session_id: Some(session_id_clone.clone()),
                            error: Some(e.clone()),
                        });

                        // Mark the last assistant message as error (best-effort).
                        let mut version_opt: Option<u64> = None;
                        if let Some(sess) = s.sessions.get_mut(&session_id_clone) {
                            let target_id = sess
                                .messages
                                .iter()
                                .rev()
                                .find(|m| !m.is_user)
                                .map(|m| m.id.clone())
                                .unwrap_or_else(|| assistant_id_clone.clone());

                            let mut m = CachedMessage::assistant(
                                target_id.clone(),
                                chrono::Utc::now().timestamp_millis(),
                            );
                            m.content = e;
                            m.has_error = Some(true);
                            sess.upsert(m);
                            version_opt = Some(sess.version);
                        }

                        if let Some(version) = version_opt {
                            s.emit_event(ChatRuntimeEvent::CacheUpdated {
                                session_id: session_id_clone.clone(),
                                version,
                            });
                        }

                        should_persist = true;
                    }
                }
                s.cancel_tx = None;
                STATUS_VERSION.fetch_add(1, Ordering::SeqCst);
            }
        }

        if should_persist {
            persist_session(&state_clone, &session_id_clone).await;
        }
    });

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Resume Handler
// ═══════════════════════════════════════════════════════════════════════════════

async fn handle_resume(
    state: Arc<RwLock<ActorState>>,
    session_id: String,
    resume_value: JsonValue,
) -> Result<(), String> {
    ensure_protocol_state_loaded(&state, &session_id).await?;

    // Verify we're in WaitingForUser state
    let (access_token, config, backend, assistant_id, resume_input) = {
        let mut s = state.write().await;
        if s.state != ChatRunState::WaitingForUser {
            return Err("Not waiting for user".to_string());
        }
        if s.active_session_id.as_ref() != Some(&session_id) {
            return Err("Session mismatch".to_string());
        }

        let assistant_id = format!("assistant_{}", chrono::Utc::now().timestamp_millis());

        let session = s
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| "Session state missing".to_string())?;
        let pending = session
            .protocol_state
            .pending_tool_execution
            .clone()
            .ok_or_else(|| "No pending tool execution state".to_string())?;

        let manual_results = build_manual_resume_outcomes(&pending, resume_value)?;
        let mut all_results = Vec::new();
        all_results.extend(pending.completed_results.clone());
        all_results.extend(pending.resolved_results.clone());
        all_results.extend(manual_results);

        for outcome in &all_results {
            session
                .protocol_state
                .history
                .push(outcome_to_protocol_tool_message(outcome));
        }
        session.protocol_state.pending_tool_execution = None;

        let resume_input = chat_proto::ChatResumeInput {
            state: Some(json_value_to_prost_struct(pending.state.clone())),
            results: all_results.iter().filter_map(tool_outcome_to_proto).collect(),
        };

        s.state = ChatRunState::Running;
        s.pending_interrupt = None;
        STATUS_VERSION.fetch_add(1, Ordering::SeqCst);

        (
            s.access_token.clone().ok_or("Not initialized")?,
            s.config.clone(),
            s.backend.clone(),
            assistant_id,
            resume_input,
        )
    };

    // Create cancel channel
    let (cancel_tx, cancel_rx) = mpsc::channel::<()>(1);
    state.write().await.cancel_tx = Some(cancel_tx);

    // Spawn stream processing task
    let state_clone = state.clone();
    let session_id_clone = session_id.clone();
    let assistant_id_clone = assistant_id.clone();

    tokio::spawn(async move {
        let result = run_stream_loop(
            state_clone.clone(),
            access_token,
            config,
            backend,
            session_id_clone.clone(),
            assistant_id_clone.clone(),
            ProtocolTurnState::default(),
            None,
            Some(resume_input),
            cancel_rx,
        )
        .await;

        // Update state based on result
        let mut should_persist = false;
        {
            let mut s = state_clone.write().await;
            if s.active_session_id.as_ref() == Some(&session_id_clone) {
                match result {
                    Ok(StreamResult::Completed) => {
                        s.active_session_id = None;
                        s.state = ChatRunState::Idle;
                        s.last_error = None;
                        s.pending_interrupt = None;
                        // Emit StatusChanged event so subscribers know chat is done
                        s.emit_event(ChatRuntimeEvent::StatusChanged {
                            state: ChatRunState::Idle,
                            session_id: Some(session_id_clone.clone()),
                            error: None,
                        });
                        should_persist = true;
                    }
                    Ok(StreamResult::WaitingForUser(pending)) => {
                        s.state = ChatRunState::WaitingForUser;
                        s.pending_interrupt = Some(pending.clone());
                        s.emit_event(ChatRuntimeEvent::InterruptPending {
                            session_id: session_id_clone.clone(),
                            interrupt_id: pending.interrupt_id,
                            interrupt_type: pending.interrupt_type,
                            display_data: pending.display_data,
                        });
                        should_persist = true;
                    }
                    Err(e) => {
                        s.active_session_id = None;
                        s.state = ChatRunState::Error;
                        s.last_error = Some(e.clone());
                        s.pending_interrupt = None;

                        s.emit_event(ChatRuntimeEvent::StatusChanged {
                            state: ChatRunState::Error,
                            session_id: Some(session_id_clone.clone()),
                            error: Some(e.clone()),
                        });

                        // Mark the last assistant message as error (best-effort).
                        let mut version_opt: Option<u64> = None;
                        if let Some(sess) = s.sessions.get_mut(&session_id_clone) {
                            let target_id = sess
                                .messages
                                .iter()
                                .rev()
                                .find(|m| !m.is_user)
                                .map(|m| m.id.clone())
                                .unwrap_or_else(|| assistant_id_clone.clone());

                            let mut m = CachedMessage::assistant(
                                target_id.clone(),
                                chrono::Utc::now().timestamp_millis(),
                            );
                            m.content = e;
                            m.has_error = Some(true);
                            sess.upsert(m);
                            version_opt = Some(sess.version);
                        }

                        if let Some(version) = version_opt {
                            s.emit_event(ChatRuntimeEvent::CacheUpdated {
                                session_id: session_id_clone.clone(),
                                version,
                            });
                        }

                        should_persist = true;
                    }
                }
                s.cancel_tx = None;
                STATUS_VERSION.fetch_add(1, Ordering::SeqCst);
            }
        }

        if should_persist {
            persist_session(&state_clone, &session_id_clone).await;
        }
    });

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Stream Processing
// ═══════════════════════════════════════════════════════════════════════════════

enum StreamResult {
    Completed,
    WaitingForUser(PendingInterrupt),
}

enum StreamControl {
    Continue,
    AutoResume(chat_proto::ChatResumeInput),
    WaitForUser(PendingInterrupt),
    Cancelled,
}

async fn run_stream_loop(
    state: Arc<RwLock<ActorState>>,
    access_token: String,
    config: ChatRuntimeConfig,
    backend: ChatExecutionBackend,
    session_id: String,
    assistant_id: String,
    initial_turn_state: ProtocolTurnState,
    initial_start: Option<chat_proto::ChatStartInput>,
    initial_resume: Option<chat_proto::ChatResumeInput>,
    mut cancel_rx: mpsc::Receiver<()>,
) -> Result<StreamResult, String> {
    tracing::info!(session_id = %session_id, "[ChatRuntime] run_stream_loop enter");
    let mut start_input = initial_start;
    let mut resume_input = initial_resume;
    let mut next_turn_state = Some(initial_turn_state);
    let mut auto_resume_count = 0;
    let mut current_assistant_id = assistant_id;

    loop {
        let mut stream = open_chat_stream(
            &backend,
            &access_token,
            &config,
            session_id.clone(),
            start_input.take(),
            resume_input.take(),
        )
        .await?;
        tracing::info!(session_id = %session_id, "[ChatRuntime] stream started, processing chunks");

        let mut turn_state = next_turn_state.take().unwrap_or_default();

        let mut persist_tick = tokio::time::interval(std::time::Duration::from_millis(500));
        persist_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut dirty_since_last_persist = false;
        let mut last_seen_version: Option<u64> = {
            let s = state.read().await;
            s.sessions.get(&session_id).map(|sess| sess.version)
        };

        loop {
            tokio::select! {
                _ = cancel_rx.recv() => {
                    tracing::info!(session_id, "Chat stream cancelled");
                    if dirty_since_last_persist {
                        persist_session(&state, &session_id).await;
                    }
                    return Ok(StreamResult::Completed);
                }
                _ = persist_tick.tick() => {
                    if dirty_since_last_persist {
                        persist_session(&state, &session_id).await;
                        dirty_since_last_persist = false;
                    }
                }
                chunk_result = stream.next() => {
                    match chunk_result {
                        None => {
                            if !turn_state.committed {
                                let mut s = state.write().await;
                                if let Some(session) = s.sessions.get_mut(&session_id) {
                                    turn_state.commit_completed(session, &current_assistant_id);
                                }
                            }
                            tracing::info!(session_id, "Chat stream completed");
                            persist_session(&state, &session_id).await;
                            return Ok(StreamResult::Completed);
                        }
                        Some(Err(e)) => {
                            return Err(format!("Stream error: {}", e));
                        }
                        Some(Ok(event)) => {
                            if let Some(fatal) = extract_fatal_error_from_event(&event) {
                                return Err(fatal);
                            }

                            if let Some(chat_stream_event::Event::Interrupt(interrupt_event)) = event.event.as_ref() {
                                if !interrupt_event.interrupts.is_empty() {
                                    return Err("Interrupt-based tool flow is no longer supported; expected external tool calling".to_string());
                                }
                            }

                            match process_stream_event(
                                &state,
                                &session_id,
                                &current_assistant_id,
                                &mut turn_state,
                                &event,
                            ).await? {
                                StreamControl::Continue => {}
                                StreamControl::AutoResume(resume) => {
                                    auto_resume_count += 1;
                                    if auto_resume_count > config.max_auto_resumes {
                                        return Err("Max auto-resume limit reached".to_string());
                                    }

                                    tracing::info!(
                                        session_id,
                                        count = auto_resume_count,
                                        "[ChatRuntime] NeedToolExecution fully auto-resolved, resuming"
                                    );

                                    persist_session(&state, &session_id).await;
                                    resume_input = Some(resume);
                                    start_input = None;
                                    next_turn_state = Some(ProtocolTurnState::default());
                                    current_assistant_id = format!(
                                        "assistant_{}_{}",
                                        chrono::Utc::now().timestamp_millis(),
                                        auto_resume_count
                                    );
                                    break;
                                }
                                StreamControl::WaitForUser(pending) => {
                                    tracing::info!(
                                        interrupt_id = %pending.interrupt_id,
                                        interrupt_type = %pending.interrupt_type,
                                        "[ChatRuntime] Waiting for user to resolve external tool call"
                                    );
                                    persist_session(&state, &session_id).await;
                                    return Ok(StreamResult::WaitingForUser(pending));
                                }
                                StreamControl::Cancelled => {
                                    persist_session(&state, &session_id).await;
                                    return Ok(StreamResult::Completed);
                                }
                            }

                            let current_version: Option<u64> = {
                                let s = state.read().await;
                                s.sessions.get(&session_id).map(|sess| sess.version)
                            };
                            if current_version.is_some() && current_version != last_seen_version {
                                last_seen_version = current_version;
                                dirty_since_last_persist = true;
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn process_stream_event(
    state: &Arc<RwLock<ActorState>>,
    session_id: &str,
    assistant_id: &str,
    turn_state: &mut ProtocolTurnState,
    event: &ChatStreamEvent,
) -> Result<StreamControl, String> {
    match event.event.as_ref() {
        Some(chat_stream_event::Event::Delta(delta)) => {
            if delta.content.is_empty() {
                return Ok(StreamControl::Continue);
            }

            turn_state.record_delta(&delta.content);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let mut s = state.write().await;
            let version_opt = if let Some(session) = s.sessions.get_mut(session_id) {
                session
                    .assistant_content
                    .insert(assistant_id.to_string(), turn_state.assistant_content.clone());

                let mut message = CachedMessage::assistant(assistant_id.to_string(), now_ms);
                message.content = turn_state.assistant_content.clone();
                message.thinking = session.assistant_thinking.get(assistant_id).cloned();
                session.upsert(message);
                Some(session.version)
            } else {
                None
            };

            if let Some(version) = version_opt {
                s.emit_event(ChatRuntimeEvent::CacheUpdated {
                    session_id: session_id.to_string(),
                    version,
                });
            }
            Ok(StreamControl::Continue)
        }
        Some(chat_stream_event::Event::ThinkingStart(_)) => Ok(StreamControl::Continue),
        Some(chat_stream_event::Event::ThinkingEnd(thinking)) => {
            turn_state.record_reasoning(&thinking.content);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let mut s = state.write().await;
            let version_opt = if let Some(session) = s.sessions.get_mut(session_id) {
                session
                    .assistant_thinking
                    .insert(assistant_id.to_string(), thinking.content.clone());

                let mut message = CachedMessage::assistant(assistant_id.to_string(), now_ms);
                message.content = session
                    .assistant_content
                    .get(assistant_id)
                    .cloned()
                    .unwrap_or_default();
                message.thinking = Some(thinking.content.clone());
                session.upsert(message);
                Some(session.version)
            } else {
                None
            };

            if let Some(version) = version_opt {
                s.emit_event(ChatRuntimeEvent::CacheUpdated {
                    session_id: session_id.to_string(),
                    version,
                });
            }
            Ok(StreamControl::Continue)
        }
        Some(chat_stream_event::Event::ToolCallStart(tool_call)) => {
            turn_state.record_tool_call_start(&tool_call.id, &tool_call.tool_name);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let mut s = state.write().await;
            let version_opt = if let Some(session) = s.sessions.get_mut(session_id) {
                session
                    .tool_call_name
                    .insert(tool_call.id.clone(), tool_call.tool_name.clone());

                let mut message = CachedMessage::assistant(
                    format!("tool_call:{}", tool_call.id),
                    now_ms,
                );
                message.content = format!("Tool call: `{}`", tool_call.tool_name);
                message.tool_name = Some(tool_call.tool_name.clone());
                session.upsert(message);
                Some(session.version)
            } else {
                None
            };

            if let Some(version) = version_opt {
                s.emit_event(ChatRuntimeEvent::CacheUpdated {
                    session_id: session_id.to_string(),
                    version,
                });
            }
            Ok(StreamControl::Continue)
        }
        Some(chat_stream_event::Event::ToolCallDelta(tool_call)) => {
            turn_state.record_tool_call_delta(&tool_call.id, &tool_call.arguments_delta);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let mut s = state.write().await;
            let version_opt = if let Some(session) = s.sessions.get_mut(session_id) {
                let entry = session
                    .tool_call_args
                    .entry(tool_call.id.clone())
                    .or_insert_with(BTreeMap::new);
                let combined = format!(
                    "{}{}",
                    entry.get(&0).cloned().unwrap_or_default(),
                    tool_call.arguments_delta
                );
                entry.insert(0, combined.clone());

                let tool_name = session
                    .tool_call_name
                    .get(&tool_call.id)
                    .cloned()
                    .unwrap_or_else(|| {
                        turn_state
                            .tool_calls
                            .iter()
                            .find(|call| call.id == tool_call.id)
                            .map(|call| call.tool_name.clone())
                            .unwrap_or_else(|| "(unknown)".to_string())
                    });

                let mut message = CachedMessage::assistant(
                    format!("tool_call:{}", tool_call.id),
                    now_ms,
                );
                message.content = format!(
                    "Tool call: `{}`\n\n```json\n{}\n```",
                    tool_name,
                    combined
                );
                message.tool_name = Some(tool_name);
                session.upsert(message);
                Some(session.version)
            } else {
                None
            };

            if let Some(version) = version_opt {
                s.emit_event(ChatRuntimeEvent::CacheUpdated {
                    session_id: session_id.to_string(),
                    version,
                });
            }
            Ok(StreamControl::Continue)
        }
        Some(chat_stream_event::Event::ToolDelta(tool_delta)) => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let mut s = state.write().await;
            let version_opt = if let Some(session) = s.sessions.get_mut(session_id) {
                if !tool_delta.tool_name.trim().is_empty() {
                    session
                        .tool_call_name
                        .insert(tool_delta.id.clone(), tool_delta.tool_name.clone());
                }

                let combined = {
                    let entry = session
                        .tool_result_content
                        .entry(tool_delta.id.clone())
                        .or_insert_with(String::new);
                    entry.push_str(&tool_delta.delta);
                    entry.clone()
                };

                let tool_name = session
                    .tool_call_name
                    .get(&tool_delta.id)
                    .cloned()
                    .unwrap_or_else(|| tool_delta.tool_name.clone());

                let message = CachedMessage::tool_result(
                    format!("tool_result:{}", tool_delta.id),
                    tool_name,
                    combined,
                    now_ms,
                );
                session.upsert(message);
                Some(session.version)
            } else {
                None
            };

            if let Some(version) = version_opt {
                s.emit_event(ChatRuntimeEvent::CacheUpdated {
                    session_id: session_id.to_string(),
                    version,
                });
            }
            Ok(StreamControl::Continue)
        }
        Some(chat_stream_event::Event::ToolResult(tool_result)) => {
            let outcome = ToolExecutionOutcome {
                tool_call_id: tool_result.id.clone(),
                tool_name: tool_result.tool_name.clone(),
                result: Some(tool_result.result.clone()),
                result_parts: None,
                error: None,
            };
            turn_state.record_tool_result_message(&outcome);

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let mut s = state.write().await;
            let version_opt = if let Some(session) = s.sessions.get_mut(session_id) {
                if !tool_result.tool_name.trim().is_empty() {
                    session
                        .tool_call_name
                        .insert(tool_result.id.clone(), tool_result.tool_name.clone());
                }
                session
                    .tool_result_content
                    .insert(tool_result.id.clone(), tool_result.result.clone());

                let tool_name = session
                    .tool_call_name
                    .get(&tool_result.id)
                    .cloned()
                    .unwrap_or_else(|| tool_result.tool_name.clone());

                let message = CachedMessage::tool_result(
                    format!("tool_result:{}", tool_result.id),
                    tool_name,
                    tool_result.result.clone(),
                    now_ms,
                );
                session.upsert(message);
                Some(session.version)
            } else {
                None
            };

            if let Some(version) = version_opt {
                s.emit_event(ChatRuntimeEvent::CacheUpdated {
                    session_id: session_id.to_string(),
                    version,
                });
            }
            Ok(StreamControl::Continue)
        }
        Some(chat_stream_event::Event::NeedToolExecution(need_tool)) => {
            handle_need_tool_execution_event(
                state,
                session_id,
                assistant_id,
                turn_state,
                &need_tool,
            )
            .await
        }
        Some(chat_stream_event::Event::Done(_)) => {
            let mut s = state.write().await;
            if let Some(session) = s.sessions.get_mut(session_id) {
                turn_state.commit_completed(session, assistant_id);
            }
            Ok(StreamControl::Continue)
        }
        Some(chat_stream_event::Event::Cancelled(_)) => Ok(StreamControl::Cancelled),
        Some(chat_stream_event::Event::Error(err)) => Err(if err.message.trim().is_empty() {
            "Agent error".to_string()
        } else {
            err.message.clone()
        }),
        Some(chat_stream_event::Event::RunStart(_))
        | Some(chat_stream_event::Event::TurnStart(_))
        | Some(chat_stream_event::Event::Usage(_))
        | Some(chat_stream_event::Event::Custom(_))
        | Some(chat_stream_event::Event::Interrupt(_)) => Ok(StreamControl::Continue),
        None => Ok(StreamControl::Continue),
    }
}

async fn handle_need_tool_execution_event(
    state: &Arc<RwLock<ActorState>>,
    session_id: &str,
    assistant_id: &str,
    turn_state: &mut ProtocolTurnState,
    event: &chat_proto::ChatNeedToolExecutionEvent,
) -> Result<StreamControl, String> {
    let state_json = event
        .state
        .as_ref()
        .map(prost_struct_to_json)
        .ok_or_else(|| "NeedToolExecution missing resume state".to_string())?;

    let completed_results = event
        .completed_results
        .iter()
        .filter_map(proto_tool_outcome_to_runtime)
        .collect::<Vec<_>>();
    for outcome in &completed_results {
        turn_state.record_tool_result_message(outcome);
    }

    let mut resolved_results = Vec::new();
    let mut pending_calls = Vec::new();
    let mut things_changed = false;
    let mut trigger_scheduler_sync_needed = false;

    let handler_outcomes = {
        let s = state.read().await;
        event
            .tool_calls
            .iter()
            .map(|tool_call| {
                let arguments = tool_call
                    .arguments
                    .as_ref()
                    .map(prost_struct_to_json)
                    .unwrap_or_else(|| json!({}));
                let default_display = tool_call_display_payload(&tool_call.tool_name, &arguments);
                let action = s.handler_registry.process(&tool_call.id, &default_display);
                (tool_call.clone(), arguments, default_display, action)
            })
            .collect::<Vec<_>>()
    };

    for (tool_call, arguments, default_display, action) in handler_outcomes {
        let pending_call = PendingToolCall {
            tool_call_id: tool_call.id.clone(),
            tool_name: tool_call.tool_name.clone(),
            arguments,
            display_data: default_display.clone(),
        };

        match action {
            InterruptAction::AutoResume(values) => {
                let resume_value = values
                    .get(&tool_call.id)
                    .cloned()
                    .or_else(|| values.values().next().cloned())
                    .unwrap_or_else(|| RichHandlerResult::Json(json!({ "error": "handler did not return a resume value" })));
                let outcome = raw_resume_value_to_outcome(&pending_call, resume_value);
                if crate::interrupt_handler::extract_interrupt_type(&default_display)
                    == "trigger_rule_published"
                    && outcome.error.is_none()
                {
                    trigger_scheduler_sync_needed = true;
                }
                things_changed = true;
                turn_state.record_tool_result_message(&outcome);
                resolved_results.push(outcome);
            }
            InterruptAction::WaitForUser { pending } => {
                let display_data = pending
                    .iter()
                    .find(|item| item.interrupt_id == tool_call.id)
                    .map(|item| item.display_data.clone())
                    .unwrap_or_else(|| default_display.clone());
                pending_calls.push(PendingToolCall {
                    display_data,
                    ..pending_call
                });
            }
            InterruptAction::Skip => {
                pending_calls.push(pending_call);
            }
        }
    }

    let first_pending_interrupt = pending_calls.first().map(|call| PendingInterrupt {
        interrupt_id: call.tool_call_id.clone(),
        interrupt_type: {
            let extracted = crate::interrupt_handler::extract_interrupt_type(&call.display_data);
            if extracted.trim().is_empty() {
                call.tool_name.clone()
            } else {
                extracted
            }
        },
        display_data: call.display_data.clone(),
    });

    let mut s = state.write().await;
    if let Some(session) = s.sessions.get_mut(session_id) {
        if pending_calls.is_empty() {
            turn_state.commit_completed(session, assistant_id);
        } else {
            let pending_state = PendingToolExecutionState {
                state: state_json,
                completed_results: completed_results.clone(),
                resolved_results: resolved_results.clone(),
                pending_calls: pending_calls.clone(),
            };
            turn_state.commit_need_tool_execution(session, assistant_id, pending_state);
        }
    }

    if trigger_scheduler_sync_needed {
        s.emit_event(ChatRuntimeEvent::TriggerSchedulerSyncRequested);
    }
    if things_changed {
        s.emit_event(ChatRuntimeEvent::ThingsChanged);
    }
    drop(s);

    if let Some(pending) = first_pending_interrupt {
        return Ok(StreamControl::WaitForUser(pending));
    }

    let mut all_results = Vec::new();
    all_results.extend(completed_results);
    all_results.extend(resolved_results);

    Ok(StreamControl::AutoResume(chat_proto::ChatResumeInput {
        state: event.state.clone(),
        results: all_results.iter().filter_map(tool_outcome_to_proto).collect(),
    }))
}

async fn persist_session(state: &Arc<RwLock<ActorState>>, session_id: &str) {
    let (sdk, messages, protocol_state_json) = {
        let s = state.read().await;
        let session = s.sessions.get(session_id);
        let msgs = session
            .map(|sess| sess.get_messages())
            .unwrap_or_default();
        let protocol_state_json = session
            .and_then(|sess| serde_json::to_string(&sess.protocol_state).ok())
            .unwrap_or_else(|| serde_json::to_string(&ChatProtocolSessionState::default()).unwrap_or_default());
        (s.sdk.clone(), msgs, protocol_state_json)
    };

    if let Some(sdk) = sdk {
        let _ = sdk.upsert_chat_runtime_state_json(
            session_id.to_string(),
            protocol_state_json,
        );

        // Persist individual messages
        for msg in &messages {
            let _ = sdk.upsert_chat_message_json(
                session_id.to_string(),
                msg.id.clone(),
                msg.timestamp_ms,
                cached_message_storage_json(session_id, msg),
            );
        }

        // Persist session metadata (so it appears in session history)
        // Extract title from first user message, or use a default
        let title = messages.iter().find(|m| m.is_user).map(|m| {
            let content = m.content.trim();
            if content.len() > 50 {
                // Walk back from byte 47 to the nearest char boundary so we never
                // panic on multi-byte UTF-8 characters (e.g. CJK: 3 bytes each).
                let mut cut = 47.min(content.len());
                while !content.is_char_boundary(cut) {
                    cut -= 1;
                }
                format!("{}...", &content[..cut])
            } else {
                content.to_string()
            }
        });

        let message_count = messages.len() as i32;
        let _ = sdk.upsert_chat_session(session_id.to_string(), title, message_count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn json_value_to_prost_struct(val: JsonValue) -> prost_types::Struct {
    let fields = match val {
        JsonValue::Object(map) => map
            .into_iter()
            .map(|(k, v)| (k, json_value_to_prost_value(v)))
            .collect(),
        _ => {
            let mut m = std::collections::BTreeMap::new();
            m.insert("value".to_string(), json_value_to_prost_value(val));
            m
        }
    };
    prost_types::Struct { fields }
}

fn json_value_to_prost_value(val: JsonValue) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match val {
        JsonValue::Null => Kind::NullValue(0),
        JsonValue::Bool(b) => Kind::BoolValue(b),
        JsonValue::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        JsonValue::String(s) => Kind::StringValue(s),
        JsonValue::Array(arr) => {
            let values = arr.into_iter().map(json_value_to_prost_value).collect();
            Kind::ListValue(prost_types::ListValue { values })
        }
        JsonValue::Object(map) => {
            let fields = map
                .into_iter()
                .map(|(k, v)| (k, json_value_to_prost_value(v)))
                .collect();
            Kind::StructValue(prost_types::Struct { fields })
        }
    };
    prost_types::Value { kind: Some(kind) }
}

fn prost_struct_to_json(s: &prost_types::Struct) -> JsonValue {
    let map: serde_json::Map<String, JsonValue> = s
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), prost_value_to_json(v)))
        .collect();
    JsonValue::Object(map)
}

fn prost_value_to_json(v: &prost_types::Value) -> JsonValue {
    use prost_types::value::Kind;
    match &v.kind {
        None => JsonValue::Null,
        Some(Kind::NullValue(_)) => JsonValue::Null,
        Some(Kind::BoolValue(b)) => JsonValue::Bool(*b),
        Some(Kind::NumberValue(n)) => json!(*n),
        Some(Kind::StringValue(s)) => JsonValue::String(s.clone()),
        Some(Kind::ListValue(list)) => {
            JsonValue::Array(list.values.iter().map(prost_value_to_json).collect())
        }
        Some(Kind::StructValue(s)) => prost_struct_to_json(s),
    }
}
