//! Chat runtime types shared across SDK, mobile, and CLI.

use crate::types::ChatSession;
use chrono::{DateTime, Utc};
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

// ═══════════════════════════════════════════════════════════════════════════════
// Run State
// ═══════════════════════════════════════════════════════════════════════════════

/// Chat run state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ChatRunState {
    #[default]
    Idle,
    Running,
    WaitingForUser,
    Error,
}

/// Pending interrupt awaiting user action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingInterrupt {
    pub interrupt_id: String,
    pub interrupt_type: String,
    pub display_data: JsonValue,
}

/// Status information about a chat run
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChatRunStatus {
    pub state: ChatRunState,
    pub session_id: Option<String>,
    pub error_message: Option<String>,
    pub pending_interrupt: Option<PendingInterrupt>,
}

/// Versioned export of a persisted chat session for debugging or replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatSessionExportBundle {
    pub version: u32,
    pub exported_at: DateTime<Utc>,
    pub session: ChatSession,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<CachedMessage>,
    pub protocol_state: ChatProtocolSessionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_status: Option<ChatRunStatus>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub metadata: serde_json::Map<String, JsonValue>,
}

impl ChatSessionExportBundle {
    pub const VERSION: u32 = 1;

    pub fn new(
        session: ChatSession,
        messages: Vec<CachedMessage>,
        protocol_state: ChatProtocolSessionState,
    ) -> Self {
        Self {
            version: Self::VERSION,
            exported_at: Utc::now(),
            session,
            messages,
            protocol_state,
            run_status: None,
            metadata: serde_json::Map::new(),
        }
    }

    pub fn with_run_status(mut self, run_status: ChatRunStatus) -> Self {
        self.run_status = Some(run_status);
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Map<String, JsonValue>) -> Self {
        self.metadata = metadata;
        self
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Cached Messages
// ═══════════════════════════════════════════════════════════════════════════════

/// A single cached message for UI rendering
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CachedUiElement {
    Markdown {
        text: String,
    },
    Thinking {
        text: String,
    },
    References {
        items: JsonValue,
    },
    Images {
        uris: Vec<String>,
    },
    ToolCall {
        tool_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        arguments_json: Option<String>,
    },
    ToolResult {
        tool_name: String,
        result: String,
    },
    SubSession {
        data: CachedSubSession,
    },
    Error {
        text: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedSubSession {
    pub parent_tool_call_id: String,
    pub sub_session_id: String,
    pub sub_run_id: String,
    pub agent_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default)]
    pub depth: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<CachedSubSessionItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CachedSubSessionItem {
    Markdown {
        text: String,
    },
    Thinking {
        text: String,
    },
    ToolCall {
        tool_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        arguments_json: Option<String>,
    },
    ToolResult {
        tool_name: String,
        result: String,
    },
    TurnStart {
        turn: u32,
    },
    Error {
        text: String,
    },
}

/// A single cached message for UI rendering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedMessage {
    pub id: String,
    pub content: String,
    pub is_user: bool,
    pub timestamp_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_error: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_session: Option<CachedSubSession>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ui_elements: Vec<CachedUiElement>,
}

impl CachedMessage {
    pub fn user(id: String, content: String, timestamp_ms: i64) -> Self {
        Self {
            id,
            content,
            is_user: true,
            timestamp_ms,
            thinking: None,
            has_error: None,
            tool_name: None,
            tool_result: None,
            references: None,
            attachments: None,
            sub_session: None,
            ui_elements: Vec::new(),
        }
    }

    pub fn assistant(id: String, timestamp_ms: i64) -> Self {
        Self {
            id,
            content: String::new(),
            is_user: false,
            timestamp_ms,
            thinking: None,
            has_error: None,
            tool_name: None,
            tool_result: None,
            references: None,
            attachments: None,
            sub_session: None,
            ui_elements: Vec::new(),
        }
    }

    pub fn tool_result(id: String, tool_name: String, result: String, timestamp_ms: i64) -> Self {
        Self {
            id,
            content: String::new(),
            is_user: false,
            timestamp_ms,
            thinking: None,
            has_error: None,
            tool_name: Some(tool_name),
            tool_result: Some(result),
            references: None,
            attachments: None,
            sub_session: None,
            ui_elements: Vec::new(),
        }
    }

    pub fn refresh_ui_elements(&mut self) {
        self.ui_elements = derive_ui_elements(self);
    }
}

fn derive_ui_elements(message: &CachedMessage) -> Vec<CachedUiElement> {
    let mut elements = Vec::new();
    let image_uris = collect_image_uris(message.attachments.as_ref(), &message.content);
    let markdown_text = strip_image_lines(&message.content);

    if message.is_user {
        append_references(&mut elements, message.references.as_ref());
        if !markdown_text.trim().is_empty() {
            elements.push(CachedUiElement::Markdown {
                text: markdown_text,
            });
        }
        append_images(&mut elements, image_uris);
        return elements;
    }

    if let Some(thinking) = message
        .thinking
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        elements.push(CachedUiElement::Thinking {
            text: thinking.clone(),
        });
    }

    if message.has_error.unwrap_or(false) {
        let text = markdown_text.trim();
        if !text.is_empty() {
            elements.push(CachedUiElement::Error {
                text: text.to_string(),
            });
        }
        append_references(&mut elements, message.references.as_ref());
        append_images(&mut elements, image_uris);
        return elements;
    }

    if let Some(result) = message
        .tool_result
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        elements.push(CachedUiElement::ToolResult {
            tool_name: message
                .tool_name
                .clone()
                .unwrap_or_else(|| "tool".to_string()),
            result: result.clone(),
        });
        append_references(&mut elements, message.references.as_ref());
        append_images(&mut elements, image_uris);
        return elements;
    }

    if let Some(tool_name) = message
        .tool_name
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        elements.push(CachedUiElement::ToolCall {
            tool_name: tool_name.clone(),
            arguments_json: parse_tool_call_arguments(&message.content),
        });
        if let Some(sub_session) = message.sub_session.clone() {
            elements.push(CachedUiElement::SubSession { data: sub_session });
        }
        append_references(&mut elements, message.references.as_ref());
        append_images(&mut elements, image_uris);
        return elements;
    }

    if !markdown_text.trim().is_empty() {
        elements.push(CachedUiElement::Markdown {
            text: markdown_text,
        });
    }

    append_references(&mut elements, message.references.as_ref());
    append_images(&mut elements, image_uris);
    elements
}

fn append_references(elements: &mut Vec<CachedUiElement>, references: Option<&JsonValue>) {
    let Some(references) = references else {
        return;
    };

    let is_non_empty = match references {
        JsonValue::Array(items) => !items.is_empty(),
        JsonValue::String(text) => !text.trim().is_empty(),
        JsonValue::Null => false,
        _ => true,
    };

    if is_non_empty {
        elements.push(CachedUiElement::References {
            items: references.clone(),
        });
    }
}

fn append_images(elements: &mut Vec<CachedUiElement>, image_uris: Vec<String>) {
    if !image_uris.is_empty() {
        elements.push(CachedUiElement::Images { uris: image_uris });
    }
}

fn collect_image_uris(attachments: Option<&JsonValue>, content: &str) -> Vec<String> {
    let mut uris = Vec::new();

    if let Some(JsonValue::Array(items)) = attachments {
        for item in items {
            let Some(item_obj) = item.as_object() else {
                continue;
            };

            let item_type = item_obj
                .get("type")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if !item_type.is_empty() && item_type != "image" {
                continue;
            }

            let candidate = item_obj
                .get("url")
                .and_then(|value| value.as_str())
                .or_else(|| item_obj.get("uri").and_then(|value| value.as_str()))
                .or_else(|| item_obj.get("image_uri").and_then(|value| value.as_str()))
                .or_else(|| item_obj.get("path").and_then(|value| value.as_str()))
                .map(str::trim)
                .unwrap_or_default();

            if !candidate.is_empty() && !uris.iter().any(|existing| existing == candidate) {
                uris.push(candidate.to_string());
            }
        }
    }

    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(uri) = trimmed.strip_prefix("[image] ") {
            let uri = uri.trim();
            if !uri.is_empty() && !uris.iter().any(|existing| existing == uri) {
                uris.push(uri.to_string());
            }
        }
    }

    uris
}

fn strip_image_lines(content: &str) -> String {
    let lines = content
        .lines()
        .filter(|line| !line.trim().starts_with("[image] "))
        .collect::<Vec<_>>();
    lines.join("\n").trim().to_string()
}

fn parse_tool_call_arguments(content: &str) -> Option<String> {
    let start_idx = content.find("```json").or_else(|| content.find("```"))?;
    let fence = if content[start_idx..].starts_with("```json") {
        "```json"
    } else {
        "```"
    };
    let after_start = &content[start_idx + fence.len()..];
    let after_newline = after_start
        .strip_prefix("\r\n")
        .or_else(|| after_start.strip_prefix('\n'))
        .unwrap_or(after_start);
    let end_idx = after_newline.find("```")?;
    let args = after_newline[..end_idx].trim();
    if args.is_empty() {
        None
    } else {
        Some(args.to_string())
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Protocol Session State
// ═══════════════════════════════════════════════════════════════════════════════

/// Device-owned protocol session state used to build start/resume requests.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChatProtocolSessionState {
    #[serde(default)]
    pub history: Vec<ProtocolHistoryMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_state: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_tool_execution: Option<PendingToolExecutionState>,
}

/// A single remi-agentloop history message persisted by the runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolHistoryMessage {
    pub id: String,
    pub role: String,
    pub content: JsonValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ProtocolToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolToolCall {
    pub id: String,
    pub tool_name: String,
    pub arguments: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolExecutionOutcome {
    pub tool_call_id: String,
    pub tool_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// Multimodal result parts (e.g. raw image bytes). Mutually exclusive with `result`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_parts: Option<Vec<ToolImagePart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A raw image returned by a tool handler (before proto serialisation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolImagePart {
    pub media_type: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingToolCall {
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments: JsonValue,
    pub display_data: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingToolExecutionState {
    pub state: JsonValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completed_results: Vec<ToolExecutionOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_results: Vec<ToolExecutionOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_calls: Vec<PendingToolCall>,
}

/// Rich result that a handler can return — either plain JSON or structured multimodal data.
#[derive(Debug, Clone)]
pub enum RichHandlerResult {
    /// Plain JSON resume value (the original behaviour).
    Json(JsonValue),
    /// Raw image bytes to be forwarded to the LLM as an image part.
    Image(ToolImagePart),
}

// ═══════════════════════════════════════════════════════════════════════════════
// Runtime Events (for subscribers)
// ═══════════════════════════════════════════════════════════════════════════════

/// Events emitted by the chat runtime
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatRuntimeEvent {
    /// Status changed
    StatusChanged {
        state: ChatRunState,
        session_id: Option<String>,
        error: Option<String>,
    },

    /// Message cache updated
    CacheUpdated { session_id: String, version: u64 },

    /// Pending interrupt for UI
    InterruptPending {
        session_id: String,
        interrupt_id: String,
        interrupt_type: String,
        display_data: JsonValue,
    },

    /// Things were modified (for UI refresh)
    ThingsChanged,

    /// A trigger was installed/updated via an interrupt; platform schedulers should resync.
    TriggerSchedulerSyncRequested,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for chat runtime
#[derive(Debug, Clone)]
pub struct ChatRuntimeConfig {
    /// Device ID for things operations
    pub device_id: String,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
    /// Maximum auto-resume attempts (prevents infinite loops)
    pub max_auto_resumes: usize,
    /// Execution backend for chat requests.
    pub backend: ChatRuntimeBackend,
    /// Consent-gated tracing configuration shared by remote and local-wasm backends.
    pub tracing: ChatTracingConfig,
}

#[derive(Debug, Clone, Default)]
pub struct ChatTracingConfig {
    /// True only when the user has explicitly allowed reporting/tracing.
    pub reporting_enabled: bool,
    /// Optional LangSmith API key. For remote mode this may be omitted and resolved server-side.
    pub langsmith_api_key: Option<String>,
    /// Optional LangSmith project/session name.
    pub langsmith_project: Option<String>,
    /// Optional LangSmith API URL override.
    pub langsmith_api_url: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub enum ChatRuntimeBackend {
    #[default]
    RemoteServer,
    LocalWasm(ChatLocalWasmConfig),
}

#[derive(Debug, Clone)]
pub struct ChatLocalWasmConfig {
    pub source: ChatLocalWasmSource,
    pub api_key: String,
    pub base_url: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ChatLocalWasmSource {
    File(PathBuf),
    /// In-memory bytes for either a raw `.wasm` or a precompiled `.cwasm`.
    ///
    /// The runtime auto-detects the format from the magic header:
    /// - `\0asm` → JIT-compiled (requires `local-wasm-compiler` feature).
    /// - Anything else → treated as a precompiled artifact (`Component::deserialize`);
    ///   no Cranelift JIT needed, safe on Android.
    Bytes(Arc<Vec<u8>>),
}

impl Default for ChatRuntimeConfig {
    fn default() -> Self {
        Self {
            device_id: String::new(),
            request_timeout_secs: 120,
            max_auto_resumes: 16,
            backend: ChatRuntimeBackend::RemoteServer,
            tracing: ChatTracingConfig::default(),
        }
    }
}
