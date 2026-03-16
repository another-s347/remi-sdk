use std::pin::Pin;
use std::sync::Arc;

use base64::Engine as _;
use futures::{Stream, StreamExt as FuturesStreamExt};
use prost_types::{Struct as ProstStruct, Value as ProstValue};
use remi_agentloop::agent::Agent;
use remi_agentloop::protocol::ProtocolEvent;
use remi_agentloop::state::AgentState;
use remi_agentloop::types::{
    Content, ContentPart, FunctionCall, ImageUrlDetail, LoopInput, Message, MessageId, Role,
    ToolCallMessage, ToolCallOutcome,
};
use remi_agentloop_wasm::WasmAgentWithHttp;
use serde_json::{Value as JsonValue, json};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::chat_client::proto as chat_proto;
use crate::chat_client::{ChatStreamEvent, chat_stream_event};
use crate::chat_types::{ChatLocalWasmSource, ChatRuntimeBackend, ChatRuntimeConfig};
use crate::remi_uri::{RemiUri, RemiUriLocation, mime_from_extension};

pub(crate) type ChatEventStream =
    Pin<Box<dyn Stream<Item = Result<ChatStreamEvent, String>> + Send>>;

pub(crate) fn load_agent(source: &ChatLocalWasmSource) -> Result<Arc<WasmAgentWithHttp>, String> {
    let agent = match source {
        #[cfg(feature = "local-wasm-compiler")]
        ChatLocalWasmSource::File(path) => WasmAgentWithHttp::from_file(path),
        #[cfg(not(feature = "local-wasm-compiler"))]
        ChatLocalWasmSource::File(_) => {
            return Err(
                "File-based WASM loading requires the `local-wasm-compiler` feature".to_string(),
            );
        }
        // from_bytes auto-detects raw WASM vs precompiled .cwasm from magic bytes;
        // precompiled blobs work on all platforms (no compiler feature needed).
        ChatLocalWasmSource::Bytes(bytes) => WasmAgentWithHttp::from_bytes(bytes.as_slice()),
    }
    .map_err(|error| format!("Failed to load local WASM agent: {}", error.message))?;

    Ok(Arc::new(agent))
}

pub(crate) async fn start_stream(
    agent: Arc<WasmAgentWithHttp>,
    config: &ChatRuntimeConfig,
    session_id: &str,
    start_input: Option<chat_proto::ChatStartInput>,
    resume_input: Option<chat_proto::ChatResumeInput>,
) -> Result<ChatEventStream, String> {
    let loop_input = if let Some(start_input) = start_input {
        loop_input_from_start(start_input, config, session_id).await?
    } else if let Some(resume_input) = resume_input {
        loop_input_from_resume(resume_input)?
    } else {
        return Err("Missing chat start/resume input".to_string());
    };

    let stream = agent.clone();
    let (tx, rx) = mpsc::channel(64);

    tokio::spawn(async move {
        match stream.chat(loop_input).await {
            Ok(event_stream) => {
                let mut event_stream = std::pin::pin!(event_stream);
                while let Some(event) = event_stream.next().await {
                    tracing::debug!(
                        event_type = protocol_event_type_name(&event),
                        "[local_wasm] received guest protocol event"
                    );
                    if tx
                        .send(map_protocol_event_to_chat_stream_event(event))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
            Err(error) => {
                let _ = tx.send(Err(error.message)).await;
            }
        }
    });

    Ok(Box::pin(ReceiverStream::new(rx)))
}

async fn loop_input_from_start(
    input: chat_proto::ChatStartInput,
    config: &ChatRuntimeConfig,
    session_id: &str,
) -> Result<LoopInput, String> {
    let mut history = Vec::with_capacity(input.history.len());
    for message in input.history {
        history.push(history_message_from_proto(message).await?);
    }

    let current = input
        .current
        .ok_or_else(|| "ChatStartInput missing current message".to_string())?;

    let metadata = build_start_metadata(input.metadata.as_ref(), config, session_id)?;
    let model = match &config.backend {
        ChatRuntimeBackend::LocalWasm(local) => local.model.clone(),
        ChatRuntimeBackend::RemoteServer => None,
    };

    Ok(LoopInput::Start {
        content: content_from_input_message(current).await?,
        history,
        extra_tools: Vec::new(),
        model,
        temperature: None,
        max_tokens: None,
        metadata,
        message_metadata: None,
        user_name: None,
        user_state: None,
    })
}

fn loop_input_from_resume(input: chat_proto::ChatResumeInput) -> Result<LoopInput, String> {
    let state_json = input
        .state
        .as_ref()
        .map(prost_struct_to_json)
        .ok_or_else(|| "ChatResumeInput missing state".to_string())?;
    let state: AgentState = serde_json::from_value(state_json)
        .map_err(|error| format!("Invalid resume state for local WASM chat: {error}"))?;

    let mut results = Vec::with_capacity(input.results.len());
    for outcome in input.results {
        results.push(tool_outcome_from_proto(&outcome)?);
    }

    Ok(LoopInput::Resume { state, results })
}

async fn history_message_from_proto(
    message: chat_proto::ChatHistoryMessage,
) -> Result<Message, String> {
    Ok(Message {
        id: if message.id.trim().is_empty() {
            MessageId::new()
        } else {
            MessageId(message.id)
        },
        role: role_from_proto(&message.role)?,
        content: content_from_proto(message.content, message.content_parts).await?,
        tool_calls: tool_calls_from_proto(&message.tool_calls)?,
        tool_call_id: trim_to_option(message.tool_call_id),
        name: None,
        reasoning_content: trim_to_option(message.reasoning_content),
        metadata: None,
    })
}

async fn content_from_input_message(
    input: chat_proto::ChatInputMessage,
) -> Result<Content, String> {
    content_from_proto(input.content, input.content_parts).await
}

async fn content_from_proto(
    content: String,
    content_parts: Vec<chat_proto::ChatContentPart>,
) -> Result<Content, String> {
    if content_parts.is_empty() {
        return Ok(Content::Text(content));
    }

    let has_text_part = content_parts.iter().any(|part| {
        matches!(
            part.value.as_ref(),
            Some(chat_proto::chat_content_part::Value::Text(_))
        )
    });

    let mut resolved_parts = Vec::new();
    if !has_text_part && !content.trim().is_empty() {
        resolved_parts.push(ContentPart::Text { text: content });
    }

    for part in content_parts {
        let value = match part.value {
            Some(chat_proto::chat_content_part::Value::Text(text)) => ContentPart::Text { text },
            Some(chat_proto::chat_content_part::Value::ImageUrl(image)) => ContentPart::ImageUrl {
                image_url: ImageUrlDetail {
                    url: image.url,
                    detail: trim_to_option(image.detail),
                },
            },
            Some(chat_proto::chat_content_part::Value::ResourceUri(resource)) => {
                resolve_resource_part(&resource.uri, &resource.resource_type).await?
            }
            None => return Err("Chat content part missing value".to_string()),
        };
        resolved_parts.push(value);
    }

    Ok(Content::Parts(resolved_parts))
}

async fn resolve_resource_part(uri: &str, resource_type: &str) -> Result<ContentPart, String> {
    match resource_type {
        "" | "image" => Ok(ContentPart::ImageUrl {
            image_url: ImageUrlDetail {
                url: resolve_image_resource_uri(uri).await?,
                detail: None,
            },
        }),
        "url" => Ok(ContentPart::Text {
            text: format!("[Link: {}]", uri),
        }),
        other => Err(format!(
            "Unsupported local WASM chat resource type '{}' for URI {}",
            other, uri
        )),
    }
}

async fn resolve_image_resource_uri(uri: &str) -> Result<String, String> {
    if uri.starts_with("https://") || uri.starts_with("http://") || uri.starts_with("data:") {
        return Ok(uri.to_string());
    }

    if uri.starts_with("remi://") {
        let parsed = RemiUri::parse(uri).map_err(|error| error.to_string())?;
        return match parsed.location {
            RemiUriLocation::Remote => Ok(parsed.path),
            RemiUriLocation::File => file_to_data_url(&parsed.path, &parsed.mime_type).await,
            RemiUriLocation::Local => Err(format!(
                "remi://local image URIs are not supported by the generic SDK local WASM backend: {}",
                uri
            )),
            RemiUriLocation::Inline => Ok(parsed.path),
        };
    }

    let mime = mime_for_path(uri);
    file_to_data_url(uri, &mime).await
}

async fn file_to_data_url(path: &str, mime: &str) -> Result<String, String> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| format!("Failed to read local chat image {}: {}", path, error))?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    Ok(format!("data:{};base64,{}", mime, encoded))
}

fn mime_for_path(path: &str) -> String {
    let ext = path.rsplit('.').next().unwrap_or("jpg");
    mime_from_extension(ext).to_string()
}

fn role_from_proto(role: &str) -> Result<Role, String> {
    match role.trim().to_ascii_lowercase().as_str() {
        "" | "user" => Ok(Role::User),
        "system" => Ok(Role::System),
        "assistant" => Ok(Role::Assistant),
        "tool" => Ok(Role::Tool),
        other => Err(format!(
            "Unsupported chat role '{}' for local WASM backend",
            other
        )),
    }
}

fn tool_calls_from_proto(
    tool_calls: &[chat_proto::ChatToolCall],
) -> Result<Option<Vec<ToolCallMessage>>, String> {
    if tool_calls.is_empty() {
        return Ok(None);
    }

    let mut calls = Vec::with_capacity(tool_calls.len());
    for call in tool_calls {
        let arguments = call
            .arguments
            .as_ref()
            .map(prost_struct_to_json)
            .unwrap_or_else(|| json!({}));
        let arguments = serde_json::to_string(&arguments)
            .map_err(|error| format!("Failed to serialize tool call arguments: {error}"))?;

        calls.push(ToolCallMessage {
            id: call.id.clone(),
            call_type: "function".to_string(),
            function: FunctionCall {
                name: call.tool_name.clone(),
                arguments,
            },
        });
    }

    Ok(Some(calls))
}

fn tool_outcome_from_proto(
    outcome: &chat_proto::ChatToolCallOutcome,
) -> Result<ToolCallOutcome, String> {
    let tool_call_id = outcome.tool_call_id.clone();
    let tool_name = outcome.tool_name.clone();
    let outcome = outcome
        .outcome
        .as_ref()
        .ok_or_else(|| "ChatToolCallOutcome missing result/error".to_string())?;

    Ok(match outcome {
        chat_proto::chat_tool_call_outcome::Outcome::Result(result) => ToolCallOutcome::Result {
            tool_call_id,
            tool_name,
            content: Content::Text(result.clone()),
        },
        chat_proto::chat_tool_call_outcome::Outcome::Error(error) => ToolCallOutcome::Error {
            tool_call_id,
            tool_name,
            error: error.clone(),
        },
        chat_proto::chat_tool_call_outcome::Outcome::Parts(parts_msg) => {
            // Encode image parts as data: URLs inside ImageUrl content parts.
            // The Kimi/OpenAI-compatible API only accepts "image_url" content type;
            // the custom "image_base64" type is rejected with HTTP 400.
            let content_parts = parts_msg
                .parts
                .iter()
                .filter_map(|part| match &part.value {
                    Some(chat_proto::chat_content_part::Value::ImageUrl(img))
                        if !img.data.is_empty() =>
                    {
                        Some(ContentPart::ImageUrl {
                            image_url: ImageUrlDetail {
                                url: format!(
                                    "data:{};base64,{}",
                                    img.media_type,
                                    base64::engine::general_purpose::STANDARD.encode(&img.data)
                                ),
                                detail: None,
                            },
                        })
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            ToolCallOutcome::Result {
                tool_call_id,
                tool_name,
                content: if content_parts.is_empty() {
                    Content::Text(String::new())
                } else {
                    Content::Parts(content_parts)
                },
            }
        }
    })
}

fn build_start_metadata(
    existing: Option<&ProstStruct>,
    config: &ChatRuntimeConfig,
    session_id: &str,
) -> Result<Option<JsonValue>, String> {
    let mut metadata = match existing {
        Some(existing) => prost_struct_to_json(existing),
        None => JsonValue::Object(serde_json::Map::new()),
    };

    let metadata_obj = metadata
        .as_object_mut()
        .ok_or_else(|| "Chat start metadata must be a JSON object".to_string())?;

    metadata_obj
        .entry("session_id".to_string())
        .or_insert_with(|| JsonValue::String(session_id.to_string()));
    metadata_obj
        .entry("traffic_mark".to_string())
        .or_insert_with(|| JsonValue::String("local_wasm".to_string()));
    if !config.device_id.trim().is_empty() {
        metadata_obj
            .entry("device_id".to_string())
            .or_insert_with(|| JsonValue::String(config.device_id.clone()));
    }

    if let ChatRuntimeBackend::LocalWasm(local) = &config.backend {
        if !local.api_key.trim().is_empty() {
            metadata_obj
                .entry("api_key".to_string())
                .or_insert_with(|| JsonValue::String(local.api_key.clone()));
        }
        if let Some(base_url) = local
            .base_url
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            metadata_obj
                .entry("base_url".to_string())
                .or_insert_with(|| JsonValue::String(base_url.clone()));
        }
        if let Some(model) = local
            .model
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            metadata_obj
                .entry("model".to_string())
                .or_insert_with(|| JsonValue::String(model.clone()));
        }
    }

    if metadata_obj.is_empty() {
        Ok(None)
    } else {
        Ok(Some(metadata))
    }
}

fn map_protocol_event_to_chat_stream_event(
    event: ProtocolEvent,
) -> Result<ChatStreamEvent, String> {
    let mapped = match event {
        ProtocolEvent::RunStart {
            thread_id,
            run_id,
            metadata,
        } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::RunStart(
                chat_proto::ChatRunStartEvent {
                    thread_id,
                    run_id,
                    metadata: metadata.as_ref().and_then(json_to_prost_struct),
                },
            )),
        },
        ProtocolEvent::Delta { content, role } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::Delta(
                chat_proto::ChatDeltaEvent {
                    content,
                    role: role.unwrap_or_default(),
                },
            )),
        },
        ProtocolEvent::ThinkingStart => ChatStreamEvent {
            event: Some(chat_stream_event::Event::ThinkingStart(
                chat_proto::ChatThinkingStartEvent {},
            )),
        },
        ProtocolEvent::ThinkingEnd { content } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::ThinkingEnd(
                chat_proto::ChatThinkingEndEvent { content },
            )),
        },
        ProtocolEvent::ToolCallStart { id, name } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::ToolCallStart(
                chat_proto::ChatToolCallStartEvent {
                    id,
                    tool_name: name,
                },
            )),
        },
        ProtocolEvent::ToolCallDelta {
            id,
            arguments_delta,
        } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::ToolCallDelta(
                chat_proto::ChatToolCallDeltaEvent {
                    id,
                    arguments_delta,
                },
            )),
        },
        ProtocolEvent::ToolDelta { id, name, delta } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::ToolDelta(
                chat_proto::ChatToolDeltaEvent {
                    id,
                    tool_name: name,
                    delta,
                },
            )),
        },
        ProtocolEvent::ToolResult { id, name, result } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::ToolResult(
                chat_proto::ChatToolResultEvent {
                    id,
                    tool_name: name,
                    result,
                },
            )),
        },
        ProtocolEvent::Interrupt { interrupts } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::Interrupt(
                chat_proto::ChatInterruptStreamEvent {
                    interrupts: interrupts
                        .into_iter()
                        .map(|interrupt| chat_proto::ChatInterruptInfo {
                            interrupt_id: interrupt.interrupt_id.0,
                            tool_call_id: interrupt.tool_call_id,
                            tool_name: interrupt.tool_name,
                            kind: interrupt.kind,
                            data: json_to_prost_struct(&interrupt.data),
                        })
                        .collect(),
                },
            )),
        },
        ProtocolEvent::TurnStart { turn } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::TurnStart(
                chat_proto::ChatTurnStartEvent { turn: turn as u32 },
            )),
        },
        ProtocolEvent::Usage {
            prompt_tokens,
            completion_tokens,
        } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::Usage(
                chat_proto::ChatUsageEvent {
                    prompt_tokens,
                    completion_tokens,
                },
            )),
        },
        ProtocolEvent::Error { message, code } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::Error(
                chat_proto::ChatErrorEvent {
                    message,
                    code: code.unwrap_or_default(),
                },
            )),
        },
        ProtocolEvent::Done => ChatStreamEvent {
            event: Some(chat_stream_event::Event::Done(chat_proto::ChatDoneEvent {})),
        },
        ProtocolEvent::Cancelled => ChatStreamEvent {
            event: Some(chat_stream_event::Event::Cancelled(
                chat_proto::ChatCancelledEvent {},
            )),
        },
        ProtocolEvent::NeedToolExecution {
            state,
            tool_calls,
            completed_results,
        } => ChatStreamEvent {
            event: Some(chat_stream_event::Event::NeedToolExecution(
                chat_proto::ChatNeedToolExecutionEvent {
                    state: serde_json::to_value(&state)
                        .ok()
                        .as_ref()
                        .and_then(json_to_prost_struct),
                    tool_calls: tool_calls
                        .iter()
                        .map(|call| build_chat_tool_call(&call.id, &call.name, &call.arguments))
                        .collect(),
                    completed_results: completed_results
                        .iter()
                        .map(tool_outcome_to_proto)
                        .collect(),
                },
            )),
        },
        ProtocolEvent::Custom { event_type, extra } => {
            return map_custom_chat_event(event_type, extra);
        }
    };

    Ok(mapped)
}

fn map_custom_chat_event(event_type: String, extra: JsonValue) -> Result<ChatStreamEvent, String> {
    if event_type == "remi_agent" {
        match extra.get("type").and_then(|value| value.as_str()) {
            Some("need_tool_execution") => {
                let state = extra.get("state").cloned().unwrap_or(JsonValue::Null);
                let tool_calls = extra
                    .get("calls")
                    .and_then(|value| value.as_array())
                    .map(|calls| {
                        calls
                            .iter()
                            .filter_map(|call| {
                                let id = call.get("id").and_then(|value| value.as_str())?;
                                let typed_call = call.get("call")?;
                                let (tool_name, arguments) =
                                    normalize_external_tool_call(typed_call)?;
                                Some(build_chat_tool_call(id, &tool_name, &arguments))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let completed_results = extra
                    .get("completed_results")
                    .and_then(|value| value.as_array())
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(json_to_chat_tool_outcome)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                return Ok(ChatStreamEvent {
                    event: Some(chat_stream_event::Event::NeedToolExecution(
                        chat_proto::ChatNeedToolExecutionEvent {
                            state: json_to_prost_struct(&state),
                            tool_calls,
                            completed_results,
                        },
                    )),
                });
            }
            Some("done") => {
                return Ok(ChatStreamEvent {
                    event: Some(chat_stream_event::Event::Done(chat_proto::ChatDoneEvent {})),
                });
            }
            _ => {}
        }
    }

    Ok(ChatStreamEvent {
        event: Some(chat_stream_event::Event::Custom(
            chat_proto::ChatCustomEvent {
                event_type,
                extra: json_to_prost_struct(&extra),
            },
        )),
    })
}

fn normalize_external_tool_call(call: &JsonValue) -> Option<(String, JsonValue)> {
    let obj = call.as_object()?;

    if let Some(tool_name) = obj.get("tool_name").and_then(|value| value.as_str()) {
        return Some((
            tool_name.to_string(),
            obj.get("arguments").cloned().unwrap_or_else(|| json!({})),
        ));
    }

    if obj.len() != 1 {
        return None;
    }

    let (variant, arguments) = obj.iter().next()?;
    let tool_name = match variant.as_str() {
        "ListThings" => "list_things_tool",
        "GetThingMarkdown" => "get_things_tool",
        "AddThing" => "add_things_tool",
        "EditThing" => "edit_things_tool",
        "RemoveThing" => "remove_things_tool",
        "MoveThing" => "move_things_tool",
        "CreateTriggerSimple" => "create_trigger_simple",
        "CreateTrigger" => "create_trigger",
        "DeleteTrigger" => "delete_trigger",
        "TestTrigger" => "test_trigger",
        "ListTriggers" => "list_triggers_tool",
        "RetrieveEvents" => "retrieve_events",
        "AbstractEvents" => "abstract_events",
        "ResolveUri" => "resolve_uri",
        "HandoffToDeepAgent" => "handoff_to_deep_agent",
        _ => return None,
    };

    Some((tool_name.to_string(), arguments.clone()))
}

fn build_chat_tool_call(
    id: &str,
    tool_name: &str,
    arguments: &JsonValue,
) -> chat_proto::ChatToolCall {
    chat_proto::ChatToolCall {
        id: id.to_string(),
        tool_name: tool_name.to_string(),
        arguments: json_to_prost_struct(arguments),
    }
}

fn tool_outcome_to_proto(outcome: &ToolCallOutcome) -> chat_proto::ChatToolCallOutcome {
    match outcome {
        ToolCallOutcome::Result {
            tool_call_id,
            tool_name,
            content,
        } => chat_proto::ChatToolCallOutcome {
            tool_call_id: tool_call_id.clone(),
            tool_name: tool_name.clone(),
            outcome: Some(chat_proto::chat_tool_call_outcome::Outcome::Result(
                content.text_content(),
            )),
        },
        ToolCallOutcome::Error {
            tool_call_id,
            tool_name,
            error,
        } => chat_proto::ChatToolCallOutcome {
            tool_call_id: tool_call_id.clone(),
            tool_name: tool_name.clone(),
            outcome: Some(chat_proto::chat_tool_call_outcome::Outcome::Error(
                error.clone(),
            )),
        },
    }
}

fn json_to_chat_tool_outcome(value: &JsonValue) -> Option<chat_proto::ChatToolCallOutcome> {
    if let Some(result) = value.get("Result") {
        let result_value = result.get("content")?;
        let result_text = match result_value {
            JsonValue::String(text) => text.clone(),
            other => serde_json::to_string(other).ok()?,
        };
        return Some(chat_proto::ChatToolCallOutcome {
            tool_call_id: result.get("tool_call_id")?.as_str()?.to_string(),
            tool_name: result.get("tool_name")?.as_str()?.to_string(),
            outcome: Some(chat_proto::chat_tool_call_outcome::Outcome::Result(
                result_text,
            )),
        });
    }

    if let Some(error) = value.get("Error") {
        let error_value = error.get("error")?;
        let error_text = match error_value {
            JsonValue::String(text) => text.clone(),
            other => serde_json::to_string(other).ok()?,
        };
        return Some(chat_proto::ChatToolCallOutcome {
            tool_call_id: error.get("tool_call_id")?.as_str()?.to_string(),
            tool_name: error.get("tool_name")?.as_str()?.to_string(),
            outcome: Some(chat_proto::chat_tool_call_outcome::Outcome::Error(
                error_text,
            )),
        });
    }

    let tool_call_id = value.get("tool_call_id")?.as_str()?;
    let tool_name = value.get("tool_name")?.as_str()?;
    if let Some(result) = value.get("result") {
        let result_text = match result {
            JsonValue::String(text) => text.clone(),
            other => serde_json::to_string(other).ok()?,
        };
        return Some(chat_proto::ChatToolCallOutcome {
            tool_call_id: tool_call_id.to_string(),
            tool_name: tool_name.to_string(),
            outcome: Some(chat_proto::chat_tool_call_outcome::Outcome::Result(
                result_text,
            )),
        });
    }
    if let Some(error) = value.get("error") {
        let error_text = match error {
            JsonValue::String(text) => text.clone(),
            other => serde_json::to_string(other).ok()?,
        };
        return Some(chat_proto::ChatToolCallOutcome {
            tool_call_id: tool_call_id.to_string(),
            tool_name: tool_name.to_string(),
            outcome: Some(chat_proto::chat_tool_call_outcome::Outcome::Error(
                error_text,
            )),
        });
    }

    None
}

fn json_to_prost_struct(value: &JsonValue) -> Option<ProstStruct> {
    let JsonValue::Object(map) = value else {
        return None;
    };

    let fields = map
        .iter()
        .filter_map(|(key, value)| json_to_prost_value(value).map(|value| (key.clone(), value)))
        .collect();

    Some(ProstStruct { fields })
}

fn json_to_prost_value(value: &JsonValue) -> Option<ProstValue> {
    use prost_types::value::Kind;

    let kind = match value {
        JsonValue::Null => Kind::NullValue(0),
        JsonValue::Bool(value) => Kind::BoolValue(*value),
        JsonValue::Number(value) => Kind::NumberValue(value.as_f64()?),
        JsonValue::String(value) => Kind::StringValue(value.clone()),
        JsonValue::Array(values) => Kind::ListValue(prost_types::ListValue {
            values: values.iter().filter_map(json_to_prost_value).collect(),
        }),
        JsonValue::Object(_) => Kind::StructValue(json_to_prost_struct(value)?),
    };

    Some(ProstValue { kind: Some(kind) })
}

fn prost_struct_to_json(value: &ProstStruct) -> JsonValue {
    let mut map = serde_json::Map::new();
    for (key, value) in &value.fields {
        map.insert(key.clone(), prost_value_to_json(value));
    }
    JsonValue::Object(map)
}

fn prost_value_to_json(value: &ProstValue) -> JsonValue {
    use prost_types::value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullValue(_)) | None => JsonValue::Null,
        Some(Kind::BoolValue(value)) => JsonValue::Bool(*value),
        Some(Kind::StringValue(value)) => JsonValue::String(value.clone()),
        Some(Kind::NumberValue(value)) => {
            prost_number_to_json_value(*value).unwrap_or(JsonValue::Null)
        }
        Some(Kind::StructValue(value)) => prost_struct_to_json(value),
        Some(Kind::ListValue(list)) => {
            JsonValue::Array(list.values.iter().map(prost_value_to_json).collect())
        }
    }
}

fn prost_number_to_json_value(number: f64) -> Option<JsonValue> {
    if !number.is_finite() {
        return None;
    }

    const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
    if number.fract() == 0.0 && number.abs() <= MAX_SAFE_INTEGER {
        if number >= 0.0 {
            return Some(JsonValue::Number(serde_json::Number::from(number as u64)));
        }
        return Some(JsonValue::Number(serde_json::Number::from(number as i64)));
    }

    serde_json::Number::from_f64(number).map(JsonValue::Number)
}

fn trim_to_option(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat_types::{ChatLocalWasmConfig, ChatRuntimeBackend};

    #[test]
    fn start_metadata_injects_local_llm_fields() {
        let config = ChatRuntimeConfig {
            device_id: "device-1".to_string(),
            backend: ChatRuntimeBackend::LocalWasm(ChatLocalWasmConfig {
                source: ChatLocalWasmSource::Bytes(Arc::new(vec![])),
                api_key: "test-key".to_string(),
                base_url: Some("https://example.com/v1".to_string()),
                model: Some("gpt-test".to_string()),
            }),
            ..Default::default()
        };

        let metadata = build_start_metadata(None, &config, "session-123")
            .expect("metadata")
            .expect("metadata object");

        assert_eq!(
            metadata.get("api_key").and_then(|value| value.as_str()),
            Some("test-key")
        );
        assert_eq!(
            metadata.get("base_url").and_then(|value| value.as_str()),
            Some("https://example.com/v1")
        );
        assert_eq!(
            metadata.get("model").and_then(|value| value.as_str()),
            Some("gpt-test")
        );
        assert_eq!(
            metadata.get("device_id").and_then(|value| value.as_str()),
            Some("device-1")
        );
        assert_eq!(
            metadata.get("session_id").and_then(|value| value.as_str()),
            Some("session-123")
        );
        assert_eq!(
            metadata
                .get("traffic_mark")
                .and_then(|value| value.as_str()),
            Some("local_wasm")
        );
    }

    #[test]
    fn custom_need_tool_execution_maps_resolve_uri() {
        let event = ProtocolEvent::Custom {
            event_type: "remi_agent".to_string(),
            extra: json!({
                "type": "need_tool_execution",
                "state": { "phase": "AwaitingToolExecution" },
                "calls": [
                    {
                        "id": "tool-1",
                        "call": {
                            "ResolveUri": {
                                "uri": "https://example.com"
                            }
                        }
                    }
                ],
                "completed_results": []
            }),
        };

        let mapped = map_protocol_event_to_chat_stream_event(event).expect("mapped event");
        let Some(chat_stream_event::Event::NeedToolExecution(need_tool)) = mapped.event else {
            panic!("expected need_tool_execution event");
        };

        assert_eq!(need_tool.tool_calls.len(), 1);
        assert_eq!(need_tool.tool_calls[0].tool_name, "resolve_uri");
    }

    #[tokio::test]
    async fn content_conversion_avoids_duplicate_text_parts() {
        let content = content_from_proto(
            "hello".to_string(),
            vec![
                chat_proto::ChatContentPart {
                    r#type: "text".to_string(),
                    value: Some(chat_proto::chat_content_part::Value::Text(
                        "hello".to_string(),
                    )),
                },
                chat_proto::ChatContentPart {
                    r#type: "image_url".to_string(),
                    value: Some(chat_proto::chat_content_part::Value::ImageUrl(
                        chat_proto::ChatImageContent {
                            url: "https://example.com/image.png".to_string(),
                            detail: "auto".to_string(),
                        },
                    )),
                },
            ],
        )
        .await
        .expect("content");

        let Content::Parts(parts) = content else {
            panic!("expected multipart content");
        };

        assert_eq!(parts.len(), 2);
    }
}

fn protocol_event_type_name(event: &ProtocolEvent) -> &'static str {
    match event {
        ProtocolEvent::RunStart { .. } => "run_start",
        ProtocolEvent::Delta { .. } => "delta",
        ProtocolEvent::ThinkingStart => "thinking_start",
        ProtocolEvent::ThinkingEnd { .. } => "thinking_end",
        ProtocolEvent::ToolCallStart { .. } => "tool_call_start",
        ProtocolEvent::ToolCallDelta { .. } => "tool_call_delta",
        ProtocolEvent::ToolDelta { .. } => "tool_delta",
        ProtocolEvent::ToolResult { .. } => "tool_result",
        ProtocolEvent::Interrupt { .. } => "interrupt",
        ProtocolEvent::TurnStart { .. } => "turn_start",
        ProtocolEvent::Usage { .. } => "usage",
        ProtocolEvent::Error { .. } => "error",
        ProtocolEvent::Done => "done",
        ProtocolEvent::Cancelled => "cancelled",
        ProtocolEvent::NeedToolExecution { .. } => "need_tool_execution",
        ProtocolEvent::Custom { .. } => "custom",
    }
}
