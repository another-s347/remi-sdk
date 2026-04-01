use std::time::Duration;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::Request;
use tonic::transport::Channel;

// Include generated proto code
pub mod proto {
    tonic::include_proto!("public_api.v1");
}

use proto::{
    ChatInterruptRequest, ChatRequest, ChatResumeInput, ChatStartInput,
    public_service_client::PublicServiceClient,
};

pub use proto::{
    ChatHistoryMessage, ChatInputMessage, ChatStreamEvent, ChatToolCall, ChatToolCallOutcome,
    chat_request, chat_stream_event,
};

impl ChatStreamEvent {
    pub fn event_name(&self) -> &'static str {
        match self.event.as_ref() {
            Some(chat_stream_event::Event::RunStart(_)) => "run_start",
            Some(chat_stream_event::Event::Delta(_)) => "delta",
            Some(chat_stream_event::Event::ThinkingStart(_)) => "thinking_start",
            Some(chat_stream_event::Event::ThinkingEnd(_)) => "thinking_end",
            Some(chat_stream_event::Event::ToolCallStart(_)) => "tool_call_start",
            Some(chat_stream_event::Event::ToolCallDelta(_)) => "tool_call_delta",
            Some(chat_stream_event::Event::ToolDelta(_)) => "tool_delta",
            Some(chat_stream_event::Event::ToolResult(_)) => "tool_result",
            Some(chat_stream_event::Event::Interrupt(_)) => "interrupt",
            Some(chat_stream_event::Event::TurnStart(_)) => "turn_start",
            Some(chat_stream_event::Event::Usage(_)) => "usage",
            Some(chat_stream_event::Event::Error(_)) => "error",
            Some(chat_stream_event::Event::Done(_)) => "done",
            Some(chat_stream_event::Event::Cancelled(_)) => "cancelled",
            Some(chat_stream_event::Event::NeedToolExecution(_)) => "need_tool_execution",
            Some(chat_stream_event::Event::Custom(_)) => "custom",
            None => "unknown",
        }
    }
}

/// Client for chat interactions with the agent service.
pub struct ChatClient {
    client: PublicServiceClient<Channel>,
    shared_transport: Option<Arc<crate::transport::SharedTransport>>,
    bearer_token: String,
    request_timeout: Duration,
    device_id: String,
}

impl ChatClient {
    /// Create a new chat client.
    pub async fn new(
        server_url: impl Into<String>,
        bearer_token: impl Into<String>,
    ) -> Result<Self> {
        let channel = Channel::from_shared(server_url.into())
            .context("Invalid server URL")?
            .connect()
            .await
            .context("Failed to connect to server")?;

        let client = PublicServiceClient::new(channel);

        Ok(Self {
            client,
            shared_transport: None,
            bearer_token: bearer_token.into(),
            request_timeout: Duration::from_secs(120),
            device_id: String::new(),
        })
    }

    /// Create a chat client that reuses the shared transport configured for auth/telemetry.
    pub async fn new_with_shared_transport(bearer_token: impl Into<String>) -> Result<Self> {
        let transport =
            crate::transport::get_shared_transport().map_err(|err| anyhow::anyhow!(err))?;
        let request_timeout = Duration::from_secs(120);
        let channel = transport
            .get_channel()
            .await
            .map_err(|err| anyhow::anyhow!(err))?;

        let client = PublicServiceClient::new(channel);

        Ok(Self {
            client,
            shared_transport: Some(transport),
            bearer_token: bearer_token.into(),
            request_timeout,
            device_id: String::new(),
        })
    }

    /// Set the device identifier to be forwarded to the agent.
    pub fn with_device_id(mut self, device_id: String) -> Self {
        self.device_id = device_id;
        self
    }

    /// Collect the full typed chat stream.
    pub async fn chat_stream(
        &mut self,
        session_id: impl Into<String>,
        start: Option<ChatStartInput>,
        resume: Option<ChatResumeInput>,
    ) -> Result<Vec<ChatStreamEvent>> {
        let session_id = session_id.into();
        let response = self
            .start_chat_with_retry(session_id, start, resume)
            .await?;

        let mut stream = response.into_inner();
        let mut items = Vec::new();
        while let Some(item) = stream
            .message()
            .await
            .context("Failed to receive chunk from stream")?
        {
            items.push(item);
        }

        Ok(items)
    }

    /// Interrupt a running chat conversation.
    pub async fn interrupt(&mut self, session_id: impl Into<String>) -> Result<bool> {
        let session_id = session_id.into();
        let mut retried_after_reconnect = false;

        let response = loop {
            let request = Request::new(ChatInterruptRequest {
                session_id: session_id.clone(),
            });
            let request = self.add_auth_header(request).await?;

            match timeout(self.request_timeout, self.client.chat_interrupt(request)).await {
                Ok(Ok(response)) => break response,
                Ok(Err(status)) => {
                    if !retried_after_reconnect
                        && crate::transport::is_recoverable_transport_status(&status)
                        && self.reconnect_shared_transport("chat_interrupt", status.to_string()).await?
                    {
                        retried_after_reconnect = true;
                        continue;
                    }

                    return Err(anyhow::Error::new(status).context("Failed to interrupt chat"));
                }
                Err(_) => {
                    if !retried_after_reconnect
                        && self
                            .reconnect_shared_transport(
                                "chat_interrupt_timeout",
                                "Interrupt request timed out".to_string(),
                            )
                            .await?
                    {
                        retried_after_reconnect = true;
                        continue;
                    }

                    anyhow::bail!("Interrupt request timed out");
                }
            }
        };

        Ok(response.into_inner().success)
    }

    /// Stream typed chat events as they arrive.
    pub async fn chat_streaming(
        &mut self,
        session_id: impl Into<String>,
        start: Option<ChatStartInput>,
        resume: Option<ChatResumeInput>,
    ) -> Result<impl Stream<Item = Result<ChatStreamEvent>> + Send + 'static> {
        let session_id = session_id.into();
        let response = self
            .start_chat_with_retry(session_id, start, resume)
            .await?;

        let mut grpc_stream = response.into_inner();
        let (tx, rx) = mpsc::channel::<Result<ChatStreamEvent>>(64);
        let shared_transport = self.shared_transport.clone();

        tokio::spawn(async move {
            loop {
                match grpc_stream.message().await {
                    Ok(Some(item)) => {
                        if tx.send(Ok(item)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        if crate::transport::is_recoverable_transport_status(&e) {
                            if let Some(transport) = shared_transport.as_ref() {
                                transport.invalidate_channel().await;
                            }
                        }
                        let _ = tx.send(Err(anyhow::anyhow!("Stream error: {}", e))).await;
                        break;
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    /// Backward-compatible name retained for the multimodal start path.
    pub async fn chat_streaming_multimodal(
        &mut self,
        session_id: impl Into<String>,
        start: Option<ChatStartInput>,
        resume: Option<ChatResumeInput>,
    ) -> Result<impl Stream<Item = Result<ChatStreamEvent>> + Send + 'static> {
        self.chat_streaming(session_id, start, resume).await
    }

    fn build_chat_request(
        &self,
        session_id: String,
        start: Option<ChatStartInput>,
        resume: Option<ChatResumeInput>,
    ) -> Result<ChatRequest> {
        let input = match (start, resume) {
            (Some(start), None) => Some(chat_request::Input::Start(start)),
            (None, Some(resume)) => Some(chat_request::Input::Resume(resume)),
            (Some(_), Some(_)) => anyhow::bail!("Provide either start or resume, not both"),
            (None, None) => anyhow::bail!("Either start or resume is required"),
        };

        Ok(ChatRequest {
            session_id,
            device_id: self.device_id.clone(),
            input,
        })
    }

    async fn add_auth_header<T>(&self, mut request: Request<T>) -> Result<Request<T>> {
        let bearer_token = crate::auth::auth_resolve_bearer_token(Some(&self.bearer_token))
            .await
            .ok_or_else(|| anyhow::anyhow!("Authentication bearer token is not configured"))?;

        crate::auth::auth_insert_bearer_header(&mut request, &bearer_token)
            .map_err(|err| anyhow::anyhow!(err))?;
        Ok(request)
    }

    async fn start_chat_with_retry(
        &mut self,
        session_id: String,
        start: Option<ChatStartInput>,
        resume: Option<ChatResumeInput>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<ChatStreamEvent>>> {
        let mut retried_after_reconnect = false;

        loop {
            let request = self.build_chat_request(session_id.clone(), start.clone(), resume.clone())?;
            let request = self.add_auth_header(Request::new(request)).await?;

            match timeout(self.request_timeout, self.client.chat(request)).await {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(status)) => {
                    if !retried_after_reconnect
                        && crate::transport::is_recoverable_transport_status(&status)
                        && self.reconnect_shared_transport("chat", status.to_string()).await?
                    {
                        retried_after_reconnect = true;
                        continue;
                    }

                    return Err(anyhow::Error::new(status).context("Failed to initiate chat stream"));
                }
                Err(_) => {
                    if !retried_after_reconnect
                        && self
                            .reconnect_shared_transport("chat_timeout", "Request timed out".to_string())
                            .await?
                    {
                        retried_after_reconnect = true;
                        continue;
                    }

                    anyhow::bail!("Request timed out");
                }
            }
        }
    }

    async fn reconnect_shared_transport(&mut self, operation: &str, reason: String) -> Result<bool> {
        let Some(transport) = self.shared_transport.clone() else {
            return Ok(false);
        };

        tracing::warn!(operation, reason = %reason, "[chat_client] invalidating shared transport after recoverable error");
        transport.invalidate_channel().await;
        let channel = transport
            .get_channel()
            .await
            .map_err(|err| anyhow::anyhow!(err))?;
        self.client = PublicServiceClient::new(channel);
        Ok(true)
    }
}
