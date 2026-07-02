use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time::timeout;
use tonic::Request;
use tonic::transport::Channel;

// Include generated proto code
pub mod proto {
    tonic::include_proto!("public_api.v1");
}

use proto::{
    GetChatSessionBundleUploadRequest, GetChatSessionBundleUploadResponse,
    ListChatSessionBundleUploadsRequest, ListChatSessionBundleUploadsResponse,
    UploadChatSessionBundleRequest, UploadChatSessionBundleResponse,
    public_service_client::PublicServiceClient,
};

/// Client for uploading local chat session feedback bundles.
pub struct ChatClient {
    client: PublicServiceClient<Channel>,
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
            bearer_token: bearer_token.into(),
            request_timeout,
            device_id: String::new(),
        })
    }

    /// Set the device identifier included in uploaded feedback bundles.
    pub fn with_device_id(mut self, device_id: String) -> Self {
        self.device_id = device_id;
        self
    }

    pub async fn upload_chat_session_bundle(
        &mut self,
        session_id: impl Into<String>,
        title: Option<String>,
        feedback_kind: impl Into<String>,
        bundle_json: String,
        metadata: Option<prost_types::Struct>,
    ) -> Result<UploadChatSessionBundleResponse> {
        let request = UploadChatSessionBundleRequest {
            session_id: session_id.into(),
            device_id: self.device_id.clone(),
            title: title.unwrap_or_default(),
            feedback_kind: feedback_kind.into(),
            bundle_json,
            metadata,
        };

        let request = self.add_auth_header(Request::new(request)).await?;
        let response = timeout(
            self.request_timeout,
            self.client.upload_chat_session_bundle(request),
        )
        .await
        .context("Upload chat session bundle timed out")??
        .into_inner();

        Ok(response)
    }

    pub async fn list_chat_session_bundle_uploads(
        &mut self,
        limit: i32,
        offset: i32,
    ) -> Result<ListChatSessionBundleUploadsResponse> {
        let request = Request::new(ListChatSessionBundleUploadsRequest { limit, offset });
        let request = self.add_auth_header(request).await?;
        let response = timeout(
            self.request_timeout,
            self.client.list_chat_session_bundle_uploads(request),
        )
        .await
        .context("List chat session bundle uploads timed out")??
        .into_inner();
        Ok(response)
    }

    pub async fn get_chat_session_bundle_upload(
        &mut self,
        upload_id: impl Into<String>,
    ) -> Result<GetChatSessionBundleUploadResponse> {
        let request = Request::new(GetChatSessionBundleUploadRequest {
            upload_id: upload_id.into(),
        });
        let request = self.add_auth_header(request).await?;
        let response = timeout(
            self.request_timeout,
            self.client.get_chat_session_bundle_upload(request),
        )
        .await
        .context("Get chat session bundle upload timed out")??
        .into_inner();
        Ok(response)
    }

    async fn add_auth_header<T>(&self, mut request: Request<T>) -> Result<Request<T>> {
        let bearer_token = crate::auth::auth_resolve_bearer_token(Some(&self.bearer_token))
            .await
            .ok_or_else(|| anyhow::anyhow!("Authentication bearer token is not configured"))?;

        crate::auth::auth_insert_bearer_header(&mut request, &bearer_token)
            .map_err(|err| anyhow::anyhow!(err))?;
        Ok(request)
    }
}
