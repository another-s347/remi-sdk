use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::time::timeout;
use tonic::Request;
use tonic::transport::Channel;

// Include generated proto code
pub mod proto {
    tonic::include_proto!("public_api.v1");
}

use proto::{
    CrdtDocumentRef, GetCrdtDocumentSnapshotRequest, GetCrdtDocumentSnapshotsRequest,
    ListCrdtDocumentKeysRequest, SyncCrdtDocumentInput, SyncCrdtDocumentRequest,
    SyncCrdtDocumentsRequest, public_service_client::PublicServiceClient,
};

const MAX_GRPC_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

fn configured_public_service_client(channel: Channel) -> PublicServiceClient<Channel> {
    PublicServiceClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES)
}

/// Public client for server-backed Remi APIs that are not covered by specialized clients.
pub struct RemiPublicClient {
    client: PublicServiceClient<Channel>,
    bearer_token: String,
    request_timeout: Duration,
}

impl RemiPublicClient {
    /// Create a new public API client
    pub async fn new(
        server_url: impl Into<String>,
        bearer_token: impl Into<String>,
    ) -> Result<Self> {
        crate::transport::configure_legacy_tcp_transport(server_url.into())
            .await
            .map_err(anyhow::Error::msg)?;
        Self::new_with_shared_transport(bearer_token).await
    }

    /// Create a public API client that reuses the shared transport configured for auth/telemetry
    pub async fn new_with_shared_transport(bearer_token: impl Into<String>) -> Result<Self> {
        let transport =
            crate::transport::get_shared_transport().map_err(|err| anyhow::anyhow!(err))?;
        let request_timeout = transport.request_timeout();
        let channel = transport
            .get_channel()
            .await
            .map_err(|err| anyhow::anyhow!(err))?;

        let client = configured_public_service_client(channel);

        Ok(Self {
            client,
            bearer_token: bearer_token.into(),
            request_timeout,
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
}

#[derive(Debug, Clone)]
pub struct ServerCrdtDocumentKey {
    pub document_uuid: String,
    pub data_type: i32,
    pub canonical_head: Vec<u8>,
}

#[async_trait]
pub trait CrdtSyncTransport: Send {
    async fn sync_crdt_document(
        &mut self,
        device_id: String,
        document_uuid: String,
        data_type: i32,
        sync_message: Vec<u8>,
    ) -> Result<(Vec<Vec<u8>>, String)>;

    async fn sync_crdt_documents(
        &mut self,
        device_id: String,
        documents: Vec<(String, i32, Vec<u8>)>,
    ) -> Result<Vec<(String, i32, Vec<Vec<u8>>, String)>> {
        let mut responses = Vec::with_capacity(documents.len());
        for (document_uuid, data_type, sync_message) in documents {
            let (sync_messages, last_sync_at) = self
                .sync_crdt_document(
                    device_id.clone(),
                    document_uuid.clone(),
                    data_type,
                    sync_message,
                )
                .await?;
            responses.push((document_uuid, data_type, sync_messages, last_sync_at));
        }
        Ok(responses)
    }

    async fn get_crdt_document_snapshot(
        &mut self,
        device_id: String,
        document_uuid: String,
        data_type: i32,
        reset_sync_state: bool,
    ) -> Result<(Vec<u8>, String)>;

    async fn get_crdt_document_snapshots(
        &mut self,
        device_id: String,
        documents: Vec<(String, i32)>,
        reset_sync_state: bool,
    ) -> Result<Vec<(String, i32, Vec<u8>, String)>> {
        let mut snapshots = Vec::with_capacity(documents.len());
        for (document_uuid, data_type) in documents {
            let (automerge_doc, last_sync_at) = self
                .get_crdt_document_snapshot(
                    device_id.clone(),
                    document_uuid.clone(),
                    data_type,
                    reset_sync_state,
                )
                .await?;
            snapshots.push((document_uuid, data_type, automerge_doc, last_sync_at));
        }
        Ok(snapshots)
    }

    async fn list_crdt_document_keys(&mut self) -> Result<Vec<ServerCrdtDocumentKey>>;
}

#[async_trait]
impl CrdtSyncTransport for RemiPublicClient {
    async fn sync_crdt_document(
        &mut self,
        device_id: String,
        document_uuid: String,
        data_type: i32,
        sync_message: Vec<u8>,
    ) -> Result<(Vec<Vec<u8>>, String)> {
        RemiPublicClient::sync_crdt_document(
            self,
            device_id,
            document_uuid,
            data_type,
            sync_message,
        )
        .await
    }

    async fn sync_crdt_documents(
        &mut self,
        device_id: String,
        documents: Vec<(String, i32, Vec<u8>)>,
    ) -> Result<Vec<(String, i32, Vec<Vec<u8>>, String)>> {
        RemiPublicClient::sync_crdt_documents(self, device_id, documents).await
    }

    async fn get_crdt_document_snapshot(
        &mut self,
        device_id: String,
        document_uuid: String,
        data_type: i32,
        reset_sync_state: bool,
    ) -> Result<(Vec<u8>, String)> {
        RemiPublicClient::get_crdt_document_snapshot(
            self,
            device_id,
            document_uuid,
            data_type,
            reset_sync_state,
        )
        .await
    }

    async fn get_crdt_document_snapshots(
        &mut self,
        device_id: String,
        documents: Vec<(String, i32)>,
        reset_sync_state: bool,
    ) -> Result<Vec<(String, i32, Vec<u8>, String)>> {
        RemiPublicClient::get_crdt_document_snapshots(self, device_id, documents, reset_sync_state)
            .await
    }

    async fn list_crdt_document_keys(&mut self) -> Result<Vec<ServerCrdtDocumentKey>> {
        RemiPublicClient::list_crdt_document_keys(self).await
    }
}

impl RemiPublicClient {
    // ========== CRDT V3 Multi-Document Sync ==========

    /// Sync a single CRDT document with the server.
    pub async fn sync_crdt_document(
        &mut self,
        device_id: String,
        document_uuid: String,
        data_type: i32,
        sync_message: Vec<u8>,
    ) -> Result<(Vec<Vec<u8>>, String)> {
        let request = Request::new(SyncCrdtDocumentRequest {
            device_id,
            document_uuid,
            data_type,
            sync_message,
        });

        let request = self.add_auth_header(request).await?;

        let response = timeout(
            self.request_timeout,
            self.client.sync_crdt_document(request),
        )
        .await
        .context("Sync CRDT document timed out")??
        .into_inner();

        Ok((response.sync_messages, response.last_sync_at))
    }

    /// Sync multiple CRDT documents with the server in a single roundtrip.
    pub async fn sync_crdt_documents(
        &mut self,
        device_id: String,
        documents: Vec<(String, i32, Vec<u8>)>,
    ) -> Result<Vec<(String, i32, Vec<Vec<u8>>, String)>> {
        let request = Request::new(SyncCrdtDocumentsRequest {
            device_id,
            documents: documents
                .into_iter()
                .map(
                    |(document_uuid, data_type, sync_message)| SyncCrdtDocumentInput {
                        document_uuid,
                        data_type,
                        sync_message,
                    },
                )
                .collect(),
        });

        let request = self.add_auth_header(request).await?;

        let response = timeout(
            self.request_timeout,
            self.client.sync_crdt_documents(request),
        )
        .await
        .context("Sync CRDT documents timed out")??
        .into_inner();

        Ok(response
            .documents
            .into_iter()
            .map(|document| {
                (
                    document.document_uuid,
                    document.data_type,
                    document.sync_messages,
                    document.last_sync_at,
                )
            })
            .collect())
    }

    /// Fetch the latest CRDT document snapshot (for bootstrap).
    pub async fn get_crdt_document_snapshot(
        &mut self,
        device_id: String,
        document_uuid: String,
        data_type: i32,
        reset_sync_state: bool,
    ) -> Result<(Vec<u8>, String)> {
        let request = Request::new(GetCrdtDocumentSnapshotRequest {
            device_id,
            document_uuid,
            data_type,
            reset_sync_state,
        });

        let request = self.add_auth_header(request).await?;

        let response = timeout(
            self.request_timeout,
            self.client.get_crdt_document_snapshot(request),
        )
        .await
        .context("Get CRDT document snapshot timed out")??
        .into_inner();

        Ok((response.automerge_doc, response.last_sync_at))
    }

    /// Fetch multiple CRDT document snapshots in one request.
    pub async fn get_crdt_document_snapshots(
        &mut self,
        device_id: String,
        documents: Vec<(String, i32)>,
        reset_sync_state: bool,
    ) -> Result<Vec<(String, i32, Vec<u8>, String)>> {
        let request = Request::new(GetCrdtDocumentSnapshotsRequest {
            device_id,
            documents: documents
                .into_iter()
                .map(|(document_uuid, data_type)| CrdtDocumentRef {
                    document_uuid,
                    data_type,
                })
                .collect(),
            reset_sync_state,
        });

        let request = self.add_auth_header(request).await?;

        let response = timeout(
            self.request_timeout,
            self.client.get_crdt_document_snapshots(request),
        )
        .await
        .context("Get CRDT document snapshots timed out")??
        .into_inner();

        Ok(response
            .snapshots
            .into_iter()
            .map(|snapshot| {
                (
                    snapshot.document_uuid,
                    snapshot.data_type,
                    snapshot.automerge_doc,
                    snapshot.last_sync_at,
                )
            })
            .collect())
    }

    /// List all CRDT document keys for the user, including the current canonical server head.
    pub async fn list_crdt_document_keys(&mut self) -> Result<Vec<ServerCrdtDocumentKey>> {
        let request = Request::new(ListCrdtDocumentKeysRequest {});

        let request = self.add_auth_header(request).await?;

        let response = timeout(
            self.request_timeout,
            self.client.list_crdt_document_keys(request),
        )
        .await
        .context("List CRDT document keys timed out")??
        .into_inner();

        Ok(response
            .keys
            .into_iter()
            .map(|k| ServerCrdtDocumentKey {
                document_uuid: k.document_uuid,
                data_type: k.data_type,
                canonical_head: k.canonical_head,
            })
            .collect())
    }
}
