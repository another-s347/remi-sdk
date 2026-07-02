use anyhow::{Context, Result};
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use super::TriggerSdk;
use crate::things_local::BootstrapStashedDocument;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BootstrapStashPayload {
    version: u8,
    documents: Vec<BootstrapStashedDocument>,
}

impl TriggerSdk {
    /// Get a single CRDT document by key (uuid + data_type).
    pub fn crdt_get_document(
        &self,
        uuid: &str,
        data_type: &str,
    ) -> Result<Option<crate::types::CrdtDocumentRow>> {
        self.storage.get_crdt_document(uuid, data_type)
    }

    #[cfg(test)]
    pub(crate) fn crdt_save_document(
        &self,
        uuid: &str,
        data_type: &str,
        automerge_doc: &[u8],
        sync_state: &[u8],
        dirty: bool,
        last_sync_at: Option<&str>,
    ) -> Result<()> {
        self.storage.save_crdt_document(
            uuid,
            data_type,
            automerge_doc,
            sync_state,
            dirty,
            last_sync_at,
        )
    }

    /// Get all dirty CRDT documents (for sync), ordered by sync priority.
    pub fn crdt_get_dirty_documents(&self) -> Result<Vec<crate::types::CrdtDocumentRow>> {
        self.storage.get_dirty_crdt_documents()
    }

    /// List all CRDT documents with payloads.
    pub fn crdt_list_documents(&self) -> Result<Vec<crate::types::CrdtDocumentRow>> {
        self.storage.list_crdt_documents()
    }

    /// List all CRDT document keys (uuid, data_type).
    pub fn crdt_list_document_keys(&self) -> Result<Vec<(String, String)>> {
        self.storage.list_crdt_document_keys()
    }

    pub fn things_bootstrap_stash_local_snapshot_if_needed(&self, device_id: &str) -> Result<bool> {
        const STASH_KEY: &str = "things.bootstrap.stash_snapshot_json";

        if self.storage.get_internal_kv(STASH_KEY)?.is_some() {
            tracing::debug!(
                device_id,
                "Bootstrap stash already exists; skipping new stash"
            );
            return Ok(false);
        }

        let dirty_documents = self
            .storage
            .get_dirty_crdt_documents()
            .context("Failed to load dirty CRDT documents for bootstrap stash")?;
        if dirty_documents.is_empty() {
            tracing::debug!(
                device_id,
                "No dirty CRDT documents found for bootstrap stash"
            );
            return Ok(false);
        }

        let dirty_document_keys: Vec<String> = dirty_documents
            .iter()
            .map(|doc| format!("{}:{}", doc.uuid, doc.data_type))
            .collect();

        let payload = serialize_bootstrap_stash_documents(&dirty_documents)
            .context("Failed to serialize bootstrap stash CRDT documents")?;
        self.storage
            .set_internal_kv(STASH_KEY, &payload)
            .context("Failed to persist bootstrap stash snapshot")?;
        tracing::info!(
            device_id,
            dirty_doc_count = dirty_document_keys.len(),
            dirty_document_keys = ?dirty_document_keys,
            "Persisted bootstrap stash from dirty CRDT documents"
        );
        Ok(true)
    }

    pub fn things_bootstrap_has_stash(&self) -> Result<bool> {
        const STASH_KEY: &str = "things.bootstrap.stash_snapshot_json";
        Ok(self.storage.get_internal_kv(STASH_KEY)?.is_some())
    }

    /// V3: Bootstrap by replaying stashed local changes.
    ///
    /// In V3 multi-document architecture, bootstrapping works differently:
    /// 1. Server sync is handled per-document, so there's no single "server snapshot"
    /// 2. Instead, we replay the stashed local snapshot to fresh documents
    /// 3. These documents will be marked dirty and synced on next sync cycle
    ///
    /// Note: The `_server_snapshot_doc` parameter is ignored in V3 - server data
    /// comes through per-document sync, not a single snapshot.
    pub fn things_bootstrap_from_server_snapshot_and_replay_stash(
        &self,
        device_id: &str,
        _server_snapshot_doc: Vec<u8>,
        _server_last_sync_at: Option<&str>,
    ) -> Result<()> {
        const STASH_KEY: &str = "things.bootstrap.stash_snapshot_json";
        const DONE_KEY: &str = "things.bootstrap.done";

        let stash_json = self
            .storage
            .get_internal_kv(STASH_KEY)?
            .ok_or_else(|| anyhow::anyhow!("No bootstrap stash found"))?;
        let documents = parse_bootstrap_replay_source(&stash_json)?;

        tracing::info!(
            device_id,
            "Replaying bootstrap stash onto fresh local documents"
        );

        let stashed_document_keys: Vec<String> = documents
            .iter()
            .map(|doc| format!("{}:{}", doc.uuid, doc.data_type))
            .collect();
        self.things_local_service()
            .restore_stashed_documents(device_id, &documents, true)
            .context("Failed to restore bootstrap-stashed CRDT documents")?;
        self.storage.set_internal_kv(DONE_KEY, "1")?;
        self.storage.delete_internal_kv(STASH_KEY)?;
        tracing::info!(
            device_id,
            stashed_doc_count = stashed_document_keys.len(),
            stashed_document_keys = ?stashed_document_keys,
            "Restored bootstrap-stashed CRDT documents onto fresh storage"
        );

        Ok(())
    }

    /// Replay stashed local changes onto the current V3 document set.
    ///
    /// Use this when the current storage already contains server documents pulled during
    /// first-sync bootstrap. Unlike `things_bootstrap_from_server_snapshot_and_replay_stash`,
    /// this preserves the pulled server state and layers the stashed local changes on top.
    pub fn things_bootstrap_replay_stash_onto_current_documents(
        &self,
        device_id: &str,
    ) -> Result<()> {
        const STASH_KEY: &str = "things.bootstrap.stash_snapshot_json";
        const DONE_KEY: &str = "things.bootstrap.done";

        let stash_json = self
            .storage
            .get_internal_kv(STASH_KEY)?
            .ok_or_else(|| anyhow::anyhow!("No bootstrap stash found"))?;
        let documents = parse_bootstrap_replay_source(&stash_json)?;
        let stashed_document_keys: Vec<String> = documents
            .iter()
            .map(|doc| format!("{}:{}", doc.uuid, doc.data_type))
            .collect();
        self.things_local_service()
            .merge_stashed_documents(device_id, &documents)
            .context("Failed to merge bootstrap-stashed CRDT documents")?;
        self.storage.set_internal_kv(DONE_KEY, "1")?;
        self.storage.delete_internal_kv(STASH_KEY)?;
        tracing::info!(
            device_id,
            stashed_doc_count = stashed_document_keys.len(),
            stashed_document_keys = ?stashed_document_keys,
            "Merged bootstrap-stashed CRDT documents onto current storage"
        );
        Ok(())
    }
}

fn serialize_bootstrap_stash_documents(
    dirty_documents: &[crate::types::CrdtDocumentRow],
) -> Result<String> {
    let payload = BootstrapStashPayload {
        version: 1,
        documents: dirty_documents
            .iter()
            .map(|row| BootstrapStashedDocument {
                uuid: row.uuid.clone(),
                data_type: row.data_type.clone(),
                automerge_doc_base64: base64::engine::general_purpose::STANDARD
                    .encode(&row.automerge_doc),
            })
            .collect(),
    };

    serde_json::to_string(&payload).context("Failed to encode bootstrap stash payload")
}

fn parse_bootstrap_replay_source(stash_json: &str) -> Result<Vec<BootstrapStashedDocument>> {
    let payload = serde_json::from_str::<BootstrapStashPayload>(stash_json)
        .context("Failed to parse bootstrap stash payload")?;
    Ok(payload.documents)
}
