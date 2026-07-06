use std::time::Instant;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::RemiPublicClient;
use crate::RemiSdk;
use crate::crdt_sync;
use crate::public_client::{CrdtSyncTransport, ServerCrdtDocumentKey};
use crate::things_crdt::{
    DocumentKey, DocumentPersistence, DocumentState, ThingsDocumentSet, ThingsSyncSummary,
    parse_optional_domain_datetime,
};

use remi_things_crdt::CrdtDataType;

mod pull;
mod session;
use pull::{
    LocalReachabilityFilters, build_local_reachability_filters, clean_document_should_receive,
    pull_missing_documents,
};
use session::sync_document_rows_batch;

struct ThingsSyncOutput {
    pub doc_bytes: Vec<u8>,
    pub sync_state_bytes: Vec<u8>,
    pub last_sync_at: Option<String>,
    pub rpc_rounds: usize,
    pub server_reply_messages: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalBootstrapState {
    Empty,
    DirtyUnsynced,
    HasSyncedHistory,
}

// ============================================================================
// V3 Multi-Document Batched Sync
// ============================================================================

/// Output from v3 batched sync
#[derive(Debug, Clone, Serialize)]
pub struct ThingsV3SyncMetrics {
    pub total_elapsed_ms: u64,
    pub bootstrap_pull_ms: u64,
    pub phase1_push_ms: u64,
    pub phase1b_receive_ms: u64,
    pub phase2_pull_ms: u64,
    pub list_keys_calls: usize,
    pub snapshot_downloads: usize,
    pub phase1_documents_synced: usize,
    pub phase1b_documents_synced: usize,
    pub phase2_documents_synced: usize,
    pub phase1_rpc_rounds: usize,
    pub phase1b_rpc_rounds: usize,
    pub phase1_batch_calls: usize,
    pub phase1b_batch_calls: usize,
    pub phase1_server_reply_messages: usize,
    pub phase1b_server_reply_messages: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ThingsV3SyncOutput {
    /// Number of documents synced
    pub documents_synced: usize,
    pub documents_pushed: usize,
    pub documents_pulled: usize,
    /// Last sync timestamp (from server)
    pub last_sync_at: Option<String>,
    pub generated_event_range: Option<(i64, i64)>,
    /// Timing and work-distribution metrics for the sync run.
    pub metrics: ThingsV3SyncMetrics,
}

impl ThingsV3SyncOutput {
    pub fn summary(&self) -> ThingsSyncSummary {
        ThingsSyncSummary::from(self)
    }
}

impl From<&ThingsV3SyncOutput> for ThingsSyncSummary {
    fn from(output: &ThingsV3SyncOutput) -> Self {
        Self {
            documents_synced: output.documents_synced,
            documents_pushed: output.documents_pushed,
            documents_pulled: output.documents_pulled,
            last_sync_at: parse_optional_domain_datetime(output.last_sync_at.as_deref())
                .unwrap_or(None),
            generated_event_range: output.generated_event_range,
        }
    }
}

impl From<ThingsV3SyncOutput> for ThingsSyncSummary {
    fn from(output: ThingsV3SyncOutput) -> Self {
        Self {
            documents_synced: output.documents_synced,
            documents_pushed: output.documents_pushed,
            documents_pulled: output.documents_pulled,
            last_sync_at: parse_optional_domain_datetime(output.last_sync_at.as_deref())
                .unwrap_or(None),
            generated_event_range: output.generated_event_range,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThingsSyncMode {
    Incremental,
    Full,
}

impl ThingsSyncMode {
    fn allows_server_discovery(self) -> bool {
        matches!(self, Self::Full)
    }
}

pub async fn sync_things_full(
    sdk: &RemiSdk,
    client: &mut RemiPublicClient,
    device_id: &str,
) -> Result<ThingsV3SyncOutput> {
    sync_v3_documents_with_transport_mode(sdk, client, device_id, ThingsSyncMode::Full).await
}

pub async fn sync_things_incremental(
    sdk: &RemiSdk,
    client: &mut RemiPublicClient,
    device_id: &str,
) -> Result<ThingsV3SyncOutput> {
    sync_v3_documents_with_transport_mode(sdk, client, device_id, ThingsSyncMode::Incremental).await
}

/// Convert CrdtDataType to proto enum value
fn data_type_to_proto(data_type: &CrdtDataType) -> i32 {
    match data_type {
        CrdtDataType::Root => 1,
        CrdtDataType::Collection => 2,
        CrdtDataType::ThingMarkdown => 3,
    }
}

fn parse_row_data_type(data_type: &str) -> Result<CrdtDataType> {
    match data_type {
        "root" => Ok(CrdtDataType::Root),
        "collection" => Ok(CrdtDataType::Collection),
        "thing_markdown" => Ok(CrdtDataType::ThingMarkdown),
        _ => anyhow::bail!("Unknown CRDT data type: {data_type}"),
    }
}

fn document_key_from_storage(uuid: &str, data_type: &str) -> Result<DocumentKey> {
    Ok(DocumentKey {
        uuid: uuid.to_string(),
        data_type: parse_row_data_type(data_type)?,
    })
}

fn merge_event_range(current: &mut Option<(i64, i64)>, next: Option<(i64, i64)>) {
    let Some((next_first, next_last)) = next else {
        return;
    };
    match current {
        Some((first, last)) => {
            *first = (*first).min(next_first);
            *last = (*last).max(next_last);
        }
        None => *current = Some((next_first, next_last)),
    }
}

fn local_canonical_head(doc_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut doc = automerge::AutoCommit::load(doc_bytes).ok()?;
    let heads = doc.get_heads();
    if heads.len() != 1 {
        return None;
    }

    Some(heads[0].as_ref().to_vec())
}

fn document_is_at_server_head(doc_bytes: &[u8], server_head: &[u8]) -> bool {
    if server_head.is_empty() {
        return false;
    }

    local_canonical_head(doc_bytes)
        .as_deref()
        .map(|local_head| local_head == server_head)
        .unwrap_or(false)
}

fn build_server_head_map(
    server_keys: &[ServerCrdtDocumentKey],
) -> std::collections::HashMap<(String, String), Vec<u8>> {
    let mut out = std::collections::HashMap::new();

    for key in server_keys {
        let dt_str = proto_data_type_to_str(key.data_type);
        if dt_str.is_empty() || key.canonical_head.is_empty() {
            continue;
        }

        out.insert(
            (key.document_uuid.clone(), dt_str.to_string()),
            key.canonical_head.clone(),
        );
    }

    out
}

fn has_sync_history(doc: &crate::types::CrdtDocumentRow) -> bool {
    let initial_sync_state = crate::crdt_sync::init_sync_state();
    !doc.sync_state.is_empty() && doc.sync_state != initial_sync_state
}

fn optional_sync_timestamp(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn observe_sync_timestamp(observed: &mut Option<String>, candidate: Option<String>) {
    if let Some(ts) = candidate {
        *observed = Some(ts);
    }
}

fn classify_local_bootstrap_state(
    sdk: &RemiSdk,
    dirty_docs: &[crate::types::CrdtDocumentRow],
) -> LocalBootstrapState {
    if dirty_docs.iter().any(has_sync_history) {
        return LocalBootstrapState::HasSyncedHistory;
    }

    let all_keys = sdk.crdt_list_document_keys().unwrap_or_default();
    if all_keys.is_empty() {
        return LocalBootstrapState::Empty;
    }

    let has_synced_history = all_keys.iter().any(|(uuid, dt)| {
        sdk.crdt_get_document(uuid, dt)
            .ok()
            .flatten()
            .map(|doc| has_sync_history(&doc))
            .unwrap_or(false)
    });

    if has_synced_history {
        LocalBootstrapState::HasSyncedHistory
    } else {
        LocalBootstrapState::DirtyUnsynced
    }
}

fn never_synced_dirty_keys(dirty_docs: &[crate::types::CrdtDocumentRow]) -> Vec<(String, String)> {
    dirty_docs
        .iter()
        .filter(|doc| !has_sync_history(doc))
        .map(|doc| (doc.uuid.clone(), doc.data_type.clone()))
        .collect()
}

enum ServerKeyDiscovery {
    Available(Vec<ServerCrdtDocumentKey>),
    Unavailable,
}

impl ServerKeyDiscovery {
    fn keys(&self) -> Option<&[ServerCrdtDocumentKey]> {
        match self {
            Self::Available(keys) => Some(keys),
            Self::Unavailable => None,
        }
    }
}

/// Sync all dirty v3 CRDT documents with the server in priority order,
/// then pull any server-side documents that are missing locally.
///
/// Phase 1 (push): Dirty documents synced in order: Root → Collections → ThingMarkdown.
/// Phase 1b (receive): Existing local documents whose canonical head differs from the
///   server's current head are re-synced to receive changes made by other devices.
///   When head metadata is unavailable, this falls back to the previous full receive sync.
/// Phase 2 (pull): Discover server-side documents via `list_crdt_document_keys`,
///   download any missing ones via `get_crdt_document_snapshot`, then sync them
///   through the Automerge protocol so both sides share a sync state.
pub async fn sync_v3_documents_with_server(
    sdk: &RemiSdk,
    client: &mut RemiPublicClient,
    device_id: &str,
) -> Result<ThingsV3SyncOutput> {
    sync_v3_documents_with_transport_mode(sdk, client, device_id, ThingsSyncMode::Full).await
}

pub async fn sync_v3_documents_with_server_mode(
    sdk: &RemiSdk,
    client: &mut RemiPublicClient,
    device_id: &str,
    mode: ThingsSyncMode,
) -> Result<ThingsV3SyncOutput> {
    sync_v3_documents_with_transport_mode(sdk, client, device_id, mode).await
}

pub async fn sync_v3_documents_with_transport<T>(
    sdk: &RemiSdk,
    client: &mut T,
    device_id: &str,
) -> Result<ThingsV3SyncOutput>
where
    T: CrdtSyncTransport,
{
    sync_v3_documents_with_transport_mode(sdk, client, device_id, ThingsSyncMode::Full).await
}

pub async fn sync_v3_documents_with_transport_mode<T>(
    sdk: &RemiSdk,
    client: &mut T,
    device_id: &str,
    mode: ThingsSyncMode,
) -> Result<ThingsV3SyncOutput>
where
    T: CrdtSyncTransport,
{
    let total_started_at = Instant::now();
    let mut documents_synced = 0;
    let mut last_sync_at: Option<String> = None;
    let sync_run_id = uuid::Uuid::new_v4().to_string();
    let mut generated_event_range: Option<(i64, i64)> = None;
    let mut effective_mode = mode;
    let mut prefetched_server_keys: Option<Vec<ServerCrdtDocumentKey>> = None;
    let mut bootstrap_pull_ms = 0u64;
    let phase1_push_ms: u64;
    let phase1b_receive_ms: u64;
    let mut phase2_pull_ms = 0u64;
    let mut list_keys_calls = 0usize;
    let mut snapshot_downloads = 0usize;
    let mut phase1_documents_synced = 0usize;
    let mut phase1b_documents_synced = 0usize;
    let mut phase2_documents_synced = 0usize;
    let mut phase1_rpc_rounds = 0usize;
    let mut phase1b_rpc_rounds = 0usize;
    let mut phase1_batch_calls = 0usize;
    let mut phase1b_batch_calls = 0usize;
    let mut phase1_server_reply_messages = 0usize;
    let mut phase1b_server_reply_messages = 0usize;

    tracing::info!(device_id = device_id, ?mode, "Starting Things v3 sync run");

    // ── Pre-flight: detect first-ever sync ───────────────────────────────
    // If the client has never synced any document, the only local docs are
    // auto-initialised root docs.  Pushing an independently-created root to
    // a server that already holds Device A's root creates an Automerge fork
    // conflict on `collection_uuids` — whichever actor ID sorts higher
    // "wins", which may be the empty list, causing data loss.
    //
    // Prevention: when no document has been synced yet, pull from the server
    // **first**, discarding auto-created local docs that would conflict.

    let dirty_docs = sdk
        .crdt_get_dirty_documents()
        .context("Failed to load dirty CRDT documents")?;

    let bootstrap_state = classify_local_bootstrap_state(sdk, &dirty_docs);
    let never_synced_dirty_keys = never_synced_dirty_keys(&dirty_docs);
    tracing::info!(
        device_id = device_id,
        ?bootstrap_state,
        dirty_doc_count = dirty_docs.len(),
        never_synced_dirty_doc_count = never_synced_dirty_keys.len(),
        "Computed local bootstrap state for Things sync"
    );

    if effective_mode == ThingsSyncMode::Incremental
        && bootstrap_state != LocalBootstrapState::HasSyncedHistory
    {
        tracing::info!(
            device_id = device_id,
            requested_mode = ?mode,
            ?bootstrap_state,
            "Incremental sync upgraded to full sync because bootstrap discovery is required"
        );
        effective_mode = ThingsSyncMode::Full;
    }

    // On true first sync, pull from server before pushing so that locally
    // auto-created docs don't fork the server's canonical root.
    let mut documents_pulled = 0;
    if effective_mode.allows_server_discovery()
        && bootstrap_state != LocalBootstrapState::HasSyncedHistory
    {
        let bootstrap_started_at = Instant::now();
        let had_existing_bootstrap_stash = match sdk.things_bootstrap_has_stash() {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    device_id = device_id,
                    error = %err,
                    "Failed to check for existing bootstrap stash; assuming none"
                );
                false
            }
        };
        let created_bootstrap_stash =
            match sdk.things_bootstrap_stash_local_snapshot_if_needed(device_id) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        device_id = device_id,
                        error = %err,
                        "Failed to persist bootstrap stash before destructive bootstrap"
                    );
                    false
                }
            };
        let bootstrap_stash_ready = created_bootstrap_stash || had_existing_bootstrap_stash;
        list_keys_calls += 1;
        let server_keys = client.list_crdt_document_keys().await.unwrap_or_default();
        prefetched_server_keys = Some(server_keys.clone());
        tracing::info!(
            device_id = device_id,
            server_doc_count = server_keys.len(),
            had_existing_bootstrap_stash = had_existing_bootstrap_stash,
            created_bootstrap_stash = created_bootstrap_stash,
            bootstrap_stash_ready = bootstrap_stash_ready,
            "Fetched server keys during bootstrap discovery"
        );

        if !server_keys.is_empty() {
            tracing::info!(
                device_id = device_id,
                server_doc_count = server_keys.len(),
                "First sync detected — pulling server documents before pushing"
            );

            let deleted_local_doc_keys: Vec<String> = never_synced_dirty_keys
                .iter()
                .map(|(uuid, data_type)| format!("{}:{}", uuid, data_type))
                .collect();
            tracing::info!(
                device_id = device_id,
                deleted_local_doc_count = deleted_local_doc_keys.len(),
                deleted_local_doc_keys = ?deleted_local_doc_keys,
                "Deleting never-synced local documents before bootstrap pull"
            );

            if !never_synced_dirty_keys.is_empty() && !bootstrap_stash_ready {
                anyhow::bail!(
                    "Refusing destructive bootstrap without a local stash; {} never-synced dirty CRDT documents would be deleted",
                    never_synced_dirty_keys.len()
                );
            }

            // Delete never-synced auto-created local docs so that
            // `pull_missing_documents` treats them as missing and downloads
            // the server's versions instead.
            for (uuid, data_type) in &never_synced_dirty_keys {
                let key = document_key_from_storage(uuid, data_type)?;
                if let Some(event_range) = sdk
                    .things_delete_raw_document_for_sync(device_id, &sync_run_id, key)
                    .with_context(|| {
                        format!(
                            "Failed to delete never-synced local CRDT document {uuid}:{data_type}"
                        )
                    })?
                {
                    merge_event_range(&mut generated_event_range, Some(event_range));
                }
            }

            let pulled = pull_missing_documents(
                sdk,
                client,
                device_id,
                &sync_run_id,
                Some(&server_keys),
                None,
            )
            .await;
            match pulled {
                Ok(output) => {
                    documents_pulled = output.documents_pulled;
                    documents_synced += output.documents_pulled;
                    phase2_documents_synced += output.documents_pulled;
                    snapshot_downloads += output.snapshot_downloads;
                    list_keys_calls += output.list_keys_calls;
                    merge_event_range(&mut generated_event_range, output.generated_event_range);
                    observe_sync_timestamp(&mut last_sync_at, output.last_sync_at);

                    if bootstrap_stash_ready {
                        sdk.things_bootstrap_replay_stash_onto_current_documents(device_id)
                            .context(
                                "Failed to replay stashed local changes after first-sync bootstrap",
                            )?;
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        device_id = device_id,
                        error = %err,
                        "First-sync pull failed (will continue with push)"
                    );
                }
            }
        }
        bootstrap_pull_ms = bootstrap_started_at.elapsed().as_millis() as u64;
    }

    // ── Phase 1: push dirty local documents ──────────────────────────────
    // Re-fetch dirty list because auto-created docs may have been deleted
    // and pulled docs are saved as clean.

    let dirty_docs = sdk
        .crdt_get_dirty_documents()
        .context("Failed to load dirty CRDT documents")?;

    let mut synced_in_phase1 = std::collections::HashSet::new();
    let phase1_started_at = Instant::now();

    let mut dirty_root_docs = Vec::new();
    let mut dirty_collection_docs = Vec::new();
    let mut dirty_markdown_docs = Vec::new();
    for doc_row in dirty_docs {
        match doc_row.data_type.as_str() {
            "root" => dirty_root_docs.push(doc_row),
            "collection" => dirty_collection_docs.push(doc_row),
            "thing_markdown" => dirty_markdown_docs.push(doc_row),
            _ => {}
        }
    }

    tracing::info!(
        device_id = device_id,
        dirty_root_docs = dirty_root_docs
            .iter()
            .map(|doc| doc.uuid.clone())
            .collect::<Vec<_>>()
            .join(","),
        dirty_collection_docs = dirty_collection_docs
            .iter()
            .map(|doc| doc.uuid.clone())
            .collect::<Vec<_>>()
            .join(","),
        dirty_markdown_docs = dirty_markdown_docs
            .iter()
            .map(|doc| doc.uuid.clone())
            .collect::<Vec<_>>()
            .join(","),
        "Phase 1 dirty document batches prepared"
    );

    for dirty_batch in [dirty_root_docs, dirty_collection_docs, dirty_markdown_docs] {
        let (results, batch_calls) = sync_document_rows_batch(client, device_id, dirty_batch).await;
        phase1_batch_calls += batch_calls;
        for (doc_row, result) in results {
            match result {
                Ok(output) => {
                    synced_in_phase1.insert((doc_row.uuid.clone(), doc_row.data_type.clone()));
                    tracing::info!(
                        device_id = device_id,
                        uuid = doc_row.uuid,
                        data_type = doc_row.data_type,
                        rpc_rounds = output.rpc_rounds,
                        server_reply_messages = output.server_reply_messages,
                        last_sync_at = ?output.last_sync_at,
                        "Phase 1 synced document"
                    );
                    let key = document_key_from_storage(&doc_row.uuid, &doc_row.data_type)?;
                    let event_range = sdk
                        .things_save_synced_document_clean(
                            device_id,
                            &sync_run_id,
                            key,
                            output.doc_bytes,
                            output.sync_state_bytes,
                            output.last_sync_at.as_deref(),
                        )
                        .context("Failed to save synced CRDT document")?;
                    merge_event_range(&mut generated_event_range, event_range);

                    documents_synced += 1;
                    phase1_documents_synced += 1;
                    phase1_rpc_rounds += output.rpc_rounds;
                    phase1_server_reply_messages += output.server_reply_messages;
                    observe_sync_timestamp(&mut last_sync_at, output.last_sync_at);
                }
                Err(err) => {
                    tracing::warn!(
                        device_id = device_id,
                        uuid = doc_row.uuid,
                        data_type = doc_row.data_type,
                        error = %err,
                        "Failed to sync CRDT document, will retry later"
                    );
                }
            }
        }
    }
    phase1_push_ms = phase1_started_at.elapsed().as_millis() as u64;

    tracing::info!(
        device_id = device_id,
        ?effective_mode,
        phase1_documents_synced = documents_synced,
        "Finished phase 1 dirty-document push"
    );

    // ── Phase 1b: receive updates from other devices for existing docs ───
    // Phase 1 only syncs dirty (locally-modified) documents. Clean docs that
    // already exist locally still need a receive path for changes made by
    // OTHER devices. Use the server's canonical head metadata to skip docs
    // that are already converged. If key discovery itself is unavailable,
    // skip the receive/pull phases for this run rather than falling back to
    // a broad receive sync over every local document.
    let server_key_discovery = if let Some(keys) = prefetched_server_keys.take() {
        tracing::info!(
            device_id = device_id,
            server_doc_count = keys.len(),
            "Reusing prefetched server keys for phase 1b/2"
        );
        ServerKeyDiscovery::Available(keys)
    } else {
        match client.list_crdt_document_keys().await {
            Ok(keys) => {
                list_keys_calls += 1;
                tracing::info!(
                    device_id = device_id,
                    server_doc_count = keys.len(),
                    "Fetched server keys for phase 1b/2"
                );
                ServerKeyDiscovery::Available(keys)
            }
            Err(err) => {
                tracing::warn!(
                    device_id = device_id,
                    error = %err,
                    "Failed to list server CRDT keys for phase 1b optimization; skipping receive/pull phases to avoid cold-start sync storms"
                );
                ServerKeyDiscovery::Unavailable
            }
        }
    };
    let server_head_by_key = server_key_discovery
        .keys()
        .map(build_server_head_map)
        .unwrap_or_default();
    let mut reachability = match build_local_reachability_filters(sdk, device_id) {
        Ok(filters) => filters,
        Err(err) => {
            tracing::warn!(
                device_id = device_id,
                error = %err,
                "Failed to build local reachability filters for phase 1b; falling back to broad receive sync"
            );
            LocalReachabilityFilters::default()
        }
    };

    let phase1b_started_at = Instant::now();
    if server_key_discovery.keys().is_some() {
        let mut all_keys = sdk.crdt_list_document_keys().unwrap_or_default();

        // Sort: root first, then collection, then thing_markdown
        all_keys.sort_by_key(|(_, dt)| match dt.as_str() {
            "root" => 0,
            "collection" => 1,
            "thing_markdown" => 2,
            _ => 3,
        });

        let mut receive_root_docs = Vec::new();
        let mut receive_collection_docs = Vec::new();
        let mut receive_markdown_docs = Vec::new();

        for (uuid, data_type_str) in all_keys {
            if synced_in_phase1.contains(&(uuid.clone(), data_type_str.clone())) {
                continue;
            }

            let data_type = match data_type_str.as_str() {
                "root" => CrdtDataType::Root,
                "collection" => CrdtDataType::Collection,
                "thing_markdown" => CrdtDataType::ThingMarkdown,
                _ => continue,
            };

            let doc_row = match sdk.crdt_get_document(&uuid, &data_type_str) {
                Ok(Some(row)) => row,
                _ => continue,
            };

            if !doc_row.dirty
                && !clean_document_should_receive(
                    &uuid,
                    &data_type_str,
                    &doc_row.automerge_doc,
                    &reachability,
                )
            {
                tracing::debug!(
                    device_id = device_id,
                    uuid = uuid,
                    data_type = data_type_str,
                    "Phase 1b: skipping clean unreachable document"
                );
                continue;
            }

            if !doc_row.dirty {
                if let Some(server_head) =
                    server_head_by_key.get(&(uuid.clone(), data_type_str.clone()))
                {
                    if document_is_at_server_head(&doc_row.automerge_doc, server_head) {
                        tracing::debug!(
                            device_id = device_id,
                            uuid = uuid,
                            data_type = data_type_str,
                            "Phase 1b: skipping clean document already at server canonical head"
                        );
                        continue;
                    }
                }
            }

            match data_type {
                CrdtDataType::Root => receive_root_docs.push(doc_row),
                CrdtDataType::Collection => receive_collection_docs.push(doc_row),
                CrdtDataType::ThingMarkdown => receive_markdown_docs.push(doc_row),
            }
        }

        for receive_batch in [
            receive_root_docs,
            receive_collection_docs,
            receive_markdown_docs,
        ] {
            let receive_batch_keys = receive_batch
                .iter()
                .map(|doc| format!("{}:{}", doc.uuid, doc.data_type))
                .collect::<Vec<_>>();
            tracing::info!(
                device_id = device_id,
                receive_doc_count = receive_batch_keys.len(),
                receive_doc_keys = ?receive_batch_keys,
                "Phase 1b receive batch prepared"
            );
            let (results, batch_calls) =
                sync_document_rows_batch(client, device_id, receive_batch).await;
            phase1b_batch_calls += batch_calls;
            for (doc_row, result) in results {
                match result {
                    Ok(output) => {
                        tracing::info!(
                            device_id = device_id,
                            uuid = doc_row.uuid,
                            data_type = doc_row.data_type,
                            rpc_rounds = output.rpc_rounds,
                            server_reply_messages = output.server_reply_messages,
                            last_sync_at = ?output.last_sync_at,
                            "Phase 1b received document updates"
                        );
                        documents_synced += 1;
                        phase1b_documents_synced += 1;
                        phase1b_rpc_rounds += output.rpc_rounds;
                        phase1b_server_reply_messages += output.server_reply_messages;
                        documents_pulled += 1;
                        let key = document_key_from_storage(&doc_row.uuid, &doc_row.data_type)?;
                        let event_range = sdk.things_apply_remote_documents(
                            device_id,
                            &sync_run_id,
                            vec![(
                                key,
                                DocumentState {
                                    automerge_doc: output.doc_bytes,
                                    sync_state: output.sync_state_bytes,
                                    dirty: false,
                                    last_sync_at: output.last_sync_at.clone(),
                                },
                            )],
                        )?;
                        merge_event_range(&mut generated_event_range, event_range);
                        observe_sync_timestamp(&mut last_sync_at, output.last_sync_at);
                        if doc_row.data_type == "collection" {
                            if let Ok(updated_filters) =
                                build_local_reachability_filters(sdk, device_id)
                            {
                                reachability = updated_filters;
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            device_id = device_id,
                            uuid = doc_row.uuid,
                            data_type = doc_row.data_type,
                            error = %err,
                            "Phase 1b: failed to receive updates for document (non-fatal)"
                        );
                    }
                }
            }
        }
    } else {
        tracing::warn!(
            device_id = device_id,
            "Skipping phase 1b receive sync because server key discovery is unavailable"
        );
    }
    phase1b_receive_ms = phase1b_started_at.elapsed().as_millis() as u64;

    // ── Phase 2: pull any remaining missing server-side documents ────────

    if let Some(server_keys) = server_key_discovery.keys() {
        let phase2_started_at = Instant::now();
        let pulled = pull_missing_documents(
            sdk,
            client,
            device_id,
            &sync_run_id,
            Some(server_keys),
            Some(&mut reachability),
        )
        .await;
        match pulled {
            Ok(output) => {
                documents_pulled += output.documents_pulled;
                documents_synced += output.documents_pulled;
                phase2_documents_synced += output.documents_pulled;
                snapshot_downloads += output.snapshot_downloads;
                list_keys_calls += output.list_keys_calls;
                merge_event_range(&mut generated_event_range, output.generated_event_range);
                observe_sync_timestamp(&mut last_sync_at, output.last_sync_at);
            }
            Err(err) => {
                tracing::warn!(
                    device_id = device_id,
                    error = %err,
                    "Failed to pull missing server documents (non-fatal)"
                );
            }
        }
        phase2_pull_ms = phase2_started_at.elapsed().as_millis() as u64;
    } else {
        tracing::warn!(
            device_id = device_id,
            "Skipping phase 2 pull because server key discovery is unavailable"
        );
    }

    tracing::info!(
        device_id = device_id,
        ?effective_mode,
        documents_synced = documents_synced,
        last_sync_at = ?last_sync_at,
        "Completed Things v3 sync run"
    );

    Ok(ThingsV3SyncOutput {
        documents_synced,
        documents_pushed: phase1_documents_synced,
        documents_pulled,
        last_sync_at,
        generated_event_range,
        metrics: ThingsV3SyncMetrics {
            total_elapsed_ms: total_started_at.elapsed().as_millis() as u64,
            bootstrap_pull_ms,
            phase1_push_ms,
            phase1b_receive_ms,
            phase2_pull_ms,
            list_keys_calls,
            snapshot_downloads,
            phase1_documents_synced,
            phase1b_documents_synced,
            phase2_documents_synced,
            phase1_rpc_rounds,
            phase1b_rpc_rounds,
            phase1_batch_calls,
            phase1b_batch_calls,
            phase1_server_reply_messages,
            phase1b_server_reply_messages,
        },
    })
}

/// Convert proto data_type integer to storage string.
fn proto_data_type_to_str(proto_dt: i32) -> &'static str {
    match proto_dt {
        1 => "root",
        2 => "collection",
        3 => "thing_markdown",
        _ => "",
    }
}

/// Load all v3 CRDT documents from storage into a ThingsDocumentSet
pub(crate) fn load_document_set_from_storage(
    sdk: &RemiSdk,
    device_id: &str,
) -> Result<ThingsDocumentSet> {
    DocumentPersistence::new(sdk.things_storage()).load_document_set(device_id)
}

#[cfg(test)]
mod tests;
