use anyhow::{Context, Result};

use crate::TriggerClient;
use crate::TriggerSdk;
use crate::crdt_sync;
use crate::things_crdt::{DocumentKey, DocumentState, ThingsDocumentSet};
use crate::trigger_client::{CrdtSyncTransport, ServerCrdtDocumentKey};

use remi_things_crdt::CrdtDataType;

struct ThingsSyncOutput {
    pub doc_bytes: Vec<u8>,
    pub sync_state_bytes: Vec<u8>,
    pub last_sync_at: String,
}

// ============================================================================
// V3 Multi-Document Batched Sync
// ============================================================================

/// Output from v3 batched sync
pub struct ThingsV3SyncOutput {
    /// Number of documents synced
    pub documents_synced: usize,
    /// Last sync timestamp (from server)
    pub last_sync_at: Option<String>,
}

/// Convert CrdtDataType to proto enum value
fn data_type_to_proto(data_type: &CrdtDataType) -> i32 {
    match data_type {
        CrdtDataType::Root => 1,
        CrdtDataType::Collection => 2,
        CrdtDataType::ThingMarkdown => 3,
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

#[derive(Default)]
struct LocalReachabilityFilters {
    active_collections: Option<std::collections::HashSet<String>>,
    active_things: Option<std::collections::HashSet<String>>,
}

fn build_local_reachability_filters(
    sdk: &TriggerSdk,
    device_id: &str,
) -> Result<LocalReachabilityFilters> {
    match sdk.crdt_get_document(remi_things_crdt::ROOT_DOC_UUID, "root") {
        Ok(Some(row)) if row.last_sync_at.is_some() => {
            let doc_set = load_document_set_from_storage(sdk, device_id)?;
            Ok(LocalReachabilityFilters {
                active_collections: Some(doc_set.live_collection_uuids_from_root()?),
                active_things: Some(doc_set.live_thing_uuids_from_root()?),
            })
        }
        _ => Ok(LocalReachabilityFilters::default()),
    }
}

fn clean_document_should_receive(
    uuid: &str,
    data_type_str: &str,
    doc_bytes: &[u8],
    filters: &LocalReachabilityFilters,
) -> bool {
    match data_type_str {
        "collection" => match &filters.active_collections {
            Some(active) if !active.contains(uuid) => remi_things_crdt::extract_collection_doc_view(
                doc_bytes,
                uuid,
            )
            .map(|view| {
                !view
                    .meta
                    .tombstone
                    .as_ref()
                    .map(|t| t.deleted)
                    .unwrap_or(false)
            })
            .unwrap_or(true),
            _ => true,
        },
        "thing_markdown" => filters
            .active_things
            .as_ref()
            .map(|active| active.contains(uuid))
            .unwrap_or(true),
        _ => true,
    }
}

fn should_pull_missing_document(
    uuid: &str,
    data_type_str: &str,
    filters: &LocalReachabilityFilters,
) -> bool {
    match data_type_str {
        "collection" => filters
            .active_collections
            .as_ref()
            .map(|active| active.contains(uuid))
            .unwrap_or(true),
        "thing_markdown" => filters
            .active_things
            .as_ref()
            .map(|active| active.contains(uuid))
            .unwrap_or(true),
        _ => true,
    }
}

fn update_reachability_from_downloaded_doc(
    uuid: &str,
    data_type_str: &str,
    doc_bytes: &[u8],
    filters: &mut LocalReachabilityFilters,
) {
    match data_type_str {
        "root" => {
            if let Some(active_collections) = filters.active_collections.as_mut() {
                if let Ok(view) = remi_things_crdt::extract_root_view(doc_bytes) {
                    *active_collections = view.collection_uuids.into_iter().collect();
                }
            }
        }
        "collection" => {
            let collection_is_active = filters
                .active_collections
                .as_ref()
                .map(|active| active.contains(uuid))
                .unwrap_or(true);
            if !collection_is_active {
                return;
            }

            if let Some(active_things) = filters.active_things.as_mut() {
                if let Ok(view) = remi_things_crdt::extract_collection_doc_view(doc_bytes, uuid) {
                    for thing in &view.things {
                        if !thing
                            .tombstone
                            .as_ref()
                            .map(|t| t.deleted)
                            .unwrap_or(false)
                        {
                            active_things.insert(thing.id.clone());
                        }
                    }
                }
            }
        }
        _ => {}
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
    sdk: &TriggerSdk,
    client: &mut TriggerClient,
    device_id: &str,
) -> Result<ThingsV3SyncOutput> {
    sync_v3_documents_with_transport(sdk, client, device_id).await
}

pub async fn sync_v3_documents_with_transport(
    sdk: &TriggerSdk,
    client: &mut dyn CrdtSyncTransport,
    device_id: &str,
) -> Result<ThingsV3SyncOutput> {
    let mut documents_synced = 0;
    let mut last_sync_at: Option<String> = None;

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

    let has_ever_synced = dirty_docs.iter().any(|d| d.last_sync_at.is_some()) || {
        // Also check non-dirty docs
        sdk.crdt_list_document_keys()
            .unwrap_or_default()
            .iter()
            .any(|(uuid, dt)| {
                sdk.crdt_get_document(uuid, dt)
                    .ok()
                    .flatten()
                    .map(|d| d.last_sync_at.is_some())
                    .unwrap_or(false)
            })
    };

    // On true first sync, pull from server before pushing so that locally
    // auto-created docs don't fork the server's canonical root.
    let mut documents_pulled = 0;
    if !has_ever_synced {
        let stashed_local_snapshot = sdk
            .things_bootstrap_stash_local_snapshot_if_needed(device_id)
            .unwrap_or(false);
        let server_keys = client.list_crdt_document_keys().await.unwrap_or_default();

        if !server_keys.is_empty() {
            tracing::info!(
                device_id = device_id,
                server_doc_count = server_keys.len(),
                "First sync detected — pulling server documents before pushing"
            );

            // Delete never-synced auto-created local docs so that
            // `pull_missing_documents` treats them as missing and downloads
            // the server's versions instead.
            for doc_row in &dirty_docs {
                if doc_row.last_sync_at.is_none() {
                    let _ = sdk.crdt_delete_document(&doc_row.uuid, &doc_row.data_type);
                }
            }

            let pulled = pull_missing_documents(sdk, client, device_id, Some(&server_keys)).await;
            match pulled {
                Ok((count, pull_last_sync)) => {
                    documents_pulled = count;
                    documents_synced += count;
                    if let Some(ts) = pull_last_sync {
                        last_sync_at = Some(ts);
                    }

                    if stashed_local_snapshot {
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
    }

    // ── Phase 1: push dirty local documents ──────────────────────────────
    // Re-fetch dirty list because auto-created docs may have been deleted
    // and pulled docs are saved as clean.

    let dirty_docs = sdk
        .crdt_get_dirty_documents()
        .context("Failed to load dirty CRDT documents")?;

    let mut synced_in_phase1 = std::collections::HashSet::new();

    for doc_row in dirty_docs {
        let data_type = match doc_row.data_type.as_str() {
            "root" => CrdtDataType::Root,
            "collection" => CrdtDataType::Collection,
            "thing_markdown" => CrdtDataType::ThingMarkdown,
            _ => continue,
        };

        let result = sync_single_v3_document(
            client,
            device_id,
            &doc_row.uuid,
            &data_type,
            doc_row.automerge_doc,
            doc_row.sync_state,
        )
        .await;

        match result {
            Ok(output) => {
                synced_in_phase1.insert((doc_row.uuid.clone(), doc_row.data_type.clone()));
                sdk.crdt_save_document(
                    &doc_row.uuid,
                    &doc_row.data_type,
                    &output.doc_bytes,
                    &output.sync_state_bytes,
                    false, // no longer dirty
                    Some(&output.last_sync_at),
                )
                .context("Failed to save synced CRDT document")?;

                documents_synced += 1;
                if !output.last_sync_at.is_empty() {
                    last_sync_at = Some(output.last_sync_at);
                }
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

    // ── Phase 1b: receive updates from other devices for existing docs ───
    // Phase 1 only syncs dirty (locally-modified) documents. Clean docs that
    // already exist locally still need a receive path for changes made by
    // OTHER devices. Use the server's canonical head metadata to skip docs
    // that are already converged. If key discovery itself is unavailable,
    // skip the receive/pull phases for this run rather than falling back to
    // a broad receive sync over every local document.
    let server_key_discovery = match client.list_crdt_document_keys().await {
        Ok(keys) => ServerKeyDiscovery::Available(keys),
        Err(err) => {
            tracing::warn!(
                device_id = device_id,
                error = %err,
                "Failed to list server CRDT keys for phase 1b optimization; skipping receive/pull phases to avoid cold-start sync storms"
            );
            ServerKeyDiscovery::Unavailable
        }
    };
    let server_head_by_key = server_key_discovery
        .keys()
        .map(build_server_head_map)
        .unwrap_or_default();
    let reachability = match build_local_reachability_filters(sdk, device_id) {
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

    if server_key_discovery.keys().is_some() {
        let mut all_keys = sdk.crdt_list_document_keys().unwrap_or_default();

        // Sort: root first, then collection, then thing_markdown
        all_keys.sort_by_key(|(_, dt)| match dt.as_str() {
            "root" => 0,
            "collection" => 1,
            "thing_markdown" => 2,
            _ => 3,
        });

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

            let result = sync_single_v3_document(
                client,
                device_id,
                &uuid,
                &data_type,
                doc_row.automerge_doc,
                doc_row.sync_state,
            )
            .await;

            match result {
                Ok(output) => {
                    sdk.crdt_save_document(
                        &uuid,
                        &data_type_str,
                        &output.doc_bytes,
                        &output.sync_state_bytes,
                        false,
                        Some(&output.last_sync_at),
                    )
                    .context("Failed to save synced CRDT document")?;

                    documents_synced += 1;
                    // Count as "pulled" so SnapshotReplace is emitted for UI refresh
                    documents_pulled += 1;
                    if !output.last_sync_at.is_empty() {
                        last_sync_at = Some(output.last_sync_at);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        device_id = device_id,
                        uuid = uuid,
                        data_type = data_type_str,
                        error = %err,
                        "Phase 1b: failed to receive updates for document (non-fatal)"
                    );
                }
            }
        }
    } else {
        tracing::warn!(
            device_id = device_id,
            "Skipping phase 1b receive sync because server key discovery is unavailable"
        );
    }

    // ── Phase 2: pull any remaining missing server-side documents ────────

    if let Some(server_keys) = server_key_discovery.keys() {
        let pulled = pull_missing_documents(sdk, client, device_id, Some(server_keys)).await;
        match pulled {
            Ok((count, pull_last_sync)) => {
                documents_pulled += count;
                documents_synced += count;
                if let Some(ts) = pull_last_sync {
                    last_sync_at = Some(ts);
                }
            }
            Err(err) => {
                tracing::warn!(
                    device_id = device_id,
                    error = %err,
                    "Failed to pull missing server documents (non-fatal)"
                );
            }
        }
    } else {
        tracing::warn!(
            device_id = device_id,
            "Skipping phase 2 pull because server key discovery is unavailable"
        );
    }

    // ── Notify Flutter if new documents were pulled ──────────────────────
    // Emit a SnapshotReplace event so the UI refreshes with the newly
    // downloaded collections/things without requiring a manual reload.
    if documents_pulled > 0 {
        if let Err(err) = sdk.emit_snapshot_replace(device_id) {
            tracing::warn!(
                device_id = device_id,
                error = %err,
                "Failed to emit SnapshotReplace after pull (non-fatal)"
            );
        }
    }

    Ok(ThingsV3SyncOutput {
        documents_synced,
        last_sync_at,
    })
}

/// Discover server-side CRDT documents and download any that are missing locally.
///
/// Returns (documents_pulled, last_sync_at).
async fn pull_missing_documents(
    sdk: &TriggerSdk,
    client: &mut dyn CrdtSyncTransport,
    device_id: &str,
    prefetched_server_keys: Option<&[ServerCrdtDocumentKey]>,
) -> Result<(usize, Option<String>)> {
    // Get a set of all local document keys for fast lookup
    let local_keys: std::collections::HashSet<(String, String)> = sdk
        .crdt_list_document_keys()
        .context("Failed to list local CRDT document keys")?
        .into_iter()
        .collect();

    let mut reachability = match build_local_reachability_filters(sdk, device_id) {
        Ok(filters) => filters,
        Err(err) => {
            tracing::warn!(
                device_id = device_id,
                error = %err,
                "Failed to build local reachability filters for phase 2; falling back to broad pull"
            );
            LocalReachabilityFilters::default()
        }
    };

    // Ask the server for its full list of document keys unless the caller
    // already fetched them for phase 1b.
    let fetched_server_keys;
    let server_keys: &[ServerCrdtDocumentKey] = if let Some(keys) = prefetched_server_keys {
        keys
    } else {
        fetched_server_keys = client
            .list_crdt_document_keys()
            .await
            .context("Failed to list server CRDT document keys")?;
        &fetched_server_keys
    };

    // Determine which server documents are missing locally
    let mut missing: Vec<(String, i32)> = Vec::new();
    for key in server_keys {
        let uuid = &key.document_uuid;
        let proto_dt = key.data_type;
        let dt_str = proto_data_type_to_str(proto_dt);
        if dt_str.is_empty() || local_keys.contains(&(uuid.clone(), dt_str.to_string())) {
            continue;
        }

        missing.push((uuid.clone(), proto_dt));
    }

    if missing.is_empty() {
        tracing::debug!(device_id = device_id, "No missing server documents to pull");
        return Ok((0, None));
    }

    tracing::info!(
        device_id = device_id,
        count = missing.len(),
        "Pulling missing server documents"
    );

    let mut documents_pulled = 0;
    let mut last_sync_at: Option<String> = None;

    // Sort: Root first, then Collection, then ThingMarkdown (matches push order)
    missing.sort_by_key(|(_, dt)| *dt);

    for (uuid, proto_dt) in &missing {
        let dt_str = proto_data_type_to_str(*proto_dt);
        let data_type = match dt_str {
            "root" => CrdtDataType::Root,
            "collection" => CrdtDataType::Collection,
            "thing_markdown" => CrdtDataType::ThingMarkdown,
            _ => continue,
        };

        if !should_pull_missing_document(uuid, dt_str, &reachability) {
            tracing::debug!(
                uuid = %uuid,
                data_type = dt_str,
                "Skipping pull for unreachable document"
            );
            continue;
        }

        // Download snapshot from server
        match client
            .get_crdt_document_snapshot(
                device_id.to_string(),
                uuid.clone(),
                *proto_dt,
                true, // reset_sync_state — we have no local sync state yet
            )
            .await
        {
            Ok((doc_bytes, sync_at)) => {
                if doc_bytes.is_empty() {
                    tracing::debug!(
                        uuid = uuid,
                        data_type = dt_str,
                        "Server returned empty doc, skipping"
                    );
                    continue;
                }

                // Clone before move into sync_single_v3_document so we can
                // fall back to saving the raw snapshot on sync failure.
                let doc_bytes_fallback = doc_bytes.clone();

                // Now run a single Automerge sync round so both sides share sync state.
                // Start from the downloaded snapshot (local doc) with empty sync state.
                let sync_result = sync_single_v3_document(
                    client,
                    device_id,
                    uuid,
                    &data_type,
                    doc_bytes,
                    Vec::new(), // fresh sync state
                )
                .await;

                match sync_result {
                    Ok(output) => {
                        sdk.crdt_save_document(
                            uuid,
                            dt_str,
                            &output.doc_bytes,
                            &output.sync_state_bytes,
                            false,
                            Some(&output.last_sync_at),
                        )
                        .context("Failed to save pulled CRDT document")?;

                        update_reachability_from_downloaded_doc(
                            uuid,
                            dt_str,
                            &output.doc_bytes,
                            &mut reachability,
                        );

                        documents_pulled += 1;
                        if !output.last_sync_at.is_empty() {
                            last_sync_at = Some(output.last_sync_at);
                        }
                    }
                    Err(err) => {
                        // Sync round failed; still save the raw snapshot so we have *something*
                        tracing::warn!(uuid = uuid, data_type = dt_str, error = %err,
                            "Sync round after snapshot download failed, saving raw snapshot");
                        sdk.crdt_save_document(
                            uuid,
                            dt_str,
                            &doc_bytes_fallback,
                            &[],
                            false,
                            if sync_at.is_empty() {
                                None
                            } else {
                                Some(&sync_at)
                            },
                        )
                        .context("Failed to save raw snapshot")?;
                        update_reachability_from_downloaded_doc(
                            uuid,
                            dt_str,
                            &doc_bytes_fallback,
                            &mut reachability,
                        );
                        documents_pulled += 1;
                        if !sync_at.is_empty() {
                            last_sync_at = Some(sync_at);
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    uuid = uuid,
                    data_type = dt_str,
                    error = %err,
                    "Failed to download CRDT document snapshot, skipping"
                );
            }
        }
    }

    Ok((documents_pulled, last_sync_at))
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

/// Sync a single v3 CRDT document with the server
async fn sync_single_v3_document(
    client: &mut dyn CrdtSyncTransport,
    device_id: &str,
    uuid: &str,
    data_type: &CrdtDataType,
    doc_bytes: Vec<u8>,
    sync_state_bytes: Vec<u8>,
) -> Result<ThingsSyncOutput> {
    let mut session = crdt_sync::AutomergeSyncSession::new_with_device_id(
        &doc_bytes,
        &sync_state_bytes,
        device_id,
    )
    .context("Failed to init CRDT document sync session")?;

    const MAX_ROUNDS: usize = 20;
    const MAX_STALL_ROUNDS: usize = 3;

    let mut last_sync_at = String::new();
    let mut server_msgs: Vec<Vec<u8>> = Vec::new();
    let mut prev_outgoing: Vec<u8> = Vec::new();
    let mut prev_server_msgs: Vec<Vec<u8>> = Vec::new();
    let mut stall_rounds: usize = 0;

    let proto_data_type = data_type_to_proto(data_type);

    for round in 0..MAX_ROUNDS {
        if !server_msgs.is_empty() {
            session
                .apply_server_messages(&server_msgs)
                .context("Failed to apply server messages for CRDT document")?;
            server_msgs.clear();
        }

        let outgoing = session.generate_client_message().unwrap_or_default();
        let outgoing_for_compare = outgoing.clone();

        if outgoing.is_empty() {
            tracing::debug!(
                device_id = device_id,
                uuid = uuid,
                data_type = data_type.as_str(),
                round = round + 1,
                "sync_single_v3_document: converged with no outgoing message"
            );
            break;
        }

        // Use the v3 sync endpoint with document key
        let (next_server_msgs, last) = client
            .sync_crdt_document(
                device_id.to_string(),
                uuid.to_string(),
                proto_data_type,
                outgoing,
            )
            .await
            .context("Failed to sync CRDT document with server")?;

        last_sync_at = last;

        if outgoing_for_compare == prev_outgoing && next_server_msgs == prev_server_msgs {
            stall_rounds += 1;
        } else {
            stall_rounds = 0;
        }

        prev_outgoing = outgoing_for_compare;
        prev_server_msgs = next_server_msgs.clone();
        server_msgs = next_server_msgs;

        let reply_bytes: usize = server_msgs.iter().map(|msg| msg.len()).sum();
        tracing::debug!(
            device_id = device_id,
            uuid = uuid,
            data_type = data_type.as_str(),
            round = round + 1,
            outgoing_bytes = prev_outgoing.len(),
            reply_count = server_msgs.len(),
            reply_bytes = reply_bytes,
            stall_rounds = stall_rounds,
            "sync_single_v3_document: round complete"
        );

        if stall_rounds >= MAX_STALL_ROUNDS {
            tracing::warn!(
                device_id = device_id,
                uuid = uuid,
                data_type = data_type.as_str(),
                round = round + 1,
                "sync_single_v3_document: breaking after repeated identical handshake rounds"
            );
            session
                .apply_server_messages(&server_msgs)
                .context("Failed to apply stalled server messages")?;
            server_msgs.clear();
            break;
        }
    }

    if !server_msgs.is_empty() {
        session
            .apply_server_messages(&server_msgs)
            .context("Failed to apply final server messages")?;
    }

    Ok(ThingsSyncOutput {
        doc_bytes: session.doc_bytes(),
        sync_state_bytes: session.sync_state_bytes(),
        last_sync_at,
    })
}

/// Load all v3 CRDT documents from storage into a ThingsDocumentSet
pub fn load_document_set_from_storage(
    sdk: &TriggerSdk,
    device_id: &str,
) -> Result<ThingsDocumentSet> {
    let mut doc_set = ThingsDocumentSet::new(device_id);

    // Load all documents from storage
    let keys = sdk
        .crdt_list_document_keys()
        .context("Failed to list CRDT document keys")?;

    for (uuid, data_type_str) in keys {
        let data_type: CrdtDataType = match data_type_str.as_str() {
            "root" => CrdtDataType::Root,
            "collection" => CrdtDataType::Collection,
            "thing_markdown" => CrdtDataType::ThingMarkdown,
            _ => continue,
        };

        if let Some(row) = sdk
            .crdt_get_document(&uuid, &data_type_str)
            .context("Failed to get CRDT document")?
        {
            let key = DocumentKey {
                uuid: uuid.clone(),
                data_type,
            };
            doc_set.set(
                key,
                DocumentState {
                    automerge_doc: row.automerge_doc,
                    sync_state: row.sync_state,
                    dirty: row.dirty,
                    last_sync_at: row.last_sync_at,
                },
            );
        }
    }

    Ok(doc_set)
}

/// Save a ThingsDocumentSet back to storage
pub fn save_document_set_to_storage(sdk: &TriggerSdk, doc_set: &ThingsDocumentSet) -> Result<()> {
    for key in doc_set.keys() {
        if let Some(state) = doc_set.get(key) {
            sdk.crdt_save_document(
                &key.uuid,
                key.data_type_str(),
                &state.automerge_doc,
                &state.sync_state,
                state.dirty,
                state.last_sync_at.as_deref(),
            )
            .context("Failed to save CRDT document to storage")?;
        }
    }
    Ok(())
}

/// Save only dirty documents from a ThingsDocumentSet to storage
pub fn save_dirty_documents_to_storage(
    sdk: &TriggerSdk,
    doc_set: &ThingsDocumentSet,
) -> Result<usize> {
    let dirty = doc_set.dirty_documents();
    let count = dirty.len();

    for (key, state) in dirty {
        sdk.crdt_save_document(
            &key.uuid,
            key.data_type_str(),
            &state.automerge_doc,
            &state.sync_state,
            state.dirty,
            state.last_sync_at.as_deref(),
        )
        .context("Failed to save dirty CRDT document to storage")?;
    }

    Ok(count)
}

#[cfg(test)]
mod tests_v3 {
    use super::*;

    #[test]
    fn test_document_key_data_type_str() {
        let root = DocumentKey::root();
        assert_eq!(root.data_type_str(), "root");

        let coll = DocumentKey::collection("coll-1");
        assert_eq!(coll.data_type_str(), "collection");

        let md = DocumentKey::thing_markdown("thing-1");
        assert_eq!(md.data_type_str(), "thing_markdown");
    }

    #[test]
    fn document_head_match_detects_identical_single_head_doc() {
        let doc = remi_things_crdt::Schema::init_root_doc("device-a").unwrap();
        let head = local_canonical_head(&doc).unwrap();

        assert!(document_is_at_server_head(&doc, &head));
    }

    #[test]
    fn document_head_match_requires_non_empty_server_head() {
        let doc = remi_things_crdt::Schema::init_root_doc("device-a").unwrap();

        assert!(!document_is_at_server_head(&doc, &[]));
    }
}
