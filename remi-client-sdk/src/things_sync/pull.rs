use super::*;

#[derive(Default)]
pub(super) struct LocalReachabilityFilters {
    active_collections: Option<std::collections::HashSet<String>>,
    active_things: Option<std::collections::HashSet<String>>,
    active_content_documents: Option<std::collections::HashSet<String>>,
}

pub(super) struct PullMissingDocumentsOutput {
    pub(super) documents_pulled: usize,
    pub(super) last_sync_at: Option<String>,
    pub(super) generated_event_range: Option<(i64, i64)>,
    pub(super) snapshot_downloads: usize,
    pub(super) list_keys_calls: usize,
}

pub(super) fn build_local_reachability_filters(
    sdk: &RemiSdk,
    device_id: &str,
) -> Result<LocalReachabilityFilters> {
    let has_synced_non_root_documents = sdk
        .crdt_list_document_keys()
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, data_type)| data_type != "root")
        .any(|(uuid, data_type)| {
            sdk.crdt_get_document(&uuid, &data_type)
                .ok()
                .flatten()
                .map(|row| has_sync_history(&row))
                .unwrap_or(false)
        });

    if !has_synced_non_root_documents {
        return Ok(LocalReachabilityFilters::default());
    }

    let doc_set = load_document_set_from_storage(sdk, device_id)?;
    Ok(LocalReachabilityFilters {
        active_collections: Some(doc_set.active_collection_uuids()?),
        active_things: Some(doc_set.active_thing_uuids()?),
        active_content_documents: Some(doc_set.active_content_document_uuids()?),
    })
}

pub(super) fn clean_document_should_receive(
    uuid: &str,
    data_type_str: &str,
    doc_bytes: &[u8],
    filters: &LocalReachabilityFilters,
) -> bool {
    match data_type_str {
        "collection" => match &filters.active_collections {
            Some(active) if !active.contains(uuid) => {
                remi_things_crdt::extract_collection_doc_view(doc_bytes, uuid)
                    .map(|view| {
                        !view
                            .meta
                            .tombstone
                            .as_ref()
                            .map(|t| t.deleted)
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
            }
            _ => true,
        },
        "thing_markdown" => filters
            .active_things
            .as_ref()
            .zip(filters.active_content_documents.as_ref())
            .map(|(active_things, active_content_documents)| {
                active_things.contains(uuid) || active_content_documents.contains(uuid)
            })
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
        // Collections are top-level discoverable documents. Filtering them by
        // the current local root index prevents devices from learning about
        // collections created elsewhere after both sides already have sync
        // history.
        "collection" => true,
        "thing_markdown" => filters
            .active_things
            .as_ref()
            .zip(filters.active_content_documents.as_ref())
            .map(|(active_things, active_content_documents)| {
                active_things.contains(uuid) || active_content_documents.contains(uuid)
            })
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
        "collection" => {
            let Ok(view) = remi_things_crdt::extract_collection_doc_view(doc_bytes, uuid) else {
                return;
            };

            let collection_is_live = !view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false);

            if let Some(active_collections) = filters.active_collections.as_mut() {
                if collection_is_live {
                    active_collections.insert(uuid.to_string());
                } else {
                    active_collections.remove(uuid);
                }
            }

            if !collection_is_live {
                if let Some(active_things) = filters.active_things.as_mut() {
                    for thing in &view.things {
                        active_things.remove(&thing.id);
                    }
                }
                return;
            }

            if let Some(active_things) = filters.active_things.as_mut() {
                for thing in &view.things {
                    if !thing.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false) {
                        active_things.insert(thing.id.clone());
                    } else {
                        active_things.remove(&thing.id);
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
pub(super) async fn pull_missing_documents<T>(
    sdk: &RemiSdk,
    client: &mut T,
    device_id: &str,
    sync_run_id: &str,
    prefetched_server_keys: Option<&[ServerCrdtDocumentKey]>,
    reachability: Option<&mut LocalReachabilityFilters>,
) -> Result<PullMissingDocumentsOutput>
where
    T: CrdtSyncTransport,
{
    // Get a set of all local document keys for fast lookup
    let local_keys: std::collections::HashSet<(String, String)> = sdk
        .crdt_list_document_keys()
        .context("Failed to list local CRDT document keys")?
        .into_iter()
        .collect();

    let mut owned_reachability;
    let reachability = match reachability {
        Some(filters) => filters,
        None => {
            owned_reachability = match build_local_reachability_filters(sdk, device_id) {
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
            &mut owned_reachability
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
        return Ok(PullMissingDocumentsOutput {
            documents_pulled: 0,
            last_sync_at: None,
            generated_event_range: None,
            snapshot_downloads: 0,
            list_keys_calls: usize::from(prefetched_server_keys.is_none()),
        });
    }

    tracing::info!(
        device_id = device_id,
        count = missing.len(),
        "Pulling missing server documents"
    );

    let mut documents_pulled = 0;
    let mut last_sync_at: Option<String> = None;
    let mut generated_event_range: Option<(i64, i64)> = None;
    let mut local_snapshot_downloads = 0usize;

    // Sort: Root first, then Collection, then ThingMarkdown (matches push order)
    missing.sort_by_key(|(_, dt)| *dt);

    let batch_documents: Vec<(String, i32)> = missing
        .iter()
        .filter_map(|(uuid, proto_dt)| {
            let dt_str = proto_data_type_to_str(*proto_dt);
            if should_pull_missing_document(uuid, dt_str, reachability) {
                Some((uuid.clone(), *proto_dt))
            } else {
                tracing::debug!(
                    uuid = %uuid,
                    data_type = dt_str,
                    "Skipping pull for unreachable document"
                );
                None
            }
        })
        .collect();

    if batch_documents.is_empty() {
        return Ok(PullMissingDocumentsOutput {
            documents_pulled: 0,
            last_sync_at: None,
            generated_event_range: None,
            snapshot_downloads: 0,
            list_keys_calls: usize::from(prefetched_server_keys.is_none()),
        });
    }

    match client
        .get_crdt_document_snapshots(device_id.to_string(), batch_documents, true)
        .await
    {
        Ok(snapshots) => {
            for (uuid, proto_dt, doc_bytes, sync_at) in snapshots {
                local_snapshot_downloads += 1;
                let dt_str = proto_data_type_to_str(proto_dt);

                if doc_bytes.is_empty() {
                    tracing::debug!(
                        uuid = uuid,
                        data_type = dt_str,
                        "Server returned empty doc, skipping"
                    );
                    continue;
                }

                let snapshot_sync_at = optional_sync_timestamp(sync_at);
                let key = document_key_from_storage(&uuid, dt_str)?;
                let event_range = sdk.things_apply_remote_documents(
                    device_id,
                    sync_run_id,
                    vec![(
                        key,
                        DocumentState {
                            automerge_doc: doc_bytes.clone(),
                            sync_state: Vec::new(),
                            dirty: false,
                            last_sync_at: snapshot_sync_at.clone(),
                        },
                    )],
                )?;
                merge_event_range(&mut generated_event_range, event_range);

                update_reachability_from_downloaded_doc(&uuid, dt_str, &doc_bytes, reachability);
                if dt_str == "collection" {
                    if let Ok(updated_filters) = build_local_reachability_filters(sdk, device_id) {
                        *reachability = updated_filters;
                    }
                }

                documents_pulled += 1;
                observe_sync_timestamp(&mut last_sync_at, snapshot_sync_at);
            }
        }
        Err(err) => {
            tracing::warn!(
                device_id = device_id,
                error = %err,
                "Batch snapshot download failed, falling back to per-document pulls"
            );

            for (uuid, proto_dt) in &missing {
                let dt_str = proto_data_type_to_str(*proto_dt);
                if !should_pull_missing_document(uuid, dt_str, reachability) {
                    continue;
                }

                match client
                    .get_crdt_document_snapshot(
                        device_id.to_string(),
                        uuid.clone(),
                        *proto_dt,
                        true,
                    )
                    .await
                {
                    Ok((doc_bytes, sync_at)) => {
                        local_snapshot_downloads += 1;
                        if doc_bytes.is_empty() {
                            tracing::debug!(
                                uuid = uuid,
                                data_type = dt_str,
                                "Server returned empty doc, skipping"
                            );
                            continue;
                        }

                        let snapshot_sync_at = optional_sync_timestamp(sync_at);
                        let key = document_key_from_storage(uuid, dt_str)?;
                        let event_range = sdk.things_apply_remote_documents(
                            device_id,
                            sync_run_id,
                            vec![(
                                key,
                                DocumentState {
                                    automerge_doc: doc_bytes.clone(),
                                    sync_state: Vec::new(),
                                    dirty: false,
                                    last_sync_at: snapshot_sync_at.clone(),
                                },
                            )],
                        )?;
                        merge_event_range(&mut generated_event_range, event_range);

                        update_reachability_from_downloaded_doc(
                            uuid,
                            dt_str,
                            &doc_bytes,
                            reachability,
                        );
                        if dt_str == "collection" {
                            if let Ok(updated_filters) =
                                build_local_reachability_filters(sdk, device_id)
                            {
                                *reachability = updated_filters;
                            }
                        }

                        documents_pulled += 1;
                        observe_sync_timestamp(&mut last_sync_at, snapshot_sync_at);
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
        }
    }

    Ok(PullMissingDocumentsOutput {
        documents_pulled,
        last_sync_at,
        generated_event_range,
        snapshot_downloads: local_snapshot_downloads,
        list_keys_calls: usize::from(prefetched_server_keys.is_none()),
    })
}
