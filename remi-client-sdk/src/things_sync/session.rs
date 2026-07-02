use super::*;

async fn sync_single_v3_document<T>(
    client: &mut T,
    device_id: &str,
    uuid: &str,
    data_type: &CrdtDataType,
    doc_bytes: Vec<u8>,
    sync_state_bytes: Vec<u8>,
) -> Result<ThingsSyncOutput>
where
    T: CrdtSyncTransport,
{
    let mut session = crdt_sync::AutomergeSyncSession::new_with_device_id(
        &doc_bytes,
        &sync_state_bytes,
        device_id,
    )
    .context("Failed to init CRDT document sync session")?;

    const MAX_ROUNDS: usize = 20;
    const MAX_STALL_ROUNDS: usize = 3;

    let mut last_sync_at = None;
    let mut server_msgs: Vec<Vec<u8>> = Vec::new();
    let mut prev_outgoing: Vec<u8> = Vec::new();
    let mut prev_server_msgs: Vec<Vec<u8>> = Vec::new();
    let mut stall_rounds: usize = 0;
    let mut rpc_rounds: usize = 0;
    let mut server_reply_messages: usize = 0;

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
        rpc_rounds += 1;
        server_reply_messages += next_server_msgs.len();

        last_sync_at = optional_sync_timestamp(last);

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
        rpc_rounds,
        server_reply_messages,
    })
}

struct BatchSyncSession {
    uuid: String,
    data_type: CrdtDataType,
    session: crdt_sync::AutomergeSyncSession,
    pending_server_messages: Vec<Vec<u8>>,
    prev_outgoing: Vec<u8>,
    prev_server_messages: Vec<Vec<u8>>,
    stall_rounds: usize,
    last_sync_at: Option<String>,
    rpc_rounds: usize,
    server_reply_messages: usize,
    finished: bool,
}

struct BatchSyncRunOutput {
    outputs: Vec<ThingsSyncOutput>,
    batch_calls: usize,
}

async fn sync_v3_document_batch<T>(
    client: &mut T,
    device_id: &str,
    documents: Vec<(String, CrdtDataType, Vec<u8>, Vec<u8>)>,
) -> Result<BatchSyncRunOutput>
where
    T: CrdtSyncTransport,
{
    if documents.is_empty() {
        return Ok(BatchSyncRunOutput {
            outputs: Vec::new(),
            batch_calls: 0,
        });
    }

    const MAX_ROUNDS: usize = 20;
    const MAX_STALL_ROUNDS: usize = 3;

    let mut sessions = Vec::with_capacity(documents.len());
    for (uuid, data_type, doc_bytes, sync_state_bytes) in documents {
        sessions.push(BatchSyncSession {
            uuid,
            data_type,
            session: crdt_sync::AutomergeSyncSession::new_with_device_id(
                &doc_bytes,
                &sync_state_bytes,
                device_id,
            )
            .context("Failed to init CRDT document sync session")?,
            pending_server_messages: Vec::new(),
            prev_outgoing: Vec::new(),
            prev_server_messages: Vec::new(),
            stall_rounds: 0,
            last_sync_at: None,
            rpc_rounds: 0,
            server_reply_messages: 0,
            finished: false,
        });
    }

    let mut batch_calls = 0usize;
    for round in 0..MAX_ROUNDS {
        let mut batch_requests = Vec::new();
        let mut request_indices = Vec::new();

        for (index, state) in sessions.iter_mut().enumerate() {
            if state.finished {
                continue;
            }

            if !state.pending_server_messages.is_empty() {
                state
                    .session
                    .apply_server_messages(&state.pending_server_messages)
                    .context("Failed to apply server messages for CRDT document batch")?;
                state.pending_server_messages.clear();
            }

            let outgoing = state.session.generate_client_message().unwrap_or_default();
            if outgoing.is_empty() {
                tracing::debug!(
                    device_id = device_id,
                    uuid = state.uuid,
                    data_type = state.data_type.as_str(),
                    round = round + 1,
                    "sync_v3_document_batch: document converged with no outgoing message"
                );
                state.finished = true;
                continue;
            }

            request_indices.push((index, outgoing.clone()));
            batch_requests.push((
                state.uuid.clone(),
                data_type_to_proto(&state.data_type),
                outgoing,
            ));
        }

        if batch_requests.is_empty() {
            break;
        }

        let responses = client
            .sync_crdt_documents(device_id.to_string(), batch_requests)
            .await
            .context("Failed to sync CRDT document batch with server")?;
        batch_calls += 1;
        let mut responses_by_key = responses
            .into_iter()
            .map(|(document_uuid, data_type, sync_messages, last_sync_at)| {
                ((document_uuid, data_type), (sync_messages, last_sync_at))
            })
            .collect::<std::collections::HashMap<_, _>>();

        for (index, outgoing_for_compare) in request_indices {
            let state = &mut sessions[index];
            let response_key = (state.uuid.clone(), data_type_to_proto(&state.data_type));
            let (next_server_messages, last_sync_at) =
                responses_by_key.remove(&response_key).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Batch CRDT sync response missing document {} ({})",
                        state.uuid,
                        state.data_type.as_str()
                    )
                })?;

            state.rpc_rounds += 1;
            state.server_reply_messages += next_server_messages.len();
            state.last_sync_at = optional_sync_timestamp(last_sync_at);

            if outgoing_for_compare == state.prev_outgoing
                && next_server_messages == state.prev_server_messages
            {
                state.stall_rounds += 1;
            } else {
                state.stall_rounds = 0;
            }

            state.prev_outgoing = outgoing_for_compare;
            state.prev_server_messages = next_server_messages.clone();
            state.pending_server_messages = next_server_messages;

            let reply_bytes: usize = state
                .pending_server_messages
                .iter()
                .map(|msg| msg.len())
                .sum();
            tracing::debug!(
                device_id = device_id,
                uuid = state.uuid,
                data_type = state.data_type.as_str(),
                round = round + 1,
                outgoing_bytes = state.prev_outgoing.len(),
                reply_count = state.pending_server_messages.len(),
                reply_bytes = reply_bytes,
                stall_rounds = state.stall_rounds,
                "sync_v3_document_batch: round complete"
            );

            if state.stall_rounds >= MAX_STALL_ROUNDS {
                tracing::warn!(
                    device_id = device_id,
                    uuid = state.uuid,
                    data_type = state.data_type.as_str(),
                    round = round + 1,
                    "sync_v3_document_batch: breaking after repeated identical handshake rounds"
                );
                state
                    .session
                    .apply_server_messages(&state.pending_server_messages)
                    .context("Failed to apply stalled server messages in batch sync")?;
                state.pending_server_messages.clear();
                state.finished = true;
            }
        }

        if !responses_by_key.is_empty() {
            return Err(anyhow::anyhow!(
                "Batch CRDT sync returned {} unexpected document responses",
                responses_by_key.len()
            ));
        }
    }

    let mut outputs = Vec::with_capacity(sessions.len());
    for mut state in sessions {
        if !state.pending_server_messages.is_empty() {
            state
                .session
                .apply_server_messages(&state.pending_server_messages)
                .context("Failed to apply final server messages for batched CRDT sync")?;
        }

        outputs.push(ThingsSyncOutput {
            doc_bytes: state.session.doc_bytes(),
            sync_state_bytes: state.session.sync_state_bytes(),
            last_sync_at: state.last_sync_at,
            rpc_rounds: state.rpc_rounds,
            server_reply_messages: state.server_reply_messages,
        });
    }

    Ok(BatchSyncRunOutput {
        outputs,
        batch_calls,
    })
}

pub(super) async fn sync_document_rows_batch<T>(
    client: &mut T,
    device_id: &str,
    doc_rows: Vec<crate::types::CrdtDocumentRow>,
) -> (
    Vec<(crate::types::CrdtDocumentRow, Result<ThingsSyncOutput>)>,
    usize,
)
where
    T: CrdtSyncTransport,
{
    if doc_rows.is_empty() {
        return (Vec::new(), 0);
    }

    let batch_inputs = match doc_rows
        .iter()
        .map(|doc_row| {
            Ok((
                doc_row.uuid.clone(),
                parse_row_data_type(&doc_row.data_type)?,
                doc_row.automerge_doc.clone(),
                doc_row.sync_state.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()
    {
        Ok(inputs) => inputs,
        Err(err) => {
            return (
                doc_rows
                    .into_iter()
                    .map(|doc_row| {
                        let message =
                            format!("Failed to prepare CRDT document for batch sync: {err}");
                        (doc_row, Err(anyhow::anyhow!(message)))
                    })
                    .collect(),
                0,
            );
        }
    };

    match sync_v3_document_batch(client, device_id, batch_inputs).await {
        Ok(run_output) => (
            doc_rows
                .into_iter()
                .zip(run_output.outputs.into_iter().map(Ok))
                .collect(),
            run_output.batch_calls,
        ),
        Err(err) => {
            tracing::warn!(
                device_id = device_id,
                error = %err,
                document_count = doc_rows.len(),
                "Batch CRDT sync failed; falling back to per-document sync"
            );

            let mut results = Vec::with_capacity(doc_rows.len());
            for doc_row in doc_rows {
                let result = match parse_row_data_type(&doc_row.data_type) {
                    Ok(data_type) => {
                        sync_single_v3_document(
                            client,
                            device_id,
                            &doc_row.uuid,
                            &data_type,
                            doc_row.automerge_doc.clone(),
                            doc_row.sync_state.clone(),
                        )
                        .await
                    }
                    Err(err) => Err(err),
                };
                results.push((doc_row, result));
            }
            (results, 0)
        }
    }
}
