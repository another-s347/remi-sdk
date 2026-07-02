use super::*;
use base64::Engine as _;
use remi_things_crdt::CrdtDataType;
use std::str::FromStr;

impl<'a> ThingsLocalService<'a> {
    pub fn restore_stashed_documents(
        &self,
        device_id: &str,
        documents: &[BootstrapStashedDocument],
        clear_existing_documents: bool,
    ) -> Result<ThingsMutationResult<()>> {
        if clear_existing_documents {
            self.storage.delete_all_crdt_documents().context(
                "Failed to clear existing Things CRDT documents before document restore",
            )?;
        }

        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before document restore")?;
        let mut events = Vec::new();

        for document in sorted_stashed_documents(documents) {
            let key = stashed_document_key(&document)?;
            let automerge_doc = decode_stashed_document_bytes(&document)?;
            doc_set.set(
                key.clone(),
                DocumentState {
                    automerge_doc,
                    sync_state: Vec::new(),
                    dirty: true,
                    last_sync_at: None,
                },
            );
            events.push(document_event_for_key(
                &key,
                ThingsDocumentChangeKind::Updated,
            ));
        }

        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, ())
    }

    pub fn merge_stashed_documents(
        &self,
        device_id: &str,
        documents: &[BootstrapStashedDocument],
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before document merge")?;
        let mut stashed_doc_set = ThingsDocumentSet::new(device_id);
        let mut events = Vec::new();

        for document in sorted_stashed_documents(documents) {
            let key = stashed_document_key(&document)?;
            let incoming_bytes = decode_stashed_document_bytes(&document)?;
            stashed_doc_set.set(
                key.clone(),
                DocumentState {
                    automerge_doc: incoming_bytes.clone(),
                    sync_state: Vec::new(),
                    dirty: true,
                    last_sync_at: None,
                },
            );
            let merged_state = if let Some(existing) = doc_set.get(&key) {
                let mut merged = automerge::AutoCommit::load(&existing.automerge_doc)
                    .context("Failed to load current CRDT document")?;
                let mut incoming = automerge::AutoCommit::load(&incoming_bytes)
                    .context("Failed to load stashed CRDT document")?;
                merged
                    .merge(&mut incoming)
                    .context("Failed to merge stashed CRDT document")?;
                DocumentState {
                    automerge_doc: merged.save(),
                    sync_state: existing.sync_state.clone(),
                    dirty: true,
                    last_sync_at: existing.last_sync_at.clone(),
                }
            } else {
                DocumentState {
                    automerge_doc: incoming_bytes,
                    sync_state: Vec::new(),
                    dirty: true,
                    last_sync_at: None,
                }
            };

            doc_set.set(key.clone(), merged_state);
            events.push(document_event_for_key(
                &key,
                ThingsDocumentChangeKind::Updated,
            ));
        }

        let stashed_snapshot = stashed_doc_set
            .extract_snapshot()
            .context("Failed to extract bootstrap-stashed Things snapshot")?;
        events.extend(
            replay_missing_snapshot_into_document_set(&mut doc_set, &stashed_snapshot)
                .context("Failed to replay missing bootstrap-stashed Things snapshot entities")?,
        );

        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, ())
    }
}

fn sorted_stashed_documents(
    documents: &[BootstrapStashedDocument],
) -> Vec<BootstrapStashedDocument> {
    let mut sorted_documents = documents.to_vec();
    sorted_documents
        .sort_by_key(|doc| (stashed_data_type_sort_key(&doc.data_type), doc.uuid.clone()));
    sorted_documents
}

fn stashed_data_type_sort_key(data_type: &str) -> u8 {
    match data_type {
        "root" => 0,
        "collection" => 1,
        "thing_markdown" => 2,
        _ => 3,
    }
}

fn stashed_document_key(document: &BootstrapStashedDocument) -> Result<DocumentKey> {
    let data_type = CrdtDataType::from_str(&document.data_type)
        .map_err(|err| anyhow!("Invalid stashed CRDT data type: {err}"))?;
    Ok(DocumentKey {
        uuid: document.uuid.clone(),
        data_type,
    })
}

fn decode_stashed_document_bytes(document: &BootstrapStashedDocument) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(&document.automerge_doc_base64)
        .context("Failed to decode stashed CRDT document")
}
