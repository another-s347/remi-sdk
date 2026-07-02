use super::*;

pub(crate) trait CrdtDocumentRepository {
    fn list_crdt_documents(&self) -> Result<Vec<crate::types::CrdtDocumentRow>>;
    fn save_crdt_document(
        &self,
        uuid: &str,
        data_type: &str,
        automerge_doc: &[u8],
        sync_state: &[u8],
        dirty: bool,
        last_sync_at: Option<&str>,
    ) -> Result<()>;
}

impl CrdtDocumentRepository for crate::storage::Storage {
    fn list_crdt_documents(&self) -> Result<Vec<crate::types::CrdtDocumentRow>> {
        self.list_crdt_documents()
    }

    fn save_crdt_document(
        &self,
        uuid: &str,
        data_type: &str,
        automerge_doc: &[u8],
        sync_state: &[u8],
        dirty: bool,
        last_sync_at: Option<&str>,
    ) -> Result<()> {
        self.save_crdt_document(
            uuid,
            data_type,
            automerge_doc,
            sync_state,
            dirty,
            last_sync_at,
        )
    }
}

pub(crate) struct DocumentPersistence<'a, R: CrdtDocumentRepository + ?Sized> {
    repository: &'a R,
}

impl<'a, R: CrdtDocumentRepository + ?Sized> DocumentPersistence<'a, R> {
    pub(crate) fn new(repository: &'a R) -> Self {
        Self { repository }
    }

    pub(crate) fn load_or_init_document_set(&self, device_id: &str) -> Result<ThingsDocumentSet> {
        let mut doc_set = self.load_document_set(device_id)?;
        if !doc_set.has_root_document() {
            doc_set.init_root()?;
        }
        Ok(doc_set)
    }

    pub(crate) fn load_document_set(&self, device_id: &str) -> Result<ThingsDocumentSet> {
        let mut doc_set = ThingsDocumentSet::new(device_id);

        let rows = self
            .repository
            .list_crdt_documents()
            .context("Failed to list CRDT documents")?;

        for row in rows {
            let Some(key) = DocumentKey::from_storage_parts(&row.uuid, &row.data_type)? else {
                continue;
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

        Ok(doc_set)
    }

    pub(crate) fn save_document_set_mark_clean(
        &self,
        doc_set: &mut ThingsDocumentSet,
    ) -> Result<()> {
        let keys = doc_set.documents.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            if let Some(state) = doc_set.documents.get(&key) {
                self.repository
                    .save_crdt_document(
                        &key.uuid,
                        key.data_type_str(),
                        &state.automerge_doc,
                        &state.sync_state,
                        false,
                        state.last_sync_at.as_deref(),
                    )
                    .with_context(|| format!("Failed to save clean CRDT document {:?}", key))?;
            }
            doc_set.store_mut().set_persisted_state(&key, true, None);
        }
        Ok(())
    }

    pub(crate) fn save_dirty_documents_with_compaction(
        &self,
        doc_set: &mut ThingsDocumentSet,
        threshold: usize,
    ) -> Result<(usize, usize)> {
        let dirty_keys = doc_set.store_mut().dirty_document_keys();

        let count = dirty_keys.len();
        let mut compacted = 0;

        for key in &dirty_keys {
            if doc_set.maybe_compact_with_threshold(key, threshold)? {
                compacted += 1;
            }

            if let Some(state) = doc_set.documents.get(key) {
                self.repository
                    .save_crdt_document(
                        &key.uuid,
                        key.data_type_str(),
                        &state.automerge_doc,
                        &state.sync_state,
                        state.dirty,
                        state.last_sync_at.as_deref(),
                    )
                    .with_context(|| format!("Failed to save dirty CRDT document {:?}", key))?;
            }
        }

        Ok((count, compacted))
    }

    pub(crate) fn save_document_state(
        &self,
        doc_set: &mut ThingsDocumentSet,
        key: &DocumentKey,
        mark_clean: bool,
        last_sync_at: Option<&str>,
    ) -> Result<()> {
        if let Some(state) = doc_set.documents.get_mut(key) {
            let dirty = if mark_clean { false } else { state.dirty };
            self.repository
                .save_crdt_document(
                    &key.uuid,
                    key.data_type_str(),
                    &state.automerge_doc,
                    &state.sync_state,
                    dirty,
                    last_sync_at.or(state.last_sync_at.as_deref()),
                )
                .with_context(|| format!("Failed to save CRDT document {:?}", key))?;
        }
        doc_set
            .store_mut()
            .set_persisted_state(key, mark_clean, last_sync_at);
        Ok(())
    }
}

impl ThingsDocumentSet {
    /// Check if any document has pending changes
    pub(crate) fn has_pending_changes(&self) -> bool {
        self.store_view().documents.values().any(|s| s.dirty)
    }

    /// Make documents field accessible for deletion
    pub(crate) fn remove_document(&mut self, key: &DocumentKey) -> Option<DocumentState> {
        self.store_mut().remove_document(key)
    }
}
