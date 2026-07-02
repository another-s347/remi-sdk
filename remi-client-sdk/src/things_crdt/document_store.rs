use super::*;

/// Document key: (uuid, data_type)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DocumentKey {
    pub uuid: String,
    pub data_type: CrdtDataType,
}

impl DocumentKey {
    pub fn root() -> Self {
        Self {
            uuid: ROOT_DOC_UUID.to_string(),
            data_type: CrdtDataType::Root,
        }
    }

    pub fn collection(uuid: &str) -> Self {
        Self {
            uuid: uuid.to_string(),
            data_type: CrdtDataType::Collection,
        }
    }

    pub fn thing_markdown(uuid: &str) -> Self {
        Self {
            uuid: uuid.to_string(),
            data_type: CrdtDataType::ThingMarkdown,
        }
    }

    pub fn thing_content(uuid: &str) -> Self {
        Self {
            uuid: uuid.to_string(),
            data_type: CrdtDataType::ThingMarkdown,
        }
    }

    pub fn from_storage_parts(uuid: &str, data_type: &str) -> Result<Option<Self>> {
        let data_type = match data_type {
            "root" => CrdtDataType::Root,
            "collection" => CrdtDataType::Collection,
            "thing_markdown" => CrdtDataType::ThingMarkdown,
            _ => return Ok(None),
        };
        Ok(Some(Self {
            uuid: uuid.to_string(),
            data_type,
        }))
    }

    pub fn data_type_str(&self) -> &'static str {
        self.data_type.as_str()
    }
}

/// In-memory document with sync state
#[derive(Debug, Clone)]
pub struct DocumentState {
    pub automerge_doc: Vec<u8>,
    pub sync_state: Vec<u8>,
    pub dirty: bool,
    pub last_sync_at: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DocumentStoreView<'a> {
    pub(super) documents: &'a HashMap<DocumentKey, DocumentState>,
}

impl<'a> DocumentStoreView<'a> {
    pub(super) fn new(documents: &'a HashMap<DocumentKey, DocumentState>) -> Self {
        Self { documents }
    }

    pub(super) fn get(&self, key: &DocumentKey) -> Option<&'a DocumentState> {
        self.documents.get(key)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&'a DocumentKey, &'a DocumentState)> {
        self.documents.iter()
    }
}

pub(super) struct DocumentStoreMut<'a> {
    device_id: &'a str,
    documents: &'a mut HashMap<DocumentKey, DocumentState>,
}

impl<'a> DocumentStoreMut<'a> {
    pub(super) fn new(
        device_id: &'a str,
        documents: &'a mut HashMap<DocumentKey, DocumentState>,
    ) -> Self {
        Self {
            device_id,
            documents,
        }
    }

    pub(super) fn set(&mut self, key: DocumentKey, state: DocumentState) {
        self.documents.insert(key, state);
    }

    pub(super) fn set_persisted_state(
        &mut self,
        key: &DocumentKey,
        mark_clean: bool,
        last_sync_at: Option<&str>,
    ) {
        if let Some(state) = self.documents.get_mut(key) {
            if mark_clean {
                state.dirty = false;
            }
            if let Some(last_sync_at) = last_sync_at {
                state.last_sync_at = Some(last_sync_at.to_string());
            }
        }
    }

    pub(super) fn dirty_document_keys(&self) -> Vec<DocumentKey> {
        self.documents
            .iter()
            .filter(|(_, state)| state.dirty)
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub(super) fn remove_document(&mut self, key: &DocumentKey) -> Option<DocumentState> {
        self.documents.remove(key)
    }

    pub(super) fn maybe_compact_with_threshold(
        &mut self,
        key: &DocumentKey,
        threshold: usize,
    ) -> Result<bool> {
        let Some(state) = self.documents.get(key) else {
            return Ok(false);
        };

        if !needs_compaction(&state.automerge_doc, threshold) {
            return Ok(false);
        }

        let compacted = match key.data_type {
            CrdtDataType::Root => compact_root_doc(&state.automerge_doc, self.device_id)
                .context("Failed to compact root document")?,
            CrdtDataType::Collection => {
                compact_collection_doc(&state.automerge_doc, &key.uuid, self.device_id)
                    .context("Failed to compact collection document")?
            }
            CrdtDataType::ThingMarkdown => {
                let doc = AutoCommit::load(&state.automerge_doc)
                    .context("Failed to load thing content document for compaction")?;
                let thing_uuid = match doc.get(automerge::ROOT, "thing_uuid")? {
                    Some((AmValue::Scalar(value), _)) => match value.as_ref() {
                        ScalarValue::Str(value) => value.to_string(),
                        _ => key.uuid.clone(),
                    },
                    _ => key.uuid.clone(),
                };
                compact_thing_content_doc(
                    &state.automerge_doc,
                    &key.uuid,
                    &thing_uuid,
                    self.device_id,
                )
                .context("Failed to compact thing content document")?
            }
        };

        if let Some(state) = self.documents.get_mut(key) {
            state.automerge_doc = compacted;
            state.dirty = true;
        }

        Ok(true)
    }
}
