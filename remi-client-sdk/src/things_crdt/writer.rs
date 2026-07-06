use super::*;

pub(super) struct ThingsDomainWriter<'a> {
    device_id: &'a str,
    documents: &'a mut HashMap<DocumentKey, DocumentState>,
}

impl<'a> ThingsDomainWriter<'a> {
    pub(super) fn new(
        device_id: &'a str,
        documents: &'a mut HashMap<DocumentKey, DocumentState>,
    ) -> Self {
        Self {
            device_id,
            documents,
        }
    }

    pub(super) fn new_document_state(&self, automerge_doc: Vec<u8>) -> DocumentState {
        DocumentState {
            automerge_doc,
            sync_state: Vec::new(),
            dirty: true,
            last_sync_at: None,
        }
    }

    pub(super) fn init_root(&mut self) -> Result<()> {
        let key = DocumentKey::root();
        if self.documents.contains_key(&key) {
            return Ok(());
        }

        let doc_bytes = Schema::init_root_doc(self.device_id)?;
        self.documents
            .insert(key, self.new_document_state(doc_bytes));
        Ok(())
    }

    pub(super) fn get_or_init_collection(&mut self, collection_uuid: &str) -> Result<()> {
        let key = DocumentKey::collection(collection_uuid);
        if !self.documents.contains_key(&key) {
            let doc_bytes = Schema::init_collection_doc(self.device_id, collection_uuid)?;
            self.documents
                .insert(key, self.new_document_state(doc_bytes));
            self.add_collection(collection_uuid)?;
        }
        Ok(())
    }

    pub(super) fn get_or_init_thing_content(
        &mut self,
        document_uuid: &str,
        thing_uuid: &str,
        content_type: &str,
    ) -> Result<()> {
        let key = DocumentKey::thing_content(document_uuid);
        if !self.documents.contains_key(&key) {
            let doc_bytes = Schema::init_thing_content_doc(
                self.device_id,
                document_uuid,
                thing_uuid,
                content_type,
            )?;
            self.documents
                .insert(key, self.new_document_state(doc_bytes));
        }
        Ok(())
    }

    fn apply_document_update<F>(&mut self, key: &DocumentKey, update: F) -> Result<()>
    where
        F: FnOnce(&[u8]) -> Result<Vec<u8>>,
    {
        let current_doc = self
            .documents
            .get(key)
            .map(|state| state.automerge_doc.clone())
            .ok_or_else(|| anyhow::anyhow!("Missing document for key {:?}", key))?;
        let updated_doc = update(&current_doc)?;
        let state = self
            .documents
            .get_mut(key)
            .ok_or_else(|| anyhow::anyhow!("Missing document for key {:?}", key))?;
        state.automerge_doc = updated_doc;
        state.dirty = true;
        Ok(())
    }

    pub(super) fn apply_collection_update(
        &mut self,
        collection_uuid: &str,
        op: CollectionOp,
    ) -> Result<()> {
        self.get_or_init_collection(collection_uuid)?;
        let key = DocumentKey::collection(collection_uuid);
        self.apply_document_update(&key, |doc| {
            apply_collection_op(doc, self.device_id, collection_uuid, op)
        })
    }

    pub(super) fn apply_thing_content_update(
        &mut self,
        document_uuid: &str,
        thing_uuid: &str,
        content_type: &str,
        op: ThingMarkdownOp,
    ) -> Result<()> {
        self.get_or_init_thing_content(document_uuid, thing_uuid, content_type)?;
        let key = DocumentKey::thing_content(document_uuid);
        self.apply_document_update(&key, |doc| {
            apply_thing_markdown_op(doc, self.device_id, thing_uuid, op)
        })
    }

    pub(super) fn apply_thing_markdown_update(
        &mut self,
        thing_uuid: &str,
        op: ThingMarkdownOp,
    ) -> Result<()> {
        self.apply_thing_content_update(thing_uuid, thing_uuid, "markdown", op)
    }

    pub(super) fn root_collection_list_ids(doc: &AutoCommit) -> Result<Vec<ObjId>> {
        Ok(doc
            .get_all(automerge::ROOT, Schema::KEY_COLLECTION_UUIDS)?
            .into_iter()
            .filter_map(|(value, obj_id)| match value {
                AmValue::Object(ObjType::List) => Some(obj_id),
                _ => None,
            })
            .collect())
    }

    pub(super) fn root_list_contains_collection(
        doc: &AutoCommit,
        list_obj: &ObjId,
        collection_uuid: &str,
    ) -> Result<bool> {
        for index in 0..doc.length(list_obj) {
            if let Some((AmValue::Scalar(value), _)) = doc.get(list_obj, index)? {
                if let ScalarValue::Str(existing_uuid) = value.as_ref() {
                    if existing_uuid == collection_uuid {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    pub(super) fn add_root_collection_membership(
        device_id: &str,
        doc_bytes: &[u8],
        collection_uuid: &str,
    ) -> Result<Vec<u8>> {
        let mut doc = if doc_bytes.is_empty() {
            let init_bytes = Schema::init_root_doc(device_id)?;
            AutoCommit::load(&init_bytes).context("Failed to load init root doc")?
        } else {
            AutoCommit::load(doc_bytes).context("Failed to load root doc")?
        };
        doc.set_actor(ActorId::from(device_id.as_bytes().to_vec()));

        let list_objs = Self::root_collection_list_ids(&doc)?;

        let mut already_present = false;
        for list_obj in &list_objs {
            if Self::root_list_contains_collection(&doc, list_obj, collection_uuid)? {
                already_present = true;
                break;
            }
        }

        if !already_present {
            let target_list = if let Some(existing) = list_objs.first() {
                existing.clone()
            } else {
                doc.put_object(automerge::ROOT, Schema::KEY_COLLECTION_UUIDS, ObjType::List)
                    .context("Failed to create collection_uuids list")?
            };
            doc.insert(
                &target_list,
                doc.length(&target_list),
                collection_uuid.to_string(),
            )
            .context("Failed to add collection uuid to root list")?;
        }

        Ok(doc.save())
    }

    pub(super) fn add_collection(&mut self, collection_uuid: &str) -> Result<()> {
        self.init_root()?;
        let key = DocumentKey::root();
        let device_id = self.device_id;
        self.apply_document_update(&key, |doc| {
            Self::add_root_collection_membership(device_id, doc, collection_uuid)
        })
    }

    pub(super) fn update_collection_meta(
        &mut self,
        collection_uuid: &str,
        title: Option<String>,
        status: Option<String>,
        attrs_json: Option<String>,
    ) -> Result<()> {
        self.apply_collection_update(
            collection_uuid,
            CollectionOp::UpdateMeta {
                title,
                status,
                attrs_json,
            },
        )
    }

    pub(super) fn ensure_live_collection_exists(&self, collection_uuid: &str) -> Result<()> {
        let key = DocumentKey::collection(collection_uuid);
        let Some(state) = self.documents.get(&key) else {
            anyhow::bail!(
                "collection '{}' must exist before adding or reparenting things",
                collection_uuid
            );
        };

        let view = extract_collection_doc_view(&state.automerge_doc, collection_uuid)?;
        if view
            .meta
            .tombstone
            .as_ref()
            .map(|t| t.deleted)
            .unwrap_or(false)
        {
            anyhow::bail!("collection '{}' is deleted", collection_uuid);
        }

        Ok(())
    }

    pub(super) fn upsert_thing_meta(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        datatype: Option<ThingDatatype>,
        status: Option<String>,
        title: Option<String>,
        parent_uuid: Option<String>,
        attrs_json: Option<String>,
    ) -> Result<()> {
        self.ensure_live_collection_exists(collection_uuid)?;
        self.apply_collection_update(
            collection_uuid,
            CollectionOp::UpsertThingMeta {
                thing_id: thing_uuid.to_string(),
                datatype,
                status,
                status_timestamp_ms: None,
                title,
                parent_id: FieldPatch::from_compat_option_str(parent_uuid.as_deref()),
                built_in: None,
                attrs_json,
            },
        )
    }

    pub(super) fn delete_thing(&mut self, collection_uuid: &str, thing_uuid: &str) -> Result<()> {
        if !self
            .documents
            .contains_key(&DocumentKey::collection(collection_uuid))
        {
            return Ok(());
        }

        self.apply_collection_update(
            collection_uuid,
            CollectionOp::DeleteThing {
                thing_id: thing_uuid.to_string(),
            },
        )
    }

    pub(super) fn add_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry: ContentEntry,
    ) -> Result<()> {
        let built_in = ThingBuiltInFieldsUpdate {
            add_entries: vec![entry],
            ..Default::default()
        };

        self.apply_collection_update(
            collection_uuid,
            CollectionOp::UpsertThingMeta {
                thing_id: thing_uuid.to_string(),
                datatype: None,
                status: None,
                status_timestamp_ms: None,
                title: None,
                parent_id: FieldPatch::Noop,
                built_in: Some(built_in),
                attrs_json: None,
            },
        )
    }

    pub(super) fn update_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry_id: &str,
        title: Option<Option<String>>,
        order: Option<f64>,
        payload: Option<ContentEntryPayload>,
    ) -> Result<()> {
        let entry_update = remi_things_crdt::ContentEntryUpdate {
            id: entry_id.to_string(),
            title,
            order,
            payload,
        };

        let built_in = ThingBuiltInFieldsUpdate {
            update_entries: vec![entry_update],
            ..Default::default()
        };

        self.apply_collection_update(
            collection_uuid,
            CollectionOp::UpsertThingMeta {
                thing_id: thing_uuid.to_string(),
                datatype: None,
                status: None,
                status_timestamp_ms: None,
                title: None,
                parent_id: FieldPatch::Noop,
                built_in: Some(built_in),
                attrs_json: None,
            },
        )
    }

    pub(super) fn delete_content_entry(
        &mut self,
        collection_uuid: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<()> {
        let built_in = ThingBuiltInFieldsUpdate {
            delete_entry_ids: vec![entry_id.to_string()],
            ..Default::default()
        };

        self.apply_collection_update(
            collection_uuid,
            CollectionOp::UpsertThingMeta {
                thing_id: thing_uuid.to_string(),
                datatype: None,
                status: None,
                status_timestamp_ms: None,
                title: None,
                parent_id: FieldPatch::Noop,
                built_in: Some(built_in),
                attrs_json: None,
            },
        )
    }

    pub(super) fn set_thing_content(&mut self, thing_uuid: &str, content: Content) -> Result<()> {
        self.apply_thing_markdown_update(thing_uuid, ThingMarkdownOp::SetContent { content })
    }

    pub(super) fn set_thing_content_document(
        &mut self,
        document_uuid: &str,
        thing_uuid: &str,
        content_type: &str,
        content: Content,
    ) -> Result<()> {
        self.apply_thing_content_update(
            document_uuid,
            thing_uuid,
            content_type,
            ThingMarkdownOp::SetContent { content },
        )
    }

    pub(super) fn splice_thing_text(
        &mut self,
        thing_uuid: &str,
        block_id: &str,
        index: usize,
        delete: usize,
        insert: &str,
    ) -> Result<()> {
        self.apply_thing_markdown_update(
            thing_uuid,
            ThingMarkdownOp::SpliceText {
                block_id: block_id.to_string(),
                index,
                delete,
                insert: insert.to_string(),
            },
        )
    }
}
