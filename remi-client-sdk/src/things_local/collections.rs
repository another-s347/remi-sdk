use super::*;

impl<'a> ThingsLocalService<'a> {
    pub fn upsert_collection(
        &self,
        device_id: &str,
        upsert: ThingCollectionUpsert,
    ) -> Result<ThingsMutationResult<ThingCollectionEntry>> {
        self.upsert_collection_with_context(ThingsMutationContext::local_command(device_id), upsert)
    }

    pub fn upsert_collection_with_context(
        &self,
        context: ThingsMutationContext,
        upsert: ThingCollectionUpsert,
    ) -> Result<ThingsMutationResult<ThingCollectionEntry>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before collection upsert")?;

        let before_snapshot = doc_set.extract_snapshot()?;
        let existing_collection = before_snapshot
            .collections
            .iter()
            .find(|collection| collection.uuid == upsert.uuid)
            .cloned();
        let is_create = existing_collection.is_none();
        let effective_collection_type = if is_create
            || upsert.collection_type != CollectionType::Normal
            || upsert.app_id.is_some()
        {
            upsert.collection_type
        } else {
            existing_collection
                .as_ref()
                .map(|collection| collection.collection_type)
                .unwrap_or_default()
        };
        let effective_app_id = if effective_collection_type == CollectionType::App {
            upsert
                .app_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .or_else(|| existing_collection.and_then(|collection| collection.app_id))
        } else {
            None
        };
        let trigger =
            crate::things_crdt::trigger_update_from_field_patch(upsert.trigger_uuid_patch());
        let mut events = self.ensure_system_collections_in_doc_set(&mut doc_set)?;
        events.extend(doc_set.update_collection_meta_with_timestamps(
            &upsert.uuid,
            Some(upsert.title.clone()),
            None,
            trigger,
            upsert.created_at.clone(),
            upsert.updated_at.clone(),
        )?);
        let existing_attrs = doc_set
            .collection_view(&upsert.uuid)
            .ok()
            .and_then(|view| view.meta.attrs);
        events.extend(doc_set.update_collection_attrs(
            &upsert.uuid,
            Some(collection_metadata_attrs(
                existing_attrs,
                effective_collection_type,
                effective_app_id.as_deref(),
                None,
            )),
        )?);
        let snapshot = doc_set.extract_snapshot()?;
        let created = snapshot
            .collections
            .iter()
            .find(|collection| collection.uuid == upsert.uuid)
            .cloned()
            .ok_or_else(|| anyhow!("Collection not found after upsert"))?;

        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, created)?;

        let op_type = if is_create {
            ThingsOperationType::CreateCollection
        } else {
            ThingsOperationType::UpdateCollection
        };
        let summary = if is_create {
            format!("Created collection '{}'", result.value.title)
        } else {
            format!("Updated collection '{}'", result.value.title)
        };
        let details = json!({
            "uuid": upsert.uuid,
            "title": upsert.title,
            "collection_type": effective_collection_type.as_str(),
            "app_id": effective_app_id,
            "trigger_uuid": upsert.trigger_uuid,
        });

        self.record_user_change_log(
            &context,
            &mut result.change_log_ids,
            op_type,
            "collection",
            &result.value.uuid,
            &summary,
            &details.to_string(),
            None,
            true,
        );

        Ok(result)
    }

    pub fn delete_collection(
        &self,
        device_id: &str,
        uuid: &str,
    ) -> Result<ThingsMutationResult<ThingsDeleteCollectionOutcome>> {
        self.delete_collection_with_context(ThingsMutationContext::local_command(device_id), uuid)
    }

    pub fn delete_collection_with_context(
        &self,
        context: ThingsMutationContext,
        uuid: &str,
    ) -> Result<ThingsMutationResult<ThingsDeleteCollectionOutcome>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before collection delete")?;
        let mut events = self.ensure_system_collections_in_doc_set(&mut doc_set)?;
        let before = doc_set.extract_snapshot()?;
        let Some(collection) = before
            .collections
            .iter()
            .find(|collection| collection.uuid == uuid)
        else {
            return Ok(ThingsMutationResult {
                value: ThingsDeleteCollectionOutcome::default(),
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        };
        if collection.is_system_collection() || collection.archived_at.is_some() {
            return Ok(ThingsMutationResult {
                value: ThingsDeleteCollectionOutcome::default(),
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        }
        let collection_title = collection.title.clone();

        let things_in_collection: Vec<_> = before
            .things
            .iter()
            .filter(|thing| thing.collection_uuid == uuid)
            .cloned()
            .collect();

        let archived_at = format_domain_datetime(Utc::now());
        let existing_attrs = doc_set.collection_view(uuid)?.meta.attrs;
        events.extend(doc_set.update_collection_attrs(
            uuid,
            Some(collection_metadata_attrs(
                existing_attrs,
                collection.collection_type,
                collection.app_id.as_deref(),
                Some(&archived_at),
            )),
        )?);
        let mut result = self.pipeline.commit_local_documents(
            &context,
            &mut doc_set,
            events,
            ThingsDeleteCollectionOutcome {
                deleted: true,
                removed_triggers: Vec::new(),
            },
        )?;

        let summary = format!("Archived collection '{}'", collection_title);
        let details = json!({
            "uuid": uuid,
            "title": collection_title,
            "archived": true,
            "archived_at": archived_at,
            "things_count": things_in_collection.len(),
        });
        self.record_user_change_log(
            &context,
            &mut result.change_log_ids,
            ThingsOperationType::DeleteCollection,
            "collection",
            uuid,
            &summary,
            &details.to_string(),
            None,
            true,
        );

        Ok(result)
    }

    pub fn restore_collection(
        &self,
        device_id: &str,
        uuid: &str,
    ) -> Result<ThingsMutationResult<Option<ThingCollectionEntry>>> {
        self.restore_collection_with_context(ThingsMutationContext::local_command(device_id), uuid)
    }

    pub fn restore_collection_with_context(
        &self,
        context: ThingsMutationContext,
        uuid: &str,
    ) -> Result<ThingsMutationResult<Option<ThingCollectionEntry>>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before collection restore")?;
        let mut events = self.ensure_system_collections_in_doc_set(&mut doc_set)?;
        let before = doc_set.extract_snapshot()?;
        let Some(collection) = before
            .collections
            .iter()
            .find(|collection| collection.uuid == uuid)
            .cloned()
        else {
            return Ok(ThingsMutationResult {
                value: None,
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        };
        if collection.archived_at.is_none() {
            return Ok(ThingsMutationResult {
                value: Some(collection),
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        }

        let existing_attrs = doc_set.collection_view(uuid)?.meta.attrs;
        events.extend(doc_set.update_collection_attrs(
            uuid,
            Some(collection_metadata_attrs(
                existing_attrs,
                collection.collection_type,
                collection.app_id.as_deref(),
                None,
            )),
        )?);
        let snapshot = doc_set.extract_snapshot()?;
        let restored = snapshot
            .collections
            .into_iter()
            .find(|collection| collection.uuid == uuid);
        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, restored)?;

        let details = json!({
            "uuid": uuid,
            "title": collection.title.clone(),
            "restored": true,
        });
        let summary = format!("Restored collection '{}'", collection.title);
        self.record_user_change_log(
            &context,
            &mut result.change_log_ids,
            ThingsOperationType::UpdateCollection,
            "collection",
            uuid,
            &summary,
            &details.to_string(),
            None,
            true,
        );

        Ok(result)
    }
}
