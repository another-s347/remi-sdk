use super::*;
use crate::things_local::{ThingsLocalEvent, ThingsLocalService};
use crate::types::{EntityActionBinding, ResolvedEntityActionBinding};

impl RemiSdk {
    pub(crate) fn things_local_service(&self) -> ThingsLocalService<'_> {
        ThingsLocalService::with_broadcaster(&self.storage, &self.things_event_tx)
    }

    fn enqueue_search_collection_by_id(&self, device_id: &str, collection_uuid: &str) {
        match self.things_get_collection(device_id, collection_uuid) {
            Ok(Some(collection)) => {
                self.enqueue_search_document(SearchDocument::from_collection(&collection));
            }
            Ok(None) => {
                self.enqueue_search_actions(vec![SearchIngestAction::delete_entity(
                    crate::search::SearchEntityKind::Collection,
                    collection_uuid,
                )]);
            }
            Err(error) => {
                tracing::warn!(
                    device_id = %device_id,
                    collection_uuid = %collection_uuid,
                    error = %error,
                    "Failed to snapshot collection for search index"
                );
            }
        }
    }

    fn enqueue_search_thing_by_id(&self, device_id: &str, thing_uuid: &str) {
        match self.things_get_thing(device_id, thing_uuid, true) {
            Ok(Some(thing)) => {
                self.enqueue_search_document(SearchDocument::from_thing(&thing));
            }
            Ok(None) => {
                self.enqueue_search_actions(vec![SearchIngestAction::delete_entity(
                    crate::search::SearchEntityKind::Thing,
                    thing_uuid,
                )]);
            }
            Err(error) => {
                tracing::warn!(
                    device_id = %device_id,
                    thing_uuid = %thing_uuid,
                    error = %error,
                    "Failed to snapshot thing for search index"
                );
            }
        }
    }

    /// Compatibility entry point for clients that explicitly request a full
    /// Things/Collections refresh event. Sync apply paths emit durable events
    /// through the mutation pipeline directly.
    pub fn emit_snapshot_replace(&self, device_id: &str) -> Result<()> {
        let snapshot = self.things_local_service().snapshot(device_id)?;
        let event = ThingsEvent::SnapshotReplaced {
            device_id: device_id.to_string(),
            collections: snapshot.collections,
            things: snapshot.things,
            dirty: snapshot.dirty,
            last_sync_at: None,
        };
        let context = ThingsMutationContext::maintenance(device_id, DirtyPolicy::MarkClean);
        ThingsMutationPipeline::with_broadcaster(&self.storage, &self.things_event_tx)
            .emit_snapshot_replaced(&context, event)
            .map(|_| ())
    }

    pub fn things_watch_since(
        &self,
        device_id: &str,
        after_event_id: i64,
        limit: u32,
    ) -> Result<Vec<ThingsLocalEvent>> {
        self.things_local_service()
            .watch_since(device_id, after_event_id, limit)
    }

    pub fn things_ack_events(&self, device_id: &str, until_event_id: i64) -> Result<u64> {
        self.things_local_service()
            .ack_events(device_id, until_event_id)
    }

    #[cfg(test)]
    pub(crate) fn things_apply_remote_document(
        &self,
        device_id: &str,
        sync_run_id: &str,
        key: crate::things_crdt::DocumentKey,
        state: crate::things_crdt::DocumentState,
    ) -> Result<Option<(i64, i64)>> {
        self.things_apply_remote_documents(device_id, sync_run_id, vec![(key, state)])
    }

    pub(crate) fn things_apply_remote_documents(
        &self,
        device_id: &str,
        sync_run_id: &str,
        documents: Vec<(
            crate::things_crdt::DocumentKey,
            crate::things_crdt::DocumentState,
        )>,
    ) -> Result<Option<(i64, i64)>> {
        let context = ThingsMutationContext::remote_sync(device_id, sync_run_id);
        let result = ThingsMutationPipeline::with_broadcaster(&self.storage, &self.things_event_tx)
            .apply_remote_documents(&context, documents)?;
        if result.event_range.is_some() {
            self.enqueue_search_rebuild();
        }
        Ok(result.event_range)
    }

    pub(crate) fn things_save_synced_document_clean(
        &self,
        device_id: &str,
        sync_run_id: &str,
        key: crate::things_crdt::DocumentKey,
        automerge_doc: Vec<u8>,
        sync_state: Vec<u8>,
        last_sync_at: Option<&str>,
    ) -> Result<Option<(i64, i64)>> {
        let context = ThingsMutationContext::remote_sync(device_id, sync_run_id);
        let result = ThingsMutationPipeline::with_broadcaster(&self.storage, &self.things_event_tx)
            .save_synced_document_clean(&context, key, automerge_doc, sync_state, last_sync_at)
            .context("Failed to save synced Things CRDT document clean")?;
        if result.event_range.is_some() {
            self.enqueue_search_rebuild();
        }
        Ok(result.event_range)
    }

    pub(crate) fn things_delete_raw_document_for_sync(
        &self,
        device_id: &str,
        sync_run_id: &str,
        key: crate::things_crdt::DocumentKey,
    ) -> Result<Option<(i64, i64)>> {
        let context = ThingsMutationContext::remote_sync(device_id, sync_run_id);
        let result = ThingsMutationPipeline::with_broadcaster(&self.storage, &self.things_event_tx)
            .delete_raw_document(&context, key)
            .context("Failed to delete raw Things CRDT document through pipeline")?;
        if result.event_range.is_some() {
            self.enqueue_search_rebuild();
        }
        Ok(result.event_range)
    }

    pub fn things_list_snapshot(&self, device_id: &str) -> Result<ThingsSnapshotState> {
        self.things_list_snapshot_with_options(
            device_id,
            true,
            crate::things_crdt::SnapshotOptions {
                include_content: true,
            },
        )
    }

    /// Snapshot optimized for agent/tools and context prompts.
    ///
    /// - Omits thing `data.content` entirely.
    /// - Still returns collections + things metadata.
    pub fn things_list_snapshot_lite(&self, device_id: &str) -> Result<ThingsSnapshotState> {
        self.things_list_snapshot_with_options(
            device_id,
            true,
            crate::things_crdt::SnapshotOptions {
                include_content: false,
            },
        )
    }

    /// Flexible snapshot builder for tools/UI.
    ///
    /// Use this to avoid extracting content (and optionally avoid extracting things at all).
    pub fn things_list_snapshot_with_options(
        &self,
        device_id: &str,
        _include_things: bool,
        snapshot_options: crate::things_crdt::SnapshotOptions,
    ) -> Result<ThingsSnapshotState> {
        self.things_local_service()
            .snapshot_with_options(device_id, snapshot_options)
    }

    /// Store a batch of actor attribution metadata (fetched from server) into the local cache.
    pub fn things_upsert_actor_meta(&self, items: &[crate::storage::ActorMetaEntry]) -> Result<()> {
        self.storage
            .upsert_things_actor_meta_batch(items)
            .context("Failed to store actor meta batch")?;
        if !items.is_empty() {
            self.enqueue_search_rebuild();
        }
        Ok(())
    }

    pub fn things_has_pending_changes(&self, device_id: &str) -> Result<bool> {
        self.things_local_service().has_pending_changes(device_id)
    }

    pub fn things_list_collections(&self, device_id: &str) -> Result<Vec<ThingCollectionEntry>> {
        self.things_local_service().list_collections(device_id)
    }

    pub fn things_get_collection(
        &self,
        device_id: &str,
        collection_uuid: &str,
    ) -> Result<Option<ThingCollectionEntry>> {
        self.things_local_service()
            .get_collection(device_id, collection_uuid)
    }

    pub fn things_list_things(
        &self,
        device_id: &str,
        include_content: bool,
    ) -> Result<Vec<ThingEntry>> {
        self.things_local_service()
            .list_things(device_id, include_content)
    }

    pub fn things_get_thing(
        &self,
        device_id: &str,
        thing_uuid: &str,
        include_content: bool,
    ) -> Result<Option<ThingEntry>> {
        self.things_local_service()
            .get_thing(device_id, thing_uuid, include_content)
    }

    pub fn things_upsert_collection(
        &self,
        device_id: &str,
        upsert: ThingCollectionUpsert,
    ) -> Result<ThingCollectionEntry> {
        let result = self
            .things_local_service()
            .upsert_collection(device_id, upsert)?;
        let value = result.value;
        self.enqueue_search_document(SearchDocument::from_collection(&value));
        Ok(value)
    }

    pub fn things_delete_collection(&self, device_id: &str, uuid: &str) -> Result<bool> {
        let result = self
            .things_local_service()
            .delete_collection(device_id, uuid)?;
        let outcome = result.value;
        if !outcome.deleted {
            return Ok(false);
        }

        self.enqueue_search_collection_by_id(device_id, uuid);
        Ok(true)
    }

    pub fn things_restore_collection(
        &self,
        device_id: &str,
        uuid: &str,
    ) -> Result<Option<ThingCollectionEntry>> {
        let result = self
            .things_local_service()
            .restore_collection(device_id, uuid)?;
        if let Some(collection) = &result.value {
            self.enqueue_search_document(SearchDocument::from_collection(collection));
        }
        Ok(result.value)
    }

    pub fn things_ensure_app_collection(
        &self,
        device_id: &str,
        app_id: &str,
        title: Option<String>,
    ) -> Result<ThingCollectionEntry> {
        let result = self
            .things_local_service()
            .ensure_app_collection(device_id, app_id, title)?;
        self.enqueue_search_document(SearchDocument::from_collection(&result.value));
        Ok(result.value)
    }

    pub fn things_upsert_thing(&self, device_id: &str, upsert: ThingUpsert) -> Result<ThingEntry> {
        let result = self
            .things_local_service()
            .upsert_thing(device_id, upsert)?;
        let value = result.value;
        self.enqueue_search_document(SearchDocument::from_thing(&value));
        Ok(value)
    }

    pub fn things_move_thing(
        &self,
        device_id: &str,
        thing_uuid: &str,
        target_collection_uuid: &str,
        target_parent_uuid: Option<String>,
    ) -> Result<ThingEntry> {
        let result = self.things_local_service().move_thing(
            device_id,
            thing_uuid,
            target_collection_uuid,
            target_parent_uuid,
        )?;
        self.enqueue_search_document(SearchDocument::from_thing(&result.value));
        Ok(result.value)
    }

    pub fn things_move_thing_with_updates(
        &self,
        device_id: &str,
        thing_uuid: &str,
        target_collection_uuid: &str,
        target_parent_uuid: Option<String>,
        title: Option<String>,
        datatype: Option<ThingDatatype>,
    ) -> Result<ThingEntry> {
        let result = self.things_local_service().move_thing_with_updates(
            device_id,
            thing_uuid,
            target_collection_uuid,
            target_parent_uuid,
            title,
            datatype,
        )?;
        self.enqueue_search_document(SearchDocument::from_thing(&result.value));
        Ok(result.value)
    }

    pub fn things_splice_text(
        &self,
        device_id: &str,
        thing_uuid: &str,
        block_id: &str,
        index: usize,
        delete: usize,
        insert: &str,
    ) -> Result<bool> {
        let result = self
            .things_local_service()
            .splice_text(device_id, thing_uuid, block_id, index, delete, insert)?;
        let value = result.value;
        if value {
            self.enqueue_search_thing_by_id(device_id, thing_uuid);
        }
        Ok(value)
    }

    /// Get the markdown content of a thing.
    /// Returns the markdown text content, or None if the thing doesn't exist or has no markdown content.
    pub fn things_get_thing_markdown(
        &self,
        device_id: &str,
        thing_uuid: &str,
    ) -> Result<Option<String>> {
        self.things_local_service()
            .get_thing_markdown(device_id, thing_uuid)
    }

    /// Edit the content of a thing using editor-level operations.
    ///
    /// # Operations
    /// - `overwrite`: Replace all content with `new_content`
    /// - `set_title`: Only change the title (ignores content fields)
    /// - `str_replace`: Find `old_str` and replace with `new_str` (must match exactly once)
    /// - `insert_at_line`: Insert `insert_text` after line `line_number` (1-based, 0 = prepend)
    /// - `append`: Append `append_text` to the end
    ///
    /// # Returns
    /// JSON result with success status, or error with current content for retry
    pub fn things_edit_content(
        &self,
        device_id: &str,
        thing_uuid: &str,
        operation: &str,
        // Optional fields depending on operation
        new_title: Option<&str>,
        new_content: Option<&str>,
        old_str: Option<&str>,
        new_str: Option<&str>,
        line_number: Option<usize>,
        insert_text: Option<&str>,
        append_text: Option<&str>,
    ) -> Result<String> {
        let result = self.things_local_service().edit_content(
            device_id,
            thing_uuid,
            operation,
            new_title,
            new_content,
            old_str,
            new_str,
            line_number,
            insert_text,
            append_text,
        )?;
        let value = result.value;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(value)
    }

    /// Set the status of a thing.
    /// Status values: "none", "in-progress", "stalled", "done"
    pub fn set_thing_status(
        &self,
        device_id: &str,
        thing_uuid: &str,
        status: &str,
    ) -> Result<bool> {
        let result = self
            .things_local_service()
            .set_thing_status(device_id, thing_uuid, status)?;
        let value = result.value;
        if value {
            self.enqueue_search_thing_by_id(device_id, thing_uuid);
        }
        Ok(value)
    }

    pub fn things_delete_thing(
        &self,
        device_id: &str,
        collection_uuid: &str,
        uuid: &str,
    ) -> Result<bool> {
        let result = self
            .things_local_service()
            .delete_thing(device_id, collection_uuid, uuid)?;
        let value = result.value;
        if value {
            self.enqueue_search_thing_by_id(device_id, uuid);
        }
        Ok(value)
    }

    pub fn things_restore_thing(
        &self,
        device_id: &str,
        thing_uuid: &str,
        target_collection_uuid: Option<String>,
        target_parent_uuid: Option<String>,
    ) -> Result<Option<ThingEntry>> {
        let result = self.things_local_service().restore_thing(
            device_id,
            thing_uuid,
            target_collection_uuid,
            target_parent_uuid,
        )?;
        if let Some(thing) = &result.value {
            self.enqueue_search_document(SearchDocument::from_thing(thing));
        }
        Ok(result.value)
    }

    pub fn things_set_status(
        &self,
        device_id: &str,
        thing_uuid: &str,
        status: &str,
        timestamp_ms: Option<i64>,
    ) -> Result<String> {
        let result =
            self.things_local_service()
                .set_status(device_id, thing_uuid, status, timestamp_ms)?;
        let value = result.value;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(value)
    }

    pub fn list_collection_action_bindings(
        &self,
        device_id: &str,
        collection_uuid: &str,
    ) -> Result<Vec<EntityActionBinding>> {
        self.things_local_service()
            .list_collection_action_bindings(device_id, collection_uuid)
    }

    pub fn list_thing_action_bindings(
        &self,
        device_id: &str,
        thing_uuid: &str,
    ) -> Result<Vec<EntityActionBinding>> {
        self.things_local_service()
            .list_thing_action_bindings(device_id, thing_uuid)
    }

    pub fn resolve_collection_action_bindings(
        &self,
        device_id: &str,
        collection_uuid: &str,
    ) -> Result<Vec<ResolvedEntityActionBinding>> {
        self.resolve_entity_action_bindings(
            self.list_collection_action_bindings(device_id, collection_uuid)?,
        )
    }

    pub fn resolve_thing_action_bindings(
        &self,
        device_id: &str,
        thing_uuid: &str,
    ) -> Result<Vec<ResolvedEntityActionBinding>> {
        self.resolve_entity_action_bindings(self.list_thing_action_bindings(device_id, thing_uuid)?)
    }

    fn resolve_entity_action_bindings(
        &self,
        bindings: Vec<EntityActionBinding>,
    ) -> Result<Vec<ResolvedEntityActionBinding>> {
        let mut resolved = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let action = self.fetch_action(&binding.action_uuid)?;
            resolved.push(ResolvedEntityActionBinding {
                action_uuid: binding.action_uuid,
                label_override: binding.label_override,
                args_json: binding.args_json,
                action_title: action.as_ref().map(|item| item.title.clone()),
                action_description: action.as_ref().map(|item| item.description.clone()),
                action_enabled: action.as_ref().map(|item| item.enabled),
                action_missing: action.is_none(),
            });
        }
        Ok(resolved)
    }

    pub fn things_set_collection_action_bindings(
        &self,
        device_id: &str,
        collection_uuid: &str,
        bindings: &[EntityActionBinding],
    ) -> Result<()> {
        self.things_local_service().set_collection_action_bindings(
            device_id,
            collection_uuid,
            bindings,
        )?;
        self.enqueue_search_collection_by_id(device_id, collection_uuid);
        Ok(())
    }

    pub fn get_collection_card_jsx(
        &self,
        device_id: &str,
        collection_uuid: &str,
    ) -> Result<Option<String>> {
        self.things_local_service()
            .get_collection_card_jsx(device_id, collection_uuid)
    }

    pub fn things_set_collection_card_jsx(
        &self,
        device_id: &str,
        collection_uuid: &str,
        card_jsx: Option<&str>,
    ) -> Result<()> {
        self.things_local_service().set_collection_card_jsx(
            device_id,
            collection_uuid,
            card_jsx,
        )?;
        self.enqueue_search_collection_by_id(device_id, collection_uuid);
        Ok(())
    }

    pub fn things_set_thing_action_bindings(
        &self,
        device_id: &str,
        thing_uuid: &str,
        bindings: &[EntityActionBinding],
    ) -> Result<()> {
        self.things_local_service()
            .set_thing_action_bindings(device_id, thing_uuid, bindings)?;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(())
    }

    pub fn execute_collection_action_now(
        &self,
        device_id: &str,
        collection_uuid: &str,
        action_uuid: &str,
    ) -> Result<ActionInvocationRecord> {
        let binding = self
            .list_collection_action_bindings(device_id, collection_uuid)?
            .into_iter()
            .find(|item| item.action_uuid == action_uuid)
            .ok_or_else(|| {
                anyhow!(
                    "Collection '{}' is not bound to action '{}'",
                    collection_uuid,
                    action_uuid
                )
            })?;
        self.execute_action_now(
            action_uuid,
            ActionInvocationSourceKind::CollectionManual,
            Some("collection"),
            Some(collection_uuid),
            binding.args_json,
            Some(device_id),
        )
    }

    pub fn execute_thing_action_now(
        &self,
        device_id: &str,
        thing_uuid: &str,
        action_uuid: &str,
    ) -> Result<ActionInvocationRecord> {
        let binding = self
            .list_thing_action_bindings(device_id, thing_uuid)?
            .into_iter()
            .find(|item| item.action_uuid == action_uuid)
            .ok_or_else(|| {
                anyhow!(
                    "Thing '{}' is not bound to action '{}'",
                    thing_uuid,
                    action_uuid
                )
            })?;
        self.execute_action_now(
            action_uuid,
            ActionInvocationSourceKind::ThingManual,
            Some("thing"),
            Some(thing_uuid),
            binding.args_json,
            Some(device_id),
        )
    }

    /// Add a content block to a thing (V3 multi-value).
    ///
    /// # Arguments
    /// * `device_id` - Device identifier
    /// * `thing_uuid` - Thing UUID
    /// * `block_json` - JSON string of content block
    ///
    /// Block JSON format:
    /// ```json
    /// {
    ///   "id": "block-uuid",
    ///   "title": "Optional title",
    ///   "order": 0.0,
    ///   "payload": {
    ///     "type": "location",
    ///     "loc_type": "coordinate",
    ///     "lat": 39.9,
    ///     "lng": 116.4,
    ///     "coord_system": "wgs84"
    ///   }
    /// }
    /// ```
    pub fn things_add_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry: ContentEntry,
    ) -> Result<String> {
        let result = self
            .things_local_service()
            .add_content_entry(device_id, thing_uuid, entry)?;
        let value = result.value;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(value)
    }

    /// Update a content entry on a thing (V3 multi-value).
    ///
    /// # Arguments
    /// * `device_id` - Device identifier
    /// * `thing_uuid` - Thing UUID
    /// * `update` - Typed fields to update
    ///
    pub fn things_update_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        update: ContentEntryUpdate,
    ) -> Result<()> {
        self.things_local_service()
            .update_content_entry(device_id, thing_uuid, update)?;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(())
    }

    /// Delete a content entry from a thing (V3 multi-value).
    pub fn things_delete_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<()> {
        self.things_local_service()
            .delete_content_entry(device_id, thing_uuid, entry_id)?;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(())
    }

    pub fn things_add_json_object_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        title: Option<&str>,
        data: Option<&Value>,
        schema: Option<&Value>,
    ) -> Result<String> {
        let result = self
            .things_local_service()
            .add_json_object_content_entry(device_id, thing_uuid, title, data, schema)?;
        let value = result.value;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(value)
    }

    pub fn things_get_json_object_entry_data(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<Option<Value>> {
        self.things_local_service()
            .read_json_object_entry_data(device_id, thing_uuid, entry_id)
    }

    pub fn things_get_json_object_entry_schema(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<Option<Value>> {
        self.things_local_service()
            .read_json_object_entry_schema(device_id, thing_uuid, entry_id)
    }

    pub fn things_set_json_object_entry_data(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
        data: &Value,
    ) -> Result<()> {
        self.things_local_service()
            .set_json_object_entry_data(device_id, thing_uuid, entry_id, data)?;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(())
    }

    pub fn things_set_json_object_entry_schema(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
        schema: Option<&Value>,
    ) -> Result<()> {
        self.things_local_service()
            .set_json_object_entry_schema(device_id, thing_uuid, entry_id, schema)?;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(())
    }

    pub fn things_update_json_object_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
        title: Option<Option<String>>,
        data: &Value,
        schema: Option<&Value>,
    ) -> Result<()> {
        self.things_local_service()
            .update_json_object_entry(device_id, thing_uuid, entry_id, title, data, schema)?;
        self.enqueue_search_thing_by_id(device_id, thing_uuid);
        Ok(())
    }

    /// Get all content entries of a thing.
    pub fn things_get_content_entries(
        &self,
        device_id: &str,
        thing_uuid: &str,
    ) -> Result<Vec<ContentEntry>> {
        self.things_local_service()
            .list_content_entries(device_id, thing_uuid)
    }

    // ===== Things Change Log API =====

    /// List recent change log entries with pagination.
    /// Returns entries ordered by created_at DESC (newest first).
    pub fn things_list_change_log(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<ThingsChangeLogEntry>> {
        self.things_local_service().list_change_log(limit, offset)
    }

    /// List change log entries for a specific entity.
    pub fn things_list_change_log_for_entity(
        &self,
        entity_type: &str,
        entity_uuid: &str,
        limit: u32,
    ) -> Result<Vec<ThingsChangeLogEntry>> {
        self.things_local_service()
            .list_change_log_for_entity(entity_type, entity_uuid, limit)
    }

    /// Get a single change log entry by ID.
    pub fn things_get_change_log(&self, log_id: i64) -> Result<Option<ThingsChangeLogEntry>> {
        self.things_local_service().get_change_log(log_id)
    }

    /// Get content snapshots for a thing (version history).
    pub fn things_list_content_snapshots(
        &self,
        thing_uuid: &str,
        limit: u32,
    ) -> Result<Vec<ThingsContentSnapshot>> {
        self.things_local_service()
            .list_content_snapshots(thing_uuid, limit)
    }

    /// Preview an undo operation without executing it.
    /// Returns information about what would be undone and any conflicts.
    pub fn things_preview_undo(&self, device_id: &str, log_id: i64) -> Result<ThingsUndoPreview> {
        self.things_local_service().preview_undo(device_id, log_id)
    }

    /// Execute an undo operation.
    pub fn things_execute_undo(
        &self,
        device_id: &str,
        execution: ThingsUndoExecution,
    ) -> Result<String> {
        let result = self
            .things_local_service()
            .execute_undo(device_id, execution)?;
        self.enqueue_search_rebuild();
        Ok(result)
    }

    /// Cleanup old change logs and snapshots (retention policy).
    pub fn things_cleanup_change_logs(&self, older_than_days: i64) -> Result<(u64, u64)> {
        self.things_local_service()
            .cleanup_change_logs(older_than_days)
    }
}
