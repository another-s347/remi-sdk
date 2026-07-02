use super::*;

fn collect_descendant_things(snapshot: &ThingsSnapshot, root_uuid: &str) -> Vec<ThingEntry> {
    let mut descendants = Vec::new();
    let mut stack = vec![root_uuid.to_string()];
    while let Some(parent_uuid) = stack.pop() {
        for child in snapshot
            .things
            .iter()
            .filter(|thing| thing.parent_uuid.as_deref() == Some(parent_uuid.as_str()))
        {
            descendants.push(child.clone());
            stack.push(child.uuid.clone());
        }
    }
    descendants
}

impl<'a> ThingsLocalService<'a> {
    pub fn upsert_thing(
        &self,
        device_id: &str,
        upsert: ThingUpsert,
    ) -> Result<ThingsMutationResult<ThingEntry>> {
        self.upsert_thing_with_context(ThingsMutationContext::local_command(device_id), upsert)
    }

    pub fn upsert_thing_with_context(
        &self,
        context: ThingsMutationContext,
        mut upsert: ThingUpsert,
    ) -> Result<ThingsMutationResult<ThingEntry>> {
        if upsert.collection_uuid.trim().is_empty() {
            upsert.collection_uuid = SYSTEM_DEFAULT_COLLECTION_ID.to_string();
        }

        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before thing upsert")?;
        let mut events = self.ensure_system_collections_in_doc_set(&mut doc_set)?;

        let before_snapshot = doc_set.extract_snapshot()?;
        let existing = before_snapshot
            .things
            .iter()
            .find(|thing| thing.uuid == upsert.uuid);

        let (is_create, old_collection_uuid) = match existing {
            Some(thing) => (false, Some(thing.collection_uuid.clone())),
            None => match doc_set.find_thing_collection_uuid(&upsert.uuid) {
                Some(collection_uuid) => (false, Some(collection_uuid)),
                None => (true, None),
            },
        };
        let old_content_json = existing.and_then(|thing| serde_json::to_string(thing).ok());

        let is_move = !is_create
            && old_collection_uuid
                .as_ref()
                .map_or(false, |old| *old != upsert.collection_uuid);

        if is_move {
            let old_collection = old_collection_uuid.as_ref().unwrap();
            tracing::info!(
                thing_uuid = upsert.uuid,
                from_collection = old_collection.as_str(),
                to_collection = upsert.collection_uuid.as_str(),
                "things_upsert_thing: moving thing - tombstoning in source collection"
            );
            events.extend(doc_set.delete_thing(old_collection, &upsert.uuid)?);
        }

        let trigger =
            crate::things_crdt::trigger_update_from_field_patch(upsert.trigger_uuid_patch());
        events.extend(doc_set.upsert_thing_meta_with_timestamps(
            &upsert.collection_uuid,
            &upsert.uuid,
            Some(upsert.datatype.clone()),
            None,
            Some(upsert.title.clone()),
            upsert.parent_uuid.clone(),
            trigger,
            upsert.created_at.clone(),
            upsert.updated_at.clone(),
        )?);

        if let Some(ref data) = upsert.data {
            events.extend(doc_set.set_thing_content_from_payload(
                &upsert.uuid,
                &upsert.datatype,
                data,
            )?);
            events.extend(doc_set.sync_content_entries_from_snapshot_payload(
                &upsert.collection_uuid,
                &upsert.uuid,
                data,
            )?);
        }

        let snapshot = doc_set.extract_snapshot()?;
        let created = snapshot
            .things
            .iter()
            .find(|thing| thing.uuid == upsert.uuid)
            .cloned()
            .ok_or_else(|| anyhow!("Thing not found after upsert"))?;

        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, created)?;

        let op_type = if is_create {
            ThingsOperationType::CreateThing
        } else if is_move {
            ThingsOperationType::MoveThing
        } else {
            ThingsOperationType::UpdateThing
        };
        let summary = if is_create {
            format!("Created thing '{}'", result.value.title)
        } else if is_move {
            format!("Moved thing '{}'", result.value.title)
        } else {
            format!("Updated thing '{}'", result.value.title)
        };
        let details = if is_move {
            json!({
                "uuid": upsert.uuid,
                "title": upsert.title,
                "from_collection_uuid": old_collection_uuid,
                "to_collection_uuid": upsert.collection_uuid,
                "collection_uuid": upsert.collection_uuid,
                "datatype": format!("{:?}", upsert.datatype),
            })
        } else {
            json!({
                "uuid": upsert.uuid,
                "title": upsert.title,
                "collection_uuid": upsert.collection_uuid,
                "datatype": format!("{:?}", upsert.datatype),
            })
        };

        let log_id = if !is_create {
            if context.change_log_policy == ChangeLogPolicy::RecordUserVisible {
                if let Ok(Some(recent)) =
                    self.storage.find_recent_thing_update_log(&upsert.uuid, 300)
                {
                    Some(recent.id)
                } else {
                    self.record_user_change_log(
                        &context,
                        &mut result.change_log_ids,
                        op_type,
                        "thing",
                        &upsert.uuid,
                        &summary,
                        &details.to_string(),
                        None,
                        true,
                    )
                }
            } else {
                None
            }
        } else {
            self.record_user_change_log(
                &context,
                &mut result.change_log_ids,
                op_type,
                "thing",
                &upsert.uuid,
                &summary,
                &details.to_string(),
                None,
                true,
            )
        };

        if let Some(log_id) = log_id {
            if !result.change_log_ids.contains(&log_id) {
                result.change_log_ids.push(log_id);
            }
            if !is_create
                && self
                    .storage
                    .get_things_content_snapshot_by_log_id(log_id)
                    .ok()
                    .flatten()
                    .is_none()
            {
                if let Some(old_json) = old_content_json {
                    let _ = self.storage.insert_things_content_snapshot(
                        &context.device_id,
                        &upsert.uuid,
                        &old_json,
                        Some(log_id),
                    );
                }
            }
        }

        Ok(result)
    }

    pub fn set_thing_status(
        &self,
        device_id: &str,
        thing_uuid: &str,
        status: &str,
    ) -> Result<ThingsMutationResult<bool>> {
        self.set_thing_status_with_context(
            ThingsMutationContext::local_command(device_id),
            thing_uuid,
            status,
        )
    }

    pub fn set_thing_status_with_context(
        &self,
        context: ThingsMutationContext,
        thing_uuid: &str,
        status: &str,
    ) -> Result<ThingsMutationResult<bool>> {
        let status = ThingStatus::from_str(status).map_err(anyhow::Error::msg)?;
        let status_str = status.as_str();

        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before status update")?;

        let collection_uuid = {
            let snapshot = doc_set.extract_snapshot()?;
            match snapshot
                .things
                .iter()
                .find(|thing| thing.uuid == thing_uuid)
            {
                Some(thing) => thing.collection_uuid.clone(),
                None => {
                    tracing::warn!(
                        thing_uuid,
                        "set_thing_status: thing not in snapshot, scanning collection docs"
                    );
                    doc_set
                        .find_thing_collection_uuid(thing_uuid)
                        .ok_or_else(|| anyhow!("Thing not found: {}", thing_uuid))?
                }
            }
        };

        let timestamp_ms = Some(Utc::now().timestamp_millis());
        let events = doc_set.upsert_thing_meta(
            &collection_uuid,
            thing_uuid,
            None,
            Some(status_str.to_string()),
            None,
            None,
            remi_things_crdt::TriggerUpdate::Noop,
        )?;
        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, true)?;

        let summary = format!("Set thing status to '{}'", status_str);
        let details = json!({
            "uuid": thing_uuid,
            "status": status_str,
            "timestamp_ms": timestamp_ms,
        });
        self.record_user_change_log(
            &context,
            &mut result.change_log_ids,
            ThingsOperationType::UpdateThing,
            "thing",
            thing_uuid,
            &summary,
            &details.to_string(),
            None,
            true,
        );

        Ok(result)
    }

    pub fn delete_thing(
        &self,
        device_id: &str,
        collection_uuid: &str,
        uuid: &str,
    ) -> Result<ThingsMutationResult<bool>> {
        self.delete_thing_with_context(
            ThingsMutationContext::local_command(device_id),
            collection_uuid,
            uuid,
        )
    }

    pub fn delete_thing_with_context(
        &self,
        context: ThingsMutationContext,
        _collection_uuid: &str,
        uuid: &str,
    ) -> Result<ThingsMutationResult<bool>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before thing delete")?;
        let mut events = self.ensure_trash_collection_in_doc_set(&mut doc_set)?;
        let before = doc_set.extract_snapshot()?;
        let Some(thing) = before.things.iter().find(|thing| thing.uuid == uuid) else {
            return Ok(ThingsMutationResult {
                value: false,
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        };
        if thing.collection_uuid == SYSTEM_TRASH_COLLECTION_ID && thing.archived_at.is_some() {
            return Ok(ThingsMutationResult {
                value: false,
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        }
        let thing_title = thing.title.clone();
        let thing_content_json = serde_json::to_string(thing).ok();

        let mut archived_things = Vec::new();
        archived_things.push(thing.clone());
        archived_things.extend(collect_descendant_things(&before, uuid));
        let archived_at = format_domain_datetime(Utc::now());

        for item in &archived_things {
            let source_collection_uuid = item.collection_uuid.clone();
            let entries = if source_collection_uuid != SYSTEM_TRASH_COLLECTION_ID {
                doc_set
                    .get_content_entries(&source_collection_uuid, &item.uuid)
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            if source_collection_uuid != SYSTEM_TRASH_COLLECTION_ID {
                events.extend(doc_set.delete_thing(&source_collection_uuid, &item.uuid)?);
            }
            let target_parent_uuid = if item.uuid == uuid {
                None
            } else {
                item.parent_uuid.clone()
            };
            events.extend(doc_set.upsert_thing_meta(
                SYSTEM_TRASH_COLLECTION_ID,
                &item.uuid,
                Some(item.datatype.clone()),
                Some(item.status.clone()),
                Some(item.title.clone()),
                target_parent_uuid,
                crate::things_crdt::trigger_update_from_field_patch(item.trigger_uuid_patch()),
            )?);
            for entry in entries {
                events.extend(doc_set.add_content_entry(
                    SYSTEM_TRASH_COLLECTION_ID,
                    &item.uuid,
                    entry,
                )?);
            }
            let existing_attrs = doc_set
                .collection_view(SYSTEM_TRASH_COLLECTION_ID)?
                .things
                .into_iter()
                .find(|thing| thing.id == item.uuid)
                .and_then(|thing| thing.attrs);
            events.extend(doc_set.update_thing_attrs(
                SYSTEM_TRASH_COLLECTION_ID,
                &item.uuid,
                Some(thing_archive_attrs(
                    existing_attrs,
                    Some(&archived_at),
                    Some(&source_collection_uuid),
                )),
            )?);
        }

        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, true)?;

        let summary = format!("Archived thing '{}'", thing_title);
        let details = json!({
            "uuid": uuid,
            "title": thing_title,
            "collection_uuid": thing.collection_uuid.clone(),
            "to_collection_uuid": SYSTEM_TRASH_COLLECTION_ID,
            "archived": true,
            "archived_at": archived_at,
            "child_count": archived_things.len().saturating_sub(1),
        });
        let parent_log_id = self.record_user_change_log(
            &context,
            &mut result.change_log_ids,
            ThingsOperationType::DeleteThing,
            "thing",
            uuid,
            &summary,
            &details.to_string(),
            None,
            true,
        );

        if let Some(parent_id) = parent_log_id {
            if let Some(content_json) = thing_content_json {
                let _ = self.storage.insert_things_content_snapshot(
                    &context.device_id,
                    uuid,
                    &content_json,
                    Some(parent_id),
                );
            }

            let mut cascade_ids = Vec::new();
            for child in archived_things.iter().skip(1) {
                let child_summary = format!("Archived thing '{}' (cascade)", child.title);
                let child_details = json!({
                    "uuid": child.uuid,
                    "title": child.title,
                    "parent_uuid": uuid,
                    "to_collection_uuid": SYSTEM_TRASH_COLLECTION_ID,
                    "cascade_reason": "parent_thing_archived",
                });
                if let Some(cascade_id) = self.record_user_change_log(
                    &context,
                    &mut result.change_log_ids,
                    ThingsOperationType::DeleteThing,
                    "thing",
                    &child.uuid,
                    &child_summary,
                    &child_details.to_string(),
                    Some(parent_id),
                    false,
                ) {
                    cascade_ids.push(cascade_id);
                    if let Ok(child_json) = serde_json::to_string(child) {
                        let _ = self.storage.insert_things_content_snapshot(
                            &context.device_id,
                            &child.uuid,
                            &child_json,
                            Some(cascade_id),
                        );
                    }
                }
            }
            if !cascade_ids.is_empty() {
                let _ = self
                    .storage
                    .update_things_change_log_cascade_ids(parent_id, &cascade_ids);
            }
        }

        Ok(result)
    }

    pub fn restore_thing(
        &self,
        device_id: &str,
        thing_uuid: &str,
        target_collection_uuid: Option<String>,
        target_parent_uuid: Option<String>,
    ) -> Result<ThingsMutationResult<Option<ThingEntry>>> {
        self.restore_thing_with_context(
            ThingsMutationContext::local_command(device_id),
            thing_uuid,
            target_collection_uuid,
            target_parent_uuid,
        )
    }

    pub fn restore_thing_with_context(
        &self,
        context: ThingsMutationContext,
        thing_uuid: &str,
        target_collection_uuid: Option<String>,
        target_parent_uuid: Option<String>,
    ) -> Result<ThingsMutationResult<Option<ThingEntry>>> {
        let explicit_target = target_collection_uuid
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        if explicit_target.as_deref() == Some(SYSTEM_TRASH_COLLECTION_ID) {
            anyhow::bail!("restore target collection must not be the trash collection");
        }

        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before thing restore")?;
        let mut events = match explicit_target.as_deref() {
            Some(target) => {
                self.ensure_target_system_collection_in_doc_set(&mut doc_set, target)?
            }
            None => Vec::new(),
        };
        let before = doc_set.extract_snapshot()?;
        let Some(thing) = before
            .things
            .iter()
            .find(|thing| thing.uuid == thing_uuid)
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
        if thing.archived_at.is_none() {
            return Ok(ThingsMutationResult {
                value: Some(thing),
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        }

        let target_collection = match explicit_target {
            Some(target) => {
                let collection = before
                    .collections
                    .iter()
                    .find(|collection| collection.uuid == target)
                    .ok_or_else(|| anyhow!("Restore target collection not found: {target}"))?;
                if collection.archived_at.is_some() {
                    anyhow::bail!("restore target collection is archived: {target}");
                }
                target
            }
            None => thing
                .archived_from_collection_uuid
                .as_deref()
                .filter(|uuid| {
                    before.collections.iter().any(|collection| {
                        collection.uuid == *uuid && collection.archived_at.is_none()
                    })
                })
                .unwrap_or(SYSTEM_DEFAULT_COLLECTION_ID)
                .to_string(),
        };
        events.extend(
            self.ensure_target_system_collection_in_doc_set(&mut doc_set, &target_collection)?,
        );

        let mut restoring_things = Vec::new();
        restoring_things.push(thing.clone());
        restoring_things.extend(collect_descendant_things(&before, thing_uuid));

        for item in &restoring_things {
            let source_collection_uuid = item.collection_uuid.clone();
            let entries = if source_collection_uuid != target_collection {
                doc_set
                    .get_content_entries(&source_collection_uuid, &item.uuid)
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            if source_collection_uuid != target_collection {
                events.extend(doc_set.delete_thing(&source_collection_uuid, &item.uuid)?);
            }
            let target_parent = if item.uuid == thing_uuid {
                target_parent_uuid.clone()
            } else {
                item.parent_uuid.clone()
            };
            events.extend(doc_set.upsert_thing_meta(
                &target_collection,
                &item.uuid,
                Some(item.datatype.clone()),
                Some(item.status.clone()),
                Some(item.title.clone()),
                target_parent,
                crate::things_crdt::trigger_update_from_field_patch(item.trigger_uuid_patch()),
            )?);
            for entry in entries {
                events.extend(doc_set.add_content_entry(&target_collection, &item.uuid, entry)?);
            }
            let existing_attrs = doc_set
                .collection_view(&target_collection)?
                .things
                .into_iter()
                .find(|thing| thing.id == item.uuid)
                .and_then(|thing| thing.attrs);
            events.extend(doc_set.update_thing_attrs(
                &target_collection,
                &item.uuid,
                Some(thing_archive_attrs(existing_attrs, None, None)),
            )?);
        }

        let after = doc_set.extract_snapshot()?;
        let restored = after
            .things
            .into_iter()
            .find(|thing| thing.uuid == thing_uuid);
        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, restored)?;

        let details = json!({
            "uuid": thing_uuid,
            "title": thing.title.clone(),
            "restored": true,
            "to_collection_uuid": target_collection,
            "child_count": restoring_things.len().saturating_sub(1),
        });
        let summary = format!("Restored thing '{}'", thing.title);
        self.record_user_change_log(
            &context,
            &mut result.change_log_ids,
            ThingsOperationType::MoveThing,
            "thing",
            thing_uuid,
            &summary,
            &details.to_string(),
            None,
            true,
        );

        Ok(result)
    }

    pub fn move_thing(
        &self,
        device_id: &str,
        thing_uuid: &str,
        target_collection_uuid: &str,
        target_parent_uuid: Option<String>,
    ) -> Result<ThingsMutationResult<ThingEntry>> {
        self.move_thing_with_context(
            ThingsMutationContext::local_command(device_id),
            thing_uuid,
            target_collection_uuid,
            target_parent_uuid,
        )
    }

    pub fn move_thing_with_context(
        &self,
        context: ThingsMutationContext,
        thing_uuid: &str,
        target_collection_uuid: &str,
        target_parent_uuid: Option<String>,
    ) -> Result<ThingsMutationResult<ThingEntry>> {
        let target_collection_uuid = target_collection_uuid.trim();
        let target_collection_uuid = if target_collection_uuid.is_empty() {
            SYSTEM_DEFAULT_COLLECTION_ID.to_string()
        } else {
            target_collection_uuid.to_string()
        };

        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before thing move")?;
        let mut events =
            self.ensure_target_system_collection_in_doc_set(&mut doc_set, &target_collection_uuid)?;

        let snapshot =
            doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
                include_content: false,
            })?;
        let thing = snapshot
            .things
            .iter()
            .find(|item| item.uuid == thing_uuid)
            .cloned()
            .ok_or_else(|| anyhow!("Thing not found: {thing_uuid}"))?;
        let source_collection_uuid = thing.collection_uuid.clone();
        let is_collection_move = source_collection_uuid != target_collection_uuid;
        let entries = if is_collection_move {
            doc_set
                .get_content_entries(&source_collection_uuid, thing_uuid)
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        if is_collection_move {
            events.extend(doc_set.delete_thing(&source_collection_uuid, thing_uuid)?);
        }

        events.extend(doc_set.upsert_thing_meta(
            &target_collection_uuid,
            thing_uuid,
            Some(thing.datatype.clone()),
            Some(thing.status.clone()),
            Some(thing.title.clone()),
            target_parent_uuid.clone(),
            crate::things_crdt::trigger_update_from_field_patch(thing.trigger_uuid_patch()),
        )?);

        for entry in entries {
            events.extend(doc_set.add_content_entry(&target_collection_uuid, thing_uuid, entry)?);
        }

        let after = doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
            include_content: false,
        })?;
        let moved = after
            .things
            .iter()
            .find(|item| item.uuid == thing_uuid)
            .cloned()
            .ok_or_else(|| anyhow!("Thing not found after move: {thing_uuid}"))?;

        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, moved)?;

        let details = json!({
            "uuid": thing_uuid,
            "title": thing.title,
            "from_collection_uuid": source_collection_uuid,
            "to_collection_uuid": target_collection_uuid,
            "to_parent_uuid": target_parent_uuid,
        });
        self.record_user_change_log(
            &context,
            &mut result.change_log_ids,
            ThingsOperationType::MoveThing,
            "thing",
            thing_uuid,
            &format!("Moved thing '{}'", result.value.title),
            &details.to_string(),
            None,
            true,
        );

        Ok(result)
    }

    pub fn move_thing_with_updates(
        &self,
        device_id: &str,
        thing_uuid: &str,
        target_collection_uuid: &str,
        target_parent_uuid: Option<String>,
        title: Option<String>,
        datatype: Option<ThingDatatype>,
    ) -> Result<ThingsMutationResult<ThingEntry>> {
        let context = ThingsMutationContext::local_command(device_id);
        let mut result = self.move_thing_with_context(
            context.clone(),
            thing_uuid,
            target_collection_uuid,
            target_parent_uuid,
        )?;

        if title.is_none() && datatype.is_none() {
            return Ok(result);
        }

        let moved = result.value.clone();
        let update = ThingUpsert {
            uuid: moved.uuid,
            title: title.unwrap_or(moved.title),
            datatype: datatype.unwrap_or(moved.datatype),
            data: None,
            collection_uuid: moved.collection_uuid,
            trigger_uuid: moved.trigger_uuid,
            trigger_uuid_patch: Default::default(),
            parent_uuid: moved.parent_uuid,
            created_at: None,
            updated_at: None,
        };
        let update_result = self.upsert_thing_with_context(context, update)?;

        result.value = update_result.value;
        result.events.extend(update_result.events);
        result.dirty_documents.extend(update_result.dirty_documents);
        result.change_log_ids.extend(update_result.change_log_ids);
        result.event_range = merge_event_ranges(result.event_range, update_result.event_range);
        Ok(result)
    }

    pub fn set_status(
        &self,
        device_id: &str,
        thing_uuid: &str,
        status: &str,
        timestamp_ms: Option<i64>,
    ) -> Result<ThingsMutationResult<String>> {
        self.set_status_with_context(
            ThingsMutationContext::local_command(device_id),
            thing_uuid,
            status,
            timestamp_ms,
        )
    }

    pub fn set_status_with_context(
        &self,
        context: ThingsMutationContext,
        thing_uuid: &str,
        status: &str,
        timestamp_ms: Option<i64>,
    ) -> Result<ThingsMutationResult<String>> {
        let status = ThingStatus::from_str(status).map_err(anyhow::Error::msg)?;
        let status_str = status.as_str();

        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before status update")?;

        let before = doc_set.extract_snapshot()?;
        let (thing_title, collection_uuid) =
            match before.things.iter().find(|thing| thing.uuid == thing_uuid) {
                Some(thing) => (thing.title.clone(), thing.collection_uuid.clone()),
                None => {
                    tracing::warn!(
                        thing_uuid,
                        "things_set_status: thing not in snapshot, scanning collection docs"
                    );
                    let collection_uuid = doc_set
                        .find_thing_collection_uuid(thing_uuid)
                        .ok_or_else(|| anyhow!("Thing not found: {}", thing_uuid))?;
                    (String::new(), collection_uuid)
                }
            };

        let events = doc_set.upsert_thing_meta(
            &collection_uuid,
            thing_uuid,
            None,
            Some(status_str.to_string()),
            None,
            None,
            remi_things_crdt::TriggerUpdate::Noop,
        )?;
        let value = format!("Status updated: {} -> {}", thing_title, status_str);
        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, value)?;

        let summary = format!("Changed status of '{}' to '{}'", thing_title, status_str);
        let details = json!({
            "uuid": thing_uuid,
            "title": thing_title,
            "collection_uuid": collection_uuid,
            "status": status_str,
            "timestamp_ms": timestamp_ms,
        });
        let details_json = serde_json::to_string(&details).unwrap_or_default();
        self.record_user_change_log(
            &context,
            &mut result.change_log_ids,
            ThingsOperationType::UpdateThing,
            "thing",
            thing_uuid,
            &summary,
            &details_json,
            None,
            false,
        );

        Ok(result)
    }
}
