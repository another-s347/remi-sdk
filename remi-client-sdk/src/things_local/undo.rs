use super::*;

impl<'a> ThingsLocalService<'a> {
    pub fn preview_undo(&self, device_id: &str, log_id: i64) -> Result<ThingsUndoPreview> {
        let log_entry = self
            .storage
            .get_things_change_log(log_id)?
            .ok_or_else(|| anyhow!("Change log entry not found: {}", log_id))?;

        if !log_entry.can_undo {
            return Err(anyhow!("This operation cannot be undone"));
        }

        let snapshot = self.load_document_set(device_id)?.extract_snapshot()?;
        let cascade_entries = self.storage.get_things_change_log_cascades(log_id)?;
        let conflict = check_undo_conflict(&log_entry, &snapshot)?;

        Ok(ThingsUndoPreview {
            log_entry,
            needs_cascade_restore: !cascade_entries.is_empty(),
            conflict,
            cascade_entries,
        })
    }

    pub fn execute_undo(&self, device_id: &str, execution: ThingsUndoExecution) -> Result<String> {
        let preview = self.preview_undo(device_id, execution.log_id)?;
        let log_entry = &preview.log_entry;

        if let Some(conflict) = &preview.conflict {
            if execution.resolution_option.is_none() {
                return Err(anyhow!(
                    "Conflict detected: {}. Please provide a resolution option.",
                    conflict.description
                ));
            }
        }

        let message = match log_entry.op_type {
            ThingsOperationType::CreateCollection => {
                self.delete_collection(device_id, &log_entry.entity_uuid)?;
                format!("Undone: {}", log_entry.summary)
            }
            ThingsOperationType::CreateThing => {
                let details: serde_json::Value =
                    serde_json::from_str(&log_entry.details_json).unwrap_or_default();
                let collection_uuid = details["collection_uuid"].as_str().unwrap_or("");
                self.delete_thing(device_id, collection_uuid, &log_entry.entity_uuid)?;
                format!("Undone: {}", log_entry.summary)
            }
            ThingsOperationType::DeleteCollection => {
                self.restore_deleted_collection(device_id, log_entry, &preview)?
            }
            ThingsOperationType::DeleteThing => {
                self.restore_deleted_thing(device_id, log_entry, &execution, &preview)?
            }
            ThingsOperationType::UpdateCollection | ThingsOperationType::UpdateThing => {
                self.restore_from_snapshot(device_id, log_entry)?
            }
            ThingsOperationType::MoveThing => self.undo_move_thing(device_id, log_entry)?,
            ThingsOperationType::MoveThings => {
                self.undo_move_things(device_id, log_entry, &preview)?
            }
            ThingsOperationType::DeleteThings => {
                self.undo_delete_things(device_id, log_entry, &preview)?
            }
            _ => {
                return Err(anyhow!(
                    "Undo not supported for operation type: {:?}",
                    log_entry.op_type
                ));
            }
        };

        self.record_undo_change_log(device_id, log_entry, &message)?;
        Ok(message)
    }

    fn snapshot_for_undo(&self, device_id: &str) -> Result<ThingsSnapshot> {
        let snapshot_state = self.snapshot(device_id)?;
        Ok(ThingsSnapshot {
            collections: snapshot_state.collections,
            things: snapshot_state.things,
        })
    }

    fn restore_deleted_collection(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        preview: &ThingsUndoPreview,
    ) -> Result<String> {
        let details: serde_json::Value =
            serde_json::from_str(&log_entry.details_json).unwrap_or_default();
        let title = details["title"].as_str().unwrap_or("Restored Collection");

        self.upsert_collection(
            device_id,
            ThingCollectionUpsert {
                uuid: log_entry.entity_uuid.clone(),
                title: title.to_string(),
                collection_type: Default::default(),
                app_id: details
                    .get("app_id")
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string()),
                created_at: None,
                updated_at: None,
            },
        )?;

        for cascade_entry in &preview.cascade_entries {
            if cascade_entry.entity_type == "thing" {
                if let Some(snapshot) = self
                    .storage
                    .get_things_content_snapshot_by_log_id(cascade_entry.id)?
                {
                    if let Ok(thing_data) =
                        serde_json::from_str::<ThingEntry>(&snapshot.content_json)
                    {
                        self.upsert_thing(
                            device_id,
                            ThingUpsert {
                                uuid: thing_data.uuid,
                                title: thing_data.title,
                                datatype: thing_data.datatype,
                                data: Some(thing_data.data),
                                collection_uuid: thing_data.collection_uuid,
                                parent_uuid: thing_data.parent_uuid,
                                created_at: None,
                                updated_at: None,
                            },
                        )?;
                    }
                }
            }
        }

        Ok(format!("Restored collection '{}' and its contents", title))
    }

    fn restore_deleted_thing(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        execution: &ThingsUndoExecution,
        preview: &ThingsUndoPreview,
    ) -> Result<String> {
        let content_snapshot = self
            .storage
            .get_things_content_snapshot_by_log_id(log_entry.id)?
            .ok_or_else(|| anyhow!("No content snapshot found for deleted thing"))?;

        let thing_data: ThingEntry = serde_json::from_str(&content_snapshot.content_json)
            .context("Failed to parse thing snapshot")?;

        let snapshot = self.snapshot_for_undo(device_id)?;
        let parent_exists = snapshot
            .collections
            .iter()
            .any(|c| c.uuid == thing_data.collection_uuid);

        let target_collection = if !parent_exists {
            match execution.resolution_option.as_deref() {
                Some("cascade_restore") => {
                    return Err(anyhow!(
                        "Cascade restore of parent collection not yet implemented. Use 'move_to_other' instead."
                    ));
                }
                Some("move_to_other") => execution
                    .target_collection_uuid
                    .as_ref()
                    .ok_or_else(|| anyhow!("Target collection UUID required for move_to_other"))?
                    .clone(),
                Some("cancel") => {
                    return Err(anyhow!("Undo cancelled by user"));
                }
                _ => {
                    return Err(anyhow!(
                        "Parent collection deleted. Please provide a resolution option."
                    ));
                }
            }
        } else {
            thing_data.collection_uuid.clone()
        };
        let restored_thing_uuid = thing_data.uuid.clone();
        let restored_thing_title = thing_data.title.clone();

        self.upsert_thing(
            device_id,
            ThingUpsert {
                uuid: thing_data.uuid,
                title: thing_data.title,
                datatype: thing_data.datatype,
                data: Some(thing_data.data),
                collection_uuid: target_collection,
                parent_uuid: thing_data.parent_uuid,
                created_at: None,
                updated_at: None,
            },
        )?;

        for cascade_entry in &preview.cascade_entries {
            if cascade_entry.entity_type == "thing"
                && cascade_entry.entity_uuid != restored_thing_uuid
            {
                if let Some(child_snapshot) = self
                    .storage
                    .get_things_content_snapshot_by_log_id(cascade_entry.id)?
                {
                    if let Ok(child_data) =
                        serde_json::from_str::<ThingEntry>(&child_snapshot.content_json)
                    {
                        self.upsert_thing(
                            device_id,
                            ThingUpsert {
                                uuid: child_data.uuid,
                                title: child_data.title,
                                datatype: child_data.datatype,
                                data: Some(child_data.data),
                                collection_uuid: child_data.collection_uuid,
                                parent_uuid: child_data.parent_uuid,
                                created_at: None,
                                updated_at: None,
                            },
                        )?;
                    }
                }
            }
        }

        Ok(format!("Restored thing '{}'", restored_thing_title))
    }

    fn restore_from_snapshot(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
    ) -> Result<String> {
        let content_snapshot = self
            .storage
            .get_things_content_snapshot_by_log_id(log_entry.id)?
            .ok_or_else(|| anyhow!("No content snapshot found for update operation"))?;

        if log_entry.entity_type == "collection" {
            let collection_data: ThingCollectionEntry =
                serde_json::from_str(&content_snapshot.content_json)
                    .context("Failed to parse collection snapshot")?;
            let collection_title = collection_data.title.clone();

            self.upsert_collection(
                device_id,
                ThingCollectionUpsert {
                    uuid: collection_data.uuid,
                    title: collection_data.title,
                    collection_type: collection_data.collection_type,
                    app_id: collection_data.app_id.clone(),
                    created_at: None,
                    updated_at: None,
                },
            )?;

            Ok(format!(
                "Restored collection '{}' to previous state",
                collection_title
            ))
        } else {
            let thing_data: ThingEntry = serde_json::from_str(&content_snapshot.content_json)
                .context("Failed to parse thing snapshot")?;
            let thing_title = thing_data.title.clone();

            self.upsert_thing(
                device_id,
                ThingUpsert {
                    uuid: thing_data.uuid,
                    title: thing_data.title,
                    datatype: thing_data.datatype,
                    data: Some(thing_data.data),
                    collection_uuid: thing_data.collection_uuid,
                    parent_uuid: thing_data.parent_uuid,
                    created_at: None,
                    updated_at: None,
                },
            )?;

            Ok(format!(
                "Restored thing '{}' to previous state",
                thing_title
            ))
        }
    }

    fn undo_move_thing(&self, device_id: &str, log_entry: &ThingsChangeLogEntry) -> Result<String> {
        let details: serde_json::Value =
            serde_json::from_str(&log_entry.details_json).unwrap_or_default();
        let original_collection = details["from_collection_uuid"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing from_collection_uuid in move details"))?;
        let thing_uuid = &log_entry.entity_uuid;

        let snapshot = self.snapshot_for_undo(device_id)?;
        let thing = snapshot
            .things
            .iter()
            .find(|t| t.uuid == *thing_uuid)
            .ok_or_else(|| anyhow!("Thing not found for undo-move: {}", thing_uuid))?;

        self.upsert_thing(
            device_id,
            ThingUpsert {
                uuid: thing.uuid.clone(),
                title: thing.title.clone(),
                datatype: thing.datatype.clone(),
                data: Some(thing.data.clone()),
                collection_uuid: original_collection.to_string(),
                parent_uuid: thing.parent_uuid.clone(),
                created_at: None,
                updated_at: None,
            },
        )?;

        Ok(format!(
            "Moved thing back to original collection '{}'",
            original_collection
        ))
    }

    fn undo_move_things(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        preview: &ThingsUndoPreview,
    ) -> Result<String> {
        let details: serde_json::Value =
            serde_json::from_str(&log_entry.details_json).unwrap_or_default();
        let moved_items = details["moved_items"].as_array();
        let mut restored_count = 0;

        if let Some(items) = moved_items {
            for item in items {
                let thing_uuid = item["uuid"].as_str().unwrap_or("");
                let original_collection = item["from_collection_uuid"].as_str().unwrap_or("");

                if thing_uuid.is_empty() || original_collection.is_empty() {
                    continue;
                }

                let snapshot = self.snapshot_for_undo(device_id)?;
                if let Some(thing) = snapshot.things.iter().find(|t| t.uuid == thing_uuid) {
                    self.upsert_thing(
                        device_id,
                        ThingUpsert {
                            uuid: thing.uuid.clone(),
                            title: thing.title.clone(),
                            datatype: thing.datatype.clone(),
                            data: Some(thing.data.clone()),
                            collection_uuid: original_collection.to_string(),
                            parent_uuid: thing.parent_uuid.clone(),
                            created_at: None,
                            updated_at: None,
                        },
                    )?;
                    restored_count += 1;
                }
            }
        }

        for cascade_entry in &preview.cascade_entries {
            if cascade_entry.op_type == ThingsOperationType::MoveThing {
                let cascade_details: serde_json::Value =
                    serde_json::from_str(&cascade_entry.details_json).unwrap_or_default();
                let thing_uuid = &cascade_entry.entity_uuid;
                let original_collection = cascade_details["from_collection_uuid"]
                    .as_str()
                    .unwrap_or("");

                if original_collection.is_empty() {
                    continue;
                }

                let snapshot = self.snapshot_for_undo(device_id)?;
                if let Some(thing) = snapshot.things.iter().find(|t| t.uuid == *thing_uuid) {
                    self.upsert_thing(
                        device_id,
                        ThingUpsert {
                            uuid: thing.uuid.clone(),
                            title: thing.title.clone(),
                            datatype: thing.datatype.clone(),
                            data: Some(thing.data.clone()),
                            collection_uuid: original_collection.to_string(),
                            parent_uuid: thing.parent_uuid.clone(),
                            created_at: None,
                            updated_at: None,
                        },
                    )?;
                    restored_count += 1;
                }
            }
        }

        if restored_count == 0 {
            return Err(anyhow!("No things found to restore from batch move"));
        }

        Ok(format!(
            "Restored {} things to original collections",
            restored_count
        ))
    }

    fn undo_delete_things(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        preview: &ThingsUndoPreview,
    ) -> Result<String> {
        let mut restored_count = 0;

        if let Some(snapshot) = self
            .storage
            .get_things_content_snapshot_by_log_id(log_entry.id)?
        {
            if let Ok(things) = serde_json::from_str::<Vec<ThingEntry>>(&snapshot.content_json) {
                for thing_data in things {
                    self.upsert_thing(
                        device_id,
                        ThingUpsert {
                            uuid: thing_data.uuid,
                            title: thing_data.title,
                            datatype: thing_data.datatype,
                            data: Some(thing_data.data),
                            collection_uuid: thing_data.collection_uuid,
                            parent_uuid: thing_data.parent_uuid,
                            created_at: None,
                            updated_at: None,
                        },
                    )?;
                    restored_count += 1;
                }
            } else if let Ok(thing_data) =
                serde_json::from_str::<ThingEntry>(&snapshot.content_json)
            {
                self.upsert_thing(
                    device_id,
                    ThingUpsert {
                        uuid: thing_data.uuid,
                        title: thing_data.title,
                        datatype: thing_data.datatype,
                        data: Some(thing_data.data),
                        collection_uuid: thing_data.collection_uuid,
                        parent_uuid: thing_data.parent_uuid,
                        created_at: None,
                        updated_at: None,
                    },
                )?;
                restored_count += 1;
            }
        }

        for cascade_entry in &preview.cascade_entries {
            if cascade_entry.op_type == ThingsOperationType::DeleteThing {
                if let Some(child_snapshot) = self
                    .storage
                    .get_things_content_snapshot_by_log_id(cascade_entry.id)?
                {
                    if let Ok(thing_data) =
                        serde_json::from_str::<ThingEntry>(&child_snapshot.content_json)
                    {
                        let snapshot = self.snapshot_for_undo(device_id)?;
                        let parent_exists = snapshot
                            .collections
                            .iter()
                            .any(|c| c.uuid == thing_data.collection_uuid);

                        if parent_exists {
                            self.upsert_thing(
                                device_id,
                                ThingUpsert {
                                    uuid: thing_data.uuid,
                                    title: thing_data.title,
                                    datatype: thing_data.datatype,
                                    data: Some(thing_data.data),
                                    collection_uuid: thing_data.collection_uuid,
                                    parent_uuid: thing_data.parent_uuid,
                                    created_at: None,
                                    updated_at: None,
                                },
                            )?;
                            restored_count += 1;
                        }
                    }
                }
            }
        }

        if restored_count == 0 {
            return Err(anyhow!("No things found to restore from batch delete"));
        }

        Ok(format!("Restored {} deleted things", restored_count))
    }
}
