use super::*;

impl<'a> ThingsLocalService<'a> {
    pub fn list_change_log(&self, limit: u32, offset: u32) -> Result<Vec<ThingsChangeLogEntry>> {
        self.storage.list_things_change_log(limit, offset)
    }

    pub fn list_change_log_for_entity(
        &self,
        entity_type: &str,
        entity_uuid: &str,
        limit: u32,
    ) -> Result<Vec<ThingsChangeLogEntry>> {
        self.storage
            .list_things_change_log_for_entity(entity_type, entity_uuid, limit)
    }

    pub fn get_change_log(&self, log_id: i64) -> Result<Option<ThingsChangeLogEntry>> {
        self.storage.get_things_change_log(log_id)
    }

    pub fn cleanup_change_logs(&self, older_than_days: i64) -> Result<(u64, u64)> {
        let snapshots_deleted = self
            .storage
            .cleanup_things_content_snapshots(older_than_days)?;
        let logs_deleted = self.storage.cleanup_things_change_log(older_than_days)?;
        Ok((logs_deleted, snapshots_deleted))
    }

    pub fn list_content_snapshots(
        &self,
        thing_uuid: &str,
        limit: u32,
    ) -> Result<Vec<ThingsContentSnapshot>> {
        self.storage
            .list_things_content_snapshots(thing_uuid, limit)
    }

    pub fn get_unsynced_change_logs(&self, limit: u32) -> Result<Vec<ThingsChangeLogEntry>> {
        self.storage.get_unsynced_change_logs(limit)
    }

    pub fn mark_change_logs_synced(&self, ids: &[i64]) -> Result<()> {
        self.storage.mark_change_logs_synced(ids)
    }

    pub fn get_unsynced_content_snapshots(&self, limit: u32) -> Result<Vec<ThingsContentSnapshot>> {
        self.storage.get_unsynced_content_snapshots(limit)
    }

    pub fn mark_content_snapshots_synced(&self, ids: &[i64]) -> Result<()> {
        self.storage.mark_content_snapshots_synced(ids)
    }

    pub fn insert_synced_change_log(
        &self,
        device_id: &str,
        op_type: ThingsOperationType,
        entity_type: &str,
        entity_uuid: &str,
        summary: &str,
        details_json: &str,
        created_at: i64,
    ) -> Result<i64> {
        self.storage.insert_synced_change_log(
            device_id,
            op_type,
            entity_type,
            entity_uuid,
            summary,
            details_json,
            created_at,
        )
    }

    pub fn insert_synced_content_snapshot(
        &self,
        device_id: &str,
        thing_uuid: &str,
        content_json: &str,
        created_at: i64,
    ) -> Result<i64> {
        self.storage
            .insert_synced_content_snapshot(device_id, thing_uuid, content_json, created_at)
    }

    pub fn watch_since(
        &self,
        device_id: &str,
        after_event_id: i64,
        limit: u32,
    ) -> Result<Vec<ThingsLocalEvent>> {
        self.storage
            .list_things_local_events_since(device_id, after_event_id, limit)
    }

    pub fn ack_events(&self, device_id: &str, until_event_id: i64) -> Result<u64> {
        self.storage
            .ack_things_local_events(device_id, until_event_id)
    }

    pub(super) fn record_user_change_log(
        &self,
        context: &ThingsMutationContext,
        change_log_ids: &mut Vec<i64>,
        op_type: ThingsOperationType,
        entity_type: &str,
        entity_uuid: &str,
        summary: &str,
        details_json: &str,
        parent_log_id: Option<i64>,
        can_undo: bool,
    ) -> Option<i64> {
        if context.change_log_policy != ChangeLogPolicy::RecordUserVisible {
            return None;
        }

        if let Ok(change_log_id) = self.storage.insert_things_change_log(
            &context.device_id,
            op_type,
            entity_type,
            entity_uuid,
            summary,
            details_json,
            parent_log_id,
            can_undo,
        ) {
            change_log_ids.push(change_log_id);
            Some(change_log_id)
        } else {
            None
        }
    }

    pub fn record_undo_change_log(
        &self,
        device_id: &str,
        log_entry: &ThingsChangeLogEntry,
        message: &str,
    ) -> Result<Option<i64>> {
        self.storage.mark_things_change_log_undone(log_entry.id)?;

        let context = ThingsMutationContext::local_command(device_id);
        let undo_op_type = log_entry
            .op_type
            .to_undo_variant()
            .unwrap_or(log_entry.op_type);
        let details = json!({
            "undone_log_id": log_entry.id,
            "original_op": log_entry.op_type.as_str(),
        });
        let mut change_log_ids = Vec::new();
        Ok(self.record_user_change_log(
            &context,
            &mut change_log_ids,
            undo_op_type,
            &log_entry.entity_type,
            &log_entry.entity_uuid,
            message,
            &details.to_string(),
            None,
            false,
        ))
    }
}
