use super::*;

impl TriggerSdk {
    pub fn list_trigger_logs_json(
        &self,
        trigger_uuid: &str,
        limit: Option<u32>,
        run_type: Option<TriggerRunType>,
    ) -> Result<String> {
        let logs = self
            .storage
            .list_trigger_logs(trigger_uuid, limit, run_type)?;
        to_string(&logs).context("Failed to serialize trigger logs")
    }
    pub fn export_trigger_logs_json(
        &self,
        trigger_uuid: &str,
        run_type: Option<TriggerRunType>,
    ) -> Result<String> {
        let logs = self.storage.export_trigger_logs(trigger_uuid, run_type)?;
        to_string(&logs).context("Failed to serialize trigger logs for export")
    }

    // ===== Notification queries =====

    pub fn list_notifications_grouped_json(&self, limit: u32) -> Result<String> {
        let groups = self.storage.list_notifications_grouped(limit)?;
        to_string(&groups).context("Failed to serialize notification groups")
    }

    pub fn list_notifications_by_category_json(
        &self,
        category: &str,
        limit: u32,
    ) -> Result<String> {
        let items = self
            .storage
            .list_notifications_by_category(category, limit)?;
        to_string(&items).context("Failed to serialize notifications by category")
    }

    pub fn list_notifications_flat_json(&self, limit: u32, offset: u32) -> Result<String> {
        let items = self.storage.list_notifications_flat(limit, offset)?;
        to_string(&items).context("Failed to serialize flat notifications")
    }

    pub fn get_latest_unread_notification_json(&self) -> Result<Option<String>> {
        let entry = self.storage.get_latest_unread_notification()?;
        match entry {
            Some(e) => Ok(Some(
                to_string(&e).context("Failed to serialize latest unread notification")?,
            )),
            None => Ok(None),
        }
    }

    pub fn get_unread_notification_count(&self) -> Result<i64> {
        self.storage.get_unread_notification_count()
    }

    pub fn mark_notification_read(&self, notification_id: i64) -> Result<()> {
        self.storage.mark_notification_read(notification_id)?;
        self.emit_notification_event(NotificationEvent::Read { notification_id });
        Ok(())
    }

    pub fn record_notification_response(
        &self,
        notification_id: i64,
        action: NotificationResponseAction,
    ) -> Result<()> {
        self.storage
            .record_notification_response(notification_id, &action)?;
        self.emit_notification_event(NotificationEvent::Responded {
            notification_id,
            action,
        });
        Ok(())
    }

    pub fn mark_category_notifications_read(&self, category: &str) -> Result<()> {
        self.storage.mark_category_notifications_read(category)?;
        self.emit_notification_event(NotificationEvent::CategoryRead {
            category: category.to_string(),
        });
        Ok(())
    }

    pub fn mark_all_notifications_read(&self) -> Result<()> {
        self.storage.mark_all_notifications_read()?;
        self.emit_notification_event(NotificationEvent::AllRead);
        Ok(())
    }

    pub fn delete_notifications_by_category(&self, category: &str) -> Result<()> {
        self.storage.delete_notifications_by_category(category)?;
        self.emit_notification_event(NotificationEvent::CategoryDeleted {
            category: category.to_string(),
        });
        Ok(())
    }
}
