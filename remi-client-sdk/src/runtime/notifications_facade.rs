use super::*;

impl RemiSdk {
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
        if let Some(notification) = self.storage.get_notification(notification_id)? {
            self.enqueue_search_document(SearchDocument::from_notification(&notification));
        }
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
        if let Some(notification) = self.storage.get_notification(notification_id)? {
            self.enqueue_search_document(SearchDocument::from_notification(&notification));
        }
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
        self.enqueue_search_actions(vec![SearchIngestAction::DeleteByParent {
            kind: crate::search::SearchEntityKind::Notification,
            parent_id: category.to_string(),
        }]);
        self.emit_notification_event(NotificationEvent::CategoryDeleted {
            category: category.to_string(),
        });
        Ok(())
    }
}
