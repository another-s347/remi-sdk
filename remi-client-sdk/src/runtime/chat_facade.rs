use super::*;

impl TriggerSdk {
    // ===== Chat Session Management =====

    /// Create or update a chat session
    pub fn upsert_chat_session(
        &self,
        session_id: String,
        title: Option<String>,
        message_count: i32,
    ) -> Result<()> {
        let now = Utc::now();
        let session = crate::types::ChatSession {
            session_id,
            title,
            created_at: now,
            last_activity: now,
            message_count,
        };
        self.storage.upsert_chat_session(&session)
    }

    /// Get a specific chat session
    pub fn get_chat_session(&self, session_id: &str) -> Result<Option<crate::types::ChatSession>> {
        self.storage.get_chat_session(session_id)
    }

    /// List all chat sessions
    pub fn list_chat_sessions(&self, limit: Option<u32>) -> Result<Vec<crate::types::ChatSession>> {
        self.storage.list_chat_sessions(limit)
    }

    pub fn create_agent_version(&self, version: &crate::types::AgentVersion) -> Result<()> {
        self.storage.create_agent_version(version)
    }

    pub fn get_agent_version(
        &self,
        version_id: &str,
    ) -> Result<Option<crate::types::AgentVersion>> {
        self.storage.get_agent_version(version_id)
    }

    pub fn list_agent_versions(&self, agent_id: &str) -> Result<Vec<crate::types::AgentVersion>> {
        self.storage.list_agent_versions(agent_id)
    }

    pub fn update_agent_version(&self, update: &crate::types::AgentVersionUpdate) -> Result<()> {
        self.storage.update_agent_version(update)
    }

    pub fn mark_agent_version_applied(
        &self,
        version_id: &str,
        agent_id: &str,
        applied_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.storage
            .mark_agent_version_applied(version_id, agent_id, applied_at)
    }

    pub fn delete_agent_version(&self, version_id: &str) -> Result<()> {
        self.storage.delete_agent_version(version_id)
    }

    pub fn create_eval_dataset(&self, dataset: &crate::types::EvalDataset) -> Result<()> {
        self.storage.create_eval_dataset(dataset)
    }

    pub fn get_eval_dataset(&self, dataset_id: &str) -> Result<Option<crate::types::EvalDataset>> {
        self.storage.get_eval_dataset(dataset_id)
    }

    pub fn list_eval_datasets(&self) -> Result<Vec<crate::types::EvalDataset>> {
        self.storage.list_eval_datasets()
    }

    pub fn update_eval_dataset(&self, update: &crate::types::EvalDatasetUpdate) -> Result<()> {
        self.storage.update_eval_dataset(update)
    }

    pub fn delete_eval_dataset(&self, dataset_id: &str) -> Result<()> {
        self.storage.delete_eval_dataset(dataset_id)
    }

    pub fn add_session_to_dataset(&self, item: &crate::types::EvalDatasetSession) -> Result<()> {
        self.storage.add_session_to_dataset(item)
    }

    pub fn remove_session_from_dataset(&self, dataset_id: &str, session_id: &str) -> Result<()> {
        self.storage
            .remove_session_from_dataset(dataset_id, session_id)
    }

    pub fn list_dataset_sessions(
        &self,
        dataset_id: &str,
    ) -> Result<Vec<crate::types::EvalDatasetSession>> {
        self.storage.list_dataset_sessions(dataset_id)
    }

    pub fn create_eval_dataset_run(
        &self,
        run: &crate::types::EvalDatasetRun,
        items: &[crate::types::EvalDatasetRunItem],
    ) -> Result<()> {
        self.storage.create_eval_dataset_run(run, items)
    }

    pub fn list_eval_dataset_runs(
        &self,
        dataset_id: &str,
    ) -> Result<Vec<crate::types::EvalDatasetRun>> {
        self.storage.list_eval_dataset_runs(dataset_id)
    }

    pub fn get_eval_dataset_run(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::types::EvalDatasetRun>> {
        self.storage.get_eval_dataset_run(run_id)
    }

    pub fn list_eval_dataset_run_items(
        &self,
        run_id: &str,
    ) -> Result<Vec<crate::types::EvalDatasetRunItem>> {
        self.storage.list_eval_dataset_run_items(run_id)
    }

    pub fn save_eval_dataset_run_evals(
        &self,
        run_id: &str,
        dataset_id: &str,
        evals: &[crate::types::EvalDatasetRunEval],
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.storage
            .save_eval_dataset_run_evals(run_id, dataset_id, evals, updated_at)
    }

    pub fn list_eval_dataset_run_evals(
        &self,
        run_id: &str,
    ) -> Result<Vec<crate::types::EvalDatasetRunEval>> {
        self.storage.list_eval_dataset_run_evals(run_id)
    }

    /// Update session activity (last activity time and message count)
    pub fn update_session_activity(
        &self,
        session_id: String,
        title: Option<String>,
        message_count: i32,
    ) -> Result<()> {
        let update = crate::types::ChatSessionUpdate {
            session_id,
            title,
            last_activity: Utc::now(),
            message_count,
        };
        self.storage.update_session_activity(&update)
    }

    /// Delete a chat session
    pub fn delete_chat_session(&self, session_id: &str) -> Result<()> {
        self.storage.delete_chat_session(session_id)
    }

    // ===== Chat Message History =====

    /// Create or update a chat message (stored as raw JSON) for a given session.
    pub fn upsert_chat_message_json(
        &self,
        session_id: String,
        message_id: String,
        created_at_ms: i64,
        message_json: String,
    ) -> Result<()> {
        self.storage.upsert_chat_message_json(
            &session_id,
            &message_id,
            created_at_ms,
            &message_json,
        )
    }

    /// List chat messages (raw JSON strings) for a session.
    pub fn list_chat_messages_json(
        &self,
        session_id: &str,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<String>> {
        self.storage
            .list_chat_messages_json(session_id, limit, offset)
    }

    /// Delete chat messages for a session (keeps the session record).
    pub fn delete_chat_messages(&self, session_id: &str) -> Result<()> {
        self.storage.delete_chat_messages(session_id)
    }

    /// Persist runtime-owned chat protocol state for a session.
    pub fn upsert_chat_runtime_state_json(
        &self,
        session_id: String,
        state_json: String,
    ) -> Result<()> {
        self.storage
            .upsert_chat_runtime_state_json(&session_id, &state_json)
    }

    /// Load runtime-owned chat protocol state for a session.
    pub fn get_chat_runtime_state_json(&self, session_id: &str) -> Result<Option<String>> {
        self.storage.get_chat_runtime_state_json(session_id)
    }

    /// Delete runtime-owned chat protocol state for a session.
    pub fn delete_chat_runtime_state(&self, session_id: &str) -> Result<()> {
        self.storage.delete_chat_runtime_state(session_id)
    }
}
