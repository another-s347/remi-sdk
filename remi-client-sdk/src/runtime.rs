use crate::context_prompt;
use crate::notification_events::NotificationEvent;
use crate::realtime::{RemiRealtimeEvent, SupabaseRealtimeManager};
use crate::search::{
    SearchChange, SearchConfig, SearchDocument, SearchIndexStatus, SearchIngestAction,
    SearchIngestProvider, SearchQuery, SearchResult, SearchService,
};
use crate::storage::Storage;
use crate::things_crdt::{
    ContentEntry, ContentEntryUpdate, ThingCollectionEntry, ThingCollectionUpsert, ThingDatatype,
    ThingEntry, ThingUpsert, ThingsSnapshot, ThingsSnapshotState,
};
use crate::things_events::ThingsEvent;
use crate::things_local::{DirtyPolicy, ThingsMutationContext, ThingsMutationPipeline};
use crate::types::{
    ActionDefinition, ActionInvocationRecord, ActionInvocationSourceKind,
    NotificationResponseAction, ThingsChangeLogEntry, ThingsContentSnapshot, ThingsUndoExecution,
    ThingsUndoPreview,
};
use anyhow::{Context, Result, anyhow};
#[cfg(feature = "quickjs")]
use base64::Engine;
use chrono::Utc;
use serde_json::to_string;
use serde_json::{Value, json};
use std::path::Path;
#[cfg(feature = "quickjs")]
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

mod chat_facade;
mod notifications_facade;
mod path_tools;
mod things_documents;
mod things_facade;
pub use path_tools::VirtualFsCatResult;

#[cfg(feature = "quickjs")]
const DEFAULT_ACTION_HTTP_TIMEOUT_MS: u64 = 30_000;

#[cfg(feature = "quickjs")]
fn parse_notification_source(value: &str) -> Result<crate::types::NotificationSource> {
    match value.trim().to_ascii_lowercase().as_str() {
        "push" => Ok(crate::types::NotificationSource::Push),
        "system" => Ok(crate::types::NotificationSource::System),
        "chat" => Ok(crate::types::NotificationSource::Chat),
        other => anyhow::bail!("Unsupported notification source '{other}'"),
    }
}

#[cfg(feature = "quickjs")]
fn default_action_notification_source(
    _source_kind: &ActionInvocationSourceKind,
) -> crate::types::NotificationSource {
    crate::types::NotificationSource::System
}

#[cfg(feature = "quickjs")]
fn action_http_request_handler(action_uuid: String) -> crate::quickjs::QuickJsHostHandler {
    Arc::new(move |request| {
        execute_action_http_request(&action_uuid, request).map_err(|error| error.to_string())
    })
}

#[cfg(feature = "quickjs")]
fn action_notify_send_handler(
    storage: Storage,
    notification_event_tx: broadcast::Sender<NotificationEvent>,
    search: SearchService,
    action_uuid: String,
    source_kind: ActionInvocationSourceKind,
) -> crate::quickjs::QuickJsHostHandler {
    let default_source = default_action_notification_source(&source_kind);
    Arc::new(move |request| {
        let result: Result<Value> = (|| {
            let title = request
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("notify.send requires a non-empty title"))?;
            let body = request
                .get("body")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let category = request
                .get("category")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("action:{action_uuid}"));
            let source = request
                .get("source")
                .and_then(Value::as_str)
                .map(parse_notification_source)
                .transpose()?
                .unwrap_or_else(|| default_source.clone());

            let notification_id = storage.insert_notification(&source, &category, title, body)?;
            if let Some(notification) = storage.get_notification(notification_id)? {
                search.enqueue_document(SearchDocument::from_notification(&notification))?;
            }
            let _ = notification_event_tx.send(NotificationEvent::Added {
                notification_id,
                category: category.clone(),
                source: source.clone(),
                title: title.to_string(),
            });
            Ok(json!({
                "notification_id": notification_id,
                "source": source,
                "category": category,
                "title": title,
                "body": body,
            }))
        })();

        result.map_err(|error| error.to_string())
    })
}

#[cfg(feature = "quickjs")]
fn action_notify_list_handler(storage: Storage) -> crate::quickjs::QuickJsHostHandler {
    Arc::new(move |request| {
        let result: Result<Value> = (|| {
            let limit = request
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .min(u32::MAX as u64) as u32;

            if let Some(category) = request.get("category").and_then(Value::as_str) {
                let notifications = storage.list_notifications_by_category(category, limit)?;
                return Ok(json!({
                    "mode": "category",
                    "category": category,
                    "items": notifications,
                }));
            }

            if request
                .get("flat")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                let offset = request
                    .get("offset")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    .min(u32::MAX as u64) as u32;
                let notifications = storage.list_notifications_flat(limit, offset)?;
                return Ok(json!({
                    "mode": "flat",
                    "offset": offset,
                    "items": notifications,
                }));
            }

            let groups = storage.list_notifications_grouped(limit)?;
            Ok(json!({
                "mode": "grouped",
                "groups": groups,
            }))
        })();

        result.map_err(|error| error.to_string())
    })
}

#[cfg(feature = "quickjs")]
fn action_notify_mark_read_handler(
    storage: Storage,
    notification_event_tx: broadcast::Sender<NotificationEvent>,
    search: SearchService,
) -> crate::quickjs::QuickJsHostHandler {
    Arc::new(move |request| {
        let result: Result<Value> = (|| {
            let notification_id = request
                .get("notificationId")
                .or_else(|| request.get("notification_id"))
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow!("notify.markRead requires notificationId"))?;
            storage.mark_notification_read(notification_id)?;
            if let Some(notification) = storage.get_notification(notification_id)? {
                search.enqueue_document(SearchDocument::from_notification(&notification))?;
            }
            let _ = notification_event_tx.send(NotificationEvent::Read { notification_id });
            Ok(json!({ "notification_id": notification_id, "read": true }))
        })();

        result.map_err(|error| error.to_string())
    })
}

#[cfg(feature = "quickjs")]
fn action_notify_mark_category_read_handler(
    storage: Storage,
    notification_event_tx: broadcast::Sender<NotificationEvent>,
) -> crate::quickjs::QuickJsHostHandler {
    Arc::new(move |request| {
        let result: Result<Value> = (|| {
            let category = request
                .get("category")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("notify.markCategoryRead requires category"))?;
            storage.mark_category_notifications_read(category)?;
            let _ = notification_event_tx.send(NotificationEvent::CategoryRead {
                category: category.to_string(),
            });
            Ok(json!({ "category": category, "read": true }))
        })();

        result.map_err(|error| error.to_string())
    })
}

#[cfg(feature = "quickjs")]
fn action_notify_mark_all_read_handler(
    storage: Storage,
    notification_event_tx: broadcast::Sender<NotificationEvent>,
) -> crate::quickjs::QuickJsHostHandler {
    Arc::new(move |_request| {
        storage
            .mark_all_notifications_read()
            .map_err(|error| error.to_string())?;
        let _ = notification_event_tx.send(NotificationEvent::AllRead);
        Ok(json!({ "read": true }))
    })
}

#[cfg(feature = "quickjs")]
fn action_notify_delete_category_handler(
    storage: Storage,
    notification_event_tx: broadcast::Sender<NotificationEvent>,
    search: SearchService,
) -> crate::quickjs::QuickJsHostHandler {
    Arc::new(move |request| {
        let result: Result<Value> = (|| {
            let category = request
                .get("category")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("notify.deleteCategory requires category"))?;
            storage.delete_notifications_by_category(category)?;
            search.enqueue_actions(vec![SearchIngestAction::DeleteByParent {
                kind: crate::search::SearchEntityKind::Notification,
                parent_id: category.to_string(),
            }])?;
            let _ = notification_event_tx.send(NotificationEvent::CategoryDeleted {
                category: category.to_string(),
            });
            Ok(json!({ "category": category, "deleted": true }))
        })();

        result.map_err(|error| error.to_string())
    })
}

#[cfg(feature = "quickjs")]
fn execute_action_http_request(action_uuid: &str, request: Value) -> Result<Value> {
    let request_object = request
        .as_object()
        .ok_or_else(|| anyhow!("http.request expects an object payload"))?;
    let method = request_object
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("GET")
        .to_ascii_uppercase();
    let url = request_object
        .get("url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("http.request requires a non-empty url"))?;
    let method = reqwest::Method::from_bytes(method.as_bytes())
        .with_context(|| format!("Unsupported HTTP method '{method}'"))?;
    let timeout_ms = request_object
        .get("timeout_ms")
        .or_else(|| request_object.get("timeoutMs"))
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_ACTION_HTTP_TIMEOUT_MS);

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .user_agent(format!("remi-action/{action_uuid}"))
        .build()
        .context("Failed to build action HTTP client")?;

    let mut request_builder = client.request(method.clone(), url);

    if let Some(headers) = request_object.get("headers").and_then(Value::as_object) {
        for (name, value) in headers {
            let header_value = match value {
                Value::String(text) => text.clone(),
                _ => {
                    serde_json::to_string(value).context("Failed to serialize HTTP header value")?
                }
            };
            request_builder = request_builder.header(name, header_value);
        }
    }

    if let Some(query) = request_object.get("query") {
        request_builder = request_builder.query(query);
    }

    let has_json_body = request_object.get("json").is_some();
    if let Some(json_body) = request_object.get("json") {
        request_builder = request_builder.json(json_body);
    } else if let Some(body_base64) = request_object.get("body_base64").and_then(Value::as_str) {
        let body_bytes = base64::engine::general_purpose::STANDARD
            .decode(body_base64)
            .context("Invalid base64 in http.request body_base64")?;
        request_builder = request_builder.body(body_bytes);
    } else if let Some(body_text) = request_object
        .get("body_text")
        .or_else(|| request_object.get("text"))
        .and_then(Value::as_str)
    {
        request_builder = request_builder.body(body_text.to_string());
    } else if let Some(body) = request_object.get("body") {
        match body {
            Value::String(text) => {
                request_builder = request_builder.body(text.clone());
            }
            _ => {
                request_builder = request_builder
                    .body(serde_json::to_vec(body).context("Failed to serialize HTTP body")?);
                if !has_json_body && request_object.get("headers").is_none() {
                    request_builder = request_builder.header("content-type", "application/json");
                }
            }
        }
    }

    let response = request_builder
        .send()
        .with_context(|| format!("HTTP request failed for {url}"))?;
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .bytes()
        .context("Failed to read HTTP response body")?;
    let body_base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let body_text = String::from_utf8(bytes.to_vec()).ok();
    let body_json = body_text
        .as_ref()
        .and_then(|text| serde_json::from_str::<Value>(text).ok());

    let headers_json = headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .map(|text| Value::String(text.to_string()))
                    .unwrap_or_else(|_| Value::String(String::new())),
            )
        })
        .collect::<serde_json::Map<String, Value>>();

    Ok(json!({
        "ok": status.is_success(),
        "status": status.as_u16(),
        "status_text": status.canonical_reason().unwrap_or_default(),
        "url": url,
        "method": method.as_str(),
        "headers": headers_json,
        "body_text": body_text,
        "body_json": body_json,
        "body_base64": body_base64,
        "content_length": bytes.len(),
    }))
}

#[cfg(feature = "quickjs")]
fn build_action_quickjs_bindings(
    storage: &Storage,
    notification_event_tx: &broadcast::Sender<NotificationEvent>,
    search: &SearchService,
    action: &ActionDefinition,
    execution_input: &Value,
) -> crate::quickjs::QuickJsHostBindings {
    let source_kind = execution_input
        .get("source")
        .and_then(|value| value.get("kind"))
        .and_then(Value::as_str)
        .and_then(|value| ActionInvocationSourceKind::from_str(value).ok())
        .unwrap_or(ActionInvocationSourceKind::System);
    let storage = storage.clone();
    let search = search.clone();

    crate::quickjs::QuickJsHostBindings {
        http_request: Some(action_http_request_handler(action.action_uuid.clone())),
        notify_send: Some(action_notify_send_handler(
            storage.clone(),
            notification_event_tx.clone(),
            search.clone(),
            action.action_uuid.clone(),
            source_kind.clone(),
        )),
        notify_list: Some(action_notify_list_handler(storage.clone())),
        notify_mark_read: Some(action_notify_mark_read_handler(
            storage.clone(),
            notification_event_tx.clone(),
            search.clone(),
        )),
        notify_mark_category_read: Some(action_notify_mark_category_read_handler(
            storage.clone(),
            notification_event_tx.clone(),
        )),
        notify_mark_all_read: Some(action_notify_mark_all_read_handler(
            storage.clone(),
            notification_event_tx.clone(),
        )),
        notify_delete_category: Some(action_notify_delete_category_handler(
            storage,
            notification_event_tx.clone(),
            search,
        )),
    }
}

pub struct NotificationCallback;

pub struct RemiSdk {
    storage: Storage,
    things_event_tx: broadcast::Sender<ThingsEvent>,
    notification_event_tx: broadcast::Sender<NotificationEvent>,
    realtime: Arc<SupabaseRealtimeManager>,
    search: SearchService,
}

impl RemiSdk {
    pub fn initialize(db_path: impl AsRef<Path>) -> Result<Self> {
        Self::initialize_with_search_config(db_path, SearchConfig::default())
    }

    pub fn initialize_with_search_config(
        db_path: impl AsRef<Path>,
        search_config: SearchConfig,
    ) -> Result<Self> {
        let storage = Storage::new(db_path)?;
        storage.seed_builtin_actions(&crate::action_builtin::builtin_actions())?;
        let search = SearchService::open(storage.clone(), search_config)?;
        let (things_event_tx, _rx) = broadcast::channel(2048);
        let (notification_event_tx, _rx) = broadcast::channel(2048);
        Ok(Self {
            storage,
            things_event_tx,
            notification_event_tx,
            realtime: Arc::new(SupabaseRealtimeManager::new()),
            search,
        })
    }

    pub fn things_subscribe(&self) -> broadcast::Receiver<ThingsEvent> {
        self.things_event_tx.subscribe()
    }

    pub fn notifications_subscribe(&self) -> broadcast::Receiver<NotificationEvent> {
        self.notification_event_tx.subscribe()
    }

    pub fn realtime_manager(&self) -> Arc<SupabaseRealtimeManager> {
        self.realtime.clone()
    }

    pub fn realtime_subscribe(&self) -> broadcast::Receiver<RemiRealtimeEvent> {
        self.realtime.subscribe()
    }

    pub fn search(&self, query: SearchQuery) -> Result<Vec<SearchResult>> {
        self.search.search(query)
    }

    pub fn search_json(&self, query_json: &str) -> Result<String> {
        let query: SearchQuery =
            serde_json::from_str(query_json).context("Failed to parse search query JSON")?;
        let results = self.search(query)?;
        to_string(&results).context("Failed to serialize search results")
    }

    pub fn search_index_status(&self) -> SearchIndexStatus {
        self.search.status()
    }

    pub fn search_index_status_json(&self) -> Result<String> {
        to_string(&self.search_index_status()).context("Failed to serialize search index status")
    }

    pub fn register_search_provider(&self, provider: Arc<dyn SearchIngestProvider>) {
        self.search.register_provider(provider);
    }

    pub fn enqueue_search_change(&self, change: SearchChange) -> Result<()> {
        self.search.enqueue_change(change)
    }

    pub fn flush_search_index(&self) -> Result<()> {
        self.search.flush()
    }

    pub fn rebuild_search_index(&self) -> Result<()> {
        self.search.rebuild()
    }

    pub fn start_search_index_rebuild(&self) -> Result<()> {
        self.search.enqueue_rebuild()
    }

    fn enqueue_search_document(&self, document: SearchDocument) {
        if let Err(error) = self.search.enqueue_document(document) {
            warn!(error = %error, "Failed to enqueue search document");
        }
    }

    fn enqueue_search_actions(&self, actions: Vec<SearchIngestAction>) {
        if let Err(error) = self.search.enqueue_actions(actions) {
            warn!(error = %error, "Failed to enqueue search index actions");
        }
    }

    fn enqueue_search_rebuild(&self) {
        if let Err(error) = self.search.enqueue_rebuild() {
            warn!(error = %error, "Failed to enqueue search index rebuild");
        }
    }

    pub(crate) fn things_storage(&self) -> &Storage {
        &self.storage
    }

    fn emit_notification_event(&self, event: NotificationEvent) {
        // Ignore send errors (no active subscribers).
        let _ = self.notification_event_tx.send(event);
    }

    /// Wipe all local data and broadcast DataWiped events to all streams.
    /// This is the canonical logout path — Flutter notifiers will clear
    /// in-memory state when they receive the DataWiped event.
    pub fn wipe_all_data_and_notify(&self) -> Result<()> {
        self.wipe_all_data_and_notify_for_device("local")
    }

    /// Wipe all local data, persist a durable Things DataWiped event for the
    /// given device, and broadcast DataWiped events to all streams.
    pub fn wipe_all_data_and_notify_for_device(&self, device_id: &str) -> Result<()> {
        self.storage.wipe_all_data()?;
        let context = ThingsMutationContext::maintenance(device_id, DirtyPolicy::MarkClean);
        ThingsMutationPipeline::with_broadcaster(&self.storage, &self.things_event_tx)
            .emit_data_wiped(&context)?;
        self.emit_notification_event(NotificationEvent::DataWiped);
        self.enqueue_search_actions(vec![SearchIngestAction::Clear]);
        info!(device_id = %device_id, "Wiped all local data and notified all event streams");
        Ok(())
    }

    /// Attribute all anonymous (user_id IS NULL) local data to the given user.
    /// Called after a successful login so locally-created content is owned by the user.
    pub fn claim_anonymous_data(&self, user_id: &str) -> Result<()> {
        let claimed = self.storage.claim_anonymous_data(user_id)?;
        info!(user_id = %user_id, claimed, "Claimed anonymous data after login");
        Ok(())
    }

    pub fn list_actions(&self) -> Result<Vec<ActionDefinition>> {
        self.storage.list_actions()
    }

    pub fn list_actions_json(&self) -> Result<String> {
        let actions = self.list_actions()?;
        to_string(&actions).context("Failed to serialize actions")
    }

    pub fn fetch_action(&self, action_uuid: &str) -> Result<Option<ActionDefinition>> {
        self.storage.fetch_action(action_uuid)
    }

    pub fn fetch_action_json(&self, action_uuid: &str) -> Result<Option<String>> {
        self.fetch_action(action_uuid)?
            .map(|action| to_string(&action).context("Failed to serialize action"))
            .transpose()
    }

    pub fn latest_action_invocation_json(&self, action_uuid: &str) -> Result<Option<String>> {
        self.storage
            .latest_action_invocation(action_uuid)?
            .map(|record| {
                to_string(&record).context("Failed to serialize latest action invocation")
            })
            .transpose()
    }

    pub fn execute_action_now(
        &self,
        action_uuid: &str,
        source_kind: ActionInvocationSourceKind,
        source_entity_type: Option<&str>,
        source_entity_uuid: Option<&str>,
        args_json: Value,
        device_id: Option<&str>,
    ) -> Result<ActionInvocationRecord> {
        let action = self
            .storage
            .fetch_action(action_uuid)?
            .ok_or_else(|| anyhow!("Action not found: {action_uuid}"))?;

        let started_at = Utc::now();
        let invocation_uuid = uuid::Uuid::new_v4().to_string();
        let execution_input = json!({
            "action": {
                "uuid": action.action_uuid,
                "name": action.name,
                "version": action.version,
            },
            "source": {
                "kind": source_kind.as_str(),
                "entity_type": source_entity_type,
                "entity_uuid": source_entity_uuid,
            },
            "args": args_json,
            "context": {
                "device_id": device_id,
            }
        });

        let execution_started = std::time::Instant::now();
        let execution_result = self.run_action_script(&action, &execution_input);
        let finished_at = Utc::now();
        let duration_ms = execution_started.elapsed().as_millis() as u64;

        let (result_json, console_logs, error_json) = match execution_result {
            Ok((result_json, console_logs)) => (Some(result_json), console_logs, None),
            Err(error) => (
                None,
                Vec::new(),
                Some(json!({
                    "message": error.to_string(),
                })),
            ),
        };

        let record = ActionInvocationRecord {
            invocation_uuid,
            action_uuid: action_uuid.to_string(),
            source_kind,
            source_entity_type: source_entity_type.map(|value| value.to_string()),
            source_entity_uuid: source_entity_uuid.map(|value| value.to_string()),
            args_json: execution_input.get("args").cloned().unwrap_or(Value::Null),
            result_json,
            console_logs,
            error_json,
            started_at,
            finished_at,
            duration_ms,
            device_id: device_id.map(|value| value.to_string()),
        };

        self.storage.insert_action_invocation(&record)?;
        Ok(record)
    }

    pub fn build_session_context(
        &self,
        device_id: &str,
        granted_permissions: &[String],
        active_context_json: Option<&str>,
    ) -> Result<String> {
        use std::time::Instant;

        let total = Instant::now();

        let t0 = Instant::now();
        let snapshot_state = self.things_local_service().snapshot_lite(device_id)?;
        info!(
            device_id = %device_id,
            active_context = active_context_json.is_some(),
            permissions = granted_permissions.len(),
            ms = t0.elapsed().as_millis(),
            "build_session_context: snapshot_lite"
        );

        let t1 = Instant::now();
        let snapshot = ThingsSnapshot {
            collections: snapshot_state.collections,
            things: snapshot_state.things,
        };
        info!(
            collections = snapshot.collections.len(),
            things = snapshot.things.len(),
            ms = t1.elapsed().as_millis(),
            "build_session_context: normalize_snapshot"
        );

        let t2 = Instant::now();
        let out = context_prompt::build_context_prompt_markdown(
            granted_permissions,
            &snapshot,
            active_context_json,
        )?;
        info!(
            out_bytes = out.len(),
            ms = t2.elapsed().as_millis(),
            total_ms = total.elapsed().as_millis(),
            "build_session_context: build_context_prompt_markdown"
        );

        Ok(out)
    }

    fn run_action_script(
        &self,
        action: &ActionDefinition,
        execution_input: &Value,
    ) -> Result<(Value, Vec<String>)> {
        #[cfg(feature = "quickjs")]
        {
            let input_json = serde_json::to_string(execution_input)
                .context("Failed to serialize action execution input")?;
            let script = format!(
                "const __remiInput = {input_json};\nconst input = __remiInput;\nconst action = input.action;\nconst source = input.source;\nconst args = input.args;\nconst context = input.context;\n{}",
                action.script_source
            );
            let bindings = build_action_quickjs_bindings(
                &self.storage,
                &self.notification_event_tx,
                &self.search,
                action,
                execution_input,
            );
            let output = crate::quickjs::quickjs_eval_with_bindings(&script, bindings)
                .context("Action QuickJS execution failed")?;
            let result_json = serde_json::from_str(&output.json_result)
                .context("Action returned non-JSON result")?;
            return Ok((result_json, output.console_logs));
        }

        #[cfg(not(feature = "quickjs"))]
        {
            let _ = action;
            let _ = execution_input;
            anyhow::bail!("This SDK build does not enable the quickjs feature for actions")
        }
    }
}

#[cfg(test)]
mod db_observability_tests;

#[cfg(test)]
mod events_tests;

#[cfg(test)]
mod tests;
