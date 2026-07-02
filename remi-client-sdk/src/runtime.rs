use crate::context_prompt;
use crate::events_events::EventsEvent;
use crate::notification_events::NotificationEvent;
use crate::realtime::{RemiRealtimeEvent, SupabaseRealtimeManager};
use crate::storage::Storage;
use crate::things_crdt::{
    ContentEntry, ContentEntryUpdate, FieldPatch, ThingCollectionEntry, ThingCollectionUpsert,
    ThingDatatype, ThingEntry, ThingUpsert, ThingsSnapshot, ThingsSnapshotState,
};
use crate::things_events::ThingsEvent;
use crate::things_local::{DirtyPolicy, ThingsMutationContext, ThingsMutationPipeline};
use crate::trigger_events::TriggerEvent;
use crate::types::{
    ActionDefinition, ActionInvocationRecord, ActionInvocationSourceKind, EventPayload,
    NotificationResponseAction, StoredTrigger, ThingsChangeLogEntry, ThingsContentSnapshot,
    ThingsUndoExecution, ThingsUndoPreview, TriggerExecutionSummary, TriggerInfo, TriggerLogLevel,
    TriggerRegistration, TriggerReplaySummary, TriggerRule, TriggerRunType,
};
use anyhow::{Context, Result, anyhow};
#[cfg(feature = "quickjs")]
use base64::Engine;
use chrono::{DateTime, Datelike, FixedOffset, Local, TimeZone, Timelike, Utc};
use rule_trigger_engine::{
    EvaluationContext, MonitoringEvent, PreconditionPolicy, Rule as EngineRule, TriggerConfig,
};
use serde_json::to_string;
use serde_json::{Value, json};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

mod chat_facade;
mod notifications_facade;
mod path_tools;
mod scheduling;
mod things_documents;
mod things_facade;
pub use path_tools::VirtualFsCatResult;
#[cfg(test)]
use scheduling::extract_cron_from_preconditions;
use scheduling::{
    build_trigger_occurrences, describe_timing_sources, extract_repeat_frequency_from_conditions,
    extract_timings_from_rules, filter_events_at_time, normalize_timer_preconditions,
    repeat_min_gap, resolve_post_run_next_fire, resolve_registration_next_fire,
    select_current_event,
};

const DEFAULT_TRIGGER_NOTIFICATION_ACTION_UUID: &str = "builtin.trigger_notification";

#[cfg(feature = "quickjs")]
const DEFAULT_ACTION_HTTP_TIMEOUT_MS: u64 = 30_000;

const DEFAULT_TIMEZONE_OFFSET: &str = "+08:00";

fn default_timezone() -> FixedOffset {
    FixedOffset::from_str(DEFAULT_TIMEZONE_OFFSET).unwrap_or_else(|_| {
        FixedOffset::east_opt(8 * 3600).expect("UTC+08:00 offset must be valid")
    })
}

fn local_timezone_offset_string() -> String {
    let seconds = Local::now().offset().local_minus_utc();
    format_offset_seconds(seconds)
}

fn format_offset_seconds(total_seconds: i32) -> String {
    let sign = if total_seconds < 0 { '-' } else { '+' };
    let abs = total_seconds.abs();
    let hours = abs / 3600;
    let minutes = (abs % 3600) / 60;
    format!("{sign}{hours:02}:{minutes:02}")
}

#[cfg(feature = "quickjs")]
fn default_action_notification_source(
    source_kind: &ActionInvocationSourceKind,
) -> crate::types::NotificationSource {
    match source_kind {
        ActionInvocationSourceKind::Trigger => crate::types::NotificationSource::Trigger,
        ActionInvocationSourceKind::CollectionManual
        | ActionInvocationSourceKind::ThingManual
        | ActionInvocationSourceKind::System => crate::types::NotificationSource::System,
    }
}

#[cfg(feature = "quickjs")]
fn parse_notification_source(value: &str) -> Result<crate::types::NotificationSource> {
    match value.trim().to_ascii_lowercase().as_str() {
        "trigger" => Ok(crate::types::NotificationSource::Trigger),
        "push" => Ok(crate::types::NotificationSource::Push),
        "system" => Ok(crate::types::NotificationSource::System),
        "chat" => Ok(crate::types::NotificationSource::Chat),
        other => anyhow::bail!("Unsupported notification source '{other}'"),
    }
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
) -> crate::quickjs::QuickJsHostHandler {
    Arc::new(move |request| {
        let result: Result<Value> = (|| {
            let notification_id = request
                .get("notificationId")
                .or_else(|| request.get("notification_id"))
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow!("notify.markRead requires notificationId"))?;
            storage.mark_notification_read(notification_id)?;
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

    crate::quickjs::QuickJsHostBindings {
        http_request: Some(action_http_request_handler(action.action_uuid.clone())),
        notify_send: Some(action_notify_send_handler(
            storage.clone(),
            notification_event_tx.clone(),
            action.action_uuid.clone(),
            source_kind.clone(),
        )),
        notify_list: Some(action_notify_list_handler(storage.clone())),
        notify_mark_read: Some(action_notify_mark_read_handler(
            storage.clone(),
            notification_event_tx.clone(),
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
        )),
    }
}

fn default_trigger_notification_args(trigger: &StoredTrigger, fire_time: DateTime<Utc>) -> Value {
    json!({
        "title": trigger.name,
        "body": format!(
            "触发器「{}」已于 {} 触发",
            trigger.name,
            fire_time.with_timezone(&default_timezone()).format("%H:%M")
        ),
        "category": trigger.trigger_uuid,
        "source": "trigger",
    })
}

fn notification_id_from_action_result(result: Option<&Value>) -> Option<i64> {
    result
        .and_then(Value::as_object)
        .and_then(|value| value.get("notification_id"))
        .and_then(Value::as_i64)
}

fn parse_event_query_datetime(input: &str, end_of_day: bool) -> Result<DateTime<Utc>> {
    let input = input.trim();
    if input.is_empty() {
        anyhow::bail!("timestamp must not be empty");
    }

    if let Ok(parsed) = DateTime::parse_from_rfc3339(input) {
        return Ok(parsed.with_timezone(&Utc));
    }

    let local_offset = FixedOffset::from_str(&local_timezone_offset_string())
        .unwrap_or_else(|_| default_timezone());

    if let Ok(parsed) = chrono::NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        let naive = if end_of_day {
            parsed.and_hms_milli_opt(23, 59, 59, 999)
        } else {
            parsed.and_hms_opt(0, 0, 0)
        }
        .ok_or_else(|| anyhow!("Failed to resolve local date: {input}"))?;

        return local_offset
            .from_local_datetime(&naive)
            .single()
            .map(|dt| dt.with_timezone(&Utc))
            .ok_or_else(|| {
                anyhow!(
                    "Failed to resolve local date with offset {}: {input}",
                    local_offset
                )
            });
    }

    for pattern in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(input, pattern) {
            return local_offset
                .from_local_datetime(&naive)
                .single()
                .map(|dt| dt.with_timezone(&Utc))
                .ok_or_else(|| {
                    anyhow!(
                        "Failed to resolve local datetime with offset {}: {input}",
                        local_offset
                    )
                });
        }
    }

    anyhow::bail!(
        "Invalid timestamp '{input}'. Expected RFC3339/ISO-8601 or local datetime like 2026-04-02 09:00:00"
    )
}

pub trait TriggerCallback: Send + Sync {
    fn on_trigger(&self, summary: &TriggerExecutionSummary) -> Result<()>;
}

pub struct NotificationCallback;

impl TriggerCallback for NotificationCallback {
    fn on_trigger(&self, summary: &TriggerExecutionSummary) -> Result<()> {
        let fired_at_local = summary.fired_at.with_timezone(&default_timezone());
        info!(
            trigger_id = %summary.trigger_id,
            name = %summary.name,
            result = summary.result,
            fired_at_utc = %summary.fired_at,
            fired_at_local = %fired_at_local,
            "Trigger fired"
        );
        Ok(())
    }
}

pub struct TriggerSdk {
    storage: Storage,
    things_event_tx: broadcast::Sender<ThingsEvent>,
    trigger_event_tx: broadcast::Sender<TriggerEvent>,
    events_event_tx: broadcast::Sender<EventsEvent>,
    notification_event_tx: broadcast::Sender<NotificationEvent>,
    realtime: Arc<SupabaseRealtimeManager>,
}

impl TriggerSdk {
    pub fn initialize(db_path: impl AsRef<Path>) -> Result<Self> {
        let storage = Storage::new(db_path)?;
        storage.seed_builtin_actions(&crate::action_builtin::builtin_actions())?;
        let (things_event_tx, _rx) = broadcast::channel(2048);
        let (trigger_event_tx, _rx) = broadcast::channel(2048);
        let (events_event_tx, _rx) = broadcast::channel(2048);
        let (notification_event_tx, _rx) = broadcast::channel(2048);
        Ok(Self {
            storage,
            things_event_tx,
            trigger_event_tx,
            events_event_tx,
            notification_event_tx,
            realtime: Arc::new(SupabaseRealtimeManager::new()),
        })
    }

    pub fn things_subscribe(&self) -> broadcast::Receiver<ThingsEvent> {
        self.things_event_tx.subscribe()
    }

    pub fn triggers_subscribe(&self) -> broadcast::Receiver<TriggerEvent> {
        self.trigger_event_tx.subscribe()
    }

    pub fn events_subscribe(&self) -> broadcast::Receiver<EventsEvent> {
        self.events_event_tx.subscribe()
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

    pub(crate) fn things_storage(&self) -> &Storage {
        &self.storage
    }

    fn emit_trigger_event(&self, event: TriggerEvent) {
        // Ignore send errors (no active subscribers).
        let _ = self.trigger_event_tx.send(event);
    }

    fn emit_events_event(&self, event: EventsEvent) {
        // Ignore send errors (no active subscribers).
        let _ = self.events_event_tx.send(event);
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
        self.emit_trigger_event(TriggerEvent::DataWiped);
        self.emit_events_event(EventsEvent::DataWiped);
        self.emit_notification_event(NotificationEvent::DataWiped);
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

    pub fn register_trigger(&self, params: TriggerRegistration) -> Result<String> {
        self.register_trigger_inner(params)
    }

    fn register_trigger_inner(&self, params: TriggerRegistration) -> Result<String> {
        // UUID must be provided
        if params.trigger_uuid.is_empty() {
            anyhow::bail!("Trigger UUID is required but not provided.");
        }

        let now = Utc::now();
        let local_timezone_offset = local_timezone_offset_string();
        let normalized_precondition =
            normalize_timer_preconditions(&params.precondition, now, &local_timezone_offset)?;
        let params = TriggerRegistration {
            precondition: normalized_precondition,
            ..params
        };

        let timings = extract_timings_from_rules(&params.precondition, &params.condition)?;
        let next_fire = resolve_registration_next_fire(&timings, now, &local_timezone_offset)?;

        let trigger_uuid = params.trigger_uuid.clone();
        let inserted_uuid = self.storage.insert_trigger(params, next_fire)?;
        self.emit_trigger_event(TriggerEvent::TriggerUpsert { trigger_uuid });
        Ok(inserted_uuid)
    }

    pub fn record_event(&self, event: EventPayload) -> Result<()> {
        let event_type = event.event_type.clone();
        let event_ts = event.timestamp;
        self.storage.insert_event(&event)?;
        self.schedule_event_triggers(&event_type, event_ts)?;

        // Emit event notification to subscribers (e.g. UI).
        self.emit_events_event(EventsEvent::EventRecorded {
            event_type: event_type.clone(),
            timestamp: event_ts.to_rfc3339(),
        });

        Ok(())
    }

    /// Schedule triggers that react to Connectivity events.
    /// Prefer recording a Connectivity event via `record_event`; this shim exists for callers
    /// that still separate event persistence from trigger scheduling.
    pub fn schedule_network_change_triggers(&self, due_at: DateTime<Utc>) -> Result<()> {
        self.schedule_event_triggers("Connectivity", due_at)
    }

    /// Schedule triggers that react to Location events.
    /// Prefer recording a Location event via `record_event`; this shim exists for callers
    /// that still separate event persistence from trigger scheduling.
    pub fn schedule_location_change_triggers(&self, due_at: DateTime<Utc>) -> Result<()> {
        self.schedule_event_triggers("Location", due_at)
    }

    fn schedule_event_triggers(&self, event_type: &str, due_at: DateTime<Utc>) -> Result<()> {
        let triggers = self.storage.list_triggers()?;
        for trigger in triggers {
            let timings = extract_timings_from_rules(&trigger.precondition, &trigger.condition)
                .with_context(|| {
                    format!("Failed to inspect trigger {} timings", trigger.trigger_id)
                })?;
            let matches_event = timings.iter().any(|timing| {
                matches!(
                    timing,
                    rule_trigger_engine::TriggerTiming::Event { event_type: configured }
                        if configured == event_type
                )
            });
            if !matches_event {
                continue;
            }

            // Mark due; `run_due_triggers()` will execute it and reschedule appropriately.
            self.storage
                .mark_trigger_due(&trigger.trigger_id, due_at)
                .with_context(|| format!("Failed to mark trigger due: {}", trigger.trigger_id))?;
        }
        Ok(())
    }

    pub fn next_deadline(&self, now_unix: Option<i64>) -> Result<Option<DateTime<Utc>>> {
        self.storage.next_deadline(now_unix)
    }

    pub fn list_events_json(&self, limit: Option<u32>, offset: u32) -> Result<String> {
        let events = self.storage.list_events(limit, offset)?;
        let payloads: Vec<EventPayload> = events.into_iter().map(EventPayload::from).collect();
        to_string(&payloads).context("Failed to serialize events")
    }

    pub fn events_list_between_json(&self, start_time: &str, end_time: &str) -> Result<String> {
        let start = parse_event_query_datetime(start_time, false).context("Invalid start_time")?;
        let end = parse_event_query_datetime(end_time, true).context("Invalid end_time")?;
        if start > end {
            anyhow::bail!("start_time must be <= end_time");
        }

        let events = self
            .storage
            .list_events_between_utc(start.timestamp(), end.timestamp())?;
        let payloads: Vec<EventPayload> = events.into_iter().map(EventPayload::from).collect();
        to_string(&payloads).context("Failed to serialize events")
    }

    pub fn events_abstract_json(&self, top_n: u32) -> Result<String> {
        #[derive(Default)]
        struct Bucket {
            total: u32,
            counts: std::collections::BTreeMap<String, u32>,
        }

        // For now, read all events and bucket by UTC hour.
        // If needed, we can optimize with SQL aggregation.
        let events = self.storage.list_events(None, 0)?;
        let mut buckets: std::collections::BTreeMap<String, Bucket> =
            std::collections::BTreeMap::new();

        for ev in events {
            let dt = ev.timestamp;
            let hour_key = format!(
                "{:04}-{:02}-{:02} {:02}:00",
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour()
            );

            let bucket = buckets.entry(hour_key).or_default();
            bucket.total += 1;
            let et = ev.event_type.clone();
            *bucket.counts.entry(et).or_insert(0) += 1;
        }

        let mut hours_json = Vec::new();
        for (hour, bucket) in buckets {
            let mut top: Vec<(String, u32)> = bucket.counts.into_iter().collect();
            top.sort_by(|a, b| b.1.cmp(&a.1));
            top.truncate(top_n as usize);
            let top_types: Vec<serde_json::Value> = top
                .into_iter()
                .map(|(t, c)| json!({"type": t, "count": c}))
                .collect();
            hours_json.push(json!({
                "hour": hour,
                "total_events": bucket.total,
                "top_types": top_types,
            }));
        }

        to_string(&json!({"hours": hours_json, "top_n": top_n})).context("Failed to serialize")
    }

    pub fn event_count(&self) -> Result<i64> {
        self.storage.events_count()
    }

    pub fn event_time_range(&self) -> Result<Option<(DateTime<Utc>, DateTime<Utc>)>> {
        self.storage.events_time_range()
    }

    pub fn list_triggers(&self) -> Result<Vec<TriggerInfo>> {
        self.storage.list_triggers()
    }

    pub fn list_triggers_json(&self) -> Result<String> {
        let triggers = self.list_triggers()?;
        to_string(&triggers).context("Failed to serialize triggers")
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

    /// Pause or resume a trigger. Returns the updated paused state.
    pub fn set_trigger_paused(&self, trigger_uuid: &str, paused: bool) -> Result<()> {
        self.storage.set_trigger_paused(trigger_uuid, paused)
    }

    /// Record a binding between a trigger and a thing/collection.
    pub fn upsert_trigger_binding(
        &self,
        trigger_uuid: &str,
        entity_type: &str,
        entity_uuid: &str,
    ) -> Result<()> {
        self.storage
            .upsert_trigger_binding(trigger_uuid, entity_type, entity_uuid)
    }

    /// Remove the `trigger_bindings` row for the given entity.
    ///
    /// Use this when unbinding a trigger so that `is_trigger_bound` correctly reflects
    /// the new state before calling `delete_trigger_if_unbound`.
    pub fn delete_trigger_binding(&self, entity_type: &str, entity_uuid: &str) -> Result<()> {
        self.storage
            .delete_trigger_binding(entity_type, entity_uuid)
    }

    /// Get the trigger UUID currently bound to a specific entity from the trigger_bindings table.
    ///
    /// This supplements the CRDT snapshot lookup and catches stale bindings that the CRDT
    /// may not reflect (e.g., edge cases from migrations or non-CRDT binding paths).
    pub fn get_trigger_for_entity(
        &self,
        entity_type: &str,
        entity_uuid: &str,
    ) -> Result<Option<String>> {
        self.storage
            .get_trigger_for_entity(entity_type, entity_uuid)
    }

    /// Delete a trigger definition if it is no longer bound to any entity.
    ///
    /// Returns `true` if the trigger was deleted.
    pub fn delete_trigger_if_unbound(&self, trigger_uuid: &str) -> Result<bool> {
        if self.storage.is_trigger_bound(trigger_uuid)? {
            return Ok(false);
        }

        let deleted = self.storage.delete_trigger(trigger_uuid)?;
        if deleted {
            self.emit_trigger_event(TriggerEvent::TriggerDelete {
                trigger_uuid: trigger_uuid.to_string(),
            });
        }

        Ok(deleted)
    }

    /// Delete a trigger and all its bindings unconditionally.
    ///
    /// This is the correct method for an explicit user-initiated delete:
    /// it clears the CRDT on every bound entity, removes all `trigger_bindings`
    /// rows, deletes the trigger record, and emits the `TriggerDelete` event.
    ///
    /// Returns `true` if the trigger record was found and deleted.
    pub fn delete_trigger_and_bindings(&self, device_id: &str, trigger_uuid: &str) -> Result<bool> {
        // 1. Collect every entity currently bound to this trigger.
        let bound = self.storage.get_entities_for_trigger(trigger_uuid)?;

        // 2. Clear the CRDT trigger_uuid on each bound entity (best-effort).
        for (entity_type, entity_uuid) in &bound {
            let result = match entity_type.as_str() {
                "collection" => self.things_patch_collection_trigger_uuid(
                    device_id,
                    entity_uuid,
                    FieldPatch::Clear,
                ),
                "thing" => {
                    self.things_patch_thing_trigger_uuid(device_id, entity_uuid, FieldPatch::Clear)
                }
                other => {
                    tracing::warn!(entity_type = %other, "Unknown entity_type in trigger_bindings; skipping CRDT clear");
                    Ok(())
                }
            };
            if let Err(e) = result {
                tracing::warn!(
                    entity_type,
                    entity_uuid,
                    "Failed to clear CRDT trigger on entity: {e}"
                );
            }
        }

        // 3. Remove all trigger_bindings rows for this trigger.
        let removed = self.storage.delete_all_bindings_for_trigger(trigger_uuid)?;
        tracing::debug!(trigger_uuid, removed, "Removed trigger_bindings rows");

        // 4. Delete the trigger record.
        let deleted = self.storage.delete_trigger(trigger_uuid)?;
        if deleted {
            self.emit_trigger_event(TriggerEvent::TriggerDelete {
                trigger_uuid: trigger_uuid.to_string(),
            });
        }

        Ok(deleted)
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
        let triggers = self.storage.list_triggers_for_context_prompt(50)?;
        info!(
            triggers = triggers.len(),
            ms = t2.elapsed().as_millis(),
            "build_session_context: list_triggers_for_context_prompt"
        );

        let t3 = Instant::now();
        let out = context_prompt::build_context_prompt_markdown(
            granted_permissions,
            &snapshot,
            &triggers,
            active_context_json,
        )?;
        info!(
            out_bytes = out.len(),
            ms = t3.elapsed().as_millis(),
            total_ms = total.elapsed().as_millis(),
            "build_session_context: build_context_prompt_markdown"
        );

        Ok(out)
    }

    pub fn run_due_triggers<C>(&self, callback: &C) -> Result<Vec<TriggerExecutionSummary>>
    where
        C: TriggerCallback,
    {
        let now = Utc::now();
        let due = self.storage.fetch_due_triggers(now)?;
        let mut summaries = Vec::new();

        for trigger in due {
            let fire_time = trigger.next_fire.unwrap_or(now);
            let summary = self.execute_trigger(&trigger, fire_time, TriggerRunType::Automatic)?;
            callback.on_trigger(&summary)?;

            // Parse rules to determine the next schedule.
            let precondition: Vec<TriggerRule> = serde_json::from_str(&trigger.precondition_json)
                .context("Failed to parse precondition JSON")?;
            let condition: Vec<TriggerRule> = serde_json::from_str(&trigger.condition_json)
                .context("Failed to parse condition JSON")?;
            let timings = extract_timings_from_rules(&precondition, &condition)?;

            // Ensure the computed next fire is strictly after "now" even if this run is late.
            let next_fire =
                resolve_post_run_next_fire(&timings, fire_time, now, DEFAULT_TIMEZONE_OFFSET)?;

            self.storage.update_next_fire(
                &trigger.trigger_uuid,
                summary.fired_at,
                next_fire,
                summary.result,
            )?;

            self.emit_trigger_event(TriggerEvent::TriggerFired {
                trigger_uuid: trigger.trigger_uuid.clone(),
                fired_at: summary.fired_at.to_rfc3339(),
                next_fire: next_fire.map(|dt| dt.to_rfc3339()),
                result: summary.result,
            });
            summaries.push(summary);
        }

        Ok(summaries)
    }

    /// Get things and collections bound to a trigger
    fn get_bound_entities_for_trigger(
        &self,
        trigger_uuid: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let entities = self.storage.get_entities_for_trigger(trigger_uuid)?;
        let mut thing_uuids = Vec::new();
        let mut collection_uuids = Vec::new();

        for (entity_type, entity_uuid) in entities {
            match entity_type.as_str() {
                "thing" => thing_uuids.push(entity_uuid),
                "collection" => collection_uuids.push(entity_uuid),
                _ => warn!(entity_type = %entity_type, "Unknown entity type in trigger binding"),
            }
        }

        Ok((thing_uuids, collection_uuids))
    }

    /// Public API to get entities bound to a trigger (for FFI/Flutter bridge)
    pub fn get_bound_entities_for_trigger_api(
        &self,
        trigger_uuid: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        self.get_bound_entities_for_trigger(trigger_uuid)
    }

    pub fn run_trigger_now<C>(&self, trigger_uuid: &str, callback: &C) -> Result<bool>
    where
        C: TriggerCallback,
    {
        let trigger = self
            .storage
            .fetch_trigger(trigger_uuid)?
            .ok_or_else(|| anyhow!("Trigger not found: {trigger_uuid}"))?;
        let fire_time = Utc::now();
        let summary = self.execute_trigger(&trigger, fire_time, TriggerRunType::Manual)?;
        callback.on_trigger(&summary)?;

        // Parse rules to determine the next schedule.
        let precondition: Vec<TriggerRule> = serde_json::from_str(&trigger.precondition_json)
            .context("Failed to parse precondition JSON")?;
        let condition: Vec<TriggerRule> = serde_json::from_str(&trigger.condition_json)
            .context("Failed to parse condition JSON")?;
        let timings = extract_timings_from_rules(&precondition, &condition)?;
        let next_fire =
            resolve_post_run_next_fire(&timings, fire_time, fire_time, DEFAULT_TIMEZONE_OFFSET)?;

        self.storage.update_next_fire(
            &trigger.trigger_uuid,
            summary.fired_at,
            next_fire,
            summary.result,
        )?;

        self.emit_trigger_event(TriggerEvent::TriggerFired {
            trigger_uuid: trigger.trigger_uuid.clone(),
            fired_at: summary.fired_at.to_rfc3339(),
            next_fire: next_fire.map(|dt| dt.to_rfc3339()),
            result: summary.result,
        });

        Ok(summary.result)
    }

    /// Simulate time advancing to a specific point and run all due triggers
    /// Returns summaries of all executed triggers
    pub fn simulate_time_to<C>(
        &self,
        target_time: DateTime<Utc>,
        callback: &C,
    ) -> Result<Vec<TriggerExecutionSummary>>
    where
        C: TriggerCallback,
    {
        let due = self.storage.fetch_due_triggers(target_time)?;
        let mut summaries = Vec::new();

        for trigger in due {
            let fire_time = trigger.next_fire.unwrap_or(target_time);
            let summary = self.execute_trigger(&trigger, fire_time, TriggerRunType::Automatic)?;
            callback.on_trigger(&summary)?;

            // Parse rules to determine the next schedule.
            let precondition: Vec<TriggerRule> = serde_json::from_str(&trigger.precondition_json)
                .context("Failed to parse precondition JSON")?;
            let condition: Vec<TriggerRule> = serde_json::from_str(&trigger.condition_json)
                .context("Failed to parse condition JSON")?;
            let timings = extract_timings_from_rules(&precondition, &condition)?;

            let next_fire = resolve_post_run_next_fire(
                &timings,
                fire_time,
                target_time,
                DEFAULT_TIMEZONE_OFFSET,
            )?;
            self.storage.update_next_fire(
                &trigger.trigger_uuid,
                summary.fired_at,
                next_fire,
                summary.result,
            )?;

            self.emit_trigger_event(TriggerEvent::TriggerFired {
                trigger_uuid: trigger.trigger_uuid.clone(),
                fired_at: summary.fired_at.to_rfc3339(),
                next_fire: next_fire.map(|dt| dt.to_rfc3339()),
                result: summary.result,
            });
            summaries.push(summary);
        }

        Ok(summaries)
    }

    pub fn replay_trigger(
        &self,
        trigger_uuid: &str,
        start_iso: Option<String>,
        end_iso: Option<String>,
    ) -> Result<TriggerReplaySummary> {
        let trigger = self
            .storage
            .fetch_trigger(trigger_uuid)?
            .ok_or_else(|| anyhow!("Trigger not found: {trigger_uuid}"))?;

        let range = self
            .storage
            .events_time_range()?
            .ok_or_else(|| anyhow!("No events available for replay"))?;

        let start = match start_iso {
            Some(value) => DateTime::parse_from_rfc3339(&value)
                .map(|dt| dt.with_timezone(&Utc))
                .context("Invalid replay start timestamp")?,
            None => range.0,
        };

        let end = match end_iso {
            Some(value) => DateTime::parse_from_rfc3339(&value)
                .map(|dt| dt.with_timezone(&Utc))
                .context("Invalid replay end timestamp")?,
            None => range.1,
        };

        if end <= start {
            return Err(anyhow!("Replay window must be positive"));
        }

        // Parse rules to extract schedule metadata + optional repeat frequency.
        let precondition: Vec<TriggerRule> = serde_json::from_str(&trigger.precondition_json)
            .context("Failed to parse precondition JSON")?;
        let condition: Vec<TriggerRule> = serde_json::from_str(&trigger.condition_json)
            .context("Failed to parse condition JSON")?;
        let timings = extract_timings_from_rules(&precondition, &condition)?;
        let repeat_freq = extract_repeat_frequency_from_conditions(&condition);
        let events: Vec<MonitoringEvent> = self
            .storage
            .list_events_between_utc(start.timestamp(), end.timestamp())?
            .into_iter()
            .map(|ev| MonitoringEvent {
                event_type: ev.event_type,
                timestamp: ev.timestamp.to_rfc3339(),
                metadata_json: serde_json::to_string(&ev.metadata)
                    .unwrap_or_else(|_| "{}".to_string()),
            })
            .collect();
        let occurrences = build_trigger_occurrences(
            &timings,
            &events,
            start,
            end,
            start,
            DEFAULT_TIMEZONE_OFFSET,
        )?;

        let mut runs_considered = 0;
        let mut runs_executed = 0;
        let mut runs_succeeded = 0;
        let mut last_success: Option<DateTime<Utc>> = None;

        for fire_time in occurrences {
            runs_considered += 1;

            if let Some(last) = last_success {
                if let Some(ref freq) = repeat_freq {
                    if let Some(min_gap) = repeat_min_gap(freq) {
                        if fire_time - last < min_gap {
                            continue;
                        }
                    }
                }
            }

            let summary = self.execute_trigger(&trigger, fire_time, TriggerRunType::Replay)?;
            runs_executed += 1;
            if summary.result {
                runs_succeeded += 1;
                last_success = Some(summary.fired_at);
            }
        }

        Ok(TriggerReplaySummary {
            trigger_id: trigger.trigger_uuid.clone(),
            start,
            end,
            runs_considered,
            runs_executed,
            runs_succeeded,
        })
    }

    /// Test a trigger configuration against stored events without registering it.
    ///
    /// This is the SDK equivalent of the `trigger-test` CLI tool. It accepts a trigger
    /// configuration (JSON) and simulates its execution over a time range using local events.
    ///
    /// # Arguments
    /// * `trigger_json` - Full trigger configuration JSON (name, version, precondition, condition)
    /// * `start_iso` - Optional start time (RFC3339); defaults to first event timestamp
    /// * `end_iso` - Optional end time (RFC3339); defaults to last event timestamp
    /// * `manual` - If true, runs once at end time ignoring precondition gates (like --manual flag)
    ///
    /// # Returns
    /// JSON string containing simulation results
    pub fn trigger_test_json(
        &self,
        trigger_json: &str,
        start_iso: Option<String>,
        end_iso: Option<String>,
        manual: bool,
    ) -> Result<String> {
        use rule_trigger_engine::TriggerEvaluationReport;

        // Parse the trigger configuration
        let config = TriggerConfig::from_json(trigger_json)
            .map_err(|e| anyhow!("Failed to parse trigger config: {e}"))?;

        // Extract timing info from preconditions
        let timings = config
            .extract_timing()
            .map_err(|e| anyhow!("Failed to extract timing: {e}"))?;

        let timing_summary = describe_timing_sources(&timings);

        let repeat_freq = timings.iter().find_map(|t| match t {
            rule_trigger_engine::TriggerTiming::RepeatFrequency { frequency } => {
                Some(frequency.clone())
            }
            _ => None,
        });

        // Get event time range
        let range = self
            .storage
            .events_time_range()?
            .ok_or_else(|| anyhow!("No events available for testing"))?;

        let start_utc = match start_iso {
            Some(value) => DateTime::parse_from_rfc3339(&value)
                .map(|dt| dt.with_timezone(&Utc))
                .context("Invalid start timestamp")?,
            None => range.0,
        };

        let end_utc = match end_iso {
            Some(value) => DateTime::parse_from_rfc3339(&value)
                .map(|dt| dt.with_timezone(&Utc))
                .context("Invalid end timestamp")?,
            None => range.1,
        };

        if end_utc <= start_utc {
            return Err(anyhow!("Test window must be positive (start < end)"));
        }

        // Fetch all events in range for simulation
        let all_events: Vec<MonitoringEvent> = self
            .storage
            .list_events_between_utc(start_utc.timestamp(), end_utc.timestamp())?
            .into_iter()
            .map(|ev| MonitoringEvent {
                event_type: ev.event_type,
                timestamp: ev.timestamp.to_rfc3339(),
                metadata_json: serde_json::to_string(&ev.metadata)
                    .unwrap_or_else(|_| "{}".to_string()),
            })
            .collect();

        let tz = default_timezone();

        // Build result structure
        #[derive(serde::Serialize)]
        struct TriggerTestResult {
            trigger_name: String,
            timing_summary: String,
            repeat_frequency: Option<String>,
            start_time: String,
            end_time: String,
            mode: String,
            events_in_window: usize,
            runs: Vec<TriggerTestRun>,
            summary: TriggerTestSummary,
        }

        #[derive(serde::Serialize)]
        struct TriggerTestRun {
            trigger_time: String,
            result: bool,
            status: String,
            report: Option<TriggerEvaluationReport>,
        }

        #[derive(serde::Serialize)]
        struct TriggerTestSummary {
            runs_considered: u32,
            runs_executed: u32,
            runs_succeeded: u32,
        }

        let freq_str = repeat_freq.as_ref().map(|f| match f {
            rule_trigger_engine::RepeatFrequency::PerDay(n) => format!("per_day({n})"),
            rule_trigger_engine::RepeatFrequency::PerWeek(n) => format!("per_week({n})"),
        });

        // Manual mode: single evaluation at end time
        if manual {
            let visible_events = filter_events_at_time(&all_events, end_utc, 120);
            let eval_ctx = EvaluationContext {
                events: &visible_events,
                current_event: select_current_event(&visible_events, &timings, end_utc),
                current_time: end_utc.timestamp(),
                timezone_offset: DEFAULT_TIMEZONE_OFFSET,
            };

            let report = config.evaluate_detailed(&eval_ctx, PreconditionPolicy::IgnoreGates);

            let run = TriggerTestRun {
                trigger_time: end_utc.with_timezone(&tz).to_rfc3339(),
                result: report.overall_result,
                status: "manual".to_string(),
                report: Some(report.clone()),
            };

            let result = TriggerTestResult {
                trigger_name: config.name,
                timing_summary,
                repeat_frequency: freq_str,
                start_time: start_utc.to_rfc3339(),
                end_time: end_utc.to_rfc3339(),
                mode: "manual".to_string(),
                events_in_window: all_events.len(),
                runs: vec![run],
                summary: TriggerTestSummary {
                    runs_considered: 1,
                    runs_executed: 1,
                    runs_succeeded: if report.overall_result { 1 } else { 0 },
                },
            };

            return serde_json::to_string(&result).context("Failed to serialize result");
        }

        let occurrences = build_trigger_occurrences(
            &timings,
            &all_events,
            start_utc,
            end_utc,
            start_utc,
            DEFAULT_TIMEZONE_OFFSET,
        )?;

        let mut runs = Vec::new();
        let mut runs_considered = 0u32;
        let mut runs_executed = 0u32;
        let mut runs_succeeded = 0u32;
        let mut last_success: Option<DateTime<Utc>> = None;

        for trigger_time_utc in occurrences {
            runs_considered += 1;

            // Check repeat frequency gating
            if let (Some(last), Some(freq)) = (last_success, &repeat_freq) {
                if let Some(min_gap) = repeat_min_gap(freq) {
                    if trigger_time_utc - last < min_gap {
                        runs.push(TriggerTestRun {
                            trigger_time: trigger_time_utc.with_timezone(&tz).to_rfc3339(),
                            result: false,
                            status: "skipped_repeat_frequency".to_string(),
                            report: None,
                        });
                        continue;
                    }
                }
            }

            let visible_events = filter_events_at_time(&all_events, trigger_time_utc, 120);
            let eval_ctx = EvaluationContext {
                events: &visible_events,
                current_event: select_current_event(&visible_events, &timings, trigger_time_utc),
                current_time: trigger_time_utc.timestamp(),
                timezone_offset: DEFAULT_TIMEZONE_OFFSET,
            };

            let report = config.evaluate_detailed(&eval_ctx, PreconditionPolicy::EnforceAsGates);

            let has_error = report
                .preconditions
                .iter()
                .chain(report.conditions.iter())
                .any(|e| e.error.is_some());

            runs_executed += 1;
            let fired = report.overall_result;
            if fired {
                runs_succeeded += 1;
                last_success = Some(trigger_time_utc);
            }

            runs.push(TriggerTestRun {
                trigger_time: trigger_time_utc.with_timezone(&tz).to_rfc3339(),
                result: fired,
                status: if has_error {
                    "error".to_string()
                } else if fired {
                    "fired".to_string()
                } else {
                    "not_fired".to_string()
                },
                report: Some(report),
            });
        }

        let result = TriggerTestResult {
            trigger_name: config.name,
            timing_summary,
            repeat_frequency: freq_str,
            start_time: start_utc.to_rfc3339(),
            end_time: end_utc.to_rfc3339(),
            mode: "automatic".to_string(),
            events_in_window: all_events.len(),
            runs,
            summary: TriggerTestSummary {
                runs_considered,
                runs_executed,
                runs_succeeded,
            },
        };

        serde_json::to_string(&result).context("Failed to serialize result")
    }

    fn execute_trigger(
        &self,
        trigger: &StoredTrigger,
        fire_time: DateTime<Utc>,
        run_type: TriggerRunType,
    ) -> Result<TriggerExecutionSummary> {
        info!(
            trigger_id = %trigger.trigger_uuid,
            version = %trigger.version,
            run_type = %run_type,
            "Executing trigger with CEL evaluation"
        );

        const EVENT_LOOKBACK_MINUTES: u32 = 60 * 24 * 7;

        // Parse JSON rules
        let precondition: Vec<TriggerRule> = match serde_json::from_str(&trigger.precondition_json)
        {
            Ok(v) => v,
            Err(err) => {
                let payload = json!({
                    "kind": "trigger_execution_report_v1",
                    "trigger_uuid": trigger.trigger_uuid,
                    "trigger_name": trigger.name,
                    "fired_at": fire_time.to_rfc3339(),
                    "run_type": run_type.as_str(),
                    "error": format!("Failed to parse precondition JSON: {err}"),
                });
                let message = serde_json::to_string(&payload)
                    .unwrap_or_else(|_| "{\"kind\":\"trigger_execution_report_v1\",\"error\":\"serialization_failed\"}".to_string());
                let _ = self.storage.insert_trigger_log(
                    &trigger.trigger_uuid,
                    TriggerLogLevel::Error,
                    &message,
                    fire_time,
                    run_type.clone(),
                );
                return Err(anyhow!("Failed to parse precondition JSON: {err}"));
            }
        };
        let condition: Vec<TriggerRule> = match serde_json::from_str(&trigger.condition_json) {
            Ok(v) => v,
            Err(err) => {
                let payload = json!({
                    "kind": "trigger_execution_report_v1",
                    "trigger_uuid": trigger.trigger_uuid,
                    "trigger_name": trigger.name,
                    "fired_at": fire_time.to_rfc3339(),
                    "run_type": run_type.as_str(),
                    "error": format!("Failed to parse condition JSON: {err}"),
                });
                let message = serde_json::to_string(&payload)
                    .unwrap_or_else(|_| "{\"kind\":\"trigger_execution_report_v1\",\"error\":\"serialization_failed\"}".to_string());
                let _ = self.storage.insert_trigger_log(
                    &trigger.trigger_uuid,
                    TriggerLogLevel::Error,
                    &message,
                    fire_time,
                    run_type.clone(),
                );
                return Err(anyhow!("Failed to parse condition JSON: {err}"));
            }
        };

        // Build rule-trigger-engine config
        let precondition_rules: Vec<EngineRule> = precondition
            .into_iter()
            .map(|rule| EngineRule {
                rule: rule.rule,
                description: rule.description,
            })
            .collect();
        let condition_rules: Vec<EngineRule> = condition
            .into_iter()
            .map(|rule| EngineRule {
                rule: rule.rule,
                description: rule.description,
            })
            .collect();

        let config = TriggerConfig {
            name: trigger.name.clone(),
            version: trigger.version.clone(),
            precondition: precondition_rules,
            condition: condition_rules,
        };
        let timings = config.extract_timing().unwrap_or_default();

        // Fetch recent events and map to engine event type
        let recent = self
            .storage
            .fetch_events_recent(fire_time, EVENT_LOOKBACK_MINUTES)?;
        let events: Vec<MonitoringEvent> = recent
            .into_iter()
            .map(|event| MonitoringEvent {
                event_type: event.event_type,
                timestamp: event.timestamp.to_rfc3339(),
                metadata_json: serde_json::to_string(&event.metadata)
                    .unwrap_or_else(|_| "{}".to_string()),
            })
            .collect();

        let eval_ctx = EvaluationContext {
            events: &events,
            current_event: select_current_event(&events, &timings, fire_time),
            current_time: fire_time.timestamp(),
            timezone_offset: DEFAULT_TIMEZONE_OFFSET,
        };

        let precondition_policy = match run_type {
            TriggerRunType::Manual => PreconditionPolicy::IgnoreGates,
            TriggerRunType::Automatic | TriggerRunType::Replay => {
                PreconditionPolicy::EnforceAsGates
            }
        };

        let report = config.evaluate_detailed(&eval_ctx, precondition_policy);

        // Persist one aggregated log entry per execution.
        let has_errors = report
            .preconditions
            .iter()
            .chain(report.conditions.iter())
            .any(|e| e.error.is_some());
        let level = if has_errors {
            TriggerLogLevel::Error
        } else {
            TriggerLogLevel::Info
        };
        let payload = json!({
            "kind": "trigger_execution_report_v1",
            "trigger_uuid": trigger.trigger_uuid,
            "trigger_name": trigger.name,
            "fired_at": fire_time.to_rfc3339(),
            "run_type": run_type.as_str(),
            "report": report,
        });
        if let Ok(message) = serde_json::to_string(&payload) {
            if let Err(err) = self.storage.insert_trigger_log(
                &trigger.trigger_uuid,
                level,
                &message,
                fire_time,
                run_type.clone(),
            ) {
                error!(
                    error = %err,
                    trigger_id = %trigger.trigger_uuid,
                    "Failed to persist trigger execution log entry"
                );
            }
        }

        if has_errors {
            warn!(
                trigger_id = %trigger.trigger_uuid,
                "Trigger execution completed with evaluation errors"
            );
        }

        let all_conditions_met = report.overall_result;

        let notification_id = None;

        if all_conditions_met {
            let explicit_action_uuid = trigger
                .action_uuid
                .as_deref()
                .filter(|value| !value.is_empty());
            let action_uuid =
                explicit_action_uuid.unwrap_or(DEFAULT_TRIGGER_NOTIFICATION_ACTION_UUID);
            let action_args = if explicit_action_uuid.is_some() {
                serde_json::from_str::<Value>(&trigger.action_args_json)
                    .unwrap_or_else(|_| Value::Object(Default::default()))
            } else {
                default_trigger_notification_args(trigger, fire_time)
            };

            match self.execute_action_now(
                action_uuid,
                ActionInvocationSourceKind::Trigger,
                Some("trigger"),
                Some(&trigger.trigger_uuid),
                action_args,
                None,
            ) {
                Ok(record) => {
                    let notification_id = if explicit_action_uuid.is_none() {
                        notification_id_from_action_result(record.result_json.as_ref())
                    } else {
                        None
                    };
                    return Ok(TriggerExecutionSummary {
                        trigger_id: trigger.trigger_uuid.clone(),
                        name: trigger.name.clone(),
                        fired_at: fire_time,
                        result: all_conditions_met,
                        run_type,
                        notification_id,
                    });
                }
                Err(error) => {
                    warn!(
                        trigger_id = %trigger.trigger_uuid,
                        action_uuid = %action_uuid,
                        error = %error,
                        "Trigger fired but action execution failed"
                    );
                }
            }
        }

        Ok(TriggerExecutionSummary {
            trigger_id: trigger.trigger_uuid.clone(),
            name: trigger.name.clone(),
            fired_at: fire_time,
            result: all_conditions_met,
            run_type,
            notification_id,
        })
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
