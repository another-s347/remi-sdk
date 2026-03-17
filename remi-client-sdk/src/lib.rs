pub mod app_keys_client;
pub mod app_update;
pub mod auth;
pub mod chat_client;
pub mod chat_runtime;
pub mod chat_types;
mod context_prompt;
pub mod crdt_sync;
pub mod data_lifecycle;
pub mod events_events;
pub mod interrupt_handler;
mod local_wasm;
pub mod location_service;
pub mod profile;
pub mod push_tokens;
pub mod realtime;
pub mod remi_uri;
mod runtime;
#[cfg(feature = "sentry-integration")]
pub mod sentry_integration;
mod storage;
pub mod telemetry;
pub mod things_client;
pub mod things_crdt;
pub mod things_crdt_v2;
pub mod things_events;
pub mod things_handlers;
pub mod things_sync;
pub mod transport;
pub mod trigger_client;
pub mod trigger_events;
mod types;
pub mod uri_resolver;
pub mod url_handlers;

pub use app_keys_client::AppKeysClient;
pub use auth::{AuthClient, AuthCredentials, SecureSessionStore, SdkBearerAuthMode};
pub use chat_client::{
    ChatClient, ChatHistoryMessage, ChatInputMessage, ChatStreamEvent, ChatToolCall,
    ChatToolCallOutcome, chat_request, chat_stream_event,
};
pub use chat_runtime::ChatRuntime;
pub use chat_types::{
    CachedMessage, ChatLocalWasmConfig, ChatLocalWasmSource, ChatRunState, ChatRunStatus,
    ChatRuntimeBackend, ChatRuntimeConfig, ChatRuntimeEvent, InterruptAction, PendingInterrupt,
};
pub use interrupt_handler::{InterruptHandler, InterruptHandlerRegistry};
pub use location_service::{
    LocationService, LocationServiceError, haversine_distance, is_within_range,
};
pub use profile::{AvatarUploadInfo, MediaUploadInfo, ProfileClient, ProfileInfo};
pub use realtime::{RealtimeConfig, RemiRealtimeEvent, SupabaseRealtimeManager};
pub use remi_uri::{RemiUri, RemiUriLocation, mime_from_extension};
pub use runtime::{NotificationCallback, TriggerCallback, TriggerSdk};
pub use things_client::ThingsClient;
pub use things_handlers::register_things_handlers;
pub use trigger_client::{ServerTriggerInfo, TriggerClient};
pub use types::{
    ChatSession, ChatSessionUpdate, CoordinateSystem, EventPayload, Location, LocationCacheEntry,
    NotificationEntry, NotificationGroup, NotificationSource, ThingsChangeLogEntry,
    ThingsContentSnapshot, ThingsOperationType, ThingsUndoConflict, ThingsUndoConflictType,
    ThingsUndoExecution, ThingsUndoPreview, ThingsUndoResolutionOption, TriggerExecutionSummary,
    TriggerLogEntry, TriggerLogLevel, TriggerRegistration, TriggerReplaySummary, TriggerRule,
    TriggerRunType,
};
pub use url_handlers::register_url_handlers;

#[derive(Debug, Default, Clone, Copy)]
pub struct TriggerContext;
