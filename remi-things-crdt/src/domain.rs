use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::str::FromStr;

use crate::{ContentEntry, ThingDatatype};

macro_rules! domain_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(raw: &str) -> Result<Self, Self::Err> {
                let value = raw.trim();
                if value.is_empty() {
                    Err(format!("{} must not be empty", stringify!($name)))
                } else {
                    Ok(Self(value.to_string()))
                }
            }
        }
    };
}

domain_id!(ThingId);
domain_id!(CollectionId);
domain_id!(ContentEntryId);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionType {
    #[default]
    Normal,
    Default,
    Trash,
    App,
}

impl CollectionType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Default => "default",
            Self::Trash => "trash",
            Self::App => "app",
        }
    }

    pub fn is_normal(self) -> bool {
        matches!(self, Self::Normal)
    }
}

impl FromStr for CollectionType {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "normal" | "type_normal" => Ok(Self::Normal),
            "default" | "type_default" => Ok(Self::Default),
            "trash" | "type_trash" => Ok(Self::Trash),
            "app" | "type_app" => Ok(Self::App),
            other => Err(format!("Invalid collection type '{other}'")),
        }
    }
}

pub fn parse_domain_datetime(raw: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| format!("Invalid RFC3339 timestamp '{raw}': {err}"))
}

pub fn parse_optional_domain_datetime(raw: Option<&str>) -> Result<Option<DateTime<Utc>>, String> {
    raw.map(parse_domain_datetime).transpose()
}

pub fn format_domain_datetime(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn parse_domain_datetime_or_unix_epoch(raw: &str) -> DateTime<Utc> {
    parse_domain_datetime(raw).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

mod rfc3339_utc {
    use super::{format_domain_datetime, parse_domain_datetime};
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_domain_datetime(*value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_domain_datetime(&value).map_err(serde::de::Error::custom)
    }
}

mod optional_rfc3339_utc {
    use super::{format_domain_datetime, parse_optional_domain_datetime};
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(value) => serializer.serialize_some(&format_domain_datetime(*value)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        parse_optional_domain_datetime(value.as_deref()).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldPatch<T> {
    Noop,
    Set(T),
    Clear,
}

impl<T> Default for FieldPatch<T> {
    fn default() -> Self {
        Self::Noop
    }
}

impl FieldPatch<String> {
    pub fn from_compat_option_str(value: Option<&str>) -> Self {
        match value {
            None => Self::Noop,
            Some(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    Self::Clear
                } else {
                    Self::Set(trimmed.to_string())
                }
            }
        }
    }

    pub fn is_noop(&self) -> bool {
        matches!(self, Self::Noop)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThingStatus {
    None,
    InProgress,
    Stalled,
    Done,
}

impl ThingStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InProgress => "in-progress",
            Self::Stalled => "stalled",
            Self::Done => "done",
        }
    }

    pub fn valid_values() -> &'static [&'static str] {
        &["none", "in-progress", "stalled", "done"]
    }
}

impl fmt::Display for ThingStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ThingStatus {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim() {
            "none" => Ok(Self::None),
            "in-progress" => Ok(Self::InProgress),
            "stalled" => Ok(Self::Stalled),
            "done" => Ok(Self::Done),
            other => Err(format!(
                "Invalid thing status '{other}', must be one of: {}",
                Self::valid_values().join(", ")
            )),
        }
    }
}

impl Serialize for ThingStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ThingStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThingCollectionEntry {
    pub uuid: String,
    pub title: String,
    #[serde(default)]
    pub collection_type: CollectionType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(
        default,
        with = "optional_rfc3339_utc",
        skip_serializing_if = "Option::is_none"
    )]
    pub archived_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub card_jsx: Option<String>,
    #[serde(with = "rfc3339_utc")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "rfc3339_utc")]
    pub updated_at: DateTime<Utc>,
    /// "user" or "application" - populated from server-side actor metadata cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_type: Option<String>,
    /// App ID when actor_type is "application".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_app_id: Option<String>,
    /// Resolved display name for the app, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingCollectionUpsert {
    pub uuid: String,
    pub title: String,
    #[serde(default)]
    pub collection_type: CollectionType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    /// Explicit trigger binding patch. Prefer this over `trigger_uuid`.
    #[serde(default, skip_serializing_if = "FieldPatch::is_noop")]
    pub trigger_uuid_patch: FieldPatch<String>,
    /// Deprecated compatibility field:
    /// - key omitted / null => no change
    /// - empty string => clear
    /// - UUID string => set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_uuid: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

impl ThingCollectionEntry {
    pub fn id(&self) -> CollectionId {
        CollectionId::new(self.uuid.clone())
    }

    pub fn created_at_utc(&self) -> Result<DateTime<Utc>, String> {
        Ok(self.created_at)
    }

    pub fn updated_at_utc(&self) -> Result<DateTime<Utc>, String> {
        Ok(self.updated_at)
    }

    pub fn trigger_uuid_patch(&self) -> FieldPatch<String> {
        FieldPatch::from_compat_option_str(self.trigger_uuid.as_deref())
    }

    pub fn is_system_collection(&self) -> bool {
        matches!(
            self.collection_type,
            CollectionType::Default | CollectionType::Trash
        )
    }
}

impl ThingCollectionUpsert {
    pub fn id(&self) -> CollectionId {
        CollectionId::new(self.uuid.clone())
    }

    pub fn created_at_utc(&self) -> Result<Option<DateTime<Utc>>, String> {
        parse_optional_domain_datetime(self.created_at.as_deref())
    }

    pub fn updated_at_utc(&self) -> Result<Option<DateTime<Utc>>, String> {
        parse_optional_domain_datetime(self.updated_at.as_deref())
    }

    pub fn trigger_uuid_patch(&self) -> FieldPatch<String> {
        match &self.trigger_uuid_patch {
            FieldPatch::Noop => FieldPatch::from_compat_option_str(self.trigger_uuid.as_deref()),
            patch => patch.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThingEntry {
    pub uuid: String,
    pub title: String,
    pub datatype: ThingDatatype,
    pub data: Value,
    pub collection_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_uuid: Option<String>,
    #[serde(default)]
    pub parent_uuid: Option<String>,
    #[serde(
        default,
        with = "optional_rfc3339_utc",
        skip_serializing_if = "Option::is_none"
    )]
    pub archived_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_from_collection_uuid: Option<String>,
    #[serde(with = "rfc3339_utc")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "rfc3339_utc")]
    pub updated_at: DateTime<Utc>,
    /// Status of the thing: "none", "in-progress", "stalled", "done".
    #[serde(default)]
    pub status: String,
    /// Timestamp when status was last changed, in milliseconds since Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_timestamp_ms: Option<i64>,
    /// "user" or "application" - populated from server-side actor metadata cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_type: Option<String>,
    /// App ID when actor_type is "application".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_app_id: Option<String>,
    /// Resolved display name for the app, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_display_name: Option<String>,
}

impl ThingEntry {
    pub fn id(&self) -> ThingId {
        ThingId::new(self.uuid.clone())
    }

    pub fn collection_id(&self) -> CollectionId {
        CollectionId::new(self.collection_uuid.clone())
    }

    pub fn parent_id(&self) -> Option<ThingId> {
        self.parent_uuid
            .as_ref()
            .map(|uuid| ThingId::new(uuid.clone()))
    }

    pub fn created_at_utc(&self) -> Result<DateTime<Utc>, String> {
        Ok(self.created_at)
    }

    pub fn updated_at_utc(&self) -> Result<DateTime<Utc>, String> {
        Ok(self.updated_at)
    }

    pub fn status_updated_at_utc(&self) -> Option<DateTime<Utc>> {
        self.status_timestamp_ms
            .and_then(DateTime::<Utc>::from_timestamp_millis)
    }

    pub fn trigger_uuid_patch(&self) -> FieldPatch<String> {
        FieldPatch::from_compat_option_str(self.trigger_uuid.as_deref())
    }

    pub fn status_enum(&self) -> Result<ThingStatus, String> {
        ThingStatus::from_str(&self.status)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingUpsert {
    pub uuid: String,
    pub title: String,
    pub datatype: ThingDatatype,
    /// Optional to support CRDT-typed markdown edits where text is updated via SpliceText ops.
    /// When omitted/null, we do not rewrite `content`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    pub collection_uuid: String,
    /// Explicit trigger binding patch. Prefer this over `trigger_uuid`.
    #[serde(default, skip_serializing_if = "FieldPatch::is_noop")]
    pub trigger_uuid_patch: FieldPatch<String>,
    /// Deprecated compatibility field:
    /// - key omitted / null => no change
    /// - empty string => clear
    /// - UUID string => set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_uuid: Option<String>,
    #[serde(default)]
    pub parent_uuid: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

impl ThingUpsert {
    pub fn id(&self) -> ThingId {
        ThingId::new(self.uuid.clone())
    }

    pub fn collection_id(&self) -> CollectionId {
        CollectionId::new(self.collection_uuid.clone())
    }

    pub fn parent_id(&self) -> Option<ThingId> {
        self.parent_uuid
            .as_ref()
            .map(|uuid| ThingId::new(uuid.clone()))
    }

    pub fn created_at_utc(&self) -> Result<Option<DateTime<Utc>>, String> {
        parse_optional_domain_datetime(self.created_at.as_deref())
    }

    pub fn updated_at_utc(&self) -> Result<Option<DateTime<Utc>>, String> {
        parse_optional_domain_datetime(self.updated_at.as_deref())
    }

    pub fn trigger_uuid_patch(&self) -> FieldPatch<String> {
        match &self.trigger_uuid_patch {
            FieldPatch::Noop => FieldPatch::from_compat_option_str(self.trigger_uuid.as_deref()),
            patch => patch.clone(),
        }
    }
}

impl ContentEntry {
    pub fn id(&self) -> ContentEntryId {
        ContentEntryId::new(self.id.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsSnapshot {
    pub collections: Vec<ThingCollectionEntry>,
    pub things: Vec<ThingEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsSnapshotState {
    pub collections: Vec<ThingCollectionEntry>,
    pub things: Vec<ThingEntry>,
    pub dirty: bool,
    #[serde(default, with = "optional_rfc3339_utc")]
    pub last_sync_at: Option<DateTime<Utc>>,
}

pub type ThingCollection = ThingCollectionEntry;
pub type Thing = ThingEntry;

impl ThingsSnapshotState {
    pub fn last_sync_at_utc(&self) -> Result<Option<DateTime<Utc>>, String> {
        Ok(self.last_sync_at)
    }
}

/// Operation type for Things change log entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThingsOperationType {
    CreateCollection,
    UpdateCollection,
    DeleteCollection,
    CreateThing,
    UpdateThing,
    DeleteThing,
    MoveThing,
    MoveThings,
    DeleteThings,
    SyncApplied,
    UndoCreateCollection,
    UndoUpdateCollection,
    UndoDeleteCollection,
    UndoCreateThing,
    UndoUpdateThing,
    UndoDeleteThing,
    UndoMoveThing,
    UndoMoveThings,
    UndoDeleteThings,
    RedoCreateCollection,
    RedoUpdateCollection,
    RedoDeleteCollection,
    RedoCreateThing,
    RedoUpdateThing,
    RedoDeleteThing,
    RedoMoveThing,
    RedoMoveThings,
    RedoDeleteThings,
}

impl ThingsOperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CreateCollection => "create_collection",
            Self::UpdateCollection => "update_collection",
            Self::DeleteCollection => "delete_collection",
            Self::CreateThing => "create_thing",
            Self::UpdateThing => "update_thing",
            Self::DeleteThing => "delete_thing",
            Self::MoveThing => "move_thing",
            Self::MoveThings => "move_things",
            Self::DeleteThings => "delete_things",
            Self::SyncApplied => "sync_applied",
            Self::UndoCreateCollection => "undo_create_collection",
            Self::UndoUpdateCollection => "undo_update_collection",
            Self::UndoDeleteCollection => "undo_delete_collection",
            Self::UndoCreateThing => "undo_create_thing",
            Self::UndoUpdateThing => "undo_update_thing",
            Self::UndoDeleteThing => "undo_delete_thing",
            Self::UndoMoveThing => "undo_move_thing",
            Self::UndoMoveThings => "undo_move_things",
            Self::UndoDeleteThings => "undo_delete_things",
            Self::RedoCreateCollection => "redo_create_collection",
            Self::RedoUpdateCollection => "redo_update_collection",
            Self::RedoDeleteCollection => "redo_delete_collection",
            Self::RedoCreateThing => "redo_create_thing",
            Self::RedoUpdateThing => "redo_update_thing",
            Self::RedoDeleteThing => "redo_delete_thing",
            Self::RedoMoveThing => "redo_move_thing",
            Self::RedoMoveThings => "redo_move_things",
            Self::RedoDeleteThings => "redo_delete_things",
        }
    }

    pub fn is_undo(&self) -> bool {
        matches!(
            self,
            Self::UndoCreateCollection
                | Self::UndoUpdateCollection
                | Self::UndoDeleteCollection
                | Self::UndoCreateThing
                | Self::UndoUpdateThing
                | Self::UndoDeleteThing
                | Self::UndoMoveThing
                | Self::UndoMoveThings
                | Self::UndoDeleteThings
        )
    }

    pub fn is_redo(&self) -> bool {
        matches!(
            self,
            Self::RedoCreateCollection
                | Self::RedoUpdateCollection
                | Self::RedoDeleteCollection
                | Self::RedoCreateThing
                | Self::RedoUpdateThing
                | Self::RedoDeleteThing
                | Self::RedoMoveThing
                | Self::RedoMoveThings
                | Self::RedoDeleteThings
        )
    }

    pub fn to_undo_variant(&self) -> Option<Self> {
        match self {
            Self::CreateCollection => Some(Self::UndoCreateCollection),
            Self::UpdateCollection => Some(Self::UndoUpdateCollection),
            Self::DeleteCollection => Some(Self::UndoDeleteCollection),
            Self::CreateThing => Some(Self::UndoCreateThing),
            Self::UpdateThing => Some(Self::UndoUpdateThing),
            Self::DeleteThing => Some(Self::UndoDeleteThing),
            Self::MoveThing => Some(Self::UndoMoveThing),
            Self::MoveThings => Some(Self::UndoMoveThings),
            Self::DeleteThings => Some(Self::UndoDeleteThings),
            _ => None,
        }
    }

    pub fn to_redo_variant(&self) -> Option<Self> {
        match self {
            Self::UndoCreateCollection => Some(Self::RedoCreateCollection),
            Self::UndoUpdateCollection => Some(Self::RedoUpdateCollection),
            Self::UndoDeleteCollection => Some(Self::RedoDeleteCollection),
            Self::UndoCreateThing => Some(Self::RedoCreateThing),
            Self::UndoUpdateThing => Some(Self::RedoUpdateThing),
            Self::UndoDeleteThing => Some(Self::RedoDeleteThing),
            Self::UndoMoveThing => Some(Self::RedoMoveThing),
            Self::UndoMoveThings => Some(Self::RedoMoveThings),
            Self::UndoDeleteThings => Some(Self::RedoDeleteThings),
            _ => None,
        }
    }
}

impl fmt::Display for ThingsOperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ThingsOperationType {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "create_collection" => Ok(Self::CreateCollection),
            "update_collection" => Ok(Self::UpdateCollection),
            "delete_collection" => Ok(Self::DeleteCollection),
            "create_thing" => Ok(Self::CreateThing),
            "update_thing" => Ok(Self::UpdateThing),
            "delete_thing" => Ok(Self::DeleteThing),
            "move_thing" => Ok(Self::MoveThing),
            "move_things" => Ok(Self::MoveThings),
            "delete_things" => Ok(Self::DeleteThings),
            "sync_applied" => Ok(Self::SyncApplied),
            "undo_create_collection" => Ok(Self::UndoCreateCollection),
            "undo_update_collection" => Ok(Self::UndoUpdateCollection),
            "undo_delete_collection" => Ok(Self::UndoDeleteCollection),
            "undo_create_thing" => Ok(Self::UndoCreateThing),
            "undo_update_thing" => Ok(Self::UndoUpdateThing),
            "undo_delete_thing" => Ok(Self::UndoDeleteThing),
            "undo_move_thing" => Ok(Self::UndoMoveThing),
            "undo_move_things" => Ok(Self::UndoMoveThings),
            "undo_delete_things" => Ok(Self::UndoDeleteThings),
            "redo_create_collection" => Ok(Self::RedoCreateCollection),
            "redo_update_collection" => Ok(Self::RedoUpdateCollection),
            "redo_delete_collection" => Ok(Self::RedoDeleteCollection),
            "redo_create_thing" => Ok(Self::RedoCreateThing),
            "redo_update_thing" => Ok(Self::RedoUpdateThing),
            "redo_delete_thing" => Ok(Self::RedoDeleteThing),
            "redo_move_thing" => Ok(Self::RedoMoveThing),
            "redo_move_things" => Ok(Self::RedoMoveThings),
            "redo_delete_things" => Ok(Self::RedoDeleteThings),
            _ => Err(format!("Unknown operation type: {raw}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsChangeLogEntry {
    pub id: i64,
    pub device_id: String,
    pub op_type: ThingsOperationType,
    pub entity_type: String,
    pub entity_uuid: String,
    pub summary: String,
    pub details_json: String,
    pub parent_log_id: Option<i64>,
    pub cascade_log_ids_json: Option<String>,
    pub created_at: DateTime<Utc>,
    pub can_undo: bool,
    pub synced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsContentSnapshot {
    pub id: i64,
    pub device_id: String,
    pub thing_uuid: String,
    pub content_json: String,
    pub change_log_id: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub synced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsUndoPreview {
    pub log_entry: ThingsChangeLogEntry,
    pub needs_cascade_restore: bool,
    pub conflict: Option<ThingsUndoConflict>,
    pub cascade_entries: Vec<ThingsChangeLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsUndoConflict {
    pub conflict_type: ThingsUndoConflictType,
    pub description: String,
    pub options: Vec<ThingsUndoResolutionOption>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThingsUndoConflictType {
    ParentDeleted,
    EntityModified,
    EntityExists,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsUndoResolutionOption {
    pub id: String,
    pub label: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsUndoExecution {
    pub log_id: i64,
    pub resolution_option: Option<String>,
    pub target_collection_uuid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThingsMutationEvent {
    pub event_id: i64,
    pub device_id: String,
    pub source: String,
    pub entity_type: String,
    pub entity_uuid: String,
    pub change_kind: String,
    pub payload_json: String,
    pub created_at: i64,
    pub sync_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ThingsSyncSummary {
    pub documents_synced: usize,
    pub documents_pushed: usize,
    pub documents_pulled: usize,
    #[serde(default, with = "optional_rfc3339_utc")]
    pub last_sync_at: Option<DateTime<Utc>>,
    pub generated_event_range: Option<(i64, i64)>,
}
