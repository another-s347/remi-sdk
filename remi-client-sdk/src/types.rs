use chrono::{DateTime, Utc};
pub use remi_things_crdt::{
    ThingsChangeLogEntry, ThingsContentSnapshot, ThingsOperationType, ThingsUndoConflict,
    ThingsUndoConflictType, ThingsUndoExecution, ThingsUndoPreview, ThingsUndoResolutionOption,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::str::FromStr;

// ============================================================================
// Things Change Log Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionDefinition {
    pub action_uuid: String,
    pub name: String,
    pub title: String,
    pub description: String,
    pub version: String,
    pub category: String,
    pub enabled: bool,
    pub metadata_json: Value,
    pub script_source: String,
    pub input_schema_json: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema_json: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionInvocationSourceKind {
    CollectionManual,
    ThingManual,
    System,
}

impl ActionInvocationSourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CollectionManual => "collection_manual",
            Self::ThingManual => "thing_manual",
            Self::System => "system",
        }
    }
}

impl fmt::Display for ActionInvocationSourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ActionInvocationSourceKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "collection_manual" => Ok(Self::CollectionManual),
            "thing_manual" => Ok(Self::ThingManual),
            "system" => Ok(Self::System),
            other => Err(format!("Unsupported action invocation source '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionInvocationRecord {
    pub invocation_uuid: String,
    pub action_uuid: String,
    pub source_kind: ActionInvocationSourceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_entity_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_entity_uuid: Option<String>,
    pub args_json: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_json: Option<Value>,
    pub console_logs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_json: Option<Value>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntityActionBinding {
    pub action_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_override: Option<String>,
    #[serde(default = "default_empty_object")]
    pub args_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolvedEntityActionBinding {
    pub action_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_override: Option<String>,
    #[serde(default = "default_empty_object")]
    pub args_json: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_enabled: Option<bool>,
    #[serde(default)]
    pub action_missing: bool,
}

// ===== Notification Types =====

/// The originating subsystem of a notification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NotificationSource {
    Push,
    System,
    Chat,
}

impl NotificationSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            NotificationSource::Push => "push",
            NotificationSource::System => "system",
            NotificationSource::Chat => "chat",
        }
    }
}

impl fmt::Display for NotificationSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NotificationSource {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "push" => Ok(Self::Push),
            "system" => Ok(Self::System),
            "chat" => Ok(Self::Chat),
            other => Err(format!("Unsupported notification source '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NotificationResponseAction {
    Ok,
    NotRightTime,
}

impl NotificationResponseAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            NotificationResponseAction::Ok => "ok",
            NotificationResponseAction::NotRightTime => "not_right_time",
        }
    }
}

impl fmt::Display for NotificationResponseAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NotificationResponseAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "ok" => Ok(Self::Ok),
            "not_right_time" => Ok(Self::NotRightTime),
            other => Err(format!(
                "Unsupported notification response action '{other}'"
            )),
        }
    }
}

/// A single notification entry persisted in SQLite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationEntry {
    pub id: i64,
    pub source: NotificationSource,
    /// Grouping key for related notifications.
    pub category: String,
    /// Human-readable title for the notification (e.g. trigger name).
    pub title: String,
    /// Main notification body text.
    pub body: String,
    pub is_read: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_action: Option<NotificationResponseAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub responded_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Aggregated group of notifications sharing the same category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationGroup {
    pub category: String,
    pub title: String,
    pub source: NotificationSource,
    pub latest_at: DateTime<Utc>,
    pub unread_count: i64,
    pub total_count: i64,
    pub items: Vec<NotificationEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VirtualFsNodeKind {
    Tree,
    Directory,
    File,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualFsReadResult {
    pub path: String,
    pub kind: VirtualFsNodeKind,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualFsProfileStep {
    pub name: String,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualFsProfileResult {
    pub operation: String,
    pub path: String,
    pub total_ms: u64,
    pub output_bytes: usize,
    pub steps: Vec<VirtualFsProfileStep>,
}

fn default_empty_object() -> Value {
    Value::Object(Default::default())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatSession {
    pub session_id: String,
    pub title: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub message_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatSessionUpdate {
    pub session_id: String,
    pub title: Option<String>,
    pub last_activity: DateTime<Utc>,
    pub message_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentVersion {
    pub version_id: String,
    pub agent_id: String,
    pub name: String,
    pub raw_markdown: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub applied_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentVersionUpdate {
    pub version_id: String,
    pub name: String,
    pub raw_markdown: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDataset {
    pub dataset_id: String,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDatasetUpdate {
    pub dataset_id: String,
    pub name: String,
    pub description: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDatasetSession {
    pub dataset_id: String,
    pub session_id: String,
    pub added_at: DateTime<Utc>,
    pub title: Option<String>,
    pub last_activity: Option<DateTime<Utc>>,
    pub message_count: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDatasetRun {
    pub run_id: String,
    pub dataset_id: String,
    pub agent_id: String,
    pub agent_version_id: Option<String>,
    pub agent_version_name: Option<String>,
    pub variant_id: String,
    pub variant_label: String,
    pub source_session_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDatasetRunItem {
    pub run_id: String,
    pub dataset_id: String,
    pub session_id: String,
    pub title: Option<String>,
    pub last_activity: Option<DateTime<Utc>>,
    pub message_count: Option<i32>,
    pub final_text: String,
    pub reasoning: Option<String>,
    pub prompt_tokens: i32,
    pub completion_tokens: i32,
    pub tool_results_json: String,
    pub done: bool,
    pub cancelled: bool,
    pub interrupted: bool,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDatasetRunEval {
    pub run_id: String,
    pub dataset_id: String,
    pub session_id: String,
    pub analysis_agent_id: String,
    pub score: String,
    pub summary: String,
    pub rationale: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ============================================================================
// Location Types for Trigger System
// ============================================================================

/// Coordinate system type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CoordinateSystem {
    #[default]
    Wgs84, // GPS standard coordinate system
    Gcj02, // Chinese GCJ-02 coordinate system (Mars coordinates)
}

impl CoordinateSystem {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Wgs84 => "wgs84",
            Self::Gcj02 => "gcj02",
        }
    }
}

impl FromStr for CoordinateSystem {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "wgs84" | "wgs-84" => Ok(Self::Wgs84),
            "gcj02" | "gcj-02" => Ok(Self::Gcj02),
            _ => Err(format!("Unknown coordinate system: {}", s)),
        }
    }
}

/// Location object for CEL expressions in triggers
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Location {
    /// Precise coordinate
    #[serde(rename = "coordinate")]
    Coordinate {
        lat: f64,
        lng: f64,
        /// Coordinate system type, defaults to WGS-84
        #[serde(default)]
        coord_system: CoordinateSystem,
        /// Original place name (if obtained from geocoding)
        #[serde(skip_serializing_if = "Option::is_none")]
        source_name: Option<String>,
    },

    /// Fuzzy location name (requires nearby search)
    #[serde(rename = "fuzzy")]
    FuzzyName {
        name: String,
        place_type: String, // Google Places API type
    },

    /// Invalid location (failed to obtain)
    #[serde(rename = "invalid")]
    Invalid { error: String },
}

impl Location {
    /// Create a WGS-84 coordinate
    pub fn coordinate(lat: f64, lng: f64) -> Self {
        Location::Coordinate {
            lat,
            lng,
            coord_system: CoordinateSystem::Wgs84,
            source_name: None,
        }
    }

    /// Create a WGS-84 coordinate with source name
    pub fn coordinate_from_name(lat: f64, lng: f64, name: impl Into<String>) -> Self {
        Location::Coordinate {
            lat,
            lng,
            coord_system: CoordinateSystem::Wgs84,
            source_name: Some(name.into()),
        }
    }

    /// Create a coordinate with specified coordinate system
    pub fn coordinate_with_system(lat: f64, lng: f64, coord_system: CoordinateSystem) -> Self {
        Location::Coordinate {
            lat,
            lng,
            coord_system,
            source_name: None,
        }
    }

    /// Create a fuzzy location
    pub fn fuzzy(name: impl Into<String>, place_type: impl Into<String>) -> Self {
        Location::FuzzyName {
            name: name.into(),
            place_type: place_type.into(),
        }
    }

    /// Create an invalid location with error
    pub fn invalid(error: impl Into<String>) -> Self {
        Location::Invalid {
            error: error.into(),
        }
    }

    pub fn is_valid(&self) -> bool {
        !matches!(self, Location::Invalid { .. })
    }

    pub fn is_fuzzy(&self) -> bool {
        matches!(self, Location::FuzzyName { .. })
    }

    pub fn as_coordinate(&self) -> Option<(f64, f64)> {
        match self {
            Location::Coordinate { lat, lng, .. } => Some((*lat, *lng)),
            _ => None,
        }
    }

    pub fn coord_system(&self) -> Option<CoordinateSystem> {
        match self {
            Location::Coordinate { coord_system, .. } => Some(*coord_system),
            _ => None,
        }
    }

    pub fn source_name(&self) -> Option<&str> {
        match self {
            Location::Coordinate { source_name, .. } => source_name.as_deref(),
            Location::FuzzyName { name, .. } => Some(name.as_str()),
            _ => None,
        }
    }
}

/// Cached location entry from geocoding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationCacheEntry {
    pub name: String,
    pub is_fuzzy: bool,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub coord_system: CoordinateSystem,
    pub place_id: Option<String>,
    pub place_type: Option<String>,
    pub formatted_address: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl LocationCacheEntry {
    /// Convert to Location enum
    pub fn to_location(&self) -> Location {
        if self.is_fuzzy {
            Location::FuzzyName {
                name: self.name.clone(),
                place_type: self.place_type.clone().unwrap_or_default(),
            }
        } else if let (Some(lat), Some(lng)) = (self.latitude, self.longitude) {
            Location::Coordinate {
                lat,
                lng,
                coord_system: self.coord_system,
                source_name: Some(self.name.clone()),
            }
        } else {
            Location::Invalid {
                error: format!("Cached location '{}' has no coordinates", self.name),
            }
        }
    }
}

// ============================================================================
// V3 CRDT Document Types
// ============================================================================

/// Row from crdt_documents table
#[derive(Debug, Clone)]
pub struct CrdtDocumentRow {
    pub uuid: String,
    pub data_type: String,
    pub automerge_doc: Vec<u8>,
    pub sync_state: Vec<u8>,
    pub dirty: bool,
    pub last_sync_at: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl CrdtDocumentRow {
    /// Parse data_type string to CrdtDataType enum
    pub fn crdt_data_type(&self) -> Option<remi_things_crdt::CrdtDataType> {
        match self.data_type.as_str() {
            "root" => Some(remi_things_crdt::CrdtDataType::Root),
            "collection" => Some(remi_things_crdt::CrdtDataType::Collection),
            "thing_markdown" => Some(remi_things_crdt::CrdtDataType::ThingMarkdown),
            _ => None,
        }
    }
}
