use crate::storage::{Storage, StoredChatMessage};
use crate::things_crdt::{SnapshotOptions, ThingCollectionEntry, ThingEntry};
use crate::things_local::ThingsLocalService;
use crate::types::{ActionDefinition, AgentVersion, ChatSession, NotificationEntry};
use anyhow::{Context, Result, anyhow};
use charabia::Tokenize;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock, mpsc};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::Value as TantivyValue;
use tantivy::schema::{
    FAST, Field, FieldType, INDEXED, IndexRecordOption, STORED, STRING, Schema, TantivyDocument,
    TextFieldIndexing, TextOptions,
};
use tantivy::tokenizer::{MAX_TOKEN_LEN, Token, TokenStream, Tokenizer};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, Term};
use tracing::warn;

const SEARCH_REBUILD_DEVICE_ID: &str = "search-index";
const DEFAULT_SEARCH_LIMIT: u32 = 20;
const MAX_SEARCH_LIMIT: u32 = 100;
const DEFAULT_WRITER_HEAP_BYTES: usize = 50_000_000;
const SEARCH_INDEX_VERSION: &str = "4";
const SEARCH_TEXT_TOKENIZER: &str = "remi_charabia";
const NUMERIC_FRAGMENT_MIN_LEN: usize = 2;
const NUMERIC_FRAGMENT_MAX_LEN: usize = 16;
const NUMERIC_FRAGMENT_MAX_COUNT: usize = 512;

#[derive(Clone, Default)]
struct CharabiaTantivyTokenizer;

struct CharabiaTantivyTokenStream {
    tokens: Vec<Token>,
    current: Token,
    next_index: usize,
}

impl Tokenizer for CharabiaTantivyTokenizer {
    type TokenStream<'a> = CharabiaTantivyTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let mut position = 0;
        let tokens = text
            .tokenize()
            .filter(|token| !token.is_separator())
            .filter_map(|token| {
                let lemma = token.lemma();
                if lemma.is_empty() || lemma.len() >= MAX_TOKEN_LEN {
                    return None;
                }
                let tantivy_token = Token {
                    offset_from: token.byte_start,
                    offset_to: token.byte_end,
                    position,
                    text: lemma.to_string(),
                    position_length: 1,
                };
                position += 1;
                Some(tantivy_token)
            })
            .collect();
        CharabiaTantivyTokenStream {
            tokens,
            current: Token::default(),
            next_index: 0,
        }
    }
}

impl TokenStream for CharabiaTantivyTokenStream {
    fn advance(&mut self) -> bool {
        let Some(token) = self.tokens.get(self.next_index) else {
            return false;
        };
        self.current = token.clone();
        self.next_index += 1;
        true
    }

    fn token(&self) -> &Token {
        &self.current
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.current
    }
}

fn register_search_tokenizers(index: &Index) {
    index
        .tokenizers()
        .register(SEARCH_TEXT_TOKENIZER, CharabiaTantivyTokenizer);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchEntityKind {
    Collection,
    Thing,
    ChatSession,
    ChatMessage,
    Notification,
    Action,
    AgentVersion,
}

impl SearchEntityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Collection => "collection",
            Self::Thing => "thing",
            Self::ChatSession => "chat_session",
            Self::ChatMessage => "chat_message",
            Self::Notification => "notification",
            Self::Action => "action",
            Self::AgentVersion => "agent_version",
        }
    }
}

impl fmt::Display for SearchEntityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SearchEntityKind {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "collection" => Ok(Self::Collection),
            "thing" => Ok(Self::Thing),
            "chat_session" => Ok(Self::ChatSession),
            "chat_message" => Ok(Self::ChatMessage),
            "notification" => Ok(Self::Notification),
            "action" => Ok(Self::Action),
            "agent_version" => Ok(Self::AgentVersion),
            other => Err(format!("Unsupported search entity kind '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_path: Option<PathBuf>,
    #[serde(default = "default_writer_heap_bytes")]
    pub writer_heap_bytes: usize,
    #[serde(default = "default_auto_rebuild")]
    pub auto_rebuild: bool,
}

fn default_writer_heap_bytes() -> usize {
    DEFAULT_WRITER_HEAP_BYTES
}

fn default_auto_rebuild() -> bool {
    true
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            index_path: None,
            writer_heap_bytes: DEFAULT_WRITER_HEAP_BYTES,
            auto_rebuild: true,
        }
    }
}

impl SearchConfig {
    pub fn index_path_for_db_path(&self, db_path: &Path) -> PathBuf {
        self.index_path
            .clone()
            .unwrap_or_else(|| default_search_index_path(db_path))
    }
}

pub fn default_search_index_path(db_path: &Path) -> PathBuf {
    let mut value = db_path.as_os_str().to_os_string();
    value.push(".search");
    PathBuf::from(value)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub query: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<SearchEntityKind>,
    /// User-visible content fields to search, for example `title`, `markdown`, or `body`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    /// Structured business fields to search as text, for example `status` or `datatype`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub business_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<SearchFilterGroup>,
    #[serde(default = "default_search_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

fn default_search_limit() -> u32 {
    DEFAULT_SEARCH_LIMIT
}

impl Default for SearchQuery {
    fn default() -> Self {
        Self {
            query: String::new(),
            kinds: Vec::new(),
            fields: Vec::new(),
            business_fields: Vec::new(),
            filter: None,
            limit: DEFAULT_SEARCH_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchFilterLogic {
    And,
    Or,
}

impl Default for SearchFilterLogic {
    fn default() -> Self {
        Self::And
    }
}

impl FromStr for SearchFilterLogic {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "and" => Ok(Self::And),
            "or" => Ok(Self::Or),
            other => Err(format!("Unsupported search filter logic '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchFilterOp {
    Eq,
    #[serde(rename = "nq")]
    Nq,
    Gt,
    Gte,
    Lt,
    Lte,
    Include,
    Exclude,
}

impl FromStr for SearchFilterOp {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "eq" | "=" | "==" => Ok(Self::Eq),
            "nq" | "ne" | "!=" | "<>" => Ok(Self::Nq),
            "gt" | ">" => Ok(Self::Gt),
            "gte" | ">=" => Ok(Self::Gte),
            "lt" | "<" => Ok(Self::Lt),
            "lte" | "<=" => Ok(Self::Lte),
            "include" | "includes" | "contains" => Ok(Self::Include),
            "exclude" | "excludes" | "not_contains" => Ok(Self::Exclude),
            other => Err(format!("Unsupported search filter op '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchFieldFilter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    pub op: SearchFilterOp,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<JsonValue>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchFilterGroup {
    #[serde(default)]
    pub logic: SearchFilterLogic,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<SearchFieldFilter>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<SearchFilterGroup>,
}

impl SearchFilterGroup {
    fn is_empty(&self) -> bool {
        self.filters.is_empty() && self.groups.iter().all(Self::is_empty)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub doc_id: String,
    pub kind: SearchEntityKind,
    pub entity_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub title: String,
    pub field: String,
    pub snippet: String,
    pub score: f32,
    pub created_at_ms: i64,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub metadata: JsonValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchIndexPhase {
    NotBuilt,
    Queued,
    Building,
    Updating,
    Ready,
    Error,
}

impl SearchIndexPhase {
    pub fn is_in_progress(self) -> bool {
        matches!(self, Self::Queued | Self::Building | Self::Updating)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchIndexStatus {
    pub phase: SearchIndexPhase,
    pub in_progress: bool,
    pub index_path: PathBuf,
    pub indexed_doc_count: u64,
    pub generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_done: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_remaining_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_started_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_finished_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
struct SearchIndexState {
    phase: SearchIndexPhase,
    index_path: PathBuf,
    generation: u64,
    progress_done: Option<u64>,
    progress_total: Option<u64>,
    current_stage: Option<String>,
    last_started_at_ms: Option<i64>,
    last_finished_at_ms: Option<i64>,
    last_error: Option<String>,
}

impl SearchIndexState {
    fn new(index_path: PathBuf, meta_exists: bool) -> Self {
        Self {
            phase: if meta_exists {
                SearchIndexPhase::Ready
            } else {
                SearchIndexPhase::NotBuilt
            },
            index_path,
            generation: 0,
            progress_done: None,
            progress_total: None,
            current_stage: None,
            last_started_at_ms: None,
            last_finished_at_ms: None,
            last_error: None,
        }
    }

    fn queued(&mut self) {
        self.phase = SearchIndexPhase::Queued;
        self.last_started_at_ms = Some(now_ms());
        self.progress_done = None;
        self.progress_total = None;
        self.current_stage = Some("Queued".to_string());
        self.last_error = None;
    }

    fn begin(&mut self, phase: SearchIndexPhase) {
        self.phase = phase;
        self.last_started_at_ms = Some(now_ms());
        self.progress_done = Some(0);
        self.progress_total = None;
        self.current_stage = Some(
            match phase {
                SearchIndexPhase::Building => "Starting rebuild",
                SearchIndexPhase::Updating => "Starting update",
                SearchIndexPhase::Queued => "Queued",
                SearchIndexPhase::NotBuilt => "Not built",
                SearchIndexPhase::Ready => "Ready",
                SearchIndexPhase::Error => "Error",
            }
            .to_string(),
        );
        self.last_error = None;
    }

    fn progress(&mut self, done: u64, total: Option<u64>, stage: impl Into<String>) {
        self.progress_done = Some(done);
        self.progress_total = total;
        self.current_stage = Some(stage.into());
    }

    fn finish_ok(&mut self) {
        self.phase = SearchIndexPhase::Ready;
        self.generation = self.generation.saturating_add(1);
        self.progress_done = None;
        self.progress_total = None;
        self.current_stage = None;
        self.last_finished_at_ms = Some(now_ms());
        self.last_error = None;
    }

    fn finish_error(&mut self, error: impl ToString) {
        self.phase = SearchIndexPhase::Error;
        self.progress_done = None;
        self.progress_total = None;
        self.current_stage = None;
        self.last_finished_at_ms = Some(now_ms());
        self.last_error = Some(error.to_string());
    }

    fn to_status(&self, indexed_doc_count: u64) -> SearchIndexStatus {
        let progress_percent = match (self.progress_done, self.progress_total) {
            (Some(done), Some(total)) if total > 0 => {
                Some(((done as f64 / total as f64) * 100.0).clamp(0.0, 100.0))
            }
            _ => None,
        };
        let estimated_remaining_ms = estimate_remaining_ms(
            self.last_started_at_ms,
            self.progress_done,
            self.progress_total,
        );
        SearchIndexStatus {
            phase: self.phase,
            in_progress: self.phase.is_in_progress(),
            index_path: self.index_path.clone(),
            indexed_doc_count,
            generation: self.generation,
            progress_done: self.progress_done,
            progress_total: self.progress_total,
            progress_percent,
            estimated_remaining_ms,
            current_stage: self.current_stage.clone(),
            last_started_at_ms: self.last_started_at_ms,
            last_finished_at_ms: self.last_finished_at_ms,
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchContentField {
    pub field: String,
    pub value: String,
}

impl SearchContentField {
    pub fn new(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchBusinessFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchDocument {
    pub doc_id: String,
    pub kind: SearchEntityKind,
    pub entity_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub title: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_fields: Vec<SearchContentField>,
    #[serde(default)]
    pub business: SearchBusinessFields,
    #[serde(default)]
    pub metadata: JsonValue,
    pub created_at_ms: i64,
}

impl SearchDocument {
    pub fn new(
        kind: SearchEntityKind,
        entity_id: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
        created_at_ms: i64,
    ) -> Self {
        let entity_id = entity_id.into();
        let title = title.into();
        let body = body.into();
        let mut content_fields = Vec::new();
        push_content_field(&mut content_fields, "title", &title);
        push_content_field(&mut content_fields, "body", &body);
        Self {
            doc_id: format!("{}:{entity_id}", kind.as_str()),
            kind,
            entity_id,
            parent_id: None,
            title,
            body,
            content_fields,
            business: SearchBusinessFields {
                created_at_ms: Some(created_at_ms),
                ..Default::default()
            },
            metadata: JsonValue::Null,
            created_at_ms,
        }
    }

    pub fn with_parent_id(mut self, parent_id: impl Into<String>) -> Self {
        self.parent_id = Some(parent_id.into());
        self
    }

    pub fn with_doc_id(mut self, doc_id: impl Into<String>) -> Self {
        self.doc_id = doc_id.into();
        self
    }

    pub fn with_metadata(mut self, metadata: JsonValue) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_content_fields(mut self, content_fields: Vec<SearchContentField>) -> Self {
        self.content_fields = content_fields
            .into_iter()
            .filter(|field| !field.field.trim().is_empty() && !field.value.trim().is_empty())
            .collect();
        if self.body.trim().is_empty() {
            self.body = self
                .content_fields
                .iter()
                .filter(|field| field.field != "title")
                .map(|field| field.value.as_str())
                .collect::<Vec<_>>()
                .join("\n");
        }
        self
    }

    pub fn with_business_fields(mut self, business: SearchBusinessFields) -> Self {
        self.business = business;
        if self.business.created_at_ms.is_none() {
            self.business.created_at_ms = Some(self.created_at_ms);
        }
        if self.business.updated_at_ms.is_none() {
            self.business.updated_at_ms = self.business.created_at_ms;
        }
        self
    }

    pub fn from_collection(collection: &ThingCollectionEntry) -> Self {
        Self::new(
            SearchEntityKind::Collection,
            collection.uuid.clone(),
            collection.title.clone(),
            String::new(),
            collection.created_at.timestamp_millis(),
        )
        .with_content_fields(vec![SearchContentField::new(
            "title",
            collection.title.clone(),
        )])
        .with_business_fields(SearchBusinessFields {
            created_at_ms: Some(collection.created_at.timestamp_millis()),
            updated_at_ms: Some(collection.updated_at.timestamp_millis()),
            app_id: collection.app_id.clone(),
            collection_uuid: Some(collection.uuid.clone()),
            collection_type: Some(collection.collection_type.as_str().to_string()),
            archived: Some(collection.archived_at.is_some()),
            ..Default::default()
        })
        .with_metadata(json!({
            "collection_type": collection.collection_type.as_str(),
            "app_id": collection.app_id,
            "archived_at": collection.archived_at,
            "created_at": collection.created_at,
            "updated_at": collection.updated_at,
            "actor_type": collection.actor_type,
            "actor_app_id": collection.actor_app_id,
            "actor_display_name": collection.actor_display_name,
        }))
    }

    pub fn from_thing(thing: &ThingEntry) -> Self {
        let mut content_fields = Vec::new();
        push_content_field(&mut content_fields, "title", &thing.title);
        push_json_content_fields(&mut content_fields, thing.datatype.as_str(), &thing.data);
        let body = join_content_field_values(&content_fields, false);
        Self::new(
            SearchEntityKind::Thing,
            thing.uuid.clone(),
            thing.title.clone(),
            body,
            thing.created_at.timestamp_millis(),
        )
        .with_content_fields(content_fields)
        .with_parent_id(thing.collection_uuid.clone())
        .with_business_fields(SearchBusinessFields {
            created_at_ms: Some(thing.created_at.timestamp_millis()),
            updated_at_ms: Some(thing.updated_at.timestamp_millis()),
            status: Some(thing.status.clone()),
            datatype: Some(thing.datatype.as_str().to_string()),
            collection_uuid: Some(thing.collection_uuid.clone()),
            archived: Some(thing.archived_at.is_some()),
            ..Default::default()
        })
        .with_metadata(json!({
            "datatype": thing.datatype,
            "collection_uuid": thing.collection_uuid,
            "parent_uuid": thing.parent_uuid,
            "archived_at": thing.archived_at,
            "archived_from_collection_uuid": thing.archived_from_collection_uuid,
            "created_at": thing.created_at,
            "updated_at": thing.updated_at,
            "status": thing.status,
            "status_timestamp_ms": thing.status_timestamp_ms,
            "actor_type": thing.actor_type,
            "actor_app_id": thing.actor_app_id,
            "actor_display_name": thing.actor_display_name,
        }))
    }

    pub fn from_chat_session(session: &ChatSession) -> Self {
        let title = session
            .title
            .clone()
            .unwrap_or_else(|| session.session_id.clone());
        Self::new(
            SearchEntityKind::ChatSession,
            session.session_id.clone(),
            title.clone(),
            String::new(),
            session.created_at.timestamp_millis(),
        )
        .with_content_fields(vec![SearchContentField::new("title", title)])
        .with_business_fields(SearchBusinessFields {
            created_at_ms: Some(session.created_at.timestamp_millis()),
            updated_at_ms: Some(session.last_activity.timestamp_millis()),
            session_id: Some(session.session_id.clone()),
            ..Default::default()
        })
        .with_metadata(json!({
            "session_id": session.session_id,
            "title": session.title,
            "created_at": session.created_at,
            "last_activity": session.last_activity,
            "message_count": session.message_count,
        }))
    }

    pub fn from_chat_message(
        session_id: &str,
        message_id: &str,
        created_at_ms: i64,
        message_json: &str,
    ) -> Self {
        let parsed = serde_json::from_str::<JsonValue>(message_json).unwrap_or(JsonValue::Null);
        let title = extract_chat_message_title(&parsed).unwrap_or_else(|| message_id.to_string());
        let mut content_fields = Vec::new();
        push_content_field(&mut content_fields, "title", &title);
        push_chat_message_content_field(&mut content_fields, &parsed, message_json);
        let body = join_content_field_values(&content_fields, false);
        Self::new(
            SearchEntityKind::ChatMessage,
            message_id.to_string(),
            title,
            body,
            created_at_ms,
        )
        .with_content_fields(content_fields)
        .with_doc_id(format!("chat_message:{session_id}:{message_id}"))
        .with_parent_id(session_id.to_string())
        .with_business_fields(SearchBusinessFields {
            created_at_ms: Some(created_at_ms),
            session_id: Some(session_id.to_string()),
            ..Default::default()
        })
        .with_metadata(json!({
            "session_id": session_id,
            "message_id": message_id,
            "message_json": parsed,
        }))
    }

    pub(crate) fn from_stored_chat_message(message: &StoredChatMessage) -> Self {
        Self::from_chat_message(
            &message.session_id,
            &message.message_id,
            message.created_at_ms,
            &message.message_json,
        )
    }

    pub fn from_notification(notification: &NotificationEntry) -> Self {
        let mut content_fields = Vec::new();
        push_content_field(&mut content_fields, "title", &notification.title);
        push_content_field(&mut content_fields, "notification_body", &notification.body);
        let body = join_content_field_values(&content_fields, false);
        Self::new(
            SearchEntityKind::Notification,
            notification.id.to_string(),
            notification.title.clone(),
            body,
            notification.created_at.timestamp_millis(),
        )
        .with_content_fields(content_fields)
        .with_parent_id(notification.category.clone())
        .with_business_fields(SearchBusinessFields {
            created_at_ms: Some(notification.created_at.timestamp_millis()),
            category: Some(notification.category.clone()),
            source: Some(notification.source.as_str().to_string()),
            ..Default::default()
        })
        .with_metadata(json!({
            "id": notification.id,
            "source": notification.source,
            "category": notification.category,
            "is_read": notification.is_read,
            "response_action": notification.response_action,
            "responded_at": notification.responded_at,
            "created_at": notification.created_at,
        }))
    }

    pub fn from_action(action: &ActionDefinition) -> Self {
        let mut content_fields = Vec::new();
        push_content_field(&mut content_fields, "title", &action.title);
        push_content_field(&mut content_fields, "action_name", &action.name);
        push_content_field(&mut content_fields, "description", &action.description);
        push_content_field(&mut content_fields, "script_source", &action.script_source);
        let body = join_content_field_values(&content_fields, false);
        Self::new(
            SearchEntityKind::Action,
            action.action_uuid.clone(),
            action.title.clone(),
            body,
            0,
        )
        .with_content_fields(content_fields)
        .with_business_fields(SearchBusinessFields {
            category: Some(action.category.clone()),
            source: Some("action".to_string()),
            ..Default::default()
        })
        .with_metadata(json!({
            "action_uuid": action.action_uuid,
            "name": action.name,
            "version": action.version,
            "category": action.category,
            "enabled": action.enabled,
            "metadata_json": action.metadata_json,
            "input_schema_json": action.input_schema_json,
            "output_schema_json": action.output_schema_json,
        }))
    }

    pub fn from_agent_version(version: &AgentVersion) -> Self {
        let mut content_fields = Vec::new();
        push_content_field(&mut content_fields, "title", &version.name);
        push_content_field(&mut content_fields, "markdown", &version.raw_markdown);
        let body = join_content_field_values(&content_fields, false);
        Self::new(
            SearchEntityKind::AgentVersion,
            version.version_id.clone(),
            version.name.clone(),
            body,
            version.created_at.timestamp_millis(),
        )
        .with_content_fields(content_fields)
        .with_parent_id(version.agent_id.clone())
        .with_business_fields(SearchBusinessFields {
            created_at_ms: Some(version.created_at.timestamp_millis()),
            updated_at_ms: Some(version.updated_at.timestamp_millis()),
            agent_id: Some(version.agent_id.clone()),
            ..Default::default()
        })
        .with_metadata(json!({
            "version_id": version.version_id,
            "agent_id": version.agent_id,
            "name": version.name,
            "created_at": version.created_at,
            "updated_at": version.updated_at,
            "applied_at": version.applied_at,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SearchIngestAction {
    Upsert {
        document: SearchDocument,
    },
    Delete {
        doc_id: String,
    },
    DeleteByParent {
        kind: SearchEntityKind,
        parent_id: String,
    },
    Clear,
}

impl SearchIngestAction {
    pub fn upsert(document: SearchDocument) -> Self {
        Self::Upsert { document }
    }

    pub fn delete_doc_id(doc_id: impl Into<String>) -> Self {
        Self::Delete {
            doc_id: doc_id.into(),
        }
    }

    pub fn delete_entity(kind: SearchEntityKind, entity_id: impl AsRef<str>) -> Self {
        Self::Delete {
            doc_id: format!("{}:{}", kind.as_str(), entity_id.as_ref()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchChange {
    pub source: String,
    pub kind: SearchEntityKind,
    pub entity_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub metadata: JsonValue,
}

pub struct SearchIngestContext<'a> {
    storage: &'a Storage,
}

impl<'a> SearchIngestContext<'a> {
    pub(crate) fn new(storage: &'a Storage) -> Self {
        Self { storage }
    }

    pub fn db_path(&self) -> &Path {
        self.storage.db_path()
    }
}

pub trait SearchIngestProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    fn rebuild_documents(
        &self,
        context: &SearchIngestContext<'_>,
    ) -> Result<Vec<SearchIngestAction>>;

    fn documents_for_change(
        &self,
        _context: &SearchIngestContext<'_>,
        _change: &SearchChange,
    ) -> Result<Vec<SearchIngestAction>> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
pub struct SearchService {
    inner: Arc<SearchServiceInner>,
}

struct SearchServiceInner {
    index: Index,
    reader: IndexReader,
    fields: SearchSchemaFields,
    sender: mpsc::Sender<SearchWorkerCommand>,
    providers: Arc<RwLock<Vec<Arc<dyn SearchIngestProvider>>>>,
    state: Arc<RwLock<SearchIndexState>>,
}

impl SearchService {
    pub(crate) fn open(storage: Storage, config: SearchConfig) -> Result<Self> {
        let index_path = config.index_path_for_db_path(storage.db_path());
        let (schema, fields) = build_search_schema();
        let (index, index_was_ready) = open_or_create_index(&index_path, schema)?;
        register_search_tokenizers(&index);
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .context("Failed to create Tantivy search reader")?;
        let writer_heap_bytes = config.writer_heap_bytes.max(15_000_000);
        let (sender, receiver) = mpsc::channel();
        let providers: Arc<RwLock<Vec<Arc<dyn SearchIngestProvider>>>> =
            Arc::new(RwLock::new(vec![
                Arc::new(CoreSearchProvider) as Arc<dyn SearchIngestProvider>
            ]));
        let state = Arc::new(RwLock::new(SearchIndexState::new(
            index_path.clone(),
            index_was_ready,
        )));
        let worker = SearchWorker {
            storage,
            index: index.clone(),
            writer_heap_bytes,
            fields: fields.clone(),
            providers: providers.clone(),
            state: state.clone(),
        };
        thread::Builder::new()
            .name("remi-search-indexer".to_string())
            .spawn(move || worker.run(receiver))
            .context("Failed to spawn search index worker")?;
        let service = Self {
            inner: Arc::new(SearchServiceInner {
                index,
                reader,
                fields,
                sender,
                providers,
                state,
            }),
        };
        if config.auto_rebuild && !index_was_ready {
            service.enqueue_rebuild()?;
        }
        Ok(service)
    }

    pub fn register_provider(&self, provider: Arc<dyn SearchIngestProvider>) {
        match self.inner.providers.write() {
            Ok(mut providers) => providers.push(provider),
            Err(error) => warn!(error = %error, "Failed to register search ingest provider"),
        }
    }

    pub fn status(&self) -> SearchIndexStatus {
        let indexed_doc_count = self
            .inner
            .reader
            .reload()
            .ok()
            .map(|_| self.inner.reader.searcher().num_docs())
            .unwrap_or_default();
        match self.inner.state.read() {
            Ok(state) => state.to_status(indexed_doc_count),
            Err(error) => SearchIndexStatus {
                phase: SearchIndexPhase::Error,
                in_progress: false,
                index_path: PathBuf::new(),
                indexed_doc_count,
                generation: 0,
                progress_done: None,
                progress_total: None,
                progress_percent: None,
                estimated_remaining_ms: None,
                current_stage: None,
                last_started_at_ms: None,
                last_finished_at_ms: Some(now_ms()),
                last_error: Some(format!("Search index status lock is poisoned: {error}")),
            },
        }
    }

    pub fn search(&self, query: SearchQuery) -> Result<Vec<SearchResult>> {
        let text = query.query.trim().to_string();
        let has_filter = query
            .filter
            .as_ref()
            .map(|filter| !filter.is_empty())
            .unwrap_or(false);
        if text.is_empty() && !has_filter {
            return Ok(Vec::new());
        }

        self.wait_for_pending_index_work()?;
        self.inner
            .reader
            .reload()
            .context("Failed to reload Tantivy search reader")?;
        let searcher = self.inner.reader.searcher();
        let parser =
            QueryParser::for_index(&self.inner.index, self.parser_fields_for_query(&query));
        let limit = query.limit.clamp(1, MAX_SEARCH_LIMIT) as usize;
        let offset = query.offset as usize;
        let fetch_limit = (limit + offset)
            .saturating_mul(12)
            .clamp(limit + offset, 5_000);
        let kinds: HashSet<SearchEntityKind> = query.kinds.iter().copied().collect();
        let content_fields = normalized_filter_strings(&query.fields);

        let mut results = if text.is_empty() {
            let all_query = AllQuery;
            self.search_with_query(
                &searcher,
                &all_query,
                &text,
                &query.business_fields,
                &kinds,
                &content_fields,
                query.filter.as_ref(),
                limit,
                offset,
                fetch_limit,
            )?
        } else {
            let (parsed_query, parse_errors) = parser.parse_query_lenient(&text);
            for error in parse_errors {
                warn!(query = %text, error = %error, "Lenient search query parse error");
            }
            self.search_with_query(
                &searcher,
                parsed_query.as_ref(),
                &text,
                &query.business_fields,
                &kinds,
                &content_fields,
                query.filter.as_ref(),
                limit,
                offset,
                fetch_limit,
            )?
        };
        if results.is_empty() && !text.is_empty() {
            if let Some(fuzzy_text) = fuzzy_fallback_query_text(&text) {
                let mut fuzzy_parser =
                    QueryParser::for_index(&self.inner.index, vec![self.inner.fields.content]);
                fuzzy_parser.set_field_fuzzy(self.inner.fields.content, true, 1, true);
                let (parsed_query, parse_errors) = fuzzy_parser.parse_query_lenient(&fuzzy_text);
                for error in parse_errors {
                    warn!(query = %fuzzy_text, error = %error, "Lenient fuzzy search query parse error");
                }
                results = self.search_with_query(
                    &searcher,
                    parsed_query.as_ref(),
                    &text,
                    &query.business_fields,
                    &kinds,
                    &content_fields,
                    query.filter.as_ref(),
                    limit,
                    offset,
                    fetch_limit,
                )?;
            }
        }

        Ok(results)
    }

    fn wait_for_pending_index_work(&self) -> Result<()> {
        let in_progress = self
            .inner
            .state
            .read()
            .map(|state| state.phase.is_in_progress())
            .unwrap_or(false);
        if in_progress {
            self.flush()?;
        }
        Ok(())
    }

    fn parser_fields_for_query(&self, query: &SearchQuery) -> Vec<Field> {
        let mut fields = Vec::new();
        if query.business_fields.is_empty() || !query.fields.is_empty() {
            fields.push(self.inner.fields.content);
            if query_contains_numeric_fragment(&query.query) {
                fields.push(self.inner.fields.numeric_fragments);
            }
        }
        for field in &query.business_fields {
            if let Some(field) = business_search_field(&self.inner.fields, field) {
                fields.push(field);
            }
        }
        if fields.is_empty() {
            fields.push(self.inner.fields.content);
        }
        fields
    }

    #[allow(clippy::too_many_arguments)]
    fn search_with_query(
        &self,
        searcher: &tantivy::Searcher,
        parsed_query: &dyn tantivy::query::Query,
        snippet_query: &str,
        business_fields: &[String],
        kinds: &HashSet<SearchEntityKind>,
        content_fields: &HashSet<String>,
        filter: Option<&SearchFilterGroup>,
        limit: usize,
        offset: usize,
        fetch_limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let top_docs = searcher
            .search(
                parsed_query,
                &TopDocs::with_limit(fetch_limit).order_by_score(),
            )
            .context("Failed to execute Tantivy search")?;
        let mut results_by_doc_id: HashMap<String, SearchResult> = HashMap::new();
        for (score, address) in top_docs {
            let doc = searcher
                .doc::<TantivyDocument>(address)
                .context("Failed to read Tantivy search document")?;
            if let Some(filter) = filter {
                if !document_matches_filter_group(&doc, &self.inner.fields, filter) {
                    continue;
                }
            }
            let Some(result) = document_to_result(
                &doc,
                &self.inner.fields,
                score,
                snippet_query,
                business_fields,
            ) else {
                continue;
            };
            if !kinds.is_empty() && !kinds.contains(&result.kind) {
                continue;
            }
            if !content_fields.is_empty()
                && !content_fields.contains(&normalize_search_field_name(&result.field))
                && business_search_field(&self.inner.fields, &result.field).is_none()
            {
                continue;
            }
            match results_by_doc_id.get(&result.doc_id) {
                Some(existing) if existing.score >= result.score => {}
                _ => {
                    results_by_doc_id.insert(result.doc_id.clone(), result);
                }
            }
        }

        let mut results = results_by_doc_id.into_values().collect::<Vec<_>>();
        results.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| b.created_at_ms.cmp(&a.created_at_ms))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }

    pub fn enqueue_document(&self, document: SearchDocument) -> Result<()> {
        self.enqueue_actions(vec![SearchIngestAction::upsert(document)])
    }

    pub fn enqueue_actions(&self, actions: Vec<SearchIngestAction>) -> Result<()> {
        if actions.is_empty() {
            return Ok(());
        }
        self.mark_queued();
        self.inner
            .sender
            .send(SearchWorkerCommand::Apply(actions))
            .map_err(|error| self.worker_unavailable_error(error.to_string()))
    }

    pub fn enqueue_change(&self, change: SearchChange) -> Result<()> {
        self.mark_queued();
        self.inner
            .sender
            .send(SearchWorkerCommand::Change(change))
            .map_err(|error| self.worker_unavailable_error(error.to_string()))
    }

    pub fn enqueue_rebuild(&self) -> Result<()> {
        self.mark_queued();
        self.inner
            .sender
            .send(SearchWorkerCommand::Rebuild { response: None })
            .map_err(|error| self.worker_unavailable_error(error.to_string()))
    }

    pub fn rebuild(&self) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.mark_queued();
        self.inner
            .sender
            .send(SearchWorkerCommand::Rebuild { response: Some(tx) })
            .map_err(|error| self.worker_unavailable_error(error.to_string()))?;
        rx.recv().context("Search worker stopped during rebuild")?
    }

    pub fn flush(&self) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.mark_queued();
        self.inner
            .sender
            .send(SearchWorkerCommand::Flush(tx))
            .map_err(|error| self.worker_unavailable_error(error.to_string()))?;
        rx.recv().context("Search worker stopped during flush")?
    }

    fn mark_queued(&self) {
        if let Ok(mut state) = self.inner.state.write() {
            state.queued();
        }
    }

    fn worker_unavailable_error(&self, error: String) -> anyhow::Error {
        if let Ok(mut state) = self.inner.state.write() {
            state.finish_error(format!("Search worker is unavailable: {error}"));
        }
        anyhow!("Search worker is unavailable: {error}")
    }
}

struct CoreSearchProvider;

impl SearchIngestProvider for CoreSearchProvider {
    fn provider_id(&self) -> &'static str {
        "remi-core"
    }

    fn rebuild_documents(
        &self,
        context: &SearchIngestContext<'_>,
    ) -> Result<Vec<SearchIngestAction>> {
        let storage = context.storage;
        let mut actions = Vec::new();

        match ThingsLocalService::new(storage).snapshot_with_options(
            SEARCH_REBUILD_DEVICE_ID,
            SnapshotOptions {
                include_content: true,
            },
        ) {
            Ok(snapshot) => {
                actions.extend(snapshot.collections.iter().map(|collection| {
                    SearchIngestAction::upsert(SearchDocument::from_collection(collection))
                }));
                actions.extend(
                    snapshot
                        .things
                        .iter()
                        .map(|thing| SearchIngestAction::upsert(SearchDocument::from_thing(thing))),
                );
            }
            Err(error) => warn!(error = %error, "Failed to rebuild Things search documents"),
        }

        actions.extend(
            storage.list_chat_sessions(None)?.iter().map(|session| {
                SearchIngestAction::upsert(SearchDocument::from_chat_session(session))
            }),
        );
        actions.extend(storage.list_all_chat_messages()?.iter().map(|message| {
            SearchIngestAction::upsert(SearchDocument::from_stored_chat_message(message))
        }));
        actions.extend(
            storage
                .list_notifications_flat(u32::MAX, 0)?
                .iter()
                .map(|notification| {
                    SearchIngestAction::upsert(SearchDocument::from_notification(notification))
                }),
        );
        actions.extend(
            storage
                .list_actions()?
                .iter()
                .map(|action| SearchIngestAction::upsert(SearchDocument::from_action(action))),
        );
        actions.extend(storage.list_all_agent_versions()?.iter().map(|version| {
            SearchIngestAction::upsert(SearchDocument::from_agent_version(version))
        }));

        Ok(actions)
    }
}

enum SearchWorkerCommand {
    Apply(Vec<SearchIngestAction>),
    Change(SearchChange),
    Rebuild {
        response: Option<mpsc::SyncSender<Result<()>>>,
    },
    Flush(mpsc::SyncSender<Result<()>>),
}

struct SearchWorker {
    storage: Storage,
    index: Index,
    writer_heap_bytes: usize,
    fields: SearchSchemaFields,
    providers: Arc<RwLock<Vec<Arc<dyn SearchIngestProvider>>>>,
    state: Arc<RwLock<SearchIndexState>>,
}

impl SearchWorker {
    fn run(mut self, receiver: mpsc::Receiver<SearchWorkerCommand>) {
        while let Ok(command) = receiver.recv() {
            match command {
                SearchWorkerCommand::Apply(actions) => {
                    let result = self.run_tracked(SearchIndexPhase::Updating, |worker| {
                        worker.apply_and_commit(&actions)
                    });
                    if let Err(error) = result {
                        warn!(error = %error, "Failed to apply search index actions");
                    }
                }
                SearchWorkerCommand::Change(change) => {
                    let result = self.run_tracked(SearchIndexPhase::Updating, |worker| {
                        worker.apply_change(change)
                    });
                    if let Err(error) = result {
                        warn!(error = %error, "Failed to process search index change");
                    }
                }
                SearchWorkerCommand::Rebuild { response } => {
                    let result =
                        self.run_tracked(SearchIndexPhase::Building, |worker| worker.rebuild());
                    if let Some(response) = response {
                        let _ = response.send(result);
                    } else if let Err(error) = result {
                        warn!(error = %error, "Failed to rebuild search index");
                    }
                }
                SearchWorkerCommand::Flush(response) => {
                    let result = self.run_tracked(SearchIndexPhase::Updating, |_worker| Ok(()));
                    let _ = response.send(result);
                }
            }
        }
    }

    fn run_tracked<F>(&mut self, phase: SearchIndexPhase, f: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        self.begin_status(phase);
        let result = f(self);
        match &result {
            Ok(()) => self.finish_status_ok(),
            Err(error) => self.finish_status_error(error),
        }
        result
    }

    fn begin_status(&self, phase: SearchIndexPhase) {
        if let Ok(mut state) = self.state.write() {
            state.begin(phase);
        }
    }

    fn finish_status_ok(&self) {
        if let Ok(mut state) = self.state.write() {
            state.finish_ok();
        }
    }

    fn finish_status_error(&self, error: &anyhow::Error) {
        if let Ok(mut state) = self.state.write() {
            state.finish_error(error);
        }
    }

    fn update_progress(&self, done: u64, total: Option<u64>, stage: impl Into<String>) {
        if let Ok(mut state) = self.state.write() {
            state.progress(done, total, stage);
        }
    }

    fn apply_change(&mut self, change: SearchChange) -> Result<()> {
        let context = SearchIngestContext::new(&self.storage);
        let providers = self.providers_snapshot()?;
        let mut actions = Vec::new();
        for provider in providers {
            actions.extend(
                provider
                    .documents_for_change(&context, &change)
                    .with_context(|| {
                        format!(
                            "Search ingest provider '{}' failed to process a change",
                            provider.provider_id()
                        )
                    })?,
            );
        }
        self.apply_and_commit(&actions)
    }

    fn rebuild(&mut self) -> Result<()> {
        self.update_progress(0, None, "Collecting documents");
        let action_batches = {
            let context = SearchIngestContext::new(&self.storage);
            let providers = self.providers_snapshot()?;
            let mut batches = Vec::with_capacity(providers.len());
            let provider_count = providers.len();
            for (index, provider) in providers.into_iter().enumerate() {
                self.update_progress(
                    index as u64,
                    Some(provider_count as u64),
                    format!("Collecting {}", provider.provider_id()),
                );
                let actions = provider.rebuild_documents(&context).with_context(|| {
                    format!(
                        "Search ingest provider '{}' failed to rebuild documents",
                        provider.provider_id()
                    )
                })?;
                batches.push(actions);
            }
            batches
        };
        let total_actions = action_batches.iter().map(Vec::len).sum::<usize>() as u64;

        let mut writer = self.open_writer()?;
        self.update_progress(0, Some(total_actions), "Clearing index");
        writer
            .delete_all_documents()
            .context("Failed to clear search index")?;
        let mut processed = 0;
        for actions in action_batches {
            self.apply_actions(
                &mut writer,
                &actions,
                Some(ProgressScope {
                    processed: &mut processed,
                    total: total_actions,
                    stage: "Indexing documents",
                }),
            )?;
        }
        self.update_progress(total_actions, Some(total_actions), "Committing index");
        writer
            .commit()
            .context("Failed to commit rebuilt search index")?;
        Ok(())
    }

    fn providers_snapshot(&self) -> Result<Vec<Arc<dyn SearchIngestProvider>>> {
        self.providers
            .read()
            .map(|providers| providers.clone())
            .map_err(|error| anyhow!("Search ingest provider registry is poisoned: {error}"))
    }

    fn open_writer(&self) -> Result<IndexWriter> {
        self.index
            .writer(self.writer_heap_bytes)
            .context("Failed to create Tantivy search writer")
    }

    fn apply_and_commit(&mut self, actions: &[SearchIngestAction]) -> Result<()> {
        if actions.is_empty() {
            return Ok(());
        }
        let mut writer = self.open_writer()?;
        let mut processed = 0;
        self.apply_actions(
            &mut writer,
            actions,
            Some(ProgressScope {
                processed: &mut processed,
                total: actions.len() as u64,
                stage: "Applying updates",
            }),
        )?;
        self.update_progress(
            actions.len() as u64,
            Some(actions.len() as u64),
            "Committing index",
        );
        writer
            .commit()
            .context("Failed to commit search index actions")?;
        Ok(())
    }

    fn apply_actions(
        &self,
        writer: &mut IndexWriter,
        actions: &[SearchIngestAction],
        mut progress: Option<ProgressScope<'_>>,
    ) -> Result<()> {
        for action in actions {
            match action {
                SearchIngestAction::Upsert { document } => {
                    writer.delete_term(Term::from_field_text(self.fields.doc_id, &document.doc_id));
                    for tantivy_doc in document_to_tantivy_documents(document, &self.fields) {
                        writer
                            .add_document(tantivy_doc)
                            .context("Failed to add search document")?;
                    }
                }
                SearchIngestAction::Delete { doc_id } => {
                    writer.delete_term(Term::from_field_text(self.fields.doc_id, doc_id));
                }
                SearchIngestAction::DeleteByParent { kind, parent_id } => {
                    let key = parent_key(*kind, parent_id);
                    writer.delete_term(Term::from_field_text(self.fields.parent_key, &key));
                }
                SearchIngestAction::Clear => {
                    writer
                        .delete_all_documents()
                        .context("Failed to clear search index")?;
                }
            }
            if let Some(progress) = progress.as_mut() {
                *progress.processed = progress.processed.saturating_add(1);
                if should_update_progress(*progress.processed, progress.total) {
                    self.update_progress(*progress.processed, Some(progress.total), progress.stage);
                }
            }
        }
        Ok(())
    }
}

struct ProgressScope<'a> {
    processed: &'a mut u64,
    total: u64,
    stage: &'static str,
}

fn should_update_progress(done: u64, total: u64) -> bool {
    done == total || done <= 10 || done % 100 == 0
}

#[derive(Clone)]
struct SearchSchemaFields {
    index_version: Field,
    doc_id: Field,
    kind: Field,
    entity_id: Field,
    parent_id: Field,
    parent_key: Field,
    title: Field,
    body: Field,
    metadata: Field,
    content_field: Field,
    content: Field,
    numeric_fragments: Field,
    created_at_ms: Field,
    updated_at_ms: Field,
    status: Field,
    datatype: Field,
    category: Field,
    source: Field,
    app_id: Field,
    agent_id: Field,
    session_id: Field,
    collection_uuid: Field,
    collection_type: Field,
    archived: Field,
}

fn build_search_schema() -> (Schema, SearchSchemaFields) {
    let mut builder = Schema::builder();
    let search_text = search_text_options();
    let index_version = builder.add_text_field("index_version", STRING | STORED);
    let doc_id = builder.add_text_field("doc_id", STRING | STORED);
    let kind = builder.add_text_field("kind", STRING | STORED);
    let entity_id = builder.add_text_field("entity_id", STRING | STORED);
    let parent_id = builder.add_text_field("parent_id", STRING | STORED);
    let parent_key = builder.add_text_field("parent_key", STRING | STORED);
    let title = builder.add_text_field("title", search_text.clone());
    let body = builder.add_text_field("body", search_text.clone());
    let metadata = builder.add_text_field("metadata", STRING | STORED);
    let content_field = builder.add_text_field("content_field", STRING | STORED);
    let content = builder.add_text_field("content", search_text);
    let numeric_fragments = builder.add_text_field("numeric_fragments", STRING);
    let created_at_ms = builder.add_i64_field("created_at_ms", INDEXED | STORED | FAST);
    let updated_at_ms = builder.add_i64_field("updated_at_ms", INDEXED | STORED | FAST);
    let status = builder.add_text_field("status", STRING | STORED);
    let datatype = builder.add_text_field("datatype", STRING | STORED);
    let category = builder.add_text_field("category", STRING | STORED);
    let source = builder.add_text_field("source", STRING | STORED);
    let app_id = builder.add_text_field("app_id", STRING | STORED);
    let agent_id = builder.add_text_field("agent_id", STRING | STORED);
    let session_id = builder.add_text_field("session_id", STRING | STORED);
    let collection_uuid = builder.add_text_field("collection_uuid", STRING | STORED);
    let collection_type = builder.add_text_field("collection_type", STRING | STORED);
    let archived = builder.add_text_field("archived", STRING | STORED);
    let schema = builder.build();
    (
        schema,
        SearchSchemaFields {
            index_version,
            doc_id,
            kind,
            entity_id,
            parent_id,
            parent_key,
            title,
            body,
            metadata,
            content_field,
            content,
            numeric_fragments,
            created_at_ms,
            updated_at_ms,
            status,
            datatype,
            category,
            source,
            app_id,
            agent_id,
            session_id,
            collection_uuid,
            collection_type,
            archived,
        },
    )
}

fn search_text_options() -> TextOptions {
    TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(SEARCH_TEXT_TOKENIZER)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
        .set_stored()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn estimate_remaining_ms(
    started_at_ms: Option<i64>,
    done: Option<u64>,
    total: Option<u64>,
) -> Option<i64> {
    let started_at_ms = started_at_ms?;
    let done = done?;
    let total = total?;
    if done == 0 || total == 0 || done >= total {
        return Some(0);
    }
    let elapsed_ms = now_ms().saturating_sub(started_at_ms);
    if elapsed_ms <= 0 {
        return None;
    }
    let remaining = total.saturating_sub(done);
    Some(((elapsed_ms as f64 / done as f64) * remaining as f64).round() as i64)
}

fn open_or_create_index(index_path: &Path, schema: Schema) -> Result<(Index, bool)> {
    fs::create_dir_all(index_path).with_context(|| {
        format!(
            "Failed to create search index directory {}",
            index_path.display()
        )
    })?;
    let meta_path = index_path.join("meta.json");
    if meta_path.exists() {
        let index = Index::open_in_dir(index_path).with_context(|| {
            format!(
                "Failed to open Tantivy search index {}",
                index_path.display()
            )
        })?;
        if search_schema_has_required_fields(&index.schema()) {
            Ok((index, true))
        } else {
            drop(index);
            fs::remove_dir_all(index_path).with_context(|| {
                format!(
                    "Failed to clear outdated Tantivy search index {}",
                    index_path.display()
                )
            })?;
            fs::create_dir_all(index_path).with_context(|| {
                format!(
                    "Failed to recreate search index directory {}",
                    index_path.display()
                )
            })?;
            Index::create_in_dir(index_path, schema)
                .map(|index| (index, false))
                .with_context(|| {
                    format!(
                        "Failed to recreate Tantivy search index {}",
                        index_path.display()
                    )
                })
        }
    } else {
        Index::create_in_dir(index_path, schema)
            .map(|index| (index, false))
            .with_context(|| {
                format!(
                    "Failed to create Tantivy search index {}",
                    index_path.display()
                )
            })
    }
}

fn search_schema_has_required_fields(schema: &Schema) -> bool {
    let has_required_fields = [
        "index_version",
        "doc_id",
        "kind",
        "entity_id",
        "parent_key",
        "title",
        "body",
        "metadata",
        "content_field",
        "content",
        "numeric_fragments",
        "created_at_ms",
        "updated_at_ms",
        "status",
        "datatype",
        "category",
        "source",
        "app_id",
        "agent_id",
        "session_id",
        "collection_uuid",
        "collection_type",
        "archived",
    ]
    .iter()
    .all(|field| schema.get_field(field).is_ok());
    has_required_fields
        && ["title", "body", "content"]
            .iter()
            .all(|field| schema_field_uses_tokenizer(schema, field, SEARCH_TEXT_TOKENIZER))
}

fn schema_field_uses_tokenizer(schema: &Schema, field_name: &str, tokenizer: &str) -> bool {
    let Ok(field) = schema.get_field(field_name) else {
        return false;
    };
    match schema.get_field_entry(field).field_type() {
        FieldType::Str(options) => options
            .get_indexing_options()
            .map(|indexing| indexing.tokenizer() == tokenizer)
            .unwrap_or(false),
        _ => false,
    }
}

fn document_to_tantivy_documents(
    document: &SearchDocument,
    fields: &SearchSchemaFields,
) -> Vec<TantivyDocument> {
    let content_fields = document_content_fields(document);
    content_fields
        .iter()
        .map(|content_field| document_to_tantivy_segment(document, content_field, fields))
        .collect()
}

fn document_to_tantivy_segment(
    document: &SearchDocument,
    content_field: &SearchContentField,
    fields: &SearchSchemaFields,
) -> TantivyDocument {
    let mut tantivy_doc = TantivyDocument::default();
    tantivy_doc.add_text(fields.index_version, SEARCH_INDEX_VERSION);
    tantivy_doc.add_text(fields.doc_id, &document.doc_id);
    tantivy_doc.add_text(fields.kind, document.kind.as_str());
    tantivy_doc.add_text(fields.entity_id, &document.entity_id);
    tantivy_doc.add_text(
        fields.parent_id,
        document.parent_id.as_deref().unwrap_or_default(),
    );
    tantivy_doc.add_text(
        fields.parent_key,
        document
            .parent_id
            .as_ref()
            .map(|parent_id| parent_key(document.kind, parent_id))
            .unwrap_or_default(),
    );
    tantivy_doc.add_text(fields.title, &document.title);
    tantivy_doc.add_text(fields.body, &document.body);
    tantivy_doc.add_text(
        fields.metadata,
        &serde_json::to_string(&document.metadata).unwrap_or_default(),
    );
    tantivy_doc.add_text(fields.content_field, &content_field.field);
    tantivy_doc.add_text(fields.content, &content_field.value);
    for fragment in numeric_search_fragments([
        document.title.as_str(),
        document.body.as_str(),
        content_field.value.as_str(),
    ]) {
        tantivy_doc.add_text(fields.numeric_fragments, &fragment);
    }
    let created_at_ms = document
        .business
        .created_at_ms
        .unwrap_or(document.created_at_ms);
    let updated_at_ms = document.business.updated_at_ms.unwrap_or(created_at_ms);
    tantivy_doc.add_i64(fields.created_at_ms, created_at_ms);
    tantivy_doc.add_i64(fields.updated_at_ms, updated_at_ms);
    add_optional_text(
        &mut tantivy_doc,
        fields.status,
        document.business.status.as_deref(),
    );
    add_optional_text(
        &mut tantivy_doc,
        fields.datatype,
        document.business.datatype.as_deref(),
    );
    add_optional_text(
        &mut tantivy_doc,
        fields.category,
        document.business.category.as_deref(),
    );
    add_optional_text(
        &mut tantivy_doc,
        fields.source,
        document.business.source.as_deref(),
    );
    add_optional_text(
        &mut tantivy_doc,
        fields.app_id,
        document.business.app_id.as_deref(),
    );
    add_optional_text(
        &mut tantivy_doc,
        fields.agent_id,
        document.business.agent_id.as_deref(),
    );
    add_optional_text(
        &mut tantivy_doc,
        fields.session_id,
        document.business.session_id.as_deref(),
    );
    add_optional_text(
        &mut tantivy_doc,
        fields.collection_uuid,
        document.business.collection_uuid.as_deref(),
    );
    add_optional_text(
        &mut tantivy_doc,
        fields.collection_type,
        document.business.collection_type.as_deref(),
    );
    tantivy_doc.add_text(
        fields.archived,
        if document.business.archived.unwrap_or(false) {
            "true"
        } else {
            "false"
        },
    );
    tantivy_doc
}

fn document_to_result(
    doc: &TantivyDocument,
    fields: &SearchSchemaFields,
    score: f32,
    query: &str,
    business_fields: &[String],
) -> Option<SearchResult> {
    let kind = SearchEntityKind::from_str(&field_text(doc, fields.kind)).ok()?;
    let doc_id = field_text(doc, fields.doc_id);
    let entity_id = field_text(doc, fields.entity_id);
    let parent_id = empty_to_none(field_text(doc, fields.parent_id));
    let title = field_text(doc, fields.title);
    let body = field_text(doc, fields.content);
    let field = infer_result_field(doc, fields, query, business_fields);
    let metadata =
        serde_json::from_str(&field_text(doc, fields.metadata)).unwrap_or(JsonValue::Null);
    let created_at_ms = doc
        .get_first(fields.created_at_ms)
        .and_then(|value| value.as_i64())
        .unwrap_or_default();
    Some(SearchResult {
        doc_id,
        kind,
        entity_id,
        parent_id,
        title: title.clone(),
        field: field.clone(),
        snippet: make_result_snippet(doc, fields, query, &title, &body, &field),
        score,
        created_at_ms,
        metadata,
    })
}

fn field_text(doc: &TantivyDocument, field: Field) -> String {
    doc.get_first(field)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string()
}

fn field_i64(doc: &TantivyDocument, field: Field) -> Option<i64> {
    doc.get_first(field).and_then(|value| value.as_i64())
}

fn add_optional_text(doc: &mut TantivyDocument, field: Field, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        doc.add_text(field, value);
    }
}

fn document_content_fields(document: &SearchDocument) -> Vec<SearchContentField> {
    let mut fields = document.content_fields.clone();
    if fields.is_empty() {
        push_content_field(&mut fields, "title", &document.title);
        push_content_field(&mut fields, "body", &document.body);
    }
    if fields.is_empty() {
        fields.push(SearchContentField::new("content", ""));
    }
    fields
}

fn normalized_filter_strings(values: &[String]) -> HashSet<String> {
    values
        .iter()
        .map(|value| normalize_search_field_name(value))
        .filter(|value| !value.is_empty())
        .collect()
}

fn normalize_search_field_name(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_separator = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            previous_was_separator = false;
        } else if !previous_was_separator {
            normalized.push('_');
            previous_was_separator = true;
        }
    }
    normalized.trim_matches('_').to_string()
}

fn business_search_field(fields: &SearchSchemaFields, name: &str) -> Option<Field> {
    match normalize_search_field_name(name).as_str() {
        "status" => Some(fields.status),
        "datatype" | "data_type" => Some(fields.datatype),
        "category" => Some(fields.category),
        "source" => Some(fields.source),
        "app_id" => Some(fields.app_id),
        "agent_id" => Some(fields.agent_id),
        "session_id" => Some(fields.session_id),
        "collection_uuid" | "collection_id" => Some(fields.collection_uuid),
        "collection_type" => Some(fields.collection_type),
        "archived" | "deleted" => Some(fields.archived),
        _ => None,
    }
}

fn infer_result_field(
    doc: &TantivyDocument,
    fields: &SearchSchemaFields,
    query: &str,
    business_fields: &[String],
) -> String {
    let terms = normalized_query_terms(query, 1);
    for business_field in business_fields {
        let Some(field) = business_search_field(fields, business_field) else {
            continue;
        };
        let value = field_text(doc, field).to_lowercase();
        if !value.is_empty() && terms.iter().any(|term| value.contains(term)) {
            return normalize_search_field_name(business_field);
        }
    }
    let field = field_text(doc, fields.content_field).trim().to_string();
    if field.is_empty() {
        "content".to_string()
    } else {
        field
    }
}

fn make_result_snippet(
    doc: &TantivyDocument,
    fields: &SearchSchemaFields,
    query: &str,
    title: &str,
    body: &str,
    result_field: &str,
) -> String {
    if business_search_field(fields, result_field).is_some() {
        if let Some(field) = business_search_field(fields, result_field) {
            let value = field_text(doc, field);
            if !value.is_empty() {
                return format!("{result_field}: {value}");
            }
        }
    }
    make_snippet(query, title, body)
}

fn document_matches_filter_group(
    doc: &TantivyDocument,
    fields: &SearchSchemaFields,
    group: &SearchFilterGroup,
) -> bool {
    let mut results = group
        .filters
        .iter()
        .map(|filter| document_matches_field_filter(doc, fields, filter))
        .chain(
            group
                .groups
                .iter()
                .map(|group| document_matches_filter_group(doc, fields, group)),
        )
        .peekable();

    if results.peek().is_none() {
        return true;
    }

    match group.logic {
        SearchFilterLogic::And => results.all(|matched| matched),
        SearchFilterLogic::Or => results.any(|matched| matched),
    }
}

fn document_matches_field_filter(
    doc: &TantivyDocument,
    fields: &SearchSchemaFields,
    filter: &SearchFieldFilter,
) -> bool {
    if filter.fields.is_empty() {
        return false;
    }
    let field_values = filter
        .fields
        .iter()
        .flat_map(|field| document_filter_values(doc, fields, field))
        .collect::<Vec<_>>();
    if field_values.is_empty() {
        return matches!(filter.op, SearchFilterOp::Nq | SearchFilterOp::Exclude);
    }

    match filter.op {
        SearchFilterOp::Eq => any_filter_value_match(&field_values, &filter.values, values_equal),
        SearchFilterOp::Nq => !any_filter_value_match(&field_values, &filter.values, values_equal),
        SearchFilterOp::Gt => any_filter_value_match(&field_values, &filter.values, |a, b| {
            compare_filter_values(a, b)
                .map(|ordering| ordering.is_gt())
                .unwrap_or(false)
        }),
        SearchFilterOp::Gte => any_filter_value_match(&field_values, &filter.values, |a, b| {
            compare_filter_values(a, b)
                .map(|ordering| ordering.is_gt() || ordering.is_eq())
                .unwrap_or(false)
        }),
        SearchFilterOp::Lt => any_filter_value_match(&field_values, &filter.values, |a, b| {
            compare_filter_values(a, b)
                .map(|ordering| ordering.is_lt())
                .unwrap_or(false)
        }),
        SearchFilterOp::Lte => any_filter_value_match(&field_values, &filter.values, |a, b| {
            compare_filter_values(a, b)
                .map(|ordering| ordering.is_lt() || ordering.is_eq())
                .unwrap_or(false)
        }),
        SearchFilterOp::Include => {
            any_filter_value_match(&field_values, &filter.values, values_include)
        }
        SearchFilterOp::Exclude => {
            !any_filter_value_match(&field_values, &filter.values, values_include)
        }
    }
}

fn any_filter_value_match(
    field_values: &[JsonValue],
    filter_values: &[JsonValue],
    predicate: fn(&JsonValue, &JsonValue) -> bool,
) -> bool {
    if filter_values.is_empty() {
        return false;
    }
    field_values.iter().any(|field_value| {
        filter_values
            .iter()
            .any(|value| predicate(field_value, value))
    })
}

fn document_filter_values(
    doc: &TantivyDocument,
    fields: &SearchSchemaFields,
    field_name: &str,
) -> Vec<JsonValue> {
    match normalize_search_field_name(field_name).as_str() {
        "doc_id" => text_filter_value(field_text(doc, fields.doc_id)),
        "kind" => text_filter_value(field_text(doc, fields.kind)),
        "entity_id" | "id" => text_filter_value(field_text(doc, fields.entity_id)),
        "parent_id" | "parent_uuid" => text_filter_value(field_text(doc, fields.parent_id)),
        "parent_key" => text_filter_value(field_text(doc, fields.parent_key)),
        "title" => text_filter_value(field_text(doc, fields.title)),
        "body" => text_filter_value(field_text(doc, fields.body)),
        "content" => text_filter_value(field_text(doc, fields.content)),
        "field" | "content_field" => text_filter_value(field_text(doc, fields.content_field)),
        "metadata" => text_filter_value(field_text(doc, fields.metadata)),
        "created_at" | "created_at_ms" => i64_filter_value(field_i64(doc, fields.created_at_ms)),
        "updated_at" | "updated_at_ms" => i64_filter_value(field_i64(doc, fields.updated_at_ms)),
        "status" => text_filter_value(field_text(doc, fields.status)),
        "datatype" | "data_type" => text_filter_value(field_text(doc, fields.datatype)),
        "category" => text_filter_value(field_text(doc, fields.category)),
        "source" => text_filter_value(field_text(doc, fields.source)),
        "app_id" => text_filter_value(field_text(doc, fields.app_id)),
        "agent_id" => text_filter_value(field_text(doc, fields.agent_id)),
        "session_id" => text_filter_value(field_text(doc, fields.session_id)),
        "collection_uuid" | "collection_id" => {
            text_filter_value(field_text(doc, fields.collection_uuid))
        }
        "collection_type" => text_filter_value(field_text(doc, fields.collection_type)),
        "archived" | "deleted" => {
            bool_filter_value(parse_filter_bool(&field_text(doc, fields.archived)))
        }
        _ => Vec::new(),
    }
}

fn text_filter_value(value: String) -> Vec<JsonValue> {
    if value.is_empty() {
        Vec::new()
    } else {
        vec![JsonValue::String(value)]
    }
}

fn i64_filter_value(value: Option<i64>) -> Vec<JsonValue> {
    value
        .map(|value| vec![JsonValue::Number(value.into())])
        .unwrap_or_default()
}

fn bool_filter_value(value: Option<bool>) -> Vec<JsonValue> {
    value
        .map(|value| vec![JsonValue::Bool(value)])
        .unwrap_or_default()
}

fn values_equal(field_value: &JsonValue, filter_value: &JsonValue) -> bool {
    if let (Some(left), Some(right)) = (json_as_bool(field_value), json_as_bool(filter_value)) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (json_as_f64(field_value), json_as_f64(filter_value)) {
        return (left - right).abs() < f64::EPSILON;
    }
    json_to_match_string(field_value).eq_ignore_ascii_case(&json_to_match_string(filter_value))
}

fn values_include(field_value: &JsonValue, filter_value: &JsonValue) -> bool {
    let haystack = json_to_match_string(field_value).to_lowercase();
    let needle = json_to_match_string(filter_value).to_lowercase();
    !needle.is_empty() && haystack.contains(&needle)
}

fn compare_filter_values(
    field_value: &JsonValue,
    filter_value: &JsonValue,
) -> Option<std::cmp::Ordering> {
    let left = json_as_f64(field_value)?;
    let right = json_as_f64(filter_value)?;
    left.partial_cmp(&right)
}

fn json_as_f64(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(value) => value.as_f64(),
        JsonValue::String(value) => value.trim().parse::<f64>().ok(),
        JsonValue::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn json_as_bool(value: &JsonValue) -> Option<bool> {
    match value {
        JsonValue::Bool(value) => Some(*value),
        JsonValue::String(value) => parse_filter_bool(value),
        JsonValue::Number(value) => value.as_i64().map(|value| value != 0),
        _ => None,
    }
}

fn parse_filter_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" | "deleted" | "archived" => Some(true),
        "false" | "0" | "no" | "n" | "active" | "not_deleted" | "not_archived" => Some(false),
        _ => None,
    }
}

fn json_to_match_string(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => String::new(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => value.clone(),
        JsonValue::Array(values) => values
            .iter()
            .map(json_to_match_string)
            .collect::<Vec<_>>()
            .join(" "),
        JsonValue::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn empty_to_none(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn fuzzy_fallback_query_text(query: &str) -> Option<String> {
    let terms = normalized_query_terms(query, 4);
    if terms.is_empty() {
        None
    } else {
        Some(terms.join(" "))
    }
}

fn normalized_query_terms(query: &str, min_chars: usize) -> Vec<String> {
    let mut terms = Vec::new();
    let mut current = String::new();
    for ch in query.chars() {
        if ch.is_alphanumeric() {
            current.extend(ch.to_lowercase());
        } else {
            push_normalized_query_term(&mut terms, &mut current, min_chars);
        }
    }
    push_normalized_query_term(&mut terms, &mut current, min_chars);
    terms
}

fn query_contains_numeric_fragment(query: &str) -> bool {
    let mut digits = 0;
    for ch in query.chars() {
        if ch.is_ascii_digit() {
            digits += 1;
            if digits >= NUMERIC_FRAGMENT_MIN_LEN {
                return true;
            }
        } else {
            digits = 0;
        }
    }
    false
}

fn numeric_search_fragments<'a>(values: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut fragments = HashSet::new();
    for value in values {
        collect_numeric_search_fragments(value, &mut fragments);
        if fragments.len() >= NUMERIC_FRAGMENT_MAX_COUNT {
            break;
        }
    }
    let mut fragments = fragments.into_iter().collect::<Vec<_>>();
    fragments.sort();
    fragments
}

fn collect_numeric_search_fragments(value: &str, fragments: &mut HashSet<String>) {
    let mut run = String::new();
    for ch in value.chars() {
        if ch.is_ascii_digit() {
            run.push(ch);
        } else {
            push_numeric_run_fragments(&run, fragments);
            run.clear();
        }
        if fragments.len() >= NUMERIC_FRAGMENT_MAX_COUNT {
            return;
        }
    }
    push_numeric_run_fragments(&run, fragments);
}

fn push_numeric_run_fragments(run: &str, fragments: &mut HashSet<String>) {
    let len = run.len();
    if len < NUMERIC_FRAGMENT_MIN_LEN {
        return;
    }
    for start in 0..len {
        let max_end = len.min(start.saturating_add(NUMERIC_FRAGMENT_MAX_LEN));
        for end in start + NUMERIC_FRAGMENT_MIN_LEN..=max_end {
            fragments.insert(run[start..end].to_string());
            if fragments.len() >= NUMERIC_FRAGMENT_MAX_COUNT {
                return;
            }
        }
    }
}

fn push_normalized_query_term(terms: &mut Vec<String>, current: &mut String, min_chars: usize) {
    if current.chars().count() >= min_chars {
        terms.push(std::mem::take(current));
    } else {
        current.clear();
    }
}

fn parent_key(kind: SearchEntityKind, parent_id: &str) -> String {
    format!("{}:{parent_id}", kind.as_str())
}

fn extract_chat_message_title(value: &JsonValue) -> Option<String> {
    value
        .get("role")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .map(|role| format!("{role} message"))
        .or_else(|| {
            value
                .get("type")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|kind| !kind.is_empty())
                .map(|kind| format!("{kind} message"))
        })
}

fn push_content_field(fields: &mut Vec<SearchContentField>, field: &str, value: &str) {
    let field = normalize_search_field_name(field);
    let value = value.trim();
    if field.is_empty() || value.is_empty() {
        return;
    }
    fields.push(SearchContentField::new(field, value));
}

fn push_json_content_fields(
    fields: &mut Vec<SearchContentField>,
    fallback_field: &str,
    value: &JsonValue,
) {
    match value {
        JsonValue::Object(map) => {
            for (key, value) in map {
                if should_skip_content_data_key(key) {
                    continue;
                }
                let field = if normalize_search_field_name(key) == "content" {
                    fallback_field
                } else {
                    key
                };
                push_json_string_values(fields, field, value);
            }
        }
        _ => push_json_string_values(fields, fallback_field, value),
    }
}

fn should_skip_content_data_key(key: &str) -> bool {
    matches!(
        normalize_search_field_name(key).as_str(),
        "uuid"
            | "id"
            | "title"
            | "datatype"
            | "data_type"
            | "collection_uuid"
            | "collection_id"
            | "parent_uuid"
            | "parent_id"
            | "archived_at"
            | "archived_from_collection_uuid"
            | "created_at"
            | "updated_at"
            | "status"
            | "status_timestamp_ms"
            | "actor_type"
            | "actor_app_id"
            | "actor_display_name"
    )
}

fn push_json_string_values(fields: &mut Vec<SearchContentField>, field: &str, value: &JsonValue) {
    let mut output = String::new();
    push_json_scalar_text(value, &mut output);
    push_content_field(fields, field, &output);
}

fn push_chat_message_content_field(
    fields: &mut Vec<SearchContentField>,
    parsed: &JsonValue,
    raw_message_json: &str,
) {
    if let Some(content) = parsed.get("content") {
        push_json_string_values(fields, "message_content", content);
    } else {
        push_content_field(fields, "message_json", raw_message_json);
    }
}

fn join_content_field_values(fields: &[SearchContentField], include_title: bool) -> String {
    fields
        .iter()
        .filter(|field| include_title || field.field != "title")
        .map(|field| field.value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn push_json_scalar_text(value: &JsonValue, output: &mut String) {
    match value {
        JsonValue::Null => {}
        JsonValue::Bool(value) => push_text(output, if *value { "true" } else { "false" }),
        JsonValue::Number(value) => push_text(output, &value.to_string()),
        JsonValue::String(value) => push_text(output, value),
        JsonValue::Array(items) => {
            for item in items {
                push_json_scalar_text(item, output);
            }
        }
        JsonValue::Object(map) => {
            for value in map.values() {
                push_json_scalar_text(value, output);
            }
        }
    }
}

fn push_text(output: &mut String, value: &str) {
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    if !output.is_empty() {
        output.push('\n');
    }
    output.push_str(value);
}

fn make_snippet(query: &str, title: &str, body: &str) -> String {
    let terms = normalized_query_terms(query, 1);
    let title_lower = title.to_lowercase();
    let body_lower = body.to_lowercase();
    let title_has_match = terms.iter().any(|term| title_lower.contains(term));
    let body_has_match = terms.iter().any(|term| body_lower.contains(term));
    let source = if title_has_match || (!body_has_match && !title.trim().is_empty()) {
        title
    } else {
        body
    };
    let source = source.trim();
    if source.is_empty() {
        return String::new();
    }

    let source_lower = source.to_lowercase();
    let first_match = terms.iter().find_map(|term| source_lower.find(term));
    let start = first_match.map(|pos| pos.saturating_sub(80)).unwrap_or(0);
    let end = source.len().min(start.saturating_add(180));
    slice_on_char_boundaries(source, start, end)
        .trim()
        .to_string()
}

fn slice_on_char_boundaries(source: &str, start: usize, end: usize) -> &str {
    let mut safe_start = start.min(source.len());
    while safe_start > 0 && !source.is_char_boundary(safe_start) {
        safe_start -= 1;
    }
    let mut safe_end = end.min(source.len());
    while safe_end > safe_start && !source.is_char_boundary(safe_end) {
        safe_end -= 1;
    }
    source.get(safe_start..safe_end).unwrap_or_default()
}
