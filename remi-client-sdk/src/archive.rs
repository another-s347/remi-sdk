use crate::chat_types::ChatSessionExportBundle;
use crate::things_crdt::{ContentTypeRegistry, ThingEntry, ThingsSnapshotState};
use crate::types::{CrdtDocumentRow, EventPayload, InternalKvRecord, PreferenceRecord, TriggerRegistration};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read, Write};

pub const DATA_ARCHIVE_VERSION: u32 = 1;
pub const MANIFEST_PATH: &str = "metadata/manifest.json";
pub const THINGS_SNAPSHOT_PATH: &str = "metadata/things_snapshot_state.json";
pub const TRIGGERS_PATH: &str = "metadata/triggers.json";
pub const TRIGGER_BINDINGS_PATH: &str = "metadata/trigger_bindings.json";
pub const EVENTS_PATH: &str = "metadata/events.json";
pub const PREFERENCES_PATH: &str = "metadata/preferences.json";
pub const INTERNAL_KV_PATH: &str = "metadata/internal_kv.json";
pub const CRDT_DOCUMENTS_PATH: &str = "metadata/crdt_documents.json";
pub const CHAT_INDEX_PATH: &str = "chat/sessions/index.json";

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataArchiveImportMode {
    Restore,
    Merge,
}

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
pub struct DataArchiveCounts {
    pub collections: usize,
    pub things: usize,
    pub triggers: usize,
    pub trigger_bindings: usize,
    pub chat_sessions: usize,
    pub events: usize,
    pub preferences: usize,
    pub crdt_documents: usize,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct DataArchiveManifest {
    pub version: u32,
    pub exported_at: DateTime<Utc>,
    pub device_id: String,
    pub domains: Vec<String>,
    pub counts: DataArchiveCounts,
}

impl DataArchiveManifest {
    pub fn new(device_id: &str, counts: DataArchiveCounts) -> Self {
        Self {
            version: DATA_ARCHIVE_VERSION,
            exported_at: Utc::now(),
            device_id: device_id.to_string(),
            domains: vec![
                "things".to_string(),
                "triggers".to_string(),
                "chat".to_string(),
                "events".to_string(),
                "preferences".to_string(),
            ],
            counts,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
pub struct DataArchiveImportReport {
    pub collections_imported: usize,
    pub things_imported: usize,
    pub triggers_imported: usize,
    pub trigger_bindings_imported: usize,
    pub chat_sessions_imported: usize,
    pub chat_messages_imported: usize,
    pub events_imported: usize,
    pub preferences_imported: usize,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ArchivedTrigger {
    pub registration: TriggerRegistration,
    #[serde(default)]
    pub is_paused: bool,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ArchivedTriggerBinding {
    pub trigger_uuid: String,
    pub entity_type: String,
    pub entity_uuid: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ArchivedPreferenceEntry {
    pub key: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub value_type: String,
    pub value_json: String,
    pub updated_at: i64,
}

impl From<PreferenceRecord> for ArchivedPreferenceEntry {
    fn from(value: PreferenceRecord) -> Self {
        Self {
            key: value.key,
            display_name: value.display_name,
            description: value.description,
            value_type: value.value_type,
            value_json: value.value_json,
            updated_at: value.updated_at,
        }
    }
}

impl From<ArchivedPreferenceEntry> for PreferenceRecord {
    fn from(value: ArchivedPreferenceEntry) -> Self {
        Self {
            key: value.key,
            display_name: value.display_name,
            description: value.description,
            value_type: value.value_type,
            value_json: value.value_json,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ArchivedInternalKvEntry {
    pub key: String,
    pub value: String,
}

impl From<InternalKvRecord> for ArchivedInternalKvEntry {
    fn from(value: InternalKvRecord) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ArchivedCrdtDocument {
    pub uuid: String,
    pub data_type: String,
    pub automerge_doc_base64: String,
    pub sync_state_base64: String,
    pub dirty: bool,
    pub last_sync_at: Option<String>,
}

impl From<CrdtDocumentRow> for ArchivedCrdtDocument {
    fn from(value: CrdtDocumentRow) -> Self {
        Self {
            uuid: value.uuid,
            data_type: value.data_type,
            automerge_doc_base64: base64::engine::general_purpose::STANDARD.encode(value.automerge_doc),
            sync_state_base64: base64::engine::general_purpose::STANDARD.encode(value.sync_state),
            dirty: value.dirty,
            last_sync_at: value.last_sync_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ArchivePayload {
    pub manifest: DataArchiveManifest,
    pub things: ThingsSnapshotState,
    pub triggers: Vec<ArchivedTrigger>,
    pub trigger_bindings: Vec<ArchivedTriggerBinding>,
    pub events: Vec<EventPayload>,
    pub preferences: Vec<ArchivedPreferenceEntry>,
    pub internal_kv: Vec<ArchivedInternalKvEntry>,
    pub crdt_documents: Vec<ArchivedCrdtDocument>,
    pub chat_sessions: Vec<ChatSessionExportBundle>,
}

pub fn build_vfs_entries(
    snapshot: &ThingsSnapshotState,
    triggers: &[ArchivedTrigger],
) -> Result<Vec<(String, String)>> {
    let mut files = Vec::new();
    let mut trigger_map = BTreeMap::new();
    for trigger in triggers {
        trigger_map.insert(trigger.registration.trigger_uuid.clone(), trigger);
    }

    for trigger in triggers {
        let base = format!("vfs/trigger/{}", trigger.registration.trigger_uuid);
        files.push((format!("{base}/name"), trigger.registration.name.clone()));
        let rule_json = serde_json::to_string_pretty(&serde_json::json!({
            "version": trigger.registration.version,
            "precondition": trigger.registration.precondition,
            "condition": trigger.registration.condition,
        }))
        .context("Failed to serialize trigger rule for archive")?;
        files.push((format!("{base}/rule.json"), rule_json));
    }

    let mut children_by_parent: BTreeMap<(String, Option<String>), Vec<&ThingEntry>> = BTreeMap::new();
    for thing in &snapshot.things {
        children_by_parent
            .entry((thing.collection_uuid.clone(), thing.parent_uuid.clone()))
            .or_default()
            .push(thing);
    }
    for things in children_by_parent.values_mut() {
        things.sort_by(|left, right| left.uuid.cmp(&right.uuid));
    }

    for collection in &snapshot.collections {
        let base = format!("vfs/collection/{}", collection.uuid);
        files.push((format!("{base}/name"), collection.title.clone()));
        if let Some(trigger_uuid) = &collection.trigger_uuid {
            files.push((format!("{base}/trigger"), trigger_uuid.clone()));
        }
        append_collection_things(
            &mut files,
            &children_by_parent,
            &collection.uuid,
            None,
            &format!("{base}/things"),
        )?;
    }

    if !trigger_map.is_empty() {
        files.sort_by(|left, right| left.0.cmp(&right.0));
    }
    Ok(files)
}

fn append_collection_things(
    files: &mut Vec<(String, String)>,
    children_by_parent: &BTreeMap<(String, Option<String>), Vec<&ThingEntry>>,
    collection_uuid: &str,
    parent_uuid: Option<&str>,
    base_dir: &str,
) -> Result<()> {
    let content_registry = ContentTypeRegistry::new();
    let key = (collection_uuid.to_string(), parent_uuid.map(|value| value.to_string()));
    let Some(children) = children_by_parent.get(&key) else {
        return Ok(());
    };

    for thing in children {
        let thing_base = format!("{base_dir}/{}", thing.uuid);
        files.push((format!("{thing_base}/name"), thing.title.clone()));
        files.push((format!("{thing_base}/status"), thing.status.clone()));
        if let Some(trigger_uuid) = &thing.trigger_uuid {
            files.push((format!("{thing_base}/trigger"), trigger_uuid.clone()));
        }

        let (markdown, entries) = content_registry
            .extract_thing_snapshot_parts(&thing.data)
            .with_context(|| format!("Failed to extract thing content for {}", thing.uuid))?;
        files.push((
            format!("{thing_base}/content.md"),
            markdown.unwrap_or_default(),
        ));
        for (index, entry) in entries.iter().enumerate() {
            files.push((
                format!("{thing_base}/entries.{index}"),
                serde_json::to_string_pretty(&content_registry.serialize_content_entry(entry))
                    .context("Failed to serialize content entry for archive")?,
            ));
        }

        append_collection_things(
            files,
            children_by_parent,
            collection_uuid,
            Some(&thing.uuid),
            &format!("{thing_base}/things"),
        )?;
    }

    Ok(())
}

pub fn write_archive(payload: &ArchivePayload) -> Result<Vec<u8>> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::FileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);

    write_json_entry(&mut writer, MANIFEST_PATH, &payload.manifest, options)?;
    write_json_entry(&mut writer, THINGS_SNAPSHOT_PATH, &payload.things, options)?;
    write_json_entry(&mut writer, TRIGGERS_PATH, &payload.triggers, options)?;
    write_json_entry(
        &mut writer,
        TRIGGER_BINDINGS_PATH,
        &payload.trigger_bindings,
        options,
    )?;
    write_json_entry(&mut writer, EVENTS_PATH, &payload.events, options)?;
    write_json_entry(&mut writer, PREFERENCES_PATH, &payload.preferences, options)?;
    write_json_entry(&mut writer, INTERNAL_KV_PATH, &payload.internal_kv, options)?;
    write_json_entry(
        &mut writer,
        CRDT_DOCUMENTS_PATH,
        &payload.crdt_documents,
        options,
    )?;

    let mut chat_ids = Vec::with_capacity(payload.chat_sessions.len());
    for session in &payload.chat_sessions {
        chat_ids.push(session.session.session_id.clone());
        write_json_entry(
            &mut writer,
            &chat_session_path(&session.session.session_id),
            session,
            options,
        )?;
    }
    write_json_entry(&mut writer, CHAT_INDEX_PATH, &chat_ids, options)?;

    for (path, content) in build_vfs_entries(&payload.things, &payload.triggers)? {
        writer
            .start_file(path, options)
            .context("Failed to start VFS archive entry")?;
        writer
            .write_all(content.as_bytes())
            .context("Failed to write VFS archive entry")?;
    }

    let cursor = writer.finish().context("Failed to finalize data archive")?;
    Ok(cursor.into_inner())
}

pub fn read_manifest(archive_bytes: &[u8]) -> Result<DataArchiveManifest> {
    let mut archive = open_archive(archive_bytes)?;
    let manifest = read_required_json_entry(&mut archive, MANIFEST_PATH)?;
    Ok(manifest)
}

pub fn read_archive_payload(archive_bytes: &[u8]) -> Result<ArchivePayload> {
    let mut archive = open_archive(archive_bytes)?;
    let manifest: DataArchiveManifest = read_required_json_entry(&mut archive, MANIFEST_PATH)?;
    if manifest.version != DATA_ARCHIVE_VERSION {
        anyhow::bail!(
            "Unsupported data archive version {} (expected {})",
            manifest.version,
            DATA_ARCHIVE_VERSION
        );
    }

    let things = read_required_json_entry(&mut archive, THINGS_SNAPSHOT_PATH)?;
    let triggers = read_optional_json_entry(&mut archive, TRIGGERS_PATH)?.unwrap_or_default();
    let trigger_bindings =
        read_optional_json_entry(&mut archive, TRIGGER_BINDINGS_PATH)?.unwrap_or_default();
    let events = read_optional_json_entry(&mut archive, EVENTS_PATH)?.unwrap_or_default();
    let preferences = read_optional_json_entry(&mut archive, PREFERENCES_PATH)?.unwrap_or_default();
    let internal_kv = read_optional_json_entry(&mut archive, INTERNAL_KV_PATH)?.unwrap_or_default();
    let crdt_documents =
        read_optional_json_entry(&mut archive, CRDT_DOCUMENTS_PATH)?.unwrap_or_default();

    let chat_ids: Vec<String> = read_optional_json_entry(&mut archive, CHAT_INDEX_PATH)?.unwrap_or_default();
    let mut seen_ids = BTreeSet::new();
    let mut chat_sessions = Vec::new();
    for session_id in chat_ids {
        if !seen_ids.insert(session_id.clone()) {
            continue;
        }
        let session = read_required_json_entry(&mut archive, &chat_session_path(&session_id))?;
        chat_sessions.push(session);
    }

    Ok(ArchivePayload {
        manifest,
        things,
        triggers,
        trigger_bindings,
        events,
        preferences,
        internal_kv,
        crdt_documents,
        chat_sessions,
    })
}

fn open_archive(archive_bytes: &[u8]) -> Result<zip::ZipArchive<Cursor<Vec<u8>>>> {
    let cursor = Cursor::new(archive_bytes.to_vec());
    zip::ZipArchive::new(cursor).context("Failed to open data archive")
}

fn chat_session_path(session_id: &str) -> String {
    format!("chat/sessions/{session_id}.json")
}

fn write_json_entry<T: Serialize>(
    writer: &mut zip::ZipWriter<Cursor<Vec<u8>>>,
    path: &str,
    value: &T,
    options: zip::write::FileOptions,
) -> Result<()> {
    writer
        .start_file(path, options)
        .with_context(|| format!("Failed to start archive entry {path}"))?;
    let json = serde_json::to_vec_pretty(value)
        .with_context(|| format!("Failed to serialize archive entry {path}"))?;
    writer
        .write_all(&json)
        .with_context(|| format!("Failed to write archive entry {path}"))?;
    Ok(())
}

fn read_required_json_entry<T: DeserializeOwned>(
    archive: &mut zip::ZipArchive<Cursor<Vec<u8>>>,
    path: &str,
) -> Result<T> {
    read_optional_json_entry(archive, path)?
        .ok_or_else(|| anyhow!("Missing required archive entry: {path}"))
}

fn read_optional_json_entry<T: DeserializeOwned>(
    archive: &mut zip::ZipArchive<Cursor<Vec<u8>>>,
    path: &str,
) -> Result<Option<T>> {
    let mut file = match archive.by_name(path) {
        Ok(file) => file,
        Err(zip::result::ZipError::FileNotFound) => return Ok(None),
        Err(error) => {
            return Err(anyhow!(error)).with_context(|| format!("Failed to read archive entry {path}"));
        }
    };
    let mut text = String::new();
    file.read_to_string(&mut text)
        .with_context(|| format!("Failed to read archive entry {path}"))?;
    let value = serde_json::from_str(&text)
        .with_context(|| format!("Failed to parse archive JSON entry {path}"))?;
    Ok(Some(value))
}