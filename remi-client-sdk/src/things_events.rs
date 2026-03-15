use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

use crate::things_crdt::{ThingCollectionEntry, ThingEntry};

/// SDK -> UI event stream for Things updates.
///
/// This is designed to support precise, incremental UI updates without requiring
/// full snapshot refreshes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ThingsEvent {
    /// Replace the entire Things snapshot in one shot.
    ///
    /// This is used for large state replacement flows (e.g., after sync/bootstrap/recovery)
    /// where emitting per-entity diffs is undesirable.
    SnapshotReplace {
        device_id: String,
        collections: Vec<ThingCollectionEntry>,
        things: Vec<ThingEntry>,
        dirty: bool,
        last_sync_at: Option<String>,
    },
    CollectionUpsert {
        device_id: String,
        collection_uuid: String,
        fields: BTreeMap<String, JsonValue>,
    },
    CollectionDelete {
        device_id: String,
        collection_uuid: String,
    },
    ThingUpsert {
        device_id: String,
        thing_uuid: String,
        fields: BTreeMap<String, JsonValue>,
    },
    ThingDelete {
        device_id: String,
        thing_uuid: String,
    },
    ThingStatusSet {
        device_id: String,
        thing_uuid: String,
        status: String,
        status_timestamp_ms: i64,
    },
    ThingMarkdownSplice {
        device_id: String,
        thing_uuid: String,
        block_id: String,
        index: u32,
        delete: u32,
        insert: String,
    },
    /// All local data has been wiped (logout). UI should clear all state.
    DataWiped,
}

impl ThingsEvent {
    pub fn device_id(&self) -> &str {
        match self {
            ThingsEvent::SnapshotReplace { device_id, .. }
            | ThingsEvent::CollectionUpsert { device_id, .. }
            | ThingsEvent::CollectionDelete { device_id, .. }
            | ThingsEvent::ThingUpsert { device_id, .. }
            | ThingsEvent::ThingDelete { device_id, .. }
            | ThingsEvent::ThingStatusSet { device_id, .. }
            | ThingsEvent::ThingMarkdownSplice { device_id, .. } => device_id,
            ThingsEvent::DataWiped => "",
        }
    }
}
