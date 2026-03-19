//! Built-in interrupt handlers for Things operations and Trigger publishing.

use crate::TriggerSdk;
use crate::interrupt_handler::{InterruptHandler, extract_interrupt_data};
use crate::things_crdt::ThingsSnapshot;
use crate::types::{TriggerRegistration, TriggerRule};
use serde::{Deserialize, Serialize};
use serde::Deserializer;
use serde_json::{Value as JsonValue, json};
use std::sync::Arc;

// ══════════════════════════════════════════════════════════════════════════════�?
// Things Interrupt Payloads
// ══════════════════════════════════════════════════════════════════════════════�?

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingCollectionInfo {
    pub uuid: String,
    pub title: String,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingInfo {
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    pub uuid: String,
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    pub title: String,
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    pub datatype: String,
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    pub data_json: String,
    #[serde(default)]
    pub parent_uuid: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    pub collection_uuid: String,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThingsThingEnvelope {
    pub thing: ThingInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThingsCollectionEnvelope {
    pub collection: ThingCollectionInfo,
}

fn deserialize_string_or_default<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn normalize_optional_string(s: Option<String>) -> Option<String> {
    match s {
        None => None,
        Some(v) => {
            let t = v.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        }
    }
}

fn normalize_string(v: &str) -> Option<String> {
    let t = v.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

fn normalize_required_uuid(value: &str, field: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(format!("{field} is required"))
    } else {
        Ok(trimmed.to_string())
    }
}

fn load_things_snapshot(
    sdk: &TriggerSdk,
    device_id: &str,
    include_things: bool,
) -> Result<ThingsSnapshot, String> {
    let snapshot_json = sdk
        .things_list_snapshot_json_with_options(
            device_id,
            include_things,
            crate::things_crdt::SnapshotOptions {
                include_content: false,
            },
        )
        .map_err(|e| format!("Failed to read local things snapshot: {e}"))?;

    serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse local things snapshot: {e}"))
}

/// Payload for things_thing_content_edit - matches agent's interrupt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingContentEdit {
    pub uuid: String,
    pub operation: String,
    #[serde(default)]
    pub new_title: Option<String>,
    #[serde(default)]
    pub new_content: Option<String>,
    #[serde(default)]
    pub old_str: Option<String>,
    #[serde(default)]
    pub new_str: Option<String>,
    #[serde(default)]
    pub line_number: Option<i64>,
    #[serde(default)]
    pub insert_text: Option<String>,
    #[serde(default)]
    pub append_text: Option<String>,
}

/// Payload for things_thing_removed - matches agent's interrupt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingRemoveInfo {
    pub uuid: String,
    #[serde(default)]
    pub collection_uuid: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

/// Payload for things_thing_moved - matches agent's interrupt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingMoveInfo {
    pub uuid: String,
    pub to_collection_uuid: String,
    #[serde(default)]
    pub to_parent_uuid: Option<String>,
}

/// Payload for events_retrieve_request - matches agent's interrupt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsRetrieveRequest {
    pub start_time: String,
    pub end_time: String,
}

/// Payload for events_abstract_request - matches agent's interrupt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsAbstractRequest {
    #[serde(default)]
    pub top_n: Option<JsonValue>,
}

// ══════════════════════════════════════════════════════════════════════════════�?
// Handler Implementations
// ══════════════════════════════════════════════════════════════════════════════�?

/// Handler for things_collection_added
pub struct CollectionAddedHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl CollectionAddedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for CollectionAddedHandler {
    fn handle(&self, interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        tracing::debug!(
            interrupt_id = %interrupt_id,
            payload = %payload,
            "[CollectionAddedHandler] Received interrupt"
        );
        let data = extract_interrupt_data(payload, "things_collection_added");
        tracing::debug!(
            extracted_data = %data,
            "[CollectionAddedHandler] Extracted data from payload"
        );
        let info: ThingCollectionInfo = if data.get("collection").is_some() {
            let env: ThingsCollectionEnvelope = serde_json::from_value(data)
                .map_err(|e| format!("Failed to parse collection envelope: {}", e))?;
            env.collection
        } else {
            serde_json::from_value(data)
                .map_err(|e| format!("Failed to parse collection: {}", e))?
        };

        let collection_uuid = normalize_required_uuid(&info.uuid, "uuid")?;

        let mut collection_obj = serde_json::Map::new();
        collection_obj.insert("uuid".to_string(), json!(collection_uuid));
        collection_obj.insert("title".to_string(), json!(info.title));
        if let Some(created_at) = normalize_optional_string(info.created_at) {
            collection_obj.insert("created_at".to_string(), json!(created_at));
        }
        if let Some(updated_at) = normalize_optional_string(info.updated_at) {
            collection_obj.insert("updated_at".to_string(), json!(updated_at));
        }
        let collection_json = JsonValue::Object(collection_obj);

        self.sdk
            .things_upsert_collection_json(&self.device_id, &collection_json.to_string())
            .map_err(|e| format!("Failed to upsert collection: {}", e))?;

        tracing::info!(
            uuid = %collection_uuid,
            title = %info.title,
            payload_json = %collection_json,
            "[CollectionAddedHandler] Collection added via interrupt"
        );

        Ok(json!({
            "confirmed": true,
            "uuid": collection_uuid,
        }))
    }
}

/// Handler for things_collection_edited
pub struct CollectionEditedHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl CollectionEditedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for CollectionEditedHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "things_collection_edited");
        let info: ThingCollectionInfo = if data.get("collection").is_some() {
            let env: ThingsCollectionEnvelope = serde_json::from_value(data)
                .map_err(|e| format!("Failed to parse collection envelope: {}", e))?;
            env.collection
        } else {
            serde_json::from_value(data)
                .map_err(|e| format!("Failed to parse collection: {}", e))?
        };

        let collection_uuid = normalize_required_uuid(&info.uuid, "uuid")?;

        let mut collection_obj = serde_json::Map::new();
        collection_obj.insert("uuid".to_string(), json!(collection_uuid));
        collection_obj.insert("title".to_string(), json!(info.title));
        if let Some(created_at) = normalize_optional_string(info.created_at) {
            collection_obj.insert("created_at".to_string(), json!(created_at));
        }
        if let Some(updated_at) = normalize_optional_string(info.updated_at) {
            collection_obj.insert("updated_at".to_string(), json!(updated_at));
        }
        let collection_json = JsonValue::Object(collection_obj);

        self.sdk
            .things_upsert_collection_json(&self.device_id, &collection_json.to_string())
            .map_err(|e| format!("Failed to upsert collection: {}", e))?;

        tracing::info!(uuid = %collection_uuid, title = %info.title, "Collection edited via interrupt");

        Ok(json!({
            "confirmed": true,        }))
    }
}

/// Handler for things_collection_removed
pub struct CollectionRemovedHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl CollectionRemovedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for CollectionRemovedHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "things_collection_removed");
        let uuid = data
            .get("uuid")
            .and_then(|v| v.as_str())
            .ok_or("Missing uuid in collection_removed")?;

        self.sdk
            .things_delete_collection(&self.device_id, uuid)
            .map_err(|e| format!("Failed to delete collection: {}", e))?;

        tracing::info!(uuid, "Collection removed via interrupt");

        Ok(json!({
            "confirmed": true,        }))
    }
}

/// Handler for things_thing_added
pub struct ThingAddedHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl ThingAddedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for ThingAddedHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "things_thing_added");
        let info: ThingInfo = if data.get("thing").is_some() {
            let env: ThingsThingEnvelope = serde_json::from_value(data)
                .map_err(|e| format!("Failed to parse thing envelope: {}", e))?;
            env.thing
        } else {
            serde_json::from_value(data).map_err(|e| format!("Failed to parse thing: {}", e))?
        };

        let thing_uuid = normalize_required_uuid(&info.uuid, "uuid")?;
        let collection_uuid = normalize_required_uuid(&info.collection_uuid, "collection_uuid")?;
        let datatype = normalize_string(&info.datatype).unwrap_or_else(|| "markdown".to_string());

        let snapshot = load_things_snapshot(&self.sdk, &self.device_id, false)?;
        if snapshot
            .collections
            .iter()
            .find(|c| c.uuid == collection_uuid)
            .is_none()
        {
            return Err(format!("Collection not found: {collection_uuid}"));
        }

        // Convert incoming (mobile/agent) payload to SDK v2 ThingUpsert JSON.
        // ThingUpsert requires: uuid, title, datatype, (optional)data, collection_uuid, (optional)parent_uuid.
        let data_value: Option<JsonValue> =
            normalize_string(&info.data_json).and_then(|s| serde_json::from_str(&s).ok());

        let mut thing_obj = serde_json::Map::new();
        thing_obj.insert("uuid".to_string(), json!(thing_uuid));
        thing_obj.insert("title".to_string(), json!(info.title));
        thing_obj.insert("datatype".to_string(), json!(datatype));
        thing_obj.insert("collection_uuid".to_string(), json!(collection_uuid));
        if let Some(parent_uuid) = normalize_optional_string(info.parent_uuid) {
            thing_obj.insert("parent_uuid".to_string(), json!(parent_uuid));
        }
        if let Some(created_at) = normalize_optional_string(info.created_at) {
            thing_obj.insert("created_at".to_string(), json!(created_at));
        }
        if let Some(updated_at) = normalize_optional_string(info.updated_at) {
            thing_obj.insert("updated_at".to_string(), json!(updated_at));
        }
        if let Some(v) = data_value {
            thing_obj.insert("data".to_string(), v);
        }
        let thing_json = JsonValue::Object(thing_obj);

        self.sdk
            .things_upsert_thing_json(&self.device_id, &thing_json.to_string())
            .map_err(|e| format!("Failed to upsert thing: {}", e))?;

        tracing::info!(uuid = %thing_uuid, title = %info.title, "Thing added via interrupt");

        Ok(json!({
            "confirmed": true,
            "uuid": thing_uuid,
            "collection_uuid": collection_uuid,
        }))
    }
}

/// Handler for things_thing_edited
pub struct ThingEditedHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl ThingEditedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for ThingEditedHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "things_thing_edited");
        let info: ThingInfo = if data.get("thing").is_some() {
            let env: ThingsThingEnvelope = serde_json::from_value(data)
                .map_err(|e| format!("Failed to parse thing envelope: {}", e))?;
            env.thing
        } else {
            serde_json::from_value(data).map_err(|e| format!("Failed to parse thing: {}", e))?
        };

        let thing_uuid = normalize_required_uuid(&info.uuid, "uuid")?;
        let collection_uuid = normalize_required_uuid(&info.collection_uuid, "collection_uuid")?;
        let datatype = normalize_string(&info.datatype).unwrap_or_else(|| "markdown".to_string());

        // Convert incoming payload to SDK v2 ThingUpsert JSON.
        let data_value: Option<JsonValue> =
            normalize_string(&info.data_json).and_then(|s| serde_json::from_str(&s).ok());

        let mut thing_obj = serde_json::Map::new();
        thing_obj.insert("uuid".to_string(), json!(thing_uuid));
        thing_obj.insert("title".to_string(), json!(info.title));
        thing_obj.insert("datatype".to_string(), json!(datatype));
        thing_obj.insert("collection_uuid".to_string(), json!(collection_uuid));
        if let Some(parent_uuid) = normalize_optional_string(info.parent_uuid) {
            thing_obj.insert("parent_uuid".to_string(), json!(parent_uuid));
        }
        if let Some(created_at) = normalize_optional_string(info.created_at) {
            thing_obj.insert("created_at".to_string(), json!(created_at));
        }
        if let Some(updated_at) = normalize_optional_string(info.updated_at) {
            thing_obj.insert("updated_at".to_string(), json!(updated_at));
        }
        if let Some(v) = data_value {
            thing_obj.insert("data".to_string(), v);
        }
        let thing_json = JsonValue::Object(thing_obj);

        self.sdk
            .things_upsert_thing_json(&self.device_id, &thing_json.to_string())
            .map_err(|e| format!("Failed to upsert thing: {}", e))?;

        tracing::info!(uuid = %thing_uuid, title = %info.title, "Thing edited via interrupt");

        Ok(json!({
            "confirmed": true,        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn thing_envelope_allows_nullable_required_strings() {
        let envelope: ThingsThingEnvelope = serde_json::from_value(json!({
            "thing": {
                "uuid": null,
                "title": "hello",
                "datatype": null,
                "data_json": null,
                "parent_uuid": null,
                "collection_uuid": "collection-1",
                "created_at": null,
                "updated_at": null
            }
        }))
        .expect("envelope should deserialize");

        assert_eq!(envelope.thing.uuid, "");
        assert_eq!(envelope.thing.datatype, "");
        assert_eq!(envelope.thing.data_json, "");
        assert_eq!(envelope.thing.collection_uuid, "collection-1");
        assert_eq!(envelope.thing.parent_uuid, None);
    }
}

/// Handler for things_thing_removed
pub struct ThingRemovedHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl ThingRemovedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for ThingRemovedHandler {
    fn handle(&self, interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        tracing::info!(interrupt_id = %interrupt_id, payload = %payload, "[ThingRemovedHandler] Received interrupt");

        let data = extract_interrupt_data(payload, "things_thing_removed");
        tracing::info!(data = %data, "[ThingRemovedHandler] Extracted data");

        let info: ThingRemoveInfo = serde_json::from_value(data)
            .map_err(|e| format!("Failed to parse thing_removed: {}", e))?;
        tracing::info!(uuid = %info.uuid, collection_uuid = ?info.collection_uuid, "[ThingRemovedHandler] Parsed info");

        // collection_uuid is ignored by v2 delete implementation, pass empty string if not provided
        let collection_uuid = info.collection_uuid.as_deref().unwrap_or("");

        tracing::info!(device_id = %self.device_id, collection_uuid = %collection_uuid, uuid = %info.uuid, "[ThingRemovedHandler] Calling things_delete_thing");
        self.sdk
            .things_delete_thing(&self.device_id, collection_uuid, &info.uuid)
            .map_err(|e| format!("Failed to delete thing: {}", e))?;

        tracing::info!(uuid = %info.uuid, "[ThingRemovedHandler] Thing removed successfully");

        Ok(json!({
            "confirmed": true,        }))
    }
}

/// Handler for things_thing_content_edit
pub struct ThingContentEditHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl ThingContentEditHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for ThingContentEditHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "things_thing_content_edit");
        let edit: ThingContentEdit = serde_json::from_value(data)
            .map_err(|e| format!("Failed to parse content edit: {}", e))?;

        let result = self
            .sdk
            .things_edit_content(
                &self.device_id,
                &edit.uuid,
                &edit.operation,
                edit.new_title.as_deref(),
                edit.new_content.as_deref(),
                edit.old_str.as_deref(),
                edit.new_str.as_deref(),
                edit.line_number.map(|n| n as usize),
                edit.insert_text.as_deref(),
                edit.append_text.as_deref(),
            )
            .map_err(|e| format!("Failed to edit content: {}", e))?;

        tracing::info!(uuid = %edit.uuid, operation = %edit.operation, "Thing content edited via interrupt");

        // Return the edit result for resume_value
        let result_json: JsonValue =
            serde_json::from_str(&result).unwrap_or_else(|_| json!({"result": result}));

        Ok(json!({
            "confirmed": true,            "result": result_json,
        }))
    }
}

/// Handler for things_thing_moved
pub struct ThingMovedHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl ThingMovedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for ThingMovedHandler {
    fn handle(&self, interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        tracing::info!(interrupt_id = %interrupt_id, payload = %payload, "[ThingMovedHandler] Received interrupt");

        let data = extract_interrupt_data(payload, "things_thing_moved");
        tracing::info!(data = %data, "[ThingMovedHandler] Extracted data");

        let info: ThingMoveInfo = match serde_json::from_value(data.clone()) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, data = %data, "[ThingMovedHandler] Failed to parse payload");
                return Ok(json!({
                    "confirmed": false,                    "error": format!("Failed to parse things_thing_moved: {e}"),
                }));
            }
        };
        tracing::info!(uuid = %info.uuid, to_collection_uuid = %info.to_collection_uuid, to_parent_uuid = ?info.to_parent_uuid, "[ThingMovedHandler] Parsed info");

        let to_collection_uuid = info.to_collection_uuid.trim();
        if to_collection_uuid.is_empty() {
            return Ok(json!({
                "confirmed": false,                "error": "to_collection_uuid is required",
            }));
        }

        let snapshot_json = match self.sdk.things_list_snapshot_json(&self.device_id) {
            Ok(v) => v,
            Err(e) => {
                return Ok(json!({
                    "confirmed": false,                    "uuid": info.uuid,
                    "error": format!("Failed to read local things snapshot: {e}"),
                }));
            }
        };

        let snapshot: ThingsSnapshot = match serde_json::from_str(&snapshot_json) {
            Ok(v) => v,
            Err(e) => {
                return Ok(json!({
                    "confirmed": false,                    "uuid": info.uuid,
                    "error": format!("Failed to parse local things snapshot: {e}"),
                }));
            }
        };

        let existing = match snapshot.things.iter().find(|t| t.uuid == info.uuid) {
            Some(t) => t,
            None => {
                tracing::warn!(uuid = %info.uuid, "[ThingMovedHandler] Thing not found in local snapshot");
                return Ok(json!({
                    "confirmed": false,                    "uuid": info.uuid,
                    "error": "Thing not found in local snapshot",
                }));
            }
        };
        tracing::info!(existing_uuid = %existing.uuid, existing_title = %existing.title, "[ThingMovedHandler] Found existing thing");

        let parent_uuid = info
            .to_parent_uuid
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        // Perform move as an upsert that preserves title/datatype, and only changes collection/parent.
        // We intentionally omit `data` so content is not rewritten.
        let mut thing_obj = serde_json::Map::new();
        thing_obj.insert("uuid".to_string(), json!(existing.uuid));
        thing_obj.insert("title".to_string(), json!(existing.title));
        thing_obj.insert("datatype".to_string(), json!(existing.datatype.to_string()));
        thing_obj.insert(
            "collection_uuid".to_string(),
            json!(to_collection_uuid.to_string()),
        );
        if let Some(ref p) = parent_uuid {
            thing_obj.insert("parent_uuid".to_string(), json!(p));
        } else {
            // Explicitly clear parent when caller passed empty/None.
            thing_obj.insert("parent_uuid".to_string(), json!(""));
        }

        let thing_json = JsonValue::Object(thing_obj);
        tracing::info!(thing_json = %thing_json, "[ThingMovedHandler] Calling things_upsert_thing_json");

        if let Err(e) = self
            .sdk
            .things_upsert_thing_json(&self.device_id, &thing_json.to_string())
        {
            tracing::error!(error = %e, "[ThingMovedHandler] Failed to upsert");
            return Ok(json!({
                "confirmed": false,                "uuid": info.uuid,
                "error": format!("Failed to apply move via upsert: {e}"),
            }));
        }

        tracing::info!(uuid = %existing.uuid, "[ThingMovedHandler] Thing moved successfully");

        Ok(json!({
            "confirmed": true,            "uuid": existing.uuid,
            "to_collection_uuid": to_collection_uuid,
            "to_parent_uuid": parent_uuid,
        }))
    }
}

/// Handler for events_retrieve_request
pub struct EventsRetrieveRequestHandler {
    sdk: Arc<TriggerSdk>,
}

impl EventsRetrieveRequestHandler {
    pub fn new(sdk: Arc<TriggerSdk>) -> Self {
        Self { sdk }
    }
}

impl InterruptHandler for EventsRetrieveRequestHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "events_retrieve_request");
        let req: EventsRetrieveRequest = match serde_json::from_value(data) {
            Ok(v) => v,
            Err(e) => {
                return Ok(
                    json!({                    "error": format!("Failed to parse events_retrieve_request: {e}"),
                        "events": [],
                    }),
                );
            }
        };

        match self
            .sdk
            .events_list_between_json(&req.start_time, &req.end_time)
        {
            Ok(events_json) => {
                let events: JsonValue =
                    serde_json::from_str(&events_json).unwrap_or(JsonValue::String(events_json));
                Ok(json!({                    "events": events,
                }))
            }
            Err(e) => Ok(
                json!({                "error": format!("Failed to list events: {e}"),
                    "events": [],
                }),
            ),
        }
    }
}

/// Handler for events_abstract_request
pub struct EventsAbstractRequestHandler {
    sdk: Arc<TriggerSdk>,
}

impl EventsAbstractRequestHandler {
    pub fn new(sdk: Arc<TriggerSdk>) -> Self {
        Self { sdk }
    }
}

impl InterruptHandler for EventsAbstractRequestHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "events_abstract_request");
        let req: EventsAbstractRequest =
            serde_json::from_value(data).unwrap_or(EventsAbstractRequest { top_n: None });

        let top_n: u32 = match req.top_n {
            Some(JsonValue::Number(n)) => n.as_u64().unwrap_or(3) as u32,
            Some(JsonValue::String(s)) => s.trim().parse::<u32>().unwrap_or(3),
            Some(JsonValue::Bool(b)) => {
                if b {
                    1
                } else {
                    3
                }
            }
            Some(_) => 3,
            None => 3,
        };

        match self.sdk.events_abstract_json(top_n) {
            Ok(summary_json) => {
                let summary: JsonValue =
                    serde_json::from_str(&summary_json).unwrap_or(JsonValue::String(summary_json));
                // Return the summary directly - interrupt_id is added by the registry
                // as the key in the resume map, not as a field inside the value.
                Ok(summary)
            }
            Err(e) => Ok(
                json!({                "error": format!("Failed to abstract events: {e}"),
                    "hours": [],
                    "top_n": top_n,
                }),
            ),
        }
    }
}

/// Handler for things_list_snapshot_request - returns current things state
pub struct ThingsListSnapshotHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThingsListSnapshotRequest {
    #[serde(default)]
    pub entity_type: Option<String>,
    #[serde(default)]
    pub include_content: Option<bool>,
}

impl ThingsListSnapshotHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for ThingsListSnapshotHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "things_list_snapshot_request");
        let req: ThingsListSnapshotRequest = serde_json::from_value(data).unwrap_or_default();

        let entity_type = req
            .entity_type
            .unwrap_or_else(|| "all".to_string())
            .trim()
            .to_lowercase();
        let include_things = !matches!(entity_type.as_str(), "collection" | "collections");
        let include_content = req.include_content.unwrap_or(false);

        let snapshot_json = self
            .sdk
            .things_list_snapshot_json_with_options(
                &self.device_id,
                include_things,
                crate::things_crdt::SnapshotOptions { include_content },
            )
            .map_err(|e| format!("Failed to get snapshot: {}", e))?;

        let snapshot: JsonValue = serde_json::from_str(&snapshot_json)
            .map_err(|e| format!("Failed to parse snapshot: {}", e))?;

        tracing::info!("Things list snapshot provided via interrupt");

        Ok(json!({            "things_list_snapshot": snapshot,
        }))
    }
}

/// Payload for things_get_thing_markdown_request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingsGetThingMarkdownRequest {
    pub uuid: String,
}

/// Handler for things_get_thing_markdown_request - returns markdown for a single thing
pub struct ThingsGetThingMarkdownHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl ThingsGetThingMarkdownHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for ThingsGetThingMarkdownHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "things_get_thing_markdown_request");
        let req: ThingsGetThingMarkdownRequest = serde_json::from_value(data)
            .map_err(|e| format!("Failed to parse things_get_thing_markdown_request: {e}"))?;

        let markdown = self
            .sdk
            .things_get_thing_markdown(&self.device_id, &req.uuid)
            .map_err(|e| format!("Failed to get thing markdown: {e}"))?
            .unwrap_or_default();

        let content_entries_json = self
            .sdk
            .things_get_content_entries(&self.device_id, &req.uuid)
            .unwrap_or_else(|_| "[]".to_string());
        let content_entries: JsonValue =
            serde_json::from_str(&content_entries_json).unwrap_or(JsonValue::Array(vec![]));

        Ok(json!({
            "thing_markdown": {
                "uuid": req.uuid,
                "markdown": markdown,
                "content_entries": content_entries,
            }
        }))
    }
}

// ══════════════════════════════════════════════════════════════════════════════�?
// Trigger Rule Published Handler
// ══════════════════════════════════════════════════════════════════════════════�?

/// Payload for trigger_rule_published - matches agent's interrupt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerRulePublished {
    pub trigger_uuid: String,
    pub name: String,
    /// The trigger config JSON containing name, precondition, condition arrays
    pub rule_config_json: JsonValue,
    #[serde(default)]
    pub user_request: Option<String>,
    #[serde(default)]
    pub event_analysis: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    pub bind_uuid: String,
    pub bind_type: String, // "thing" or "collection"
    #[serde(default)]
    pub version: Option<JsonValue>,
}

/// Handler for trigger_rule_published - installs trigger locally and binds to entity
pub struct TriggerRulePublishedHandler {
    sdk: Arc<TriggerSdk>,
    #[allow(dead_code)]
    device_id: String,
}

impl TriggerRulePublishedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for TriggerRulePublishedHandler {
    fn handle(&self, interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        tracing::debug!(
            interrupt_id = %interrupt_id,
            payload = %payload,
            "[TriggerRulePublishedHandler] Received interrupt"
        );
        let data = extract_interrupt_data(payload, "trigger_rule_published");
        let info: TriggerRulePublished = serde_json::from_value(data)
            .map_err(|e| format!("Failed to parse trigger_rule_published: {}", e))?;

        let trigger_uuid = normalize_required_uuid(&info.trigger_uuid, "trigger_uuid")?;
        let bind_uuid = normalize_required_uuid(&info.bind_uuid, "bind_uuid")?;
        let bind_type = info.bind_type.trim().to_lowercase();

        tracing::info!(
            trigger_uuid = %trigger_uuid,
            bind_uuid = %bind_uuid,
            bind_type = %bind_type,
            name = %info.name,
            "[TriggerRulePublishedHandler] Parsed trigger rule info"
        );

        // Parse the rule_config_json to extract precondition and condition arrays.
        // The backend may send this as either a JSON object or a JSON string.
        let rule_config: JsonValue = match info.rule_config_json {
            JsonValue::String(s) => serde_json::from_str(&s).unwrap_or_else(|_| json!({})),
            other => other,
        };

        // Extract precondition array
        let precondition: Vec<TriggerRule> = rule_config
            .get("precondition")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        // Extract condition array
        let condition: Vec<TriggerRule> = rule_config
            .get("condition")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        // Build TriggerRegistration
        let version = match info.version {
            Some(JsonValue::String(s)) if !s.trim().is_empty() => s,
            Some(JsonValue::Number(n)) => n.to_string(),
            Some(JsonValue::Bool(b)) => b.to_string(),
            Some(v) => v.to_string(),
            None => "1.0".to_string(),
        };

        let registration = TriggerRegistration {
            trigger_uuid: trigger_uuid.clone(),
            name: info.name.clone(),
            version,
            precondition,
            condition,
        };

        // Bind first; if binding fails we must not register or install the trigger.
        let entity_type = match bind_type.as_str() {
            "thing" => "thing",
            "collection" => "collection",
            other => {
                return Err(format!("Invalid bind_type: {}", other));
            }
        };

        let existing_trigger_uuid = match entity_type {
            "collection" => {
                let snapshot = load_things_snapshot(&self.sdk, &self.device_id, false)?;
                let collection_uuids: Vec<&str> = snapshot
                    .collections
                    .iter()
                    .map(|c| c.uuid.as_str())
                    .collect();
                tracing::info!(
                    bind_uuid = %bind_uuid,
                    existing_collections = ?collection_uuids,
                    "[TriggerRulePublishedHandler] Looking for collection in snapshot"
                );
                let collection = snapshot
                    .collections
                    .iter()
                    .find(|c| c.uuid == bind_uuid)
                    .ok_or_else(|| format!("Collection not found: {bind_uuid}"))?;
                collection.trigger_uuid.clone()
            }
            "thing" => {
                let snapshot = load_things_snapshot(&self.sdk, &self.device_id, true)?;
                let thing_uuids: Vec<&str> =
                    snapshot.things.iter().map(|t| t.uuid.as_str()).collect();
                tracing::info!(
                    bind_uuid = %bind_uuid,
                    existing_things = ?thing_uuids,
                    "[TriggerRulePublishedHandler] Looking for thing in snapshot"
                );
                let thing = snapshot
                    .things
                    .iter()
                    .find(|t| t.uuid == bind_uuid)
                    .ok_or_else(|| format!("Thing not found: {bind_uuid}"))?;
                thing.trigger_uuid.clone()
            }
            _ => unreachable!(),
        };

        // Fallback: if the CRDT snapshot has no trigger_uuid for this entity, check the
        // trigger_bindings table. This handles edge cases where the CRDT may be out of sync
        // (e.g., after migration, or the binding was set through a non-CRDT path). Without
        // this, the old trigger registration would survive in the `triggers` table and keep
        // firing after the entity is rebound.
        let existing_trigger_uuid = if existing_trigger_uuid.is_none() {
            match self.sdk.get_trigger_for_entity(entity_type, &bind_uuid) {
                Ok(found) => {
                    if found.is_some() {
                        tracing::info!(
                            bind_uuid = %bind_uuid,
                            entity_type = %entity_type,
                            old_trigger_uuid = ?found,
                            "[TriggerRulePublishedHandler] CRDT had no trigger_uuid; \
                             found stale binding in trigger_bindings table"
                        );
                    }
                    found
                }
                Err(e) => {
                    tracing::warn!(
                        "[TriggerRulePublishedHandler] Failed to query trigger_bindings \
                         for entity {} {}: {}",
                        entity_type,
                        bind_uuid,
                        e
                    );
                    None
                }
            }
        } else {
            existing_trigger_uuid
        };

        let bind_result = match entity_type {
            "collection" => self.sdk.things_set_collection_trigger_uuid(
                &self.device_id,
                &bind_uuid,
                Some(&trigger_uuid),
            ),
            "thing" => self.sdk.things_set_thing_trigger_uuid(
                &self.device_id,
                &bind_uuid,
                Some(&trigger_uuid),
            ),
            _ => unreachable!(),
        };

        if let Err(e) = bind_result {
            tracing::warn!(
                "Failed to bind trigger to {} {}: {}",
                entity_type,
                bind_uuid,
                e
            );
            return Err(format!(
                "Failed to bind trigger to {} {}: {}",
                entity_type, bind_uuid, e
            ));
        }

        // Register the trigger locally after the binding succeeds.
        if let Err(e) = self.sdk.register_trigger(registration) {
            tracing::error!("Failed to register trigger {}: {}", info.name, e);

            // Best-effort rollback to clear the binding if registration fails.
            // Use Some("") (TriggerUpdate::Clear) — None is TriggerUpdate::Noop and skips the write.
            let rollback_result = match entity_type {
                "collection" => self.sdk.things_set_collection_trigger_uuid(
                    &self.device_id,
                    &bind_uuid,
                    Some(""),
                ),
                "thing" => {
                    self.sdk
                        .things_set_thing_trigger_uuid(&self.device_id, &bind_uuid, Some(""))
                }
                _ => unreachable!(),
            };
            if let Err(rollback_error) = rollback_result {
                tracing::warn!(
                    "Failed to rollback trigger binding for {} {}: {}",
                    entity_type,
                    bind_uuid,
                    rollback_error
                );
            }

            return Err(format!("Failed to register trigger: {}", e));
        }

        tracing::info!(
            "Trigger {} ({}) registered locally via interrupt",
            info.name,
            trigger_uuid
        );

        // Keep trigger_bindings table consistent for scheduling/analytics.
        if let Err(e) = self
            .sdk
            .upsert_trigger_binding(&trigger_uuid, entity_type, &bind_uuid)
        {
            tracing::warn!(
                "Failed to upsert trigger binding {} -> {} {}: {}",
                trigger_uuid,
                entity_type,
                bind_uuid,
                e
            );
        }

        if let Some(old_trigger_uuid) = existing_trigger_uuid {
            if old_trigger_uuid != trigger_uuid {
                if let Err(e) = self.sdk.delete_trigger_if_unbound(&old_trigger_uuid) {
                    tracing::warn!(
                        "Failed to delete old trigger {} after rebinding: {}",
                        old_trigger_uuid,
                        e
                    );
                }
            }
        }

        tracing::info!(
            "Trigger {} bound to {} {}",
            trigger_uuid,
            entity_type,
            bind_uuid
        );

        // Resume value: keep consistent with other handlers.
        Ok(json!({
            "confirmed": true,            "trigger_uuid": trigger_uuid,
            "name": info.name,
            "bound_to": {
                "type": entity_type,
                "uuid": bind_uuid,
            },
        }))
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Trigger Rule Deleted Handler
// ══════════════════════════════════════════════════════════════════════════════

/// Payload for trigger_rule_deleted - matches agent's interrupt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerRuleDeleted {
    pub trigger_uuid: String,
    pub bind_uuid: String,
    pub bind_type: String, // "thing" or "collection"
}

/// Handler for trigger_rule_deleted - unbinds and removes a trigger locally
pub struct TriggerRuleDeletedHandler {
    sdk: Arc<TriggerSdk>,
    device_id: String,
}

impl TriggerRuleDeletedHandler {
    pub fn new(sdk: Arc<TriggerSdk>, device_id: String) -> Self {
        Self { sdk, device_id }
    }
}

impl InterruptHandler for TriggerRuleDeletedHandler {
    fn handle(&self, interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        tracing::debug!(
            interrupt_id = %interrupt_id,
            payload = %payload,
            "[TriggerRuleDeletedHandler] Received interrupt"
        );
        let data = extract_interrupt_data(payload, "trigger_rule_deleted");
        let info: TriggerRuleDeleted = serde_json::from_value(data)
            .map_err(|e| format!("Failed to parse trigger_rule_deleted: {}", e))?;

        let trigger_uuid = normalize_required_uuid(&info.trigger_uuid, "trigger_uuid")?;
        let bind_uuid = normalize_required_uuid(&info.bind_uuid, "bind_uuid")?;
        let bind_type = info.bind_type.trim().to_lowercase();

        if bind_type != "thing" && bind_type != "collection" {
            return Err(format!(
                "Invalid bind_type '{}'; expected 'thing' or 'collection'",
                bind_type
            ));
        }

        tracing::info!(
            trigger_uuid = %trigger_uuid,
            bind_uuid = %bind_uuid,
            bind_type = %bind_type,
            "[TriggerRuleDeletedHandler] Deleting trigger"
        );

        // delete_trigger_and_bindings clears the CRDT on every bound entity,
        // removes all trigger_bindings rows, and force-deletes the trigger record.
        let deleted = self
            .sdk
            .delete_trigger_and_bindings(&self.device_id, &trigger_uuid)
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "delete_trigger_and_bindings failed for {}: {}",
                    trigger_uuid,
                    e
                );
                false
            });

        tracing::info!(
            trigger_uuid = %trigger_uuid,
            deleted = %deleted,
            "Trigger deleted via interrupt"
        );

        Ok(json!({
            "confirmed": true,
            "trigger_uuid": trigger_uuid,
            "deleted": deleted,
            "unbound_from": {
                "type": bind_type,
                "uuid": bind_uuid,
            },
        }))
    }
}

// ══════════════════════════════════════════════════════════════════════════════�?
// Registry Builder Helper
// ══════════════════════════════════════════════════════════════════════════════�?

use crate::interrupt_handler::InterruptHandlerRegistry;

// ══════════════════════════════════════════════════════════════════════════════
// Triggers List Handler
// ══════════════════════════════════════════════════════════════════════════════

/// Handler for triggers_list_request - returns local triggers snapshot
pub struct TriggersListHandler {
    sdk: Arc<TriggerSdk>,
}

impl TriggersListHandler {
    pub fn new(sdk: Arc<TriggerSdk>) -> Self {
        Self { sdk }
    }
}

impl InterruptHandler for TriggersListHandler {
    fn handle(&self, _interrupt_id: &str, _payload: &JsonValue) -> Result<JsonValue, String> {
        let json_str = self
            .sdk
            .list_triggers_json()
            .map_err(|e| format!("Failed to list triggers: {e}"))?;

        let triggers: JsonValue = serde_json::from_str(&json_str)
            .map_err(|e| format!("Failed to parse triggers JSON: {e}"))?;

        tracing::info!("Triggers list provided via interrupt");

        Ok(json!({ "triggers": triggers }))
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Trigger Test Request Handler
// ══════════════════════════════════════════════════════════════════════════════

/// Payload for trigger_test_request
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TriggerTestRequest {
    pub trigger_json: String,
    #[serde(default)]
    pub start_iso: Option<String>,
    #[serde(default)]
    pub end_iso: Option<String>,
    #[serde(default)]
    pub manual: bool,
}

/// Handler for trigger_test_request - runs trigger simulation against local events
pub struct TriggerTestRequestHandler {
    sdk: Arc<TriggerSdk>,
}

impl TriggerTestRequestHandler {
    pub fn new(sdk: Arc<TriggerSdk>) -> Self {
        Self { sdk }
    }
}

impl InterruptHandler for TriggerTestRequestHandler {
    fn handle(&self, _interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String> {
        let data = extract_interrupt_data(payload, "trigger_test_request");
        let req: TriggerTestRequest = serde_json::from_value(data)
            .map_err(|e| format!("Failed to parse trigger_test_request: {e}"))?;

        if req.trigger_json.trim().is_empty() {
            return Err("trigger_json is required".to_string());
        }

        let result_json = self
            .sdk
            .trigger_test_json(&req.trigger_json, req.start_iso, req.end_iso, req.manual)
            .map_err(|e| format!("Trigger test failed: {e}"))?;

        let result: JsonValue = serde_json::from_str(&result_json)
            .map_err(|e| format!("Failed to parse trigger test result: {e}"))?;

        tracing::info!("Trigger test completed via interrupt");

        Ok(result)
    }
}

/// Register all built-in things handlers
pub fn register_things_handlers(
    registry: &mut InterruptHandlerRegistry,
    sdk: Arc<TriggerSdk>,
    device_id: &str,
) {
    let device_id = device_id.to_string();

    registry.register(
        "things_collection_added",
        CollectionAddedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_collection_edited",
        CollectionEditedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_collection_removed",
        CollectionRemovedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_thing_added",
        ThingAddedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_thing_edited",
        ThingEditedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_thing_removed",
        ThingRemovedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_thing_content_edit",
        ThingContentEditHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_thing_moved",
        ThingMovedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_list_snapshot_request",
        ThingsListSnapshotHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "things_get_thing_markdown_request",
        ThingsGetThingMarkdownHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "events_retrieve_request",
        EventsRetrieveRequestHandler::new(sdk.clone()),
    );
    registry.register(
        "events_abstract_request",
        EventsAbstractRequestHandler::new(sdk.clone()),
    );
    registry.register(
        "trigger_rule_published",
        TriggerRulePublishedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "trigger_rule_deleted",
        TriggerRuleDeletedHandler::new(sdk.clone(), device_id.clone()),
    );
    registry.register(
        "triggers_list_request",
        TriggersListHandler::new(sdk.clone()),
    );
    registry.register(
        "trigger_test_request",
        TriggerTestRequestHandler::new(sdk.clone()),
    );
}
