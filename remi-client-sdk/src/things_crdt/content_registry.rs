use super::*;

#[derive(Debug, Default, Clone, Copy)]
pub struct ContentTypeRegistry;

#[derive(Debug, Clone, PartialEq, Eq)]
enum RegisteredContentType {
    Markdown,
    JsonObject,
    Url,
    Location,
    Date,
    Image,
    Custom(String),
}

impl ContentTypeRegistry {
    pub fn new() -> Self {
        Self
    }

    fn detect_payload_type(&self, value: &Value) -> Result<RegisteredContentType> {
        let payload_type = value
            .get("type")
            .and_then(|entry_type| entry_type.as_str())
            .filter(|entry_type| !entry_type.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("Missing payload type"))?;

        match payload_type {
            "markdown" => Ok(RegisteredContentType::Markdown),
            "json_object" => Ok(RegisteredContentType::JsonObject),
            "url" => Ok(RegisteredContentType::Url),
            "location" => Ok(RegisteredContentType::Location),
            "date" => Ok(RegisteredContentType::Date),
            "image" => Ok(RegisteredContentType::Image),
            other => Ok(RegisteredContentType::Custom(other.to_string())),
        }
    }

    pub fn parse_content_entry(&self, value: &Value) -> Result<ContentEntry> {
        let id = value
            .get("id")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let title = value
            .get("title")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        let order = value
            .get("order")
            .and_then(|item| item.as_f64())
            .unwrap_or(0.0);
        let payload_value = value
            .get("payload")
            .ok_or_else(|| anyhow::anyhow!("Missing payload"))?;

        Ok(ContentEntry {
            id,
            title,
            order,
            payload: self.parse_content_entry_payload(payload_value)?,
        })
    }

    pub fn parse_content_entry_update(
        &self,
        value: &Value,
    ) -> Result<(
        Option<Option<String>>,
        Option<f64>,
        Option<ContentEntryPayload>,
    )> {
        let title = if value.get("title").is_some() {
            Some(
                value
                    .get("title")
                    .and_then(|item| item.as_str())
                    .map(|item| item.to_string()),
            )
        } else {
            None
        };
        let order = value.get("order").and_then(|item| item.as_f64());
        let payload = value
            .get("payload")
            .map(|payload| self.parse_content_entry_payload(payload))
            .transpose()?;

        Ok((title, order, payload))
    }

    pub fn parse_content_entry_payload(&self, value: &Value) -> Result<ContentEntryPayload> {
        match self.detect_payload_type(value)? {
            RegisteredContentType::Markdown => self.parse_markdown_payload(value),
            RegisteredContentType::JsonObject => self.parse_json_object_payload(value),
            RegisteredContentType::Url => self.parse_url_payload(value),
            RegisteredContentType::Location => self.parse_location_payload(value),
            RegisteredContentType::Date => self.parse_date_payload(value),
            RegisteredContentType::Image => self.parse_image_payload(value),
            RegisteredContentType::Custom(content_type) => {
                self.parse_custom_payload(&content_type, value)
            }
        }
    }

    pub fn serialize_content_entry(&self, entry: &ContentEntry) -> Value {
        json!({
            "id": entry.id,
            "title": entry.title,
            "order": entry.order,
            "payload": self.serialize_content_entry_payload(&entry.payload),
        })
    }

    pub fn serialize_content_entries(&self, entries: &[ContentEntry]) -> Value {
        Value::Array(
            entries
                .iter()
                .map(|entry| self.serialize_content_entry(entry))
                .collect(),
        )
    }

    pub fn serialize_thing_built_in(
        &self,
        built_in: &remi_things_crdt::ThingBuiltInFieldsView,
    ) -> Value {
        let mut obj = serde_json::Map::new();
        if !built_in.content_entries.is_empty() {
            obj.insert(
                "content_entries".to_string(),
                self.serialize_content_entries(&built_in.content_entries),
            );
        }
        if let Some(extra) = &built_in.extra {
            obj.insert("extra".to_string(), extra.clone());
        }
        Value::Object(obj)
    }

    pub fn serialize_thing_snapshot_data(
        &self,
        meta: &remi_things_crdt::ThingMetaView,
        content: Option<&remi_things_crdt::view::ContentView>,
        options: SnapshotOptions,
    ) -> Value {
        let mut obj = serde_json::Map::new();
        obj.insert("status".to_string(), json!(meta.status));
        obj.insert("datatype".to_string(), json!(meta.datatype));
        obj.insert(
            "attrs".to_string(),
            json!(strip_internal_timestamp_attrs(meta.attrs.as_ref())),
        );
        if options.include_content {
            obj.insert("content".to_string(), json!(content));
        }
        obj.insert(
            "built_in".to_string(),
            self.serialize_thing_built_in(&meta.built_in),
        );
        Value::Object(obj)
    }

    pub fn find_first_payload_by_kind(
        &self,
        entries: &[ContentEntry],
        kind: &remi_things_crdt::ContentEntryKind,
    ) -> Option<Value> {
        entries
            .iter()
            .find(|entry| &entry.kind() == kind)
            .map(|entry| self.serialize_content_entry_payload(&entry.payload))
    }

    fn parse_markdown_payload(&self, value: &Value) -> Result<ContentEntryPayload> {
        let doc_uuid = value
            .get("doc_uuid")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        Ok(ContentEntryPayload::Markdown { doc_uuid })
    }

    fn parse_json_object_payload(&self, value: &Value) -> Result<ContentEntryPayload> {
        let data_doc_uuid = value
            .get("data_doc_uuid")
            .and_then(|item| item.as_str())
            .filter(|item| !item.trim().is_empty())
            .map(|item| item.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let schema_doc_uuid = value
            .get("schema_doc_uuid")
            .and_then(|item| item.as_str())
            .filter(|item| !item.trim().is_empty())
            .map(|item| item.to_string());
        Ok(ContentEntryPayload::JsonObject(JsonObjectField {
            data_doc_uuid,
            schema_doc_uuid,
        }))
    }

    fn parse_url_payload(&self, value: &Value) -> Result<ContentEntryPayload> {
        let url = value
            .get("url")
            .and_then(|item| item.as_str())
            .unwrap_or("")
            .to_string();
        let title = value
            .get("title")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        let description = value
            .get("description")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        let image_url = value
            .get("image_url")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        let favicon_url = value
            .get("favicon_url")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        let site_name = value
            .get("site_name")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        let resolved = value
            .get("resolved")
            .and_then(|item| item.as_bool())
            .unwrap_or(false);
        Ok(ContentEntryPayload::Url(remi_things_crdt::UrlField {
            url,
            title,
            description,
            image_url,
            favicon_url,
            site_name,
            resolved,
        }))
    }

    fn parse_location_payload(&self, value: &Value) -> Result<ContentEntryPayload> {
        use remi_things_crdt::LocationField;

        let loc_type = value
            .get("loc_type")
            .and_then(|entry_type| entry_type.as_str())
            .unwrap_or("");
        let location = match loc_type {
            "coordinate" => {
                let lat = value
                    .get("lat")
                    .and_then(|item| item.as_f64())
                    .ok_or_else(|| anyhow::anyhow!("Missing lat"))?;
                let lng = value
                    .get("lng")
                    .and_then(|item| item.as_f64())
                    .ok_or_else(|| anyhow::anyhow!("Missing lng"))?;
                let coord_system = value
                    .get("coord_system")
                    .and_then(|item| item.as_str())
                    .unwrap_or("wgs84")
                    .to_string();
                let source_name = value
                    .get("source_name")
                    .and_then(|item| item.as_str())
                    .map(|item| item.to_string());
                LocationField::Coordinate {
                    lat,
                    lng,
                    coord_system,
                    source_name,
                }
            }
            "fuzzy" => {
                let name = value
                    .get("name")
                    .and_then(|item| item.as_str())
                    .unwrap_or("")
                    .to_string();
                let place_type = value
                    .get("place_type")
                    .and_then(|item| item.as_str())
                    .unwrap_or("")
                    .to_string();
                LocationField::Fuzzy { name, place_type }
            }
            _ => anyhow::bail!("Invalid location type: {}", loc_type),
        };

        Ok(ContentEntryPayload::Location(location))
    }

    fn parse_date_payload(&self, value: &Value) -> Result<ContentEntryPayload> {
        use remi_things_crdt::DateField;

        let timestamp_ms = value
            .get("timestamp_ms")
            .and_then(|item| item.as_i64())
            .ok_or_else(|| anyhow::anyhow!("Missing timestamp_ms"))?;
        let has_time = value
            .get("has_time")
            .and_then(|item| item.as_bool())
            .unwrap_or(false);
        let timezone = value
            .get("timezone")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        Ok(ContentEntryPayload::Date(DateField {
            timestamp_ms,
            has_time,
            timezone,
        }))
    }

    fn parse_image_payload(&self, value: &Value) -> Result<ContentEntryPayload> {
        let uri = value
            .get("uri")
            .and_then(|item| item.as_str())
            .unwrap_or("")
            .to_string();
        let caption = value
            .get("caption")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        let width = value
            .get("width")
            .and_then(|item| item.as_u64())
            .map(|item| item as u32);
        let height = value
            .get("height")
            .and_then(|item| item.as_u64())
            .map(|item| item as u32);
        let size_bytes = value.get("size_bytes").and_then(|item| item.as_u64());
        let device_id = value
            .get("device_id")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string());
        Ok(ContentEntryPayload::Image(remi_things_crdt::ImageField {
            uri,
            caption,
            width,
            height,
            size_bytes,
            device_id,
        }))
    }

    // External custom payload contract:
    // - Legacy explicit custom wrapper uses {"type":"custom","data":<json>}.
    // - Future extension types use {"type":"<external-type>", ...}.
    //   For object payloads, every field except `type` is preserved verbatim.
    //   For scalar/array payloads, callers should wrap the value as
    //   {"type":"<external-type>","data":<json>} so round-tripping stays lossless.
    fn parse_custom_payload(
        &self,
        content_type: &str,
        value: &Value,
    ) -> Result<ContentEntryPayload> {
        let data = if content_type == "custom" {
            value.get("data").cloned().unwrap_or(Value::Null)
        } else {
            match value {
                Value::Object(map) => {
                    let mut data = serde_json::Map::new();
                    for (key, field_value) in map {
                        if key != "type" {
                            data.insert(key.clone(), field_value.clone());
                        }
                    }
                    Value::Object(data)
                }
                _ => Value::Null,
            }
        };

        Ok(ContentEntryPayload::Custom {
            content_type: content_type.to_string(),
            data,
        })
    }

    pub fn serialize_content_entry_payload(&self, payload: &ContentEntryPayload) -> Value {
        match payload {
            ContentEntryPayload::Markdown { doc_uuid } => json!({
                "type": "markdown",
                "doc_uuid": doc_uuid,
            }),
            ContentEntryPayload::JsonObject(field) => json!({
                "type": "json_object",
                "data_doc_uuid": field.data_doc_uuid,
                "schema_doc_uuid": field.schema_doc_uuid,
            }),
            ContentEntryPayload::Url(url) => json!({
                "type": "url",
                "url": url.url,
                "title": url.title,
                "description": url.description,
                "image_url": url.image_url,
                "favicon_url": url.favicon_url,
                "site_name": url.site_name,
                "resolved": url.resolved,
            }),
            ContentEntryPayload::Location(location) => self.serialize_location_payload(location),
            ContentEntryPayload::Date(date) => json!({
                "type": "date",
                "timestamp_ms": date.timestamp_ms,
                "has_time": date.has_time,
                "timezone": date.timezone,
            }),
            ContentEntryPayload::Image(image) => json!({
                "type": "image",
                "uri": image.uri,
                "caption": image.caption,
                "width": image.width,
                "height": image.height,
                "size_bytes": image.size_bytes,
                "device_id": image.device_id,
            }),
            ContentEntryPayload::Custom { content_type, data } => {
                self.serialize_custom_payload(content_type, data)
            }
        }
    }

    // Serialize back to the external SDK/API shape described above.
    fn serialize_custom_payload(&self, content_type: &str, data: &Value) -> Value {
        if content_type == "custom" {
            return json!({
                "type": "custom",
                "data": data,
            });
        }

        match data {
            Value::Object(map) => {
                let mut obj = serde_json::Map::new();
                obj.insert("type".to_string(), Value::String(content_type.to_string()));
                for (key, value) in map {
                    obj.insert(key.clone(), value.clone());
                }
                Value::Object(obj)
            }
            other => json!({
                "type": content_type,
                "data": other,
            }),
        }
    }

    fn serialize_location_payload(&self, location: &remi_things_crdt::LocationField) -> Value {
        match location {
            remi_things_crdt::LocationField::Coordinate {
                lat,
                lng,
                coord_system,
                source_name,
            } => json!({
                "type": "location",
                "loc_type": "coordinate",
                "lat": lat,
                "lng": lng,
                "coord_system": coord_system,
                "source_name": source_name,
            }),
            remi_things_crdt::LocationField::Fuzzy { name, place_type } => json!({
                "type": "location",
                "loc_type": "fuzzy",
                "name": name,
                "place_type": place_type,
            }),
        }
    }

    pub fn extract_markdown_text_from_snapshot_data(&self, data: &Value) -> Option<String> {
        if let Some(markdown) = data.get("markdown").and_then(|value| value.as_str()) {
            return Some(markdown.to_string());
        }

        data.get("content")
            .and_then(|content| content.get("blocks"))
            .and_then(|blocks| blocks.as_array())
            .and_then(|blocks| {
                blocks
                    .iter()
                    .find(|block| block.get("id") == Some(&Value::String("main".to_string())))
            })
            .and_then(|block| block.get("text"))
            .and_then(|text| text.as_str())
            .map(|text| text.to_string())
    }

    pub fn extract_thing_snapshot_parts(
        &self,
        data: &Value,
    ) -> Result<(Option<String>, Vec<ContentEntry>)> {
        Ok((
            self.extract_markdown_text_from_snapshot_data(data),
            self.extract_content_entries_from_snapshot_data(data)?,
        ))
    }

    pub fn markdown_content_from_value(
        &self,
        original_datatype: &ThingDatatype,
        payload: &Value,
    ) -> Content {
        if original_datatype.is_markdownish() {
            let text = match payload {
                Value::String(s) => s.clone(),
                Value::Object(map) => map
                    .get("markdown")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| payload.to_string()),
                _ => payload.to_string(),
            };
            return Content::Markdown {
                blocks: vec![remi_things_crdt::Block {
                    id: "main".to_string(),
                    r#type: "markdown".to_string(),
                    attrs_json: None,
                    text: Some(text),
                }],
            };
        }

        let attrs = json!({
            "embed_kind": original_datatype.as_str(),
            "payload": payload,
        })
        .to_string();

        Content::Markdown {
            blocks: vec![remi_things_crdt::Block {
                id: "main".to_string(),
                r#type: original_datatype.to_string(),
                attrs_json: Some(attrs),
                text: None,
            }],
        }
    }

    pub fn extract_content_entries_from_snapshot_data(
        &self,
        data: &Value,
    ) -> Result<Vec<ContentEntry>> {
        let Some(entries) = data
            .get("built_in")
            .and_then(|built_in| built_in.get("content_entries"))
            .and_then(|entries| entries.as_array())
        else {
            return Ok(Vec::new());
        };

        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let payload = entry
                .get("payload")
                .ok_or_else(|| anyhow::anyhow!("Missing payload in stashed content entry"))?;

            out.push(ContentEntry {
                id: entry
                    .get("id")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Missing id in stashed content entry"))?
                    .to_string(),
                title: entry
                    .get("title")
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string()),
                order: entry
                    .get("order")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0),
                payload: self.parse_content_entry_payload(payload)?,
            });
        }

        Ok(out)
    }
}
