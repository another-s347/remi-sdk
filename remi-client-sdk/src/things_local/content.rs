use super::*;

impl<'a> ThingsLocalService<'a> {
    pub fn list_content_entries(
        &self,
        device_id: &str,
        thing_uuid: &str,
    ) -> Result<Vec<ContentEntry>> {
        let doc_set = self.load_document_set(device_id)?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        doc_set.get_content_entries(&collection_uuid, thing_uuid)
    }

    pub fn read_json_object_entry_data(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<Option<Value>> {
        let doc_set = self.load_document_set(device_id)?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let entries = doc_set.get_content_entries(&collection_uuid, thing_uuid)?;
        let field = resolve_json_object_entry_field(&entries, entry_id)?;
        doc_set.get_thing_json_content(&field.data_doc_uuid, thing_uuid)
    }

    pub fn read_json_object_entry_schema(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<Option<Value>> {
        let doc_set = self.load_document_set(device_id)?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let entries = doc_set.get_content_entries(&collection_uuid, thing_uuid)?;
        let field = resolve_json_object_entry_field(&entries, entry_id)?;
        match field.schema_doc_uuid {
            Some(schema_doc_uuid) => doc_set.get_thing_json_content(&schema_doc_uuid, thing_uuid),
            None => Ok(None),
        }
    }

    pub fn add_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry: ContentEntry,
    ) -> Result<ThingsMutationResult<String>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before content entry add")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let id = entry.id.clone();
        let events = doc_set.add_content_entry(&collection_uuid, thing_uuid, entry)?;
        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, id)
    }

    pub fn update_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        update: ContentEntryUpdate,
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before content entry update")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let events = doc_set.update_content_entry(
            &collection_uuid,
            thing_uuid,
            &update.id,
            update.title,
            update.order,
            update.payload,
        )?;
        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, ())
    }

    pub fn delete_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before content entry delete")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let entries = doc_set.get_content_entries(&collection_uuid, thing_uuid)?;
        let json_object_field =
            entries
                .iter()
                .find(|entry| entry.id == entry_id)
                .and_then(|entry| match &entry.payload {
                    ContentEntryPayload::JsonObject(field) => Some(field.clone()),
                    _ => None,
                });

        let mut deleted_documents = Vec::new();
        let events = doc_set.delete_content_entry(&collection_uuid, thing_uuid, entry_id)?;
        if let Some(field) = json_object_field {
            let data_key = DocumentKey::thing_content(&field.data_doc_uuid);
            doc_set.remove_document(&data_key);
            deleted_documents.push(data_key);

            if let Some(schema_doc_uuid) = field.schema_doc_uuid {
                let schema_key = DocumentKey::thing_content(&schema_doc_uuid);
                doc_set.remove_document(&schema_key);
                deleted_documents.push(schema_key);
            }
        }
        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline.commit_local_documents_with_deletions(
            &context,
            &mut doc_set,
            deleted_documents,
            events,
            (),
        )
    }

    pub fn add_json_object_content_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        title: Option<&str>,
        data: Option<&Value>,
        schema: Option<&Value>,
    ) -> Result<ThingsMutationResult<String>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before json object entry add")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let entries = doc_set.get_content_entries(&collection_uuid, thing_uuid)?;
        let order = entries
            .iter()
            .map(|entry| entry.order)
            .fold(-1.0_f64, f64::max)
            + 1.0;

        let data_value = data.cloned().unwrap_or_else(|| json!({}));
        validate_json_object_data(schema, &data_value)?;

        let data_doc_uuid = uuid::Uuid::new_v4().to_string();
        doc_set.set_thing_json_content(
            &data_doc_uuid,
            thing_uuid,
            "json_object_data",
            &data_value,
        )?;

        let schema_doc_uuid = if let Some(schema_value) = schema {
            let schema_doc_uuid = uuid::Uuid::new_v4().to_string();
            doc_set.set_thing_json_content(
                &schema_doc_uuid,
                thing_uuid,
                "json_object_schema",
                schema_value,
            )?;
            Some(schema_doc_uuid)
        } else {
            None
        };

        let entry = ContentEntry {
            id: uuid::Uuid::new_v4().to_string(),
            title: title.map(|value| value.to_string()),
            order,
            payload: ContentEntryPayload::JsonObject(JsonObjectField {
                data_doc_uuid,
                schema_doc_uuid,
            }),
        };

        let entry_id = entry.id.clone();
        let (logged_data_doc_uuid, logged_schema_doc_uuid) = match &entry.payload {
            ContentEntryPayload::JsonObject(field) => {
                (field.data_doc_uuid.clone(), field.schema_doc_uuid.clone())
            }
            _ => unreachable!("json object entry payload must stay json object"),
        };
        let events = doc_set.add_content_entry(&collection_uuid, thing_uuid, entry)?;
        let context = ThingsMutationContext::local_command(device_id);
        let result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, entry_id)?;
        tracing::info!(
            device_id,
            thing_uuid,
            collection_uuid,
            entry_id = %result.value,
            title = title.unwrap_or(""),
            data_doc_uuid = %logged_data_doc_uuid,
            schema_doc_uuid = ?logged_schema_doc_uuid,
            "Created json_object content entry"
        );
        Ok(result)
    }

    pub fn set_json_object_entry_data(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
        data: &Value,
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before json object data update")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let entries = doc_set.get_content_entries(&collection_uuid, thing_uuid)?;
        let field = resolve_json_object_entry_field(&entries, entry_id)?;
        let schema = match &field.schema_doc_uuid {
            Some(schema_doc_uuid) => doc_set.get_thing_json_content(schema_doc_uuid, thing_uuid)?,
            None => None,
        };

        validate_json_object_data(schema.as_ref(), data)?;
        doc_set.set_thing_json_content(
            &field.data_doc_uuid,
            thing_uuid,
            "json_object_data",
            data,
        )?;
        let events = vec![ThingsDocumentEvent::content_entry(
            ThingsDocumentChangeKind::Updated,
            &collection_uuid,
            thing_uuid,
            entry_id,
        )];
        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, ())
    }

    pub fn set_json_object_entry_schema(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
        schema: Option<&Value>,
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before json object schema update")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let entries = doc_set.get_content_entries(&collection_uuid, thing_uuid)?;
        let mut field = resolve_json_object_entry_field(&entries, entry_id)?;
        let mut events = Vec::new();
        let mut deleted_documents = Vec::new();
        let data = doc_set
            .get_thing_json_content(&field.data_doc_uuid, thing_uuid)?
            .unwrap_or_else(|| json!({}));

        validate_json_object_data(schema, &data)?;

        match schema {
            Some(schema_value) => {
                let schema_doc_uuid = field
                    .schema_doc_uuid
                    .clone()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                doc_set.set_thing_json_content(
                    &schema_doc_uuid,
                    thing_uuid,
                    "json_object_schema",
                    schema_value,
                )?;
                if field.schema_doc_uuid.as_deref() != Some(schema_doc_uuid.as_str()) {
                    field.schema_doc_uuid = Some(schema_doc_uuid);
                    events.extend(doc_set.update_content_entry(
                        &collection_uuid,
                        thing_uuid,
                        entry_id,
                        None,
                        None,
                        Some(ContentEntryPayload::JsonObject(field)),
                    )?);
                }
            }
            None => {
                if let Some(schema_doc_uuid) = field.schema_doc_uuid.take() {
                    let schema_key = DocumentKey::thing_content(&schema_doc_uuid);
                    doc_set.remove_document(&schema_key);
                    deleted_documents.push(schema_key);
                    events.extend(doc_set.update_content_entry(
                        &collection_uuid,
                        thing_uuid,
                        entry_id,
                        None,
                        None,
                        Some(ContentEntryPayload::JsonObject(field)),
                    )?);
                }
            }
        }

        events.push(ThingsDocumentEvent::content_entry(
            ThingsDocumentChangeKind::Updated,
            &collection_uuid,
            thing_uuid,
            entry_id,
        ));
        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline.commit_local_documents_with_deletions(
            &context,
            &mut doc_set,
            deleted_documents,
            events,
            (),
        )
    }

    pub fn update_json_object_entry(
        &self,
        device_id: &str,
        thing_uuid: &str,
        entry_id: &str,
        title: Option<Option<String>>,
        data: &Value,
        schema: Option<&Value>,
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before json object entry update")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let entries = doc_set.get_content_entries(&collection_uuid, thing_uuid)?;
        let mut field = resolve_json_object_entry_field(&entries, entry_id)?;

        validate_json_object_data(schema, data)?;

        let mut payload_changed = false;
        let mut deleted_documents = Vec::new();

        match schema {
            Some(schema_value) => {
                let schema_doc_uuid = field
                    .schema_doc_uuid
                    .clone()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                doc_set.set_thing_json_content(
                    &schema_doc_uuid,
                    thing_uuid,
                    "json_object_schema",
                    schema_value,
                )?;
                if field.schema_doc_uuid.as_deref() != Some(schema_doc_uuid.as_str()) {
                    field.schema_doc_uuid = Some(schema_doc_uuid);
                    payload_changed = true;
                }
            }
            None => {
                if let Some(schema_doc_uuid) = field.schema_doc_uuid.take() {
                    let schema_key = DocumentKey::thing_content(&schema_doc_uuid);
                    doc_set.remove_document(&schema_key);
                    deleted_documents.push(schema_key);
                    payload_changed = true;
                }
            }
        }

        doc_set.set_thing_json_content(
            &field.data_doc_uuid,
            thing_uuid,
            "json_object_data",
            data,
        )?;

        let title_changed = title.is_some();
        let logged_data_doc_uuid = field.data_doc_uuid.clone();
        let logged_schema_doc_uuid = field.schema_doc_uuid.clone();

        let mut events = if title.is_some() || payload_changed {
            doc_set.update_content_entry(
                &collection_uuid,
                thing_uuid,
                entry_id,
                title,
                None,
                payload_changed.then_some(ContentEntryPayload::JsonObject(field)),
            )?
        } else {
            Vec::new()
        };

        if events.is_empty() {
            events.push(ThingsDocumentEvent::content_entry(
                ThingsDocumentChangeKind::Updated,
                &collection_uuid,
                thing_uuid,
                entry_id,
            ));
        }
        let context = ThingsMutationContext::local_command(device_id);
        let result = self.pipeline.commit_local_documents_with_deletions(
            &context,
            &mut doc_set,
            deleted_documents,
            events,
            (),
        )?;
        tracing::info!(
            device_id,
            thing_uuid,
            collection_uuid,
            entry_id,
            data_doc_uuid = %logged_data_doc_uuid,
            schema_doc_uuid = ?logged_schema_doc_uuid,
            title_changed,
            payload_changed,
            "Updated json_object content entry"
        );
        Ok(result)
    }
}

fn resolve_json_object_entry_field(
    entries: &[ContentEntry],
    entry_id: &str,
) -> Result<JsonObjectField> {
    let entry = entries
        .iter()
        .find(|entry| entry.id == entry_id)
        .ok_or_else(|| anyhow!("Content entry not found: {}", entry_id))?;
    match &entry.payload {
        ContentEntryPayload::JsonObject(field) => Ok(field.clone()),
        other => anyhow::bail!(
            "Content entry '{}' is not a json_object entry (found {:?})",
            entry_id,
            other.kind()
        ),
    }
}

fn validate_json_object_data(schema: Option<&Value>, data: &Value) -> Result<()> {
    if !data.is_object() {
        anyhow::bail!("json_object data must be a JSON object")
    }

    if let Some(schema) = schema {
        let compiled =
            JSONSchema::compile(schema).map_err(|error| anyhow!("Invalid JSON Schema: {error}"))?;
        let errors = compiled
            .validate(data)
            .map(|_| Vec::new())
            .unwrap_or_else(|errors| errors.map(|error| error.to_string()).collect::<Vec<_>>());
        if !errors.is_empty() {
            anyhow::bail!("JSON object does not satisfy schema: {}", errors.join("; "));
        }
    }

    Ok(())
}
