use super::*;

impl<'a> ThingsLocalService<'a> {
    pub fn set_collection_action_bindings(
        &self,
        device_id: &str,
        collection_uuid: &str,
        bindings: &[EntityActionBinding],
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context(
                "Failed to load Things document set before collection action binding update",
            )?;
        let collection = doc_set.collection_view(collection_uuid)?;
        let attrs = encode_entity_action_bindings(collection.meta.attrs.as_ref(), bindings);
        let events = doc_set.update_collection_attrs(collection_uuid, Some(attrs))?;
        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, ())
    }

    pub fn list_collection_action_bindings(
        &self,
        device_id: &str,
        collection_uuid: &str,
    ) -> Result<Vec<EntityActionBinding>> {
        let doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before listing collection bindings")?;
        let collection = doc_set.collection_view(collection_uuid)?;
        Ok(decode_entity_action_bindings(
            collection.meta.attrs.as_ref(),
        ))
    }

    pub fn get_collection_card_jsx(
        &self,
        device_id: &str,
        collection_uuid: &str,
    ) -> Result<Option<String>> {
        let doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before reading collection card jsx")?;
        let collection = doc_set.collection_view(collection_uuid)?;
        Ok(decode_collection_card_jsx(collection.meta.attrs.as_ref()))
    }

    pub fn set_collection_card_jsx(
        &self,
        device_id: &str,
        collection_uuid: &str,
        card_jsx: Option<&str>,
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before collection card jsx update")?;
        let collection = doc_set.collection_view(collection_uuid)?;
        let attrs = encode_collection_card_jsx(collection.meta.attrs.as_ref(), card_jsx);
        let events = doc_set.update_collection_attrs(collection_uuid, Some(attrs))?;
        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, ())
    }

    pub fn set_thing_action_bindings(
        &self,
        device_id: &str,
        thing_uuid: &str,
        bindings: &[EntityActionBinding],
    ) -> Result<ThingsMutationResult<()>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before thing action binding update")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let collection = doc_set.collection_view(&collection_uuid)?;
        let thing = collection
            .things
            .iter()
            .find(|item| item.id == thing_uuid)
            .ok_or_else(|| anyhow!("Thing not found: {thing_uuid}"))?;
        let attrs = encode_entity_action_bindings(thing.attrs.as_ref(), bindings);
        let events = doc_set.update_thing_attrs(&collection_uuid, thing_uuid, Some(attrs))?;
        let context = ThingsMutationContext::local_command(device_id);
        self.pipeline
            .commit_local_documents(&context, &mut doc_set, events, ())
    }

    pub fn list_thing_action_bindings(
        &self,
        device_id: &str,
        thing_uuid: &str,
    ) -> Result<Vec<EntityActionBinding>> {
        let doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(device_id)
            .context("Failed to load Things document set before listing thing bindings")?;
        let collection_uuid = resolve_thing_collection_uuid(&doc_set, thing_uuid)?;
        let collection = doc_set.collection_view(&collection_uuid)?;
        let thing = collection
            .things
            .into_iter()
            .find(|item| item.id == thing_uuid)
            .ok_or_else(|| anyhow!("Thing not found: {thing_uuid}"))?;
        Ok(decode_entity_action_bindings(thing.attrs.as_ref()))
    }
}

fn strip_internal_entity_attrs(attrs: Option<&Value>) -> Value {
    let mut map = match attrs {
        Some(Value::Object(existing)) => existing.clone(),
        _ => serde_json::Map::new(),
    };
    map.remove("__remi_created_at");
    map.remove("__remi_updated_at");
    Value::Object(map)
}

fn decode_entity_action_bindings(attrs: Option<&Value>) -> Vec<EntityActionBinding> {
    attrs
        .and_then(|value| value.as_object())
        .and_then(|map| map.get(ENTITY_ACTION_BINDINGS_ATTR_KEY))
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<EntityActionBinding>>(value).ok())
        .unwrap_or_default()
}

fn encode_entity_action_bindings(attrs: Option<&Value>, bindings: &[EntityActionBinding]) -> Value {
    let mut root = match strip_internal_entity_attrs(attrs) {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };

    if bindings.is_empty() {
        root.remove(ENTITY_ACTION_BINDINGS_ATTR_KEY);
    } else {
        root.insert(
            ENTITY_ACTION_BINDINGS_ATTR_KEY.to_string(),
            serde_json::to_value(bindings).unwrap_or_else(|_| Value::Array(Vec::new())),
        );
    }

    Value::Object(root)
}

fn encode_collection_card_jsx(attrs: Option<&Value>, card_jsx: Option<&str>) -> Value {
    let mut root = match strip_internal_entity_attrs(attrs) {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };

    if let Some(card_jsx) = card_jsx.map(str::trim).filter(|value| !value.is_empty()) {
        root.insert(
            COLLECTION_CARD_JSX_ATTR_KEY.to_string(),
            Value::String(card_jsx.to_string()),
        );
    } else {
        root.remove(COLLECTION_CARD_JSX_ATTR_KEY);
    }

    Value::Object(root)
}

fn decode_collection_card_jsx(attrs: Option<&Value>) -> Option<String> {
    attrs
        .and_then(|value| value.as_object())
        .and_then(|map| map.get(COLLECTION_CARD_JSX_ATTR_KEY))
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
}
