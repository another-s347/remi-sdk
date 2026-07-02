use super::*;

fn attrs_string(attrs: Option<&Value>, key: &str) -> Option<String> {
    attrs
        .and_then(Value::as_object)
        .and_then(|map| map.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn collection_type_from_attrs(attrs: Option<&Value>) -> CollectionType {
    attrs_string(attrs, COLLECTION_TYPE_ATTR_KEY)
        .as_deref()
        .and_then(|value| value.parse::<CollectionType>().ok())
        .unwrap_or_default()
}

fn archived_at_from_attrs(attrs: Option<&Value>) -> Option<chrono::DateTime<chrono::Utc>> {
    attrs_string(attrs, ARCHIVED_AT_ATTR_KEY)
        .as_deref()
        .and_then(|value| parse_domain_datetime(value).ok())
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ThingsDomainReader<'a> {
    store: DocumentStoreView<'a>,
}

impl<'a> ThingsDomainReader<'a> {
    pub(super) fn new(store: DocumentStoreView<'a>) -> Self {
        Self { store }
    }

    pub(super) fn collection_view(&self, collection_uuid: &str) -> Result<CollectionDocView> {
        let key = DocumentKey::collection(collection_uuid);
        match self.store.get(&key) {
            Some(state) => extract_collection_doc_view(&state.automerge_doc, collection_uuid),
            None => Ok(CollectionDocView {
                schema_version: CURRENT_SCHEMA_VERSION,
                meta: remi_things_crdt::CollectionMetaView {
                    id: collection_uuid.to_string(),
                    title: String::new(),
                    status: "active".to_string(),
                    edit_clock: remi_things_crdt::view::EditClock::zero(),
                    tombstone: None,
                    trigger: None,
                    attrs: None,
                },
                things: Vec::new(),
            }),
        }
    }

    pub(super) fn thing_markdown_view(&self, thing_uuid: &str) -> Result<ThingMarkdownView> {
        let key = DocumentKey::thing_content(thing_uuid);
        match self.store.get(&key) {
            Some(state) => extract_thing_markdown_view(&state.automerge_doc, thing_uuid),
            None => Ok(ThingMarkdownView {
                schema_version: CURRENT_SCHEMA_VERSION,
                thing_uuid: thing_uuid.to_string(),
                content: None,
            }),
        }
    }

    pub(super) fn thing_content_view(
        &self,
        document_uuid: &str,
        thing_uuid: &str,
    ) -> Result<Option<ThingContentView>> {
        let key = DocumentKey::thing_content(document_uuid);
        let Some(state) = self.store.get(&key) else {
            return Ok(None);
        };

        Ok(Some(extract_thing_content_view(
            &state.automerge_doc,
            document_uuid,
            thing_uuid,
        )?))
    }

    pub(super) fn get_content_entries(
        &self,
        collection_uuid: &str,
        thing_uuid: &str,
    ) -> Result<Vec<ContentEntry>> {
        let key = DocumentKey::collection(collection_uuid);
        let state = self.store.get(&key).context("Collection not found")?;

        let view = extract_collection_doc_view(&state.automerge_doc, collection_uuid)?;
        let collection_deleted = view
            .meta
            .tombstone
            .as_ref()
            .map(|t| t.deleted)
            .unwrap_or(false);
        if collection_deleted {
            anyhow::bail!("Thing not found");
        }

        let thing = view
            .things
            .iter()
            .find(|thing| thing.id == thing_uuid)
            .context("Thing not found")?;

        let thing_deleted = thing.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false);
        if thing_deleted {
            anyhow::bail!("Thing not found");
        }

        Ok(thing.built_in.content_entries.clone())
    }

    pub(super) fn find_thing_collection_uuid(&self, thing_uuid: &str) -> Option<String> {
        for (key, state) in self.store.iter() {
            if key.data_type != CrdtDataType::Collection {
                continue;
            }

            let view = match extract_collection_doc_view(&state.automerge_doc, &key.uuid) {
                Ok(view) => view,
                Err(_) => continue,
            };

            if view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false)
            {
                continue;
            }

            for thing in &view.things {
                let thing_deleted = thing.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false);
                if !thing_deleted && thing.id == thing_uuid {
                    return Some(key.uuid.clone());
                }
            }
        }

        None
    }

    pub(super) fn collection_uuids_from_documents(&self) -> Result<Vec<String>> {
        let mut collection_uuids = Vec::new();

        for (key, state) in self.store.iter() {
            if key.data_type != CrdtDataType::Collection {
                continue;
            }

            let view = extract_collection_doc_view(&state.automerge_doc, &key.uuid)?;
            if view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false)
            {
                continue;
            }

            collection_uuids.push(key.uuid.clone());
        }

        collection_uuids.sort();
        Ok(collection_uuids)
    }

    pub(super) fn active_collection_uuids(&self) -> Result<HashSet<String>> {
        let mut live = HashSet::new();

        for coll_uuid in self.collection_uuids_from_documents()? {
            live.insert(coll_uuid);
        }

        Ok(live)
    }

    pub(super) fn active_thing_uuids(&self) -> Result<HashSet<String>> {
        let live_collections = self.active_collection_uuids()?;
        let mut live_things = HashSet::new();

        for coll_uuid in &live_collections {
            let key = DocumentKey::collection(coll_uuid);
            let Some(state) = self.store.get(&key) else {
                continue;
            };

            let view = extract_collection_doc_view(&state.automerge_doc, coll_uuid)?;
            if view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false)
            {
                continue;
            }

            for thing in &view.things {
                if !thing.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false) {
                    live_things.insert(thing.id.clone());
                }
            }
        }

        Ok(live_things)
    }

    pub(super) fn active_content_document_uuids(&self) -> Result<HashSet<String>> {
        let live_collections = self.active_collection_uuids()?;
        let mut live_docs = HashSet::new();

        for coll_uuid in &live_collections {
            let key = DocumentKey::collection(coll_uuid);
            let Some(state) = self.store.get(&key) else {
                continue;
            };

            let view = extract_collection_doc_view(&state.automerge_doc, coll_uuid)?;
            if view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false)
            {
                continue;
            }

            for thing in &view.things {
                if thing.tombstone.as_ref().map(|t| t.deleted).unwrap_or(false) {
                    continue;
                }

                for entry in self.get_content_entries(coll_uuid, &thing.id)? {
                    match entry.payload {
                        ContentEntryPayload::Markdown { doc_uuid } => {
                            live_docs.insert(doc_uuid);
                        }
                        ContentEntryPayload::JsonObject(field) => {
                            live_docs.insert(field.data_doc_uuid);
                            if let Some(schema_doc_uuid) = field.schema_doc_uuid {
                                live_docs.insert(schema_doc_uuid);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(live_docs)
    }

    pub(super) fn extract_snapshot(&self) -> Result<ThingsSnapshot> {
        self.extract_snapshot_with_options(SnapshotOptions::default())
    }

    pub(super) fn extract_snapshot_with_options(
        &self,
        options: SnapshotOptions,
    ) -> Result<ThingsSnapshot> {
        let mut collections = Vec::new();
        let mut things = Vec::new();
        let content_registry = ContentTypeRegistry::new();

        let mut collection_views = Vec::new();
        for (key, state) in self.store.iter() {
            if key.data_type != CrdtDataType::Collection {
                continue;
            }

            let coll_view = extract_collection_doc_view(&state.automerge_doc, &key.uuid)?;
            let deleted = coll_view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false);
            if deleted {
                continue;
            }

            collection_views.push((key.uuid.clone(), coll_view));
        }
        collection_views.sort_by(|(left, _), (right, _)| left.cmp(right));

        for (coll_uuid, coll_view) in collection_views {
            let deleted = coll_view
                .meta
                .tombstone
                .as_ref()
                .map(|t| t.deleted)
                .unwrap_or(false);
            if deleted {
                continue;
            }

            let trigger_uuid = desired_trigger_uuid(deleted, &coll_view.meta.trigger);
            let collection_timestamps = extract_entity_timestamps(coll_view.meta.attrs.as_ref());
            let collection_attrs = coll_view.meta.attrs.as_ref();
            collections.push(ThingCollectionEntry {
                uuid: coll_view.meta.id.clone(),
                title: coll_view.meta.title.clone(),
                collection_type: collection_type_from_attrs(collection_attrs),
                app_id: attrs_string(collection_attrs, COLLECTION_APP_ID_ATTR_KEY),
                archived_at: archived_at_from_attrs(collection_attrs),
                trigger_uuid,
                card_jsx: extract_collection_card_jsx(coll_view.meta.attrs.as_ref()),
                created_at: parse_domain_datetime_or_unix_epoch(
                    &collection_timestamps.created_at.unwrap_or_default(),
                ),
                updated_at: parse_domain_datetime_or_unix_epoch(
                    &collection_timestamps.updated_at.unwrap_or_default(),
                ),
                actor_type: None,
                actor_app_id: None,
                actor_display_name: None,
            });

            for thing_meta in &coll_view.things {
                let thing_deleted = thing_meta
                    .tombstone
                    .as_ref()
                    .map(|t| t.deleted)
                    .unwrap_or(false);
                if thing_deleted {
                    continue;
                }

                let thing_trigger = desired_trigger_uuid(thing_deleted, &thing_meta.trigger);
                let thing_timestamps = extract_entity_timestamps(thing_meta.attrs.as_ref());
                let thing_attrs = thing_meta.attrs.as_ref();
                let content = if options.include_content {
                    let md_view = self.thing_markdown_view(&thing_meta.id)?;
                    md_view.content
                } else {
                    None
                };
                let data = content_registry.serialize_thing_snapshot_data(
                    thing_meta,
                    content.as_ref(),
                    options,
                );

                things.push(ThingEntry {
                    uuid: thing_meta.id.clone(),
                    title: thing_meta.title.clone().unwrap_or_default(),
                    datatype: thing_meta.datatype.clone(),
                    data,
                    collection_uuid: coll_uuid.clone(),
                    trigger_uuid: thing_trigger,
                    parent_uuid: thing_meta.parent_id.clone(),
                    archived_at: archived_at_from_attrs(thing_attrs),
                    archived_from_collection_uuid: attrs_string(
                        thing_attrs,
                        ARCHIVED_FROM_COLLECTION_UUID_ATTR_KEY,
                    ),
                    created_at: parse_domain_datetime_or_unix_epoch(
                        &thing_timestamps.created_at.unwrap_or_default(),
                    ),
                    updated_at: parse_domain_datetime_or_unix_epoch(
                        &thing_timestamps.updated_at.unwrap_or_default(),
                    ),
                    status: thing_meta.status.as_storage_str().to_string(),
                    status_timestamp_ms: thing_meta.status.timestamp_ms(),
                    actor_type: None,
                    actor_app_id: None,
                    actor_display_name: None,
                });
            }
        }

        Ok(ThingsSnapshot {
            collections,
            things,
        })
    }
}
