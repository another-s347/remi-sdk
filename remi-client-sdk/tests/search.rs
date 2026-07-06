use anyhow::Result;
use chrono::Utc;
use remi_client_sdk::things_crdt::{ThingCollectionUpsert, ThingDatatype, ThingUpsert};
use remi_client_sdk::{
    AgentVersion, RemiSdk, SearchChange, SearchConfig, SearchDocument, SearchEntityKind,
    SearchFieldFilter, SearchFilterGroup, SearchFilterLogic, SearchFilterOp, SearchIndexPhase,
    SearchIngestAction, SearchIngestContext, SearchIngestProvider, SearchQuery,
};
use serde_json::json;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

#[test]
fn search_indexes_core_sdk_content_and_rebuilds() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let sdk = RemiSdk::initialize(temp.path().join("search.sqlite"))?;
    let device_id = "search-device";

    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "search-collection".to_string(),
            title: "Research Inbox".to_string(),
            collection_type: Default::default(),
            app_id: None,
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "cjk-search-collection".to_string(),
            title: "提醒吃药".to_string(),
            collection_type: Default::default(),
            app_id: None,
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "search-thing".to_string(),
            title: "Indexable Note".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(json!({
                "markdown": "alpha searchable markdown body",
                "tags": ["tantivy", "pipeline"]
            })),
            collection_uuid: "search-collection".to_string(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "cjk-search-thing".to_string(),
            title: "Remi提醒吃药".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(json!({
                "markdown": "中文分词 mixed search marker tracking code 1234"
            })),
            collection_uuid: "search-collection".to_string(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.set_thing_status(device_id, "search-thing", "stalled")?;

    sdk.upsert_chat_session(
        "session-search".to_string(),
        Some("Planning Thread".to_string()),
        1,
    )?;
    sdk.upsert_chat_message_json(
        "session-search".to_string(),
        "message-search".to_string(),
        Utc::now().timestamp_millis(),
        json!({"role": "user", "content": "ship the tantivy pipeline"}).to_string(),
    )?;

    sdk.create_agent_version(&AgentVersion {
        version_id: "agent-version-search".to_string(),
        agent_id: "manager".to_string(),
        name: "Searchable Manager".to_string(),
        raw_markdown: "Agent instructions with nebula marker".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        applied_at: None,
    })?;

    sdk.flush_search_index()?;
    let status = sdk.search_index_status();
    assert_eq!(status.phase, SearchIndexPhase::Ready);
    assert!(!status.in_progress);
    assert!(status.indexed_doc_count > 0);
    assert!(status.index_path.ends_with("search.sqlite.search"));
    assert!(
        sdk.search_index_status_json()?
            .contains("indexed_doc_count")
    );

    assert_has_kind(&sdk, "searchable markdown", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "Note Indexable", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "Indexable-Note", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "  Indexable   Note  ", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "Idnexable Ntoe", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "Research Inbox", SearchEntityKind::Collection)?;
    assert_has_kind(&sdk, "吃药", SearchEntityKind::Collection)?;
    assert_has_kind(&sdk, "提醒", SearchEntityKind::Collection)?;
    assert_has_kind(&sdk, "Remi", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "吃药", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "12", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "34", SearchEntityKind::Thing)?;
    assert_has_result(
        SearchQuery {
            query: "吃药".to_string(),
            kinds: vec![SearchEntityKind::Collection],
            fields: vec!["title".to_string()],
            ..Default::default()
        },
        &sdk,
    )?;
    assert_has_kind(&sdk, "tantivy pipeline", SearchEntityKind::ChatMessage)?;
    assert_has_kind(&sdk, "nebula marker", SearchEntityKind::AgentVersion)?;
    assert_has_kind(&sdk, "smoke testing", SearchEntityKind::Action)?;
    assert_has_field(&sdk, "Indexable", SearchEntityKind::Thing, "title")?;
    assert_has_field(
        &sdk,
        "alpha searchable",
        SearchEntityKind::Thing,
        "markdown",
    )?;
    assert_no_results(
        SearchQuery {
            query: "alpha searchable".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            fields: vec!["title".to_string()],
            ..Default::default()
        },
        &sdk,
    )?;
    assert_no_results(
        SearchQuery {
            query: "stalled".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            ..Default::default()
        },
        &sdk,
    )?;
    assert_has_result(
        SearchQuery {
            query: "stalled".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            business_fields: vec!["status".to_string()],
            ..Default::default()
        },
        &sdk,
    )?;
    assert_has_result(
        SearchQuery {
            query: "Indexable".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            filter: Some(SearchFilterGroup {
                logic: SearchFilterLogic::And,
                filters: vec![SearchFieldFilter {
                    fields: vec!["collection_uuid".to_string()],
                    op: SearchFilterOp::Eq,
                    values: vec![json!("search-collection")],
                }],
                groups: Vec::new(),
            }),
            ..Default::default()
        },
        &sdk,
    )?;
    assert_no_results(
        SearchQuery {
            query: "Indexable".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            filter: Some(SearchFilterGroup {
                logic: SearchFilterLogic::And,
                filters: vec![SearchFieldFilter {
                    fields: vec!["collection_uuid".to_string()],
                    op: SearchFilterOp::Eq,
                    values: vec![json!("missing-collection")],
                }],
                groups: Vec::new(),
            }),
            ..Default::default()
        },
        &sdk,
    )?;
    assert_has_result(
        SearchQuery {
            query: "Indexable".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            filter: Some(SearchFilterGroup {
                logic: SearchFilterLogic::And,
                filters: vec![SearchFieldFilter {
                    fields: vec!["created_at_ms".to_string()],
                    op: SearchFilterOp::Gt,
                    values: vec![json!(0)],
                }],
                groups: Vec::new(),
            }),
            ..Default::default()
        },
        &sdk,
    )?;
    assert_no_results(
        SearchQuery {
            query: "Indexable".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            filter: Some(SearchFilterGroup {
                logic: SearchFilterLogic::And,
                filters: vec![SearchFieldFilter {
                    fields: vec!["created_at_ms".to_string()],
                    op: SearchFilterOp::Gt,
                    values: vec![json!(9_999_999_999_999_i64)],
                }],
                groups: Vec::new(),
            }),
            ..Default::default()
        },
        &sdk,
    )?;
    assert_has_result(
        SearchQuery {
            query: "Indexable".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            filter: Some(SearchFilterGroup {
                logic: SearchFilterLogic::And,
                filters: vec![SearchFieldFilter {
                    fields: vec!["deleted".to_string()],
                    op: SearchFilterOp::Eq,
                    values: vec![json!(false)],
                }],
                groups: Vec::new(),
            }),
            ..Default::default()
        },
        &sdk,
    )?;
    assert_has_result(
        SearchQuery {
            query: "Indexable".to_string(),
            kinds: vec![SearchEntityKind::Thing],
            filter: Some(SearchFilterGroup {
                logic: SearchFilterLogic::Or,
                filters: Vec::new(),
                groups: vec![
                    SearchFilterGroup {
                        logic: SearchFilterLogic::And,
                        filters: vec![SearchFieldFilter {
                            fields: vec!["collection_uuid".to_string()],
                            op: SearchFilterOp::Eq,
                            values: vec![json!("missing-collection")],
                        }],
                        groups: Vec::new(),
                    },
                    SearchFilterGroup {
                        logic: SearchFilterLogic::And,
                        filters: vec![SearchFieldFilter {
                            fields: vec!["datatype".to_string()],
                            op: SearchFilterOp::Eq,
                            values: vec![json!("markdown")],
                        }],
                        groups: Vec::new(),
                    },
                ],
            }),
            ..Default::default()
        },
        &sdk,
    )?;

    sdk.rebuild_search_index()?;
    assert_eq!(sdk.search_index_status().phase, SearchIndexPhase::Ready);
    assert_has_kind(&sdk, "searchable markdown", SearchEntityKind::Thing)?;
    assert_has_kind(&sdk, "tantivy pipeline", SearchEntityKind::ChatMessage)?;

    let json_results = sdk.search_json(&serde_json::to_string(&SearchQuery {
        query: "nebula".to_string(),
        kinds: vec![SearchEntityKind::AgentVersion],
        limit: 5,
        offset: 0,
        ..Default::default()
    })?)?;
    assert!(json_results.contains("agent-version-search"));

    Ok(())
}

#[test]
fn search_rebuild_uses_registered_provider() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let sdk = RemiSdk::initialize_with_search_config(
        temp.path().join("provider-search.sqlite"),
        SearchConfig::default(),
    )?;
    sdk.register_search_provider(Arc::new(StaticProvider));
    sdk.rebuild_search_index()?;

    let results = sdk.search(SearchQuery {
        query: "custom provider marker".to_string(),
        kinds: vec![SearchEntityKind::Action],
        limit: 10,
        offset: 0,
        ..Default::default()
    })?;
    assert!(
        results
            .iter()
            .any(|result| result.entity_id == "provider-doc")
    );
    Ok(())
}

#[test]
fn search_change_pipeline_uses_registered_provider() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let sdk = RemiSdk::initialize(temp.path().join("change-search.sqlite"))?;
    sdk.register_search_provider(Arc::new(ChangeProvider));

    sdk.enqueue_search_change(SearchChange {
        source: "test-change".to_string(),
        kind: SearchEntityKind::Action,
        entity_id: "change-provider-doc".to_string(),
        parent_id: None,
        metadata: json!({"body": "change pipeline marker"}),
    })?;
    sdk.flush_search_index()?;

    let results = sdk.search(SearchQuery {
        query: "change pipeline marker".to_string(),
        kinds: vec![SearchEntityKind::Action],
        limit: 10,
        offset: 0,
        ..Default::default()
    })?;
    assert!(
        results
            .iter()
            .any(|result| result.entity_id == "change-provider-doc")
    );
    Ok(())
}

#[test]
fn search_index_status_reports_async_rebuild_progress() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let sdk = RemiSdk::initialize(temp.path().join("progress-search.sqlite"))?;
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    sdk.register_search_provider(Arc::new(BlockingProvider {
        started_tx,
        release_rx: Mutex::new(release_rx),
    }));

    sdk.start_search_index_rebuild()?;
    started_rx.recv_timeout(Duration::from_secs(5))?;
    let status = sdk.search_index_status();
    assert_eq!(status.phase, SearchIndexPhase::Building);
    assert!(status.in_progress);
    assert!(status.progress_done.is_some());
    assert!(status.progress_total.is_some());
    assert!(
        status
            .current_stage
            .as_deref()
            .unwrap_or_default()
            .contains("blocking-test-provider")
    );

    release_tx.send(())?;
    sdk.flush_search_index()?;
    let status = sdk.search_index_status();
    assert_eq!(status.phase, SearchIndexPhase::Ready);
    assert!(!status.in_progress);
    assert!(status.progress_done.is_none());
    assert!(status.progress_total.is_none());
    Ok(())
}

fn assert_has_kind(sdk: &RemiSdk, query: &str, kind: SearchEntityKind) -> Result<()> {
    let results = sdk.search(SearchQuery {
        query: query.to_string(),
        kinds: vec![kind],
        limit: 10,
        offset: 0,
        ..Default::default()
    })?;
    assert!(
        results.iter().any(|result| result.kind == kind),
        "expected {kind:?} result for query {query:?}, got {results:?}"
    );
    Ok(())
}

fn assert_has_field(sdk: &RemiSdk, query: &str, kind: SearchEntityKind, field: &str) -> Result<()> {
    let results = sdk.search(SearchQuery {
        query: query.to_string(),
        kinds: vec![kind],
        limit: 10,
        offset: 0,
        ..Default::default()
    })?;
    assert!(
        results
            .iter()
            .any(|result| result.kind == kind && result.field == field),
        "expected {kind:?} result in field {field:?} for query {query:?}, got {results:?}"
    );
    Ok(())
}

fn assert_has_result(query: SearchQuery, sdk: &RemiSdk) -> Result<()> {
    let results = sdk.search(query.clone())?;
    assert!(
        !results.is_empty(),
        "expected at least one result for query {query:?}"
    );
    Ok(())
}

fn assert_no_results(query: SearchQuery, sdk: &RemiSdk) -> Result<()> {
    let results = sdk.search(query.clone())?;
    assert!(
        results.is_empty(),
        "expected no results for query {query:?}, got {results:?}"
    );
    Ok(())
}

struct StaticProvider;

impl SearchIngestProvider for StaticProvider {
    fn provider_id(&self) -> &'static str {
        "static-test-provider"
    }

    fn rebuild_documents(
        &self,
        _context: &SearchIngestContext<'_>,
    ) -> Result<Vec<SearchIngestAction>> {
        Ok(vec![SearchIngestAction::upsert(
            SearchDocument::new(
                SearchEntityKind::Action,
                "provider-doc",
                "Provider Doc",
                "custom provider marker",
                0,
            )
            .with_metadata(json!({"provider": "static"})),
        )])
    }
}

struct ChangeProvider;

impl SearchIngestProvider for ChangeProvider {
    fn provider_id(&self) -> &'static str {
        "change-test-provider"
    }

    fn rebuild_documents(
        &self,
        _context: &SearchIngestContext<'_>,
    ) -> Result<Vec<SearchIngestAction>> {
        Ok(Vec::new())
    }

    fn documents_for_change(
        &self,
        _context: &SearchIngestContext<'_>,
        change: &SearchChange,
    ) -> Result<Vec<SearchIngestAction>> {
        let body = change
            .metadata
            .get("body")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        Ok(vec![SearchIngestAction::upsert(SearchDocument::new(
            change.kind,
            change.entity_id.clone(),
            "Change Provider Doc",
            body,
            0,
        ))])
    }
}

struct BlockingProvider {
    started_tx: mpsc::Sender<()>,
    release_rx: Mutex<mpsc::Receiver<()>>,
}

impl SearchIngestProvider for BlockingProvider {
    fn provider_id(&self) -> &'static str {
        "blocking-test-provider"
    }

    fn rebuild_documents(
        &self,
        _context: &SearchIngestContext<'_>,
    ) -> Result<Vec<SearchIngestAction>> {
        let _ = self.started_tx.send(());
        self.release_rx
            .lock()
            .map_err(|error| anyhow::anyhow!("release lock poisoned: {error}"))?
            .recv_timeout(Duration::from_secs(5))?;
        Ok(vec![SearchIngestAction::upsert(SearchDocument::new(
            SearchEntityKind::Action,
            "blocking-provider-doc",
            "Blocking Provider Doc",
            "blocking provider marker",
            0,
        ))])
    }
}
