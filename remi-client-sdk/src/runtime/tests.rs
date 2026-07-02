use super::*;
use crate::things_crdt::ThingDatatype;
use crate::types::EntityActionBinding;
use chrono::TimeZone;
use croner::Cron;
#[cfg(feature = "quickjs")]
use std::io::{Read, Write};
#[cfg(feature = "quickjs")]
use std::net::TcpListener;
#[cfg(feature = "quickjs")]
use std::thread;
use tempfile::tempdir;

#[cfg(feature = "quickjs")]
fn seed_test_action(sdk: &TriggerSdk, action_uuid: &str, title: &str, script_source: &str) {
    sdk.storage
        .seed_builtin_actions(&[ActionDefinition {
            action_uuid: action_uuid.to_string(),
            name: action_uuid.replace('.', "_"),
            title: title.to_string(),
            description: format!("Test action for {action_uuid}"),
            version: "v1".to_string(),
            category: "test".to_string(),
            enabled: true,
            metadata_json: json!({ "builtin": false, "test": true }),
            script_source: script_source.trim().to_string(),
            input_schema_json: json!({ "type": "object" }),
            output_schema_json: Some(json!({ "type": "object" })),
        }])
        .expect("seed test action");
}

#[cfg(feature = "quickjs")]
fn spawn_test_http_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let addr = listener.local_addr().expect("local addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept connection");
        let mut buffer = [0_u8; 4096];
        let read = stream.read(&mut buffer).expect("read request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /action-test HTTP/1.1"));
        assert!(request.to_ascii_lowercase().contains("x-test: 1"));
        assert!(request.contains("{\"ping\":\"pong\"}"));

        let body = r#"{"ok":true,"reply":"ack"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body,
        );
        stream
            .write_all(response.as_bytes())
            .expect("write response");
    });

    (format!("http://{addr}/action-test"), handle)
}

fn seed_markdown_thing(
    sdk: &TriggerSdk,
    device_id: &str,
    collection_id: &str,
    thing_id: &str,
    markdown: &str,
) {
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: collection_id.to_string(),
            title: "Test Collection".to_string(),
            collection_type: Default::default(),
            app_id: None,
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            created_at: None,
            updated_at: None,
        },
    )
    .expect("seed collection");

    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: thing_id.to_string(),
            title: "Test Thing".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(json!({"markdown": markdown})),
            collection_uuid: collection_id.to_string(),
            trigger_uuid: None,
            trigger_uuid_patch: Default::default(),
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )
    .expect("seed thing");
}

#[test]
fn test_extract_cron_from_preconditions() {
    let preconditions = vec![TriggerRule {
        rule: "cron('0 18 * * *')".to_string(),
        description: "Every day at 6 PM".to_string(),
    }];
    assert_eq!(
        extract_cron_from_preconditions(&preconditions),
        Some("0 18 * * *".to_string())
    );

    let no_cron = vec![TriggerRule {
        rule: "in_time_range('09:00', '17:00')".to_string(),
        description: "Business hours".to_string(),
    }];
    assert_eq!(extract_cron_from_preconditions(&no_cron), None);
}

#[test]
fn test_extract_cron_with_double_quotes() {
    let preconditions = vec![TriggerRule {
        rule: r#"cron("0 9 * * *")"#.to_string(),
        description: "Morning".to_string(),
    }];
    assert_eq!(
        extract_cron_from_preconditions(&preconditions),
        Some("0 9 * * *".to_string())
    );
}

#[test]
fn test_croner_accepts_posix_sunday_zero() {
    assert!(Cron::from_str("0 10 * * 0,6").is_ok());
}

#[test]
fn test_network_change_trigger_is_marked_due_on_connectivity_event() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");

    let trigger_uuid = "trg-network-change".to_string();
    sdk.register_trigger(TriggerRegistration {
        trigger_uuid: trigger_uuid.clone(),
        name: "Network change trigger".to_string(),
        version: "v1".to_string(),
        precondition: vec![TriggerRule {
            rule: "event('Connectivity')".to_string(),
            description: "On connectivity change".to_string(),
        }],
        condition: vec![TriggerRule {
            rule: "true".to_string(),
            description: "Always true".to_string(),
        }],
        action_uuid: None,
        action_args: json!({}),
    })
    .expect("register trigger");

    // Event-driven triggers should not be due until an event arrives.
    let before = sdk
        .storage
        .fetch_due_triggers(Utc::now())
        .expect("fetch due triggers");
    assert!(
        before.iter().all(|t| t.trigger_uuid != trigger_uuid),
        "event trigger must not be due immediately after registration"
    );

    let event_ts = Utc::now();
    sdk.record_event(EventPayload {
        event_type: "Connectivity".to_string(),
        timestamp: event_ts,
        metadata: serde_json::json!({
            "message": "Update connectivity: wifi",
            "states": ["wifi"]
        }),
    })
    .expect("record connectivity event");

    let after = sdk
        .storage
        .fetch_due_triggers(event_ts + chrono::Duration::seconds(1))
        .expect("fetch due triggers after event");
    assert!(
        after.iter().any(|t| t.trigger_uuid == trigger_uuid),
        "event trigger must be marked due after Connectivity event"
    );
}

#[test]
fn test_timer_trigger_is_due_after_registration_anchor() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");

    let trigger_uuid = "trg-timer".to_string();
    sdk.register_trigger(TriggerRegistration {
        trigger_uuid: trigger_uuid.clone(),
        name: "Timer trigger".to_string(),
        version: "v1".to_string(),
        precondition: vec![TriggerRule {
            rule: "timer('1s')".to_string(),
            description: "One second after registration".to_string(),
        }],
        condition: vec![TriggerRule {
            rule: "true".to_string(),
            description: "Always true".to_string(),
        }],
        action_uuid: None,
        action_args: json!({}),
    })
    .expect("register trigger");

    let stored = sdk
        .storage
        .fetch_trigger(&trigger_uuid)
        .expect("fetch trigger")
        .expect("trigger exists");
    assert!(
        stored.next_fire.is_some(),
        "timer trigger should have next_fire"
    );
    let stored_preconditions: Vec<TriggerRule> =
        serde_json::from_str(&stored.precondition_json).expect("decode preconditions");
    assert_eq!(stored_preconditions.len(), 1);
    assert!(
        stored_preconditions[0].rule.starts_with("timer(\"")
            && stored_preconditions[0].rule.contains('T')
            && !stored_preconditions[0].rule.contains("1s"),
        "timer precondition should be normalized to an absolute RFC3339 timestamp: {}",
        stored_preconditions[0].rule
    );
}

#[test]
fn test_events_list_between_json_accepts_local_naive_timestamps() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");

    let timestamp = Utc.with_ymd_and_hms(2026, 4, 2, 1, 15, 0).single().unwrap();
    sdk.record_event(EventPayload {
        event_type: "DesktopAppFocus".to_string(),
        timestamp,
        metadata: json!({ "window_title": "VSCode" }),
    })
    .expect("record event");

    let output = sdk
        .events_list_between_json("2026-04-02 09:00:00", "2026-04-02 09:30:00")
        .expect("events between");
    let events: Vec<EventPayload> = serde_json::from_str(&output).expect("parse events json");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "DesktopAppFocus");
}

#[test]
fn test_events_abstract_json_reports_recorded_events() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");

    sdk.record_event(EventPayload {
        event_type: "DesktopAppFocus".to_string(),
        timestamp: Utc.with_ymd_and_hms(2026, 4, 2, 1, 0, 0).single().unwrap(),
        metadata: json!({ "window_title": "VSCode" }),
    })
    .expect("record focus");
    sdk.record_event(EventPayload {
        event_type: "DesktopNetworkOnline".to_string(),
        timestamp: Utc.with_ymd_and_hms(2026, 4, 2, 1, 5, 0).single().unwrap(),
        metadata: json!({ "connected": true }),
    })
    .expect("record network");

    let output = sdk.events_abstract_json(3).expect("abstract events");
    let summary: serde_json::Value = serde_json::from_str(&output).expect("parse summary");
    let hours = summary
        .get("hours")
        .and_then(|value| value.as_array())
        .expect("hours array");

    assert!(
        !hours.is_empty(),
        "abstract summary should include recorded hours"
    );
    let total_events = hours
        .iter()
        .map(|hour| {
            hour.get("total_events")
                .and_then(|value| value.as_u64())
                .unwrap_or(0)
        })
        .sum::<u64>();
    assert_eq!(total_events, 2);
}

#[test]
fn document_set_cache_returns_fresh_result_after_same_sdk_write() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";
    let content_path = format!("/collection/{collection_id}/things/{thing_id}/content.md");

    seed_markdown_thing(&sdk, device_id, collection_id, thing_id, "before");

    let initial = sdk
        .read_virtual_path(device_id, &content_path)
        .expect("initial read");
    assert_eq!(initial.content, "before");

    sdk.things_edit_content(
        device_id,
        thing_id,
        "overwrite",
        None,
        Some("after"),
        None,
        None,
        None,
        None,
        None,
    )
    .expect("edit content");

    let updated = sdk
        .read_virtual_path(device_id, &content_path)
        .expect("updated read");
    assert_eq!(updated.content, "after");
}

#[test]
fn document_set_cache_invalidates_after_external_sdk_write() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk_reader = TriggerSdk::initialize(&db_path).expect("reader sdk init");
    let sdk_writer = TriggerSdk::initialize(&db_path).expect("writer sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";
    let content_path = format!("/collection/{collection_id}/things/{thing_id}/content.md");

    seed_markdown_thing(&sdk_reader, device_id, collection_id, thing_id, "before");

    let initial = sdk_reader
        .read_virtual_path(device_id, &content_path)
        .expect("initial read");
    assert_eq!(initial.content, "before");

    sdk_writer
        .things_edit_content(
            device_id,
            thing_id,
            "overwrite",
            None,
            Some("after"),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("writer edit content");

    let updated = sdk_reader
        .read_virtual_path(device_id, &content_path)
        .expect("updated read");
    assert_eq!(updated.content, "after");
}

#[test]
fn json_object_cache_returns_fresh_result_after_same_sdk_write_and_delete() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";
    let thing_path = format!("/collection/{collection_id}/things/{thing_id}");

    seed_markdown_thing(&sdk, device_id, collection_id, thing_id, "before");

    let created = sdk
        .create_virtual_path(
            device_id,
            &thing_path,
            "json_object",
            None,
            Some("Config"),
            Some(r#"{"enabled":true,"count":1}"#),
            None,
            None,
            None,
        )
        .expect("create json_object");
    let entry_path = created
        .get("path")
        .and_then(Value::as_str)
        .expect("entry path")
        .to_string();
    let data_path = format!("{entry_path}.data.json");
    let schema_path = format!("{entry_path}.schema.json");

    let initial = sdk
        .read_virtual_path(device_id, &data_path)
        .expect("initial data read");
    assert_eq!(
        serde_json::from_str::<Value>(&initial.content).expect("parse initial json"),
        json!({
            "enabled": true,
            "count": 1
        })
    );

    sdk.edit_virtual_path(
        device_id,
        &schema_path,
        "overwrite",
        Some(&json!({
            "type": "object",
            "properties": {
                "enabled": { "type": "boolean" },
                "count": { "type": "integer", "minimum": 0 }
            },
            "required": ["enabled", "count"]
        })),
        None,
        None,
        None,
    )
    .expect("write schema");

    sdk.edit_virtual_path(
        device_id,
        &data_path,
        "overwrite",
        Some(&json!({
            "enabled": false,
            "count": 2
        })),
        None,
        None,
        None,
    )
    .expect("write data");

    let updated = sdk
        .read_virtual_path(device_id, &data_path)
        .expect("updated data read");
    assert_eq!(
        serde_json::from_str::<Value>(&updated.content).expect("parse updated json"),
        json!({
            "enabled": false,
            "count": 2
        })
    );

    let schema = sdk
        .read_virtual_path(device_id, &schema_path)
        .expect("schema read");
    assert_eq!(
        serde_json::from_str::<Value>(&schema.content).expect("parse schema json"),
        json!({
            "type": "object",
            "properties": {
                "enabled": { "type": "boolean" },
                "count": { "type": "integer", "minimum": 0 }
            },
            "required": ["enabled", "count"]
        })
    );

    sdk.delete_virtual_path(device_id, &entry_path)
        .expect("delete json object entry");
    assert!(sdk.read_virtual_path(device_id, &data_path).is_err());
    assert!(sdk.read_virtual_path(device_id, &schema_path).is_err());
}

#[test]
fn json_object_cache_invalidates_after_external_sdk_write_and_delete() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk_reader = TriggerSdk::initialize(&db_path).expect("reader sdk init");
    let sdk_writer = TriggerSdk::initialize(&db_path).expect("writer sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";
    let thing_path = format!("/collection/{collection_id}/things/{thing_id}");

    seed_markdown_thing(&sdk_reader, device_id, collection_id, thing_id, "before");

    let created = sdk_reader
        .create_virtual_path(
            device_id,
            &thing_path,
            "json_object",
            None,
            Some("Config"),
            Some(r#"{"enabled":true}"#),
            None,
            None,
            None,
        )
        .expect("create json_object");
    let entry_path = created
        .get("path")
        .and_then(Value::as_str)
        .expect("entry path")
        .to_string();
    let data_path = format!("{entry_path}.data.json");

    let initial = sdk_reader
        .read_virtual_path(device_id, &data_path)
        .expect("initial read");
    assert_eq!(
        serde_json::from_str::<Value>(&initial.content).expect("parse initial json"),
        json!({ "enabled": true })
    );

    sdk_writer
        .edit_virtual_path(
            device_id,
            &data_path,
            "overwrite",
            Some(&json!({
                "enabled": false,
                "source": "writer"
            })),
            None,
            None,
            None,
        )
        .expect("writer overwrite data");

    let updated = sdk_reader
        .read_virtual_path(device_id, &data_path)
        .expect("reader updated read");
    assert_eq!(
        serde_json::from_str::<Value>(&updated.content).expect("parse updated json"),
        json!({
            "enabled": false,
            "source": "writer"
        })
    );

    sdk_writer
        .delete_virtual_path(device_id, &entry_path)
        .expect("writer delete entry");

    assert!(sdk_reader.read_virtual_path(device_id, &data_path).is_err());
}

#[test]
fn things_upsert_thing_restores_json_object_entries_from_snapshot_payload() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";

    seed_markdown_thing(&sdk, device_id, collection_id, thing_id, "before");

    let entry_id = sdk
        .things_add_json_object_content_entry(
            device_id,
            thing_id,
            Some("Config"),
            Some(&json!({
                "enabled": true,
                "count": 1
            })),
            None,
        )
        .expect("add json object entry");

    let snapshot = sdk
        .things_list_snapshot(device_id)
        .expect("snapshot before delete");
    let thing_snapshot = snapshot
        .things
        .into_iter()
        .find(|thing| thing.uuid == thing_id)
        .expect("thing in snapshot");

    sdk.things_delete_thing(device_id, collection_id, thing_id)
        .expect("delete thing");

    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: thing_snapshot.uuid.clone(),
            title: thing_snapshot.title.clone(),
            datatype: thing_snapshot.datatype.clone(),
            data: Some(thing_snapshot.data.clone()),
            collection_uuid: thing_snapshot.collection_uuid.clone(),
            trigger_uuid: thing_snapshot.trigger_uuid.clone(),
            trigger_uuid_patch: Default::default(),
            parent_uuid: thing_snapshot.parent_uuid.clone(),
            created_at: None,
            updated_at: None,
        },
    )
    .expect("restore thing from snapshot payload");

    let restored_entries = sdk
        .things_get_content_entries(device_id, thing_id)
        .expect("restored content entries");
    assert_eq!(restored_entries.len(), 1);
    assert_eq!(restored_entries[0].id, entry_id);
    match &restored_entries[0].payload {
        crate::things_crdt::ContentEntryPayload::JsonObject(_) => {}
        other => panic!("expected json_object payload, got {other:?}"),
    }
}

#[test]
fn bootstrap_stash_round_trip_preserves_json_object_content_docs() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";

    seed_markdown_thing(&sdk, device_id, collection_id, thing_id, "before");

    let entry_id = sdk
        .things_add_json_object_content_entry(
            device_id,
            thing_id,
            Some("Config"),
            Some(&json!({
                "enabled": true,
                "count": 1
            })),
            Some(&json!({
                "type": "object",
                "properties": {
                    "enabled": { "type": "boolean" },
                    "count": { "type": "integer" }
                }
            })),
        )
        .expect("add json object entry");

    assert!(
        sdk.things_bootstrap_stash_local_snapshot_if_needed(device_id)
            .expect("stash local docs")
    );
    let last_event_id = sdk
        .things_watch_since(device_id, 0, 500)
        .expect("events before bootstrap replay")
        .last()
        .map(|event| event.event_id)
        .unwrap_or(0);

    sdk.things_bootstrap_from_server_snapshot_and_replay_stash(device_id, Vec::new(), None)
        .expect("replay stashed docs");

    let restored_data = sdk
        .things_get_json_object_entry_data(device_id, thing_id, &entry_id)
        .expect("load restored json data")
        .expect("json data should exist after replay");
    assert_eq!(
        restored_data,
        json!({
            "enabled": true,
            "count": 1
        })
    );

    let restored_schema = sdk
        .things_get_json_object_entry_schema(device_id, thing_id, &entry_id)
        .expect("load restored json schema")
        .expect("json schema should exist after replay");
    assert_eq!(
        restored_schema,
        json!({
            "type": "object",
            "properties": {
                "enabled": { "type": "boolean" },
                "count": { "type": "integer" }
            }
        })
    );
    let replay_events = sdk
        .things_watch_since(device_id, last_event_id, 100)
        .expect("events after bootstrap replay");
    assert!(
        replay_events
            .iter()
            .any(|event| event.entity_type == "thing" && event.entity_uuid == thing_id)
    );
    assert!(
        sdk.crdt_get_dirty_documents()
            .expect("dirty documents after bootstrap replay")
            .iter()
            .any(|row| row.uuid == thing_id)
    );
}

#[test]
fn bootstrap_stash_still_runs_after_done_flag_when_new_local_docs_exist() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";

    seed_markdown_thing(&sdk, device_id, collection_id, thing_id, "before");

    sdk.storage
        .set_internal_kv("things.bootstrap.done", "1")
        .expect("set bootstrap done flag");

    let entry_id = sdk
        .things_add_json_object_content_entry(
            device_id,
            thing_id,
            Some("Config"),
            Some(&json!({
                "enabled": true,
                "count": 2
            })),
            Some(&json!({
                "type": "object",
                "properties": {
                    "enabled": { "type": "boolean" },
                    "count": { "type": "integer" }
                }
            })),
        )
        .expect("add json object entry after bootstrap done");

    assert!(
        sdk.things_bootstrap_stash_local_snapshot_if_needed(device_id)
            .expect("stash should still run after done flag")
    );
    let last_event_id = sdk
        .things_watch_since(device_id, 0, 500)
        .expect("events before bootstrap replay")
        .last()
        .map(|event| event.event_id)
        .unwrap_or(0);

    sdk.things_bootstrap_from_server_snapshot_and_replay_stash(device_id, Vec::new(), None)
        .expect("replay stashed docs");

    let restored_data = sdk
        .things_get_json_object_entry_data(device_id, thing_id, &entry_id)
        .expect("load restored json data")
        .expect("json data should exist after replay");
    assert_eq!(
        restored_data,
        json!({
            "enabled": true,
            "count": 2
        })
    );
    let replay_events = sdk
        .things_watch_since(device_id, last_event_id, 100)
        .expect("events after bootstrap replay");
    assert!(
        replay_events
            .iter()
            .any(|event| event.entity_type == "thing" && event.entity_uuid == thing_id)
    );
    assert!(
        sdk.crdt_get_dirty_documents()
            .expect("dirty documents after bootstrap replay")
            .iter()
            .any(|row| row.uuid == thing_id)
    );
}

#[test]
fn sdk_seeds_builtin_actions_and_exposes_action_vfs() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let actions = sdk.list_actions().expect("list actions");

    assert!(!actions.is_empty());
    assert_eq!(actions[0].action_uuid, "builtin.echo_json");

    let tree = sdk
        .tree_virtual_path("device-test", Some("/action"))
        .expect("tree action root");
    assert!(tree.contains("builtin.echo_json/"));

    let metadata = sdk
        .read_virtual_path("device-test", "/action/builtin.echo_json/metadata.json")
        .expect("read action metadata");
    assert!(metadata.content.contains("supports_trigger"));
}

#[cfg(feature = "quickjs")]
#[test]
fn actions_can_use_http_host_api() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let (url, server) = spawn_test_http_server();

    seed_test_action(
        &sdk,
        "test.http_client",
        "HTTP Client Test",
        r#"
const response = http.post(args.url, {
headers: { "x-test": "1" },
json: { ping: "pong" },
});
return {
ok: response.ok,
status: response.status,
method: response.method,
reply: response.body_json?.reply ?? null,
};
"#,
    );

    let record = sdk
        .execute_action_now(
            "test.http_client",
            ActionInvocationSourceKind::System,
            None,
            None,
            json!({ "url": url }),
            Some("device-test"),
        )
        .expect("execute http action");

    let result = record.result_json.expect("result json");
    assert_eq!(result.get("ok"), Some(&json!(true)));
    assert_eq!(result.get("status"), Some(&json!(200)));
    assert_eq!(result.get("method"), Some(&json!("POST")));
    assert_eq!(result.get("reply"), Some(&json!("ack")));

    server.join().expect("join test server");
}

#[cfg(feature = "quickjs")]
#[test]
fn actions_can_send_notifications_and_emit_events() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let mut notification_rx = sdk.notifications_subscribe();

    seed_test_action(
        &sdk,
        "test.notify_client",
        "Notify Test",
        r#"
const created = notify.send({
title: "Action Reminder",
body: args.body,
category: "action:test",
});
const listed = notify.list({ category: "action:test", limit: 10 });
return {
notificationId: created.notification_id,
listedCount: listed.items.length,
latestTitle: listed.items[0]?.title ?? null,
};
"#,
    );

    let record = sdk
        .execute_action_now(
            "test.notify_client",
            ActionInvocationSourceKind::System,
            None,
            None,
            json!({ "body": "Stretch now" }),
            Some("device-test"),
        )
        .expect("execute notify action");

    let event = notification_rx.try_recv().expect("notification event");
    match event {
        NotificationEvent::Added {
            category,
            source,
            title,
            ..
        } => {
            assert_eq!(category, "action:test");
            assert_eq!(source, crate::types::NotificationSource::System);
            assert_eq!(title, "Action Reminder");
        }
        other => panic!("expected added event, got {other:?}"),
    }

    let result = record.result_json.expect("result json");
    assert_eq!(result.get("listedCount"), Some(&json!(1)));
    assert_eq!(result.get("latestTitle"), Some(&json!("Action Reminder")));

    let stored = sdk
        .storage
        .list_notifications_by_category("action:test", 10)
        .expect("stored notifications");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].body, "Stretch now");
}

#[test]
fn unbound_trigger_uses_default_notification_action() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let trigger_id = "trigger-default-notify".to_string();

    sdk.register_trigger(TriggerRegistration {
        trigger_uuid: trigger_id.clone(),
        name: "Water Plants".to_string(),
        version: "v1".to_string(),
        precondition: vec![TriggerRule {
            rule: "event('ManualTest')".to_string(),
            description: "manual test timing".to_string(),
        }],
        condition: Vec::new(),
        action_uuid: None,
        action_args: json!({}),
    })
    .expect("register trigger");

    let trigger = sdk
        .storage
        .fetch_trigger(&trigger_id)
        .expect("fetch trigger")
        .expect("trigger exists");
    let fire_time = Utc
        .with_ymd_and_hms(2026, 4, 15, 10, 30, 0)
        .single()
        .unwrap();
    let summary = sdk
        .execute_trigger(&trigger, fire_time, TriggerRunType::Manual)
        .expect("execute trigger");

    assert!(summary.result);
    assert!(summary.notification_id.is_some());

    let invocation = sdk
        .storage
        .latest_action_invocation(DEFAULT_TRIGGER_NOTIFICATION_ACTION_UUID)
        .expect("latest action invocation")
        .expect("default notification invocation exists");
    assert_eq!(invocation.source_kind, ActionInvocationSourceKind::Trigger);
    assert_eq!(
        invocation.source_entity_uuid.as_deref(),
        Some(trigger_id.as_str())
    );

    let invocation_notification_id = invocation
        .result_json
        .as_ref()
        .and_then(|value| value.get("notification_id"))
        .and_then(Value::as_i64);
    assert_eq!(summary.notification_id, invocation_notification_id);

    let notifications = sdk
        .storage
        .list_notifications_by_category(&trigger_id, 10)
        .expect("list notifications");
    assert_eq!(notifications.len(), 1);
    assert_eq!(
        notifications[0].source,
        crate::types::NotificationSource::Trigger
    );
    assert_eq!(notifications[0].title, "Water Plants");
    assert_eq!(
        notifications[0].body,
        "触发器「Water Plants」已于 18:30 触发"
    );
}

#[test]
fn explicit_trigger_action_overrides_default_notification_action() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let trigger_id = "trigger-custom-action".to_string();

    sdk.register_trigger(TriggerRegistration {
        trigger_uuid: trigger_id.clone(),
        name: "Echo Trigger".to_string(),
        version: "v1".to_string(),
        precondition: vec![TriggerRule {
            rule: "event('ManualTest')".to_string(),
            description: "manual test timing".to_string(),
        }],
        condition: Vec::new(),
        action_uuid: Some("builtin.echo_json".to_string()),
        action_args: json!({ "scope": "custom-trigger" }),
    })
    .expect("register trigger");

    let trigger = sdk
        .storage
        .fetch_trigger(&trigger_id)
        .expect("fetch trigger")
        .expect("trigger exists");
    let summary = sdk
        .execute_trigger(&trigger, Utc::now(), TriggerRunType::Manual)
        .expect("execute trigger");

    assert!(summary.result);
    assert_eq!(summary.notification_id, None);

    let notifications = sdk
        .storage
        .list_notifications_by_category(&trigger_id, 10)
        .expect("list notifications");
    assert!(notifications.is_empty());

    let invocation = sdk
        .storage
        .latest_action_invocation("builtin.echo_json")
        .expect("latest echo invocation")
        .expect("echo invocation exists");
    assert_eq!(invocation.source_kind, ActionInvocationSourceKind::Trigger);
    assert_eq!(
        invocation.source_entity_uuid.as_deref(),
        Some(trigger_id.as_str())
    );
}

#[test]
fn collection_and_thing_action_bindings_are_exposed_and_invokable() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";

    seed_markdown_thing(&sdk, device_id, collection_id, thing_id, "before");

    sdk.things_set_collection_action_bindings(
        device_id,
        collection_id,
        &[EntityActionBinding {
            action_uuid: "builtin.echo_json".to_string(),
            label_override: Some("Collection Echo".to_string()),
            args_json: json!({ "scope": "collection" }),
        }],
    )
    .expect("set collection bindings");
    sdk.things_set_thing_action_bindings(
        device_id,
        thing_id,
        &[EntityActionBinding {
            action_uuid: "builtin.echo_json".to_string(),
            label_override: Some("Thing Echo".to_string()),
            args_json: json!({ "scope": "thing" }),
        }],
    )
    .expect("set thing bindings");

    let collection_actions = sdk
        .read_virtual_path(
            device_id,
            &format!("/collection/{collection_id}/actions.json"),
        )
        .expect("read collection actions");
    assert!(collection_actions.content.contains("Collection Echo"));
    assert!(collection_actions.content.contains("builtin.echo_json"));

    let thing_actions = sdk
        .read_virtual_path(
            device_id,
            &format!("/collection/{collection_id}/things/{thing_id}/actions.json"),
        )
        .expect("read thing actions");
    assert!(thing_actions.content.contains("Thing Echo"));

    let collection_invocation = sdk
        .execute_collection_action_now(device_id, collection_id, "builtin.echo_json")
        .expect("invoke collection action");
    assert_eq!(
        collection_invocation.source_kind,
        ActionInvocationSourceKind::CollectionManual
    );
    assert_eq!(
        collection_invocation.source_entity_uuid.as_deref(),
        Some(collection_id)
    );

    let thing_invocation = sdk
        .execute_thing_action_now(device_id, thing_id, "builtin.echo_json")
        .expect("invoke thing action");
    assert_eq!(
        thing_invocation.source_kind,
        ActionInvocationSourceKind::ThingManual
    );
    assert_eq!(
        thing_invocation.source_entity_uuid.as_deref(),
        Some(thing_id)
    );

    let latest = sdk
        .read_virtual_path(
            device_id,
            "/action/builtin.echo_json/latest-invocation.json",
        )
        .expect("latest invocation");
    assert!(
        latest.content.contains("thing_manual") || latest.content.contains("collection_manual")
    );
}

#[test]
fn virtual_fs_create_and_edit_support_action_bindings() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sdk.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).expect("sdk init");
    let device_id = "device-test";
    let collection_id = "col-test";
    let thing_id = "thing-test";
    let trigger_id = "trigger-test";

    seed_markdown_thing(&sdk, device_id, collection_id, thing_id, "before");
    sdk.register_trigger(TriggerRegistration {
        trigger_uuid: trigger_id.to_string(),
        name: "Test Trigger".to_string(),
        version: "v1".to_string(),
        precondition: vec![TriggerRule {
            rule: "true".to_string(),
            description: "always".to_string(),
        }],
        condition: Vec::new(),
        action_uuid: None,
        action_args: json!({}),
    })
    .expect("register trigger");

    sdk.create_virtual_path(
        device_id,
        &format!("/trigger/{trigger_id}"),
        "action_binding",
        Some("builtin.echo_json"),
        None,
        Some(r#"{"scope":"trigger"}"#),
        None,
        None,
        None,
    )
    .expect("create trigger action binding");
    sdk.create_virtual_path(
        device_id,
        &format!("/collection/{collection_id}"),
        "action_binding",
        Some("builtin.echo_json"),
        Some("Collection Echo"),
        Some(r#"{"scope":"collection"}"#),
        None,
        None,
        None,
    )
    .expect("create collection action binding");
    sdk.create_virtual_path(
        device_id,
        &format!("/collection/{collection_id}/things/{thing_id}"),
        "action_binding",
        Some("builtin.echo_json"),
        Some("Thing Echo"),
        Some(r#"{"scope":"thing"}"#),
        None,
        None,
        None,
    )
    .expect("create thing action binding");

    let trigger_action = sdk
        .read_virtual_path(device_id, &format!("/trigger/{trigger_id}/action.json"))
        .expect("read trigger action binding");
    assert!(trigger_action.content.contains("builtin.echo_json"));
    assert!(trigger_action.content.contains("scope"));

    sdk.edit_virtual_path(
        device_id,
        &format!("/collection/{collection_id}/actions.json"),
        "overwrite",
        Some(&json!([
            {
                "action_uuid": "builtin.echo_json",
                "label_override": "Collection Echo Updated",
                "args_json": { "scope": "collection-updated" }
            }
        ])),
        None,
        None,
        None,
    )
    .expect("edit collection action bindings");
    sdk.edit_virtual_path(
        device_id,
        &format!("/trigger/{trigger_id}/action.json"),
        "overwrite",
        Some(&json!(null)),
        None,
        None,
        None,
    )
    .expect("clear trigger action binding");

    let updated_collection_actions = sdk
        .read_virtual_path(
            device_id,
            &format!("/collection/{collection_id}/actions.json"),
        )
        .expect("read updated collection actions");
    assert!(
        updated_collection_actions
            .content
            .contains("Collection Echo Updated")
    );

    let cleared_trigger_action = sdk
        .read_virtual_path(device_id, &format!("/trigger/{trigger_id}/action.json"))
        .expect("read cleared trigger action binding");
    assert_eq!(cleared_trigger_action.content.trim(), "null");
}
