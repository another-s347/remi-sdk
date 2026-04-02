use anyhow::{Context, Result};
use remi_client_sdk::things_crdt::{ContentEntry, ContentEntryPayload, ImageField, ThingCollectionUpsert, ThingDatatype, ThingUpsert};
use remi_client_sdk::{RemiUri, TriggerRegistration, TriggerRule, TriggerSdk};
use serde_json::{Value as JsonValue, json};
use tempfile::tempdir;

fn init_sdk() -> Result<(tempfile::TempDir, TriggerSdk)> {
    let dir = tempdir().context("tempdir")?;
    let db_path = dir.path().join("virtual-fs.sqlite3");
    let sdk = TriggerSdk::initialize(&db_path).context("init sdk")?;
    Ok((dir, sdk))
}

fn build_registration(trigger_uuid: &str, name: &str, cron_expr: &str) -> TriggerRegistration {
    TriggerRegistration {
        trigger_uuid: trigger_uuid.to_string(),
        name: name.to_string(),
        version: "1.0".to_string(),
        precondition: vec![TriggerRule {
            rule: format!("cron('{}')", cron_expr),
            description: "cron".to_string(),
        }],
        condition: Vec::new(),
    }
}

fn seed_tree_fixture(sdk: &TriggerSdk, device_id: &str, image_uri: &str) -> Result<()> {
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "c1".to_string(),
            title: "Inbox".to_string(),
            trigger_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.things_upsert_collection(
        device_id,
        ThingCollectionUpsert {
            uuid: "c2".to_string(),
            title: "Later".to_string(),
            trigger_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )?;

    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "t1".to_string(),
            title: "Buy milk".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(json!({
                "markdown": "- milk\n\n[remi-entry](remi-entry://entry-1)\n\n![](remi-entry://entry-image)"
            })),
            collection_uuid: "c1".to_string(),
            trigger_uuid: None,
            parent_uuid: None,
            created_at: None,
            updated_at: None,
        },
    )?;
    sdk.things_upsert_thing(
        device_id,
        ThingUpsert {
            uuid: "t1-child".to_string(),
            title: "Skimmed".to_string(),
            datatype: ThingDatatype::Markdown,
            data: Some(json!({"markdown": "child"})),
            collection_uuid: "c1".to_string(),
            trigger_uuid: None,
            parent_uuid: Some("t1".to_string()),
            created_at: None,
            updated_at: None,
        },
    )?;

    sdk.things_add_content_entry(
        device_id,
        "t1",
        ContentEntry {
            id: "entry-1".to_string(),
            title: Some("Note".to_string()),
            order: 0.0,
            payload: ContentEntryPayload::Custom {
                content_type: "test-entry".to_string(),
                data: json!({"foo": 1}),
            },
        },
    )?;

    sdk.things_add_content_entry(
        device_id,
        "t1",
        ContentEntry {
            id: "entry-image".to_string(),
            title: Some("Image".to_string()),
            order: 1.0,
            payload: ContentEntryPayload::Image(ImageField::new(image_uri.to_string())),
        },
    )?;

    sdk.things_add_content_entry(
        device_id,
        "t1",
        ContentEntry {
            id: "entry-unused".to_string(),
            title: Some("Unused".to_string()),
            order: 2.0,
            payload: ContentEntryPayload::Custom {
                content_type: "test-entry".to_string(),
                data: json!({"unused": true}),
            },
        },
    )?;

    for index in 1..=6 {
        let trigger_uuid = format!("tr-{index}");
        sdk.register_trigger(build_registration(&trigger_uuid, &format!("Trigger {index}"), &format!("{} 9 * * *", index % 5)))?;
    }

    sdk.things_set_collection_trigger_uuid(device_id, "c1", Some("tr-1"))?;
    sdk.upsert_trigger_binding("tr-1", "collection", "c1")?;
    sdk.things_set_thing_trigger_uuid(device_id, "t1", Some("tr-2"))?;
    sdk.upsert_trigger_binding("tr-2", "thing", "t1")?;

    Ok(())
}

#[test]
fn virtual_fs_tree_and_read_cover_root_trigger_and_thing_files() -> Result<()> {
    let (dir, sdk) = init_sdk()?;
    let device_id = "device-a";
    let image_path = dir.path().join("sample.png");
    std::fs::write(&image_path, [137u8, 80, 78, 71]).context("write image")?;
    let image_uri = RemiUri::from_local_file(&image_path.to_string_lossy(), "image/png", device_id).to_uri_string();
    seed_tree_fixture(&sdk, device_id, &image_uri)?;

    let tree = sdk.tree_virtual_path(device_id, None)?;
    assert!(tree.contains("/"));
    assert!(tree.contains("trigger/"));
    assert!(tree.contains("Has 1 More"));
    assert!(tree.contains("collection/"));
    assert!(tree.contains("c1/ [name=\"Inbox\"]"));
    assert!(tree.contains("trigger [value=\"tr-1\"]"));
    assert!(tree.contains("t1/ [name=\"Buy milk\", status=\"none\"]"));
    assert!(tree.contains("status [value=\"none\"]"));
    assert!(tree.contains("allowed: none, in-progress, stalled, done"));

    let collection_name = sdk.read_virtual_path(device_id, "/collection/c1/name")?;
    assert_eq!(collection_name.content, "Inbox");

    let collection_trigger = sdk.read_virtual_path(device_id, "/collection/c1/trigger")?;
    assert_eq!(collection_trigger.content, "tr-1");

    let thing_name = sdk.read_virtual_path(device_id, "/collection/c1/things/t1/name")?;
    assert_eq!(thing_name.content, "Buy milk");

    let thing_trigger = sdk.read_virtual_path(device_id, "/collection/c1/things/t1/trigger")?;
    assert_eq!(thing_trigger.content, "tr-2");

    let thing_content = sdk.read_virtual_path(device_id, "/collection/c1/things/t1/content.md")?;
    assert!(thing_content.content.contains("- milk"));
    assert!(thing_content.content.contains("[内容](/collection/c1/things/t1/entries.0)"));
    assert!(thing_content.content.contains("[IMG](/collection/c1/things/t1/entries.1)"));
    assert!(!thing_content.content.contains("entries.2"));

    let entry = sdk.read_virtual_path(device_id, "/collection/c1/things/t1/entries.0")?;
    let entry_json: JsonValue = serde_json::from_str(&entry.content)?;
    assert_eq!(entry_json["title"], json!("Note"));
    assert_eq!(entry_json["payload"]["content_type"], json!("test-entry"));

    let trigger_name = sdk.read_virtual_path(device_id, "/trigger/tr-1/name")?;
    assert_eq!(trigger_name.content, "Trigger 1");

    let trigger_rule = sdk.read_virtual_path(device_id, "/trigger/tr-1/rule.json")?;
    let trigger_rule_json: JsonValue = serde_json::from_str(&trigger_rule.content)?;
    assert_eq!(trigger_rule_json["version"], json!("1.0"));
    assert!(trigger_rule_json["precondition"].is_array());

    Ok(())
}

#[test]
fn virtual_fs_edit_delete_and_move_cover_supported_paths() -> Result<()> {
    let (dir, sdk) = init_sdk()?;
    let device_id = "device-b";
    let image_path = dir.path().join("sample.png");
    std::fs::write(&image_path, [137u8, 80, 78, 71]).context("write image")?;
    let image_uri = RemiUri::from_local_file(&image_path.to_string_lossy(), "image/png", device_id).to_uri_string();
    seed_tree_fixture(&sdk, device_id, &image_uri)?;

    sdk.edit_virtual_path(device_id, "/collection/c1/name", "overwrite", Some(&json!("Inbox Renamed")), None, None, None)?;
    sdk.edit_virtual_path(device_id, "/collection/c1/trigger", "overwrite", Some(&json!("tr-3")), None, None, None)?;
    sdk.edit_virtual_path(device_id, "/collection/c1/things/t1/name", "overwrite", Some(&json!("Buy oat milk")), None, None, None)?;
    sdk.edit_virtual_path(device_id, "/collection/c1/things/t1/trigger", "overwrite", Some(&json!("")), None, None, None)?;
    sdk.edit_virtual_path(device_id, "/collection/c1/things/t1/status", "overwrite", Some(&json!("done")), None, None, None)?;
    sdk.edit_virtual_path(device_id, "/collection/c1/things/t1/content.md", "append", Some(&json!("- eggs")), None, None, None)?;
    sdk.edit_virtual_path(device_id, "/collection/c1/things/t1/entries.0", "overwrite", Some(&json!({
        "title": "Updated entry",
        "payload": {
            "type": "custom",
            "content_type": "test-entry",
            "data": {"foo": 2}
        }
    })), None, None, None)?;
    sdk.edit_virtual_path(device_id, "/trigger/tr-1/name", "overwrite", Some(&json!("Morning Trigger")), None, None, None)?;
    sdk.edit_virtual_path(device_id, "/trigger/tr-1/rule.json", "overwrite", Some(&json!({
        "version": "1.1",
        "precondition": [{"rule": "cron('15 9 * * *')", "description": "updated cron"}],
        "condition": []
    })), None, None, None)?;

    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/name")?.content, "Inbox Renamed");
    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/trigger")?.content, "tr-3");
    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/things/t1/name")?.content, "Buy oat milk");
    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/things/t1/trigger")?.content, "");
    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/things/t1/status")?.content, "done");
    let updated_content = sdk.read_virtual_path(device_id, "/collection/c1/things/t1/content.md")?.content;
    assert!(updated_content.contains("- eggs"));
    assert!(updated_content.contains("[内容](/collection/c1/things/t1/entries.0)"));
    assert!(updated_content.contains("[IMG](/collection/c1/things/t1/entries.1)"));
    assert_eq!(sdk.read_virtual_path(device_id, "/trigger/tr-1/name")?.content, "Morning Trigger");

    let updated_rule: JsonValue = serde_json::from_str(&sdk.read_virtual_path(device_id, "/trigger/tr-1/rule.json")?.content)?;
    assert_eq!(updated_rule["version"], json!("1.1"));
    assert_eq!(updated_rule["precondition"][0]["rule"], json!("cron('15 9 * * *')"));

    sdk.move_virtual_path(device_id, "/collection/c1/things/t1", "/collection/c2/things")?;
    let snapshot = sdk.things_list_snapshot_lite(device_id)?;
    let moved = snapshot.things.iter().find(|item| item.uuid == "t1").context("moved thing")?;
    assert_eq!(moved.collection_uuid, "c2");

    sdk.delete_virtual_path(device_id, "/collection/c2/things/t1/entries.0")?;
    let remaining_entries = sdk.things_get_content_entries(device_id, "t1")?;
    assert_eq!(remaining_entries.len(), 2);
    assert_eq!(remaining_entries[0].id, "entry-image");
    assert_eq!(remaining_entries[1].id, "entry-unused");

    sdk.delete_virtual_path(device_id, "/collection/c2/things/t1/entries.0")?;
    let remaining_entries = sdk.things_get_content_entries(device_id, "t1")?;
    assert_eq!(remaining_entries.len(), 1);
    assert_eq!(remaining_entries[0].id, "entry-unused");

    sdk.delete_virtual_path(device_id, "/collection/c2/things/t1/entries.0")?;
    assert!(sdk.things_get_content_entries(device_id, "t1")?.is_empty());

    sdk.delete_virtual_path(device_id, "/trigger/tr-1")?;
    assert!(!sdk.list_triggers()?.iter().any(|item| item.trigger_id == "tr-1"));

    sdk.delete_virtual_path(device_id, "/collection/c2/things/t1")?;
    assert!(sdk.things_get_thing_markdown(device_id, "t1")?.is_none());

    let created_trigger = sdk.create_virtual_path(device_id, "/trigger", "trigger", Some("Wake Up"), None, None, Some("/collection/c1"), Some("tr-new"))?;
    assert_eq!(created_trigger["uuid"], json!("tr-new"));
    assert_eq!(created_trigger["path"], json!("/trigger/tr-new"));
    assert_eq!(sdk.read_virtual_path(device_id, "/trigger/tr-new/name")?.content, "Wake Up");
    let created_trigger_rule: JsonValue = serde_json::from_str(&sdk.read_virtual_path(device_id, "/trigger/tr-new/rule.json")?.content)?;
    assert_eq!(created_trigger_rule["version"], json!("1.0"));
    assert_eq!(created_trigger_rule["precondition"], json!([]));
    assert_eq!(created_trigger_rule["condition"], json!([]));
    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/trigger")?.content, "tr-new");

    let created_collection = sdk.create_virtual_path(device_id, "/collection", "collection", None, None, None, None, Some("c3"))?;
    assert_eq!(created_collection["uuid"], json!("c3"));
    assert_eq!(created_collection["path"], json!("/collection/c3"));
    assert!(created_collection.get("scaffold_tree").is_none());
    assert!(created_collection.get("scaffold_paths").is_none());

    let created_thing = sdk.create_virtual_path(device_id, "/collection/c1/things", "thing", Some("Draft"), Some("hello"), None, None, Some("t-new"))?;
    assert_eq!(created_thing["uuid"], json!("t-new"));
    assert_eq!(created_thing["path"], json!("/collection/c1/things/t-new"));
    assert!(created_thing.get("scaffold_tree").is_none());
    assert!(created_thing.get("scaffold_paths").is_none());
    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/things/t-new/name")?.content, "Draft");
    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/things/t-new/content.md")?.content, "hello");

    let chat_image_uri = "remi://remote/chat-image.png?type=image%2Fpng";
    let created_image = sdk.create_virtual_path(
        device_id,
        "/collection/c1/things/t-new",
        "image",
        Some("Chat Snapshot"),
        None,
        Some(chat_image_uri),
        None,
        Some("entry-chat-image"),
    )?;
    assert_eq!(created_image["uuid"], json!("entry-chat-image"));
    assert_eq!(created_image["path"], json!("/collection/c1/things/t-new/entries.0"));
    assert_eq!(created_image["source_uri"], json!(chat_image_uri));
    let created_image_entry: JsonValue = serde_json::from_str(&sdk.read_virtual_path(device_id, "/collection/c1/things/t-new/entries.0")?.content)?;
    assert_eq!(created_image_entry["title"], json!("Chat Snapshot"));
    assert_eq!(created_image_entry["payload"]["type"], json!("image"));
    assert_eq!(created_image_entry["payload"]["uri"], json!(chat_image_uri));

    let created_thing_trigger = sdk.create_virtual_path(device_id, "/trigger", "trigger", Some("Thing Trigger"), None, None, Some("/collection/c1/things/t-new/trigger"), Some("tr-thing"))?;
    assert_eq!(created_thing_trigger["uuid"], json!("tr-thing"));
    assert_eq!(sdk.read_virtual_path(device_id, "/collection/c1/things/t-new/trigger")?.content, "tr-thing");

    Ok(())
}

#[test]
fn virtual_fs_returns_friendly_errors_for_invalid_and_unsupported_paths() -> Result<()> {
    let (dir, sdk) = init_sdk()?;
    let device_id = "device-c";
    let image_path = dir.path().join("sample.png");
    std::fs::write(&image_path, [137u8, 80, 78, 71]).context("write image")?;
    let image_uri = RemiUri::from_local_file(&image_path.to_string_lossy(), "image/png", device_id).to_uri_string();
    seed_tree_fixture(&sdk, device_id, &image_uri)?;

    let err = sdk.move_virtual_path(device_id, "/trigger/tr-1", "/collection/c1/things")
        .expect_err("trigger move should fail");
    assert!(err.to_string().contains("move_unsupported"));

    let err = sdk.delete_virtual_path(device_id, "/collection/c1/name")
        .expect_err("deleting a file node should fail");
    assert!(err.to_string().contains("delete_unsupported"));

    let err = sdk.read_virtual_path(device_id, "/collection/missing/name")
        .expect_err("missing collection should fail");
    assert!(err.to_string().contains("collection_not_found"));

    let err = sdk.create_virtual_path(device_id, "/collection/c1", "trigger", Some("Bad Parent"), None, None, None, Some("tr-bad"))
        .expect_err("trigger create under collection should fail");
    assert!(err.to_string().contains("invalid_parent"));

    let err = sdk.create_virtual_path(device_id, "/trigger", "trigger", Some("Bad Bind"), None, None, Some("/trigger/tr-1"), Some("tr-bad-bind"))
        .expect_err("trigger bind to trigger path should fail");
    assert!(err.to_string().contains("invalid_bind_path"));

    let err = sdk.create_virtual_path(device_id, "/collection/c1/things/t1", "image", Some("Bad Image"), None, Some("https://example.com/foo.png"), None, Some("entry-bad-image"))
        .expect_err("image create with non-remi uri should fail");
    assert!(err.to_string().contains("invalid_source_uri"));

    let err = sdk.create_virtual_path(device_id, "/collection/c1/things", "image", Some("Bad Parent"), None, Some("remi://remote/chat.png?type=image%2Fpng"), None, Some("entry-bad-parent"))
        .expect_err("image create under things directory should fail");
    assert!(err.to_string().contains("invalid_parent"));

    let err = sdk.edit_virtual_path(device_id, "/collection/c1/things/t1", "overwrite", Some(&json!("nope")), None, None, None)
        .expect_err("editing a directory path should fail");
    assert!(err.to_string().contains("is_directory"));

    Ok(())
}
