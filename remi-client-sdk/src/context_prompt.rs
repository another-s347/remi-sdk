use crate::things_crdt::{ThingEntry, ThingsSnapshot};
use crate::types::TriggerInfo;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy)]
pub struct EventTypeInfo {
    pub event_type: &'static str,
    pub permission_id: &'static str,
    pub description: &'static str,
    pub metadata_fields: &'static [&'static str],
    pub notes: &'static str,
    pub limitations: &'static str,
}

pub const EVENT_TYPES: &[EventTypeInfo] = &[
    EventTypeInfo {
        event_type: "Connectivity",
        permission_id: "network_state",
        description: "网络状态变化（WiFi/移动数据切换）",
        metadata_fields: &["states"],
        notes: "可用于判断用户是否在线/网络类型是否变化（粗粒度）",
        limitations: "不提供 Wi-Fi SSID 等可识别信息；网络切换仅是线索",
    },
    EventTypeInfo {
        event_type: "Location",
        permission_id: "location",
        description: "GPS位置更新",
        metadata_fields: &[
            "latitude",
            "longitude",
            "accuracy",
            "speed",
            "heading",
            "distanceFromLastMeters",
        ],
        notes: "可用于判断用户移动状态、是否到达某地点",
        limitations: "室内/弱信号误差大；后台会被系统降频；无法稳定监测几米级小范围移动",
    },
    EventTypeInfo {
        event_type: "Bluetooth",
        permission_id: "bluetooth",
        description: "蓝牙适配器状态变化",
        metadata_fields: &["state"],
        notes: "可用于判断用户是否连接特定蓝牙设备",
        limitations: "通常只能拿到开/关等粗粒度状态；后台扫描/连接会被系统强限制，不能可靠推断附近设备",
    },
    EventTypeInfo {
        event_type: "Camera",
        permission_id: "camera",
        description: "相机拍摄事件",
        metadata_fields: &[],
        notes: "可用于记录用户拍照行为",
        limitations: "一般仅能记录 App 内部触发的拍摄；无法全局监听系统/其他 App 的拍照行为",
    },
];

const MAX_COLLECTIONS: usize = 20;
const MAX_THINGS_PER_COLLECTION: usize = 50;
const MAX_SUB_THINGS_PER_THING: usize = 10;
const MAX_TRIGGERS: usize = 50;

fn parse_rfc3339_ts_millis(input: &str) -> i64 {
    DateTime::parse_from_rfc3339(input)
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis())
        .unwrap_or(0)
}

fn yaml_quote(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

fn yaml_from_json_value(value: &Value, indent: usize) -> String {
    let pad = " ".repeat(indent);
    match value {
        Value::Null => format!("{pad}null"),
        Value::Bool(b) => format!("{pad}{b}"),
        Value::Number(n) => format!("{pad}{n}"),
        Value::String(s) => format!("{pad}{}", yaml_quote(s)),
        Value::Array(arr) => {
            if arr.is_empty() {
                return format!("{pad}[]");
            }
            let mut out = String::new();
            for item in arr {
                match item {
                    Value::Object(_) | Value::Array(_) => {
                        out.push_str(&format!(
                            "{pad}-\n{}\n",
                            yaml_from_json_value(item, indent + 2)
                        ));
                    }
                    _ => {
                        let rendered = yaml_from_json_value(item, 0).trim().to_string();
                        out.push_str(&format!("{pad}- {rendered}\n"));
                    }
                }
            }
            out.trim_end().to_string()
        }
        Value::Object(map) => {
            if map.is_empty() {
                return format!("{pad}{{}}");
            }
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = String::new();
            for key in keys {
                let v = &map[key];
                match v {
                    Value::Object(_) | Value::Array(_) => {
                        out.push_str(&format!(
                            "{pad}{key}:\n{}\n",
                            yaml_from_json_value(v, indent + 2)
                        ));
                    }
                    _ => {
                        let rendered = yaml_from_json_value(v, 0).trim().to_string();
                        out.push_str(&format!("{pad}{key}: {rendered}\n"));
                    }
                }
            }
            out.trim_end().to_string()
        }
    }
}

fn build_enabled_events_section(granted_permissions: &[String]) -> String {
    let perms: BTreeSet<&str> = granted_permissions.iter().map(|s| s.as_str()).collect();
    let enabled: Vec<EventTypeInfo> = EVENT_TYPES
        .iter()
        .copied()
        .filter(|info| perms.contains(info.permission_id))
        .collect();

    if enabled.is_empty() {
        return "## Enabled Events\n\n目前仅可依赖时间与其他api判断".to_string();
    }

    let mut out = String::new();
    out.push_str("## Enabled Events\n\n```yaml\n");
    for info in enabled {
        let metadata_inline = if info.metadata_fields.is_empty() {
            "[]".to_string()
        } else {
            format!("[{}]", info.metadata_fields.join(", "))
        };
        out.push_str(&format!(
            "- type: {}\n  description: {}\n  metadata: {}\n  notes: {}\n  limitations: {}\n\n",
            info.event_type,
            yaml_quote(info.description),
            metadata_inline,
            yaml_quote(info.notes),
            yaml_quote(info.limitations)
        ));
    }
    out.push_str("```");
    out
}

fn build_user_data_overview(snapshot: &ThingsSnapshot, triggers: &[TriggerInfo]) -> String {
    let mut collections = snapshot.collections.clone();
    collections.sort_by(|a, b| {
        parse_rfc3339_ts_millis(&b.updated_at).cmp(&parse_rfc3339_ts_millis(&a.updated_at))
    });
    collections.truncate(MAX_COLLECTIONS);

    let mut things = snapshot.things.clone();
    things.sort_by(|a, b| {
        parse_rfc3339_ts_millis(&b.updated_at).cmp(&parse_rfc3339_ts_millis(&a.updated_at))
    });

    let mut things_by_collection: BTreeMap<String, Vec<ThingEntry>> = BTreeMap::new();
    for t in things {
        things_by_collection
            .entry(t.collection_uuid.clone())
            .or_default()
            .push(t);
    }

    let mut out = String::new();
    out.push_str("## User Data Overview\n\n```yaml\n");
    out.push_str("collections:\n");

    for c in collections {
        out.push_str(&format!(
            "  - title: {}\n    uuid: {}\n",
            yaml_quote(&c.title),
            yaml_quote(&c.uuid)
        ));
        out.push_str("    things:\n");

        let mut top_level: Vec<ThingEntry> = things_by_collection
            .get(&c.uuid)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|t| t.parent_uuid.is_none())
            .collect();
        top_level.sort_by(|a, b| {
            parse_rfc3339_ts_millis(&b.updated_at).cmp(&parse_rfc3339_ts_millis(&a.updated_at))
        });
        top_level.truncate(MAX_THINGS_PER_COLLECTION);

        for t in top_level {
            out.push_str(&format!(
                "      - title: {}\n        uuid: {}\n",
                yaml_quote(&t.title),
                yaml_quote(&t.uuid)
            ));
            if let Some(trigger_uuid) = t.trigger_uuid.as_ref() {
                if !trigger_uuid.is_empty() {
                    out.push_str(&format!(
                        "        trigger_uuid: {}\n",
                        yaml_quote(trigger_uuid)
                    ));
                }
            }

            let mut sub: Vec<ThingEntry> = things_by_collection
                .get(&c.uuid)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|child| child.parent_uuid.as_deref() == Some(t.uuid.as_str()))
                .collect();
            sub.sort_by(|a, b| {
                parse_rfc3339_ts_millis(&b.updated_at).cmp(&parse_rfc3339_ts_millis(&a.updated_at))
            });
            sub.truncate(MAX_SUB_THINGS_PER_THING);

            if !sub.is_empty() {
                out.push_str("        sub_things:\n");
                for st in sub {
                    out.push_str(&format!(
                        "          - title: {}\n            uuid: {}\n",
                        yaml_quote(&st.title),
                        yaml_quote(&st.uuid)
                    ));
                    if let Some(trigger_uuid) = st.trigger_uuid.as_ref() {
                        if !trigger_uuid.is_empty() {
                            out.push_str(&format!(
                                "            trigger_uuid: {}\n",
                                yaml_quote(trigger_uuid)
                            ));
                        }
                    }
                }
            }
        }
    }

    out.push_str("\ntriggers:\n");
    for trg in triggers.iter().take(MAX_TRIGGERS) {
        // Best-effort: local triggers don't currently persist a dedicated `user_request`.
        // We surface `name` for both `title` and `user_request` so the agent has a human hint.
        out.push_str(&format!(
            "  - title: {}\n    uuid: {}\n    user_request: {}\n",
            yaml_quote(&trg.name),
            yaml_quote(&trg.trigger_id),
            yaml_quote(&trg.name)
        ));
    }

    out.push_str("```");
    out
}

fn build_active_context_section(active_context_json: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = active_context_json else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }

    let value: Value = serde_json::from_str(raw).context("Invalid active_context_json")?;
    if value.is_null() {
        return Ok(None);
    }

    let rendered = yaml_from_json_value(&value, 0);
    if rendered.trim().is_empty() {
        return Ok(None);
    }

    Ok(Some(format!(
        "## Active Context\n\n```yaml\n{}\n```",
        rendered
    )))
}

pub fn build_context_prompt_markdown(
    granted_permissions: &[String],
    snapshot: &ThingsSnapshot,
    triggers: &[TriggerInfo],
    active_context_json: Option<&str>,
) -> Result<String> {
    let enabled_events = build_enabled_events_section(granted_permissions);
    let user_data = build_user_data_overview(snapshot, triggers);
    let active_context = build_active_context_section(active_context_json)?;

    let mut sections = vec![enabled_events, user_data];
    if let Some(section) = active_context {
        sections.push(section);
    }
    Ok(sections.join("\n\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_events_fallback_when_empty() {
        let snapshot = ThingsSnapshot {
            collections: vec![],
            things: vec![],
        };
        let out = build_context_prompt_markdown(&[], &snapshot, &[], None).unwrap();
        assert!(out.contains("## Enabled Events"));
        assert!(out.contains("目前仅可依赖时间与其他api判断"));
    }
}
