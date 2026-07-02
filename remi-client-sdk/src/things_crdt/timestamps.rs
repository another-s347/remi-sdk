use super::*;

const INTERNAL_CREATED_AT_ATTR_KEY: &str = "__remi_created_at";
const INTERNAL_UPDATED_AT_ATTR_KEY: &str = "__remi_updated_at";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct EntityTimestamps {
    pub(super) created_at: Option<String>,
    pub(super) updated_at: Option<String>,
}

pub(super) fn trim_timestamp_value(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub(super) fn normalize_timestamp_value(value: Option<String>) -> Result<Option<String>> {
    value
        .and_then(|item| {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .map(|timestamp| {
            parse_domain_datetime(&timestamp)
                .map(remi_things_crdt::format_domain_datetime)
                .map_err(anyhow::Error::msg)
        })
        .transpose()
}

pub(super) fn now_timestamp_rfc3339() -> String {
    remi_things_crdt::format_domain_datetime(chrono::Utc::now())
}

pub(super) fn extract_entity_timestamps(attrs: Option<&Value>) -> EntityTimestamps {
    let Some(Value::Object(map)) = attrs else {
        return EntityTimestamps::default();
    };

    EntityTimestamps {
        created_at: trim_timestamp_value(
            map.get(INTERNAL_CREATED_AT_ATTR_KEY)
                .and_then(|value| value.as_str())
                .map(str::to_string),
        ),
        updated_at: trim_timestamp_value(
            map.get(INTERNAL_UPDATED_AT_ATTR_KEY)
                .and_then(|value| value.as_str())
                .map(str::to_string),
        ),
    }
}

pub(super) fn strip_internal_timestamp_attrs(attrs: Option<&Value>) -> Option<Value> {
    match attrs {
        Some(Value::Object(map)) => {
            let filtered = map
                .iter()
                .filter(|(key, _)| {
                    *key != INTERNAL_CREATED_AT_ATTR_KEY && *key != INTERNAL_UPDATED_AT_ATTR_KEY
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<serde_json::Map<String, Value>>();

            if filtered.is_empty() {
                None
            } else {
                Some(Value::Object(filtered))
            }
        }
        Some(other) => Some(other.clone()),
        None => None,
    }
}

pub(super) fn extract_collection_card_jsx(attrs: Option<&Value>) -> Option<String> {
    attrs
        .and_then(|value| value.as_object())
        .and_then(|map| map.get("card_jsx"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub(super) fn merge_entity_timestamps_into_attrs(
    attrs: Option<&Value>,
    created_at: Option<String>,
    updated_at: Option<String>,
) -> Result<String> {
    let mut map = match attrs {
        Some(Value::Object(existing)) => existing.clone(),
        Some(_) => serde_json::Map::new(),
        None => serde_json::Map::new(),
    };

    match normalize_timestamp_value(created_at)? {
        Some(value) => {
            map.insert(
                INTERNAL_CREATED_AT_ATTR_KEY.to_string(),
                Value::String(value),
            );
        }
        None => {
            map.remove(INTERNAL_CREATED_AT_ATTR_KEY);
        }
    }

    match normalize_timestamp_value(updated_at)? {
        Some(value) => {
            map.insert(
                INTERNAL_UPDATED_AT_ATTR_KEY.to_string(),
                Value::String(value),
            );
        }
        None => {
            map.remove(INTERNAL_UPDATED_AT_ATTR_KEY);
        }
    }

    serde_json::to_string(&Value::Object(map)).context("Failed to encode timestamp attrs")
}

pub(super) fn resolve_entity_timestamps(
    existing_attrs: Option<&Value>,
    provided_created_at: Option<String>,
    provided_updated_at: Option<String>,
    existed: bool,
) -> Result<EntityTimestamps> {
    let existing = extract_entity_timestamps(existing_attrs);
    let fallback_now = now_timestamp_rfc3339();

    let created_at = normalize_timestamp_value(provided_created_at)?
        .or(existing.created_at)
        .unwrap_or_else(|| fallback_now.clone());
    let updated_at = normalize_timestamp_value(provided_updated_at)?.unwrap_or_else(|| {
        if existed {
            fallback_now.clone()
        } else {
            created_at.clone()
        }
    });

    Ok(EntityTimestamps {
        created_at: Some(created_at),
        updated_at: Some(updated_at),
    })
}
