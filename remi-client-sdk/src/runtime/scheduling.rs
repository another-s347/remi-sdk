use super::{TriggerRule, default_timezone};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Duration, FixedOffset, Utc};
use croner::Cron;
use rule_trigger_engine::{MonitoringEvent, Rule as EngineRule, TriggerConfig};
use std::str::FromStr;

pub(super) fn compute_next_fire(
    cron_expr: &str,
    anchor: DateTime<Utc>,
    now: Option<DateTime<Utc>>, // if provided, ensure next fire is after this time
) -> Result<Option<DateTime<Utc>>> {
    let schedule = Cron::from_str(cron_expr).context("Invalid cron expression")?;
    let tz = default_timezone();
    let effective_anchor = match now {
        Some(now) if now > anchor => now,
        _ => anchor,
    };
    let anchor_local = effective_anchor.with_timezone(&tz);
    let next_local = schedule
        .find_next_occurrence(&anchor_local, false)
        .context("Invalid cron expression")?;
    Ok(Some(next_local.with_timezone(&Utc)))
}

/// Filter events that fall within a time window ending at `current` and spanning `window_minutes` back.
pub(super) fn filter_events_at_time(
    all_events: &[MonitoringEvent],
    current: DateTime<Utc>,
    window_minutes: i64,
) -> Vec<MonitoringEvent> {
    let start = current - Duration::minutes(window_minutes);
    all_events
        .iter()
        .filter_map(|e| {
            let dt = DateTime::parse_from_rfc3339(&e.timestamp)
                .ok()?
                .with_timezone(&Utc);
            if dt >= start && dt <= current {
                Some(e.clone())
            } else {
                None
            }
        })
        .collect()
}

pub(super) fn select_current_event<'a>(
    events: &'a [MonitoringEvent],
    timings: &[rule_trigger_engine::TriggerTiming],
    fire_time: DateTime<Utc>,
) -> Option<&'a MonitoringEvent> {
    let event_types: Vec<&str> = timings
        .iter()
        .filter_map(|timing| match timing {
            rule_trigger_engine::TriggerTiming::Event { event_type } => Some(event_type.as_str()),
            _ => None,
        })
        .collect();

    events
        .iter()
        .filter_map(|event| {
            let matches_type = event_types.is_empty()
                || event_types
                    .iter()
                    .any(|event_type| *event_type == event.event_type);
            if !matches_type {
                return None;
            }

            let timestamp = DateTime::parse_from_rfc3339(&event.timestamp)
                .ok()?
                .with_timezone(&Utc);
            if timestamp > fire_time {
                return None;
            }

            Some((timestamp, event))
        })
        .max_by_key(|(timestamp, _)| *timestamp)
        .map(|(_, event)| event)
}

#[cfg(test)]
pub(super) fn extract_cron_from_preconditions(preconditions: &[TriggerRule]) -> Option<String> {
    for rule in preconditions {
        // Look for cron() function calls in the rule
        let rule_text = rule.rule.trim();
        if rule_text.starts_with("cron(") {
            // Extract the cron expression from cron('...') or cron("...")
            if let Some(start) = rule_text.find('\'') {
                if let Some(end) = rule_text[start + 1..].find('\'') {
                    return Some(rule_text[start + 1..start + 1 + end].to_string());
                }
            }
            if let Some(start) = rule_text.find('"') {
                if let Some(end) = rule_text[start + 1..].find('"') {
                    return Some(rule_text[start + 1..start + 1 + end].to_string());
                }
            }
        }
    }
    None
}

pub(super) fn extract_timings_from_rules(
    preconditions: &[TriggerRule],
    conditions: &[TriggerRule],
) -> Result<Vec<rule_trigger_engine::TriggerTiming>> {
    let precondition_rules: Vec<EngineRule> = preconditions
        .iter()
        .cloned()
        .map(|rule| EngineRule {
            rule: rule.rule,
            description: rule.description,
        })
        .collect();
    let condition_rules: Vec<EngineRule> = conditions
        .iter()
        .cloned()
        .map(|rule| EngineRule {
            rule: rule.rule,
            description: rule.description,
        })
        .collect();

    // Dummy values: timing extraction only uses rule bodies.
    let config = TriggerConfig {
        name: "timing-extract".to_string(),
        version: "v1".to_string(),
        precondition: precondition_rules,
        condition: condition_rules,
    };

    config
        .extract_timing()
        .map_err(|err| anyhow!("Failed to extract timing: {err}"))
}

pub(super) fn extract_repeat_frequency_from_conditions(
    conditions: &[TriggerRule],
) -> Option<rule_trigger_engine::RepeatFrequency> {
    for rule in conditions {
        let rule_text = rule.rule.trim();

        if let Some(arg) = rule_text
            .strip_prefix("repeat_per_day(")
            .and_then(|s| s.strip_suffix(')'))
        {
            let value = arg.trim().parse::<u32>().ok()?;
            if value > 0 {
                return Some(rule_trigger_engine::RepeatFrequency::PerDay(value));
            }
        }

        if let Some(arg) = rule_text
            .strip_prefix("repeat_per_week(")
            .and_then(|s| s.strip_suffix(')'))
        {
            let value = arg.trim().parse::<u32>().ok()?;
            if value > 0 {
                return Some(rule_trigger_engine::RepeatFrequency::PerWeek(value));
            }
        }
    }
    None
}

pub(super) fn repeat_min_gap(freq: &rule_trigger_engine::RepeatFrequency) -> Option<Duration> {
    match *freq {
        rule_trigger_engine::RepeatFrequency::PerDay(times) if times > 0 => {
            let seconds = (24 * 60 * 60) / i64::from(times);
            Some(Duration::seconds(seconds.max(1)))
        }
        rule_trigger_engine::RepeatFrequency::PerWeek(times) if times > 0 => {
            let seconds = (7 * 24 * 60 * 60) / i64::from(times);
            Some(Duration::seconds(seconds.max(1)))
        }
        _ => None,
    }
}

pub(super) fn normalize_timer_preconditions(
    preconditions: &[TriggerRule],
    anchor: DateTime<Utc>,
    timezone_offset: &str,
) -> Result<Vec<TriggerRule>> {
    preconditions
        .iter()
        .cloned()
        .map(|mut rule| {
            let timings = extract_timings_from_rules(&[rule.clone()], &[])?;
            if let Some(rule_trigger_engine::TriggerTiming::Timer { value }) = timings.first() {
                let normalized =
                    rule_trigger_engine::normalize_timer_literal(value, anchor, timezone_offset)
                        .map_err(|err| {
                            anyhow!(
                                "Failed to normalize timer precondition '{}': {err}",
                                rule.description
                            )
                        })?;
                rule.rule = format!("timer(\"{}\")", normalized);
            }
            Ok(rule)
        })
        .collect()
}

pub(super) fn resolve_registration_next_fire(
    timings: &[rule_trigger_engine::TriggerTiming],
    anchor: DateTime<Utc>,
    timezone_offset: &str,
) -> Result<Option<DateTime<Utc>>> {
    let mut earliest: Option<DateTime<Utc>> = None;
    let mut has_supported = false;
    let mut has_event = false;

    for timing in timings {
        match timing {
            rule_trigger_engine::TriggerTiming::Cron { expression } => {
                has_supported = true;
                if let Some(next) = compute_next_fire(expression, anchor, Some(anchor))? {
                    earliest = Some(match earliest {
                        Some(current) => current.min(next),
                        None => next,
                    });
                }
            }
            rule_trigger_engine::TriggerTiming::Timer { value } => {
                has_supported = true;
                let resolved =
                    rule_trigger_engine::resolve_timer_literal(value, anchor, timezone_offset)
                        .map_err(|err| {
                            anyhow!("Failed to resolve timer precondition '{value}': {err}")
                        })?;
                let due_at = if resolved <= anchor { anchor } else { resolved };
                earliest = Some(match earliest {
                    Some(current) => current.min(due_at),
                    None => due_at,
                });
            }
            rule_trigger_engine::TriggerTiming::Event { .. } => {
                has_supported = true;
                has_event = true;
            }
            rule_trigger_engine::TriggerTiming::RepeatFrequency { .. } => {}
        }
    }

    if let Some(next) = earliest {
        Ok(Some(next))
    } else if has_event {
        Ok(None)
    } else if has_supported {
        Ok(None)
    } else {
        Err(anyhow!(
            "No supported timing found in trigger rules (expected cron(...), timer(...), or event(...))."
        ))
    }
}

pub(super) fn resolve_post_run_next_fire(
    timings: &[rule_trigger_engine::TriggerTiming],
    fire_time: DateTime<Utc>,
    lower_bound: DateTime<Utc>,
    timezone_offset: &str,
) -> Result<Option<DateTime<Utc>>> {
    let mut earliest: Option<DateTime<Utc>> = None;

    for timing in timings {
        match timing {
            rule_trigger_engine::TriggerTiming::Cron { expression } => {
                if let Some(next) = compute_next_fire(expression, fire_time, Some(lower_bound))? {
                    earliest = Some(match earliest {
                        Some(current) => current.min(next),
                        None => next,
                    });
                }
            }
            rule_trigger_engine::TriggerTiming::Timer { value } => {
                let resolved =
                    rule_trigger_engine::resolve_timer_literal(value, fire_time, timezone_offset)
                        .map_err(|err| {
                        anyhow!("Failed to resolve timer precondition '{value}': {err}")
                    })?;
                if resolved > lower_bound {
                    earliest = Some(match earliest {
                        Some(current) => current.min(resolved),
                        None => resolved,
                    });
                }
            }
            rule_trigger_engine::TriggerTiming::Event { .. }
            | rule_trigger_engine::TriggerTiming::RepeatFrequency { .. } => {}
        }
    }

    Ok(earliest)
}

pub(super) fn build_trigger_occurrences(
    timings: &[rule_trigger_engine::TriggerTiming],
    all_events: &[MonitoringEvent],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    registration_anchor: DateTime<Utc>,
    timezone_offset: &str,
) -> Result<Vec<DateTime<Utc>>> {
    use std::collections::BTreeSet;

    let mut occurrences = BTreeSet::new();
    let tz = FixedOffset::from_str(timezone_offset).unwrap_or_else(|_| default_timezone());

    for timing in timings {
        match timing {
            rule_trigger_engine::TriggerTiming::Cron { expression } => {
                let schedule = Cron::from_str(expression)
                    .with_context(|| format!("Invalid cron expression: {expression}"))?;
                let mut next_local = schedule
                    .find_next_occurrence(&start.with_timezone(&tz), false)
                    .with_context(|| {
                        format!("Failed to find next occurrence for cron: {expression}")
                    })?;
                while next_local.with_timezone(&Utc) <= end {
                    occurrences.insert(next_local.with_timezone(&Utc));
                    next_local = schedule
                        .find_next_occurrence(&next_local, false)
                        .with_context(|| {
                            format!("Failed to find next occurrence for cron: {expression}")
                        })?;
                }
            }
            rule_trigger_engine::TriggerTiming::Timer { value } => {
                let at = rule_trigger_engine::resolve_timer_literal(
                    value,
                    registration_anchor,
                    timezone_offset,
                )
                .map_err(|err| anyhow!("Failed to resolve timer precondition '{value}': {err}"))?;
                if at >= start && at <= end {
                    occurrences.insert(at);
                }
            }
            rule_trigger_engine::TriggerTiming::Event { event_type } => {
                for event in all_events {
                    if event.event_type != *event_type {
                        continue;
                    }
                    if let Ok(at) = DateTime::parse_from_rfc3339(&event.timestamp) {
                        let at = at.with_timezone(&Utc);
                        if at >= start && at <= end {
                            occurrences.insert(at);
                        }
                    }
                }
            }
            rule_trigger_engine::TriggerTiming::RepeatFrequency { .. } => {}
        }
    }

    Ok(occurrences.into_iter().collect())
}

pub(super) fn describe_timing_sources(timings: &[rule_trigger_engine::TriggerTiming]) -> String {
    if timings.is_empty() {
        return "none".to_string();
    }

    timings
        .iter()
        .map(|timing| match timing {
            rule_trigger_engine::TriggerTiming::Cron { expression } => {
                format!("cron({expression})")
            }
            rule_trigger_engine::TriggerTiming::Timer { value } => format!("timer({value})"),
            rule_trigger_engine::TriggerTiming::Event { event_type } => {
                format!("event({event_type})")
            }
            rule_trigger_engine::TriggerTiming::RepeatFrequency { frequency } => match frequency {
                rule_trigger_engine::RepeatFrequency::PerDay(n) => format!("repeat_per_day({n})"),
                rule_trigger_engine::RepeatFrequency::PerWeek(n) => {
                    format!("repeat_per_week({n})")
                }
            },
        })
        .collect::<Vec<_>>()
        .join(", ")
}
