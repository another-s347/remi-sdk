//! Interrupt handler trait and registration.

use crate::chat_types::{InterruptAction, RichHandlerResult};
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

// ═══════════════════════════════════════════════════════════════════════════════
// Handler Trait
// ═══════════════════════════════════════════════════════════════════════════════

/// Handler for a specific interrupt type
pub trait InterruptHandler: Send + Sync {
    /// Process the interrupt and return a plain JSON resume value.
    ///
    /// Returns `Ok(resume_value)` on success, which will be sent to continue the chat.
    /// Returns `Err(message)` if handling fails.
    fn handle(&self, interrupt_id: &str, payload: &JsonValue) -> Result<JsonValue, String>;

    /// Process the interrupt and return a rich result (text or multimodal).
    ///
    /// Override this method to return image or other multimodal content.
    /// The default implementation wraps the `handle` result in `RichHandlerResult::Json`.
    fn handle_rich(&self, interrupt_id: &str, payload: &JsonValue) -> Result<RichHandlerResult, String> {
        self.handle(interrupt_id, payload).map(RichHandlerResult::Json)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Handler Registry
// ═══════════════════════════════════════════════════════════════════════════════

/// Registry of interrupt handlers by type
#[derive(Default)]
pub struct InterruptHandlerRegistry {
    handlers: HashMap<String, Arc<dyn InterruptHandler>>,
}

impl InterruptHandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler for a specific interrupt type
    pub fn register<H: InterruptHandler + 'static>(
        &mut self,
        interrupt_type: impl Into<String>,
        handler: H,
    ) {
        self.handlers
            .insert(interrupt_type.into(), Arc::new(handler));
    }

    /// Register a handler (builder pattern)
    pub fn with_handler<H: InterruptHandler + 'static>(
        mut self,
        interrupt_type: impl Into<String>,
        handler: H,
    ) -> Self {
        self.register(interrupt_type, handler);
        self
    }

    /// Process an interrupt using registered handlers.
    /// Returns an InterruptAction with the interrupt_id -> resume_value mapping.
    pub fn process(&self, interrupt_id: &str, payload: &JsonValue) -> InterruptAction {
        use crate::chat_types::PendingInterruptInfo;

        // Extract interrupt type from payload
        let interrupt_type = extract_interrupt_type(payload);
        tracing::info!(interrupt_id = %interrupt_id, interrupt_type = %interrupt_type, "[InterruptHandlerRegistry] Processing interrupt");

        if let Some(handler) = self.handlers.get(&interrupt_type) {
            tracing::info!(interrupt_type = %interrupt_type, "[InterruptHandlerRegistry] Found handler, invoking");
            match handler.handle_rich(interrupt_id, payload) {
                Ok(rich_result) => {
                    tracing::info!(interrupt_type = %interrupt_type, "[InterruptHandlerRegistry] Handler succeeded, AutoResume");
                    let mut map = HashMap::new();
                    map.insert(interrupt_id.to_string(), rich_result);
                    InterruptAction::AutoResume(map)
                }
                Err(e) => {
                    tracing::warn!(interrupt_id, interrupt_type, error = %e, "[InterruptHandlerRegistry] Handler failed, AutoResume with error");
                    let mut map = HashMap::new();
                    map.insert(
                        interrupt_id.to_string(),
                        RichHandlerResult::Json(json!({
                            "error": e,
                            "interrupt_type": interrupt_type,
                        })),
                    );
                    InterruptAction::AutoResume(map)
                }
            }
        } else {
            // No handler registered → wait for user
            tracing::info!(interrupt_type = %interrupt_type, "[InterruptHandlerRegistry] No handler registered, WaitForUser");
            InterruptAction::WaitForUser {
                pending: vec![PendingInterruptInfo {
                    interrupt_id: interrupt_id.to_string(),
                    interrupt_type,
                    display_data: payload.clone(),
                }],
            }
        }
    }

    /// Check if a handler is registered for a type
    pub fn has_handler(&self, interrupt_type: &str) -> bool {
        self.handlers.contains_key(interrupt_type)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Extract interrupt type from payload.
///
/// Supports multiple formats:
/// - `{"type": "things_thing_added", ...}`
/// - `{"payload": {"type": "things_thing_added", ...}}`
/// - `{"things_thing_added": {...}}` (oneof style)
pub fn extract_interrupt_type(payload: &JsonValue) -> String {
    // Try direct "type" field
    if let Some(t) = payload.get("type").and_then(|v| v.as_str()) {
        return t.to_string();
    }

    // Try nested in "payload"
    if let Some(inner) = payload.get("payload") {
        if let Some(t) = inner.get("type").and_then(|v| v.as_str()) {
            return t.to_string();
        }
        // Try oneof style in payload
        if let Some(obj) = inner.as_object() {
            for key in obj.keys() {
                if key.starts_with("things_")
                    || key.starts_with("events_")
                    || key.starts_with("trigger_")
                    || key.starts_with("triggers_")
                {
                    return key.clone();
                }
            }
        }
    }

    // Try oneof style at root
    if let Some(obj) = payload.as_object() {
        for key in obj.keys() {
            if key.starts_with("things_")
                || key.starts_with("events_")
                || key.starts_with("trigger_")
                || key.starts_with("triggers_")
            {
                return key.clone();
            }
        }

        // Some interrupt shapes wrap data under value/display_data
        if let Some(inner) = obj.get("value") {
            let t = extract_interrupt_type(inner);
            if t != "unknown" {
                return t;
            }
        }
        if let Some(inner) = obj.get("display_data") {
            let t = extract_interrupt_type(inner);
            if t != "unknown" {
                return t;
            }
        }
    }

    "unknown".to_string()
}

/// Extract the actual data from interrupt payload (unwrap nested structures)
pub fn extract_interrupt_data(payload: &JsonValue, interrupt_type: &str) -> JsonValue {
    // Try payload.payload.<type>
    if let Some(inner) = payload.get("payload").and_then(|p| p.get(interrupt_type)) {
        return inner.clone();
    }

    // Try payload.<type>
    if let Some(inner) = payload.get(interrupt_type) {
        return inner.clone();
    }

    // Try payload.payload (if type was at root)
    if let Some(inner) = payload.get("payload") {
        return inner.clone();
    }

    // Try value.* wrappers used by some transports/event adapters
    if let Some(value_obj) = payload.get("value") {
        if let Some(inner) = value_obj.get("payload").and_then(|p| p.get(interrupt_type)) {
            return inner.clone();
        }
        if let Some(inner) = value_obj.get(interrupt_type) {
            return inner.clone();
        }
        if let Some(inner) = value_obj.get("payload") {
            return inner.clone();
        }
        return value_obj.clone();
    }

    if let Some(display_obj) = payload.get("display_data") {
        if let Some(inner) = display_obj.get("payload").and_then(|p| p.get(interrupt_type)) {
            return inner.clone();
        }
        if let Some(inner) = display_obj.get(interrupt_type) {
            return inner.clone();
        }
        if let Some(inner) = display_obj.get("payload") {
            return inner.clone();
        }
        return display_obj.clone();
    }

    payload.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_type_direct() {
        let payload = json!({"type": "things_thing_added", "data": {}});
        assert_eq!(extract_interrupt_type(&payload), "things_thing_added");
    }

    #[test]
    fn test_extract_type_nested() {
        let payload = json!({"payload": {"type": "things_thing_edited", "thing": {}}});
        assert_eq!(extract_interrupt_type(&payload), "things_thing_edited");
    }

    #[test]
    fn test_extract_type_oneof() {
        let payload = json!({"payload": {"things_collection_added": {"uuid": "123"}}});
        assert_eq!(extract_interrupt_type(&payload), "things_collection_added");
    }

    #[test]
    fn test_extract_type_triggers_list_request() {
        // triggers_ prefix (plural) must also be recognized
        let payload = json!({"id": "interrupt-abc", "payload": {"triggers_list_request": {}}});
        assert_eq!(extract_interrupt_type(&payload), "triggers_list_request");
    }

    #[test]
    fn test_extract_type_trigger_rule_published() {
        let payload = json!({"id": "interrupt-xyz", "payload": {"trigger_rule_published": {"trigger_uuid": "t1"}}});
        assert_eq!(extract_interrupt_type(&payload), "trigger_rule_published");
    }
}
