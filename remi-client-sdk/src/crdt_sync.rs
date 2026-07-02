use anyhow::{Context, Result};
use automerge::AutoCommit;
use automerge::sync::{self, SyncDoc};

pub(crate) fn device_actor_id(device_id: &str) -> automerge::ActorId {
    // Actor IDs must be stable per device and unique across devices.
    // Using the device_id bytes is deterministic and avoids inheriting the server actor
    // when bootstrapping from a server-provided snapshot.
    let bytes = if device_id.is_empty() {
        b"remi-device".to_vec()
    } else {
        device_id.as_bytes().to_vec()
    };
    automerge::ActorId::from(bytes)
}

/// Initialize a new automerge sync state (encoded bytes).
///
/// This uses `automerge::sync::State::encode()`, which persists only the state that should be
/// reused across connections (per Automerge docs).
pub(crate) fn init_sync_state() -> Vec<u8> {
    sync::State::new().encode()
}

pub(crate) fn decode_sync_state(sync_state_bytes: &[u8]) -> sync::State {
    if sync_state_bytes.is_empty() {
        return sync::State::new();
    }
    sync::State::decode(sync_state_bytes).unwrap_or_else(|_| sync::State::new())
}

/// In-memory sync session which keeps `sync::State` across multiple rounds.
///
/// Note: `sync::State::encode()` is intended for persistence between sessions.
/// For multi-round convergence loops, keep the `State` in memory and only
/// encode it when you want to persist.
pub(crate) struct AutomergeSyncSession {
    doc: AutoCommit,
    state: sync::State,
}

impl AutomergeSyncSession {
    pub(crate) fn new_with_device_id(
        doc_bytes: &[u8],
        sync_state_bytes: &[u8],
        device_id: &str,
    ) -> Result<Self> {
        let mut doc = AutoCommit::load(doc_bytes).context("Failed to load document")?;
        doc.set_actor(device_actor_id(device_id));
        let state = decode_sync_state(sync_state_bytes);
        Ok(Self { doc, state })
    }

    pub(crate) fn apply_server_messages(&mut self, server_messages: &[Vec<u8>]) -> Result<()> {
        for bytes in server_messages {
            if bytes.is_empty() {
                continue;
            }
            let msg =
                sync::Message::decode(bytes).context("Failed to decode server sync message")?;
            self.doc
                .sync()
                .receive_sync_message(&mut self.state, msg)
                .context("Failed to apply server sync message")?;
        }
        Ok(())
    }

    pub(crate) fn generate_client_message(&mut self) -> Option<Vec<u8>> {
        self.doc
            .sync()
            .generate_sync_message(&mut self.state)
            .map(|m| m.encode())
    }

    pub(crate) fn doc_bytes(&mut self) -> Vec<u8> {
        self.doc.save()
    }

    pub(crate) fn sync_state_bytes(&self) -> Vec<u8> {
        self.state.encode()
    }
}
