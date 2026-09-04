use std::{str::FromStr, sync::Arc, time::Duration};

use hyper_util::rt::TokioIo;
use once_cell::sync::{Lazy, OnceCell};
use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::Code;
use tonic::Status;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tower::service_fn;
use weaver_core::{AppAddr, ClientAddr, NetworkId, VirtualName};
use weaver_crypto::{MemberCertificate, NetworkRootPublic, derive_device_id};
use weaver_net::{
    ConfigSyncOptions, KEY_MEMBER_CERTIFICATE, LocalBinding, MembershipStores, NetworkHandle,
    NetworkHandleOpenOptions, NetworkMembership, PresenceSyncOptions,
};
use weaver_relay_core::InvitationBundle;
use weaver_store::{SecretStore, StateStore, StoreKey, StoreScope};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
pub const OFFLINE_TRANSPORT_ERROR: &str =
    "Remi SDK is running in offline mode; remote transport is unavailable";

#[derive(Debug, Clone, Deserialize)]
pub struct TransportConfig {
    #[serde(default, rename = "connectTimeoutMs")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, rename = "requestTimeoutMs")]
    pub request_timeout_ms: Option<u64>,
    /// "tcp" (plain TCP gRPC) or "offline".
    #[serde(default, rename = "transportMode")]
    pub transport_mode: Option<String>,
    /// Host:port for plain TCP gRPC.
    #[serde(default, rename = "tcpGrpcAddr")]
    pub tcp_grpc_addr: Option<String>,
}

pub struct TransportState {
    pub mode: SharedTransportMode,
    connector: TransportConnector,
    pub request_timeout: Duration,
}

enum TransportConnector {
    Offline,
    Tcp(Endpoint),
    Weaver {
        endpoint: Endpoint,
        network: Arc<NetworkHandle>,
        source: ClientAddr,
        service_name: VirtualName,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedTransportMode {
    Offline,
    Tcp,
    Weaver,
}

#[derive(Clone)]
pub struct WeaverTransportOptions {
    pub root: NetworkRootPublic,
    pub state_store: Arc<dyn StateStore>,
    pub secret_store: Arc<dyn SecretStore>,
    pub client_app_addr: AppAddr,
    pub service_name: VirtualName,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub config_sync: ConfigSyncOptions,
    pub presence_sync: PresenceSyncOptions,
    pub allow_insecure_test_stores: bool,
}

impl WeaverTransportOptions {
    pub fn new(
        root: NetworkRootPublic,
        state_store: Arc<dyn StateStore>,
        secret_store: Arc<dyn SecretStore>,
        client_app_addr: AppAddr,
    ) -> Result<Self, String> {
        Ok(Self {
            root,
            state_store,
            secret_store,
            client_app_addr,
            service_name: VirtualName::from_str("remi.virtual")
                .map_err(|error| format!("Invalid default Weaver service name: {error}"))?,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            config_sync: ConfigSyncOptions::default(),
            presence_sync: PresenceSyncOptions::default(),
            allow_insecure_test_stores: false,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeaverNetworkStatus {
    pub joined: bool,
    pub online: bool,
    pub degraded: bool,
    pub network_id: NetworkId,
    pub endpoint_id: String,
    pub config_revision: u64,
    pub relay_registered: bool,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct WeaverEnrollmentOptions {
    pub expected_root: NetworkRootPublic,
    pub state_store: Arc<dyn StateStore>,
    pub secret_store: Arc<dyn SecretStore>,
    pub timeout: Duration,
    pub allow_insecure_test_stores: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeaverEnrollmentResult {
    pub network_id: NetworkId,
    pub client_app_addr: AppAddr,
    pub service_name: VirtualName,
    pub config_revision: u64,
}

pub async fn redeem_weaver_invitation(
    options: WeaverEnrollmentOptions,
    invitation_text: &str,
) -> Result<WeaverEnrollmentResult, String> {
    let invitation = InvitationBundle::from_text(invitation_text)
        .map_err(|error| format!("Invalid Weaver invitation: {error}"))?;
    invitation
        .verify(&options.expected_root, wall_now_ms())
        .map_err(|error| format!("Invalid Weaver invitation: {error}"))?;
    let stores = MembershipStores {
        state: options.state_store,
        secrets: options.secret_store,
        allow_insecure_test_stores: options.allow_insecure_test_stores,
    };
    let head = NetworkMembership::redeem_invitation(
        &stores,
        &options.expected_root,
        &invitation,
        wall_now_ms(),
        options.timeout,
    )
    .await
    .map_err(|error| format!("Failed to redeem Weaver invitation: {error}"))?;
    Ok(WeaverEnrollmentResult {
        network_id: invitation.network_id,
        client_app_addr: invitation.client_app_addr,
        service_name: invitation.service_name,
        config_revision: head.revision,
    })
}

pub async fn reset_weaver_identity(options: WeaverEnrollmentOptions) -> Result<(), String> {
    if SHARED_TRANSPORT.get().is_some() {
        return Err(
            "Weaver transport is active; restart the host before resetting network identity"
                .to_string(),
        );
    }
    NetworkMembership::reset(
        &MembershipStores {
            state: options.state_store,
            secrets: options.secret_store,
            allow_insecure_test_stores: options.allow_insecure_test_stores,
        },
        options.expected_root.network_id(),
    )
    .await
    .map_err(|error| format!("Failed to reset Weaver identity: {error}"))
}

fn wall_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

pub struct SharedTransport {
    state: TransportState,
    channel: Mutex<Option<Channel>>,
}

impl SharedTransport {
    pub fn request_timeout(&self) -> Duration {
        self.state.request_timeout
    }

    pub fn mode(&self) -> SharedTransportMode {
        self.state.mode
    }

    pub fn is_offline(&self) -> bool {
        self.state.mode == SharedTransportMode::Offline
    }

    pub async fn get_channel(&self) -> Result<Channel, String> {
        if self.is_offline() {
            tracing::debug!("[transport] get_channel: offline transport has no remote channel");
            return Err(OFFLINE_TRANSPORT_ERROR.to_string());
        }

        let mut guard = self.channel.lock().await;
        if let Some(ch) = guard.as_ref() {
            tracing::debug!("[transport] get_channel: returning cached channel");
            return Ok(ch.clone());
        }

        tracing::info!("[transport] get_channel: no cached channel, creating channel...");
        let channel = match &self.state.connector {
            TransportConnector::Offline => return Err(OFFLINE_TRANSPORT_ERROR.to_string()),
            // Keep a lazy TCP channel so tonic can re-establish the underlying
            // connection on the next request after transient network loss.
            TransportConnector::Tcp(endpoint) => endpoint.clone().connect_lazy(),
            TransportConnector::Weaver {
                endpoint,
                network,
                source,
                service_name,
            } => {
                let network = network.clone();
                let source = *source;
                let service_name = service_name.clone();
                endpoint
                    .clone()
                    .connect_with_connector(service_fn(move |_| {
                        let network = network.clone();
                        let service_name = service_name.clone();
                        async move {
                            network
                                .connect_tcp_name(source, &service_name)
                                .await
                                .map(TokioIo::new)
                                .map_err(std::io::Error::other)
                        }
                    }))
                    .await
                    .map_err(|error| format!("Failed to connect to Remi over Weaver: {error}"))?
            }
        };
        tracing::info!("[transport] get_channel: channel ready");
        guard.replace(channel.clone());

        Ok(channel)
    }

    /// Invalidate the cached channel so the next `get_channel` reconnects.
    pub async fn invalidate_channel(&self) {
        let mut guard = self.channel.lock().await;
        if guard.take().is_some() {
            tracing::info!(
                "[transport] invalidate_channel: cached channel dropped, will reconnect on next use"
            );
        }
    }
}

pub fn is_recoverable_transport_status(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Cancelled | Code::Unknown | Code::DeadlineExceeded
    ) || is_recoverable_transport_message(status.message())
}

pub fn is_recoverable_transport_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();

    [
        "connection reset",
        "broken pipe",
        "connection refused",
        "connection aborted",
        "timed out",
        "deadline has elapsed",
        "transport error",
        "tcp connect error",
        "dns error",
        "network unreachable",
        "temporarily unavailable",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::{
        OFFLINE_TRANSPORT_ERROR, SharedTransport, SharedTransportMode, TransportConnector,
        build_transport_state, is_offline_transport_config, is_recoverable_transport_message,
    };
    use serde_json::json;
    use tokio::sync::Mutex;

    #[test]
    fn detects_connection_reset_messages() {
        assert!(is_recoverable_transport_message(
            "transport error: connection reset by peer"
        ));
        assert!(is_recoverable_transport_message(
            "deadline has elapsed while waiting for response"
        ));
        assert!(!is_recoverable_transport_message("permission denied"));
    }

    #[test]
    fn detects_explicit_offline_transport_config() {
        let config = serde_json::from_value(json!({
            "transportMode": "offline"
        }))
        .expect("valid config");

        assert!(is_offline_transport_config(&config));
    }

    #[test]
    fn detects_tcp_without_remote_address_as_offline() {
        let config = serde_json::from_value(json!({
            "transportMode": "tcp",
            "tcpGrpcAddr": "  "
        }))
        .expect("valid config");

        assert!(is_offline_transport_config(&config));
    }

    #[test]
    fn detects_unknown_transport_mode_as_offline() {
        let config = serde_json::from_value(json!({
            "transportMode": "unsupported",
            "tcpGrpcAddr": "127.0.0.1:50051"
        }))
        .expect("valid config");

        assert!(is_offline_transport_config(&config));
    }

    #[test]
    fn accepts_valid_tcp_remote_config() {
        let config = serde_json::from_value(json!({
            "transportMode": "tcp",
            "tcpGrpcAddr": "127.0.0.1:50051"
        }))
        .expect("valid config");

        assert!(!is_offline_transport_config(&config));
    }

    #[tokio::test]
    async fn offline_transport_get_channel_fails_without_network_setup() {
        let state = build_transport_state(
            &json!({
                "transportMode": "offline",
                "requestTimeoutMs": 1234
            })
            .to_string(),
        )
        .await
        .expect("offline transport state should build");

        assert_eq!(state.mode, SharedTransportMode::Offline);
        assert!(matches!(state.connector, TransportConnector::Offline));

        let transport = SharedTransport {
            state,
            channel: Mutex::new(None),
        };

        let error = transport
            .get_channel()
            .await
            .expect_err("offline transport should not create a channel");
        assert_eq!(error, OFFLINE_TRANSPORT_ERROR);
    }
}

static SHARED_TRANSPORT: OnceCell<Arc<SharedTransport>> = OnceCell::new();

/// Serializes initialization so that only one `build_transport_state` runs at a
/// time. Without this, two concurrent callers can both pass the `OnceCell::get()`
/// fast-path and race to configure the shared transport.
static TRANSPORT_INIT_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub async fn configure_shared_transport(config_json: &str) -> Result<Arc<SharedTransport>, String> {
    // Fast path — already configured.
    if let Some(existing) = SHARED_TRANSPORT.get() {
        tracing::info!(
            "[transport] configure_shared_transport: fast-path reuse (already configured)"
        );
        return Ok(existing.clone());
    }

    tracing::info!("[transport] configure_shared_transport: first call, acquiring init lock...");
    // Serialize initialization to prevent duplicate shared transport creation.
    let _lock = TRANSPORT_INIT_LOCK.lock().await;

    // Double-check after acquiring the lock.
    if let Some(existing) = SHARED_TRANSPORT.get() {
        tracing::info!(
            "[transport] configure_shared_transport: double-check hit, reusing existing"
        );
        return Ok(existing.clone());
    }

    tracing::info!("[transport] configure_shared_transport: building new transport state...");
    let state = build_transport_state(config_json).await?;
    let shared = Arc::new(SharedTransport {
        state,
        channel: Mutex::new(None),
    });

    let _ = SHARED_TRANSPORT.set(shared.clone());
    tracing::info!(
        "[transport] configure_shared_transport: transport created and stored in OnceCell"
    );
    Ok(SHARED_TRANSPORT.get().cloned().unwrap_or(shared))
}

pub async fn configure_legacy_tcp_transport(
    server_url: impl Into<String>,
) -> Result<Arc<SharedTransport>, String> {
    let server_url = server_url.into();
    let address = server_url
        .trim()
        .strip_prefix("http://")
        .or_else(|| server_url.trim().strip_prefix("https://"))
        .unwrap_or(server_url.trim());
    configure_shared_transport(
        &serde_json::json!({
            "transportMode": "tcp",
            "tcpGrpcAddr": address,
        })
        .to_string(),
    )
    .await
}

/// Configures the process-wide transport with host-owned Weaver stores.
/// Invitation enrollment must have completed before this is called.
pub async fn configure_shared_transport_with_weaver(
    options: WeaverTransportOptions,
) -> Result<Arc<SharedTransport>, String> {
    if let Some(existing) = SHARED_TRANSPORT.get() {
        if existing.mode() != SharedTransportMode::Weaver {
            return Err("Shared transport is already configured with a different mode".to_string());
        }
        return Ok(existing.clone());
    }

    let _lock = TRANSPORT_INIT_LOCK.lock().await;
    if let Some(existing) = SHARED_TRANSPORT.get() {
        if existing.mode() != SharedTransportMode::Weaver {
            return Err("Shared transport is already configured with a different mode".to_string());
        }
        return Ok(existing.clone());
    }

    let network_id = options.root.network_id();
    let member_record = options
        .state_store
        .read(
            StoreScope::member(network_id),
            &StoreKey::new(KEY_MEMBER_CERTIFICATE).map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| format!("Failed to read Weaver membership: {error}"))?
        .ok_or_else(|| "Weaver membership is missing; import an invitation first".to_string())?;
    let member = MemberCertificate::from_bytes(&member_record.bytes)
        .map_err(|error| format!("Invalid Weaver membership: {error}"))?;
    if member.payload().network_id != network_id {
        return Err("Weaver membership belongs to another network".to_string());
    }
    let device_id = derive_device_id(
        network_id,
        options.client_app_addr,
        &member.payload().signing_public_key,
    );
    let source = ClientAddr::new(options.client_app_addr, device_id);
    let network = NetworkHandle::open(
        NetworkHandleOpenOptions {
            root: options.root,
            state_store: options.state_store,
            secret_store: options.secret_store,
            config_sync: options.config_sync,
            presence_sync: options.presence_sync,
            allow_insecure_test_stores: options.allow_insecure_test_stores,
        },
        [LocalBinding::Client(source)],
    )
    .await
    .map_err(|error| format!("Failed to start Weaver network: {error}"))?;
    let network = Arc::new(network);
    let endpoint = Endpoint::from_static("http://remi.virtual")
        .connect_timeout(options.connect_timeout)
        .timeout(options.request_timeout);
    let shared = Arc::new(SharedTransport {
        state: TransportState {
            mode: SharedTransportMode::Weaver,
            connector: TransportConnector::Weaver {
                endpoint,
                network,
                source,
                service_name: options.service_name,
            },
            request_timeout: options.request_timeout,
        },
        channel: Mutex::new(None),
    });
    let _ = SHARED_TRANSPORT.set(shared.clone());
    Ok(SHARED_TRANSPORT.get().cloned().unwrap_or(shared))
}

impl SharedTransport {
    pub async fn weaver_network_status(&self) -> Option<WeaverNetworkStatus> {
        let TransportConnector::Weaver { network, .. } = &self.state.connector else {
            return None;
        };
        let head = network.config_head().await;
        let relay_result = network.wait_relay_online(Duration::from_secs(1)).await;
        Some(WeaverNetworkStatus {
            joined: true,
            online: relay_result.is_ok(),
            degraded: relay_result.is_err(),
            network_id: network.network_id(),
            endpoint_id: network.endpoint_id().to_string(),
            config_revision: head.revision,
            relay_registered: relay_result.is_ok(),
            last_error: relay_result.err().map(|error| error.to_string()),
        })
    }

    pub async fn network_change(&self) {
        let TransportConnector::Weaver { network, .. } = &self.state.connector else {
            return;
        };
        network.network_change().await;
        self.invalidate_channel().await;
    }
}

pub fn get_shared_transport() -> Result<Arc<SharedTransport>, String> {
    SHARED_TRANSPORT
        .get()
        .cloned()
        .ok_or_else(|| "Shared transport is not configured".to_string())
}

pub fn is_shared_transport_offline() -> bool {
    SHARED_TRANSPORT
        .get()
        .map(|transport| transport.is_offline())
        .unwrap_or(false)
}

pub async fn build_transport_state(config_json: &str) -> Result<TransportState, String> {
    let config: TransportConfig = serde_json::from_str(config_json)
        .map_err(|err| format!("Invalid telemetry transport config: {err}"))?;

    let request_timeout = config
        .request_timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT);

    if is_offline_transport_config(&config) {
        tracing::info!("[transport] build_transport_state: offline mode, skipping network setup");
        return Ok(TransportState {
            mode: SharedTransportMode::Offline,
            connector: TransportConnector::Offline,
            request_timeout,
        });
    }

    if config
        .transport_mode
        .as_deref()
        .is_some_and(|mode| mode.trim().eq_ignore_ascii_case("weaver"))
    {
        return Err(
            "Weaver transport requires host-injected stores; call configure_shared_transport_with_weaver"
                .to_string(),
        );
    }

    let tcp_addr = config
        .tcp_grpc_addr
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "tcpGrpcAddr is required for remote transport".to_string())?;
    let endpoint_uri = format!("http://{tcp_addr}");
    tracing::info!("[transport] build_transport_state: TCP mode, addr={tcp_addr}");
    let mut endpoint = Endpoint::from_shared(endpoint_uri)
        .map_err(|err| format!("Invalid TCP gRPC endpoint: {err}"))?;
    if let Some(timeout_ms) = config.connect_timeout_ms {
        endpoint = endpoint.connect_timeout(Duration::from_millis(timeout_ms));
    } else {
        endpoint = endpoint.connect_timeout(DEFAULT_CONNECT_TIMEOUT);
    }
    endpoint = endpoint.timeout(request_timeout);
    endpoint = endpoint.tcp_keepalive(Some(Duration::from_secs(30)));

    Ok(TransportState {
        mode: SharedTransportMode::Tcp,
        connector: TransportConnector::Tcp(endpoint),
        request_timeout,
    })
}

pub fn is_offline_transport_config(config: &TransportConfig) -> bool {
    match config.transport_mode.as_deref().map(str::trim) {
        Some(mode) if mode.eq_ignore_ascii_case("offline") || mode.eq_ignore_ascii_case("none") => {
            true
        }
        Some(mode) if mode.eq_ignore_ascii_case("weaver") => false,
        Some(mode) if !mode.eq_ignore_ascii_case("tcp") => true,
        _ => config
            .tcp_grpc_addr
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none(),
    }
}
