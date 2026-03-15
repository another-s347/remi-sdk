use anyhow::Result;
use once_cell::sync::OnceCell;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

// Include generated code
pub mod proto {
    pub mod public_api {
        pub mod v1 {
            tonic::include_proto!("public_api.v1");
        }
    }
}

use proto::public_api::v1::{
    LoginRequest, LoginResponse, LogoutRequest, LogoutResponse, RefreshTokenRequest,
    RefreshTokenResponse, SignupRequest, SignupResponse,
    public_service_client::PublicServiceClient,
};

use crate::transport::{SharedTransport, configure_shared_transport};

pub const APP_KEY_PREFIX: &str = "remi_app_";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SdkBearerAuthMode {
    /// Requests will use the current SDK-managed user session.
    UserSession,
    /// Requests will use the SDK-level application API key override.
    AppKey,
}

/// Authentication client for user login/logout
#[derive(Clone)]
pub struct AuthClient {
    state: Arc<AuthClientState>,
    credentials: Arc<RwLock<Option<AuthCredentials>>>,
    refresh_lock: Arc<Mutex<()>>,
}

struct AuthClientState {
    transport: Arc<SharedTransport>,
    request_timeout: Duration,
}

/// Stored authentication credentials
#[derive(Clone, Debug)]
pub struct AuthCredentials {
    pub access_token: String,
    pub refresh_token: String,
    pub user_id: String,
    pub expires_in: i64,
    pub expires_at_unix_ms: i64,
}

const ACCESS_TOKEN_REFRESH_SKEW_SECS: i64 = 60;

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn compute_expires_at_unix_ms(expires_in_secs: i64) -> i64 {
    let now = now_unix_ms();
    if expires_in_secs <= 0 {
        return now;
    }
    let add_ms = (expires_in_secs as i128)
        .saturating_mul(1000)
        .min(i64::MAX as i128) as i64;
    now.saturating_add(add_ms)
}

fn is_access_token_valid(creds: &AuthCredentials) -> bool {
    let now = now_unix_ms();
    let skew_ms = (ACCESS_TOKEN_REFRESH_SKEW_SECS as i128)
        .saturating_mul(1000)
        .min(i64::MAX as i128) as i64;
    creds.expires_at_unix_ms > now.saturating_add(skew_ms)
}

fn normalize_token(token: Option<&str>) -> Option<&str> {
    token.map(str::trim).filter(|value| !value.is_empty())
}

fn resolve_bearer_token_value(
    explicit_token: Option<&str>,
    app_key: Option<&str>,
    user_access_token: Option<&str>,
) -> Option<String> {
    normalize_token(explicit_token)
        .or_else(|| normalize_token(app_key))
        .or_else(|| normalize_token(user_access_token))
        .map(ToOwned::to_owned)
}

fn resolve_user_access_token_value(
    explicit_token: Option<&str>,
    user_access_token: Option<&str>,
) -> Result<Option<String>, String> {
    if let Some(explicit_token) = normalize_token(explicit_token) {
        if auth_is_app_key(explicit_token) {
            return Err(
                "Application API keys cannot be used for application management RPCs".to_string(),
            );
        }

        return Ok(Some(explicit_token.to_string()));
    }

    Ok(normalize_token(user_access_token).map(ToOwned::to_owned))
}

impl AuthClient {
    pub fn from_transport(transport: Arc<SharedTransport>) -> Self {
        let state = AuthClientState {
            request_timeout: transport.request_timeout(),
            transport,
        };

        Self {
            state: Arc::new(state),
            credentials: Arc::new(RwLock::new(None)),
            refresh_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn get_channel(&self) -> Result<Channel> {
        self.state
            .transport
            .get_channel()
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }

    /// Login with email and password
    pub async fn login(&self, email: String, password: String) -> Result<LoginResponse> {
        tracing::info!("[auth] login called for email={}", email);

        let request = tonic::Request::new(LoginRequest { email, password });

        let channel = self.get_channel().await?;
        let mut client = PublicServiceClient::new(channel);
        let login_response = timeout(self.state.request_timeout, client.login(request))
            .await
            .map_err(|_| anyhow::anyhow!("Auth login timed out"))??
            .into_inner();

        tracing::info!("[auth] login: success, userId={}", login_response.user_id);

        let credentials = AuthCredentials {
            access_token: login_response.access_token.clone(),
            refresh_token: login_response.refresh_token.clone(),
            user_id: login_response.user_id.clone(),
            expires_in: login_response.expires_in,
            expires_at_unix_ms: compute_expires_at_unix_ms(login_response.expires_in),
        };
        *self.credentials.write().await = Some(credentials);

        Ok(login_response)
    }

    /// Signup with email and password (creates a new account).
    /// When email confirmation is required, returns the response with
    /// `confirmation_required = true` and empty session tokens (no credentials stored).
    pub async fn signup(&self, email: String, password: String) -> Result<SignupResponse> {
        let request = tonic::Request::new(SignupRequest { email, password });

        let channel = self.get_channel().await?;
        let mut client = PublicServiceClient::new(channel);
        let signup_response = timeout(self.state.request_timeout, client.signup(request))
            .await
            .map_err(|_| anyhow::anyhow!("Auth signup timed out"))??
            .into_inner();

        // Only store credentials when we actually received session tokens
        if !signup_response.confirmation_required {
            let credentials = AuthCredentials {
                access_token: signup_response.access_token.clone(),
                refresh_token: signup_response.refresh_token.clone(),
                user_id: signup_response.user_id.clone(),
                expires_in: signup_response.expires_in,
                expires_at_unix_ms: compute_expires_at_unix_ms(signup_response.expires_in),
            };
            *self.credentials.write().await = Some(credentials);
        }

        Ok(signup_response)
    }

    /// Logout and invalidate the current session
    pub async fn logout(&self) -> Result<LogoutResponse> {
        let access_token = {
            let creds = self.credentials.read().await;
            creds
                .as_ref()
                .map(|c| c.access_token.clone())
                .ok_or_else(|| anyhow::anyhow!("Not logged in"))?
        };

        let request = tonic::Request::new(LogoutRequest { access_token });
        let channel = self.get_channel().await?;
        let mut client = PublicServiceClient::new(channel);
        let response = timeout(self.state.request_timeout, client.logout(request))
            .await
            .map_err(|_| anyhow::anyhow!("Auth logout timed out"))??
            .into_inner();

        // Clear stored credentials
        *self.credentials.write().await = None;

        Ok(response)
    }

    /// Refresh the access token using the refresh token
    pub async fn refresh_token(&self) -> Result<RefreshTokenResponse> {
        let refresh_token = {
            let creds = self.credentials.read().await;
            creds
                .as_ref()
                .map(|c| c.refresh_token.clone())
                .ok_or_else(|| anyhow::anyhow!("Not logged in"))?
        };

        let request = tonic::Request::new(RefreshTokenRequest { refresh_token });
        let channel = self.get_channel().await?;
        let mut client = PublicServiceClient::new(channel);
        let refresh_response = timeout(self.state.request_timeout, client.refresh_token(request))
            .await
            .map_err(|_| anyhow::anyhow!("Auth refresh timed out"))??
            .into_inner();

        // Update stored credentials
        let mut creds = self.credentials.write().await;
        if let Some(credentials) = creds.as_mut() {
            credentials.access_token = refresh_response.access_token.clone();
            credentials.refresh_token = refresh_response.refresh_token.clone();
            credentials.expires_in = refresh_response.expires_in;
            credentials.expires_at_unix_ms =
                compute_expires_at_unix_ms(refresh_response.expires_in);
        }

        Ok(refresh_response)
    }

    /// Get a valid access token, automatically refreshing when expired (or near expiry).
    pub async fn get_access_token_auto_refresh(&self) -> Result<String> {
        // Fast path: token exists and is still valid.
        {
            let creds = self.credentials.read().await;
            if let Some(c) = creds.as_ref() {
                if is_access_token_valid(c) {
                    return Ok(c.access_token.clone());
                }
            }
        }

        // Slow path: serialize refresh to avoid stampedes.
        let _guard = self.refresh_lock.lock().await;

        // Re-check after acquiring lock.
        {
            let creds = self.credentials.read().await;
            if let Some(c) = creds.as_ref() {
                if is_access_token_valid(c) {
                    return Ok(c.access_token.clone());
                }
            }
        }

        // Refresh and then read updated token.
        let _ = self.refresh_token().await?;
        let creds = self
            .credentials
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Not logged in"))?;

        Ok(creds.access_token)
    }

    /// Get the current access token if logged in
    pub async fn get_access_token(&self) -> Option<String> {
        let creds = self.credentials.read().await;
        creds.as_ref().map(|c| c.access_token.clone())
    }

    /// Get the current user ID if logged in
    pub async fn get_user_id(&self) -> Option<String> {
        let creds = self.credentials.read().await;
        creds.as_ref().map(|c| c.user_id.clone())
    }

    /// Check if currently logged in
    pub async fn is_logged_in(&self) -> bool {
        let creds = self.credentials.read().await;
        creds.is_some()
    }

    /// Get all credentials if logged in
    pub async fn get_credentials(&self) -> Option<AuthCredentials> {
        let creds = self.credentials.read().await;
        creds.clone()
    }

    /// Restore previously-saved credentials directly into the in-memory store.
    /// Used by desktop/CLI to re-hydrate a session from persisted storage on
    /// startup without requiring the user to log in again.
    pub async fn restore_credentials(
        &self,
        access_token: String,
        refresh_token: String,
        user_id: String,
        expires_in: i64,
        expires_at_unix_ms: i64,
    ) {
        let credentials = AuthCredentials {
            access_token,
            refresh_token,
            user_id,
            expires_in,
            expires_at_unix_ms,
        };
        *self.credentials.write().await = Some(credentials);
    }
}

static AUTH_CLIENT: OnceCell<Arc<RwLock<Option<Arc<AuthClient>>>>> = OnceCell::new();
static CURRENT_APP_KEY: OnceCell<Arc<RwLock<Option<String>>>> = OnceCell::new();

fn auth_client_store() -> &'static Arc<RwLock<Option<Arc<AuthClient>>>> {
    AUTH_CLIENT.get_or_init(|| Arc::new(RwLock::new(None)))
}

fn app_key_store() -> &'static Arc<RwLock<Option<String>>> {
    CURRENT_APP_KEY.get_or_init(|| Arc::new(RwLock::new(None)))
}

async fn get_auth_client() -> Result<Arc<AuthClient>, String> {
    let guard = auth_client_store().read().await;
    guard
        .as_ref()
        .cloned()
        .ok_or_else(|| "Auth client is not configured".to_string())
}

pub async fn configure_auth_client(config_json: String) -> Result<(), String> {
    tracing::info!("[auth] configure_auth_client: creating new AuthClient + shared transport");
    let transport = configure_shared_transport(&config_json).await?;
    let client = Arc::new(AuthClient::from_transport(transport));

    let mut guard = auth_client_store().write().await;
    guard.replace(client);
    tracing::info!("[auth] configure_auth_client: done");
    Ok(())
}

pub async fn auth_login(email: String, password: String) -> Result<LoginResponse, String> {
    let client = get_auth_client().await?;
    client
        .login(email, password)
        .await
        .map_err(|err| err.to_string())
}

pub async fn auth_logout() -> Result<LogoutResponse, String> {
    let client = get_auth_client().await?;
    client.logout().await.map_err(|err| err.to_string())
}

pub async fn auth_refresh_token() -> Result<RefreshTokenResponse, String> {
    let client = get_auth_client().await?;
    client.refresh_token().await.map_err(|err| err.to_string())
}

pub fn auth_is_app_key(token: &str) -> bool {
    token.starts_with(APP_KEY_PREFIX)
}

/// Set the SDK-wide application API key override used by bearer-authenticated RPCs.
///
/// When present, the app key takes precedence over any logged-in user session for
/// business RPCs. Application management RPCs remain JWT-only.
pub async fn auth_set_app_key(app_key: String) -> Result<(), String> {
    let app_key = app_key.trim();

    if app_key.is_empty() {
        return Err("App key cannot be empty".to_string());
    }

    if !auth_is_app_key(app_key) {
        return Err(format!("App key must start with {APP_KEY_PREFIX}"));
    }

    let mut guard = app_key_store().write().await;
    guard.replace(app_key.to_string());
    Ok(())
}

pub async fn auth_clear_app_key() {
    let mut guard = app_key_store().write().await;
    guard.take();
}

/// Get the SDK-wide application API key override, if one is configured.
pub async fn auth_get_app_key() -> Option<String> {
    let guard = app_key_store().read().await;
    guard.clone()
}

/// Get a refreshed user access token from the configured AuthClient, if available.
pub async fn auth_get_user_access_token() -> Option<String> {
    let client = get_auth_client().await.ok()?;
    client.get_access_token_auto_refresh().await.ok()
}

/// Backward-compatible alias for retrieving the current user access token.
pub async fn auth_get_access_token() -> Option<String> {
    auth_get_user_access_token().await
}

/// Resolve the current bearer token for business RPCs.
///
/// Precedence is: SDK app key override, then the refreshed user access token.
pub async fn auth_get_bearer_token() -> Option<String> {
    let app_key = auth_get_app_key().await;
    let user_access_token = auth_get_user_access_token().await;
    resolve_bearer_token_value(None, app_key.as_deref(), user_access_token.as_deref())
}

pub async fn auth_resolve_bearer_token(explicit_token: Option<&str>) -> Option<String> {
    let app_key = auth_get_app_key().await;
    let user_access_token = auth_get_user_access_token().await;
    resolve_bearer_token_value(
        explicit_token,
        app_key.as_deref(),
        user_access_token.as_deref(),
    )
}

/// Resolve a bearer token for JWT-only RPCs.
///
/// Explicit tokens must be user JWTs; passing an application API key returns an error.
pub async fn auth_resolve_user_access_token(
    explicit_token: Option<&str>,
) -> Result<Option<String>, String> {
    let user_access_token = auth_get_user_access_token().await;
    resolve_user_access_token_value(explicit_token, user_access_token.as_deref())
}

/// Report which SDK auth mode would currently be used for business RPCs.
pub async fn auth_get_bearer_auth_mode() -> Option<SdkBearerAuthMode> {
    if auth_get_app_key().await.is_some() {
        return Some(SdkBearerAuthMode::AppKey);
    }

    if auth_get_user_access_token().await.is_some() {
        return Some(SdkBearerAuthMode::UserSession);
    }

    None
}

/// Insert an `Authorization: Bearer <token>` header into a tonic request.
pub fn auth_insert_bearer_header<T>(
    request: &mut tonic::Request<T>,
    bearer_token: &str,
) -> Result<(), String> {
    let bearer = format!("Bearer {bearer_token}")
        .parse::<MetadataValue<_>>()
        .map_err(|err| format!("Invalid bearer token: {err}"))?;
    request.metadata_mut().insert("authorization", bearer);
    Ok(())
}

pub async fn auth_get_user_id() -> Option<String> {
    let client = get_auth_client().await.ok()?;
    client.get_user_id().await
}

pub async fn auth_is_logged_in() -> bool {
    match get_auth_client().await {
        Ok(client) => client.is_logged_in().await,
        Err(_) => false,
    }
}

pub async fn auth_signup(email: String, password: String) -> Result<SignupResponse, String> {
    let client = get_auth_client().await?;
    client
        .signup(email, password)
        .await
        .map_err(|err| err.to_string())
}

/// Restore a previously-persisted session into the in-memory AuthClient.
/// `configure_auth_client` must have been called first to set up the transport.
pub async fn auth_restore_credentials(
    access_token: String,
    refresh_token: String,
    user_id: String,
    expires_in: i64,
    expires_at_unix_ms: i64,
) -> Result<(), String> {
    let client = get_auth_client().await?;
    client
        .restore_credentials(
            access_token,
            refresh_token,
            user_id,
            expires_in,
            expires_at_unix_ms,
        )
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{auth_is_app_key, resolve_bearer_token_value, resolve_user_access_token_value};

    #[test]
    fn resolves_bearer_token_with_expected_precedence() {
        let token = resolve_bearer_token_value(
            Some("explicit-token"),
            Some("remi_app_fallback"),
            Some("jwt-token"),
        );

        assert_eq!(token.as_deref(), Some("explicit-token"));
    }

    #[test]
    fn resolves_app_key_before_user_session() {
        let token = resolve_bearer_token_value(None, Some("remi_app_123"), Some("jwt-token"));

        assert_eq!(token.as_deref(), Some("remi_app_123"));
    }

    #[test]
    fn ignores_blank_explicit_token_when_resolving_bearer() {
        let token = resolve_bearer_token_value(Some("   "), Some("remi_app_123"), None);

        assert_eq!(token.as_deref(), Some("remi_app_123"));
    }

    #[test]
    fn rejects_app_key_for_user_access_token_resolution() {
        let err = resolve_user_access_token_value(Some("remi_app_123"), Some("jwt-token"))
            .expect_err("app keys must not be accepted as user access tokens");

        assert!(err.contains("Application API keys cannot be used"));
    }

    #[test]
    fn keeps_user_access_token_resolution_jwt_only() {
        let token = resolve_user_access_token_value(None, Some("jwt-token"))
            .expect("user token resolution should succeed");

        assert_eq!(token.as_deref(), Some("jwt-token"));
    }

    #[test]
    fn detects_app_key_prefix() {
        assert!(auth_is_app_key("remi_app_123"));
        assert!(!auth_is_app_key("eyJhbGciOi..."));
    }
}
