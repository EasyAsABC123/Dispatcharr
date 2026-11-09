//! Backend boundaries for the derivative Dispatcharr port. Original authorship
//! belongs to the upstream Dispatcharr authors; see ../../../ATTRIBUTION.md.
use std::{future::Future, net::IpAddr, pin::Pin, time::Duration};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug)]
pub struct PlaybackRequest {
    pub channel_id: String,
    pub query: Vec<(String, String)>,
    /// Raw HTTP header names and values. Adapters lowercase header names.
    pub headers: Vec<(String, String)>,
    /// Socket peer, before trusted-proxy resolution by the authorization layer.
    pub peer_ip: IpAddr,
}

impl PlaybackRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .rev()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct PlaybackCandidate {
    pub stream_id: i64,
    pub provider_profile_id: Option<i64>,
    pub provider_max_streams: u32,
    pub url: String,
    pub user_agent: String,
    pub headers: Vec<(String, String)>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    MpegTs,
    Fmp4,
    Hls,
}

#[derive(Clone, Debug)]
pub struct UserLimitPolicy {
    pub terminate_on_limit_exceeded: bool,
    pub prioritize_single_client_channels: bool,
    pub ignore_same_channel_connections: bool,
    pub terminate_oldest: bool,
}

impl Default for UserLimitPolicy {
    fn default() -> Self {
        Self {
            terminate_on_limit_exceeded: true,
            prioritize_single_client_channels: true,
            ignore_same_channel_connections: false,
            terminate_oldest: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PlaybackViewer {
    pub user_id: Option<i64>,
    pub username: Option<String>,
    pub ip_address: IpAddr,
    pub user_agent: String,
    pub stream_limit: u32,
    pub limit_policy: UserLimitPolicy,
}

#[derive(Clone, Debug)]
pub struct PlaybackSource {
    pub channel_id: String,
    pub channel_name: String,
    pub stream_id: i64,
    pub profile_id: i64,
    pub provider_profile_id: Option<i64>,
    /// Zero means unlimited, as in the original provider profile model.
    pub provider_max_streams: u32,
    pub url: String,
    pub user_agent: String,
    pub headers: Vec<(String, String)>,
    pub metadata: Vec<(String, String)>,
    pub output_format: OutputFormat,
    pub shutdown_delay: Duration,
    pub startup_behind: Duration,
    /// Eligible alternatives in original channel-stream/provider-profile order.
    /// All authorization and compatibility guards must run before inclusion.
    pub alternates: Vec<PlaybackCandidate>,
    pub viewer: Option<PlaybackViewer>,
}

impl PlaybackSource {
    pub fn primary(&self) -> PlaybackCandidate {
        PlaybackCandidate {
            stream_id: self.stream_id,
            provider_profile_id: self.provider_profile_id,
            provider_max_streams: self.provider_max_streams,
            url: self.url.clone(),
            user_agent: self.user_agent.clone(),
            headers: self.headers.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PlaybackError {
    pub status: u16,
    pub body: serde_json::Value,
}

impl PlaybackError {
    pub fn new(status: u16, message: &str) -> Self {
        Self {
            status,
            body: serde_json::json!({"error": message}),
        }
    }
}

/// Optional channel-deletion hook. The application supplies distributed media
/// teardown; the store calls it outside its deletion transaction. The original
/// Dispatcharr stop-before-delete path is best-effort and still deletes an
/// inactive channel or one whose teardown reports an error.
pub trait ChannelControl: Send + Sync + 'static {
    fn stop_channel(&self, channel_id: String) -> BoxFuture<'_, Result<(), PlaybackError>>;
}

/// Authorization is evaluated for every viewer, before an existing channel may
/// be reused. Resolution does not itself start an upstream or claim a provider
/// slot. Capacity-limited providers require an explicit atomic reservation layer.
pub trait PlaybackResolver: Send + Sync + 'static {
    fn resolve(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<PlaybackSource, PlaybackError>>;
    /// DRF authenticates presented credentials even on AllowAny capability routes.
    /// Adapters without an authenticator must not silently accept credentials.
    fn authenticate_optional(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move {
            let recognized = request.header("x-api-key").is_some()
                || request.header("authorization").is_some_and(|value| {
                    let scheme = value.split_whitespace().next().unwrap_or("");
                    scheme.eq_ignore_ascii_case("Bearer") || scheme.eq_ignore_ascii_case("ApiKey")
                });
            if recognized {
                Err(PlaybackError::new(
                    501,
                    "Credential authentication unavailable",
                ))
            } else {
                Ok(())
            }
        })
    }
    fn authorize_admin(&self, request: PlaybackRequest)
        -> BoxFuture<'_, Result<(), PlaybackError>>;
    fn authorize_hls_owner(
        &self,
        _request: PlaybackRequest,
        _owner_user_id: Option<i64>,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async { Err(PlaybackError::new(404, "Session not found")) })
    }
}
