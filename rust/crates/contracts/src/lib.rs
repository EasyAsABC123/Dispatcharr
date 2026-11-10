//! Backend boundaries for the derivative Dispatcharr port. Original authorship
//! belongs to the upstream Dispatcharr authors; see ../../../ATTRIBUTION.md.
mod source_ip;
pub use source_ip::SourceIpAddr;
use std::{future::Future, net::IpAddr, pin::Pin, time::Duration};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Bounded transport failures for original Dispatcharr realtime envelopes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateEventError {
    Capacity,
    Unavailable,
    InvalidMessage,
}

/// Publish a complete Channels event without changing its type/data envelope.
/// Original application event contracts belong to Dispatcharr's authors.
pub trait UpdateEventSink: Send + Sync {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>>;
    /// Retain caller-owned admission through publication. Transports that
    /// supervise physical I/O after cancellation override this to transfer the
    /// guard into each owned operation; the default retains it for send().
    fn send_retained<'a>(
        &'a self,
        group: &'a str,
        message: serde_json::Value,
        guard: std::sync::Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            let _guard = guard;
            self.send(group, message).await
        })
    }
}

/// Nonblocking notification that acknowledged live-client metadata changed.
/// Implementations schedule bounded background snapshots; no polling per viewer.
pub trait LiveStatsTrigger: Send + Sync {
    fn changed(&self);
}

/// Build a global live snapshot from acknowledged metadata, without publication.
pub trait LiveStatsSnapshotSource: Send + Sync {
    fn build(&self) -> BoxFuture<'_, Result<serde_json::Value, PlaybackError>>;
}
/// Return the shared owned JSON body and schedule its exact stats event.
/// The byte owner retains global snapshot admission through the final HTTP frame.
pub trait LiveStatsSnapshotReader: Send + Sync {
    fn snapshot(&self) -> BoxFuture<'_, Result<bytes::Bytes, PlaybackError>>;
}

/// Decode incoming WSGI/ASGI header bytes as the original Django request does.
/// Invalid UTF-8 is not a reason to discard an entire forwarded-IP chain.
/// Authentication adapters retain their independent fail-closed byte guards.
pub fn source_header_value(raw: &[u8]) -> String {
    raw.iter().map(|byte| char::from(*byte)).collect()
}

/// Canonicalize production Django/WSGI request fields. Hyphens and underscores
/// share a CGI META name; repeated fields preserve order and join with ", ".
/// Callers validate ambiguous/raw authentication fields before this conversion.
pub fn source_headers<'a>(
    values: impl IntoIterator<Item = (&'a str, &'a [u8])>,
) -> Vec<(String, String)> {
    let mut result: Vec<(String, String)> = Vec::new();
    let mut positions = std::collections::HashMap::new();
    for (name, raw) in values {
        let name = name.replace('_', "-").to_ascii_lowercase();
        let value = source_header_value(raw);
        if let Some(index) = positions.get(&name).copied() {
            let (_, previous): &mut (String, String) = &mut result[index];
            previous.push_str(", ");
            previous.push_str(&value);
        } else {
            positions.insert(name.clone(), result.len());
            result.push((name, value));
        }
    }
    result
}

#[derive(Clone, Debug)]
pub struct PlaybackRequest {
    pub channel_id: String,
    pub query: Vec<(String, String)>,
    /// Canonical incoming names and Source Latin-1-decoded, folded values.
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
    pub ip_address: SourceIpAddr,
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
    /// Standard-user permissions precede method dispatch and capability lookup.
    /// STREAMS ACL applies to DELETE handlers, not unsupported HEAD dispatch.
    fn authorize_hls_control(
        &self,
        _request: PlaybackRequest,
        _check_stream_acl: bool,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async {
            Err(PlaybackError::new(
                501,
                "HLS control authentication unavailable",
            ))
        })
    }
    fn authorize_hls_owner(
        &self,
        _request: PlaybackRequest,
        _owner_user_id: Option<i64>,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async { Err(PlaybackError::new(404, "Session not found")) })
    }
}

#[cfg(test)]
mod source_header_tests {
    use super::source_header_value;
    #[test]
    fn source_latin1_preserves_mixed_forwarding_and_utf8_bytes() {
        assert_eq!(
            source_header_value(b"203.0.113.42, \xff"),
            "203.0.113.42, ÿ"
        );
        assert_eq!(source_header_value(b"header-caf\xe9"), "header-café");
        assert_eq!(source_header_value(b"caf\xc3\xa9"), "cafÃ©");
        assert_eq!(
            super::source_headers([
                ("X-Forwarded-For", &b"10.0.0.1"[..]),
                ("X_Forwarded_For", &b"203.0.113.42, \xff"[..])
            ]),
            vec![("x-forwarded-for".into(), "10.0.0.1, 203.0.113.42, ÿ".into())]
        );
    }
}
