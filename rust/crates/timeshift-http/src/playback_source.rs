//! Provider setup ported from Dispatcharr's catch-up view/helpers.
//! Original Dispatcharr authors and contributors retain all application credit.
use dispatcharr_contracts::UserLimitPolicy;
use dispatcharr_coordination::CredentialPool;
use dispatcharr_store::{
    xc_group_credential_pool, AuthService, ChannelIdentity, Store, StoreError, User,
};
use dispatcharr_timeshift_core::{
    candidate_urls, client_duration_window, parse_timestamp, programme_age_days,
    provider_timestamp, utc_timestamp, ProviderCredentials, DEFAULT_DURATION_MINUTES,
    MAX_DURATION_MINUTES,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::time::{SystemTime, UNIX_EPOCH};

// Provider URLs and logins deliberately have no Debug derives.
pub struct CatchupCandidate {
    pub stream_id: i64,
    pub account_id: i64,
    pub profile_id: i64,
    pub max_streams: u32,
    pub credential_pool: Option<CredentialPool>,
    pub provider_stream_id: String,
    pub provider_timestamp: String,
    pub provider_timezone: Option<String>,
    pub credentials: ProviderCredentials,
    pub urls: Vec<String>,
    pub user_agent: String,
}
fn archive_credential_pool(
    group: Option<i64>,
    maximum: u32,
    credentials: &ProviderCredentials,
) -> Result<Option<CredentialPool>, StoreError> {
    let Some(group) = group.filter(|_| maximum > 0) else {
        return Ok(None);
    };
    let pool =
        xc_group_credential_pool(group, maximum, &credentials.username, &credentials.password)?;
    Ok(Some(CredentialPool {
        counter_key: pool.counter_key,
        max_streams: pool.max_streams,
    }))
}

pub struct CatchupSource {
    pub user: User,
    pub channel: ChannelIdentity,
    pub timestamp: String,
    pub duration_window: u32,
    pub redirect: bool,
    pub limit_policy: UserLimitPolicy,
    pub candidates: Vec<CatchupCandidate>,
}
#[derive(Clone)]
pub struct SqlCatchupResolver {
    store: Store,
}
impl SqlCatchupResolver {
    pub fn new(store: Store) -> Self {
        Self { store }
    }
    /// Provider setup for an established session. A bare native Proxy request
    /// must mint its source-compatible 301 before attempting this setup.
    pub async fn resolve(
        &self,
        user: User,
        channel: ChannelIdentity,
        timestamp: String,
        duration_hint: Option<&str>,
    ) -> Result<CatchupSource, StoreError> {
        self.resolve_context(user, channel, timestamp, duration_hint, false)
            .await
    }
    /// Bare Redirect handoff alone applies allowed_m3u_profile_ids. Existing
    /// API capabilities continue proxying even on a Redirect-profile channel.
    pub async fn resolve_redirect(
        &self,
        user: User,
        channel: ChannelIdentity,
        timestamp: String,
        duration_hint: Option<&str>,
    ) -> Result<CatchupSource, StoreError> {
        self.resolve_context(user, channel, timestamp, duration_hint, true)
            .await
    }
    /// First-hop validation must not require a usable provider profile: the
    /// source returns the Proxy301 before any profile walk or reservation.
    pub async fn validate_direct(
        &self,
        user: &User,
        channel: &ChannelIdentity,
        timestamp: &str,
    ) -> Result<bool, StoreError> {
        Ok(self.route_context(user, channel, timestamp).await?.0)
    }
    async fn route_context(
        &self,
        user: &User,
        channel: &ChannelIdentity,
        timestamp: &str,
    ) -> Result<(bool, Option<i64>), StoreError> {
        if user.custom_properties.get("catchup_enabled") == Some(&Value::Bool(false))
            || self
                .store
                .setting("system_settings")
                .await?
                .get("catchup_enabled")
                == Some(&Value::Bool(false))
        {
            return Err(StoreError::StatusBody(403, json!("Catch-up is disabled")));
        }
        if parse_timestamp(timestamp).is_none() {
            return Err(StoreError::StatusBody(400, json!("Invalid timestamp")));
        }
        let settings = self.store.setting("stream_settings").await?;
        let default_profile = id(settings.get("default_stream_profile"));
        let default_agent = id(settings.get("default_user_agent"));
        let row=sqlx::query("SELECT c.is_catchup,(SELECT locked AND name='Redirect' FROM core_streamprofile WHERE id=COALESCE(o.stream_profile_id,c.stream_profile_id,$2)) AS redirect,EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active) AS has_stream FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.id=$1 AND c.uuid=$3").bind(channel.id).bind(default_profile).bind(channel.uuid).fetch_optional(self.store.pool()).await?.ok_or(StoreError::NotFound)?;
        if !row.try_get::<bool, _>("is_catchup")? || !row.try_get::<bool, _>("has_stream")? {
            return Err(StoreError::StatusBody(
                400,
                json!("Timeshift not supported for this channel"),
            ));
        }
        let redirect = row.try_get::<Option<bool>, _>("redirect")?.unwrap_or(false);
        Ok((redirect, default_agent))
    }
    async fn resolve_context(
        &self,
        user: User,
        channel: ChannelIdentity,
        timestamp: String,
        duration_hint: Option<&str>,
        restrict_redirect: bool,
    ) -> Result<CatchupSource, StoreError> {
        let (redirect, default_agent) = self.route_context(&user, &channel, &timestamp).await?;
        let duration_window = match duration_hint.and_then(client_duration_window) {
            Some(window) => window,
            None => self.epg_duration(channel.id, &timestamp).await,
        };
        let default_agent = if let Some(id) = default_agent {
            sqlx::query_scalar::<_, String>(
                "SELECT user_agent FROM core_useragent WHERE id=$1 AND user_agent<>''",
            )
            .bind(id)
            .fetch_optional(self.store.pool())
            .await?
            .unwrap_or_else(|| "Dispatcharr/0.33.0".into())
        } else {
            "Dispatcharr/0.33.0".into()
        };
        let mut rows=sqlx::query("SELECT s.id::bigint AS stream_id,s.custom_properties,s.catchup_days,a.id::bigint AS account_id,a.account_type,a.server_url,a.username,a.password,a.server_group_id::bigint AS server_group_id,p.id::bigint AS profile_id,p.max_streams,p.search_pattern,p.replace_pattern,(SELECT d.custom_properties FROM m3u_m3uaccountprofile d WHERE d.m3u_account_id=a.id AND d.is_active AND d.is_default ORDER BY d.id LIMIT 1) AS default_properties,COALESCE(NULLIF(ua.user_agent,''),$2) AS user_agent FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id JOIN m3u_m3uaccountprofile p ON p.m3u_account_id=a.id LEFT JOIN core_useragent ua ON ua.id=a.user_agent_id WHERE cs.channel_id=$1 AND s.is_catchup AND a.is_active AND p.is_active AND EXISTS(SELECT 1 FROM m3u_m3uaccountprofile d WHERE d.m3u_account_id=a.id AND d.is_active AND d.is_default) ORDER BY cs.\"order\",cs.id,CASE WHEN p.is_default THEN 0 ELSE 1 END,p.id").bind(channel.id).bind(default_agent).fetch_all(self.store.pool()).await?;
        let now = utc_timestamp(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        )
        .expect("system clock UTC timestamp");
        let age = programme_age_days(&timestamp, now).unwrap_or(0);
        rows.sort_by_key(|row| {
            let days = row.try_get::<i32, _>("catchup_days").unwrap_or(0);
            !(days <= 0 || days as u64 >= age)
        });
        let allowed = if redirect && restrict_redirect {
            user.custom_properties
                .get("allowed_m3u_profile_ids")
                .filter(|value| !value.is_null())
                .map(|value| {
                    value
                        .as_array()
                        .and_then(|values| {
                            values
                                .iter()
                                .map(|value| id(Some(value)))
                                .collect::<Option<Vec<_>>>()
                        })
                        .unwrap_or_default()
                })
        } else {
            None
        };
        let mut candidates = Vec::new();
        for row in rows {
            let profile_id: i64 = row.try_get("profile_id")?;
            if allowed
                .as_ref()
                .is_some_and(|ids| !ids.contains(&profile_id))
            {
                continue;
            }
            if row.try_get::<String, _>("account_type")? != "XC" {
                continue;
            }
            let properties: Option<Value> = row.try_get("custom_properties")?;
            let Some(stream) = properties
                .as_ref()
                .and_then(|properties| properties.get("stream_id"))
                .filter(|value| !value.is_null())
            else {
                continue;
            };
            let stream = match stream {
                Value::String(value) => value.clone(),
                Value::Number(value) => value.to_string(),
                Value::Bool(value) => {
                    if *value {
                        "True".into()
                    } else {
                        "False".into()
                    }
                }
                _ => {
                    return Err(StoreError::Unsupported(
                        "Non-scalar catch-up provider stream identifiers are not implemented",
                    ))
                }
            };
            let search: String = row.try_get("search_pattern")?;
            let replacement: String = row.try_get("replace_pattern")?;
            let (Some(server), Some(username), Some(password)) = (
                row.try_get::<Option<String>, _>("server_url")?,
                row.try_get::<Option<String>, _>("username")?,
                row.try_get::<Option<String>, _>("password")?,
            ) else {
                continue;
            };
            let Some(credentials) =
                archive_profile_credentials(&server, username, password, &search, &replacement)?
            else {
                continue;
            };
            let max_streams = row.try_get::<i32, _>("max_streams")?.max(0) as u32;
            let credential_pool = archive_credential_pool(
                row.try_get("server_group_id")?,
                max_streams,
                &credentials,
            )?;
            let default_properties: Option<Value> = row.try_get("default_properties")?;
            let zone = default_properties
                .as_ref()
                .and_then(|value| value.get("server_info"))
                .and_then(|value| value.get("timezone"))
                .and_then(Value::as_str);
            let local_timestamp = provider_timestamp(&timestamp, zone);
            let urls = candidate_urls(&credentials, &stream, &local_timestamp, duration_window);
            candidates.push(CatchupCandidate {
                stream_id: row.try_get("stream_id")?,
                account_id: row.try_get("account_id")?,
                profile_id,
                max_streams,
                credential_pool,
                provider_stream_id: stream,
                provider_timestamp: local_timestamp,
                provider_timezone: zone.map(str::to_owned),
                credentials,
                urls,
                user_agent: row.try_get("user_agent")?,
            });
        }
        if candidates.is_empty() {
            return Err(if redirect && restrict_redirect {
                StoreError::StatusBody(503, json!("No available stream"))
            } else {
                StoreError::StatusBody(400, json!("Cannot build timeshift URL"))
            });
        }
        let limits = self.store.setting("user_limit_settings").await?;
        let defaults = UserLimitPolicy::default();
        let limit_policy = UserLimitPolicy {
            terminate_on_limit_exceeded: limits
                .get("terminate_on_limit_exceeded")
                .and_then(Value::as_bool)
                .unwrap_or(defaults.terminate_on_limit_exceeded),
            prioritize_single_client_channels: limits
                .get("prioritize_single_client_channels")
                .and_then(Value::as_bool)
                .unwrap_or(defaults.prioritize_single_client_channels),
            ignore_same_channel_connections: limits
                .get("ignore_same_channel_connections")
                .and_then(Value::as_bool)
                .unwrap_or(defaults.ignore_same_channel_connections),
            terminate_oldest: limits
                .get("terminate_oldest")
                .and_then(Value::as_bool)
                .unwrap_or(defaults.terminate_oldest),
        };
        Ok(CatchupSource {
            user,
            channel,
            timestamp,
            duration_window,
            redirect,
            limit_policy,
            candidates,
        })
    }
    async fn epg_duration(&self, channel: i64, timestamp: &str) -> u32 {
        let Some(datetime) = parse_timestamp(timestamp) else {
            return DEFAULT_DURATION_MINUTES;
        };
        let seconds=sqlx::query_scalar::<_,f64>("SELECT EXTRACT(EPOCH FROM(p.end_time-p.start_time))::float8 FROM epg_programdata p JOIN dispatcharr_channels_channel c ON c.epg_data_id=p.epg_id WHERE c.id=$1 AND p.start_time<=($2::text)::timestamptz AND p.end_time>($2::text)::timestamptz ORDER BY p.id LIMIT 1").bind(channel).bind(format!("{}Z",datetime.format("%Y-%m-%dT%H:%M:%S"))).fetch_optional(self.store.pool()).await.ok().flatten();
        seconds
            .map(|seconds| {
                ((seconds / 60.0).trunc() as u32)
                    .saturating_add(5)
                    .min(MAX_DURATION_MINUTES)
            })
            .unwrap_or(DEFAULT_DURATION_MINUTES)
    }
}
fn id(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
fn archive_profile_credentials(
    server: &str,
    username: String,
    password: String,
    search: &str,
    replacement: &str,
) -> Result<Option<ProviderCredentials>, StoreError> {
    if username.is_empty() || password.is_empty() {
        return Ok(None);
    }
    let Some(server_url) = normalized_server(server) else {
        return Ok(None);
    };
    if search.is_empty() || replacement.is_empty() || (search == "^(.*)$" && replacement == "$1") {
        return Ok(Some(ProviderCredentials {
            server_url,
            username,
            password,
        }));
    }
    let Some((server_url, username, password, _)) = AuthService::resolve_vod_profile_credentials(
        &server_url,
        &username,
        &password,
        search,
        replacement,
        None,
        0,
    )?
    else {
        return Ok(None);
    };
    if !url::Url::parse(&server_url)
        .is_ok_and(|provider| matches!(provider.scheme(), "http" | "https") && provider.has_host())
    {
        return Err(StoreError::Unsupported(
            "Catch-up transformed provider URL scheme is not implemented",
        ));
    }
    Ok(Some(ProviderCredentials {
        server_url,
        username,
        password,
    }))
}
fn normalized_server(value: &str) -> Option<String> {
    let mut url = url::Url::parse(value.trim()).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let mut path = url.path().trim_end_matches('/').to_string();
    if let Some(at) = path.rfind('/') {
        if let Some(parameter) = path[at..].find(';') {
            path.truncate(at + parameter);
        }
    }
    let last = path.rsplit('/').next().unwrap_or("");
    if last.ends_with(".php") {
        path = path[..path.len() - last.len()].trim_end_matches('/').into();
    }
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string().trim_end_matches('/').into())
}

#[cfg(test)]
mod tests {
    #[test]
    fn catchup_group_uses_the_shared_counter_only_for_limited_profiles() {
        let credentials = super::ProviderCredentials {
            server_url: "http://fixture.invalid".into(),
            username: " Sample-Login ".into(),
            password: " sample-pass ".into(),
        };
        assert!(super::archive_credential_pool(Some(17), 0, &credentials)
            .unwrap()
            .is_none());
        assert!(super::archive_credential_pool(None, 2, &credentials)
            .unwrap()
            .is_none());
        let group = super::archive_credential_pool(Some(17), 2, &credentials)
            .unwrap()
            .unwrap();
        let current =
            dispatcharr_store::xc_group_credential_pool(17, 2, "sample-login", "sample-pass")
                .unwrap();
        assert_eq!(group.counter_key, current.counter_key);
        assert_eq!(group.max_streams, 2);
    }

    use super::*;

    #[test]
    fn assigned_profile_transforms_both_archive_credential_segments() {
        let resolved = archive_profile_credentials(
            "http://origin.invalid/owned/player_api.php",
            "before-fixture".into(),
            "before-fixture".into(),
            r"/live/before-(?<viewer>[a-z]+)/before-(?<login>[a-z]+)/1234[.]ts$",
            "/live/$<viewer>/$<login>/1234.ts",
        )
        .unwrap()
        .unwrap();
        let urls = candidate_urls(&resolved, "sample", "2026-11-09:11-00", 30);
        assert_eq!(
            urls[0],
            "http://origin.invalid/owned/timeshift/fixture/fixture/30/2026-11-09:11-00/sample.ts"
        );
    }

    #[test]
    fn identity_keeps_existing_archive_normalization_and_no_match_skips_the_profile() {
        let base = "https://origin.invalid/example/player_api.php;local?from=test";
        let resolved = archive_profile_credentials(
            base,
            "ordinary-viewer".into(),
            "ordinary-login".into(),
            "^(.*)$",
            "$1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolved.server_url, "https://origin.invalid/example");
        assert_eq!(resolved.username, "ordinary-viewer");
        assert_eq!(resolved.password, "ordinary-login");
        assert!(archive_profile_credentials(
            base,
            "ordinary-viewer".into(),
            "ordinary-login".into(),
            r"/not-an-owned-profile$",
            "/live/fixture/fixture/1234.ts"
        )
        .unwrap()
        .is_none());
    }
}
