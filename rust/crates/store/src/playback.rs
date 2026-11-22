use crate::{AuthService, Store, StoreError, User};
use dispatcharr_contracts::{
    BoxFuture, OutputFormat, PlaybackCandidate, PlaybackError, PlaybackRequest, PlaybackResolution,
    PlaybackResolver, PlaybackSource, PlaybackViewer, UserLimitPolicy,
};
use sqlx::Row;
use std::{collections::BTreeMap, time::Duration};

// Match the production engine's existing primary+alternate capacity, before
// transferring/expanding profile commands for an unbounded provider join.
const MAX_CANDIDATES: usize = 32;
fn playback_capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        serde_json::json!({"error":"Stream candidate capacity reached"}),
    )
}

#[derive(Debug)]
pub struct ProviderStream {
    pub input_command: Option<dispatcharr_contracts::StreamCommand>,
    pub stream_id: i64,
    pub provider_profile_id: i64,
    pub provider_max_streams: u32,
    pub credential_pool: Option<dispatcharr_contracts::PlaybackCredentialPool>,
    pub url: String,
    pub user_agent: String,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct ResolvedChannel {
    pub redirect: bool,
    pub numeric_channel_id: i64,
    pub has_assigned_streams: bool,
    pub redirect_error: Option<String>,
    pub id: String,
    pub name: String,
    pub profile_id: i64,
    pub streams: Vec<ProviderStream>,
    pub shutdown_delay: Duration,
    pub startup_behind: Duration,
}

// Secret-bearing account fields deliberately do not implement Debug or Display.
struct ProviderUrl<'a> {
    account_type: &'a str,
    stored: &'a str,
    server: &'a str,
    username: &'a str,
    password: &'a str,
    stream_id: &'a str,
    search: &'a str,
    replace: &'a str,
}
impl ProviderUrl<'_> {
    fn resolve(&self) -> Result<Option<String>, StoreError> {
        // Original _resolve_live_stream_url regenerates XC URLs from CURRENT
        // account credentials; the imported URL can contain an obsolete login.
        let url =
            if self.account_type == "XC" && !self.stream_id.is_empty() && self.stream_id != "0" {
                let Some(credentials) = crate::xc_account_provider::credentials(
                    self.server,
                    self.username,
                    self.password,
                    self.search,
                    self.replace,
                )?
                else {
                    return Ok(None);
                };
                format!(
                    "{}/live/{}/{}/{}.ts",
                    credentials.url, credentials.username, credentials.password, self.stream_id
                )
            } else if matches!(self.account_type, "STD" | "XC")
                && (self.search.is_empty()
                    || self.replace.is_empty()
                    || (self.search == "^(.*)$" && self.replace == "$1"))
            {
                self.stored.to_owned()
            } else {
                return Err(StoreError::Unsupported(
                    "Provider URL transform is unsupported",
                ));
            };
        if url.len() > 65536 {
            return Err(playback_capacity());
        }
        Ok(Some(url))
    }
}

/// Source connection_pool.compute_credential_fingerprint and truncated legacy key.
/// Python Unicode case conversion can differ from Rust's; that unqualified
/// username domain remains explicit rather than splitting one real login pool.
pub(crate) fn xc_credential_pool(
    group: i64,
    maximum: u32,
    username: &str,
    password: &str,
) -> Result<dispatcharr_contracts::PlaybackCredentialPool, StoreError> {
    use sha2::{Digest, Sha256};
    if !username.is_ascii() || group <= 0 || maximum == 0 {
        return Err(StoreError::Unsupported(
            "XC credential normalization domain unsupported",
        ));
    }
    let strip = |c: char| c.is_whitespace() || matches!(c, '\u{001c}'..='\u{001f}');
    let mut digest = Sha256::new();
    digest.update(username.trim_matches(strip).to_ascii_lowercase().as_bytes());
    digest.update([0]);
    digest.update(password.trim_matches(strip).as_bytes());
    let fingerprint = format!("{:x}", digest.finalize());
    Ok(dispatcharr_contracts::PlaybackCredentialPool {
        counter_key: format!("server_group_connections:{group}:{}", &fingerprint[..16]),
        max_streams: maximum,
    })
}

fn unsupported_std_sample() -> StoreError {
    StoreError::Unsupported("STD credential sample-URL precedence is outside the qualified domain")
}

fn std_sample_credential_pool(
    group: i64,
    maximum: u32,
    sample: &str,
    username: &str,
    password: &str,
) -> Result<dispatcharr_contracts::PlaybackCredentialPool, StoreError> {
    // The verified slice is a plain HTTP(S) account sample. It is read only
    // for pool identity. The actual assigned media URL is resolved separately.
    let url = url::Url::parse(sample).map_err(|_| unsupported_std_sample())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || url.as_str() != sample
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(unsupported_std_sample());
    }
    let mut segments = url.path_segments().ok_or_else(unsupported_std_sample)?;
    let plain = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
    };
    let (first, second, third) = (
        segments.next().filter(|part| plain(part)),
        segments.next().filter(|part| plain(part)),
        segments.next().filter(|part| plain(part)),
    );
    let (Some(first), Some(second), Some(third)) = (first, second, third) else {
        return Err(unsupported_std_sample());
    };
    let pair = match segments.next() {
        Some(id)
            if first.eq_ignore_ascii_case("live") && plain(id) && segments.next().is_none() =>
        {
            (second, third)
        }
        None if ![
            "live",
            "movie",
            "series",
            "timeshift",
            "player_api.php",
            "panel_api.php",
            "xmltv.php",
        ]
        .iter()
        .any(|reserved| first.eq_ignore_ascii_case(reserved)) =>
        {
            if username.is_empty() || password.is_empty() {
                (first, second)
            } else {
                (username, password)
            }
        }
        _ => return Err(unsupported_std_sample()),
    };
    xc_credential_pool(group, maximum, pair.0, pair.1)
}

struct StdSample {
    url: Option<String>,
    oversized: bool,
    ambiguous: bool,
}

async fn std_account_samples(
    store: &Store,
    accounts: &[i64],
) -> Result<BTreeMap<i64, StdSample>, StoreError> {
    if accounts.is_empty() {
        return Ok(BTreeMap::new());
    }
    let samples = sqlx::query_as::<_, (i64, Option<String>, bool, bool)>(
        r#"SELECT requested.account_id,
                  CASE WHEN octet_length(sample.url) <= 65536 THEN sample.url END,
                  COALESCE(octet_length(sample.url) > 65536, false),
                  COALESCE(sample.next_updated_at = sample.updated_at, false)
           FROM unnest($1::bigint[]) AS requested(account_id)
           LEFT JOIN LATERAL (
               SELECT recent.url, recent.updated_at,
                      LEAD(recent.updated_at) OVER (ORDER BY recent.updated_at DESC) AS next_updated_at
               FROM (
                   SELECT s.url, s.updated_at
                   FROM dispatcharr_channels_stream s
                   WHERE s.m3u_account_id = requested.account_id AND s.url IS DISTINCT FROM ''
                   ORDER BY s.updated_at DESC
                   LIMIT 2
               ) recent
               ORDER BY recent.updated_at DESC
               LIMIT 1
           ) sample ON true"#,
    )
    .bind(accounts)
    .fetch_all(store.pool())
    .await?;
    Ok(samples
        .into_iter()
        .map(|(account, url, oversized, ambiguous)| {
            (
                account,
                StdSample {
                    url,
                    oversized,
                    ambiguous,
                },
            )
        })
        .collect())
}

struct PlaybackLookup<'a> {
    id: &'a str,
    preview: Option<PreviewLookup>,
}
#[derive(Clone, Copy)]
struct PreviewLookup {
    profile_override: Option<i64>,
    scoped: bool,
}
impl<'a> From<&'a str> for PlaybackLookup<'a> {
    fn from(id: &'a str) -> Self {
        Self { id, preview: None }
    }
}
impl Store {
    pub async fn resolve_channel(&self, channel_id: &str) -> Result<ResolvedChannel, StoreError> {
        self.resolve_channel_mode(channel_id, false, None)
            .await?
            .ok_or(StoreError::NotFound)
    }
    pub(crate) async fn resolve_channel_mode(
        &self,
        channel_id: &str,
        allow_redirect: bool,
        redirect_user: Option<&User>,
    ) -> Result<Option<ResolvedChannel>, StoreError> {
        self.resolve_channel_mode_inner(
            channel_id.into(),
            allow_redirect,
            redirect_user,
            None,
            true,
            None,
        )
        .await
    }
    pub(crate) async fn resolve_preview_channel(
        &self,
        hash: &str,
        profile_override: Option<i64>,
        selected: Option<(i64, i64)>,
        scoped: bool,
    ) -> Result<ResolvedChannel, StoreError> {
        self.resolve_channel_mode_inner(
            PlaybackLookup {
                id: hash,
                preview: Some(PreviewLookup {
                    profile_override,
                    scoped,
                }),
            },
            false,
            None,
            selected,
            false,
            None,
        )
        .await?
        .ok_or(StoreError::NotFound)
    }
    pub(crate) async fn resolve_preview_targets(
        &self,
        id: &str,
        user: Option<&User>,
    ) -> Result<ResolvedChannel, StoreError> {
        self.resolve_channel_mode_inner(id.into(), true, user, None, false, None)
            .await?
            .ok_or(StoreError::NotFound)
    }
    pub(crate) async fn resolve_channel_mode_cached(
        &self,
        channel_id: &str,
        allow_redirect: bool,
        user: Option<&User>,
        cache: Option<&crate::NotificationCache>,
    ) -> Result<Option<ResolvedChannel>, StoreError> {
        self.resolve_channel_mode_inner(channel_id.into(), allow_redirect, user, None, true, cache)
            .await
    }
    pub(crate) async fn resolve_redirect_alias_candidate(
        &self,
        channel_id: &str,
        stream_id: i64,
        profile_id: i64,
    ) -> Result<Option<ProviderStream>, StoreError> {
        let channel = self
            .resolve_channel_mode_inner(
                channel_id.into(),
                true,
                None,
                Some((stream_id, profile_id)),
                false,
                None,
            )
            .await?;
        Ok(channel.and_then(|mut channel| {
            if channel.streams.len() == 1 {
                Some(channel.streams.remove(0))
            } else {
                None
            }
        }))
    }
    pub(crate) async fn resolve_redirect_candidates(
        &self,
        channel_id: &str,
    ) -> Result<Vec<ProviderStream>, StoreError> {
        Ok(self
            .resolve_channel_mode_inner(channel_id.into(), true, None, None, false, None)
            .await?
            .map_or_else(Vec::new, |channel| channel.streams))
    }
    async fn resolve_channel_mode_inner(
        &self,
        lookup: PlaybackLookup<'_>,
        allow_redirect: bool,
        redirect_user: Option<&User>,
        borrowed: Option<(i64, i64)>,
        defer_redirect: bool,
        cache: Option<&crate::NotificationCache>,
    ) -> Result<Option<ResolvedChannel>, StoreError> {
        let settings = self.setting("stream_settings").await?;
        let default_profile = settings
            .get("default_stream_profile")
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()));
        let preview = lookup.preview;
        let object_id = if preview.is_some() {
            lookup.id.to_owned()
        } else {
            uuid::Uuid::parse_str(lookup.id)
                .map_err(|_| StoreError::NotFound)?
                .to_string()
        };
        let channel = if let Some(mode) = preview {
            sqlx::query("SELECT s.id::bigint AS id, (s.m3u_account_id IS NOT NULL) AS has_streams, s.name, NULL::bigint AS logo_id, COALESCE($2::bigint,s.stream_profile_id,$1::bigint) AS profile_id FROM dispatcharr_channels_stream s WHERE s.stream_hash=$3")
                .bind(default_profile).bind(mode.profile_override).bind(lookup.id).fetch_optional(self.pool()).await?.ok_or(StoreError::NotFound)?
        } else {
            sqlx::query("SELECT c.id::bigint AS id, EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream linked WHERE linked.channel_id=c.id) AS has_streams, COALESCE(o.name,c.name) AS name, COALESCE(o.logo_id,c.logo_id)::bigint AS logo_id, COALESCE(o.stream_profile_id,c.stream_profile_id,$2)::bigint AS profile_id FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.uuid=$1")
            .bind(uuid::Uuid::parse_str(lookup.id).map_err(|_|StoreError::NotFound)?).bind(default_profile).fetch_optional(self.pool()).await?.ok_or(StoreError::NotFound)?
        };
        let name: String = channel.try_get("name")?;
        let id: i64 = channel.try_get("id")?;
        let has_streams: bool = channel.try_get("has_streams")?;
        let logo_id: Option<i64> = channel.try_get("logo_id")?;
        let profile_id: Option<i64> = channel.try_get("profile_id")?;
        let profile_id =
            profile_id.ok_or(StoreError::Unsupported("No stream profile configured"))?;
        // CASE bounds the actual SQL wire values, not merely an already
        // allocated Rust String. An oversize value is explicit, never truncated.
        let profile = sqlx::query(r#"SELECT
            CASE WHEN octet_length(name)<=4096 THEN name END AS name,
            CASE WHEN octet_length(command)<=262144 THEN command END AS command,
            CASE WHEN octet_length(parameters)<=262144 THEN parameters END AS parameters,
            (octet_length(name)>4096 OR octet_length(command)>262144 OR octet_length(parameters)>262144) AS oversized,
            locked,is_active,user_agent_id::bigint AS user_agent_id
            FROM core_streamprofile WHERE id=$1"#)
            .bind(profile_id).fetch_optional(self.pool()).await?.ok_or(StoreError::Unsupported("Stream profile not found"))?;
        if profile.try_get::<bool, _>("oversized")? {
            return Err(playback_capacity());
        }
        // Source Channel/Stream.get_stream_profile resolves the saved/default
        // input profile even when its UI activation flag is false. Provider
        // account/profile and output-profile activation checks remain separate.
        let profile_name: String = profile.try_get("name")?;
        let profile_locked: bool = profile.try_get("locked")?;
        if preview.is_some() && profile_locked && profile_name == "Redirect" {
            return Err(StoreError::Unsupported(
                "Direct Redirect-profile Stream preview without Proxy override is unqualified",
            ));
        }
        let redirect = profile_locked && profile_name == "Redirect";
        if !allow_redirect && redirect {
            return Err(StoreError::Unsupported(
                "Redirect requires the direct response resolver",
            ));
        }
        let allowed = if allow_redirect && redirect {
            crate::redirect_playback::allowed(redirect_user)
                .map_err(|e| StoreError::StatusBody(e.status, e.body))?
        } else {
            None
        };
        // Source reuses Redis assignments BEFORE enumerating active SQL
        // candidates. A removed/deactivated assigned provider must still resolve
        // by its actual observed alias, with today's credentials and UA.
        if redirect && allowed.is_none() && borrowed.is_none() && defer_redirect {
            return Ok(Some(ResolvedChannel {
                redirect: true,
                numeric_channel_id: id,
                has_assigned_streams: has_streams,
                redirect_error: (!has_streams).then(|| "No streams assigned to channel".to_owned()),
                id: object_id.clone(),
                name,
                profile_id,
                streams: Vec::new(),
                shutdown_delay: Duration::ZERO,
                startup_behind: Duration::ZERO,
            }));
        }
        // Read-only SQL candidate hint, never reservation authority. The raw
        // actor must reobserve and adopt the actual sealed alias under its lease.
        let raw_observed = if !redirect && preview.is_none() && borrowed.is_none() {
            match cache {
                Some(cache) => cache.raw_source_assignment(id, &object_id).await?,
                None => None,
            }
        } else {
            None
        };
        let borrowed = borrowed.or(raw_observed);
        let restricted = allowed.is_some();
        let profile_command: String = profile.try_get("command")?;
        let profile_parameters: String = profile.try_get("parameters")?;
        let default_ua = settings
            .get("default_user_agent")
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()));
        // Project every potentially large text value through a byte bound in
        // this SELECT, including the selected UA. This replaces per-row UA reads.
        // Source provider requests ignore StreamProfile.user_agent: account UA
        // wins, then the configured Core default. Default-string cache I/O
        // timing remains an existing separate migration gate; do not eagerly
        // fill it for an alternate that Source has not actually selected.
        let rows = sqlx::query(r#"SELECT s.id::bigint AS stream_id,
            CASE WHEN octet_length(s.stream_hash)<=1024 THEN s.stream_hash END AS stream_hash,
            CASE WHEN octet_length(s.name)<=4096 THEN s.name END AS stream_name,
            CASE WHEN octet_length(s.url)<=65536 THEN s.url END AS url,
            a.account_type,a.id::bigint AS provider_account_id,a.server_group_id::bigint AS server_group_id,
            CASE WHEN octet_length(a.server_url)<=65536 THEN a.server_url END AS xc_server_url,
            CASE WHEN octet_length(a.username)<=65536 THEN a.username END AS xc_username,
            CASE WHEN octet_length(a.password)<=65536 THEN a.password END AS xc_password,
            (COALESCE(octet_length(a.username),0)>65536 OR COALESCE(octet_length(a.password),0)>65536) AS account_login_oversized,
            CASE WHEN octet_length(s.stream_id::text)<=4096 THEN s.stream_id::text END AS xc_stream_id,
            p.id::bigint AS provider_profile_id,
            CASE WHEN octet_length(p.name)<=4096 THEN p.name END AS provider_profile_name,
            p.max_streams,
            CASE WHEN octet_length(p.search_pattern)<=65536 THEN p.search_pattern END AS search_pattern,
            CASE WHEN octet_length(p.replace_pattern)<=65536 THEN p.replace_pattern END AS replace_pattern,
            CASE WHEN octet_length(COALESCE(NULLIF(ua.user_agent,''),du.user_agent))<=65536
              THEN COALESCE(NULLIF(ua.user_agent,''),du.user_agent) END AS user_agent,
            (COALESCE(octet_length(s.stream_hash),0)>1024 OR COALESCE(octet_length(s.name),0)>4096 OR COALESCE(octet_length(s.url),0)>65536
             OR COALESCE(octet_length(p.name),0)>4096 OR COALESCE(octet_length(p.search_pattern),0)>65536
             OR COALESCE(octet_length(p.replace_pattern),0)>65536 OR COALESCE(octet_length(COALESCE(NULLIF(ua.user_agent,''),du.user_agent)),0)>65536
             OR (a.account_type='XC' AND (COALESCE(octet_length(a.server_url),0)>65536
                 OR COALESCE(octet_length(a.username),0)>65536 OR COALESCE(octet_length(a.password),0)>65536
                 OR COALESCE(octet_length(s.stream_id::text),0)>4096))) AS oversized
            FROM dispatcharr_channels_stream s
            JOIN m3u_m3uaccountprofile p ON (($4::bigint IS NULL AND p.m3u_account_id=s.m3u_account_id) OR ($4=s.id AND p.id=$5::bigint))
            JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id
            LEFT JOIN dispatcharr_channels_channelstream cs ON cs.stream_id=s.id AND cs.channel_id=$1 AND $4::bigint IS NULL AND NOT $6::bool
            LEFT JOIN core_useragent ua ON ua.id=a.user_agent_id
            LEFT JOIN core_useragent du ON du.id=$2::bigint
            WHERE ($4::bigint IS NOT NULL AND s.id=$4 AND p.id=$5::bigint)
            OR ($4::bigint IS NULL AND (($6::bool AND s.id=$1) OR (NOT $6::bool AND cs.channel_id=$1)) AND ($6::bool OR a.is_active) AND p.is_active
            AND ($3::bigint[] IS NULL OR p.id=ANY($3))
            AND ($6::bool OR $3::bigint[] IS NOT NULL OR EXISTS(SELECT 1 FROM m3u_m3uaccountprofile d WHERE d.m3u_account_id=a.id AND d.is_default AND d.is_active)))
            ORDER BY cs."order",cs.id,p.is_default DESC,p.id LIMIT 33"#)
            .bind(id).bind(default_ua).bind(allowed).bind(borrowed.map(|v|v.0)).bind(borrowed.map(|v|v.1)).bind(preview.is_some()).fetch_all(self.pool()).await?;
        if rows.len() > MAX_CANDIDATES {
            return Err(playback_capacity());
        }
        for row in &rows {
            if row.try_get::<bool, _>("oversized")? {
                return Err(playback_capacity());
            }
        }
        let ordinary_std_pool = preview.is_none() && profile_locked && profile_name == "Proxy";
        let mut std_accounts = Vec::new();
        if ordinary_std_pool {
            for row in &rows {
                if row.try_get::<&str, _>("account_type")? == "STD"
                    && row.try_get::<Option<i64>, _>("server_group_id")?.is_some()
                    && row.try_get::<i32, _>("max_streams")? > 0
                {
                    std_accounts.push(row.try_get("provider_account_id")?);
                }
            }
            std_accounts.sort_unstable();
            std_accounts.dedup();
        }
        let std_samples = std_account_samples(self, &std_accounts).await?;
        let mut streams = Vec::new();
        let mut ffmpeg_template: Option<crate::source_input_profile::Template> = None;
        let mut unsupported = false;
        let mut credential_unavailable = false;
        for row in rows {
            let account_type: String = row.try_get("account_type")?;
            let search: String = row.try_get("search_pattern")?;
            let replace: String = row.try_get("replace_pattern")?;
            let stored: Option<String> = row.try_get("url")?;
            let server: Option<String> = row.try_get("xc_server_url")?;
            let username: Option<String> = row.try_get("xc_username")?;
            let password: Option<String> = row.try_get("xc_password")?;
            let stream_id: Option<String> = row.try_get("xc_stream_id")?;
            let url = match (ProviderUrl {
                account_type: &account_type,
                stored: stored.as_deref().unwrap_or_default(),
                server: server.as_deref().unwrap_or_default(),
                username: username.as_deref().unwrap_or_default(),
                password: password.as_deref().unwrap_or_default(),
                stream_id: stream_id.as_deref().unwrap_or_default(),
                search: &search,
                replace: &replace,
            })
            .resolve()
            {
                Ok(Some(url)) => url,
                Ok(None) => continue,
                Err(StoreError::Unsupported(_)) => {
                    unsupported = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if url.is_empty() {
                continue;
            }
            let user_agent = row
                .try_get::<Option<String>, _>("user_agent")?
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "Dispatcharr/0.33.0".to_owned());
            let input_command = if redirect {
                None
            } else {
                let input_kind = crate::source_input_profile::kind(&url);
                let proxy = profile_locked && profile_name == "Proxy";
                let automatic = proxy && input_kind != crate::source_input_profile::Kind::Ts;
                if automatic && ffmpeg_template.is_none() {
                    ffmpeg_template = Some(self.source_ffmpeg_template().await?);
                }
                let (command_name, command_locked, command_executable, command_parameters) =
                    if automatic {
                        let template = ffmpeg_template.as_ref().ok_or(StoreError::Unsupported(
                            "Automatic FFmpeg profile unavailable",
                        ))?;
                        (
                            template.name.as_str(),
                            true,
                            template.command.as_str(),
                            template.parameters.as_str(),
                        )
                    } else {
                        (
                            profile_name.as_str(),
                            profile_locked,
                            profile_command.as_str(),
                            profile_parameters.as_str(),
                        )
                    };
                let mut command = dispatcharr_contracts::StreamCommand::build(
                    command_name,
                    command_locked,
                    command_executable,
                    command_parameters,
                    Some(&url),
                    Some(&user_agent),
                    &serde_json::json!(id),
                )
                .map_err(|_| {
                    StoreError::Unsupported(
                        "Input profile command is outside the supported argument domain",
                    )
                })?;
                if input_kind == crate::source_input_profile::Kind::Udp {
                    crate::source_input_profile::udp_arguments(&mut command, &user_agent);
                }
                if proxy && !automatic {
                    None
                } else {
                    Some(command)
                }
            };
            if !redirect
                && input_command.is_none()
                && !(url.to_ascii_lowercase().starts_with("http://")
                    || url.to_ascii_lowercase().starts_with("https://"))
            {
                unsupported = true;
                continue;
            }
            let maximum: i32 = row.try_get("max_streams")?;
            let server_group: Option<i64> = row.try_get("server_group_id")?;
            let credential_pool = match (server_group, maximum > 0) {
                (Some(group), true) if account_type == "XC" => {
                    let Some(current) = crate::xc_account_provider::credentials(
                        server.as_deref().unwrap_or_default(),
                        username.as_deref().unwrap_or_default(),
                        password.as_deref().unwrap_or_default(),
                        &search,
                        &replace,
                    )?
                    else {
                        // An enforced credential group without a fingerprint has
                        // no capacity in Original; never fall back to a stale URL.
                        credential_unavailable = true;
                        continue;
                    };
                    Some(xc_credential_pool(
                        group,
                        maximum as u32,
                        &current.username,
                        &current.password,
                    )?)
                }
                (Some(group), true)
                    if account_type == "STD" && ordinary_std_pool && input_command.is_none() =>
                {
                    let account: i64 = row.try_get("provider_account_id")?;
                    let sample = std_samples
                        .get(&account)
                        .ok_or_else(unsupported_std_sample)?;
                    if sample.oversized || row.try_get::<bool, _>("account_login_oversized")? {
                        return Err(playback_capacity());
                    }
                    let sample = sample
                        .url
                        .as_deref()
                        .filter(|_| !sample.ambiguous)
                        .ok_or_else(unsupported_std_sample)?;
                    Some(std_sample_credential_pool(
                        group,
                        maximum as u32,
                        sample,
                        username.as_deref().unwrap_or_default(),
                        password.as_deref().unwrap_or_default(),
                    )?)
                }
                (Some(_), true) => {
                    return Err(StoreError::Unsupported(
                        "STD credential sample-URL precedence is not implemented",
                    ))
                }
                // Original unlimited profiles skip BOTH profile and credential counters.
                _ => None,
            };
            let mut metadata = vec![
                // Source uses the numeric Channel PK in metadata/aliases while
                // the worker/owner namespace remains the actual Channel UUID.
                ("channel_id".to_owned(), id.to_string()),
                (
                    "stream_name".to_owned(),
                    row.try_get::<String, _>("stream_name")?,
                ),
                (
                    "m3u_profile_name".to_owned(),
                    row.try_get::<String, _>("provider_profile_name")?,
                ),
                (
                    "stream_profile".to_owned(),
                    profile.try_get::<String, _>("name")?,
                ),
            ];
            if let Some(hash) = row.try_get::<Option<String>, _>("stream_hash")? {
                metadata.push(("source_stream_hash".into(), hash));
            }
            if let Some(mode) = preview {
                metadata.push(("source_preview".into(),serde_json::json!({"stream":row.try_get::<i64,_>("stream_id")?,"hash":lookup.id,"scoped":mode.scoped}).to_string()));
            }
            if raw_observed.is_some() {
                metadata.push(("source_assignment_required".into(), "true".into()));
            }
            if let Some(logo) = logo_id {
                metadata.push(("logo_id".into(), logo.to_string()));
            }
            streams.push(ProviderStream {
                input_command,
                stream_id: row.try_get("stream_id")?,
                provider_profile_id: row.try_get("provider_profile_id")?,
                provider_max_streams: u32::try_from(maximum).unwrap_or(u32::MAX),
                credential_pool,
                url,
                user_agent,
                metadata,
            });
        }
        if streams.is_empty() && !redirect {
            return Err(if credential_unavailable {
                StoreError::StatusBody(
                    503,
                    serde_json::json!({"error":"All active M3U profiles have reached maximum connection limits"}),
                )
            } else if unsupported {
                StoreError::Unsupported(
                    "Assigned providers require unimplemented URL/credential transforms",
                )
            } else {
                StoreError::Unsupported("No compatible active provider profiles assigned")
            });
        }
        if redirect && unsupported {
            return Err(StoreError::Unsupported(
                "Redirect provider URL transform is outside the supported Source domain",
            ));
        }
        let proxy = self.setting("proxy_settings").await?;
        Ok(Some(ResolvedChannel {
            redirect,
            numeric_channel_id: id,
            has_assigned_streams: has_streams,
            redirect_error: if redirect && streams.is_empty() {
                Some(
                    if !has_streams {
                        "No streams assigned to channel"
                    } else if restricted {
                        "No compatible active profile found for any assigned stream"
                    } else {
                        "No available streams for this channel"
                    }
                    .to_owned(),
                )
            } else {
                None
            },
            id: object_id.clone(),
            name,
            profile_id,
            streams,
            shutdown_delay: Duration::from_secs(
                proxy
                    .get("channel_shutdown_delay")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0),
            ),
            startup_behind: Duration::from_secs(
                proxy
                    .get("new_client_behind_seconds")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(5),
            ),
        }))
    }
}

impl AuthService {
    /// Resolve playback after the XC entry adapter authenticated its query/path
    /// credentials. The original XC policy replaces the global STREAMS check;
    /// this does not authenticate an arbitrary User supplied by an HTTP client.
    pub async fn resolve_xc_authorized(
        &self,
        mut request: PlaybackRequest,
        user: User,
        channel_id: &str,
        forced: Option<OutputFormat>,
    ) -> Result<PlaybackResolution, PlaybackError> {
        let invalid = || PlaybackError {
            status: 404,
            body: serde_json::json!({"error":"Not found"}),
        };
        let literal =
            dispatcharr_ingest_core::source_integer_literal(channel_id).ok_or_else(invalid)?;
        let channel = match literal.parse::<i64>() {
            Ok(pk) => self
                .store
                .channel_for_xc_user_by_id(&user, pk)
                .await
                .map_err(playback_error)?,
            Err(_) => None,
        };
        let channel = channel.ok_or_else(|| {
            if user.user_level >= 10 {
                PlaybackError {
                    status: 404,
                    body: serde_json::json!({"detail":"Not found."}),
                }
            } else {
                invalid()
            }
        })?;
        request.channel_id = channel.uuid.to_string();
        let channel = self
            .store
            .resolve_channel_mode_cached(
                &request.channel_id,
                true,
                Some(&user),
                self.notification_cache.as_ref(),
            )
            .await
            .map_err(playback_error)?
            .ok_or_else(|| PlaybackError::new(404, "Not found"))?;
        // Original handles a saved Redirect before any XC extension can select
        // an output format. XC does not enter the direct preview route.
        if channel.redirect {
            return self
                .playback_redirect_plan(request, Some(user), channel)
                .await;
        }
        self.playback_source_with_channel(request, Some(user), forced, channel)
            .await
            .map(|source| PlaybackResolution::Stream(Box::new(source)))
    }

    pub(crate) async fn build_playback_source(
        &self,
        request: PlaybackRequest,
        user: Option<User>,
        forced: Option<OutputFormat>,
    ) -> Result<PlaybackSource, PlaybackError> {
        if request
            .channel_id
            .rsplit_once(".p")
            .is_some_and(|(head, tail)| {
                !head.is_empty()
                    && !tail.is_empty()
                    && tail.bytes().all(|value| value.is_ascii_digit())
            })
        {
            return Err(PlaybackError::new(404, "Not found"));
        }
        let channel = self
            .store
            .resolve_channel_mode_cached(
                &request.channel_id,
                false,
                None,
                self.notification_cache.as_ref(),
            )
            .await
            .map_err(playback_error)?
            .ok_or_else(|| PlaybackError::new(404, "Not found"))?;
        self.playback_source_with_channel(request, user, forced, channel)
            .await
    }
    pub(crate) async fn playback_source_with_channel(
        &self,
        request: PlaybackRequest,
        user: Option<User>,
        forced: Option<OutputFormat>,
        channel: ResolvedChannel,
    ) -> Result<PlaybackSource, PlaybackError> {
        let stream_settings = self
            .store
            .setting("stream_settings")
            .await
            .map_err(playback_error)?;
        let output = match forced {
            Some(OutputFormat::MpegTs) => "mpegts".to_owned(),
            Some(OutputFormat::Fmp4) => "fmp4".to_owned(),
            Some(OutputFormat::Hls) => "hls".to_owned(),
            None => resolve_output_format(&request, user.as_ref(), &stream_settings),
        };
        let output_profile = if let Some(profile) =
            resolve_output_profile_id(&request, user.as_ref())
        {
            let row = sqlx::query("SELECT id::bigint AS id, CASE WHEN octet_length(command)<=262144 THEN command ELSE '' END AS command, CASE WHEN octet_length(parameters)<=262144 THEN parameters ELSE '' END AS parameters, octet_length(command)>262144 OR octet_length(parameters)>262144 AS oversized FROM core_outputprofile WHERE id=$1 AND is_active")
                .bind(profile).fetch_optional(self.store.pool()).await.map_err(StoreError::from).map_err(playback_error)?;
            row.map(|row| {
                let oversized: bool = row
                    .try_get("oversized")
                    .map_err(StoreError::from)
                    .map_err(playback_error)?;
                if oversized {
                    return Err(PlaybackError::new(
                        501,
                        "Output profile command exceeds supported bound",
                    ));
                }
                let id: i64 = row
                    .try_get("id")
                    .map_err(StoreError::from)
                    .map_err(playback_error)?;
                let command: String = row
                    .try_get("command")
                    .map_err(StoreError::from)
                    .map_err(playback_error)?;
                let parameters: String = row
                    .try_get("parameters")
                    .map_err(StoreError::from)
                    .map_err(playback_error)?;
                let command =
                    dispatcharr_contracts::StreamCommand::build_output(&command, &parameters)
                        .map_err(|_| {
                            PlaybackError::new(501, "Output profile command shape is unsupported")
                        })?;
                if !command.spawn_compatible() {
                    return Err(PlaybackError::new(
                        501,
                        "Output profile command cannot be spawned",
                    ));
                }
                Ok(dispatcharr_contracts::PlaybackOutputProfile { id, command })
            })
            .transpose()?
        } else {
            None
        };
        let limits = self
            .store
            .setting("user_limit_settings")
            .await
            .map_err(playback_error)?;
        assemble_playback_source(
            &request,
            user.as_ref(),
            self.source_client_ip(&request),
            &output,
            output_profile,
            &limits,
            channel,
        )
    }
}

pub(crate) fn assemble_playback_source(
    request: &PlaybackRequest,
    user: Option<&User>,
    ip: dispatcharr_contracts::SourceIpAddr,
    output: &str,
    output_profile: Option<dispatcharr_contracts::PlaybackOutputProfile>,
    limits: &serde_json::Value,
    channel: ResolvedChannel,
) -> Result<PlaybackSource, PlaybackError> {
    let defaults = UserLimitPolicy::default();
    let viewer = PlaybackViewer {
        user_id: user.map(|user| user.id),
        username: user.map(|user| user.username.clone()),
        ip_address: ip,
        user_agent: request.header("user-agent").unwrap_or("").to_string(),
        stream_limit: user.map_or(0, |user| user.stream_limit.max(0) as u32),
        limit_policy: UserLimitPolicy {
            terminate_on_limit_exceeded: limits
                .get("terminate_on_limit_exceeded")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(defaults.terminate_on_limit_exceeded),
            prioritize_single_client_channels: limits
                .get("prioritize_single_client_channels")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(defaults.prioritize_single_client_channels),
            ignore_same_channel_connections: limits
                .get("ignore_same_channel_connections")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(defaults.ignore_same_channel_connections),
            terminate_oldest: limits
                .get("terminate_oldest")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(defaults.terminate_oldest),
        },
    };
    let mut streams = channel.streams.into_iter();
    let stream = streams
        .next()
        .ok_or_else(|| PlaybackError::new(503, "No active stream available"))?;
    Ok(PlaybackSource {
        output_profile,
        input_command: stream.input_command,
        channel_id: channel.id,
        channel_name: channel.name,
        stream_id: stream.stream_id,
        profile_id: channel.profile_id,
        provider_profile_id: Some(stream.provider_profile_id),
        provider_max_streams: stream.provider_max_streams,
        credential_pool: stream.credential_pool,
        url: stream.url,
        user_agent: stream.user_agent,
        headers: Vec::new(),
        metadata: stream.metadata,
        output_format: match output {
            "fmp4" => OutputFormat::Fmp4,
            "hls" => OutputFormat::Hls,
            _ => OutputFormat::MpegTs,
        },
        shutdown_delay: channel.shutdown_delay,
        startup_behind: channel.startup_behind,
        alternates: streams
            .map(|stream| PlaybackCandidate {
                input_command: stream.input_command,
                stream_id: stream.stream_id,
                provider_profile_id: Some(stream.provider_profile_id),
                provider_max_streams: stream.provider_max_streams,
                credential_pool: stream.credential_pool,
                url: stream.url,
                user_agent: stream.user_agent,
                headers: Vec::new(),
                metadata: stream.metadata,
            })
            .collect(),
        viewer: Some(viewer),
    })
}

#[cfg(feature = "sqlite-copied-data-runtime")]
pub(crate) fn resolve_std_provider_url(
    url: &str,
    search: &str,
    replace: &str,
) -> Result<Option<String>, StoreError> {
    ProviderUrl {
        account_type: "STD",
        stored: url,
        server: "",
        username: "",
        password: "",
        stream_id: "",
        search,
        replace,
    }
    .resolve()
}

impl PlaybackResolver for AuthService {
    fn resolve_response(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<dispatcharr_contracts::PlaybackResolution, PlaybackError>> {
        Box::pin(self.resolve_direct_response(request))
    }
    fn authenticate_optional(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move {
            self.authenticate(&request)
                .await
                .map(|_| ())
                .map_err(playback_error)
        })
    }

    fn resolve(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<PlaybackSource, PlaybackError>> {
        Box::pin(async move {
            // Native TS is intentionally AllowAny in the original application;
            // presented credentials are still validated before any channel reuse.
            let user = self.authenticate(&request).await.map_err(playback_error)?;
            if !self
                .source_network_allowed(self.client_ip(&request), "STREAMS", None)
                .await
                .map_err(playback_error)?
            {
                return Err(PlaybackError::new(403, "Forbidden"));
            }
            self.build_playback_source(request, user, None).await
        })
    }

    fn authorize_admin(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move {
            let user = self.authenticate(&request).await.map_err(playback_error)?
                .ok_or_else(|| PlaybackError { status: 401, body: serde_json::json!({"detail":"Authentication credentials were not provided."}) })?;
            if user.user_level < 10
                || !self
                    .source_network_allowed(self.client_ip(&request), "UI", Some(&user))
                    .await
                    .map_err(playback_error)?
            {
                return Err(playback_error(StoreError::PermissionDenied));
            }
            Ok(())
        })
    }

    fn authorize_hls_control(
        &self,
        request: PlaybackRequest,
        check_stream_acl: bool,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move {
            let user=self.authenticate(&request).await.map_err(playback_error)?
                .ok_or_else(||PlaybackError{status:401,body:serde_json::json!({"detail":"Authentication credentials were not provided."})})?;
            if user.user_level < 1
                || !self
                    .source_network_allowed(self.client_ip(&request), "UI", Some(&user))
                    .await
                    .map_err(playback_error)?
            {
                return Err(playback_error(StoreError::PermissionDenied));
            }
            if check_stream_acl
                && !self
                    .source_network_allowed(self.client_ip(&request), "STREAMS", Some(&user))
                    .await
                    .map_err(playback_error)?
            {
                return Err(PlaybackError::new(403, "Forbidden"));
            }
            Ok(())
        })
    }

    fn authorize_hls_owner(
        &self,
        request: PlaybackRequest,
        owner: Option<i64>,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move {
            let user=self.authenticate(&request).await.map_err(playback_error)?.ok_or_else(||PlaybackError{status:401,body:serde_json::json!({"detail":"Authentication credentials were not provided."})})?;
            if user.user_level < 1
                || !self
                    .source_network_allowed(self.client_ip(&request), "UI", Some(&user))
                    .await
                    .map_err(playback_error)?
            {
                return Err(playback_error(StoreError::PermissionDenied));
            }
            if !self
                .source_network_allowed(self.client_ip(&request), "STREAMS", Some(&user))
                .await
                .map_err(playback_error)?
            {
                return Err(PlaybackError::new(403, "Forbidden"));
            }
            if owner != Some(user.id) {
                return Err(PlaybackError::new(404, "Session not found"));
            }
            Ok(())
        })
    }
}

pub(crate) fn playback_error(error: StoreError) -> PlaybackError {
    match error {
        StoreError::Database(_) => PlaybackError::new(503, "Database unavailable"),
        StoreError::NotFound => PlaybackError {
            status: 404,
            body: serde_json::json!({"detail":"No Stream matches the given query."}),
        },
        StoreError::Forbidden => PlaybackError::new(403, "Forbidden"),
        StoreError::PermissionDenied => PlaybackError {
            status: 403,
            body: serde_json::json!({"detail":"You do not have permission to perform this action."}),
        },
        StoreError::AuthenticationBody(body) => PlaybackError { status: 401, body },
        StoreError::Unauthorized(detail) => PlaybackError {
            status: 401,
            body: serde_json::json!({"detail":detail}),
        },
        StoreError::Unsupported(detail) => PlaybackError::new(501, detail),
        StoreError::RateLimited(_) => PlaybackError::new(429, "Too many attempts"),
        StoreError::Throttled { body, .. } => PlaybackError { status: 429, body },
        StoreError::XcDenied { status, .. } => PlaybackError {
            status,
            body: crate::xc_auth::failure_body(status),
        },
        StoreError::InvalidInput(body) => PlaybackError { status: 400, body },
        StoreError::UiPermissionFault => PlaybackError {
            status: 500,
            body: serde_json::json!({"detail":"UI permission failed"}),
        },
        // Type integration only: opt-in REST fault is not a playback authority.
        StoreError::UnhandledRestFault => PlaybackError::new(500, "Unhandled REST request"),
        StoreError::StatusBody(status, body) => PlaybackError { status, body },
    }
}

fn resolve_output_format(
    request: &PlaybackRequest,
    user: Option<&User>,
    settings: &serde_json::Value,
) -> String {
    let parameter = request
        .parameter("output_format")
        .filter(|value| !value.is_empty())
        .or_else(|| {
            request
                .parameter("output")
                .filter(|value| !value.is_empty())
        });
    if let Some(format) = parameter.and_then(|value| match value {
        "mpegts" | "ts" => Some("mpegts"),
        "fmp4" | "mp4" => Some("fmp4"),
        "hls" | "m3u8" => Some("hls"),
        _ => None,
    }) {
        return format.into();
    }
    user.and_then(|user| user.custom_properties.get("output_format"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            settings
                .get("default_output_format")
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or("mpegts")
        .to_owned()
}
pub(crate) fn resolve_output_profile_id(
    request: &PlaybackRequest,
    user: Option<&User>,
) -> Option<i64> {
    let parse_id = |value: &str| {
        dispatcharr_ingest_core::source_integer_literal(value)?
            .parse()
            .ok()
    };
    // Invalid truthy query parameters explicitly disable a user preference;
    // they do not fall back to it, matching _resolve_output_profile.
    if let Some(value) = request
        .parameter("output_profile")
        .filter(|value| !value.is_empty())
    {
        return parse_id(value);
    }
    match user.and_then(|user| user.custom_properties.get("output_profile"))? {
        serde_json::Value::Bool(value) => Some(i64::from(*value)),
        serde_json::Value::String(value) => parse_id(value),
        serde_json::Value::Number(value) => value.as_i64().or_else(|| {
            value
                .as_f64()
                .filter(|value| {
                    value.is_finite() && *value >= i64::MIN as f64 && *value <= i64::MAX as f64
                })
                .map(|value| value as i64)
        }),
        _ => None,
    }
}

// Source Channel.get_stream checks reusable public assignments before today's
// active SQL membership. This read grants no provider or ownership capability.
impl crate::NotificationCache {
    pub(crate) async fn raw_source_assignment(
        &self,
        numeric: i64,
        worker: &str,
    ) -> Result<Option<(i64, i64)>, StoreError> {
        const READ: &str = r#"
local sid=redis.call('GET',KEYS[1]); if not sid then return {} end
local pid=redis.call('GET','stream_profile:'..sid); if not pid then return {} end
if redis.call('EXISTS',KEYS[2])~=0 then
 local state=redis.call('HGET',KEYS[2],'state')
 if not (state=='active' or state=='waiting_for_clients' or state=='buffering' or state=='initializing' or state=='connecting') then return {} end
end
local s=tonumber(sid);local p=tonumber(pid)
if not s or not p or s<=0 or p<=0 or s%1~=0 or p%1~=0 then return redis.error_reply('Invalid source assignment IDs') end
return {sid,pid}
"#;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(READ)
            .arg(2)
            .arg(format!("channel_stream:{numeric}"))
            .arg(format!("live:channel:{worker}:metadata"));
        let rows: Vec<i64> = self.command(cmd).await?;
        match rows.as_slice() {
            [] => Ok(None),
            [stream, profile] => Ok(Some((*stream, *profile))),
            _ => Err(StoreError::Unsupported(
                "Source assignment read returned an invalid shape",
            )),
        }
    }
}

#[cfg(test)]
mod output_tests {
    use super::*;
    use serde_json::json;
    fn request(query: Vec<(&str, &str)>) -> PlaybackRequest {
        PlaybackRequest {
            channel_id: "fixture".into(),
            query: query
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            headers: Vec::new(),
            peer_ip: "127.0.0.1".parse().unwrap(),
        }
    }
    fn user(properties: serde_json::Value) -> User {
        User {
            id: 1,
            username: "fixture".into(),
            password: String::new(),
            is_active: true,
            user_level: 0,
            stream_limit: 0,
            custom_properties: properties,
        }
    }
    #[test]
    fn grouped_xc_current_login_fingerprint_matches_source_normalization() {
        let a = xc_credential_pool(7, 2, "  CURRENT-Login\t", "\u{001c} 密碼 \u{001f}").unwrap();
        let b = xc_credential_pool(7, 2, "current-login", "密碼").unwrap();
        assert_eq!(a, b);
        assert_eq!(a.counter_key, "server_group_connections:7:1ef5e20b3f61d0a8");
        assert_ne!(
            a.counter_key,
            xc_credential_pool(7, 2, "current-login", "rotated")
                .unwrap()
                .counter_key
        );
        assert_ne!(
            a.counter_key,
            xc_credential_pool(8, 2, "current-login", "密碼")
                .unwrap()
                .counter_key
        );
        assert!(xc_credential_pool(7, 2, "Σ", "password").is_err());
    }
    #[test]
    fn ordinary_std_account_sample_shares_typed_and_saved_pool_identity() {
        let selected = "pool-test";
        let secret = format!("{selected}-pair");
        let unselected = format!("{selected}-other");
        let typed = format!("http://origin.invalid/live/{selected}/{secret}/1.ts");
        let short = format!("http://origin.invalid/{unselected}/{secret}/1.ts");
        let typed_pool = std_sample_credential_pool(7, 1, &typed, &unselected, &secret).unwrap();
        let saved_pool = std_sample_credential_pool(7, 1, &short, selected, &secret).unwrap();
        let distinct_pool = std_sample_credential_pool(7, 1, &short, "", "").unwrap();
        assert_eq!(typed_pool, saved_pool);
        assert_ne!(typed_pool, distinct_pool);
        assert_eq!(
            typed_pool,
            xc_credential_pool(7, 1, selected, &secret).unwrap()
        );
    }

    #[test]
    fn ordinary_std_account_sample_rejects_unqualified_path_shapes() {
        for suffix in [
            "/",
            "/live/only-two.ts",
            "/movie/a/b/1.ts",
            "/a/b/1.ts?query=1",
            "/a/b/1.ts/",
            "/a%20b/c/1.ts",
        ] {
            let sample = format!("http://origin.invalid{suffix}");
            assert!(std_sample_credential_pool(7, 1, &sample, "", "").is_err());
        }
    }
    #[test]
    fn xc_live_uses_rotated_login_instead_of_stale_imported_url() {
        let provider = ProviderUrl {
            account_type: "XC",
            stored: "https://stale.invalid/live/old/old/4.ts",
            server: "https://current.invalid/provider/player_api.php?username=old#ignored",
            username: "new-user",
            password: "new-password",
            stream_id: "45",
            search: "^(.*)$",
            replace: "$1",
        };
        assert_eq!(
            provider.resolve().unwrap().as_deref(),
            Some("https://current.invalid/provider/live/new-user/new-password/45.ts")
        );
        let rotated = ProviderUrl {
            password: "rotated",
            ..provider
        };
        assert_eq!(
            rotated.resolve().unwrap().as_deref(),
            Some("https://current.invalid/provider/live/new-user/rotated/45.ts")
        );
    }
    #[test]
    fn xc_missing_current_credentials_never_reuses_stale_login() {
        let provider = ProviderUrl {
            account_type: "XC",
            stored: "https://stale.invalid/old",
            server: "https://current.invalid",
            username: "current",
            password: "",
            stream_id: "45",
            search: "^(.*)$",
            replace: "$1",
        };
        assert!(provider.resolve().unwrap().is_none());
    }
    #[test]
    fn xc_without_stream_identity_retains_source_stored_url_fallback() {
        let provider = ProviderUrl {
            account_type: "XC",
            stored: "https://stored.invalid/live.ts",
            server: "",
            username: "",
            password: "",
            stream_id: "",
            search: "^(.*)$",
            replace: "$1",
        };
        assert_eq!(
            provider.resolve().unwrap().as_deref(),
            Some(provider.stored)
        );
        assert_eq!(
            ProviderUrl {
                account_type: "STD",
                search: "unmatched",
                replace: "",
                ..provider
            }
            .resolve()
            .unwrap()
            .as_deref(),
            Some(provider.stored)
        );
    }
    #[test]
    fn query_aliases_and_user_global_precedence_match_original_helpers() {
        let user = user(json!({"output_format":"hls"}));
        assert_eq!(
            resolve_output_format(&request(vec![("output", "mp4")]), Some(&user), &json!({})),
            "fmp4"
        );
        assert_eq!(
            resolve_output_format(
                &request(vec![("output_format", "ts")]),
                Some(&user),
                &json!({"default_output_format":"fmp4"})
            ),
            "mpegts"
        );
        assert_eq!(
            resolve_output_format(
                &request(vec![("output_format", "invalid"), ("output", "hls")]),
                None,
                &json!({})
            ),
            "mpegts"
        );
        assert_eq!(
            resolve_output_format(
                &request(vec![]),
                None,
                &json!({"default_output_format":"hls"})
            ),
            "hls"
        );
        assert_eq!(
            resolve_output_format(
                &request(vec![("output_format", ""), ("output", "m3u8")]),
                None,
                &json!({})
            ),
            "hls"
        );
    }
    #[test]
    fn output_profile_id_accepts_source_integer_spelling() {
        let saved = user(json!({"output_profile":"７_"}));
        assert_eq!(
            resolve_output_profile_id(&request(vec![]), Some(&saved)),
            None
        );
        let saved = user(json!({"output_profile":" +٠_٧ "}));
        assert_eq!(
            resolve_output_profile_id(&request(vec![]), Some(&saved)),
            Some(7)
        );
        for value in ["1_2", "١٢", "\u{a0}+１２\u{3000}"] {
            assert_eq!(
                resolve_output_profile_id(&request(vec![("output_profile", value)]), Some(&saved)),
                Some(12)
            );
        }
        for value in ["1__2", "¹²", "9223372036854775808"] {
            assert_eq!(
                resolve_output_profile_id(&request(vec![("output_profile", value)]), Some(&saved)),
                None
            );
        }
    }

    #[test]
    fn invalid_output_profile_query_suppresses_user_preference() {
        let user = user(json!({"output_profile":7}));
        assert_eq!(
            resolve_output_profile_id(&request(vec![]), Some(&user)),
            Some(7)
        );
        assert_eq!(
            resolve_output_profile_id(&request(vec![("output_profile", "bad")]), Some(&user)),
            None
        );
        assert_eq!(
            resolve_output_profile_id(&request(vec![("output_profile", "")]), Some(&user)),
            Some(7)
        );
        assert_eq!(
            resolve_output_profile_id(&request(vec![("output_profile", "0")]), Some(&user)),
            Some(0)
        );
    }

    #[tokio::test]
    #[ignore = "requires real isolated Redis"]
    async fn raw_sql_hint_is_read_only_and_distinguishes_missing_from_inactive_metadata() {
        let url = std::env::var("REDIS_TEST_URL").expect("isolated Redis required");
        let cache = crate::NotificationCache::connect(&url).await.unwrap();
        let mut connection = redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let worker = uuid::Uuid::new_v4().to_string();
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let stream = id + 1;
        let profile = id + 2;
        let channel_key = format!("channel_stream:{id}");
        let stream_key = format!("stream_profile:{stream}");
        let metadata = format!("live:channel:{worker}:metadata");
        let counter = format!("profile_connections:{profile}");
        let _: () = redis::pipe()
            .set(&channel_key, stream)
            .set(&stream_key, profile)
            .set(&counter, 2)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            cache.raw_source_assignment(id, &worker).await.unwrap(),
            Some((stream, profile))
        );
        for state in [
            "active",
            "waiting_for_clients",
            "buffering",
            "initializing",
            "connecting",
            "stopped",
        ] {
            let _: () = redis::cmd("HSET")
                .arg(&metadata)
                .arg("state")
                .arg(state)
                .query_async(&mut connection)
                .await
                .unwrap();
            assert_eq!(
                cache.raw_source_assignment(id, &worker).await.unwrap(),
                (state != "stopped").then_some((stream, profile))
            );
        }
        let actual: (i64, i64, i64) = redis::pipe()
            .get(&channel_key)
            .get(&stream_key)
            .get(&counter)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(actual, (stream, profile, 2));
        let _: () = redis::cmd("DEL")
            .arg(&[channel_key, stream_key, metadata, counter])
            .query_async(&mut connection)
            .await
            .unwrap();
    }
}
