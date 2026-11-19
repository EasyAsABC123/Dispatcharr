//! Source direct Redirect-mode response selection, before live-worker reuse.
//! Original Dispatcharr authors retain algorithm/application credit.
use crate::playback::playback_error;
use crate::{AuthService, User};
use dispatcharr_contracts::{
    PlaybackCandidate, PlaybackError, PlaybackRedirectPlan, PlaybackRequest, PlaybackResolution,
    PlaybackViewer, UserLimitPolicy,
};
use serde_json::Value;
use sqlx::Row;
pub(crate) fn allowed(user: Option<&User>) -> Result<Option<Vec<i64>>, PlaybackError> {
    let Some(value) = user.and_then(|u| u.custom_properties.get("allowed_m3u_profile_ids")) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(rows) = value.as_array() else {
        return Ok(Some(Vec::new()));
    };
    if rows.len() > 65536 {
        return Err(PlaybackError::new(
            503,
            "Allowed provider profile capacity reached",
        ));
    }
    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        let id = match row {
            Value::Bool(v) => Some(i64::from(*v)),
            Value::Number(v) => v.as_i64(),
            Value::String(v) => v.trim().parse().ok(),
            _ => None,
        };
        let Some(id) = id else {
            return Err(PlaybackError::new(
                501,
                "Malformed provider allowlist value outside supported Source domain",
            ));
        };
        ids.push(id);
    }
    Ok(Some(ids))
}
struct BorrowedResolver {
    store: crate::Store,
    channel: String,
}
impl dispatcharr_contracts::RedirectAliasResolver for BorrowedResolver {
    fn candidates(
        &self,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Vec<PlaybackCandidate>, PlaybackError>> {
        Box::pin(async move {
            Ok(self
                .store
                .resolve_redirect_candidates(&self.channel)
                .await
                .map_err(playback_error)?
                .into_iter()
                .map(|s| PlaybackCandidate {
                    input_command: None,
                    stream_id: s.stream_id,
                    provider_profile_id: Some(s.provider_profile_id),
                    provider_max_streams: s.provider_max_streams,
                    credential_pool: s.credential_pool,
                    url: s.url,
                    user_agent: s.user_agent,
                    headers: Vec::new(),
                    metadata: s.metadata,
                })
                .collect())
        })
    }
    fn resolve(
        &self,
        stream_id: i64,
        profile_id: i64,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Option<PlaybackCandidate>, PlaybackError>>
    {
        Box::pin(async move {
            Ok(self
                .store
                .resolve_redirect_alias_candidate(&self.channel, stream_id, profile_id)
                .await
                .map_err(playback_error)?
                .map(|s| PlaybackCandidate {
                    input_command: None,
                    stream_id: s.stream_id,
                    provider_profile_id: Some(s.provider_profile_id),
                    provider_max_streams: s.provider_max_streams,
                    credential_pool: s.credential_pool,
                    url: s.url,
                    user_agent: s.user_agent,
                    headers: Vec::new(),
                    metadata: s.metadata,
                }))
        })
    }
}
struct PreviewSelection<'a> {
    hash: &'a str,
    override_profile: Option<i64>,
    stream: i64,
    provider: i64,
    scoped: bool,
}
impl AuthService {
    pub(crate) async fn resolve_direct_response(
        &self,
        request: PlaybackRequest,
    ) -> Result<PlaybackResolution, PlaybackError> {
        let user = self.authenticate(&request).await.map_err(playback_error)?;
        if !self
            .source_network_allowed(self.client_ip(&request), "STREAMS", None)
            .await
            .map_err(playback_error)?
        {
            return Err(PlaybackError::new(403, "Forbidden"));
        }
        // Auth, network and channel selection precede this opt-out, as in Source.
        if request
            .channel_id
            .rsplit_once(".p")
            .is_some_and(|(head, tail)| {
                !head.is_empty() && !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit())
            })
        {
            return Err(PlaybackError::new(404, "Not found"));
        }
        let skip = request
            .parameter("skip_redirect")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            && user.as_ref().is_some_and(|u| u.user_level >= 1);
        let channel = match self
            .store
            .resolve_channel_mode_cached(
                &request.channel_id,
                true,
                user.as_ref(),
                self.notification_cache.as_ref(),
            )
            .await
        {
            Ok(Some(channel)) => channel,
            Ok(None) | Err(crate::StoreError::NotFound) => {
                return self
                    .resolve_direct_stream_preview(request, user, skip)
                    .await
            }
            Err(error) => return Err(playback_error(error)),
        };
        if !channel.redirect {
            return self
                .playback_source_with_channel(request, user, None, channel)
                .await
                .map(|v| PlaybackResolution::Stream(Box::new(v)));
        }
        let capacity_only = allowed(user.as_ref())?.is_some();
        if skip {
            if let Some(proxy) = self.locked_proxy_profile().await? {
                let candidates = self
                    .store
                    .resolve_preview_targets(&request.channel_id, user.as_ref())
                    .await
                    .map_err(playback_error)?;
                for candidate in candidates.streams {
                    if self.preview_profile_available(&candidate, true).await? {
                        let hash = candidate
                            .metadata
                            .iter()
                            .find(|(k, _)| k == "source_stream_hash")
                            .map(|(_, v)| v.clone())
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| PlaybackError::new(503, "Stream has no preview id"))?;
                        return self
                            .selected_stream_preview(
                                request,
                                user,
                                PreviewSelection {
                                    hash: &hash,
                                    override_profile: Some(proxy),
                                    stream: candidate.stream_id,
                                    provider: candidate.provider_profile_id,
                                    scoped: true,
                                },
                            )
                            .await;
                    }
                }
                return Err(PlaybackError::new(
                    503,
                    "No compatible streams available for preview",
                ));
            }
            // Source Channel Redirect falls back to ordinary handoff when the
            // locked Proxy template does not exist. Stream preview differs.
        }
        let proxy = self
            .store
            .setting("proxy_settings")
            .await
            .map_err(playback_error)?;
        let validate_urls = proxy
            .get("validate_redirect_urls")
            .is_none_or(crate::channel_maintenance::source_truthy);
        let limits = self
            .store
            .setting("user_limit_settings")
            .await
            .map_err(playback_error)?;
        let defaults = UserLimitPolicy::default();
        let policy =
            |key: &str, fallback| limits.get(key).and_then(Value::as_bool).unwrap_or(fallback);
        let viewer = PlaybackViewer {
            user_id: user.as_ref().map(|u| u.id),
            username: user.as_ref().map(|u| u.username.clone()),
            ip_address: self.source_client_ip(&request),
            user_agent: request.header("user-agent").unwrap_or("").to_owned(),
            stream_limit: user.as_ref().map_or(0, |u| u.stream_limit.max(0) as u32),
            limit_policy: UserLimitPolicy {
                terminate_on_limit_exceeded: policy(
                    "terminate_on_limit_exceeded",
                    defaults.terminate_on_limit_exceeded,
                ),
                prioritize_single_client_channels: policy(
                    "prioritize_single_client_channels",
                    defaults.prioritize_single_client_channels,
                ),
                ignore_same_channel_connections: policy(
                    "ignore_same_channel_connections",
                    defaults.ignore_same_channel_connections,
                ),
                terminate_oldest: policy("terminate_oldest", defaults.terminate_oldest),
            },
        };
        let assignment = (!capacity_only).then(|| dispatcharr_contracts::RedirectAssignmentPlan {
            numeric_channel_id: channel.numeric_channel_id,
            has_assigned_streams: channel.has_assigned_streams,
            resolver: std::sync::Arc::new(BorrowedResolver {
                store: self.store.clone(),
                channel: channel.id.clone(),
            }),
        });
        let candidates = channel
            .streams
            .into_iter()
            .map(|s| PlaybackCandidate {
                input_command: None,
                stream_id: s.stream_id,
                provider_profile_id: Some(s.provider_profile_id),
                provider_max_streams: s.provider_max_streams,
                credential_pool: s.credential_pool,
                url: s.url,
                user_agent: s.user_agent,
                headers: Vec::new(),
                metadata: s.metadata,
            })
            .collect();
        Ok(PlaybackResolution::Redirect(Box::new(
            PlaybackRedirectPlan {
                channel_id: channel.id,
                candidates,
                viewer,
                capacity_only,
                assignment,
                validate_urls,
                selection_error: channel.redirect_error,
            },
        )))
    }
    async fn locked_proxy_profile(&self) -> Result<Option<i64>, PlaybackError> {
        let row = sqlx::query(
            "SELECT id::bigint AS id FROM core_streamprofile WHERE name='Proxy' AND locked",
        )
        .fetch_optional(self.store.pool())
        .await
        .map_err(crate::StoreError::from)
        .map_err(playback_error)?;
        row.map(|row| {
            row.try_get("id")
                .map_err(crate::StoreError::from)
                .map_err(playback_error)
        })
        .transpose()
    }
    async fn preview_profile_available(
        &self,
        candidate: &crate::playback::ProviderStream,
        scoped: bool,
    ) -> Result<bool, PlaybackError> {
        let hash = candidate
            .metadata
            .iter()
            .find(|(k, _)| k == "source_stream_hash")
            .map_or("", |(_, v)| v.as_str());
        let worker = if scoped {
            format!("{hash}.p{}", candidate.provider_profile_id)
        } else {
            hash.to_owned()
        };
        let alias = if scoped {
            format!(
                "stream_profile:{}:p{}",
                candidate.stream_id, candidate.provider_profile_id
            )
        } else {
            format!("stream_profile:{}:preview", candidate.stream_id)
        };
        let cache = self
            .notification_cache
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Preview cache unavailable"))?;
        const READ: &str = r#"
local assigned=redis.call('GET',KEYS[1])
if assigned and assigned~='' then
 local state=redis.call('HGET',KEYS[2],'state')
 if redis.call('EXISTS',KEYS[2])==0 or state=='active' or state=='waiting_for_clients' or state=='buffering' or state=='initializing' or state=='connecting' then return 1 end
end
if tonumber(ARGV[1])==0 then return 1 end
local count=tonumber(redis.call('GET',KEYS[3])or'0');local group=KEYS[4]~='' and tonumber(redis.call('GET',KEYS[4])or'0')or 0
if not count or not group then return redis.error_reply('Invalid preview capacity') end
if count<tonumber(ARGV[1]) and (tonumber(ARGV[2])==0 or group<tonumber(ARGV[2])) then return 1 end
return 0
"#;
        let mut command = redis::cmd("EVAL");
        command
            .arg(READ)
            .arg(4)
            .arg(alias)
            .arg(format!("live:channel:{worker}:metadata"))
            .arg(format!(
                "profile_connections:{}",
                candidate.provider_profile_id
            ))
            .arg(
                candidate
                    .credential_pool
                    .as_ref()
                    .map_or("", |p| p.counter_key.as_str()),
            )
            .arg(candidate.provider_max_streams)
            .arg(
                candidate
                    .credential_pool
                    .as_ref()
                    .map_or(0, |p| p.max_streams),
            );
        let value: i64 = cache.command(command).await.map_err(playback_error)?;
        Ok(value == 1)
    }
    async fn selected_stream_preview(
        &self,
        mut request: PlaybackRequest,
        user: Option<User>,
        selection: PreviewSelection<'_>,
    ) -> Result<PlaybackResolution, PlaybackError> {
        let PreviewSelection {
            hash,
            override_profile,
            stream,
            provider,
            scoped,
        } = selection;
        let mut channel = self
            .store
            .resolve_preview_channel(hash, override_profile, Some((stream, provider)), scoped)
            .await
            .map_err(playback_error)?;
        channel.id = if scoped {
            format!("{hash}.p{provider}")
        } else {
            hash.to_owned()
        };
        request.channel_id = channel.id.clone();
        self.playback_source_with_channel(request, user, None, channel)
            .await
            .map(|v| PlaybackResolution::Stream(Box::new(v)))
    }
    async fn resolve_direct_stream_preview(
        &self,
        request: PlaybackRequest,
        user: Option<User>,
        skip: bool,
    ) -> Result<PlaybackResolution, PlaybackError> {
        let settings = self
            .store
            .setting("stream_settings")
            .await
            .map_err(playback_error)?;
        let default_profile = settings
            .get("default_stream_profile")
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()));
        let row = sqlx::query(
            "SELECT s.id::bigint AS id,s.m3u_account_id::bigint AS account,a.is_active AS account_active, \
             COALESCE(s.stream_profile_id,$2::bigint)::bigint AS profile_id \
             FROM dispatcharr_channels_stream s LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id \
             WHERE s.stream_hash=$1",
        )
        .bind(&request.channel_id)
        .bind(default_profile)
        .fetch_optional(self.store.pool())
        .await
        .map_err(crate::StoreError::from)
        .map_err(playback_error)?
        .ok_or_else(|| playback_error(crate::StoreError::NotFound))?;
        let id: i64 = row
            .try_get("id")
            .map_err(crate::StoreError::from)
            .map_err(playback_error)?;
        let account: Option<i64> = row
            .try_get("account")
            .map_err(crate::StoreError::from)
            .map_err(playback_error)?;
        if account.is_none() {
            return Err(PlaybackError::new(503, "Stream has no M3U account"));
        }
        let profile: Option<i64> = row
            .try_get("profile_id")
            .map_err(crate::StoreError::from)
            .map_err(playback_error)?;
        let named = sqlx::query("SELECT name,locked FROM core_streamprofile WHERE id=$1")
            .bind(profile)
            .fetch_optional(self.store.pool())
            .await
            .map_err(crate::StoreError::from)
            .map_err(playback_error)?
            .ok_or_else(|| PlaybackError::new(500, "Stream profile not found"))?;
        let redirect = named
            .try_get::<bool, _>("locked")
            .map_err(crate::StoreError::from)
            .map_err(playback_error)?
            && named
                .try_get::<String, _>("name")
                .map_err(crate::StoreError::from)
                .map_err(playback_error)?
                == "Redirect";
        let override_profile = if skip && redirect {
            Some(
                self.locked_proxy_profile()
                    .await?
                    .ok_or_else(|| PlaybackError::new(500, "Proxy stream profile not found"))?,
            )
        } else {
            None
        };
        let allowed = if skip { allowed(user.as_ref())? } else { None };
        if skip
            && !row
                .try_get::<Option<bool>, _>("account_active")
                .map_err(crate::StoreError::from)
                .map_err(playback_error)?
                .unwrap_or(false)
        {
            return Err(PlaybackError::new(
                503,
                "No compatible M3U profile available for preview",
            ));
        }
        // Read the default preview's already selected profile before capacity.
        let existing = if !skip {
            self.default_preview_profile(id, &request.channel_id)
                .await?
        } else {
            None
        };
        if let Some(provider) = existing {
            return self
                .selected_stream_preview(
                    request.clone(),
                    user,
                    PreviewSelection {
                        hash: &request.channel_id,
                        override_profile: None,
                        stream: id,
                        provider,
                        scoped: false,
                    },
                )
                .await;
        }
        if allowed.is_none() {
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM m3u_m3uaccountprofile WHERE m3u_account_id=$1 AND is_default AND (NOT $2::bool OR is_active))").bind(account).bind(skip).fetch_one(self.store.pool()).await.map_err(crate::StoreError::from).map_err(playback_error)?;
            if !exists {
                return Err(PlaybackError::new(
                    503,
                    "No compatible M3U profile available for preview",
                ));
            }
        }
        let candidates = self
            .store
            .resolve_preview_channel(&request.channel_id, override_profile, None, skip)
            .await
            .map_err(playback_error)?;
        if !skip {
            return self
                .playback_source_with_channel(request, user, None, candidates)
                .await
                .map(|source| PlaybackResolution::Stream(Box::new(source)));
        }
        for candidate in candidates.streams {
            if allowed
                .as_ref()
                .is_some_and(|ids| !ids.contains(&candidate.provider_profile_id))
            {
                continue;
            }
            if self.preview_profile_available(&candidate, skip).await? {
                return self
                    .selected_stream_preview(
                        request.clone(),
                        user,
                        PreviewSelection {
                            hash: &request.channel_id,
                            override_profile,
                            stream: id,
                            provider: candidate.provider_profile_id,
                            scoped: skip,
                        },
                    )
                    .await;
            }
        }
        Err(PlaybackError::new(
            503,
            "No compatible M3U profile available for preview",
        ))
    }
    async fn default_preview_profile(
        &self,
        stream: i64,
        hash: &str,
    ) -> Result<Option<i64>, PlaybackError> {
        let cache = self
            .notification_cache
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Preview cache unavailable"))?;
        let mut command = redis::cmd("EVAL");
        command.arg(r#"local pid=redis.call('GET',KEYS[1]);if not pid or pid=='' then return {} end;local state=redis.call('HGET',KEYS[2],'state');if redis.call('EXISTS',KEYS[2])==0 or state=='active' or state=='waiting_for_clients' or state=='buffering' or state=='initializing' or state=='connecting' then return {pid} end;return {}"#).arg(2).arg(format!("stream_profile:{stream}:preview")).arg(format!("live:channel:{hash}:metadata"));
        let values: Vec<i64> = cache.command(command).await.map_err(playback_error)?;
        Ok(values.first().copied())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn user(custom_properties: Value) -> User {
        User {
            id: 1,
            username: "redirect-policy-fixture".into(),
            password: String::new(),
            is_active: true,
            user_level: 1,
            stream_limit: 1,
            custom_properties,
        }
    }
    #[test]
    fn absent_null_and_explicit_empty_provider_authorities_never_collapse() {
        for value in [
            serde_json::json!({}),
            serde_json::json!({"allowed_m3u_profile_ids":null}),
        ] {
            assert!(allowed(Some(&user(value))).unwrap().is_none());
        }
        for value in [
            serde_json::json!({"allowed_m3u_profile_ids":[]}),
            serde_json::json!({"allowed_m3u_profile_ids":"malformed"}),
        ] {
            assert_eq!(allowed(Some(&user(value))).unwrap(), Some(Vec::new()));
        }
        assert_eq!(
            allowed(Some(&user(
                serde_json::json!({"allowed_m3u_profile_ids":[3,5]})
            )))
            .unwrap(),
            Some(vec![3, 5])
        );
        assert!(allowed(Some(&user(
            serde_json::json!({"allowed_m3u_profile_ids":[{}]})
        )))
        .is_err());
    }
}
