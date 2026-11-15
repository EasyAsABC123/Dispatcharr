//! Source direct Redirect-mode response selection, before live-worker reuse.
//! Original Dispatcharr authors retain algorithm/application credit.
use crate::playback::playback_error;
use crate::{AuthService, User};
use dispatcharr_contracts::{
    PlaybackCandidate, PlaybackError, PlaybackRedirectPlan, PlaybackRequest, PlaybackResolution,
    PlaybackViewer, UserLimitPolicy,
};
use serde_json::Value;
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
        let channel = self
            .store
            .resolve_channel_mode(&request.channel_id, true, user.as_ref())
            .await
            .map_err(playback_error)?
            .ok_or_else(|| PlaybackError::new(404, "Not found"))?;
        if !channel.redirect {
            return self
                .playback_source_with_channel(request, user, None, channel)
                .await
                .map(|v| PlaybackResolution::Stream(Box::new(v)));
        }
        let capacity_only = allowed(user.as_ref())?.is_some();
        if request
            .parameter("skip_redirect")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            && user.as_ref().is_some_and(|u| u.user_level >= 1)
        {
            return Err(PlaybackError::new(
                501,
                "Profile-scoped Redirect preview identity is not implemented",
            ));
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
