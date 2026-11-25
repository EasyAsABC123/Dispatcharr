//! Ordinary signed VOD aliases retain the prior generation's public ownership.
//! Original Dispatcharr authors retain credit for the underlying behavior.
use super::{
    admission_error, key, millis, now, provider_full, redis_error, same_provider, unavailable,
};
use super::{Session, SessionPool, Ticket, VodCandidate, VodRequest, VodSource};
use crate::resolver::{ContentKind, VodResolver};
use dispatcharr_clients::vod::SignedIdleAlias;
use dispatcharr_coordination::ProviderReservation;
use dispatcharr_store::{StoreError, User};
use redis::Script;
use serde_json::{json, Value};
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};
use tokio::sync::OwnedSemaphorePermit;

pub(crate) fn signed_alias_claim(request: &VodRequest, subject: i64) -> Option<String> {
    if subject <= 0
        || request.kind != "movie"
        || request.profile_id.is_some()
        || !request.playback.query.is_empty()
    {
        return None;
    }
    let uuid = uuid::Uuid::parse_str(&request.content_id).ok()?;
    (uuid.to_string() == request.content_id).then(|| format!("movie:{uuid}:{subject}"))
}

fn claim_key(id: &str) -> String {
    format!("vod_signed_idle_alias:{id}")
}

fn allowed(user: &User, candidate: &VodCandidate) -> bool {
    user.is_active
        && user.custom_properties.get("vod_movies_enabled") != Some(&Value::Bool(false))
        && user
            .custom_properties
            .get("allowed_m3u_profile_ids")
            .is_none_or(|allowed| {
                allowed.is_null()
                    || allowed.as_array().is_some_and(|entries| {
                        entries
                            .iter()
                            .map(|id| id.as_i64().or_else(|| id.as_str()?.parse().ok()))
                            .collect::<Option<Vec<_>>>()
                            .is_some_and(|ids| ids.contains(&candidate.profile_id))
                    })
            })
}

fn matches_request(
    session: &Session,
    source: &VodSource,
    request: &VodRequest,
    requester: &User,
) -> bool {
    session.signed_alias_owner
        && session.scope.get().is_none()
        && !session.authenticated_restart
        && !*session.stop.borrow()
        && session.source.kind == source.kind
        && source.kind == ContentKind::Movie
        && session.source.content_uuid == source.content_uuid
        && source.content_uuid == request.content_id
        && session
            .source
            .user
            .as_ref()
            .is_some_and(|prior| prior.id != requester.id)
        && session.client_ip == request.client_ip_text()
        && !session.user_agent.is_empty()
        && request.playback.header("user-agent") == Some(&session.user_agent)
        && session.timeshift.borrowed() == crate::offset::requested(&request.playback)
        && source
            .candidates
            .iter()
            .any(|current| same_provider(current, &session.candidate))
        && allowed(requester, &session.candidate)
}

impl SessionPool {
    pub(crate) async fn remember_signed_alias(
        &self,
        id: &str,
        user: i64,
        claim: &str,
    ) -> Result<(), StoreError> {
        if self.stopped() {
            return Err(unavailable());
        }
        let created: i64 = Script::new(
            r#"
if redis.call('EXISTS',KEYS[1],KEYS[2],KEYS[3],KEYS[4])~=0 then return 0 end
redis.call('SET',KEYS[1],ARGV[1],'EX',300)
redis.call('SET',KEYS[2],ARGV[2],'EX',300)
return 1
"#,
        )
        .key(format!("vod_session_user:{id}"))
        .key(claim_key(id))
        .key(key(id))
        .key(format!("vod_proxy:client:{id}:stop"))
        .arg(user)
        .arg(claim)
        .invoke_async(&mut self.inner.connection.clone())
        .await
        .map_err(|_| redis_error())?;
        if created != 1 {
            return Err(unavailable());
        }
        Ok(())
    }

    pub(super) async fn enter_signed_idle_alias(
        &self,
        id: &str,
        claim: &str,
        source: &VodSource,
        request: &VodRequest,
        resolver: &dyn VodResolver,
        permit: &Arc<OwnedSemaphorePermit>,
    ) -> Result<Option<Ticket>, StoreError> {
        let Some(requester) = &source.user else {
            return Ok(None);
        };
        // Original's verified case is an independently authorized requester
        // with an available slot and policy that denies at limit.
        if requester.stream_limit <= 0
            || source.redirect
            || source.limit_policy.terminate_on_limit_exceeded
            || source.limit_policy.ignore_same_channel_connections
            || request.playback.header("range").is_none_or(str::is_empty)
        {
            return Ok(None);
        }
        let followed = {
            let registry = self.inner.sessions.lock().await;
            if self.stopped() || registry.contains_key(id) {
                return Ok(None);
            }
            let mut eligible = None;
            for previous in registry
                .values()
                .filter(|previous| matches_request(previous, source, request, requester))
            {
                let Ok(state) = previous.state.try_lock() else {
                    return Ok(None);
                };
                if state.active == 0 && !state.reserved && !state.closed {
                    if eligible.is_some() {
                        return Ok(None);
                    }
                    eligible = Some(previous.clone());
                }
            }
            eligible
        };
        let Some(previous) = followed else {
            return Ok(None);
        };
        let mut connection = self.inner.connection.clone();
        let prior_id = previous
            .source
            .user
            .as_ref()
            .expect("qualified prior owner")
            .id;
        let Some(prior) = resolver.restore_user(prior_id).await? else {
            return Ok(None);
        };
        if prior.id != prior_id || !allowed(&prior, &previous.candidate) {
            return Ok(None);
        }
        let mut state = previous.state.lock().await;
        {
            let registry = self.inner.sessions.lock().await;
            if self.stopped()
                || *previous.stop.borrow()
                || registry.contains_key(id)
                || !registry
                    .get(&previous.id)
                    .is_some_and(|current| Arc::ptr_eq(current, &previous))
                || state.active != 0
                || state.closed
                || state.reserved
            {
                return Ok(None);
            }
            if registry.values().any(|other| {
                !Arc::ptr_eq(other, &previous)
                    && matches_request(other, source, request, requester)
                    && other.state.try_lock().map_or(true, |state| {
                        state.active == 0 && !state.reserved && !state.closed
                    })
            }) {
                return Ok(None);
            }
        }
        if request.playback.header("range").is_some_and(|range| {
            crate::engine::range_is_unsatisfiable(
                range,
                previous.content_length.load(Ordering::Relaxed),
            )
        }) {
            return Err(StoreError::StatusBody(
                416,
                json!(crate::engine::RANGE_NOT_SATISFIABLE),
            ));
        }
        let candidate = &previous.candidate;
        let reserved = previous
            .lease
            .reserve_provider_retained_result(ProviderReservation {
                profile_id: Some(candidate.profile_id),
                max_streams: candidate.max_streams,
                credential_pool: candidate.credential_pool.clone(),
            })
            .await;
        if let Err(error) = reserved {
            if error == dispatcharr_coordination::Error::RedisUnavailable {
                let released = previous.lease.release_provider().await;
                previous.lease.settlement().wait().await;
                if released.is_err() {
                    state.closed = true;
                    previous.stop.send_replace(true);
                    self.inner.cleanup_failed.store(true, Ordering::Release);
                }
            }
            return Err(match error {
                dispatcharr_coordination::Error::ProfileFull
                | dispatcharr_coordination::Error::CredentialFull => provider_full(),
                _ => unavailable(),
            });
        }
        state.reserved = true;
        let observed_at = now();
        let admission = SignedIdleAlias {
            requested: id,
            prior: &previous.id,
            claim,
            requester: requester.id,
            prior_viewer: prior_id,
            requester_limit: requester.stream_limit as u32,
            content: &source.content_uuid,
            profile: candidate.profile_id,
            metadata_token: &previous.metadata_token,
            lease_key: previous.lease.owner_key(),
            lease_token: previous.lease.token(),
            now: &observed_at,
        };
        let entered =
            dispatcharr_clients::vod::enter_signed_idle_alias(&mut connection, &admission).await;
        if !matches!(entered, Ok(true)) {
            // A physical reply can be lost after Redis committed ENTER. Undo
            // only the exact private alias marker accepted by our token and
            // leave another generation untouched. No fallback for ambiguity.
            if matches!(entered, Err(dispatcharr_clients::Error::RedisUnavailable)) {
                let rollback = Script::new(ROLLBACK)
                    .key(key(&previous.id))
                    .arg(&previous.metadata_token)
                    .arg(id)
                    .invoke_async::<i64>(&mut connection)
                    .await;
                if !matches!(rollback, Ok(0 | 1)) {
                    state.closed = true;
                    previous.stop.send_replace(true);
                    self.inner.cleanup_failed.store(true, Ordering::Release);
                }
            }
            let released = previous.lease.release_provider().await;
            previous.lease.settlement().wait().await;
            state.reserved = false;
            if released.is_err() {
                state.closed = true;
                previous.stop.send_replace(true);
                self.inner.cleanup_failed.store(true, Ordering::Release);
                return Err(unavailable());
            }
            return entered
                .map(|_| None)
                .map_err(|error| admission_error(error, requester.stream_limit));
        }
        state.active = 1;
        state.epoch += 1;
        state.departing_pull = None;
        previous.last_request.store(millis(), Ordering::Relaxed);
        drop(state);
        Ok(Some(Ticket {
            session: previous,
            pool: Arc::downgrade(&self.inner),
            _request: Some(permit.clone()),
            is_new: false,
            body_started: AtomicBool::new(false),
            scope_body: None,
            cleanup_needed: true,
        }))
    }
}

const ROLLBACK: &str = r#"
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1]
 or redis.call('HGET',KEYS[1],'rust_signed_idle_alias')~=ARGV[2] then return 0 end
local requests=redis.call('HGET',KEYS[1],'request_count')
local previous_activity=redis.call('HGET',KEYS[1],'rust_signed_idle_alias_previous_activity')
if not previous_activity or redis.call('HGET',KEYS[1],'active_streams')~='1'
 or not requests or not string.match(requests,'^[1-9]%d*$') or #requests>16 or tonumber(requests)<2 then return -1 end
redis.call('HINCRBY',KEYS[1],'request_count',-1)
redis.call('HSET',KEYS[1],'active_streams','0','last_activity',previous_activity)
redis.call('HDEL',KEYS[1],'rust_signed_idle_alias','rust_signed_idle_alias_previous_activity')
return 1
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use dispatcharr_contracts::PlaybackRequest;
    use serde_json::json;

    fn request() -> VodRequest {
        VodRequest {
            resolved_client_ip: None,
            playback: PlaybackRequest {
                channel_id: String::new(),
                query: vec![],
                headers: vec![],
                peer_ip: std::net::Ipv4Addr::LOCALHOST.into(),
            },
            kind: "movie".into(),
            content_id: "8a9a28fc-426e-4d59-bb1f-5ce199c236e0".into(),
            session_id: None,
            profile_id: None,
        }
    }

    #[test]
    fn claim_binds_both_the_authenticated_subject_and_exact_canonical_movie() {
        let original = request();
        let owner = signed_alias_claim(&original, 7).expect("canonical signed movie");
        let mut follow = original.clone();
        follow.session_id = Some("vod_example".into());
        follow
            .playback
            .headers
            .push(("range".into(), "bytes=-256".into()));
        assert_eq!(
            signed_alias_claim(&follow, 7).as_deref(),
            Some(owner.as_str())
        );
        assert_ne!(signed_alias_claim(&follow, 9), Some(owner.clone()));
        follow.content_id = "1d027cdb-77a7-48ec-abef-a66eee13be32".into();
        assert_ne!(signed_alias_claim(&follow, 7), Some(owner));
        for disallowed in [
            VodRequest {
                profile_id: Some(17),
                ..original.clone()
            },
            VodRequest {
                kind: "series".into(),
                ..original.clone()
            },
            VodRequest {
                content_id: original.content_id.to_uppercase(),
                ..original.clone()
            },
        ] {
            assert_eq!(signed_alias_claim(&disallowed, 7), None);
        }
        follow = original;
        follow.playback.query.push(("offset".into(), "60".into()));
        assert_eq!(signed_alias_claim(&follow, 7), None);
        assert_eq!(signed_alias_claim(&request(), 0), None);
    }

    #[test]
    fn both_actors_must_be_active_and_authorized_for_the_retained_profile() {
        let candidate = VodCandidate {
            relation_id: 1,
            account_id: 2,
            profile_id: 11,
            max_streams: 1,
            credential_pool: None,
            url: "http://fixture.invalid/media".into(),
            user_agent: "Fixture".into(),
        };
        let prior = User {
            id: 7,
            username: "prior".into(),
            password: "!".into(),
            is_active: true,
            user_level: 0,
            stream_limit: 1,
            custom_properties: json!({"allowed_m3u_profile_ids":[11]}),
        };
        let mut requester = prior.clone();
        requester.id = 9;
        let permits_both = |requester: &User, prior: &User| {
            allowed(requester, &candidate) && allowed(prior, &candidate)
        };
        assert!(permits_both(&requester, &prior));
        requester.custom_properties = json!({"allowed_m3u_profile_ids":[12]});
        assert!(!permits_both(&requester, &prior));
        requester.custom_properties = json!({"allowed_m3u_profile_ids":[11,"invalid"]});
        assert!(!permits_both(&requester, &prior));
        requester.custom_properties = json!({"vod_movies_enabled":false});
        assert!(!permits_both(&requester, &prior));
        requester.custom_properties = json!({"allowed_m3u_profile_ids":["11"]});
        assert!(permits_both(&requester, &prior));
        assert!(!permits_both(
            &requester,
            &User {
                is_active: false,
                ..prior
            }
        ));
    }
}
