//! Source direct Redirect HTTP handoff, independent from running live workers.
//! Original Dispatcharr authors retain routing/validation/slot algorithm credit.
use super::*;
use dispatcharr_contracts::PlaybackRedirectPlan;
use dispatcharr_coordination::ProviderReservation;
fn reservation(candidate: &PlaybackCandidate) -> ProviderReservation {
    ProviderReservation {
        profile_id: candidate.provider_profile_id,
        max_streams: candidate.provider_max_streams,
        credential_pool: candidate.credential_pool.as_ref().map(|p| {
            dispatcharr_coordination::CredentialPool {
                counter_key: p.counter_key.clone(),
                max_streams: p.max_streams,
            }
        }),
    }
}
fn shared_single_profile(
    primary: &PlaybackCandidate,
    alternate: &PlaybackCandidate,
) -> Option<i64> {
    primary.provider_profile_id.filter(|id| {
        Some(*id) == alternate.provider_profile_id
            && primary.provider_max_streams == 1
            && alternate.provider_max_streams == 1
            && primary.credential_pool.is_none()
            && alternate.credential_pool.is_none()
    })
}
fn redirect_alternates(
    candidates: &[PlaybackCandidate],
    primary: i64,
) -> impl Iterator<Item = &PlaybackCandidate> {
    // The resolver preserves saved stream order, with each stream's profile
    // variants adjacent. Source starts after the current stream and wraps.
    let pivot = candidates
        .iter()
        .position(|candidate| candidate.stream_id == primary)
        .map_or(0, |index| index + 1);
    let (before, after) = candidates.split_at(pivot);
    after
        .iter()
        .chain(before)
        .filter(move |candidate| candidate.stream_id != primary)
}

fn response(url: &str) -> Result<Response<Body>, PlaybackError> {
    let manual = ["rtsp://", "rtp://", "udp://"]
        .iter()
        .any(|s| url.starts_with(s));
    let safe_http = url.split_once(':').is_some_and(|(scheme, _)| {
        ["http", "https", "ftp"]
            .iter()
            .any(|value| scheme.eq_ignore_ascii_case(value))
    });
    if manual && !url.is_ascii() {
        return Err(PlaybackError::new(
            501,
            "Non-ASCII manual redirect header is unqualified",
        ));
    }
    if !manual && !safe_http {
        return Err(PlaybackError::new(500, "Unsafe redirect scheme"));
    }
    // Django iri_to_uri leaves RFC3986 delimiters and existing%escapes intact.
    let mut location = String::with_capacity(url.len());
    for b in url.bytes() {
        if b.is_ascii_alphanumeric() || b"/#%[]=:;$&()+,?!*'@~-._".contains(&b) {
            location.push(char::from(b))
        } else {
            use std::fmt::Write;
            let _ = write!(location, "%{b:02X}");
        }
    }
    Response::builder()
        .status(if manual { 301 } else { 302 })
        .header("Location", location)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Body::empty())
        .map_err(|_| PlaybackError::new(500, "Redirect header unavailable"))
}
async fn validate(candidate: &PlaybackCandidate) -> bool {
    if ["rtsp://", "rtp://", "udp://"]
        .iter()
        .any(|s| candidate.url.starts_with(s))
    {
        return true;
    }
    let attempt = match crate::upstream::Attempt::new_requests(
        Duration::from_secs(5),
        Duration::from_secs(5),
    ) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::CONNECTION,
        reqwest::header::HeaderValue::from_static("close"),
    );
    if let Ok(head) = attempt
        .request_method(
            reqwest::Method::HEAD,
            &candidate.url,
            &candidate.user_agent,
            &[],
        )
        .map(|request| request.headers(headers.clone()))
    {
        if let Ok(reply) = head.send().await {
            if reply.status().is_success() && !attempt.cookie_budget_exceeded() {
                return true;
            }
        }
    }
    let Ok(get) = attempt
        .request(&candidate.url, &candidate.user_agent, &[])
        .map(|request| request.headers(headers))
    else {
        return false;
    };
    let Ok(reply) = get.send().await else {
        return false;
    };
    if !reply.status().is_success() || attempt.cookie_budget_exceeded() {
        return false;
    }
    // Original probes one Requests iter_content(188 * 10) result. For an
    // unencoded fixed-length response that means a full sample (or the
    // declared shorter body), potentially supplied by several frames.
    const PROBE_BYTES: u64 = 188 * 10;
    let mut remaining = if reply
        .headers()
        .contains_key(reqwest::header::CONTENT_ENCODING)
    {
        1
    } else {
        reply.content_length().unwrap_or(1).clamp(1, PROBE_BYTES) as usize
    };
    let Ok(mut body) = crate::upstream::body(reply) else {
        return false;
    };
    while let Some(frame) = body.next().await {
        match frame {
            Ok(bytes) if bytes.len() >= remaining => return true,
            Ok(bytes) => remaining -= bytes.len(),
            Err(_) => return false,
        }
    }
    false
}
impl StreamEngine {
    pub async fn redirect(
        &self,
        plan: PlaybackRedirectPlan,
    ) -> Result<Response<Body>, PlaybackError> {
        if !plan.capacity_only && plan.assignment.is_none() {
            return Err(PlaybackError::new(
                501,
                "Unrestricted Redirect assignment adoption is not implemented",
            ));
        }
        if plan.candidates.len() > 32 {
            return Err(PlaybackError::new(
                503,
                "Redirect candidate capacity unavailable",
            ));
        }
        // Pair admission/count with the same registry lock observed by shutdown.
        // A canceled HTTP caller detaches this finite owner, not its slot/effects.
        let (permit, worker) = {
            let _registry = self.inner.sessions.lock().unwrap();
            let permit = self
                .inner
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| PlaybackError::new(503, "Channel capacity reached"))?;
            self.inner.workers.fetch_add(1, Ordering::AcqRel);
            (permit, Arc::new(WorkerCount(self.inner.clone())))
        };
        let engine = self.clone();
        tokio::spawn(async move{
            let owner:Arc<dyn Send+Sync>=Arc::new((permit,worker));
            let clients=engine.inner.clients.as_ref().ok_or_else(||PlaybackError::new(501,"Redirect client-policy transport unavailable"))?;
            match clients.check_redirect_retained(&plan.channel_id,&plan.viewer,owner.clone()).await {
                Ok(())=>{},
                Err(dispatcharr_clients::Error::Stopped)=>return Response::builder().status(503).header("Content-Type","application/json").header("Retry-After","1")
                    .body(Body::from(r#"{"error": "Channel is stopping, retry shortly"}"#)).map_err(|_|PlaybackError::new(500,"Stop response unavailable")),
                Err(e)=>return Err(client_error(e,plan.viewer.stream_limit)),
            }
            let coordinator=engine.inner.coordinator.as_ref().ok_or_else(||PlaybackError::new(501,"Redirect provider coordination unavailable"))?;
            let started=Instant::now();
            let (primary,grant)=if plan.capacity_only {
                if plan.candidates.is_empty(){return Err(PlaybackError{status:503,body:serde_json::json!({"error":plan.selection_error.as_deref().unwrap_or("No compatible active profile found for any assigned stream"),"waited":format!("{}s",started.elapsed().as_secs())})})}
                let mut chosen=None;
                for candidate in &plan.candidates {
                    if coordinator.redirect_profile_has_capacity(reservation(candidate),owner.clone()).await.map_err(coordination_error)? {chosen=Some(candidate.clone());break;}
                }
                let Some(primary)=chosen else{return Err(PlaybackError{status:503,body:serde_json::json!({"error":"No compatible active profile found for any assigned stream","waited":format!("{}s",started.elapsed().as_secs())})})};
                (primary,None)
            }else{
                let assignment=plan.assignment.as_ref().ok_or_else(||PlaybackError::new(501,"Redirect assignment resolver unavailable"))?;
                if !assignment.has_assigned_streams {return Err(PlaybackError{status:503,body:serde_json::json!({"error":"No streams assigned to channel","waited":"0s"})});}
                let mut interval=Duration::from_millis(100);
                let mut final_attempt=false;
                loop {
                    // Borrow the actual active OR reserved-before-metadata
                    // assignment even if current SQL account/profile is inactive.
                    if let Some(actual)=coordinator.observe_redirect_assignment(assignment.numeric_channel_id,&plan.channel_id,owner.clone()).await.map_err(coordination_error)? {
                        let selected=assignment.resolver.resolve(actual.stream_id(),actual.profile_id()).await?.ok_or_else(||PlaybackError::new(503,"Assigned stream or profile not found"))?;
                        break (selected,None);
                    }
                    let candidates=assignment.resolver.candidates().await?;
                    if candidates.len()>32{return Err(PlaybackError::new(503,"Redirect candidate capacity unavailable"));}
                    if candidates.is_empty(){return Err(PlaybackError{status:503,body:serde_json::json!({"error":"No active profiles found for any assigned stream","waited":format!("{}s",started.elapsed().as_secs())})});}
                    let mut chosen=None;
                    for candidate in candidates {
                        match coordinator.reserve_redirect_assignment(assignment.numeric_channel_id,&plan.channel_id,candidate.stream_id,reservation(&candidate),owner.clone()).await.map_err(coordination_error)? {
                            dispatcharr_coordination::RedirectClaim::Owned(grant)=>{chosen=Some((candidate,Some(grant)));break;},
                            dispatcharr_coordination::RedirectClaim::Borrowed(actual)=>{
                                let selected=assignment.resolver.resolve(actual.stream_id(),actual.profile_id()).await?.ok_or_else(||PlaybackError::new(503,"Assigned stream or profile not found"))?;
                                chosen=Some((selected,None));break;
                            },
                            dispatcharr_coordination::RedirectClaim::Full=>{},
                        }
                    }
                    if let Some(chosen)=chosen{break chosen;}
                    let remaining=Duration::from_secs(3).saturating_sub(started.elapsed());
                    if remaining<=interval {
                        if !remaining.is_zero() && !final_attempt {final_attempt=true;continue;}
                        return Err(PlaybackError{status:503,body:serde_json::json!({"error":"All active M3U profiles have reached maximum connection limits","waited":format!("{}s",started.elapsed().as_secs())})});
                    }
                    time::sleep(interval).await;interval+=Duration::from_millis(25);
                }
            };
            let result=async{
                if !plan.validate_urls{return response(&primary.url)}
                if validate(&primary).await{return response(&primary.url)}
                let candidates=if let Some(assignment)=plan.assignment.as_ref(){assignment.resolver.candidates().await?}else{plan.candidates};
                let mut tried=std::collections::HashSet::new();tried.insert(primary.stream_id);
                for candidate in redirect_alternates(&candidates, primary.stream_id) {
                    if tried.contains(&candidate.stream_id){continue}
                    // Source exempts a channel's own current profile for its
                    // alternate validation. Require our exact Redirect grant
                    // before using the single-slot exception.
                    let retains_slot = match (grant.as_ref(), shared_single_profile(&primary, candidate)) {
                        (Some(grant), Some(profile)) => grant.holds_single_slot(profile).await.map_err(coordination_error)?,
                        _ => false,
                    };
                    if !retains_slot && !coordinator.redirect_profile_has_capacity(reservation(candidate),owner.clone()).await.map_err(coordination_error)?{continue}
                    tried.insert(candidate.stream_id);
                    if validate(candidate).await{return response(&candidate.url)}
                }
                Err(PlaybackError::new(502,"All available streams failed validation"))
            }.await;
            if let Some(grant)=grant {let _=grant.release().await.map_err(coordination_error)?;}
            // Explicit allowed-profile redirects never reserve or assign a slot.
            result
        }).await.map_err(|_|PlaybackError::new(503,"Redirect worker unavailable"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    fn candidate(url: String) -> PlaybackCandidate {
        PlaybackCandidate {
            input_command: None,
            stream_id: 1,
            provider_profile_id: Some(1),
            provider_max_streams: 0,
            credential_pool: None,
            url,
            user_agent: "Dispatcharr-redirect-peer".into(),
            headers: Vec::new(),
            metadata: Vec::new(),
        }
    }
    #[test]
    fn redirect_validation_starts_after_resolved_stream_then_wraps_saved_order() {
        let values = [(41, 1), (15, 2), (15, 3), (72, 4), (72, 5)].map(|(id, profile)| {
            let mut candidate = candidate(format!("http://owned-origin.test/{id}/{profile}"));
            candidate.stream_id = id;
            candidate.provider_profile_id = Some(profile);
            candidate
        });
        let collect = |primary| {
            redirect_alternates(&values, primary)
                .map(|candidate| (candidate.stream_id, candidate.provider_profile_id.unwrap()))
                .collect::<Vec<_>>()
        };
        assert_eq!(collect(15), [(72, 4), (72, 5), (41, 1)]);
        assert_eq!(collect(72), [(41, 1), (15, 2), (15, 3)]);
        assert_eq!(collect(41), [(15, 2), (15, 3), (72, 4), (72, 5)]);
        assert_eq!(collect(92), [(41, 1), (15, 2), (15, 3), (72, 4), (72, 5)]);
    }

    #[test]
    fn redirect_alternate_reuses_only_its_same_unshared_single_profile() {
        let mut primary = candidate("http://origin.test/primary".to_string());
        primary.provider_max_streams = 1;
        let mut alternate = candidate("http://origin.test/alternate".to_string());
        alternate.stream_id = 2;
        alternate.provider_max_streams = 1;
        assert_eq!(shared_single_profile(&primary, &alternate), Some(1));
        alternate.provider_profile_id = Some(2);
        assert_eq!(shared_single_profile(&primary, &alternate), None);
        alternate.provider_profile_id = Some(1);
        alternate.provider_max_streams = 2;
        assert_eq!(shared_single_profile(&primary, &alternate), None);
        alternate.provider_max_streams = 1;
        alternate.credential_pool = Some(dispatcharr_contracts::PlaybackCredentialPool {
            counter_key: "server_group_connections:owned".to_owned(),
            max_streams: 1,
        });
        assert_eq!(shared_single_profile(&primary, &alternate), None);
    }

    #[test]
    fn handoff_status_and_original_iri_are_source_responses_not_upstream_locations() {
        let value = response("https://provider.test/é?q=with space").unwrap();
        assert_eq!(value.status(), 302);
        assert_eq!(
            value.headers()["location"],
            "https://provider.test/%C3%A9?q=with%20space"
        );
        assert_eq!(response("rtsp://provider.test/live").unwrap().status(), 301);
        assert_eq!(response("javascript:alert(1)").unwrap_err().status, 500);
    }
    async fn headers(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut b = [0u8; 1];
        loop {
            socket.read_exact(&mut b).await.unwrap();
            bytes.push(b[0]);
            assert!(bytes.len() <= 8192);
            if bytes.ends_with(b"\r\n\r\n") {
                return String::from_utf8(bytes).unwrap();
            }
        }
    }
    #[tokio::test]
    async fn rejected_head_falls_back_to_same_cookie_session_get_and_accepts_data_not_content_type()
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let h = headers(&mut socket).await;
            assert!(h.starts_with("HEAD /profile HTTP/1.1\r\n"));
            assert!(h.to_ascii_lowercase().contains("connection: close\r\n"));
            socket.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nSet-Cookie: redirect_peer=owned; Path=/\r\nConnection: close\r\n\r\n").await.unwrap();
            drop(socket);
            let (mut socket, _) = listener.accept().await.unwrap();
            let h = headers(&mut socket).await;
            assert!(h.starts_with("GET /profile HTTP/1.1\r\n"));
            assert!(h
                .to_ascii_lowercase()
                .contains("cookie: redirect_peer=owned\r\n"));
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx").await.unwrap();
        });
        assert!(time::timeout(
            Duration::from_secs(5),
            validate(&candidate(format!("http://{address}/profile")))
        )
        .await
        .unwrap());
        time::timeout(Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn fixed_length_get_probe_waits_for_the_requested_sample() {
        for (declared, remaining, accepted) in [
            (1881, 1880 - 32, true),
            (1881, 0, false),
            (33, 1, true),
            (33, 0, false),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (first_sent, first) = tokio::sync::oneshot::channel();
            let (finish, wait_for_finish) = tokio::sync::oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert!(headers(&mut socket).await.starts_with("HEAD /profile "));
                socket.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                drop(socket);
                let (mut socket, _) = listener.accept().await.unwrap();
                assert!(headers(&mut socket).await.starts_with("GET /profile "));
                socket
                    .write_all(
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n")
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
                socket.write_all(&[b'x'; 32]).await.unwrap();
                first_sent.send(()).unwrap();
                wait_for_finish.await.unwrap();
                socket.write_all(&vec![b'x'; remaining]).await.unwrap();
            });
            let mut probe = tokio::spawn(async move {
                validate(&candidate(format!("http://{address}/profile"))).await
            });
            time::timeout(Duration::from_secs(2), first)
                .await
                .unwrap()
                .unwrap();
            let early = time::timeout(Duration::from_millis(150), &mut probe).await;
            finish.send(()).unwrap();
            assert!(
                early.is_err(),
                "Content-Length {declared} needs more than 32 bytes"
            );
            let observed = time::timeout(Duration::from_secs(2), probe)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                observed, accepted,
                "Content-Length {declared}, extra {remaining} bytes"
            );
            time::timeout(Duration::from_secs(2), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }
    #[tokio::test]
    async fn successful_head_redirect_probe_preserves_requested_profile_url_for_external_client() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert!(headers(&mut socket).await.starts_with("HEAD /profile "));
            socket.write_all(b"HTTP/1.1 302 Found\r\nLocation: /short-lived-cdn\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            drop(socket);
            let (mut socket, _) = listener.accept().await.unwrap();
            assert!(headers(&mut socket)
                .await
                .starts_with("HEAD /short-lived-cdn "));
            socket
                .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let original = format!("http://{address}/profile");
        let plan = candidate(original.clone());
        assert!(time::timeout(Duration::from_secs(5), validate(&plan))
            .await
            .unwrap());
        assert_eq!(response(&plan.url).unwrap().headers()["location"], original);
        time::timeout(Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    #[ignore = "requires owned disposable real Redis"]
    async fn allowed_redirect_never_registers_client_or_releases_a_foreign_provider_grant() {
        use dispatcharr_clients::{ClientConfig, ClientStore};
        use dispatcharr_contracts::{PlaybackViewer, UserLimitPolicy};
        use dispatcharr_coordination::{AcquireRequest, Coordinator, LeaseConfig};
        let url = std::env::var("REDIS_TEST_URL").expect("owned disposable Redis required");
        let channel = uuid::Uuid::new_v4().to_string();
        let profile = (uuid::Uuid::new_v4().as_u128() & 0x3fff_ffff) as i64 + 1;
        let coordinator = Coordinator::connect(
            &url,
            LeaseConfig {
                namespace: format!("redirect-peer-{}", uuid::Uuid::new_v4()),
                heartbeat_interval: None,
                reap_interval: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let clients = ClientStore::connect(&url, ClientConfig::default())
            .await
            .unwrap();
        let engine = StreamEngine::new_with_services(
            EngineConfig::default(),
            Some(coordinator.clone()),
            Some(clients.clone()),
        )
        .unwrap();
        let foreign = coordinator
            .acquire_vod(AcquireRequest {
                channel_id: format!("foreign-{}", uuid::Uuid::new_v4()),
                provider_profile_id: Some(profile),
                provider_max_streams: 1,
                credential_pool: None,
            })
            .await
            .unwrap();
        let make_plan = || {
            let mut item = candidate("https://provider.invalid/stream".into());
            item.provider_profile_id = Some(profile);
            item.provider_max_streams = 1;
            PlaybackRedirectPlan {
                channel_id: channel.clone(),
                candidates: vec![item],
                viewer: PlaybackViewer {
                    user_id: None,
                    username: None,
                    ip_address: "127.0.0.1".parse().unwrap(),
                    user_agent: "redirect-peer".into(),
                    stream_limit: 0,
                    limit_policy: UserLimitPolicy::default(),
                },
                capacity_only: true,
                assignment: None,
                validate_urls: false,
                selection_error: None,
            }
        };
        let result = engine.redirect(make_plan()).await;
        assert_eq!(result.err().unwrap().status, 503);
        let mut redis = redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key = format!("profile_connections:{profile}");
        assert_eq!(
            redis::cmd("GET")
                .arg(&key)
                .query_async::<i64>(&mut redis)
                .await
                .unwrap(),
            1
        );
        foreign.release().await.unwrap();
        foreign.settlement().wait().await;
        let response = engine.redirect(make_plan()).await.unwrap();
        assert_eq!(response.status(), 302);
        assert_eq!(
            redis::cmd("GET")
                .arg(&key)
                .query_async::<i64>(&mut redis)
                .await
                .unwrap(),
            0
        );
        assert!(!redis::cmd("EXISTS")
            .arg(format!("live:channel:{channel}:owner"))
            .arg(format!("live:channel:{channel}:clients"))
            .query_async::<bool>(&mut redis)
            .await
            .unwrap());
        assert!(engine.shutdown_wait(Duration::from_secs(3)).await);
        clients.shutdown().await;
        let _: () = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut redis)
            .await
            .unwrap();
    }
}
