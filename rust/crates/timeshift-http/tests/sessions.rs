//! Actual Redis + loopback HTTP session lifecycle, no fake Redis responses.
use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_store::{ChannelIdentity, StoreError, User};
use dispatcharr_timeshift_http::*;
use redis::AsyncCommands;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{net::TcpListener, task::JoinHandle};
struct Resolver {
    uuid: uuid::Uuid,
    visible: AtomicBool,
    active: AtomicBool,
}
fn user(id: i64) -> User {
    User {
        id,
        username: format!("fixture-{id}"),
        password: "!".into(),
        is_active: true,
        user_level: 1,
        stream_limit: 0,
        custom_properties: json!({}),
    }
}
impl Resolver {
    fn identity(&self) -> ChannelIdentity {
        ChannelIdentity {
            id: 42,
            uuid: self.uuid,
            name: "Fixture".into(),
            effective_name: "Fixture".into(),
        }
    }
}
impl SessionResolver for Resolver {
    fn authenticate<'a>(
        &'a self,
        request: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<User, StoreError>> {
        Box::pin(async move {
            match request.header("authorization") {
                Some("Bearer owner") => Ok(user(1)),
                Some("Bearer other") => Ok(user(2)),
                Some("Bearer disabled") => {
                    let mut user = user(3);
                    user.custom_properties = json!({"catchup_enabled":false});
                    Ok(user)
                }
                Some("Bearer viewer") => Err(StoreError::PermissionDenied),
                _ => Err(StoreError::Unauthorized(
                    "Authentication credentials were not provided.",
                )),
            }
        })
    }
    fn enabled<'a>(&'a self, user: &'a User) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            Ok(user.custom_properties.get("catchup_enabled") != Some(&Value::Bool(false)))
        })
    }
    fn channel<'a>(
        &'a self,
        _: &'a User,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'a, Result<Option<SessionChannel>, StoreError>> {
        Box::pin(async move {
            if uuid.to_string() == "fe15cb90-3bcf-4ba8-aef2-69216c97cb90" {
                let mut identity = self.identity();
                identity.uuid = uuid;
                identity.id = 43;
                return Ok(Some(SessionChannel {
                    identity,
                    is_catchup: false,
                    has_catchup_streams: false,
                }));
            }
            Ok(
                (self.visible.load(Ordering::Relaxed) && uuid == self.uuid).then(|| {
                    SessionChannel {
                        identity: self.identity(),
                        is_catchup: true,
                        has_catchup_streams: true,
                    }
                }),
            )
        })
    }
    fn bound_channel(
        &self,
        id: i64,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'_, Result<Option<ChannelIdentity>, StoreError>> {
        Box::pin(async move { Ok((id == 42 && uuid == self.uuid).then(|| self.identity())) })
    }
    fn user(&self, id: i64) -> BoxFuture<'_, Result<Option<User>, StoreError>> {
        Box::pin(async move { Ok(self.active.load(Ordering::Relaxed).then(|| user(id))) })
    }
}
async fn fixture() -> (
    CatchupSessions,
    Arc<Resolver>,
    String,
    JoinHandle<()>,
    redis::aio::MultiplexedConnection,
) {
    let url = std::env::var("REDIS_TEST_URL").expect("disposable Redis required");
    let resolver = Arc::new(Resolver {
        uuid: uuid::Uuid::parse_str("0282858c-3869-471b-a58b-e913c1b6bd33").unwrap(),
        visible: AtomicBool::new(true),
        active: AtomicBool::new(true),
    });
    let service = CatchupSessions::with_resolver(resolver.clone(), &url, SessionConfig::default())
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = service.clone().router();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    (service, resolver, format!("http://{address}"), task, redis)
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn session_api_validation_order_schema_ownership_and_revoke() {
    let (service, resolver, base, task, mut redis) = fixture().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/catchup/sessions/");
    assert_eq!(
        client
            .post(&url)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("viewer")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let response = client
        .post(&url)
        .bearer_auth("disabled")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"error":"Catch-up is disabled"})
    );
    let response = client
        .post(&url)
        .bearer_auth("owner")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"channel_uuid":["This field is required."],"start":["This field is required."]})
    );
    let response = client
        .post(&url)
        .bearer_auth("owner")
        .json(&json!({"channel_uuid":resolver.uuid,"start":" 2026-11-09T11:00:00Z ","duration":30}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let payload = response.json::<Value>().await.unwrap();
    let id = payload["session_id"].as_str().unwrap();
    assert_eq!(id.len(), 22);
    assert_eq!(payload["duration"], 30);
    assert_eq!(payload["start"], "2026-11-09T11:00:00Z");
    assert_eq!(
        payload["playback_url"],
        format!("/proxy/catchup/{}?session_id={id}", resolver.uuid)
    );
    let key = format!("timeshift:session:{id}");
    let record: std::collections::HashMap<String, String> = redis.hgetall(&key).await.unwrap();
    assert_eq!(record["user_id"], "1");
    assert_eq!(record["channel_id"], "42");
    assert_eq!(record["duration"], "30");
    assert!((59..=60).contains(&redis.ttl::<_, i64>(&key).await.unwrap()));
    let revoke = format!("{url}{id}/");
    assert_eq!(
        client
            .delete(&revoke)
            .bearer_auth("other")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert!(service.owns(id, 1).await);
    assert_eq!(
        client
            .delete(&revoke)
            .bearer_auth("owner")
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(!service.owns(id, 1).await);
    assert_eq!(
        client
            .delete(&revoke)
            .bearer_auth("owner")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    task.abort();
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn bound_session_keeps_mint_visibility_checks_active_user_and_updates_position_ttls() {
    let (service, resolver, base, task, mut redis) = fixture().await;
    let session = service
        .create(&user(1), resolver.uuid, "2026-11-09T11:00:00Z".into(), None)
        .await
        .unwrap();
    let id = &session.session_id;
    let client = reqwest::Client::new();
    let position = format!("{base}/api/catchup/sessions/{id}/position/");
    assert_eq!(
        client
            .post(&position)
            .bearer_auth("owner")
            .json(&json!({"position_secs":3}))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let stats_channel = format!("42_{id}");
    let stats = format!("timeshift:channel:{stats_channel}:clients:{id}");
    let set = format!("timeshift:channel:{stats_channel}:clients");
    let metadata = format!("timeshift:channel:{stats_channel}:metadata");
    let _: () = redis.hset(&stats, "user_id", 1).await.unwrap();
    let _: () = redis.sadd(&set, id).await.unwrap();
    let _: () = redis.hset(&metadata, "name", "Fixture").await.unwrap();
    assert_eq!(
        client
            .post(&position)
            .bearer_auth("owner")
            .json(&json!({"position_secs":72.5,"paused":true}))
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        redis
            .hget::<_, _, String>(&stats, "playback_base_secs")
            .await
            .unwrap(),
        "72.5"
    );
    assert_eq!(
        redis.hget::<_, _, String>(&stats, "paused").await.unwrap(),
        "1"
    );
    assert!((599..=600).contains(
        &redis
            .ttl::<_, i64>(format!("timeshift:session:{id}"))
            .await
            .unwrap()
    ));
    assert!((59..=60).contains(&redis.ttl::<_, i64>(&stats).await.unwrap()));
    assert_eq!(
        client
            .post(&position)
            .bearer_auth("other")
            .json(&json!({"position_secs":0}))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client
            .post(&position)
            .bearer_auth("owner")
            .json(&json!({"position_secs":4,"paused":false}))
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(!redis.hexists::<_, _, bool>(&stats, "paused").await.unwrap());
    resolver.visible.store(false, Ordering::Relaxed);
    assert!(service.resolve(id, resolver.uuid).await.unwrap().is_some());
    let _: () = redis
        .expire(format!("timeshift:session:{id}"), 5)
        .await
        .unwrap();
    resolver.active.store(false, Ordering::Relaxed);
    assert!(service.resolve(id, resolver.uuid).await.unwrap().is_none());
    assert!(
        redis
            .ttl::<_, i64>(format!("timeshift:session:{id}"))
            .await
            .unwrap()
            <= 5
    );
    let _: () = redis
        .del(&[format!("timeshift:session:{id}"), stats, set, metadata])
        .await
        .unwrap();
    task.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn twenty_two_native_api_wire_cases_match_original_python_capture() {
    let (_, resolver, base, task, _redis) = fixture().await;
    let client = reqwest::Client::new();
    let captured: Value = serde_json::from_str(include_str!(
        "../../../../scripts/rust-timeshift-contracts/python-session-goldens.json"
    ))
    .unwrap();
    let mut session = String::new();
    let mut count = 0;
    for event in captured["events"].as_array().unwrap().iter().take(22) {
        let case = event["case"].as_str().unwrap();
        let mut data = json!({"channel_uuid":resolver.uuid.to_string(),"start":"2026-11-09T11:00:00Z","duration":30});
        let mut token = Some("owner");
        let mut route = "/api/catchup/sessions/".to_string();
        let mut method = reqwest::Method::POST;
        match case {
            "missing-auth" => token = None,
            "viewer-denied" => token = Some("viewer"),
            "catchup-disabled-before-validation" => {
                token = Some("disabled");
                data = json!({});
            }
            "required-fields" => data = json!({}),
            "invalid-uuid" => data["channel_uuid"] = json!("bad"),
            "blank-start" => data["start"] = json!(" "),
            "invalid-start" => data["start"] = json!("bad"),
            "duration-zero" => data["duration"] = json!(0),
            "duration-over-cap" => data["duration"] = json!(481),
            "duration-fraction" => data["duration"] = json!("3.1"),
            "duration-null" => data["duration"] = Value::Null,
            "unsupported-channel" => {
                data["channel_uuid"] = json!("fe15cb90-3bcf-4ba8-aef2-69216c97cb90")
            }
            "missing-channel" => data["channel_uuid"] = json!(uuid::Uuid::from_u128(1).to_string()),
            "create" => {}
            name if name.starts_with("position-") => {
                route = format!("/api/catchup/sessions/{session}/position/");
                data = match name {
                    "position-without-active-playback" => json!({"position_secs":3}),
                    "position-missing-field" => json!({}),
                    "position-negative" => json!({"position_secs":-1}),
                    "position-over-cap" => json!({"position_secs":28801}),
                    "position-invalid-paused" => json!({"position_secs":1,"paused":"bad"}),
                    _ => panic!("unexpected position case"),
                };
            }
            "other-owner-hidden" | "revoke" | "revoked-session-hidden" => {
                route = format!("/api/catchup/sessions/{session}/");
                method = reqwest::Method::DELETE;
                if case == "other-owner-hidden" {
                    token = Some("other");
                }
            }
            _ => panic!("unexpected captured source case"),
        }
        let mut request = client.request(method.clone(), format!("{base}{route}"));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if method == reqwest::Method::POST {
            request = request.json(&data);
        }
        let response = request.send().await.unwrap();
        assert_eq!(
            response.status().as_u16(),
            event["status"].as_u64().unwrap() as u16,
            "case {case}"
        );
        let mut allow = response
            .headers()
            .get("allow")
            .unwrap()
            .to_str()
            .unwrap()
            .split(',')
            .map(str::trim)
            .collect::<Vec<_>>();
        allow.sort();
        let mut original_allow = event["allow"]
            .as_str()
            .unwrap()
            .split(',')
            .map(str::trim)
            .collect::<Vec<_>>();
        original_allow.sort();
        assert_eq!(allow, original_allow, "case {case}");
        let raw = response.bytes().await.unwrap();
        let mut body: Value = if raw.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&raw).unwrap()
        };
        if case == "create" {
            session = body["session_id"].as_str().unwrap().into();
            body["session_id"] = json!("<session>");
            body["playback_url"] = json!(format!(
                "/proxy/catchup/{}?session_id=<session>",
                resolver.uuid
            ));
            body["expires_at"] = json!("<handshake-deadline>");
        }
        assert_eq!(body, event["body"], "case {case}");
        count += 1;
    }
    assert_eq!(count, 22);
    task.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn missing_socket_peer_metadata_does_not_become_a_trusted_loopback_client() {
    let (service, resolver, _base, original, _redis) = fixture().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = service.router();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    assert_eq!(
        reqwest::Client::new()
            .post(format!("http://{address}/api/catchup/sessions/"))
            .bearer_auth("owner")
            .json(&json!({"channel_uuid":resolver.uuid,"start":"2026-11-09T11:00:00Z"}))
            .send()
            .await
            .unwrap()
            .status(),
        501
    );
    server.abort();
    original.abort();
}
