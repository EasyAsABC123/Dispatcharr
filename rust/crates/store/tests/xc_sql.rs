//! Original-source XC auth and side-effect contracts on disposable Django SQL.
//! Original Dispatcharr authors retain all credit; see ATTRIBUTION.md.
use axum::{body::to_bytes, response::IntoResponse};
use dispatcharr_contracts::{BoxFuture, PlaybackRequest, UpdateEventError, UpdateEventSink};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use redis::AsyncCommands;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
#[derive(Default)]
struct Sink(Mutex<Vec<Value>>);
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            assert_eq!(group, "updates");
            self.0.lock().unwrap().push(message);
            Ok(())
        })
    }
}
fn password(kind: &str) -> &str {
    match kind {
        "correct" => "xc-fixture-only",
        "wrong" => "wrong-fixture-only",
        "django" => "django-fixture-only",
        "empty" => "",
        "unicode" => "café",
        "number" => "123",
        "boolean" => "True",
        "array" => "[True, None]",
        "dict" => "{'x': False}",
        _ => panic!("unknown fixture password kind"),
    }
}
fn pickle_policy(allow: bool) -> Vec<u8> {
    let mut out = vec![0x80, 4, b'}', b'('];
    for text in [
        "XC_API",
        if allow {
            "0.0.0.0/0,::/0"
        } else {
            "192.0.2.0/24"
        },
    ] {
        out.extend([0x8c, text.len() as u8]);
        out.extend(text.as_bytes());
    }
    out.extend([b'u', b'.']);
    out
}
fn pickle_max(max: i64) -> Vec<u8> {
    assert!(max <= 255);
    let mut out = vec![0x80, 4, b'}', 0x8c, 17];
    out.extend(b"max_system_events");
    out.extend([b'K', max as u8, b's', b'.']);
    out
}
async fn prepare(
    redis: &mut redis::aio::MultiplexedConnection,
    ip: &str,
    mode: &str,
    prefill: usize,
) {
    let _: i64 = redis
        .del(&[
            format!(":1:throttle_xc_auth_{ip}"),
            ":1:coresettings:group:network_access".into(),
        ])
        .await
        .unwrap();
    if mode != "miss" {
        let _: () = redis
            .set_ex(
                ":1:coresettings:group:network_access",
                pickle_policy(mode != "deny"),
                300,
            )
            .await
            .unwrap();
    }
    if prefill > 0 {
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            - 59.0;
        let mut raw = vec![0x80, 4, b']', b'('];
        for _ in 0..prefill {
            raw.extend([b'G']);
            raw.extend(at.to_be_bytes())
        }
        raw.extend([b'e', b'.']);
        let _: () = redis
            .set_ex(format!(":1:throttle_xc_auth_{ip}"), raw, 60)
            .await
            .unwrap();
    }
}
async fn authorize(auth: &AuthService, username: &str, password: &str, ip: &str) -> Value {
    let request = PlaybackRequest {
        channel_id: String::new(),
        query: vec![],
        headers: vec![("user-agent".into(), "rust-probe-xc-UA".into())],
        peer_ip: ip.parse().unwrap(),
    };
    match auth
        .authorize_xc(&request, username, password, "XC_API")
        .await
    {
        Ok(user) => json!({"status":200,"user_id":user.id,"is_active":user.is_active}),
        Err(error) => {
            let response = error.into_response();
            let status = response.status().as_u16();
            let retry = response
                .headers()
                .get("retry-after")
                .map(|value| value.to_str().unwrap().parse::<i64>().unwrap());
            let challenge = response.headers().contains_key("www-authenticate");
            let body: Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap())
                    .unwrap();
            json!({"status":status,"body":body,"retry_after":retry,"bearer_challenge":challenge})
        }
    }
}
fn normalized_notice(mut value: Value) -> Value {
    let notice = &mut value["data"]["notification"];
    assert!(notice["id"].as_i64().is_some_and(|id| id > 0));
    let timestamp = notice["created_at"].as_str().unwrap();
    assert!(timestamp.ends_with("+00:00"));
    chrono::DateTime::parse_from_rfc3339(timestamp).unwrap();
    notice["id"] = json!("<notice-id>");
    notice["created_at"] = json!("<utc-iso>");
    value
}
async fn event_count(store: &Store, username: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM core_systemevent WHERE event_type='login_failed' AND details->>'reason'='XC auth rate limited' AND details->>'user'=$1").bind(username).fetch_one(store.pool()).await.unwrap()
}
async fn current_notice(store: &Store) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('id',id,'notification_key',notification_key,'notification_type',notification_type,'priority',priority,'source',source,'title',title,'message',message,'action_data',action_data,'is_active',is_active,'admin_only',admin_only) FROM core_systemnotification WHERE notification_key='xc-auth-lockout'").fetch_one(store.pool()).await.unwrap()
}
#[tokio::test]
#[ignore = "requires XC_DISPOSABLE_FIXTURE and captured original XC golden SQL/Redis fixture"]
async fn original_xc_credentials_inactive_networks_fail_only_limit_events_and_inbox() {
    assert_eq!(
        std::env::var("XC_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let golden: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("XC_AUTH_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let sink = Arc::new(Sink::default());
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(&url).await.unwrap())
    .with_event_sink(sink.clone());
    for case in golden["auth_cases"].as_array().unwrap() {
        let ip = case["ip"].as_str().unwrap();
        prepare(
            &mut redis,
            ip,
            case["mode"].as_str().unwrap(),
            case["prefill"].as_u64().unwrap() as usize,
        )
        .await;
        let kind = case["user_kind"].as_str().unwrap();
        let username = match case["username_kind"].as_str() {
            Some("empty") => "",
            Some("nul") => "invalid\0username",
            _ => golden["users"][kind]["username"].as_str().unwrap(),
        };
        let actual = authorize(
            &auth,
            username,
            password(case["password_kind"].as_str().unwrap()),
            ip,
        )
        .await;
        assert_eq!(
            actual,
            case["expected"],
            "XC source case {}",
            case["name"].as_str().unwrap()
        );
    }
    for case in golden["channel_cases"].as_array().unwrap() {
        let kind = case["user_kind"].as_str().unwrap();
        let user = store
            .user_by_id(golden["users"][kind]["id"].as_i64().unwrap())
            .await
            .unwrap()
            .unwrap();
        let channel = store
            .channel_for_xc_user_by_id(&user, case["channel_id"].as_i64().unwrap())
            .await
            .unwrap();
        let actual=channel.map_or(Value::Null,|channel|json!({"id":channel.id,"uuid":channel.uuid,"name":channel.name,"effective_name":channel.effective_name}));
        assert_eq!(actual, case["expected"], "source channel visibility case");
    }
    let username = golden["users"]["main"]["username"].as_str().unwrap();
    let ip = "198.51.100.240";
    prepare(&mut redis, ip, "allow", 0).await;
    let _: () = redis
        .set_ex(
            ":1:coresettings:group:system_settings",
            {
                let mut raw = vec![0x80, 4, b'}', 0x8c, 17];
                raw.extend(b"max_system_events");
                raw.extend([b'M', 0x10, 0x27, b's', b'.']);
                raw
            },
            300,
        )
        .await
        .unwrap();
    let _: i64 = redis
        .del(":1:xc_auth_lockout_notice_cooldown")
        .await
        .unwrap();
    sqlx::query("DELETE FROM core_systemevent")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN(SELECT id FROM core_systemnotification WHERE notification_key='xc-auth-lockout')").execute(store.pool()).await.unwrap();
    sqlx::query("DELETE FROM core_systemnotification WHERE notification_key='xc-auth-lockout'")
        .execute(store.pool())
        .await
        .unwrap();
    let mut sequence = Vec::new();
    for _ in 0..9 {
        sequence.push(authorize(&auth, username, password("wrong"), ip).await)
    }
    sequence.push(authorize(&auth, username, password("correct"), ip).await);
    sequence.push(authorize(&auth, username, password("wrong"), ip).await);
    let mut last = authorize(&auth, username, password("correct"), ip).await;
    // The two executions have independent wall clocks; validate retry floor
    // range separately, then retain the exact source field for comparison.
    assert!((1..=60).contains(&last["retry_after"].as_i64().unwrap()));
    last["retry_after"] = golden["lockout"]["first"]["sequence"][11]["retry_after"].clone();
    sequence.push(last);
    assert_eq!(
        sequence,
        golden["lockout"]["first"]["sequence"]
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(
        event_count(&store, username).await,
        golden["lockout"]["first"]["event_count"].as_i64().unwrap()
    );
    let details: Value =
        sqlx::query_scalar("SELECT details FROM core_systemevent ORDER BY timestamp DESC LIMIT 1")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(details, golden["lockout"]["first"]["event_details"]);
    let mut notice = current_notice(&store).await;
    let first_id = notice
        .as_object_mut()
        .unwrap()
        .remove("id")
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(notice, golden["lockout"]["first"]["notice"]);
    let cache: Vec<u8> = redis
        .get(":1:xc_auth_lockout_notice_cooldown")
        .await
        .unwrap();
    assert_eq!(cache, vec![0x80, 4, 0x88, b'.']);
    let ttl: i64 = redis
        .ttl(":1:xc_auth_lockout_notice_cooldown")
        .await
        .unwrap();
    assert!((1..=3600).contains(&ttl));
    assert_eq!(sink.0.lock().unwrap().len(), 1);
    assert_eq!(
        normalized_notice(sink.0.lock().unwrap()[0].clone()),
        golden["lockout"]["first"]["published"]
    );
    let _: i64 = redis
        .del(format!(":1:throttle_xc_auth_{ip}"))
        .await
        .unwrap();
    for _ in 0..10 {
        assert_eq!(
            authorize(&auth, username, password("wrong"), ip).await["status"],
            401
        )
    }
    assert_eq!(
        event_count(&store, username).await,
        golden["lockout"]["second"]["event_count"].as_i64().unwrap()
    );
    assert_eq!(current_notice(&store).await["id"], first_id);
    assert_eq!(sink.0.lock().unwrap().len(), 1);
    let admin = golden["users"]["admin"]["id"].as_i64().unwrap();
    sqlx::query("INSERT INTO core_notificationdismissal(user_id,notification_id,dismissed_at,action_taken) VALUES($1,$2,now(),NULL)").bind(admin).bind(first_id).execute(store.pool()).await.unwrap();
    sqlx::query("DELETE FROM core_systemevent")
        .execute(store.pool())
        .await
        .unwrap();
    for day in 1..=3 {
        sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details) VALUES('channel_start',$1,NULL,NULL,$2)").bind(chrono::DateTime::parse_from_rfc3339(&format!("2020-01-0{day}T00:00:00Z")).unwrap()).bind(json!({"owned_fixture":true})).execute(store.pool()).await.unwrap();
    }
    let _: () = redis
        .set_ex(":1:coresettings:group:system_settings", pickle_max(1), 300)
        .await
        .unwrap();
    let _: i64 = redis
        .del(&[
            ":1:xc_auth_lockout_notice_cooldown".to_owned(),
            format!(":1:throttle_xc_auth_{ip}"),
        ])
        .await
        .unwrap();
    for _ in 0..10 {
        assert_eq!(
            authorize(&auth, username, password("wrong"), ip).await["status"],
            401
        )
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core_systemevent")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(
        count,
        golden["lockout"]["third"]["event_total"].as_i64().unwrap()
    );
    assert_ne!(current_notice(&store).await["id"], first_id);
    let left: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core_notificationdismissal WHERE notification_id=$1",
    )
    .bind(first_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(left, 0);
    assert_eq!(sink.0.lock().unwrap().len(), 2);
    assert_eq!(
        normalized_notice(sink.0.lock().unwrap()[1].clone()),
        golden["lockout"]["third"]["published"]
    );
}
