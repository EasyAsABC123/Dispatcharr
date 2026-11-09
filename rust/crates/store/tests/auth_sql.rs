//! Actual original login hash-upgrade contracts on disposable Django SQL.
//! Original application credit remains with Dispatcharr contributors.
use axum::response::IntoResponse;
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::{verify_password, AuthConfig, AuthService, Store};
use serde_json::{json, Value};
static AUTH_SQL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
#[ignore = "requires AUTH_DISPOSABLE_FIXTURE and captured original login/SQL contracts"]
async fn original_pbkdf2_login_upgrade_before_active_check_and_no_wrong_password_write() {
    let _guard = AUTH_SQL_LOCK.lock().await;
    assert_eq!(
        std::env::var("AUTH_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let golden: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("AUTH_COMPAT_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    for case in golden["hash_cases"].as_array().unwrap() {
        let request = PlaybackRequest {
            channel_id: String::new(),
            query: vec![],
            headers: vec![],
            peer_ip: case["ip"].as_str().unwrap().parse().unwrap(),
        };
        let result = auth.login(&request, json!({"username":case["username"],"password":if case["correct"] == true {"hash-fixture-only"} else {"wrong-fixture-only"}})).await;
        let status = match result {
            Ok(_) => 200,
            Err(error) => error.into_response().status().as_u16(),
        };
        let encoded: String = sqlx::query_scalar("SELECT password FROM accounts_user WHERE id=$1")
            .bind(case["user_id"].as_i64().unwrap())
            .fetch_one(store.pool())
            .await
            .unwrap();
        let fields: Vec<_> = encoded.split('$').collect();
        let actual = json!({"status":status,"changed":encoded!=case["original_hash"].as_str().unwrap(),"password_valid":verify_password("hash-fixture-only",&encoded),"algorithm":fields[0],"iterations":fields[1].parse::<u32>().unwrap(),"salt_characters":fields[2].chars().count()});
        assert_eq!(
            actual,
            case["expected"],
            "source hash policy case {}",
            case["name"].as_str().unwrap()
        );
    }
}

async fn effects(store: &Store, redis: &mut redis::aio::MultiplexedConnection, ip: &str) -> Value {
    use redis::AsyncCommands;
    let events:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('event_type',event_type,'details',details) FROM core_systemevent WHERE details->>'client_ip'=$1 ORDER BY id").bind(ip).fetch_all(store.pool()).await.unwrap();
    let notices:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('notification_key',notification_key,'notification_type',notification_type,'priority',priority,'source',source,'title',title,'message',message,'action_data',action_data,'is_active',is_active,'admin_only',admin_only) FROM core_systemnotification WHERE notification_key IN('login-rate-limit','api-key-auth-lockout') ORDER BY notification_key").fetch_all(store.pool()).await.unwrap();
    let mut counts = Vec::new();
    for scope in ["login", "api_key_auth"] {
        let raw: Option<Vec<u8>> = redis
            .get(format!(":1:throttle_{scope}_{ip}"))
            .await
            .unwrap();
        counts.push(raw.map_or(0, |mut raw| history_wire(&mut raw, false)));
    }
    let login_suppression: bool = redis
        .exists(format!(":1:login_rate_limit_lockout_emitted:{ip}"))
        .await
        .unwrap();
    let login_cooldown: bool = redis
        .exists(":1:login_rate_limit_notice_cooldown")
        .await
        .unwrap();
    let api_cooldown: bool = redis
        .exists(":1:api_key_auth_lockout_notice_cooldown")
        .await
        .unwrap();
    json!({"events":events,"notices":notices,"login_history":counts[0],"api_history":counts[1],"login_suppression":login_suppression,"login_cooldown":login_cooldown,"api_cooldown":api_cooldown})
}
#[tokio::test]
#[ignore = "requires original throttle fixture plus disposable PostgreSQL/Redis"]
async fn original_shared_login_and_failure_only_api_key_limits_with_audit_and_notices() {
    let _guard = AUTH_SQL_LOCK.lock().await;
    use redis::AsyncCommands;
    assert_eq!(
        std::env::var("AUTH_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let golden: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("AUTH_COMPAT_FIXTURE").unwrap()).unwrap(),
    )
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
    // Independent service instances must observe the same budget.
    let mut services = Vec::new();
    for _ in 0..2 {
        services.push(
            AuthService::new(
                store.clone(),
                AuthConfig::new("playback-fixture-only".into()).unwrap(),
            )
            .with_notification_cache(
                dispatcharr_store::NotificationCache::connect(&url)
                    .await
                    .unwrap(),
            ),
        );
    }
    for scenario in golden["throttle_scenarios"].as_array().unwrap() {
        let ip = scenario["ip"].as_str().unwrap();
        let keys = vec![
            format!(":1:throttle_login_{ip}"),
            format!(":1:throttle_api_key_auth_{ip}"),
            format!(":1:login_rate_limit_lockout_emitted:{ip}"),
            ":1:login_rate_limit_notice_cooldown".into(),
            ":1:api_key_auth_lockout_notice_cooldown".into(),
        ];
        let _: i64 = redis.del(keys).await.unwrap();
        sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN(SELECT id FROM core_systemnotification WHERE notification_key IN('login-rate-limit','api-key-auth-lockout'))").execute(store.pool()).await.unwrap();
        sqlx::query("DELETE FROM core_systemnotification WHERE notification_key IN('login-rate-limit','api-key-auth-lockout')").execute(store.pool()).await.unwrap();
        sqlx::query("DELETE FROM core_systemevent WHERE details->>'client_ip'=$1")
            .bind(ip)
            .execute(store.pool())
            .await
            .unwrap();
        seed_history(&mut redis, scenario, ip).await;
        let _: i64 = redis
            .del(":1:coresettings:group:network_access")
            .await
            .unwrap();
        if let Some(network) = scenario["network"].as_str() {
            let mut raw = vec![
                0x80,
                4,
                b'}',
                b'(',
                0x8c,
                2,
                b'U',
                b'I',
                0x8c,
                network.len() as u8,
            ];
            raw.extend(network.as_bytes());
            raw.extend(b"u.");
            let _: () = redis
                .set_ex(":1:coresettings:group:network_access", raw, 300)
                .await
                .unwrap();
        }
        for (index, step) in scenario["steps"].as_array().unwrap().iter().enumerate() {
            let service = &services[index % 2];
            let kind = step["kind"].as_str().unwrap();
            let mut request = PlaybackRequest {
                channel_id: String::new(),
                query: vec![],
                headers: vec![("user-agent".into(), "rust-probe-auth-UA".into())],
                peer_ip: ip.parse().unwrap(),
            };
            let result = if kind.starts_with("login-") {
                let body = match kind {
                    "login-null" => json!({"username":null,"password":null}),
                    "login-blank" => json!({"username":" ","password":" "}),
                    "login-nonstring" => json!({"username":false,"password":{}}),
                    "login-nul" => {
                        json!({"username":"invalid\0name","password":"invalid\0password"})
                    }
                    _ => json!({}),
                };
                service
                    .login(&request, body)
                    .await
                    .map(|_| json!({"ok":true}))
            } else {
                if kind != "key-missing" {
                    request.headers.push((
                        "x-api-key".into(),
                        match kind {
                            "key-valid" => "auth-fixture-active",
                            "key-inactive" => "auth-fixture-inactive",
                            _ => "auth-fixture-invalid",
                        }
                        .into(),
                    ));
                }
                service
                    .authenticate(&request)
                    .await
                    .map(|user| json!({"user_id":user.map(|user|user.id)}))
            };
            let (status, body, retry) = match result {
                Ok(value) => (200, value, None),
                Err(error) => {
                    let response = error.into_response();
                    let status = response.status().as_u16();
                    let retry = response
                        .headers()
                        .get("retry-after")
                        .map(|v| v.to_str().unwrap().to_owned());
                    let body = serde_json::from_slice::<Value>(
                        &axum::body::to_bytes(response.into_body(), 8192)
                            .await
                            .unwrap(),
                    )
                    .unwrap();
                    (status, body, retry)
                }
            };
            let actual = json!({"status":status,"body":body,"retry_after":retry,"effects":effects(&store,&mut redis,ip).await});
            assert_eq!(
                actual,
                step["expected"],
                "source scenario {} step {index}",
                scenario["name"].as_str().unwrap()
            );
        }
    }
}

/// Fixture-only parser for the two source/native primitive float-list formats.
/// Rebase timestamp bytes to the current clock when restoring source history;
/// this preserves relative ages while avoiding fixture setup/build expiration.
fn history_wire(raw: &mut [u8], rebase: bool) -> usize {
    use std::time::{SystemTime, UNIX_EPOCH};
    assert!(raw.len() < 16384);
    let mut at = 0;
    let mut count = 0;
    let mut shift = None;
    while at < raw.len() {
        match raw[at] {
            0x80 => {
                assert!(at == 0);
                at += 2;
            }
            0x95 => {
                at += 9;
            }
            b']' | b'(' | b'e' | 0x94 => {
                at += 1;
            }
            b'.' => {
                assert_eq!(at + 1, raw.len());
                break;
            }
            b'G' => {
                let old = f64::from_be_bytes(raw[at + 1..at + 9].try_into().unwrap());
                assert!(old.is_finite());
                if rebase {
                    let delta = *shift.get_or_insert_with(|| {
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64()
                            - old
                    });
                    raw[at + 1..at + 9].copy_from_slice(&(old + delta).to_be_bytes());
                }
                count += 1;
                at += 9;
            }
            _ => panic!("Unexpected primitive fixture history opcode"),
        }
    }
    count
}
async fn seed_history(redis: &mut redis::aio::MultiplexedConnection, scenario: &Value, ip: &str) {
    use redis::AsyncCommands;
    if let Some(wire) = scenario["prefill_wire"].as_str() {
        let mut raw = wire
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>();
        assert!(history_wire(&mut raw, true) > 0);
        let scope = scenario["prefill_scope"].as_str().unwrap_or("login");
        let _: () = redis
            .set_ex(format!(":1:throttle_{scope}_{ip}"), raw, 60)
            .await
            .unwrap();
    }
}

async fn fixture_http(
    address: std::net::SocketAddr,
    path: &str,
    headers: &[(String, String)],
) -> (u16, Value, Option<String>, Option<String>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut request=format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 2\r\n");
    for (key, value) in headers {
        assert!(!value.contains(['\r', '\n']));
        request.push_str(&format!("{key}: {value}\r\n"));
    }
    request.push_str("\r\n{}");
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut raw = vec![];
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        socket.read_to_end(&mut raw),
    )
    .await
    .unwrap()
    .unwrap();
    let boundary = raw.windows(4).position(|v| v == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&raw[..boundary]).unwrap();
    let status = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let header = |name: &str| {
        head.lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim().to_owned())
    };
    let body = serde_json::from_slice(&raw[boundary + 4..]).unwrap();
    (
        status,
        body,
        header("retry-after"),
        header("www-authenticate"),
    )
}
#[tokio::test]
#[ignore = "requires original proxy/JWT/audit fixture plus disposable PostgreSQL/Redis"]
async fn original_proxy_availability_shared_budget_cached_settings_authenticated_bypass_and_logout()
{
    use redis::AsyncCommands;
    let _guard = AUTH_SQL_LOCK.lock().await;
    assert_eq!(
        std::env::var("AUTH_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let golden: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("AUTH_COMPAT_FIXTURE").unwrap()).unwrap(),
    )
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
    let mut config = AuthConfig::new("playback-fixture-only".into()).unwrap();
    config.trusted_proxies = Some("127.0.0.0/8".into());
    let auth = AuthService::new(store.clone(), config).with_notification_cache(
        dispatcharr_store::NotificationCache::connect(&url)
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_auth = auth.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            server_auth
                .router()
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    for scenario in golden["proxy_scenarios"].as_array().unwrap() {
        let ip = scenario["ip"].as_str().unwrap();
        let _: i64 = redis
            .del(vec![
                format!(":1:throttle_login_{ip}"),
                format!(":1:throttle_api_key_auth_{ip}"),
                format!(":1:login_rate_limit_lockout_emitted:{ip}"),
                ":1:login_rate_limit_notice_cooldown".into(),
                ":1:api_key_auth_lockout_notice_cooldown".into(),
                ":1:coresettings:group:network_access".into(),
            ])
            .await
            .unwrap();
        sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN(SELECT id FROM core_systemnotification WHERE notification_key IN('login-rate-limit','api-key-auth-lockout'))").execute(store.pool()).await.unwrap();
        sqlx::query("DELETE FROM core_systemnotification WHERE notification_key IN('login-rate-limit','api-key-auth-lockout')").execute(store.pool()).await.unwrap();
        sqlx::query("DELETE FROM core_systemevent WHERE details->>'client_ip'=$1")
            .bind(ip)
            .execute(store.pool())
            .await
            .unwrap();
        // Source's nonempty cached proxy config deliberately contrasts SQL's
        // disabled value. Generate the bounded primitive dict wire directly.
        let mut wire = vec![0x80, 4, b'}', b'('];
        for (key, value) in [("enabled", None), ("header", Some("X-Probe-Identity"))] {
            wire.extend([0x8c, key.len() as u8]);
            wire.extend(key.as_bytes());
            if let Some(value) = value {
                wire.extend([0x8c, value.len() as u8]);
                wire.extend(value.as_bytes())
            } else {
                wire.push(0x88)
            }
        }
        wire.extend(b"u.");
        let _: () = redis
            .set_ex(":1:coresettings:group:reverse_proxy_auth", wire, 300)
            .await
            .unwrap();
        seed_history(&mut redis, scenario, ip).await;
        if let Some(network) = scenario["network"].as_str() {
            let mut raw = vec![
                0x80,
                4,
                b'}',
                b'(',
                0x8c,
                2,
                b'U',
                b'I',
                0x8c,
                network.len() as u8,
            ];
            raw.extend(network.as_bytes());
            raw.extend(b"u.");
            let _: () = redis
                .set_ex(":1:coresettings:group:network_access", raw, 300)
                .await
                .unwrap();
        }
        for (index, step) in scenario["steps"].as_array().unwrap().iter().enumerate() {
            let kind = step["kind"].as_str().unwrap();
            let mut headers = vec![
                ("X-Real-IP".into(), ip.into()),
                ("User-Agent".into(), "rust-probe-auth-UA".into()),
            ];
            if kind != "proxy-missing" {
                headers.push((
                    "X-Probe-Identity".into(),
                    match kind {
                        "proxy-inactive" => "rust-probe-auth-key-inactive",
                        "proxy-unknown" => "rust-probe-no-such-identity",
                        _ => "rust-probe-auth-key-active",
                    }
                    .into(),
                ));
            }
            if kind == "proxy-authenticated" || kind == "logout-authenticated" {
                headers.push((
                    "Authorization".into(),
                    format!("Bearer {}", golden["proxy_access"].as_str().unwrap()),
                ));
            }
            if kind == "proxy-invalid-bearer" {
                headers.push(("Authorization".into(), "Bearer invalid".into()));
            }
            let path = if kind == "logout-authenticated" {
                "/api/accounts/auth/logout/"
            } else if kind == "refresh-network-denied" {
                "/api/accounts/token/refresh/"
            } else {
                "/api/accounts/auth/proxy-login/"
            };
            let (status, mut body, retry, challenge) = fixture_http(address, path, &headers).await;
            if status == 200 && kind != "logout-authenticated" {
                let claims = auth
                    .decode_token(body["access"].as_str().unwrap(), "access")
                    .unwrap();
                assert_eq!(claims.id(), golden["key_active_id"].as_i64());
                assert!(body["refresh"].is_string());
                body = json!({"token_pair":true,"user_id":claims.id()});
            }
            let actual = json!({"status":status,"body":body,"retry_after":retry,"challenge":challenge,"effects":effects(&store,&mut redis,ip).await});
            assert_eq!(
                actual,
                step["expected"],
                "proxy source case {} step {index}",
                scenario["name"].as_str().unwrap()
            );
        }
    }
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
