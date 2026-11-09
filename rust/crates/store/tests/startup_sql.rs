//! Original first-admin setup against disposable original migrated PostgreSQL.
//! Original Dispatcharr contributors retain application credit.
use dispatcharr_store::{verify_password, AuthConfig, AuthService, Store};
use serde_json::{json, Value};
use sqlx::Row;
async fn users(store: &Store, check_password: &str) -> Value {
    let rows=sqlx::query("SELECT username,email,user_level,is_superuser,is_staff,is_active,custom_properties,avatar_config,stream_limit,api_key,last_login IS NOT NULL AS login_set,date_joined IS NOT NULL AS joined_set,password FROM accounts_user ORDER BY username").fetch_all(store.pool()).await.unwrap();
    let mut result = vec![];
    for row in rows {
        let encoded: String = row.try_get("password").unwrap();
        let check_password = check_password.to_owned();
        let valid = tokio::task::spawn_blocking(move || verify_password(&check_password, &encoded))
            .await
            .unwrap();
        result.push(json!({"username":row.try_get::<String,_>("username").unwrap(),"email":row.try_get::<String,_>("email").unwrap(),"user_level":row.try_get::<i32,_>("user_level").unwrap(),"is_superuser":row.try_get::<bool,_>("is_superuser").unwrap(),"is_staff":row.try_get::<bool,_>("is_staff").unwrap(),"is_active":row.try_get::<bool,_>("is_active").unwrap(),"custom_properties":row.try_get::<Value,_>("custom_properties").unwrap(),"avatar_config":row.try_get::<Value,_>("avatar_config").unwrap(),"stream_limit":row.try_get::<i32,_>("stream_limit").unwrap(),"api_key":row.try_get::<Option<String>,_>("api_key").unwrap(),"last_login_set":row.try_get::<bool,_>("login_set").unwrap(),"date_joined_set":row.try_get::<bool,_>("joined_set").unwrap(),"password_valid":valid}));
    }
    json!(result)
}
async fn http(
    address: std::net::SocketAddr,
    method: &str,
    body: &str,
    headers: &Value,
) -> (u16, Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut request=format!("{method} /api/accounts/initialize-superuser/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",body.len());
    for (key, value) in headers.as_object().unwrap() {
        let key = key.strip_prefix("HTTP_").unwrap().replace('_', "-");
        let value = value.as_str().unwrap();
        assert!(!value.contains(['\r', '\n']));
        request.push_str(&format!("{key}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut raw = vec![];
    tokio::time::timeout(
        std::time::Duration::from_secs(45),
        socket.read_to_end(&mut raw),
    )
    .await
    .unwrap()
    .unwrap();
    let boundary = raw.windows(4).position(|v| v == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&raw[..boundary]).unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = serde_json::from_slice(&raw[boundary + 4..]).unwrap();
    (status, body)
}
#[tokio::test]
#[ignore = "requires BOOTSTRAP_DISPOSABLE_FIXTURE and fresh empty original migrated user table"]
async fn original_first_admin_ip_gate_plain_json_manager_flags_nfkc_and_existing_closure() {
    use redis::AsyncCommands;
    assert_eq!(
        std::env::var("BOOTSTRAP_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let golden: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("BOOTSTRAP_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(golden["fresh_user_table"], true);
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts_user")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(count, 0, "fresh user table required, no unowned mutations");
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        dispatcharr_store::NotificationCache::connect(&redis_url)
            .await
            .unwrap(),
    );
    for case in golden["bootstrap_cases"].as_array().unwrap() {
        if let Some(value) = case["override"].as_str() {
            std::env::set_var("DISPATCHARR_SETUP_ALLOWED_IP", value)
        } else {
            std::env::remove_var("DISPATCHARR_SETUP_ALLOWED_IP")
        }
        if let Some(seed) = case["preseed"].as_object() {
            sqlx::query("INSERT INTO accounts_user(password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,avatar_config,stream_limit,api_key) VALUES('!',NULL,FALSE,$1,'','','',FALSE,$2,NOW(),$3,'{}'::jsonb,'{}'::jsonb,0,NULL)").bind(seed["username"].as_str().unwrap()).bind(seed.get("active").and_then(Value::as_bool).unwrap_or(true)).bind(seed.get("level").and_then(Value::as_i64).unwrap_or(0)as i32).execute(store.pool()).await.unwrap();
        }
        let _: i64 = redis
            .del(":1:coresettings:group:network_access")
            .await
            .unwrap();
        if case["ui_deny"] == true {
            let mut raw = vec![0x80, 4, b'}', b'(', 0x8c, 2, b'U', b'I', 0x8c, 12];
            raw.extend(b"192.0.2.0/24");
            raw.extend(b"u.");
            let _: () = redis
                .set_ex(":1:coresettings:group:network_access", raw, 300)
                .await
                .unwrap();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Original RequestFactory and native ConnectInfo receive the same peer
        // tuple. HTTP transport is real; synthetic ACL peers are explicit.
        let peer =
            std::net::SocketAddr::new(case["peer"].as_str().unwrap().parse().unwrap(), 12345);
        let router = auth
            .clone()
            .router()
            .layer(axum::Extension(axum::extract::ConnectInfo(peer)));
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (status, body) = http(
            address,
            case["method"].as_str().unwrap(),
            case["body"].as_str().unwrap(),
            &case["headers"],
        )
        .await;
        let input: Option<Value> = serde_json::from_str(case["body"].as_str().unwrap()).ok();
        let check_password = input
            .as_ref()
            .and_then(|value| value.get("password"))
            .and_then(Value::as_str)
            .unwrap_or("bootstrap-fixture-only");
        let actual =
            json!({"status":status,"body":body,"users":users(&store,check_password).await});
        assert_eq!(
            actual,
            case["expected"],
            "bootstrap source case {}",
            case["name"].as_str().unwrap()
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts_user")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert!(count <= 2);
        sqlx::query("DELETE FROM accounts_user")
            .execute(store.pool())
            .await
            .unwrap();
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }
    std::env::remove_var("DISPATCHARR_SETUP_ALLOWED_IP");
}
