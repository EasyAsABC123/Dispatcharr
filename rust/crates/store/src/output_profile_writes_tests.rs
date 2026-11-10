//! Real original HTTP/SQL/cache/failure/resource replay. Original authors retain credit.
//! External dependencies are guarded and explicitly ignored in default runs.
use super::*;
use crate::{AuthConfig, Store};
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
// This host-only router supplies the selected default response header.
// A later packaged gate must exercise the actual app middleware independently.
async fn selected_response_headers(request: Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .entry("x-content-type-options")
        .or_insert(axum::http::HeaderValue::from_static("nosniff"));
    response
}

#[test]
fn locked_model_order_differs_from_serializer_order() {
    let old = json!({"id":91003,"name":"locked","command":"ffmpeg","parameters":"p","locked":true,"is_active":true});
    let changes = json!({"locked":false,"is_active":false})
        .as_object()
        .unwrap()
        .clone();
    let Err(StoreError::InvalidInput(error)) = merged(Some(&old), changes) else {
        panic!("protected fields must reject")
    };
    assert_eq!(
        error,
        json!(["Cannot modify locked on a protected profile."])
    );
    let changes = json!({"name":"changed","command":"changed"})
        .as_object()
        .unwrap()
        .clone();
    let Err(StoreError::InvalidInput(error)) = merged(Some(&old), changes) else {
        panic!("protected fields must reject")
    };
    assert_eq!(error, json!(["Cannot modify name on a protected profile."]));
    assert_eq!(merged(Some(&old), Map::new()).unwrap(), old);
}
#[test]
fn cached_settings_ids_follow_python_int_before_comparison() {
    for value in [
        json!(91001),
        json!(91001.75),
        json!("91_001"),
        json!("٩١٠٠١"),
        json!(" +91001 "),
    ] {
        assert!(id_matches(Some(&value), 91001));
    }
    for value in [
        json!(null),
        json!(true),
        json!(false),
        json!([]),
        json!({}),
        json!("invalid"),
        json!(91002),
    ] {
        assert!(!id_matches(Some(&value), 91001));
    }
    assert!(id_matches(Some(&json!(true)), 1));
    assert!(!id_matches(
        Some(&json!(9007199254740992.0)),
        9007199254740993
    ));
    assert!(id_matches(
        Some(&json!(9007199254740992.0)),
        9007199254740992
    ));
    assert!(!id_matches(Some(&json!(9223372036854775808.0)), i64::MAX));
    assert!(id_matches(Some(&json!(i64::MAX)), i64::MAX));
}
fn bytes(spec: &Value) -> (Vec<u8>, String) {
    use base64::Engine;
    if spec.is_null() {
        return (vec![], "application/json".into());
    }
    if let Some(value) = spec.get("json") {
        return (
            serde_json::to_vec(value).unwrap(),
            "application/json".into(),
        );
    }
    if let Some(raw) = spec["raw"].as_str() {
        return (
            base64::engine::general_purpose::STANDARD
                .decode(raw)
                .unwrap(),
            spec["content_type"]
                .as_str()
                .unwrap_or("application/json")
                .into(),
        );
    }
    if let Some(values) = spec["form"].as_object() {
        let mut out = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in values {
            out.append_pair(key, value.as_str().unwrap());
        }
        return (
            out.finish().into_bytes(),
            "application/x-www-form-urlencoded".into(),
        );
    }
    let mut out = vec![];
    for (key, value) in spec["multipart"].as_object().unwrap() {
        out.extend_from_slice(format!("--rust-probe-output-boundary\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n{}\r\n",value.as_str().unwrap()).as_bytes())
    }
    out.extend_from_slice(b"--rust-probe-output-boundary--\r\n");
    (
        out,
        "multipart/form-data; boundary=rust-probe-output-boundary".into(),
    )
}
async fn clear_faults(store: &Store) {
    for statement in [
        "DROP TRIGGER IF EXISTS rust_probe_output_fault ON accounts_user",
        "DROP TRIGGER IF EXISTS rust_probe_output_fault ON core_coresettings",
        "DROP FUNCTION IF EXISTS rust_probe_output_fault()",
    ] {
        sqlx::query(statement).execute(store.pool()).await.unwrap();
    }
}
async fn install_fault(store: &Store, target: &str) {
    let(body,when)=match target {
        "users"=>("IF NEW.username='rust-probe-output-profile-reference-string' THEN RAISE EXCEPTION 'rust-probe-output-scrub-rejected'; END IF;","BEFORE UPDATE OF custom_properties ON accounts_user"),
        "stream_update"=>("IF NEW.key='stream_settings' THEN RAISE EXCEPTION 'rust-probe-output-scrub-rejected'; END IF;","BEFORE UPDATE ON core_coresettings"),
        "dvr_update"=>("IF NEW.key='dvr_settings' THEN RAISE EXCEPTION 'rust-probe-output-scrub-rejected'; END IF;","BEFORE UPDATE ON core_coresettings"),
        "stream_insert"=>("IF NEW.key='stream_settings' THEN RAISE EXCEPTION 'rust-probe-output-scrub-rejected'; END IF;","BEFORE INSERT ON core_coresettings"),
        _=>panic!("unknown fixture SQL fault"),
    };
    let function=format!("CREATE FUNCTION rust_probe_output_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {body} RETURN NEW; END $$");
    sqlx::query(&function).execute(store.pool()).await.unwrap();
    let trigger=format!("CREATE TRIGGER rust_probe_output_fault {when} FOR EACH ROW EXECUTE FUNCTION rust_probe_output_fault()");
    sqlx::query(&trigger).execute(store.pool()).await.unwrap();
}
async fn reset(store: &Store, redis: &mut MultiplexedConnection, case: &Value) {
    clear_faults(store).await;
    let mut tx = store.pool().begin().await.unwrap();
    sqlx::query("DELETE FROM core_outputprofile WHERE id>=91000")
        .execute(&mut *tx)
        .await
        .unwrap();
    for row in case["pre"]["db"]["profiles"].as_array().unwrap() {
        sqlx::query("INSERT INTO core_outputprofile(id,name,command,parameters,locked,is_active)VALUES($1,$2,$3,$4,$5,$6)")
            .bind(row["id"].as_i64().unwrap()).bind(row["name"].as_str().unwrap()).bind(row["command"].as_str().unwrap()).bind(row["parameters"].as_str().unwrap()).bind(row["locked"].as_bool().unwrap()).bind(row["is_active"].as_bool().unwrap()).execute(&mut *tx).await.unwrap();
    }
    for user in case["pre"]["db"]["users"].as_array().unwrap() {
        let username = format!(
            "rust-probe-output-profile-{}",
            user["suffix"].as_str().unwrap()
        );
        let affected =
            sqlx::query("UPDATE accounts_user SET custom_properties=$2 WHERE username=$1")
                .bind(username)
                .bind(if user["sql_null"] == true {
                    None
                } else {
                    Some(user["custom_properties"].clone())
                })
                .execute(&mut *tx)
                .await
                .unwrap()
                .rows_affected();
        assert_eq!(affected, 1);
    }
    sqlx::query("DELETE FROM core_coresettings WHERE key IN('stream_settings','dvr_settings')")
        .execute(&mut *tx)
        .await
        .unwrap();
    for group in case["pre"]["db"]["settings"].as_array().unwrap() {
        sqlx::query("INSERT INTO core_coresettings(id,key,name,value)VALUES($1,$2,$3,$4)")
            .bind(group["id"].as_i64().unwrap())
            .bind(group["key"].as_str().unwrap())
            .bind(group["name"].as_str().unwrap())
            .bind(&group["value"])
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    sqlx::query("SELECT setval('core_outputprofile_id_seq',$1,true)")
        .bind(case["sequence"].as_i64().unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT setval('core_coresettings_id_seq',$1,$2)")
        .bind(case["settings_sequence"][0].as_i64().unwrap())
        .bind(case["settings_sequence"][1].as_bool().unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for (key, value) in case["pre"]["cache"].as_object().unwrap() {
        let key = format!(":1:{key}");
        let _: usize = redis.del(&key).await.unwrap();
        if value["redis_type"].as_str() == Some("set") {
            for member in value["members"].as_array().unwrap() {
                let _: usize = redis.sadd(&key, member.as_str().unwrap()).await.unwrap();
            }
        } else if key.ends_with(":ver") || key.contains(":groupver:") {
            // Every case starts with fixture-owned version123456. The
            // normalized witness stores type/TTL facts instead of nanoseconds.
            let _: () = redis.set(&key, "123456").await.unwrap();
        } else if !value.is_null() {
            let _: () = redis
                .set_ex(&key, crate::encode_source_cache(value).unwrap(), 300)
                .await
                .unwrap();
        }
    }
    if let Some(target) = case["setup"]["sql_failure"].as_str() {
        install_fault(store, target).await;
    }
}
async fn database(store: &Store) -> Value {
    let profiles:Vec<Value>=sqlx::query_scalar("SELECT json_build_object('id',id,'name',name,'command',command,'parameters',parameters,'locked',locked,'is_active',is_active)FROM core_outputprofile WHERE id>=91000 ORDER BY id").fetch_all(store.pool()).await.unwrap();
    let users:Vec<Value>=sqlx::query_scalar("SELECT json_build_object('suffix',substring(username from length('rust-probe-output-profile-')+1),'custom_properties',custom_properties,'sql_null',custom_properties IS NULL)FROM accounts_user WHERE username LIKE 'rust-probe-output-profile-reference-%' ORDER BY id").fetch_all(store.pool()).await.unwrap();
    let settings:Vec<Value>=sqlx::query_scalar("SELECT json_build_object('id',id,'key',key,'name',name,'value',value)FROM core_coresettings WHERE key IN('stream_settings','dvr_settings')ORDER BY key").fetch_all(store.pool()).await.unwrap();
    let settings_sequence: Value = sqlx::query_scalar(
        "SELECT json_build_array(last_value,is_called)FROM core_coresettings_id_seq",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let profile_sequence: Value = sqlx::query_scalar(
        "SELECT json_build_array(last_value,is_called)FROM core_outputprofile_id_seq",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    json!({"profiles":profiles,"users":users,"settings":settings,"settings_sequence":settings_sequence,"profile_sequence":profile_sequence})
}
async fn cache_witness(redis: &mut MultiplexedConnection) -> Value {
    let mut values = Map::new();
    for key in [
        "coresettings:group:stream_settings",
        "coresettings:group:dvr_settings",
        "coresettings:groupver:stream_settings",
        "coresettings:groupver:dvr_settings",
        "coresettings:default_user_agent",
        "coresettings:default_user_agent:ver",
    ] {
        let redis_type: String = redis::cmd("TYPE")
            .arg(format!(":1:{key}"))
            .query_async(redis)
            .await
            .unwrap();
        if redis_type == "set" {
            let mut members: Vec<String> = redis.smembers(format!(":1:{key}")).await.unwrap();
            members.sort();
            values.insert(key.into(), json!({"redis_type":"set","members":members}));
            continue;
        }
        assert!(["none", "string"].contains(&redis_type.as_str()));
        let raw: Option<Vec<u8>> = redis.get(format!(":1:{key}")).await.unwrap();
        let value = raw
            .map(|raw| crate::decode_source_cache(&raw).unwrap())
            .unwrap_or(Value::Null);
        let value = if key.ends_with(":ver") || key.contains(":groupver:") {
            let ttl: i64 = redis.ttl(format!(":1:{key}")).await.unwrap();
            json!({"type":if value.is_i64()||value.is_u64(){"int"}else{"NoneType"},"old":value==json!(123456),"advanced":value.as_i64().is_some_and(|v|v>123456),"persistent":ttl== -1})
        } else {
            value
        };
        values.insert(key.into(), value);
    }
    Value::Object(values)
}
fn equal(left: &Value, right: &Value) -> bool {
    // Canonicalize key ordering but preserve int/float/boolean/string types.
    fn canonical(value: &Value) -> Value {
        match value {
            Value::Array(values) => json!(values.iter().map(canonical).collect::<Vec<_>>()),
            Value::Object(values) => Value::Object(
                values
                    .iter()
                    .map(|(k, v)| (k.clone(), canonical(v)))
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            ),
            value => value.clone(),
        }
    }
    serde_json::to_vec(&canonical(left)).unwrap() == serde_json::to_vec(&canonical(right)).unwrap()
}
#[tokio::test]
#[ignore = "requires disposable original HTTP capture and PostgreSQL/Redis; must be explicitly run after Source capture"]
async fn original_output_profile_serializer_delete_scrub_and_cache() {
    assert_eq!(
        std::env::var("OUTPUT_PROFILE_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let path =
        std::env::var("OUTPUT_PROFILE_FIXTURE").expect("explicit source fixture path required");
    let fixture: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert!(fixture["cases"]
        .as_array()
        .is_some_and(|cases| !cases.is_empty()));
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 6)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let cache = crate::NotificationCache::connect(&redis_url).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache);
    let service = OutputProfileWrites::new(auth.clone(), Some(&redis_url))
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let admission = service.workers.clone();
    let router = service
        .router()
        .merge(auth.clone().router())
        .layer(axum::middleware::from_fn(selected_response_headers))
        .layer(axum::Extension(ConnectInfo(
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        )));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let mut failures = vec![];
    for (index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        reset(&store, &mut redis, case).await;
        assert!(
            equal(&database(&store).await, &case["pre"]["db"]),
            "source/native restore database mismatch at {index}"
        );
        assert!(
            equal(&cache_witness(&mut redis).await, &case["pre"]["cache"]),
            "source/native restore cache mismatch at {index}"
        );
        let (body, mime) = bytes(&case["body"]);
        assert_eq!(
            format!("{:x}", Sha256::digest(&body)),
            case["request_body_sha256"].as_str().unwrap(),
            "request bytes {index}"
        );
        let method = case["method"]
            .as_str()
            .unwrap()
            .parse::<reqwest::Method>()
            .unwrap();
        let mut request = client
            .request(
                method,
                format!("http://{address}{}", case["path"].as_str().unwrap()),
            )
            .header("content-type", mime)
            .body(body);
        for (key, value) in case["headers"].as_object().unwrap() {
            request = request.header(key, value.as_str().unwrap())
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let mut headers = Map::new();
        for key in [
            "content-type",
            "content-length",
            "www-authenticate",
            "x-content-type-options",
        ] {
            if let Some(value) = response.headers().get(key) {
                headers.insert(key.into(), json!(value.to_str().unwrap()));
            }
        }
        let data = response.bytes().await.unwrap();
        let mut actual = json!({"status":status,"headers":headers});
        if actual["headers"]["content-type"]
            .as_str()
            .unwrap_or("")
            .contains("json")
        {
            actual["json"] = serde_json::from_slice(&data).unwrap();
        } else {
            actual["body_length"] = json!(data.len());
            actual["body_sha256"] = json!(format!("{:x}", Sha256::digest(&data)));
        }
        for (label, left, right) in [
            ("response", actual, case["expected"]["response"].clone()),
            ("db", database(&store).await, case["expected"]["db"].clone()),
            (
                "cache",
                cache_witness(&mut redis).await,
                case["expected"]["cache"].clone(),
            ),
        ] {
            if !equal(&left, &right) {
                failures.push(format!("case {index} {label}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "OutputProfile mismatch categories only: {failures:?}"
    );
    clear_faults(&store).await;
    // Actual SQL lock, not a mocked future: admitted owned mutations must
    // survive aborted HTTP clients and the fifth request must stop before SQL.
    let mut lock = store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE core_outputprofile IN SHARE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let credentials = fixture["cases"][0]["headers"].clone();
    let mut callers = vec![];
    for index in 0..4 {
        let client = client.clone();
        let headers = credentials.clone();
        callers.push(tokio::spawn(async move{
            let mut request=client.post(format!("http://{address}/api/core/outputprofiles/")).header("content-type","application/json").body(serde_json::to_vec(&json!({"name":format!("rust-probe-output-profile-abandoned-{index}"),"command":"ffmpeg","parameters":"-i pipe:0 -c copy -f mpegts pipe:1"})).unwrap());
            for(key,value)in headers.as_object().unwrap(){request=request.header(key,value.as_str().unwrap())}
            request.send().await
        }));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let count:i64=sqlx::query_scalar("SELECT count(*)FROM pg_stat_activity WHERE datname=current_database()AND wait_event_type='Lock'AND query LIKE 'INSERT INTO core_outputprofile(name,command,%'").fetch_one(store.pool()).await.unwrap();
        if count == 4 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "four owned OutputProfile mutations did not reach SQL"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(admission.available_permits(), 0);
    let mut fifth = client
        .post(format!("http://{address}/api/core/outputprofiles/"))
        .header("content-type", "application/json")
        .body(b"{}".to_vec());
    for (key, value) in credentials.as_object().unwrap() {
        fifth = fifth.header(key, value.as_str().unwrap())
    }
    assert_eq!(fifth.send().await.unwrap().status(), 503);
    for caller in callers {
        caller.abort();
        let _ = caller.await;
    }
    lock.commit().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let count:i64=sqlx::query_scalar("SELECT count(*)FROM core_outputprofile WHERE name LIKE 'rust-probe-output-profile-abandoned-%'").fetch_one(store.pool()).await.unwrap();
        if count == 4 && admission.available_permits() == 4 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "abandoned owned OutputProfile mutations failed to complete"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut recovered=client.post(format!("http://{address}/api/core/outputprofiles/")).header("content-type","application/json").body(serde_json::to_vec(&json!({"name":"rust-probe-output-profile-recovered","command":"ffmpeg","parameters":"-i pipe:0 -c copy -f mpegts pipe:1"})).unwrap());
    for (key, value) in credentials.as_object().unwrap() {
        recovered = recovered.header(key, value.as_str().unwrap())
    }
    assert_eq!(recovered.send().await.unwrap().status(), 201);
    server.abort();
    let _ = server.await;
    physical_reply_ownership(auth, &redis_url).await;
    eprintln!("NATIVE_OUTPUT_PROFILE_RESOURCES four_owned_mutations=pass overload503=pass abandoned_work_completes_and_recovers=pass");
}

async fn physical_reply_ownership(auth: AuthService, redis_url: &str) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let target = url::Url::parse(redis_url).unwrap();
    let backend = format!(
        "{}:{}",
        target.host_str().unwrap(),
        target.port().unwrap_or(6379)
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let held = Arc::new(AtomicBool::new(false));
    let arrived = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let (held_proxy, arrived_proxy, resume_proxy) = (held.clone(), arrived.clone(), resume.clone());
    let bridge = tokio::spawn(async move {
        let (front, _) = listener.accept().await.unwrap();
        let back = tokio::net::TcpStream::connect(backend).await.unwrap();
        let (mut frontend_read, mut frontend_write) = front.into_split();
        let (mut backend_read, mut backend_write) = back.into_split();
        let replies = async move {
            let mut buffer = [0_u8; 8192];
            loop {
                let count = backend_read.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                if held_proxy.load(Ordering::Acquire) {
                    arrived_proxy.notify_one();
                    resume_proxy.notified().await;
                }
                frontend_write.write_all(&buffer[..count]).await?;
            }
            Ok::<_, std::io::Error>(())
        };
        tokio::select! {_=tokio::io::copy(&mut frontend_read,&mut backend_write)=>{},_=replies=>{}}
    });
    let mut proxy = target;
    proxy.set_host(Some("127.0.0.1")).unwrap();
    proxy.set_port(Some(port)).unwrap();
    let service = OutputProfileWrites::new(auth.clone(), Some(proxy.as_str()))
        .await
        .unwrap();
    let cache = service.cache.as_ref().unwrap().clone();
    held.store(true, Ordering::Release);
    let writers = (0..MAX_WRITERS)
        .map(|_| Arc::new(service.reserve().unwrap()))
        .collect::<Vec<_>>();
    let weak = Arc::downgrade(&writers[0]);
    let mut callers = vec![];
    for writer in &writers {
        for _ in 0..4 {
            let cache = cache.clone();
            let writer = writer.clone();
            callers.push(tokio::spawn(async move {
                let mut command = redis::cmd("GET");
                command.arg(":1:rust_probe_output_hold_missing");
                cache.command::<Option<Vec<u8>>>(command, writer).await
            }));
        }
    }
    drop(writers);
    timeout(Duration::from_secs(5), arrived.notified())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while cache.physical_operations.available_permits() != 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    // Cancel observers and let one actual deadline expire. Physical work owns
    // BOTH its physical permit and its originating writer's Arc lease.
    let timed = callers.pop().unwrap();
    for caller in callers {
        caller.abort();
        let _ = caller.await;
    }
    assert!(matches!(timed.await.unwrap(), Ok(None)));
    assert_eq!(cache.physical_operations.available_permits(), 0);
    assert_eq!(service.workers.available_permits(), 0);
    assert!(matches!(
        service.reserve(),
        Err(StoreError::StatusBody(503, _))
    ));
    let mut overflow = redis::cmd("GET");
    overflow.arg(":1:rust_probe_output_hold_missing");
    assert!(matches!(
        timeout(
            Duration::from_millis(100),
            cache.command::<Option<Vec<u8>>>(overflow, weak.upgrade().unwrap())
        )
        .await
        .unwrap(),
        Ok(None)
    ));
    held.store(false, Ordering::Release);
    resume.notify_one();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while service.workers.available_permits() != MAX_WRITERS
        || cache.physical_operations.available_permits() != MAX_COMMANDS
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "physical replies did not release writer/command ownership"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(service.reserve().unwrap());
    // Hold a new reply and cancel its observer, then force bounded shutdown.
    // Driver closure must release the actual physical request before reporting
    // cleanup, and closed admission must reject later writers.
    held.store(true, Ordering::Release);
    let writer = Arc::new(service.reserve().unwrap());
    let pending_cache = cache.clone();
    let caller = tokio::spawn(async move {
        let mut command = redis::cmd("GET");
        command.arg(":1:rust_probe_output_hold_missing");
        pending_cache
            .command::<Option<Vec<u8>>>(command, writer)
            .await
    });
    timeout(Duration::from_secs(5), arrived.notified())
        .await
        .unwrap();
    caller.abort();
    let _ = caller.await;
    assert_eq!(service.workers.available_permits(), MAX_WRITERS - 1);
    assert!(service.shutdown(Duration::from_millis(50)).await);
    assert_eq!(service.workers.available_permits(), MAX_WRITERS);
    assert_eq!(cache.physical_operations.available_permits(), MAX_COMMANDS);
    assert!(service.reserve().is_err());
    let recovered = OutputProfileWrites::new(auth, Some(redis_url))
        .await
        .unwrap();
    drop(recovered.reserve().unwrap());
    assert!(recovered.shutdown(Duration::from_millis(10)).await);
    bridge.abort();
    let _ = bridge.await;
    eprintln!("NATIVE_OUTPUT_PROFILE_PHYSICAL physical16=pass canceled_observer_retains_writer=pass caller_deadline_retains_writer=pass overload503=pass reply_recovery=pass shutdown_closes_driver_and_drains=pass");
}
