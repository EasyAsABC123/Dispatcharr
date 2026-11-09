//! Original UserAgent HTTP/serializer/FK/shared-cache contracts, real PostgreSQL.
//! Original Dispatcharr contributors retain application credit.
#[path = "../../app/src/response_headers.rs"]
mod app_response_headers;
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use redis::AsyncCommands;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};
fn normalize(value: Value) -> Value {
    match value {
        Value::Array(values) => json!(values.into_iter().map(normalize).collect::<Vec<_>>()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(k, v)| {
                    let v = if ["created_at", "updated_at"].contains(&k.as_str())
                        && v.as_str().is_some_and(|s| {
                            s.starts_with(&chrono::Datelike::year(&chrono::Utc::now()).to_string())
                                && chrono::DateTime::parse_from_rfc3339(s).is_ok()
                        }) {
                        json!("<clock>")
                    } else {
                        normalize(v)
                    };
                    (k, v)
                })
                .collect(),
        ),
        value => value,
    }
}
fn wire(spec: &Value) -> (Vec<u8>, String) {
    use base64::Engine;
    if spec.is_null() {
        return (vec![], "application/json".into());
    }
    if let Some(v) = spec.get("json") {
        return (serde_json::to_vec(v).unwrap(), "application/json".into());
    }
    if let Some(v) = spec["raw"].as_str() {
        return (
            base64::engine::general_purpose::STANDARD.decode(v).unwrap(),
            spec["content_type"]
                .as_str()
                .unwrap_or("application/json")
                .into(),
        );
    }
    if let Some(values) = spec["form"].as_object() {
        let mut out = url::form_urlencoded::Serializer::new(String::new());
        for (k, v) in values {
            out.append_pair(k, v.as_str().unwrap());
        }
        return (
            out.finish().into_bytes(),
            "application/x-www-form-urlencoded".into(),
        );
    }
    let mut out = Vec::new();
    for (k, v) in spec["multipart"].as_object().unwrap() {
        out.extend_from_slice(format!("--rust-probe-boundary\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{}\r\n",v.as_str().unwrap()).as_bytes());
    }
    out.extend_from_slice(b"--rust-probe-boundary--\r\n");
    (
        out,
        "multipart/form-data; boundary=rust-probe-boundary".into(),
    )
}
async fn restore(store: &Store, fixture: &Value, case: &Value) {
    let mut tx = store.pool().begin().await.unwrap();
    sqlx::query("UPDATE core_streamprofile SET user_agent_id=NULL WHERE user_agent_id>=90000")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE m3u_m3uaccount SET user_agent_id=NULL WHERE user_agent_id>=90000")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM core_useragent WHERE id>=90000")
        .execute(&mut *tx)
        .await
        .unwrap();
    for row in fixture["agents"].as_array().unwrap() {
        sqlx::query("INSERT INTO core_useragent(id,name,user_agent,description,is_active,created_at,updated_at) VALUES($1,$2,$3,$4,$5,TIMESTAMPTZ '2000-01-02T03:04:05.123456Z',TIMESTAMPTZ '2001-02-03T04:05:06.654321Z')").bind(row["id"].as_i64().unwrap()).bind(row["name"].as_str().unwrap()).bind(row["user_agent"].as_str().unwrap()).bind(row["description"].as_str().unwrap()).bind(row["is_active"].as_bool().unwrap()).execute(&mut *tx).await.unwrap();
    }
    sqlx::query("UPDATE core_streamprofile SET user_agent_id=90001 WHERE id=$1")
        .bind(fixture["profile_id"].as_i64().unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE m3u_m3uaccount SET user_agent_id=90001 WHERE id=$1")
        .bind(fixture["provider_id"].as_i64().unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT setval('core_useragent_id_seq',$1,true)")
        .bind(case["sequence"].as_i64().unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
fn date(value: chrono::DateTime<chrono::Utc>) -> String {
    use chrono::SecondsFormat;
    value.to_rfc3339_opts(
        if value.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        true,
    )
}
async fn db(store: &Store, fixture: &Value) -> Value {
    use sqlx::Row;
    let rows=sqlx::query("SELECT id::bigint,name,user_agent,description,is_active,created_at,updated_at FROM core_useragent WHERE id>=90000 ORDER BY id").fetch_all(store.pool()).await.unwrap();
    let agents=rows.into_iter().map(|row|{let created:chrono::DateTime<chrono::Utc>=row.get("created_at");let updated:chrono::DateTime<chrono::Utc>=row.get("updated_at");use chrono::Datelike;json!({"id":row.get::<i64,_>("id"),"name":row.get::<String,_>("name"),"user_agent":row.get::<String,_>("user_agent"),"description":row.get::<String,_>("description"),"is_active":row.get::<bool,_>("is_active"),"created_at":date(created),"updated_at":date(updated),"created_is_fixed":created.year()==2000,"updated_is_fixed":updated.year()==2001,"dates_ordered":created<=updated,"dates_utc":true})}).collect::<Vec<_>>();
    let profile = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT user_agent_id::bigint FROM core_streamprofile WHERE id=$1",
    )
    .bind(fixture["profile_id"].as_i64().unwrap())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let provider = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT user_agent_id::bigint FROM m3u_m3uaccount WHERE id=$1",
    )
    .bind(fixture["provider_id"].as_i64().unwrap())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let unchanged:bool=sqlx::query_scalar("SELECT (value->>'default_user_agent')='90001' FROM core_coresettings WHERE key='stream_settings'").fetch_one(store.pool()).await.unwrap();
    normalize(
        json!({"agents":agents,"profile_user_agent":profile,"provider_user_agent":provider,"default_setting_unchanged":unchanged}),
    )
}
async fn cached(connection: &mut redis::aio::MultiplexedConnection) -> Value {
    let value: Option<Vec<u8>> = connection
        .get(":1:coresettings:default_user_agent")
        .await
        .unwrap();
    let version: Option<Vec<u8>> = connection
        .get(":1:coresettings:default_user_agent:ver")
        .await
        .unwrap();
    let value = value
        .and_then(|v| dispatcharr_store::decode_source_cache(&v))
        .unwrap_or(Value::Null);
    let version = version
        .and_then(|v| dispatcharr_store::decode_source_cache(&v))
        .unwrap_or(Value::Null);
    let ttl: i64 = connection
        .ttl(":1:coresettings:default_user_agent:ver")
        .await
        .unwrap();
    json!({"value":value,"version_type":if version.is_i64()||version.is_u64(){"int"}else{"other"},"version_old":version==json!(123456),"version_advanced":version.as_i64().is_some_and(|v|v>123456),"version_persistent":ttl == -1})
}
async fn reset_cache(connection: &mut redis::aio::MultiplexedConnection) {
    let _: () = connection
        .set(
            ":1:coresettings:default_user_agent",
            dispatcharr_store::encode_source_cache(&json!("Rust-Probe/stale")).unwrap(),
        )
        .await
        .unwrap();
    let _: () = connection
        .set(":1:coresettings:default_user_agent:ver", 123456)
        .await
        .unwrap();
}
async fn line<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut out = Vec::new();
    for _ in 0..128 {
        out.push(reader.read_u8().await?);
        if out.ends_with(b"\r\n") {
            return Ok(out);
        }
    }
    Err(std::io::Error::other("RESP line exceeds fixture limit"))
}
async fn frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    use tokio::io::AsyncReadExt;
    let head = line(reader).await?;
    if head.first() != Some(&b'*') {
        return Err(std::io::Error::other("RESP array expected"));
    }
    let count = std::str::from_utf8(&head[1..head.len() - 2])
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v <= 128)
        .ok_or_else(|| std::io::Error::other("RESP array bound"))?;
    let mut raw = head;
    let mut args = Vec::new();
    for _ in 0..count {
        let head = line(reader).await?;
        if head.first() != Some(&b'$') {
            return Err(std::io::Error::other("RESP bulk expected"));
        }
        let length = std::str::from_utf8(&head[1..head.len() - 2])
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v <= 1024 * 1024)
            .ok_or_else(|| std::io::Error::other("RESP bulk bound"))?;
        if raw.len() + length > 1024 * 1024 {
            return Err(std::io::Error::other("RESP frame bound"));
        }
        let mut bytes = vec![0; length + 2];
        reader.read_exact(&mut bytes).await?;
        if !bytes.ends_with(b"\r\n") {
            return Err(std::io::Error::other("RESP terminator"));
        }
        args.push(bytes[..length].to_vec());
        raw.extend(head);
        raw.extend(bytes);
    }
    Ok((raw, args))
}
async fn version_proxy() -> (
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (loaded, waiting) = tokio::sync::oneshot::channel();
    let (resume, release) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let (client, _) = listener.accept().await.unwrap();
        let server = tokio::net::TcpStream::connect("127.0.0.1:6379")
            .await
            .unwrap();
        let (mut input, mut output) = client.into_split();
        let (mut replies, mut requests) = server.into_split();
        let send = async move {
            let mut seen = 0;
            let mut loaded = Some(loaded);
            let mut release = Some(release);
            while let Ok((raw, args)) = frame(&mut input).await {
                if args
                    .first()
                    .is_some_and(|v| v.eq_ignore_ascii_case(b"EVAL"))
                    && args
                        .iter()
                        .any(|v| v == b":1:coresettings:default_user_agent:ver")
                {
                    seen += 1;
                    if seen == 2 {
                        let _ = loaded.take().unwrap().send(());
                        let _ = release.take().unwrap().await;
                    }
                }
                if requests.write_all(&raw).await.is_err() {
                    break;
                }
            }
        };
        tokio::select! {_=send=>{},_=tokio::io::copy(&mut replies,&mut output)=>{}}
    });
    (
        format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        waiting,
        resume,
        task,
    )
}
#[tokio::test]
#[ignore = "requires USER_AGENT_DISPOSABLE_FIXTURE in the owned original PostgreSQL/Redis container"]
async fn original_user_agent_crud_roles_fk_and_shared_cache() {
    assert_eq!(
        std::env::var("USER_AGENT_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("USER_AGENT_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 6)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let cache = NotificationCache::connect(&redis_url).await.unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache.clone());
    let reader = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = auth
        .clone()
        .router()
        .layer(axum::middleware::from_fn(app_response_headers::apply))
        .layer(axum::Extension(axum::extract::ConnectInfo(
            "127.0.0.1:12345".parse::<std::net::SocketAddr>().unwrap(),
        )));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let mut failures = Vec::new();
    macro_rules! same{($a:expr,$b:expr,$($label:tt)*)=>{{let actual=&$a;let expected=&$b;if actual!=expected{let label=format!($($label)*);eprintln!("{label}: actual={actual:?} expected={expected:?}");failures.push(label);}}};}
    for (index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        restore(&store, &fixture, case).await;
        reset_cache(&mut redis).await;
        let (body, mime) = wire(&case["body"]);
        same!(
            format!("{:x}", Sha256::digest(&body)),
            case["request_body_sha256"].as_str().unwrap(),
            "case {index} request bytes"
        );
        let mut request = client
            .request(
                case["method"]
                    .as_str()
                    .unwrap()
                    .parse::<reqwest::Method>()
                    .unwrap(),
                format!("http://{address}{}", case["path"].as_str().unwrap()),
            )
            .header("content-type", mime)
            .body(body);
        for (k, v) in case["headers"].as_object().unwrap() {
            request = request.header(k, v.as_str().unwrap());
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter(|(k, _)| {
                [
                    "content-type",
                    "content-length",
                    "www-authenticate",
                    "x-content-type-options",
                ]
                .contains(&k.as_str())
            })
            .map(|(k, v)| {
                (
                    k.as_str().to_owned(),
                    v.as_bytes()
                        .iter()
                        .map(|v| char::from(*v))
                        .collect::<String>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let body = response.bytes().await.unwrap();
        let expected = &case["expected"]["response"];
        same!(
            status,
            expected["status"].as_u64().unwrap() as u16,
            "case {index} {} status",
            case["name"]
        );
        // Clock-dependent timestamp byte lengths can differ at exact second boundaries;
        // the serialized values are parsed and compared after explicit normalization.
        let mut headers = json!(headers);
        let mut expected_headers = expected["headers"].clone();
        if expected
            .get("json")
            .is_some_and(|v| v.to_string().contains("<clock>"))
        {
            headers.as_object_mut().unwrap().remove("content-length");
            expected_headers
                .as_object_mut()
                .unwrap()
                .remove("content-length");
        }
        same!(
            headers,
            expected_headers,
            "case {index} {} headers",
            case["name"]
        );
        if let Some(expected) = expected.get("json") {
            same!(
                normalize(serde_json::from_slice(&body).unwrap()),
                expected.clone(),
                "case {index} {} JSON",
                case["name"]
            );
        } else {
            same!(
                body.len(),
                expected["body_length"].as_u64().unwrap() as usize,
                "case {index} {} body length",
                case["name"]
            );
            same!(
                format!("{:x}", Sha256::digest(body)),
                expected["body_sha256"].as_str().unwrap(),
                "case {index} {} body hash",
                case["name"]
            );
        }
        same!(
            db(&store, &fixture).await,
            case["expected"]["db"],
            "case {index} {} DB",
            case["name"]
        );
        same!(
            cached(&mut redis).await,
            case["expected"]["cache_before_resolve"],
            "case {index} {} signal",
            case["name"]
        );
        let resolved = reader.source_default_user_agent().await.unwrap();
        same!(
            json!(&resolved),
            case["expected"]["resolved_default"],
            "case {index} {} other-native-reader",
            case["name"]
        );
        same!(
            cached(&mut redis).await,
            case["expected"]["cache_after_resolve"],
            "case {index} {} filled cache",
            case["name"]
        );
        let image = client
            .get(format!(
                "http://127.0.0.1:5656/api/channels/logos/{}/cache/",
                fixture["logo_id"].as_i64().unwrap()
            ))
            .header("host", "localhost")
            .send()
            .await
            .unwrap();
        same!(
            image.status().as_u16(),
            case["expected"]["original_image_reader"]["status"]
                .as_u64()
                .unwrap() as u16,
            "case {index} original image status"
        );
        same!(
            format!("{:x}", Sha256::digest(image.bytes().await.unwrap())),
            case["expected"]["original_image_reader"]["sha256"]
                .as_str()
                .unwrap(),
            "case {index} actual original consumer of native cache"
        );
    }

    if failures.is_empty() {
        for case in fixture["default_id_cases"].as_array().unwrap() {
            sqlx::query("UPDATE core_coresettings SET value=jsonb_set(value,'{default_user_agent}',$1::jsonb) WHERE key='stream_settings'").bind(&case["raw"]).execute(store.pool()).await.unwrap();
            let _: i64 = redis::cmd("DEL")
                .arg(":1:coresettings:group:stream_settings")
                .arg(":1:coresettings:default_user_agent")
                .query_async(&mut redis)
                .await
                .unwrap();
            assert_eq!(
                reader.source_default_user_agent().await.unwrap(),
                case["expected"].as_str().unwrap(),
                "default User-Agent int conversion {}",
                case["raw"]
            );
        }
        sqlx::query("UPDATE core_coresettings SET value=jsonb_set(value,'{default_user_agent}','90001'::jsonb) WHERE key='stream_settings'").execute(store.pool()).await.unwrap();
        let _: i64 = redis
            .del(":1:coresettings:group:stream_settings")
            .await
            .unwrap();
        restore(&store, &fixture, &fixture["cases"][0]).await;
        sqlx::query("UPDATE core_useragent SET user_agent='Rust-Probe/race-old' WHERE id=90001")
            .execute(store.pool())
            .await
            .unwrap();
        let _: i64 = redis
            .del(":1:coresettings:default_user_agent")
            .await
            .unwrap();
        let _: () = redis
            .set(":1:coresettings:default_user_agent:ver", 123456)
            .await
            .unwrap();
        let (url, loaded, resume, proxy) = version_proxy().await;
        let gated = AuthService::new(
            store.clone(),
            AuthConfig::new("playback-fixture-only".into()).unwrap(),
        )
        .with_notification_cache(NotificationCache::connect(&url).await.unwrap());
        let read = tokio::spawn(async move { gated.source_default_user_agent().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(5), loaded)
            .await
            .unwrap()
            .unwrap();
        let token = fixture["cases"][0]["headers"]["Authorization"]
            .as_str()
            .unwrap();
        let writer = client
            .patch(format!("http://{address}/api/core/useragents/90001/"))
            .header("authorization", token)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&json!({"user_agent":"Rust-Probe/race-new"})).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(writer.status(), 200);
        drop(writer);
        resume.send(()).unwrap();
        assert_eq!(read.await.unwrap(), "Rust-Probe/race-old");
        let stale: Option<Vec<u8>> = redis
            .get(":1:coresettings:default_user_agent")
            .await
            .unwrap();
        assert!(stale.is_none(), "stale in-flight fill must be fenced");
        assert_eq!(
            reader.source_default_user_agent().await.unwrap(),
            "Rust-Probe/race-new"
        );
        proxy.abort();
        let mut lock = store.pool().begin().await.unwrap();
        sqlx::query("LOCK TABLE core_useragent IN SHARE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let mut requests = Vec::new();
        for n in 0..4 {
            let client = client.clone();
            let token = token.to_owned();
            requests.push(tokio::spawn(async move{client.post(format!("http://{address}/api/core/useragents/")).header("authorization",token).header("content-type","application/json").body(serde_json::to_vec(&json!({"name":format!("rust-probe-ua-canceled-{n}"),"user_agent":format!("Rust-Probe/canceled-{n}")})).unwrap()).send().await}));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO core_useragent(name,user_agent,%'").fetch_one(store.pool()).await.unwrap();
            if count == 4 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "four mutation actors did not reach SQL"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let rejected = client
            .post(format!("http://{address}/api/core/useragents/"))
            .header("authorization", token)
            .header("content-type", "application/json")
            .body(b"{}".to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), 503);
        drop(rejected);
        for request in requests {
            request.abort();
            let _ = request.await;
        }
        lock.commit().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM core_useragent WHERE name LIKE 'rust-probe-ua-canceled-%'",
            )
            .fetch_one(store.pool())
            .await
            .unwrap();
            if count == 4 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "abandoned committed work did not finish"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let recovery=client.post(format!("http://{address}/api/core/useragents/")).header("authorization",token).header("content-type","application/json").body(serde_json::to_vec(&json!({"name":"rust-probe-ua-recovery","user_agent":"Rust-Probe/recovery"})).unwrap()).send().await.unwrap();
            if recovery.status() == 201 {
                break;
            }
            assert_eq!(recovery.status(), 503);
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        println!("NATIVE_USER_AGENT_RESOURCES stale_fill_fenced=pass four_owned_mutations=pass overload503=pass abandoned_work_completes_and_recovers=pass");
    }
    server.abort();
    assert!(
        failures.is_empty(),
        "{} UserAgent source mismatches: {}",
        failures.len(),
        failures.join("; ")
    );
}
