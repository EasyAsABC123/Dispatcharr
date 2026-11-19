//! Canonical Original EPG Programme detail contract on owned migrated PostgreSQL.
//! Original Dispatcharr, Django and DRF authors retain full behavior credit.
use dispatcharr_store::{AuthConfig, AuthService, EpgProgramReadLimits, EpgProgramReads, Store};
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn token(id: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &json!({"token_type":"access","exp":now+300,"iat":now,
                "jti":uuid::Uuid::new_v4().to_string(),"user_id":id.to_string()}),
        &jsonwebtoken::EncodingKey::from_secret(b"playback-fixture-only"),
    )
    .unwrap()
}

async fn query_program(store: &Store, id: i64) -> Option<Value> {
    sqlx::query_scalar("SELECT to_jsonb(p) FROM epg_programdata p WHERE id=$1")
        .bind(id)
        .fetch_optional(store.pool())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and Original fixture admin/standard accounts"]
async fn canonical_programme_mutations_preserve_owned_guide_and_metadata() {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let actor: Vec<(String, i64, i32)> = sqlx::query_as(
        "SELECT username,id::bigint,user_level FROM accounts_user
         WHERE is_active AND username IN ('rust-probe-admin','rust-probe-standard')
         ORDER BY username",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(actor.len(), 2);
    assert_eq!(actor[0].2, 10);
    assert_eq!(actor[1].2, 1);
    let admin = token(actor[0].1);
    let standard = token(actor[1].1);
    let tag = format!("owned m5 programme {}", uuid::Uuid::new_v4());
    let epg: i64 = sqlx::query_scalar(
        "INSERT INTO epg_epgdata(name,tvg_id) VALUES($1,$1) RETURNING id::bigint",
    )
    .bind(&tag)
    .fetch_one(store.pool())
    .await
    .unwrap();
    let ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO epg_programdata(epg_id,start_time,end_time,title,sub_title,tvg_id,program_id,custom_properties)
         SELECT $1,'2040-01-05T03:00:00Z','2040-01-05T04:00:00Z',label,'retained subtitle',$2,
                'owned-catalogue-reference','{\"season\":3,\"new\":true}'::jsonb
         FROM unnest(ARRAY['target programme','sentinel programme']) AS programmes(label)
         RETURNING id::bigint",
    )
    .bind(epg)
    .bind(&tag)
    .fetch_all(store.pool())
    .await
    .unwrap();
    let target = ids[0];
    let sentinel = ids[1];
    let before = query_program(&store, target).await.unwrap();
    let peer = query_program(&store, sentinel).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let service = EpgProgramReads::new(auth.clone(), EpgProgramReadLimits::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = service.clone().router();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
        .unwrap()
    });
    let client = reqwest::Client::new();
    let endpoint = format!("http://{address}/api/epg/programs/{target}/");
    let patch = |authorization: Option<&str>| {
        let request = client
            .patch(&endpoint)
            .header("Content-Type", "application/json")
            .body(
                json!({"description":"first revision","epg":0,"custom_properties":{}}).to_string(),
            );
        match authorization {
            Some(authorization) => request.bearer_auth(authorization),
            None => request,
        }
    };
    assert_eq!(patch(None).send().await.unwrap().status(), 401);
    assert_eq!(patch(Some(&standard)).send().await.unwrap().status(), 403);
    assert_eq!(query_program(&store, target).await.unwrap(), before);

    let response = patch(Some(&admin)).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(value["description"], "first revision");
    assert_eq!(value["season"], 3);
    assert_eq!(value["is_new"], true);
    assert!(value.get("poster_url").is_none());
    let after = query_program(&store, target).await.unwrap();
    assert_eq!(after["epg_id"], epg);
    assert_eq!(after["program_id"], before["program_id"]);
    assert_eq!(after["custom_properties"], before["custom_properties"]);

    let response = client
        .put(&endpoint)
        .bearer_auth(&admin)
        .header("Content-Type", "application/json")
        .body(r#"{"title":"unfinished"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(
        value,
        json!({"start_time":["This field is required."],
                            "end_time":["This field is required."]})
    );
    assert_eq!(query_program(&store, target).await.unwrap(), after);

    let response = client
        .put(&endpoint)
        .bearer_auth(&admin)
        .header("Content-Type", "application/json")
        .body(
            json!({"title":"second revision", "description":null,
                     "start_time":"2040-01-05T05:00:00Z","end_time":"2040-01-05T06:00:00Z"})
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(value["sub_title"], "retained subtitle");
    assert_eq!(value["title"], "second revision");
    assert!(value["description"].is_null());
    let after = query_program(&store, target).await.unwrap();
    assert_eq!(after["epg_id"], epg);
    assert_eq!(after["custom_properties"], before["custom_properties"]);

    assert_eq!(
        client
            .delete(&endpoint)
            .bearer_auth(&standard)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let response = client
        .delete(&endpoint)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    assert!(response.bytes().await.unwrap().is_empty());
    assert!(query_program(&store, target).await.is_none());
    assert_eq!(query_program(&store, sentinel).await.unwrap(), peer);
    assert_eq!(
        client
            .patch(&endpoint)
            .bearer_auth(&admin)
            .header("Content-Type", "application/json")
            .body(r#"{"start_time": null}"#)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );

    sqlx::query("DELETE FROM epg_programdata WHERE id=$1")
        .bind(sentinel)
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM epg_epgdata WHERE id=$1")
        .bind(epg)
        .execute(store.pool())
        .await
        .unwrap();
    stop.send(()).unwrap();
    server.await.unwrap();
    assert!(service.shutdown(Duration::from_secs(3)).await);
}
