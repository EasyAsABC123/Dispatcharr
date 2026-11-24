//! Actual disposable-file regression for the ordinary React name PATCH.
use super::*;
#[tokio::test]
async fn sqlite_channel_name_patch_signed_file_durable_exact_bytes() {
    let path =
        std::env::temp_dir().join(format!("dispatcharr-name-unit-{}.db", uuid::Uuid::new_v4()));
    let capture = Arc::new(Capture::default());
    let owner = owner(Some(&path), Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    let created = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1,"channel_profile_ids":[1]})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    let id = created["id"].as_i64().unwrap();
    let url = format!("/api/channels/channels/{id}/");
    let before = router(owner.clone())
        .oneshot(request(&url, None, Some(&bearer)))
        .await
        .unwrap();
    let before = to_bytes(before.into_body(), 262144).await.unwrap();
    let counts_before = counts(&owner.pool).await;
    let mut req = request(
        &url,
        Some(json!({"id":id,"name":"  React renamed channel  "})),
        Some(&bearer),
    );
    *req.method_mut() = axum::http::Method::PATCH;
    let response = router(owner.clone()).oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[axum::http::header::ALLOW],
        "GET, PUT, PATCH, DELETE, HEAD, OPTIONS"
    );
    assert_eq!(
        response.headers()[axum::http::header::CONTENT_TYPE],
        "application/json"
    );
    assert_eq!(
        response.headers()[axum::http::header::VARY],
        "Accept, origin"
    );
    let declared = response.headers()[axum::http::header::CONTENT_LENGTH]
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let actual = to_bytes(response.into_body(), 262144).await.unwrap();
    assert_eq!(declared, actual.len());
    let expected = String::from_utf8(before.to_vec())
        .unwrap()
        .replace("Unit selected stream", "React renamed channel");
    assert_eq!(actual.as_ref(), expected.as_bytes());
    println!(
        "NAME_PATCH_WIRE {}",
        json!({"method":"PATCH","path":url,"request_body":{"id":id,"name":"  React renamed channel  "},"status":200,"content_type":"application/json","allow":"GET, PUT, PATCH, DELETE, HEAD, OPTIONS","vary":"Accept, origin","declared_length":declared,"before_body_utf8":String::from_utf8(before.to_vec()).unwrap(),"body_utf8":String::from_utf8(actual.to_vec()).unwrap(),"expected_body_utf8":expected,"scope":"actual signed component route on disposable file; no new Original or Main execution"})
    );

    assert_eq!(counts(&owner.pool).await, counts_before);
    assert_eq!(
        capture.messages.lock().unwrap().len(),
        1,
        "rename sends no invented creation/update event"
    );
    assert_eq!(owner.slots.available_permits(), 1);
    owner.pool.close().await;
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT name FROM dispatcharr_channels_channel WHERE id=?")
            .bind(id)
            .fetch_one(&mut db)
            .await
            .unwrap(),
        "React renamed channel"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT stream_id,\"order\" FROM dispatcharr_channels_channelstream WHERE channel_id=?"
        )
        .bind(id)
        .fetch_all(&mut db)
        .await
        .unwrap(),
        vec![(1, 0)]
    );
    assert_eq!(sqlx::query_as::<_,(i64,bool)>("SELECT channel_profile_id,enabled FROM dispatcharr_channels_channelprofilemembership WHERE channel_id=?").bind(id).fetch_all(&mut db).await.unwrap(),vec![(1,true)]);
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(&mut db)
            .await
            .unwrap(),
        "ok"
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut db)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM django_migrations")
            .fetch_one(&mut db)
            .await
            .unwrap(),
        0
    );
    db.close().await.unwrap();
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(!std::path::Path::new(&format!("{}{suffix}", path.display())).exists());
    }
    std::fs::remove_file(path).unwrap();
}

async fn fixture() -> (std::path::PathBuf, TestOwner, Arc<Capture>, String, i64) {
    let path =
        std::env::temp_dir().join(format!("dispatcharr-name-unit-{}.db", uuid::Uuid::new_v4()));
    let capture = Arc::new(Capture::default());
    let owner = owner(Some(&path), Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    let created = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1,"channel_profile_ids":[1]})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    (
        path,
        owner,
        capture,
        bearer,
        created["id"].as_i64().unwrap(),
    )
}
fn patch(url: &str, body: Value, bearer: Option<&str>) -> Request<Body> {
    let mut r = request(url, Some(body), bearer);
    *r.method_mut() = axum::http::Method::PATCH;
    r
}
async fn close(path: std::path::PathBuf, owner: TestOwner) {
    owner.pool.close().await;
    for s in ["-wal", "-shm", "-journal"] {
        assert!(!std::path::Path::new(&format!("{}{s}", path.display())).exists());
    }
    std::fs::remove_file(path).unwrap();
}
async fn state(owner: &TestOwner, id: i64) -> (String, String) {
    sqlx::query_as("SELECT name,updated_at FROM dispatcharr_channels_channel WHERE id=?")
        .bind(id)
        .fetch_one(&owner.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn sqlite_channel_name_patch_signed_auth_network_denials_do_not_write() {
    let (path, owner, capture, admin, id) = fixture().await;
    let url = format!("/api/channels/channels/{id}/");
    let before = state(&owner, id).await;
    let wrong = crate::auth::mint_token(
        &AuthConfig::new("different-offline-signature".into()).unwrap(),
        json!(1),
        "access",
    )
    .unwrap();
    let inactive = token(&owner, 3);
    let standard = token(&owner, 2);
    for (bearer, status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(wrong.as_str()), StatusCode::UNAUTHORIZED),
        (Some(inactive.as_str()), StatusCode::UNAUTHORIZED),
        (Some(standard.as_str()), StatusCode::FORBIDDEN),
    ] {
        assert_eq!(
            router(owner.clone())
                .oneshot(patch(
                    &url,
                    json!({"id":id,"name":"must not write"}),
                    bearer
                ))
                .await
                .unwrap()
                .status(),
            status
        );
    }
    sqlx::query(
        "INSERT INTO core_coresettings(key,name,value) VALUES('network_access','unit-policy',?)",
    )
    .bind(r#"{"UI":"198.51.100.0/24"}"#)
    .execute(&owner.pool)
    .await
    .unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(patch(&url, json!({"name":"blocked"}), Some(&admin)))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(state(&owner, id).await, before);
    assert_eq!(capture.messages.lock().unwrap().len(), 1);
    assert_eq!(owner.slots.available_permits(), 1);
    close(path, owner).await;
}

#[tokio::test]
async fn sqlite_channel_name_patch_exact_validation_and_unsupported_keep_file_unchanged() {
    let (path, owner, _, admin, id) = fixture().await;
    let url = format!("/api/channels/channels/{id}/");
    let before = state(&owner, id).await;
    for (name, message) in [
        (" ".to_owned(), "This field may not be blank."),
        (
            "x".repeat(513),
            "Ensure this field has no more than 512 characters.",
        ),
        ("bad\0name".to_owned(), "Null characters are not allowed."),
    ] {
        let response = router(owner.clone())
            .oneshot(patch(&url, json!({"name":name}), Some(&admin)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), 262144).await.unwrap();
        assert_eq!(
            bytes.as_ref(),
            format!("{{\"name\":[\"{message}\"]}}").as_bytes()
        );
    }
    for body in [
        json!({}),
        json!({"name":3}),
        json!({"name":null}),
        json!({"name":"\u{1c}"}),
        json!({"name":"safe","streams":[1]}),
        json!({"name":"safe","override":null}),
    ] {
        assert_eq!(
            router(owner.clone())
                .oneshot(patch(&url, body, Some(&admin)))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_IMPLEMENTED
        );
    }
    for suffix in ["?name=Unit", "?format=api"] {
        assert_eq!(
            router(owner.clone())
                .oneshot(patch(
                    &(url.clone() + suffix),
                    json!({"name":"safe"}),
                    Some(&admin)
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_IMPLEMENTED
        );
    }
    let mut put = patch(&url, json!({"name":"not ported PUT"}), Some(&admin));
    *put.method_mut() = axum::http::Method::PUT;
    assert_eq!(
        router(owner.clone()).oneshot(put).await.unwrap().status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    let missing = value(
        router(owner.clone())
            .oneshot(patch(
                "/api/channels/channels/999/",
                json!({"name":"missing"}),
                Some(&admin),
            ))
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(
        missing,
        json!({"detail":"No Channel matches the given query."})
    );
    assert_eq!(state(&owner, id).await, before);
    assert_eq!(owner.slots.available_permits(), 1);
    close(path, owner).await;
}

#[tokio::test]
async fn sqlite_channel_name_patch_post_update_projection_failure_rolls_back_and_recovers() {
    let (path, owner, _, admin, id) = fixture().await;
    let url = format!("/api/channels/channels/{id}/");
    let uuid: String =
        sqlx::query_scalar("SELECT uuid FROM dispatcharr_channels_channel WHERE id=?")
            .bind(id)
            .fetch_one(&owner.pool)
            .await
            .unwrap();
    // Deliberate negative control, not an admitted Source donor or successful migration.
    sqlx::query("UPDATE dispatcharr_channels_channel SET uuid='invalid-unit-uuid' WHERE id=?")
        .bind(id)
        .execute(&owner.pool)
        .await
        .unwrap();
    let before = state(&owner, id).await;
    assert_eq!(
        router(owner.clone())
            .oneshot(patch(&url, json!({"name":"rollback this"}), Some(&admin)))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(state(&owner, id).await, before);
    sqlx::query("UPDATE dispatcharr_channels_channel SET uuid=? WHERE id=?")
        .bind(uuid)
        .bind(id)
        .execute(&owner.pool)
        .await
        .unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(patch(
                &url,
                json!({"id":"readonly ignored","name":"recovered"}),
                Some(&admin)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(state(&owner, id).await.0, "recovered");
    assert_eq!(owner.slots.available_permits(), 1);
    close(path, owner).await;
}

#[tokio::test]
async fn sqlite_channel_name_patch_owned_scope_fences_precede_dml_and_unicode_name_keeps_type() {
    let (path, owner, _, admin, id) = fixture().await;
    let url = format!("/api/channels/channels/{id}/");
    let before = state(&owner, id).await;
    sqlx::query("UPDATE dispatcharr_channels_channel SET auto_created=true WHERE id=?")
        .bind(id)
        .execute(&owner.pool)
        .await
        .unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(patch(
                &url,
                json!({"name":"unsupported auto"}),
                Some(&admin)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(state(&owner, id).await, before);
    sqlx::query("UPDATE dispatcharr_channels_channel SET auto_created=false WHERE id=?")
        .bind(id)
        .execute(&owner.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM dispatcharr_channels_channelgroupm3uaccount")
        .execute(&owner.pool)
        .await
        .unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(patch(&url, json!({"name":"unclaimed group"}), Some(&admin)))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(state(&owner, id).await, before);
    let group: i64 =
        sqlx::query_scalar("SELECT channel_group_id FROM dispatcharr_channels_channel WHERE id=?")
            .bind(id)
            .fetch_one(&owner.pool)
            .await
            .unwrap();
    value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&admin),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM dispatcharr_channels_channelgroupm3uaccount WHERE channel_group_id=?").bind(group).fetch_one(&owner.pool).await.unwrap(),1);
    let name = "é".repeat(512);
    let result = value(
        router(owner.clone())
            .oneshot(patch(&url, json!({"name":name.clone()}), Some(&admin)))
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(result["name"], name);
    assert!(result["name"].is_string());
    assert_eq!(result["effective_name"], result["name"]);
    close(path, owner).await;
}
