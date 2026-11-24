//! Source canonical PUT, independent of the React PATCH caller.
use super::*;
use sqlx::Row;
use std::collections::BTreeMap;

fn put(url: &str, body: Value, bearer: Option<&str>) -> Request<Body> {
    let mut r = patch(url, body, bearer);
    *r.method_mut() = axum::http::Method::PUT;
    r
}

// Survey actual SQLite storage types and quoted values for every captured table.
// Only the two expected Channel scalar changes are excluded. This is unit data,
// not a synthetic migration history or an admitted Source donor.
async fn unrelated(pool: &SqlitePool) -> BTreeMap<String, Vec<Vec<String>>> {
    fn identifier(s: &str) -> String {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
    let tables: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .fetch_all(pool)
            .await
            .unwrap();
    assert_eq!(tables.len(), 63);
    let mut result = BTreeMap::new();
    for table in tables {
        let columns = sqlx::query(&format!("PRAGMA table_info({})", identifier(&table)))
            .fetch_all(pool)
            .await
            .unwrap();
        let columns: Vec<String> = columns
            .iter()
            .map(|row| row.try_get::<String, _>("name").unwrap())
            .filter(|name| {
                table != "dispatcharr_channels_channel"
                    || !matches!(name.as_str(), "name" | "updated_at")
            })
            .collect();
        let select = columns
            .iter()
            .map(|name| {
                let name = identifier(name);
                format!("typeof({name})||':'||quote({name})")
            })
            .collect::<Vec<_>>()
            .join(",");
        let rows = sqlx::query(&format!("SELECT {select} FROM {}", identifier(&table)))
            .fetch_all(pool)
            .await
            .unwrap();
        let mut values: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                (0..columns.len())
                    .map(|index| row.try_get::<String, _>(index).unwrap())
                    .collect()
            })
            .collect();
        values.sort();
        result.insert(table, values);
    }
    result
}

#[tokio::test]
async fn sqlite_channel_name_put_requires_name_before_any_write() {
    let (path, owner, capture, admin, id) = fixture().await;
    let url = format!("/api/channels/channels/{id}/");
    let before = state(&owner, id).await;
    let before_db = unrelated(&owner.pool).await;
    let changes: i64 = sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(&owner.pool)
        .await
        .unwrap();
    for body in [json!({}), json!({"id":id})] {
        let response = router(owner.clone())
            .oneshot(put(&url, body.clone(), Some(&admin)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let raw = to_bytes(response.into_body(), 262144).await.unwrap();
        assert_eq!(raw.as_ref(), b"{\"name\":[\"This field is required.\"]}");
        println!(
            "PUT_REQUIRED_WIRE {}",
            json!({"method":"PUT","path":url,"request_body":body,"status":400,"body_utf8":String::from_utf8(raw.to_vec()).unwrap()})
        );
    }
    // This is an explicit non-partial PUT rule, not a PATCH alias.
    assert_eq!(
        router(owner.clone())
            .oneshot(patch(&url, json!({}), Some(&admin)))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    // Auth and object lookup still precede required-name validation.
    let standard = token(&owner, 2);
    for (bearer, status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(standard.as_str()), StatusCode::FORBIDDEN),
    ] {
        assert_eq!(
            router(owner.clone())
                .oneshot(put(&url, json!({}), bearer))
                .await
                .unwrap()
                .status(),
            status
        );
    }
    let missing = value(
        router(owner.clone())
            .oneshot(put("/api/channels/channels/999/", json!({}), Some(&admin)))
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
    assert_eq!(unrelated(&owner.pool).await, before_db);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT total_changes()")
            .fetch_one(&owner.pool)
            .await
            .unwrap(),
        changes
    );
    assert_eq!(capture.messages.lock().unwrap().len(), 1);
    assert_eq!(owner.slots.available_permits(), 1);
    close(path, owner).await;
}

#[tokio::test]
async fn sqlite_channel_name_put_signed_name_only_retains_all_omitted_storage_and_reopens() {
    let (path, owner, capture, admin, id) = fixture().await;
    let url = format!("/api/channels/channels/{id}/");
    let before = router(owner.clone())
        .oneshot(request(&url, None, Some(&admin)))
        .await
        .unwrap();
    assert_eq!(before.status(), StatusCode::OK);
    let before = to_bytes(before.into_body(), 262144).await.unwrap();
    let before_db = unrelated(&owner.pool).await;
    let changes: i64 = sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(&owner.pool)
        .await
        .unwrap();
    // Only name is supplied; id, flags, numbers, nullable values and relations
    // must not reset to serializer/model defaults when partial=False.
    let body = json!({"name":"  Canonical PUT renamed  "});
    let response = router(owner.clone())
        .oneshot(put(&url, body.clone(), Some(&admin)))
        .await
        .unwrap();
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
    let raw = to_bytes(response.into_body(), 262144).await.unwrap();
    assert_eq!(declared, raw.len());
    let original = String::from_utf8(before.to_vec()).unwrap();
    assert_eq!(original.matches("\"Unit selected stream\"").count(), 2);
    let expected = original.replace("\"Unit selected stream\"", "\"Canonical PUT renamed\"");
    assert_eq!(raw.as_ref(), expected.as_bytes());
    assert_eq!(unrelated(&owner.pool).await, before_db);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT total_changes()")
            .fetch_one(&owner.pool)
            .await
            .unwrap(),
        changes + 1
    );
    let fresh = router(owner.clone())
        .oneshot(request(&url, None, Some(&admin)))
        .await
        .unwrap();
    assert_eq!(to_bytes(fresh.into_body(), 262144).await.unwrap(), raw);
    assert_eq!(
        capture.messages.lock().unwrap().len(),
        1,
        "no invented PUT WS event"
    );
    assert_eq!(owner.slots.available_permits(), 1);
    println!(
        "PUT_SUCCESS_WIRE {}",
        json!({"method":"PUT","path":url,"request_body":body,"status":200,"content_type":"application/json","allow":"GET, PUT, PATCH, DELETE, HEAD, OPTIONS","vary":"Accept, origin","declared_length":declared,"before_body_utf8":original,"body_utf8":String::from_utf8(raw.to_vec()).unwrap(),"expected_body_utf8":expected,"unrelated_tables_compared":before_db.len(),"SQL_changes_delta":1,"scope":"actual signed component route on disposable file; no new Original or Main execution"})
    );
    owner.pool.close().await;
    let reopened = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    assert_eq!(unrelated(&reopened).await, before_db);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT name FROM dispatcharr_channels_channel WHERE id=?")
            .bind(id)
            .fetch_one(&reopened)
            .await
            .unwrap(),
        "Canonical PUT renamed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(&reopened)
            .await
            .unwrap(),
        "ok"
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&reopened)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM django_migrations")
            .fetch_one(&reopened)
            .await
            .unwrap(),
        0
    );
    reopened.close().await;
    close(path, owner).await;
}
