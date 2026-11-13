use super::*;
fn scalar() -> UserScalars {
    UserScalars {
        id: 101,
        username: "owned".into(),
        password: "!unusable".into(),
        is_active: true,
        user_level: 10,
        stream_limit: 0,
    }
}
#[test]
fn valid_map_preserves_nested_exact_types() {
    let raw = r#"{"a":[true,1,null],"allowed_networks":{"UI":"127.0.0.0/8"}}"#;
    let value = decode_user(
        scalar(),
        raw.len() as i32,
        Some(raw),
        RestReadLimits::default(),
    )
    .unwrap();
    assert!(value.retained_bytes > 128 * 1024);
    assert_eq!(value.user.custom_properties["a"], json!([true, 1, null]));
}
#[test]
fn sql_null_user_properties_coalesce_but_json_null_is_preserved() {
    let empty = decode_user(scalar(), 2, Some("{}"), RestReadLimits::default()).unwrap();
    assert_eq!(empty.user.custom_properties, json!({}));
    let null = decode_user(scalar(), 4, Some("null"), RestReadLimits::default()).unwrap();
    assert_eq!(null.user.custom_properties, Value::Null);
}
#[test]
fn duplicate_row_count_precedes_property_decode() {
    assert!(!classify_count(0).unwrap());
    assert!(!classify_count(2).unwrap());
    assert!(classify_count(1).unwrap());
    assert!(classify_count(3).is_err());
}
#[test]
fn missing_or_oversized_properties_refuse_before_decode() {
    assert!(properties(65537, None, 0, RestReadLimits::default()).is_err());
    assert!(properties(2, None, 0, RestReadLimits::default()).is_err());
    assert!(properties(-1, Some("{}"), 0, RestReadLimits::default()).is_err());
    assert!(properties(1, Some("{}"), 0, RestReadLimits::default()).is_err());
}
#[test]
fn scalar_bytes_are_charged() {
    assert!(properties(2, Some("{}"), 2049, RestReadLimits::default()).is_err());
    assert!(properties(2, Some("{}"), 2048, RestReadLimits::default()).is_ok());
}
#[test]
fn dense_node_quote_and_work_caps_refuse() {
    let raw = format!("[{}]", vec!["0"; 2050].join(","));
    assert!(properties(raw.len() as i32, Some(&raw), 0, RestReadLimits::default()).is_err());
    let limits = RestReadLimits {
        work_bytes: 128 * 1024,
        ..RestReadLimits::default()
    };
    assert!(properties(2, Some("{}"), 0, limits).is_err());
}
#[test]
fn excessive_depth_refused() {
    let raw = format!("{}0{}", "[".repeat(33), "]".repeat(33));
    assert!(properties(raw.len() as i32, Some(&raw), 0, RestReadLimits::default()).is_err());
}
#[test]
fn private_raw_value_amplification_refused() {
    let raw = r#"{"$serde_json::private::RawValue":"[1,2,3]"}"#;
    assert!(properties(raw.len() as i32, Some(raw), 0, RestReadLimits::default()).is_err());
}
#[test]
fn network_missing_empty_nonobject_default_is_empty() {
    for raw in ["null", "[]", "false", "{}"] {
        assert_eq!(
            network_value(
                Some(raw.len() as i32),
                Some(raw),
                false,
                RestReadLimits::default()
            )
            .unwrap(),
            json!({})
        );
    }
}
#[test]
fn nonempty_network_map_is_not_overlaid() {
    let raw = r#"{"UI":"127.0.0.0/8"}"#;
    assert_eq!(
        network_value(
            Some(raw.len() as i32),
            Some(raw),
            false,
            RestReadLimits::default()
        )
        .unwrap(),
        json!({"UI":"127.0.0.0/8"})
    );
}
#[test]
fn forbidden_sql_null_group_is_database_failure() {
    assert!(matches!(
        network_value(None, None, true, RestReadLimits::default()),
        Err(StoreError::StatusBody(503, _))
    ));
}
#[test]
fn configured_limits_cannot_expand_existing_scope() {
    for limits in [
        RestReadLimits {
            max_raw_bytes: 65537,
            ..RestReadLimits::default()
        },
        RestReadLimits {
            max_nodes: 2049,
            ..RestReadLimits::default()
        },
        RestReadLimits {
            max_depth: 33,
            ..RestReadLimits::default()
        },
        RestReadLimits {
            work_bytes: 4 * 1024 * 1024 + 1,
            ..RestReadLimits::default()
        },
    ] {
        assert!(limits.validated().is_err());
    }
}
#[tokio::test]
async fn missing_real_writer_refuses_without_network_or_new_admission() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    let auth = AuthService::new(
        crate::Store::from_pool(pool),
        crate::AuthConfig::new("owned-rest-read-pure-signing-key-only".into()).unwrap(),
    );
    assert!(auth.retained_rest_reads(RestReadLimits::default()).is_err());
    assert_eq!(auth.user_agent_workers.available_permits(), 4);
    assert_eq!(auth.store.pool().size(), 0);
}
#[tokio::test]
async fn real_writer_reused_and_foreign_writer_refused() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    let auth = AuthService::new(
        crate::Store::from_pool(pool),
        crate::AuthConfig::new("owned-rest-read-pure-signing-key-only".into()).unwrap(),
    );
    let original = Arc::new(auth.user_agent_workers.clone().try_acquire_owned().unwrap());
    let weak = Arc::downgrade(&original);
    let scoped = auth.clone().with_request_writer_permit(original).unwrap();
    let reads = scoped
        .retained_rest_reads(RestReadLimits::default())
        .unwrap();
    assert!(weak.upgrade().is_some());
    assert_eq!(auth.user_agent_workers.available_permits(), 3);
    drop(reads);
    drop(scoped);
    assert!(weak.upgrade().is_none());
    assert_eq!(auth.user_agent_workers.available_permits(), 4);
    let foreign = Arc::new(
        Arc::new(tokio::sync::Semaphore::new(4))
            .try_acquire_owned()
            .unwrap(),
    );
    assert!(auth.with_request_writer_permit(foreign).is_err());
}

#[tokio::test]
async fn oversized_header_and_malformed_bearer_do_not_open_sql() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    let auth = AuthService::new(
        crate::Store::from_pool(pool),
        crate::AuthConfig::new("owned-rest-read-pure-signing-key-only".into()).unwrap(),
    );
    let token = Arc::new(auth.user_agent_workers.clone().try_acquire_owned().unwrap());
    let scoped = auth.clone().with_request_writer_permit(token).unwrap();
    let reads = scoped
        .retained_rest_reads(RestReadLimits::default())
        .unwrap();
    let mut request = PlaybackRequest {
        channel_id: String::new(),
        query: vec![],
        headers: vec![("x-api-key".into(), "x".repeat(8193))],
        peer_ip: "127.0.0.1".parse().unwrap(),
    };
    assert!(matches!(
        reads.authenticate(&request).await,
        Err(StoreError::StatusBody(503, _))
    ));
    request.headers = vec![("authorization".into(), "Bearer too many values".into())];
    assert!(matches!(
        reads.authenticate(&request).await,
        Err(StoreError::AuthenticationBody(_))
    ));
    assert_eq!(auth.store.pool().size(), 0);
    assert_eq!(auth.user_agent_workers.available_permits(), 3);
}

#[tokio::test]
async fn aborting_sql_observer_keeps_actual_original_slot_until_work_finishes() {
    let workers = Arc::new(tokio::sync::Semaphore::new(4));
    let original = Arc::new(workers.clone().try_acquire_owned().unwrap());
    let weak = Arc::downgrade(&original);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let observer = tokio::spawn(supervised_sql_read(original, async move {
        started_tx.send(()).unwrap();
        release_rx.await.unwrap();
        Ok(())
    }));
    started_rx.await.unwrap();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert!(weak.upgrade().is_some());
    assert_eq!(workers.available_permits(), 3);
    release_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(workers.available_permits(), 4);
}
