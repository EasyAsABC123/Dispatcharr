use super::*;
fn repo(manifest: Value) -> Value {
    json!({"id":801,"name":"Alpha","url":"https://alpha.invalid/manifest.json","is_official":false,"enabled":true,"cached_manifest":manifest,"public_key":"","signature_verified":null,"last_fetched":null,"last_fetch_status":"","created_at":"2026-11-13T13:00:00.123456+00:00","updated_at":"2026-11-13T13:00:00+00:00"})
}
#[test]
fn readonly_fields_do_not_become_writable() {
    assert_eq!(changes(&json!({"id":999,"name":"changed","url":"https://other.invalid/manifest.json","is_official":true,"signature_verified":false,"updated_at":"2099-01-01","cached_manifest":{},"last_fetch_status":"bad"})).unwrap(),Changes{public_key:None,enabled:None});
}
#[test]
fn empty_put_has_no_field_replacements() {
    assert_eq!(
        changes(&json!({})).unwrap(),
        Changes {
            public_key: None,
            enabled: None
        }
    );
    assert!(UPDATE_SQL.contains("updated_at=$4"));
}
#[test]
fn public_key_trims_and_permits_blank() {
    assert_eq!(
        changes(&json!({"public_key":" \nKEY\t "}))
            .unwrap()
            .public_key,
        Some("KEY".into())
    );
    assert_eq!(
        changes(&json!({"public_key":"  "})).unwrap().public_key,
        Some("".into())
    );
}
#[test]
fn public_key_null_and_complex_are_errors() {
    for v in [Value::Null, json!({}), json!([]), json!(true)] {
        assert!(changes(&json!({"public_key":v})).is_err());
    }
}
#[test]
fn serializer_boolean_variants() {
    for v in [json!(true), json!(1), json!(1.0), json!("yes"), json!("ON")] {
        assert_eq!(changes(&json!({"enabled":v})).unwrap().enabled, Some(true));
    }
    for v in [json!(false), json!(0), json!("off"), json!("False")] {
        assert_eq!(changes(&json!({"enabled":v})).unwrap().enabled, Some(false));
    }
}
#[test]
fn invalid_and_null_boolean_refused() {
    for v in [Value::Null, json!("maybe"), json!(2), json!({}), json!([])] {
        assert!(changes(&json!({"enabled":v})).is_err());
    }
}
#[test]
fn list_body_is_explicit_native_framework_gap() {
    assert!(matches!(
        changes(&json!([])),
        Err(StoreError::StatusBody(500, _))
    ));
}
#[test]
fn nested_and_bare_manifests_match_derived_fields() {
    for m in [
        json!({"manifest":{"registry_url":"https://registry.invalid/","plugins":[{},{}]}}),
        json!({"registry_url":"https://registry.invalid/","plugins":[{},{}]}),
    ] {
        let v = serialized(&repo(m)).unwrap();
        assert_eq!(v["registry_url"], "https://registry.invalid/");
        assert_eq!(v["plugin_count"], 2);
        assert!(v.get("cached_manifest").is_none());
        assert_eq!(v.as_object().unwrap().len(), 13);
    }
}
#[test]
fn empty_manifest_and_nonlist_plugins() {
    for m in [json!({}), Value::Null, json!({"plugins":{}})] {
        let v = serialized(&repo(m)).unwrap();
        assert_eq!(v["plugin_count"], 0);
        assert_eq!(v["registry_url"], "");
    }
}
#[test]
fn serialization_clocks_retain_actual_precision() {
    let v = serialized(&repo(json!({}))).unwrap();
    assert_eq!(v["created_at"], "2026-11-13T13:00:00.123456Z");
    assert_eq!(v["updated_at"], "2026-11-13T13:00:00Z");
    assert_eq!(v["last_fetched"], Value::Null);
}
#[test]
fn malformed_json_has_source_parser_text() {
    let Err(StoreError::InvalidInput(v)) = parsed(b"{") else {
        panic!("must refuse")
    };
    assert!(v["detail"]
        .as_str()
        .unwrap()
        .starts_with("JSON parse error - Expecting property name enclosed in double quotes"));
}
#[test]
fn body_capacity_and_structural_preflight() {
    assert!(parsed(&vec![b' '; RAW_LIMIT + 1]).is_err());
    let raw = format!("{}0{}", "[".repeat(33), "]".repeat(33));
    assert!(parsed(raw.as_bytes()).is_err());
}
#[test]
fn sql_payload_cap_is_not_null_or_untyped() {
    assert!(decoded(1, None).is_err());
    assert!(decoded(-1, Some("{}".into())).is_err());
    assert!(decoded(2, Some("[]".into())).is_ok());
    assert!(decoded(1, Some("[]".into())).is_err());
}
#[test]
fn json_body_empty_is_empty_dictionary() {
    assert_eq!(parsed(b"").unwrap(), json!({}));
    assert_eq!(parsed(b"[]").unwrap(), json!([]));
}

// Pure ownership regression uses the actual AuthService four-slot budget.
#[tokio::test]
async fn original_writer_is_retained_by_success_error_and_polled_frame() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1/unused")
        .unwrap();
    let auth = AuthService::new(
        crate::Store::from_pool(pool),
        crate::AuthConfig::new("owned-pure-pluginrepo-signing-key-only".into()).unwrap(),
    );
    for error in [false, true] {
        let permit = Arc::new(auth.user_agent_workers.clone().try_acquire_owned().unwrap());
        let weak = Arc::downgrade(&permit);
        let result = if error {
            Err(StoreError::PermissionDenied)
        } else {
            Ok(Json(json!({"success":true})).into_response())
        };
        let response = retained_response(result, permit).await;
        assert!(weak.upgrade().is_some());
        assert_eq!(auth.user_agent_workers.available_permits(), 3);
        let bytes = to_bytes(response.into_body(), RESPONSE_LIMIT)
            .await
            .unwrap();
        assert!(weak.upgrade().is_some());
        assert_eq!(auth.user_agent_workers.available_permits(), 3);
        let cloned = bytes.clone();
        drop(bytes);
        assert!(weak.upgrade().is_some());
        drop(cloned);
        assert!(weak.upgrade().is_none());
        assert_eq!(auth.user_agent_workers.available_permits(), 4);
    }
}
#[tokio::test]
async fn oversized_response_refusal_retains_the_same_original_writer() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1/unused")
        .unwrap();
    let auth = AuthService::new(
        crate::Store::from_pool(pool),
        crate::AuthConfig::new("owned-pure-pluginrepo-signing-key-only".into()).unwrap(),
    );
    let workers = auth.user_agent_workers.clone();
    let permit = Arc::new(workers.clone().try_acquire_owned().unwrap());
    let weak = Arc::downgrade(&permit);
    let response =
        retained_response(Ok(vec![0u8; RESPONSE_LIMIT + 1].into_response()), permit).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
    assert!(weak.upgrade().is_some());
    assert_eq!(workers.available_permits(), 3);
    drop(bytes);
    assert!(weak.upgrade().is_none());
}

#[path = "plugin_repositories_sql_tests.rs"]
mod sql_tests;
