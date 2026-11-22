//! Copied SQLite access to the shared Original environment lookup.
use super::*;
use crate::startup::{environment_body, ip_lookup_policy};

fn source_field_order(body: Value) -> Value {
    let Value::Object(mut body) = body else {
        return body;
    };
    let fields = [
        "authenticated",
        "public_ip",
        "local_ip",
        "country_code",
        "country_name",
        "city",
        "ip_lookup_enabled",
        "ip_lookup_env_disabled",
        "ip_lookup_pending",
        "env_mode",
        "redis_tls",
        "postgres_tls",
    ];
    Value::Object(
        fields
            .into_iter()
            .filter_map(|name| body.remove(name).map(|value| (name.to_owned(), value)))
            .collect(),
    )
}

pub(super) async fn get(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let reply = async {
        let held = runtime.allocate()?;
        let _subject = held.bearer_account_user(peer, headers, false).await?;
        let system = held.environment_system_settings().await?;
        let (enabled, disabled) = ip_lookup_policy(&system);
        let result = runtime
            .ip_lookup
            .environment(
                enabled,
                Some(&runtime.cache),
                held.environment_request_cache(),
                runtime.ip_event_sink.as_ref(),
            )
            .await?;
        Ok::<_, StoreError>(Json(source_field_order(environment_body(result, disabled))))
    }
    .await;
    copied_read_headers(
        reply.into_response(),
        axum::http::HeaderValue::from_static("GET, OPTIONS"),
    )
}

#[cfg(test)]
#[test]
fn copied_env_uses_source_order_and_method_headers() {
    let payload = json!({
        "public_ip": null,
        "local_ip": null,
        "country_code": null,
        "country_name": null,
        "city": null,
        "ip_lookup_enabled": false,
        "ip_lookup_pending": false,
        "authenticated": true,
        "ip_lookup_env_disabled": true,
        "env_mode": "aio",
        "redis_tls": {"enabled":false,"verify":false,"mtls":false},
        "postgres_tls": {"enabled":false,"ssl_mode":null,"mtls":false}
    });
    let rendered = serde_json::to_vec(&source_field_order(payload)).unwrap();
    let expected = br#"{"authenticated":true,"public_ip":null,"local_ip":null,"country_code":null,"country_name":null,"city":null,"ip_lookup_enabled":false,"ip_lookup_env_disabled":true,"ip_lookup_pending":false,"env_mode":"aio","redis_tls":{"enabled":false,"verify":false,"mtls":false},"postgres_tls":{"enabled":false,"ssl_mode":null,"mtls":false}}"#;
    assert_eq!(rendered, expected);
    let response = copied_read_headers(
        Json(Value::Null).into_response(),
        axum::http::HeaderValue::from_static("GET, OPTIONS"),
    );
    assert_eq!(
        response.headers()[axum::http::header::ALLOW],
        "GET, OPTIONS"
    );
    assert_eq!(
        response.headers()[axum::http::header::VARY],
        "Accept, origin"
    );
}
