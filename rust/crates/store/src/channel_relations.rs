//! Row expansion/stat deltas ported from the original Dispatcharr API views.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{channels, AuthService, StoreError, User};
use axum::{
    extract::{ConnectInfo, OriginalUri, Path, Query, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use std::{collections::HashMap, net::SocketAddr};

async fn channel(auth: &AuthService, user: &User, id: &str) -> Result<i64, StoreError> {
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let id = id
        .parse::<i64>()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
    let mut query = QueryBuilder::<Postgres>::new(
        "SELECT c.id FROM dispatcharr_channels_channel c WHERE c.id=",
    );
    query.push_bind(id);
    channels::scope(&mut query, user);
    query
        .build_query_scalar::<i64>()
        .fetch_optional(auth.store.pool())
        .await?
        .ok_or_else(|| {
            StoreError::StatusBody(404, json!({"detail":"No Channel matches the given query."}))
        })
}

pub(crate) async fn streams(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    let id = channel(&auth, &user, &id).await?;
    let base = channels::base_url(&headers, &uri)?;
    // Unlike StreamViewSet, this relation includes inactive-provider streams.
    let rows=sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('raw',to_jsonb(s)) FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE cs.channel_id=$1 ORDER BY cs.\"order\"")
        .bind(id).fetch_all(auth.store.pool()).await?;
    Ok(Json(Value::Array(
        rows.iter()
            .map(|row| channels::stream(row, user.user_level >= 10, &base))
            .collect(),
    )))
}

fn ids(value: &str) -> Result<Vec<i64>, StoreError> {
    let mut ids = Vec::new();
    for part in value.split(',').filter(|part| !part.trim().is_empty()) {
        // Python int accepts arbitrary precision. Out-of-range IDs cannot
        // match PostgreSQL integer IDs, but must not turn into a 400 response.
        let parsed =
            crate::channel_memberships::primary_key(&Value::String(part.into())).map_err(|_| {
                StoreError::InvalidInput(
                    json!({"detail":"Invalid 'ids' value. Expected comma-separated integers."}),
                )
            })?;
        if let Some(id) = parsed {
            ids.push(id);
        }
    }
    Ok(ids)
}

pub(crate) async fn stats(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    let id = channel(&auth, &user, &id).await?;
    // Authorization/channel existence precede all delta parameter validation.
    let mut query=QueryBuilder::<Postgres>::new("SELECT jsonb_build_object('id',s.id,'stream_stats',s.stream_stats,'stream_stats_updated_at',s.stream_stats_updated_at) FROM dispatcharr_channels_stream s JOIN dispatcharr_channels_channelstream cs ON cs.stream_id=s.id WHERE cs.channel_id=");
    query.push_bind(id);
    if let Some(raw) = params.get("since").filter(|value| !value.is_empty()) {
        let since =
            crate::admin_writes::parse_datetime("since", &Value::String(raw.clone()), false)
                .map_err(|_| {
                    StoreError::InvalidInput(
                        json!({"detail":"Invalid 'since' value. Expected ISO 8601."}),
                    )
                })?;
        query
            .push(" AND s.stream_stats_updated_at>")
            .push_bind(since);
    }
    if let Some(raw) = params.get("ids").filter(|value| !value.is_empty()) {
        query.push(" AND s.id=ANY(").push_bind(ids(raw)?).push(")");
    }
    query.push(" ORDER BY s.updated_at DESC");
    let mut rows = query
        .build_query_scalar::<Value>()
        .fetch_all(auth.store.pool())
        .await?;
    for row in &mut rows {
        row["stream_stats_updated_at"] = channels::timestamp(&row["stream_stats_updated_at"]);
    }
    Ok(Json(Value::Array(rows)))
}

#[cfg(test)]
mod tests {
    use super::ids;
    #[test]
    fn integer_delta_filter_preserves_python_semantics() {
        assert_eq!(ids(" , +12,1_000,-7,, ").unwrap(), vec![12, 1000, -7]);
        assert!(ids("99999999999999999999999999999999999999999")
            .unwrap()
            .is_empty());
        assert!(ids(" , , ").unwrap().is_empty());
        for invalid in ["1.0", "true", "1,a", "_"] {
            assert!(ids(invalid).is_err());
        }
    }
}
