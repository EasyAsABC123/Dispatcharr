//! Source-derived VOD image/cache component and ownership controls.
//! Actual Original/native240 and physical/mixed host gates passed;
//! canonical app, unchanged React and broader image contracts are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_actions_sql as bounded,
    vod_api_images_resolve::Account,
    vod_api_owned_read::{OwnedConnection, ReadError, ReadWork},
    vod_api_sql::Resource,
};
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder, Row};
pub(super) async fn relation(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    resource: Resource,
    id: i64,
    account: Account,
) -> Result<Option<Value>, ReadError> {
    if account == Account::NoMatch {
        return Ok(None);
    }
    let (table, key) = match resource {
        Resource::Movie => ("vod_m3umovierelation", "movie_id"),
        Resource::Series => ("vod_m3useriesrelation", "series_id"),
        Resource::Episode => ("vod_m3uepisoderelation", "episode_id"),
    };
    let account = match account {
        Account::Id(id) => format!(" AND r.m3u_account_id={id}"),
        _ => String::new(),
    };
    let source=format!("WITH image_relation AS MATERIALIZED(SELECT r.id,r.custom_properties FROM {table} r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.{key}=$1 AND a.is_active{account} ORDER BY a.priority DESC,r.id ASC LIMIT 1) SELECT jsonb_build_object('id',id,'custom_properties',custom_properties) AS payload FROM image_relation");
    let mut rows = bounded::snapshot(
        work,
        connection,
        &source,
        id,
        work.row_bytes()
            .checked_add(1024)
            .ok_or(ReadError::Capacity)?,
    )
    .await?
    .into_rows();
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    if rows.next().is_some() {
        return Err(ReadError::WorkerFailed);
    }
    bounded::decode(work, &row).map(Some)
}
/// Source VODLogo get_object filters remain distinct from channel-logo filters.
/// IDs reuse exactly the same pinned Python isdigit/int helper. No DTO work or
/// related movie/series serialization is needed to select the stored URL.
pub(super) async fn logo(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
    pairs: &[(String, String)],
) -> Result<String, ReadError> {
    let last = |key: &str| {
        pairs
            .iter()
            .rev()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    let mut q = QueryBuilder::<Postgres>::new("SELECT octet_length(l.url)> ");
    q.push_bind(work.row_bytes() as i64)
        .push(" AS oversized,CASE WHEN octet_length(l.url)<=")
        .push_bind(work.row_bytes() as i64)
        .push(" THEN l.url END AS url FROM vod_vodlogo l WHERE l.id=")
        .push_bind(id);
    match crate::logos::ids(
        pairs
            .iter()
            .filter(|(key, _)| key == "ids")
            .map(|(_, value)| value.clone()),
    ) {
        None => {
            q.push(" AND false");
        }
        Some(ids) if !ids.is_empty() => {
            q.push(" AND l.id::text=ANY(").push_bind(ids).push(")");
        }
        _ => {}
    }
    if let Some(used) = last("used") {
        let movie = "EXISTS(SELECT 1 FROM vod_movie m WHERE m.logo_id=l.id)";
        let series = "EXISTS(SELECT 1 FROM vod_series s WHERE s.logo_id=l.id)";
        match used {
            "true" => {
                q.push(format!(" AND ({movie} OR {series})"));
            }
            "false" => {
                q.push(format!(" AND NOT {movie} AND NOT {series}"));
            }
            "movies" => {
                q.push(format!(" AND {movie}"));
            }
            "series" => {
                q.push(format!(" AND {series}"));
            }
            _ => {}
        }
    }
    if let Some(name) = last("name").filter(|value| !value.is_empty()) {
        q.push(" AND UPPER(l.name) LIKE UPPER(")
            .push_bind(crate::channels::like(name))
            .push(")");
    }
    if let Some(search) = last("search") {
        let terms = crate::channels::search_terms(search);
        if terms.len() > 32 {
            return Err(ReadError::Capacity);
        }
        for term in terms {
            let pattern = crate::channels::like(&term);
            q.push(" AND (UPPER(l.name) LIKE UPPER(")
                .push_bind(pattern.clone())
                .push(") OR UPPER(l.url) LIKE UPPER(")
                .push_bind(pattern)
                .push("))");
        }
    }
    connection.configure(work).await?;
    let row = q
        .build()
        .persistent(false)
        .fetch_optional(connection.raw())
        .await?
        .ok_or_else(|| {
            crate::StoreError::StatusBody(
                404,
                serde_json::json!({"detail":"No VODLogo matches the given query."}),
            )
        })?;
    if row.try_get::<bool, _>("oversized")? {
        return Err(ReadError::Capacity);
    }
    row.try_get("url").map_err(Into::into)
}
