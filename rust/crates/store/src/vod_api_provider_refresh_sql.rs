//! Isolated/unwired refresh SQL primitives. No encompassing transaction: Source
//! Movie/Series parent saves can precede later failure. Existing OwnedConnection
//! and ReadWork retain the physical operation; no new pool or permit budget.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_actions_account as account,
    vod_api_owned_read::{OwnedConnection, ReadError, ReadWork},
};
use serde_json::Value;
use sqlx::Row;
/// Bound the serialized model before SQLx allocates its JSON argument buffer.
/// The count-only pass allocates no model-sized output and keeps server-side
/// JSON/constraint failures in their original physical statement group.
fn encoded_model(work:&ReadWork,value:&Value)->Result<(),ReadError>{
    super::vod_api_provider_refresh_budget::encoded_bytes(value,work.work_bytes()/8).map(|_|())
}
async fn one(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    query: &str,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    let text=format!("WITH refresh_source AS MATERIALIZED ({query}) SELECT octet_length(payload::text)>$2 AS oversized,CASE WHEN octet_length(payload::text)<=$2 THEN payload::text ELSE NULL END AS payload FROM refresh_source");
    let row = sqlx::query(&text)
        .persistent(false)
        .bind(id)
        .bind(work.row_bytes() as i64)
        .fetch_optional(connection.raw())
        .await?;
    let Some(row) = row else { return Ok(None) };
    if row.try_get::<bool, _>("oversized")? {
        return Err(ReadError::Capacity);
    }
    let text: String = row.try_get("payload")?;
    let value = super::vod_api_provider_refresh_http::admitted_value(
        text.as_bytes(),
        work.work_bytes() / 16,
    )
    .map_err(|error| match error {
        super::vod_api_provider_refresh_http::Fault::Capacity => ReadError::Capacity,
        _ => ReadError::WorkerFailed,
    })?;
    if account::heap(&value)? > work.work_bytes() / 16 {
        return Err(ReadError::Capacity);
    }
    Ok(Some(value))
}
/// The task rereads Movie/relation/account after the view's selection. This is
/// deliberately separate from cached-only get_object snapshot projection.
pub(super) async fn movie_task(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(work,connection,"SELECT jsonb_build_object('relation',to_jsonb(r),'movie',to_jsonb(m),'account',to_jsonb(a),'agent',u.user_agent) AS payload FROM vod_m3umovierelation r JOIN vod_movie m ON m.id=r.movie_id JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id LEFT JOIN core_useragent u ON u.id=a.user_agent_id WHERE r.id=$1",id).await
}
pub(super) async fn movie_by_id(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(
        work,
        connection,
        "SELECT to_jsonb(m)AS payload FROM vod_movie m WHERE id=$1",
        id,
    )
    .await
}
pub(super) async fn movie_relation_by_id(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(
        work,
        connection,
        "SELECT to_jsonb(r)AS payload FROM vod_m3umovierelation r WHERE id=$1",
        id,
    )
    .await
}
pub(super) async fn movie_identity(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    key: &str,
    value: &str,
    language: &str,
) -> Result<Option<Value>, ReadError> {
    if !matches!(key, "tmdb_id" | "imdb_id") {
        return Err(ReadError::WorkerFailed);
    }
    let query=format!("WITH refresh_source AS MATERIALIZED (SELECT to_jsonb(m)AS payload FROM vod_movie m WHERE {key}=$1 AND language=$2) SELECT octet_length(payload::text)>$3 AS oversized,CASE WHEN octet_length(payload::text)<=$3 THEN payload::text ELSE NULL END AS payload FROM refresh_source");
    let mut rows = sqlx::query(&query)
        .persistent(false)
        .bind(value)
        .bind(language)
        .bind(work.row_bytes() as i64)
        .fetch_all(connection.raw())
        .await?;
    if rows.len() > 1 {
        return Err(ReadError::SourceUnhandledFault(
            "Movie identity lookup was nonunique",
        ));
    }
    let Some(row) = rows.pop() else {
        return Ok(None);
    };
    if row.try_get::<bool, _>("oversized")? {
        return Err(ReadError::Capacity);
    }
    let text: String = row.try_get("payload")?;
    let value = super::vod_api_provider_refresh_http::admitted_value(
        text.as_bytes(),
        work.work_bytes() / 16,
    )
    .map_err(|error| match error {
        super::vod_api_provider_refresh_http::Fault::Capacity => ReadError::Capacity,
        _ => ReadError::WorkerFailed,
    })?;
    if account::heap(&value)? > work.work_bytes() / 16 {
        return Err(ReadError::Capacity);
    }
    Ok(Some(value))
}
pub(super) async fn clear_identity(
    connection: &mut OwnedConnection,
    key: &str,
    id: i64,
) -> Result<(), ReadError> {
    if !matches!(key, "tmdb_id" | "imdb_id") {
        return Err(ReadError::WorkerFailed);
    }
    let query = format!("UPDATE vod_movie SET {key}=NULL WHERE id=$1");
    sqlx::query(&query)
        .persistent(false)
        .bind(id)
        .execute(connection.raw())
        .await?;
    Ok(())
}
/// Django save() overwrites the complete loaded model, including unchanged
/// columns, and stamps updated_at. Caller preserves the Source second-attempt
/// error/ID-clear logic; no automatic retry is performed here.
pub(super) async fn save_movie(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    movie: &Value,
    now: &str,
) -> Result<(), ReadError> {
    if account::heap(movie)? > work.work_bytes() / 16 {
        return Err(ReadError::Capacity);
    }
    encoded_model(work, movie)?;
    let result=sqlx::query("WITH p AS (SELECT (jsonb_populate_record(NULL::vod_movie,$1::jsonb)).*) UPDATE vod_movie m SET uuid=p.uuid,name=p.name,description=p.description,year=p.year,rating=p.rating,genre=p.genre,logo_id=p.logo_id,tmdb_id=p.tmdb_id,imdb_id=p.imdb_id,language=p.language,is_adult=p.is_adult,duration_secs=p.duration_secs,custom_properties=p.custom_properties,created_at=p.created_at,updated_at=($2::text)::timestamptz FROM p WHERE m.id=p.id").persistent(false).bind(movie).bind(now).execute(connection.raw()).await?;
    if result.rows_affected() != 1 {
        return Err(ReadError::Unsupported(
            "Concurrent Movie deletion/reinsert save phase needs Source fixture",
        ));
    }
    Ok(())
}
pub(super) async fn save_movie_relation(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    relation: &Value,
) -> Result<(), ReadError> {
    if account::heap(&relation["custom_properties"])? > work.work_bytes() / 16 {
        return Err(ReadError::Capacity);
    }
    encoded_model(work, relation)?;
    let result=sqlx::query("UPDATE vod_m3umovierelation SET custom_properties=$2,last_advanced_refresh=($3::text)::timestamptz WHERE id=$1").persistent(false).bind(relation["id"].as_i64().ok_or(ReadError::WorkerFailed)?).bind(&relation["custom_properties"]).bind(relation["last_advanced_refresh"].as_str().ok_or(ReadError::WorkerFailed)?).execute(connection.raw()).await?;
    if result.rows_affected() != 1 {
        return Err(ReadError::SourceUnhandledFault(
            "Save with update_fields did not affect any rows",
        ));
    }
    Ok(())
}
/// Source transfers each surviving relation before deleting the donor. The
/// separate statements deliberately retain committed prefixes on later faults.
pub(super) async fn transfer_and_delete(
    connection: &mut OwnedConnection,
    source: i64,
    target: i64,
) -> Result<(), ReadError> {
    sqlx::query("UPDATE vod_m3umovierelation SET movie_id=$2 WHERE movie_id=$1")
        .persistent(false)
        .bind(source)
        .bind(target)
        .execute(connection.raw())
        .await?;
    sqlx::query("DELETE FROM vod_movie WHERE id=$1")
        .persistent(false)
        .bind(source)
        .execute(connection.raw())
        .await?;
    Ok(())
}
pub(super) async fn account_by_id(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(
        work,
        connection,
        "SELECT to_jsonb(a) AS payload FROM m3u_m3uaccount a WHERE id=$1",
        id,
    )
    .await
}
pub(super) async fn logo_by_id(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(
        work,
        connection,
        "SELECT to_jsonb(l) AS payload FROM vod_vodlogo l WHERE id=$1",
        id,
    )
    .await
}
/// View-owned Series/account snapshot. Unlike Movie task loading, this must not
/// be re-read after upstream suspension; Source passes these model instances in.
pub(super) async fn series_view(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(work,connection,"SELECT jsonb_build_object('relation',to_jsonb(r),'series',to_jsonb(s),'account',to_jsonb(a),'agent',u.user_agent) AS payload FROM vod_m3useriesrelation r JOIN vod_series s ON s.id=r.series_id JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id LEFT JOIN core_useragent u ON u.id=a.user_agent_id WHERE r.id=$1",id).await
}
pub(super) async fn save_series(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    series: &Value,
    now: &str,
) -> Result<(), ReadError> {
    if account::heap(series)? > work.work_bytes() / 16 {
        return Err(ReadError::Capacity);
    }
    encoded_model(work, series)?;
    let result=sqlx::query("WITH p AS (SELECT (jsonb_populate_record(NULL::vod_series,$1::jsonb)).*) UPDATE vod_series s SET uuid=p.uuid,name=p.name,description=p.description,year=p.year,rating=p.rating,genre=p.genre,logo_id=p.logo_id,tmdb_id=p.tmdb_id,imdb_id=p.imdb_id,language=p.language,custom_properties=p.custom_properties,created_at=p.created_at,updated_at=($2::text)::timestamptz FROM p WHERE s.id=p.id").persistent(false).bind(series).bind(now).execute(connection.raw()).await?;
    if result.rows_affected() != 1 {
        return Err(ReadError::Unsupported(
            "Concurrent Series deletion/reinsert save phase needs Source fixture",
        ));
    }
    Ok(())
}
/// Source performs this selection after the HTTP response, by account/external
/// ID alone, independently of the Series view's original relation instance.
pub(super) async fn series_task_relation(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    account: i64,
    external: &str,
) -> Result<Option<Value>, ReadError> {
    let row=sqlx::query("WITH source AS MATERIALIZED (SELECT to_jsonb(r) AS payload FROM vod_m3useriesrelation r WHERE m3u_account_id=$1 AND external_series_id=$2 ORDER BY id LIMIT 1) SELECT octet_length(payload::text)>$3 AS oversized,CASE WHEN octet_length(payload::text)<=$3 THEN payload::text ELSE NULL END AS payload FROM source").persistent(false).bind(account).bind(external).bind(work.row_bytes()as i64).fetch_optional(connection.raw()).await?;
    let Some(row) = row else { return Ok(None) };
    if row.try_get::<bool, _>("oversized")? {
        return Err(ReadError::Capacity);
    }
    let raw: String = row.try_get("payload")?;
    let value = super::vod_api_provider_refresh_http::admitted_value(
        raw.as_bytes(),
        work.work_bytes() / 16,
    )
    .map_err(|error| match error {
        super::vod_api_provider_refresh_http::Fault::Capacity => ReadError::Capacity,
        _ => ReadError::WorkerFailed,
    })?;
    if account::heap(&value)? > work.work_bytes() / 16 {
        return Err(ReadError::Capacity);
    }
    Ok(Some(value))
}
pub(super) async fn stored_series_episodes(
    connection: &mut OwnedConnection,
    relation: i64,
) -> Result<bool, ReadError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM vod_m3uepisoderelation WHERE series_relation_id=$1)",
    )
    .persistent(false)
    .bind(relation)
    .fetch_one(connection.raw())
    .await?)
}
pub(super) async fn save_series_relation(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    relation: &Value,
    now: &str,
) -> Result<(), ReadError> {
    if account::heap(relation)? > work.work_bytes() / 16 {
        return Err(ReadError::Capacity);
    }
    encoded_model(work, relation)?;
    let result=sqlx::query("WITH p AS (SELECT (jsonb_populate_record(NULL::vod_m3useriesrelation,$1::jsonb)).*) UPDATE vod_m3useriesrelation r SET m3u_account_id=p.m3u_account_id,series_id=p.series_id,category_id=p.category_id,external_series_id=p.external_series_id,custom_properties=p.custom_properties,created_at=p.created_at,updated_at=($2::text)::timestamptz,last_episode_refresh=p.last_episode_refresh,last_seen=p.last_seen FROM p WHERE r.id=p.id").persistent(false).bind(relation).bind(now).execute(connection.raw()).await?;
    if result.rows_affected() != 1 {
        return Err(ReadError::Unsupported(
            "Concurrent Series relation deletion/reinsert save needs Source fixture",
        ));
    }
    Ok(())
}
pub(super) async fn series_by_id(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(
        work,
        connection,
        "SELECT to_jsonb(s) AS payload FROM vod_series s WHERE id=$1",
        id,
    )
    .await
}
pub(super) async fn series_relation_by_id(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(
        work,
        connection,
        "SELECT to_jsonb(r) AS payload FROM vod_m3useriesrelation r WHERE id=$1",
        id,
    )
    .await
}
pub(super) async fn category_by_id(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(
        work,
        connection,
        "SELECT to_jsonb(c) AS payload FROM vod_vodcategory c WHERE id=$1",
        id,
    )
    .await
}
pub(super) async fn agent_by_id(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: i64,
) -> Result<Option<Value>, ReadError> {
    one(
        work,
        connection,
        "SELECT to_jsonb(u) AS payload FROM core_useragent u WHERE id=$1",
        id,
    )
    .await
}
