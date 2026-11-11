//! Source-faithful nested content fields and per-episode model caches.
//! Actual Source/native component gates passed; app/browser acceptance is
//! separate. Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_actions_sql::{decode, snapshot},
    vod_api_dto::{CommonRow, EpisodeRow, LogoContext, LogoRow, MovieRow, SeriesRow},
    vod_api_owned_read::{OwnedConnection, ReadError, ReadWork},
    vod_api_sql::Resource,
};
use serde_json::Value;
fn integer(raw: &Value, key: &str) -> Result<i64, ReadError> {
    raw[key].as_i64().ok_or(ReadError::WorkerFailed)
}
fn optional_integer(raw: &Value, key: &str) -> Result<Option<i64>, ReadError> {
    if raw[key].is_null() {
        Ok(None)
    } else {
        integer(raw, key).map(Some)
    }
}
fn text(raw: &Value, key: &str) -> Result<String, ReadError> {
    raw[key]
        .as_str()
        .map(str::to_owned)
        .ok_or(ReadError::WorkerFailed)
}
fn optional_text(raw: &Value, key: &str) -> Result<Option<String>, ReadError> {
    if raw[key].is_null() {
        Ok(None)
    } else {
        text(raw, key).map(Some)
    }
}
fn datetime(raw: &Value, key: &str) -> Result<chrono::DateTime<chrono::Utc>, ReadError> {
    chrono::DateTime::parse_from_rfc3339(raw[key].as_str().ok_or(ReadError::WorkerFailed)?)
        .map(|date| date.with_timezone(&chrono::Utc))
        .map_err(|_| ReadError::WorkerFailed)
}
fn common(raw: &Value) -> Result<CommonRow, ReadError> {
    Ok(CommonRow {
        id: integer(raw, "id")?,
        uuid: text(raw, "uuid")?,
        name: text(raw, "name")?,
        description: optional_text(raw, "description")?,
        year: optional_integer(raw, "year")?,
        rating: optional_text(raw, "rating")?,
        genre: optional_text(raw, "genre")?,
        tmdb_id: optional_text(raw, "tmdb_id")?,
        imdb_id: optional_text(raw, "imdb_id")?,
        language: text(raw, "language")?,
        custom_properties: raw["custom_properties"].clone(),
        created_at: datetime(raw, "created_at")?,
        updated_at: datetime(raw, "updated_at")?,
    })
}
pub(super) async fn content_raw(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    resource: Resource,
    id: i64,
) -> Result<Value, ReadError> {
    let table = match resource {
        Resource::Movie => "vod_movie",
        Resource::Series => "vod_series",
        Resource::Episode => "vod_episode",
    };
    let rows = snapshot(
        work,
        connection,
        &format!("SELECT to_jsonb(c) AS payload FROM {table} c WHERE c.id=$1"),
        id,
        work.row_bytes()
            .checked_add(1024)
            .ok_or(ReadError::Capacity)?,
    )
    .await?;
    let mut rows = rows.into_rows();
    let raw = rows.next().ok_or(ReadError::SourceUnhandledFault(
        "Related content disappeared",
    ))?;
    if rows.next().is_some() {
        return Err(ReadError::WorkerFailed);
    }
    decode(work, &raw)
}
async fn count(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    sql: &str,
    id: i64,
) -> Result<i64, ReadError> {
    connection.configure(work).await?;
    Ok(sqlx::query_scalar(sql)
        .persistent(false)
        .bind(id)
        .fetch_one(connection.raw())
        .await?)
}
async fn names(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    table: &str,
    id: i64,
) -> Result<Vec<String>, ReadError> {
    let rows=snapshot(work,connection,&format!("SELECT to_jsonb(c.name) AS payload FROM {table} c WHERE c.logo_id=$1 ORDER BY c.name LIMIT 10"),id,work.row_bytes().checked_mul(12).ok_or(ReadError::Capacity)?).await?;
    rows.into_rows()
        .map(|raw| {
            decode(work, &raw)?
                .as_str()
                .map(str::to_owned)
                .ok_or(ReadError::WorkerFailed)
        })
        .collect()
}
async fn logo_snapshot(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: Option<i64>,
) -> Result<Option<Value>, ReadError> {
    let Some(id) = id else { return Ok(None) };
    let rows = snapshot(
        work,
        connection,
        "SELECT to_jsonb(l) AS payload FROM vod_vodlogo l WHERE l.id=$1",
        id,
        work.row_bytes()
            .checked_add(1024)
            .ok_or(ReadError::Capacity)?,
    )
    .await?;
    let raw = decode(
        work,
        &rows
            .into_rows()
            .next()
            .ok_or(ReadError::SourceUnhandledFault("Related logo disappeared"))?,
    )?;
    Ok(Some(raw))
}
async fn logo(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    id: Option<i64>,
) -> Result<Option<LogoRow>, ReadError> {
    let raw = logo_snapshot(work, connection, id).await?;
    logo_methods(work, connection, raw.as_ref()).await
}
async fn logo_methods(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    raw: Option<&Value>,
) -> Result<Option<LogoRow>, ReadError> {
    let Some(raw) = raw else { return Ok(None) };
    let id = integer(raw, "id")?;
    let movie_count = count(
        work,
        connection,
        "SELECT COUNT(*) FROM vod_movie WHERE logo_id=$1",
        id,
    )
    .await?;
    let series_count = count(
        work,
        connection,
        "SELECT COUNT(*) FROM vod_series WHERE logo_id=$1",
        id,
    )
    .await?;
    connection.configure(work).await?;
    let mut used = sqlx::query("SELECT 1 FROM vod_movie WHERE logo_id=$1 LIMIT 1")
        .persistent(false)
        .bind(id)
        .fetch_optional(connection.raw())
        .await?
        .is_some();
    if !used {
        connection.configure(work).await?;
        used = sqlx::query("SELECT 1 FROM vod_series WHERE logo_id=$1 LIMIT 1")
            .persistent(false)
            .bind(id)
            .fetch_optional(connection.raw())
            .await?
            .is_some();
    }
    let movie_names = names(work, connection, "vod_movie", id).await?;
    let series_names = names(work, connection, "vod_series", id).await?;
    Ok(Some(LogoRow {
        id,
        name: text(raw, "name")?,
        url: text(raw, "url")?,
        movie_count,
        series_count,
        is_used: used,
        movie_names,
        series_names,
    }))
}
async fn series(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    raw: &Value,
) -> Result<SeriesRow, ReadError> {
    let logo = logo(work, connection, optional_integer(raw, "logo_id")?).await?;
    let episode_count = count(
        work,
        connection,
        "SELECT COUNT(*) FROM vod_episode WHERE series_id=$1",
        integer(raw, "id")?,
    )
    .await?;
    Ok(SeriesRow {
        common: common(raw)?,
        logo,
        episode_count,
    })
}
pub(super) async fn project(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    resource: Resource,
    raw: &Value,
) -> Result<Value, ReadError> {
    let context = LogoContext {
        absolute_origin: None,
    };
    Ok(match resource {
        Resource::Movie => MovieRow {
            common: common(raw)?,
            logo: logo(work, connection, optional_integer(raw, "logo_id")?).await?,
            duration_secs: optional_integer(raw, "duration_secs")?,
            is_adult: raw["is_adult"].as_bool().ok_or(ReadError::WorkerFailed)?,
        }
        .project(&context),
        Resource::Series => series(work, connection, raw).await?.project(&context),
        Resource::Episode => {
            let cached = episode_context(work, connection, raw).await?;
            return project_cached_episode(work, connection, raw, &cached).await;
        }
    })
}

/// Django prefetch backfills each relation.episode to the root instance. Its
/// Series and Logo field caches are reused across nested serialization; only
/// method-field queries get fresh snapshots each time.
pub(super) struct EpisodeContext {
    series: Value,
    logo: Option<Value>,
}
pub(super) async fn episode_context(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    raw: &Value,
) -> Result<EpisodeContext, ReadError> {
    let series = content_raw(
        work,
        connection,
        Resource::Series,
        integer(raw, "series_id")?,
    )
    .await?;
    let logo = logo_snapshot(work, connection, optional_integer(&series, "logo_id")?).await?;
    Ok(EpisodeContext { series, logo })
}
pub(super) async fn project_cached_episode(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    raw: &Value,
    cached: &EpisodeContext,
) -> Result<Value, ReadError> {
    let logo = logo_methods(work, connection, cached.logo.as_ref()).await?;
    let episode_count = count(
        work,
        connection,
        "SELECT COUNT(*) FROM vod_episode WHERE series_id=$1",
        integer(&cached.series, "id")?,
    )
    .await?;
    let series = SeriesRow {
        common: common(&cached.series)?,
        logo,
        episode_count,
    };
    Ok(EpisodeRow {
        id: integer(raw, "id")?,
        uuid: text(raw, "uuid")?,
        name: text(raw, "name")?,
        series,
        description: optional_text(raw, "description")?,
        air_date: optional_text(raw, "air_date")?
            .map(|date| {
                chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                    .map_err(|_| ReadError::WorkerFailed)
            })
            .transpose()?,
        rating: optional_text(raw, "rating")?,
        duration_secs: optional_integer(raw, "duration_secs")?,
        season_number: optional_integer(raw, "season_number")?,
        episode_number: optional_integer(raw, "episode_number")?,
        tmdb_id: optional_text(raw, "tmdb_id")?,
        imdb_id: optional_text(raw, "imdb_id")?,
        custom_properties: raw["custom_properties"].clone(),
        created_at: datetime(raw, "created_at")?,
        updated_at: datetime(raw, "updated_at")?,
    }
    .project(&LogoContext {
        absolute_origin: None,
    }))
}
