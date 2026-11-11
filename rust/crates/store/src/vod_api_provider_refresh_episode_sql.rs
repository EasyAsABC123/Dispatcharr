//! Isolated/unwired ordered episode transaction. Prepared field conversions run
//! per Source bulk group; nextval effects survive rollback exactly as PostgreSQL
//! dictates. No mutation replay, full-table truncate or second pool.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_owned_read::{OwnedConnection, ReadError, ReadWork},
    vod_api_provider_refresh_actor::Clock,
    vod_api_provider_refresh_episodes::Batch,
    vod_api_provider_refresh_value as source,
};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder, Row};
fn fault() -> ReadError {
    ReadError::SourceUnhandledFault("Source episode SQL field preparation failed")
}
fn text(value: &Value) -> Result<Option<String>, ReadError> {
    if value.is_null() {
        Ok(None)
    } else {
        source::text(value).map(Some).map_err(|e| match e {
            source::Fault::Capacity => ReadError::Capacity,
            source::Fault::Type => fault(),
        })
    }
}
fn date(value: &Value) -> Result<Option<chrono::NaiveDate>, ReadError> {
    value
        .as_str()
        .map(|v| chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d").map_err(|_| fault()))
        .transpose()
}
fn object(value: &Value) -> Option<Value> {
    (!value.is_null()).then(|| value.clone())
}
async fn rows(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    sql: &str,
    id: i64,
    ids: Option<&[String]>,
) -> Result<Vec<String>, ReadError> {
    let parameter = if ids.is_some() { "$3" } else { "$2" };
    let sql=format!("WITH source AS MATERIALIZED ({sql}) SELECT octet_length(payload::text)>{parameter} AS oversized,CASE WHEN octet_length(payload::text)<={parameter} THEN payload::text ELSE NULL END AS payload FROM source");
    let query = sqlx::query(&sql).persistent(false).bind(id);
    let query = if let Some(ids) = ids {
        query.bind(ids)
    } else {
        query
    };
    let mut cursor = query.bind(work.row_bytes() as i64).fetch(connection.raw());
    let mut output = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = cursor.try_next().await? {
        if row.try_get::<bool, _>("oversized")? {
            return Err(ReadError::Capacity);
        }
        let value: String = row.try_get("payload")?;
        bytes = bytes
            .checked_add(value.capacity())
            .and_then(|n| n.checked_add(256))
            .ok_or(ReadError::Capacity)?;
        if bytes > work.work_bytes() / 16 {
            return Err(ReadError::Capacity);
        }
        output.push(value);
    }
    Ok(output)
}
pub(super) async fn episodes(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    series: i64,
) -> Result<Vec<String>, ReadError> {
    rows(work,connection,"SELECT to_jsonb(e) AS payload FROM vod_episode e WHERE series_id=$1 ORDER BY season_number,episode_number",series,None).await
}
pub(super) async fn relations(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    account: i64,
    ids: &[String],
) -> Result<Vec<String>, ReadError> {
    rows(work,connection,"SELECT to_jsonb(r) AS payload FROM vod_m3uepisoderelation r JOIN vod_episode e ON e.id=r.episode_id WHERE r.m3u_account_id=$1 AND r.stream_id=ANY($2::text[])",account,Some(ids)).await
}
/// One phase quota covers prepared field copies plus query arguments and their
/// PostgreSQL JSON/text encoding scratch. Charge the actual conservative model
/// heap, measured encoded byte count (including escaping), potential argument
/// buffer growth and per-bind storage. No row-count clamp or second allowance.
fn prepared_row<T>(
    work: &ReadWork,
    total: &mut usize,
    model: &Value,
    binds: usize,
    output: &mut Vec<T>,
) -> Result<(), ReadError> {
    let encoded=super::vod_api_provider_refresh_budget::encoded_bytes(model,work.work_bytes()/8)?;
    let charge = super::vod_api_actions_account::heap(model)?
        .checked_add(encoded.checked_mul(2).ok_or(ReadError::Capacity)?)
        .and_then(|n| n.checked_add(std::mem::size_of::<T>() * 2))
        .and_then(|n| n.checked_add(binds * 128))
        .ok_or(ReadError::Capacity)?;
    *total = total
        .checked_add(charge)
        .filter(|n| *n <= work.work_bytes() / 8)
        .ok_or(ReadError::Capacity)?;
    output.try_reserve_exact(1).map_err(|_| ReadError::Capacity)
}
struct EpisodeFields {
    uuid: uuid::Uuid,
    name: Option<String>,
    description: Option<String>,
    air_date: Option<chrono::NaiveDate>,
    rating: Option<String>,
    duration: Option<i64>,
    series: i64,
    season: String,
    number: String,
    tmdb: Option<String>,
    imdb: Option<String>,
    props: Option<Value>,
    created: String,
    updated: String,
}
fn episode_fields(value: &Value, clock: Clock) -> Result<EpisodeFields, ReadError> {
    Ok(EpisodeFields {
        uuid: uuid::Uuid::parse_str(value["uuid"].as_str().ok_or_else(fault)?)
            .map_err(|_| fault())?,
        name: text(&value["name"])?,
        description: text(&value["description"])?,
        air_date: date(&value["air_date"])?,
        rating: text(&value["rating"])?,
        duration: value["duration_secs"].as_i64(),
        series: value["series_id"].as_i64().ok_or_else(fault)?,
        season: value["season_number"].to_string(),
        number: value["episode_number"].to_string(),
        tmdb: text(&value["tmdb_id"])?,
        imdb: text(&value["imdb_id"])?,
        props: object(&value["custom_properties"]),
        created: clock.stamp(),
        updated: clock.stamp(),
    })
}
async fn insert_episodes(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    batch: &Batch,
    clock: Clock,
) -> Result<(), ReadError> {
    // Django prepares every object's fields before sending this bulk group.
    let mut prepared = Vec::new();
    let mut charged = 0;
    for episode in batch.episodes.iter().filter(|e| !e.existing) {
        if episode.date_fault {
            return Err(fault());
        }
        prepared_row(work, &mut charged, &episode.model, 14, &mut prepared)?;
        prepared.push(episode_fields(&episode.model, clock)?);
    }
    for group in prepared.chunks(3000) {
        if group.is_empty() {
            continue;
        }
        let mut query=QueryBuilder::<Postgres>::new("INSERT INTO vod_episode(uuid,name,description,air_date,rating,duration_secs,series_id,season_number,episode_number,tmdb_id,imdb_id,custom_properties,created_at,updated_at) ");
        query.push_values(group, |mut row, p| {
            row.push_bind(p.uuid)
                .push_bind(p.name.clone())
                .push_bind(p.description.clone())
                .push_bind(p.air_date)
                .push_bind(p.rating.clone())
                .push_bind(p.duration)
                .push_bind(p.series)
                .push("(")
                .push_bind_unseparated(p.season.clone())
                .push_unseparated("::numeric)::integer")
                .push("(")
                .push_bind_unseparated(p.number.clone())
                .push_unseparated("::numeric)::integer")
                .push_bind(p.tmdb.clone())
                .push_bind(p.imdb.clone())
                .push_bind(p.props.clone())
                .push("(")
                .push_bind_unseparated(p.created.clone())
                .push_unseparated("::text)::timestamptz")
                .push("(")
                .push_bind_unseparated(p.updated.clone())
                .push_unseparated("::text)::timestamptz");
        });
        query
            .push(" ON CONFLICT DO NOTHING")
            .build()
            .persistent(false)
            .execute(connection.raw())
            .await?;
    }
    Ok(())
}
async fn update_episodes(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    batch: &Batch,
) -> Result<(), ReadError> {
    let mut prepared = Vec::new();
    let mut charged = 0;
    for index in &batch.episode_updates {
        let e = &batch.episodes[*index];
        if e.date_fault {
            return Err(fault());
        }
        prepared_row(work, &mut charged, &e.model, 9, &mut prepared)?;
        prepared.push((
            e.model["id"].as_i64().ok_or_else(fault)?,
            episode_fields(&e.model, Clock { fixed: None })?,
        ));
    }
    for group in prepared.chunks(5000) {
        if group.is_empty() {
            continue;
        }
        let mut query=QueryBuilder::<Postgres>::new("UPDATE vod_episode e SET name=v.name,description=v.description,air_date=v.air_date,rating=v.rating,duration_secs=v.duration_secs,tmdb_id=v.tmdb_id,imdb_id=v.imdb_id,custom_properties=v.props FROM (");
        query.push_values(group, |mut row, (id, p)| {
            row.push_bind(*id)
                .push_bind(p.name.clone())
                .push_bind(p.description.clone())
                .push_bind(p.air_date)
                .push_bind(p.rating.clone())
                .push_bind(p.duration)
                .push_bind(p.tmdb.clone())
                .push_bind(p.imdb.clone())
                .push_bind(p.props.clone());
        });
        query.push(") AS v(id,name,description,air_date,rating,duration_secs,tmdb_id,imdb_id,props) WHERE e.id=v.id").build().persistent(false).execute(connection.raw()).await?;
    }
    Ok(())
}
struct RelationFields {
    id: Option<i64>,
    account: i64,
    episode: i64,
    series_relation: Option<i64>,
    stream: String,
    extension: Option<String>,
    props: Option<Value>,
    last: String,
    created: String,
    updated: String,
}
fn relation_fields(
    batch: &Batch,
    index: usize,
    clock: Clock,
) -> Result<Option<RelationFields>, ReadError> {
    let relation = &batch.relations[index];
    let model = &relation.model;
    let Some(episode) = batch.episodes[relation.episode].model["id"].as_i64() else {
        return Ok(None);
    };
    Ok(Some(RelationFields {
        id: model["id"].as_i64(),
        account: model["m3u_account_id"].as_i64().ok_or_else(fault)?,
        episode,
        series_relation: model["series_relation_id"].as_i64(),
        stream: model["stream_id"].as_str().ok_or_else(fault)?.into(),
        extension: text(&model["container_extension"])?,
        props: object(&model["custom_properties"]),
        last: model["last_seen"].as_str().ok_or_else(fault)?.into(),
        created: clock.stamp(),
        updated: clock.stamp(),
    }))
}
async fn insert_relations(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    batch: &Batch,
    clock: Clock,
) -> Result<(), ReadError> {
    let mut prepared = Vec::new();
    let mut charged = 0;
    for index in &batch.relation_creates {
        prepared_row(
            work,
            &mut charged,
            &batch.relations[*index].model,
            9,
            &mut prepared,
        )?;
        if let Some(fields) = relation_fields(batch, *index, clock)? {
            prepared.push(fields);
        }
    }
    for group in prepared.chunks(5000) {
        if group.is_empty() {
            continue;
        }
        let mut query=QueryBuilder::<Postgres>::new("INSERT INTO vod_m3uepisoderelation(m3u_account_id,episode_id,series_relation_id,stream_id,container_extension,custom_properties,last_seen,created_at,updated_at) ");
        query.push_values(group, |mut row, p| {
            row.push_bind(p.account)
                .push_bind(p.episode)
                .push_bind(p.series_relation)
                .push_bind(p.stream.clone())
                .push_bind(p.extension.clone())
                .push_bind(p.props.clone())
                .push("(")
                .push_bind_unseparated(p.last.clone())
                .push_unseparated("::text)::timestamptz")
                .push("(")
                .push_bind_unseparated(p.created.clone())
                .push_unseparated("::text)::timestamptz")
                .push("(")
                .push_bind_unseparated(p.updated.clone())
                .push_unseparated("::text)::timestamptz");
        });
        query
            .push(" ON CONFLICT DO NOTHING")
            .build()
            .persistent(false)
            .execute(connection.raw())
            .await?;
    }
    Ok(())
}
async fn update_relations(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    batch: &Batch,
    clock: Clock,
) -> Result<(), ReadError> {
    let mut prepared = Vec::new();
    let mut charged = 0;
    for index in &batch.relation_updates {
        prepared_row(
            work,
            &mut charged,
            &batch.relations[*index].model,
            9,
            &mut prepared,
        )?;
        if let Some(fields) = relation_fields(batch, *index, clock)? {
            prepared.push(fields);
        }
    }
    for group in prepared.chunks(5000) {
        if group.is_empty() {
            continue;
        }
        let mut query=QueryBuilder::<Postgres>::new("UPDATE vod_m3uepisoderelation r SET episode_id=v.episode_id,series_relation_id=v.series_relation_id,container_extension=v.extension,custom_properties=v.props,last_seen=v.last_seen FROM (");
        query.push_values(group, |mut row, p| {
            row.push_bind(p.id.unwrap())
                .push_bind(p.episode)
                .push_bind(p.series_relation)
                .push_bind(p.extension.clone())
                .push_bind(p.props.clone())
                .push("(")
                .push_bind_unseparated(p.last.clone())
                .push_unseparated("::text)::timestamptz");
        });
        query.push(") AS v(id,episode_id,series_relation_id,extension,props,last_seen) WHERE r.id=v.id").build().persistent(false).execute(connection.raw()).await?;
    }
    Ok(())
}
/// Same group order as Source: new Episodes, re-fetch, existing Episode UPDATE,
/// new relations, existing relation UPDATE, stale relation DELETE, commit.
pub(super) async fn apply(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    batch: &mut Batch,
    series: i64,
    account: i64,
    series_relation: Option<i64>,
    clock: Clock,
) -> Result<(), ReadError> {
    sqlx::query("BEGIN")
        .persistent(false)
        .execute(connection.raw())
        .await?;
    let result=async{
        let any_new=batch.episodes.iter().any(|e|!e.existing);insert_episodes(work,connection,batch,clock).await?;
        if any_new{let rows=episodes(work,connection,series).await?;let mut ids=std::collections::HashMap::new();for raw in rows{let row=super::vod_api_provider_refresh_http::admitted_value(raw.as_bytes(),work.work_bytes()/16).map_err(|error|match error{super::vod_api_provider_refresh_http::Fault::Capacity=>ReadError::Capacity,_=>fault()})?;
                let entry=std::mem::size_of::<((String,String),(Value,Value))>()*2+128;
                if ids.len().checked_add(1).and_then(|n|n.checked_mul(entry)).is_none_or(|n|n>work.work_bytes()/16){return Err(ReadError::Capacity);}
                ids.try_reserve(1).map_err(|_|ReadError::Capacity)?;
                ids.insert((row["season_number"].to_string(),row["episode_number"].to_string()),(row["id"].clone(),row["uuid"].clone()));}
            for episode in batch.episodes.iter_mut().filter(|e|!e.existing){if let Some((id,uuid))=ids.get(&(episode.model["season_number"].to_string(),episode.model["episode_number"].to_string())){episode.model["id"]=id.clone();episode.model["uuid"]=uuid.clone();}}
        }
        update_episodes(work,connection,batch).await?;insert_relations(work,connection,batch,clock).await?;update_relations(work,connection,batch,clock).await?;
        if let Some(relation)=series_relation{sqlx::query("DELETE FROM vod_m3uepisoderelation r WHERE (r.series_relation_id=$1 OR (r.series_relation_id IS NULL AND r.m3u_account_id=$2 AND r.episode_id IN(SELECT id FROM vod_episode WHERE series_id=$3))) AND NOT(r.stream_id=ANY($4::text[]))").persistent(false).bind(relation).bind(account).bind(series).bind(&batch.ids).execute(connection.raw()).await?;}
        else{sqlx::query("DELETE FROM vod_m3uepisoderelation r WHERE r.m3u_account_id=$1 AND r.episode_id IN(SELECT id FROM vod_episode WHERE series_id=$2) AND NOT(r.stream_id=ANY($3::text[]))").persistent(false).bind(account).bind(series).bind(&batch.ids).execute(connection.raw()).await?;}
        Ok::<_,ReadError>(())
    }.await;
    if let Err(error) = result {
        sqlx::query("ROLLBACK")
            .persistent(false)
            .execute(connection.raw())
            .await?;
        return Err(error);
    }
    sqlx::query("COMMIT")
        .persistent(false)
        .execute(connection.raw())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vod_api_draft::vod_api_owned_read::{ReadLimits, ReadRuntime};
    #[tokio::test]
    async fn prepared_encoding_quota_rejects_before_reserving_bind_rows() {
        let runtime = ReadRuntime::new(ReadLimits::default()).unwrap();
        let frame = runtime
            .run(1, |work| async move {
                let model =
                    serde_json::json!({"custom_properties":{"large":"x".repeat(4*1024*1024)}});
                let mut rows: Vec<EpisodeFields> = Vec::new();
                let mut charged = 0;
                assert!(matches!(
                    prepared_row(&work, &mut charged, &model, 14, &mut rows),
                    Err(ReadError::Capacity)
                ));
                assert_eq!(rows.capacity(), 0);
                Ok(Vec::new())
            })
            .await
            .unwrap();
        drop(frame);
        assert!(runtime.shutdown(std::time::Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
}
