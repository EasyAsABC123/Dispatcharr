//! Original Dispatcharr Episode split/migration ownership, preserving stream IDs.
use super::{bounded, number, unsupported};
use crate::StoreError;
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::PgConnection;
#[derive(Clone, Copy)]
enum Op {
    Rows,
    Relations,
    Collision,
    MoveEpisode,
    MoveAllRelations,
    MoveRelations,
    Insert,
    DeleteRelations,
    Delete,
    Empty,
    HasRelations,
}
#[cfg(test)]
const OPS: [Op; 11] = [
    Op::Rows,
    Op::Relations,
    Op::Collision,
    Op::MoveEpisode,
    Op::MoveAllRelations,
    Op::MoveRelations,
    Op::Insert,
    Op::DeleteRelations,
    Op::Delete,
    Op::Empty,
    Op::HasRelations,
];
fn statement(op: Op) -> &'static str {
    match op {
        Op::Rows=>"SELECT to_jsonb(e) AS item FROM vod_episode e WHERE e.series_id::bigint=$1 ORDER BY e.season_number,e.episode_number,e.id LIMIT 2001",
        Op::Relations=>"SELECT jsonb_build_object('id',r.id,'episode_id',r.episode_id,'destination',COALESCE(s.series_id,e.series_id)) AS item FROM vod_m3uepisoderelation r JOIN vod_episode e ON e.id=r.episode_id LEFT JOIN vod_m3useriesrelation s ON s.id=r.series_relation_id WHERE e.series_id::bigint=$1 ORDER BY r.id LIMIT 2001",
        Op::Collision=>"SELECT to_jsonb(e) AS item FROM vod_episode e WHERE e.series_id::bigint=$2 AND e.season_number IS NOT DISTINCT FROM ($1->>'season_number')::integer AND e.episode_number IS NOT DISTINCT FROM ($1->>'episode_number')::integer AND e.id::bigint<>($1->>'id')::bigint ORDER BY e.id LIMIT 1",
        Op::MoveEpisode=>"UPDATE vod_episode SET series_id=$2::bigint WHERE id::bigint=$1",
        Op::MoveAllRelations=>"UPDATE vod_m3uepisoderelation SET episode_id=$2::bigint WHERE episode_id::bigint=$1",
        Op::MoveRelations=>"UPDATE vod_m3uepisoderelation SET episode_id=$2::bigint WHERE id::bigint=ANY($1)",
        Op::Insert=>"INSERT INTO vod_episode(uuid,name,description,air_date,rating,duration_secs,series_id,season_number,episode_number,tmdb_id,imdb_id,custom_properties,created_at,updated_at) SELECT (v->>'new_uuid')::uuid,v->>'name',v->>'description',(v->>'air_date')::date,v->>'rating',(v->>'duration_secs')::integer,(v->>'target_series')::bigint,(v->>'season_number')::integer,(v->>'episode_number')::integer,v->>'tmdb_id',v->>'imdb_id',NULLIF(v->'custom_properties','null'::jsonb),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM (SELECT $1::jsonb AS v)s RETURNING to_jsonb(vod_episode) AS item",
        Op::DeleteRelations=>"DELETE FROM vod_m3uepisoderelation WHERE episode_id::bigint=$1",
        Op::Delete=>"DELETE FROM vod_episode WHERE id::bigint=$1",
        Op::Empty=>"DELETE FROM vod_episode e WHERE e.series_id::bigint=ANY($1) AND NOT EXISTS(SELECT 1 FROM vod_m3uepisoderelation r WHERE r.episode_id=e.id)",
        Op::HasRelations=>"SELECT EXISTS(SELECT 1 FROM vod_m3uepisoderelation WHERE episode_id::bigint=$1) AS present",
    }
}
async fn collision(
    c: &mut PgConnection,
    episode: &Value,
    target: i64,
) -> Result<Option<Value>, StoreError> {
    Ok(sqlx::query_scalar::<_, Value>(statement(Op::Collision))
        .bind(episode)
        .bind(target)
        .fetch_optional(c)
        .await?)
}
async fn delete(c: &mut PgConnection, id: i64) -> Result<(), StoreError> {
    sqlx::query(statement(Op::DeleteRelations))
        .bind(id)
        .execute(&mut *c)
        .await?;
    sqlx::query(statement(Op::Delete))
        .bind(id)
        .execute(c)
        .await?;
    Ok(())
}
pub(super) async fn migrate(
    c: &mut PgConnection,
    source: i64,
    target: i64,
) -> Result<(), StoreError> {
    let rows = bounded(
        sqlx::query_scalar::<_, Value>(statement(Op::Rows))
            .bind(source)
            .fetch_all(&mut *c)
            .await?,
    )?;
    for episode in rows {
        let id = number(&episode, "id")?;
        if let Some(other) = collision(c, &episode, target).await? {
            sqlx::query(statement(Op::MoveAllRelations))
                .bind(id)
                .bind(number(&other, "id")?)
                .execute(&mut *c)
                .await?;
            delete(c, id).await?;
        } else {
            sqlx::query(statement(Op::MoveEpisode))
                .bind(id)
                .bind(target)
                .execute(&mut *c)
                .await?;
        }
    }
    Ok(())
}
fn winner(destinations: &[(i64, Vec<i64>)]) -> Option<i64> {
    destinations
        .iter()
        .max_by_key(|(id, rows)| (rows.len(), *id))
        .map(|(id, _)| *id)
}
async fn clone(c: &mut PgConnection, episode: &Value, target: i64) -> Result<Value, StoreError> {
    if let Some(other) = collision(c, episode, target).await? {
        return Ok(other);
    }
    let mut body = episode.clone();
    body["new_uuid"] = json!(uuid::Uuid::new_v4());
    body["target_series"] = json!(target);
    body["stamp"] = json!(Utc::now());
    Ok(sqlx::query_scalar::<_, Value>(statement(Op::Insert))
        .bind(body)
        .fetch_one(c)
        .await?)
}
pub(super) async fn split(c: &mut PgConnection, source: i64) -> Result<(), StoreError> {
    let rows = bounded(
        sqlx::query_scalar::<_, Value>(statement(Op::Rows))
            .bind(source)
            .fetch_all(&mut *c)
            .await?,
    )?;
    if rows.is_empty() {
        return Ok(());
    }
    let relations = bounded(
        sqlx::query_scalar::<_, Value>(statement(Op::Relations))
            .bind(source)
            .fetch_all(&mut *c)
            .await?,
    )?;
    for mut episode in rows {
        let id = number(&episode, "id")?;
        let mut destinations = Vec::<(i64, Vec<i64>)>::new();
        for rel in relations
            .iter()
            .filter(|r| r["episode_id"].as_i64() == Some(id))
        {
            let target = number(rel, "destination")?;
            let relation = number(rel, "id")?;
            if let Some((_, ids)) = destinations.iter_mut().find(|(key, _)| *key == target) {
                ids.push(relation);
            } else {
                destinations.push((target, vec![relation]));
            }
        }
        if destinations.is_empty() {
            delete(c, id).await?;
            continue;
        }
        if destinations.len() == 1 && destinations[0].0 == source {
            continue;
        }
        let target = winner(&destinations).ok_or_else(unsupported)?;
        if target != source {
            if let Some(other) = collision(c, &episode, target).await? {
                let index = destinations
                    .iter()
                    .position(|(key, _)| *key == target)
                    .ok_or_else(unsupported)?;
                sqlx::query(statement(Op::MoveRelations))
                    .bind(&destinations[index].1)
                    .bind(number(&other, "id")?)
                    .execute(&mut *c)
                    .await?;
                destinations[index].1.clear();
            } else {
                sqlx::query(statement(Op::MoveEpisode))
                    .bind(id)
                    .bind(target)
                    .execute(&mut *c)
                    .await?;
                episode["series_id"] = json!(target);
            }
        }
        let home = number(&episode, "series_id")?;
        for (target, ids) in destinations {
            if target == home || ids.is_empty() {
                continue;
            }
            let other = clone(c, &episode, target).await?;
            sqlx::query(statement(Op::MoveRelations))
                .bind(&ids)
                .bind(number(&other, "id")?)
                .execute(&mut *c)
                .await?;
        }
        let present: bool = sqlx::query_scalar(statement(Op::HasRelations))
            .bind(id)
            .fetch_one(&mut *c)
            .await?;
        if !present {
            delete(c, id).await?;
        }
    }
    Ok(())
}
pub(super) async fn delete_empty_candidates(
    c: &mut PgConnection,
    ids: &[i64],
) -> Result<(), StoreError> {
    sqlx::query(statement(Op::Empty))
        .bind(ids)
        .execute(c)
        .await?;
    Ok(())
}
#[cfg(test)]
pub(super) fn schema_statements() -> Vec<(&'static str, String)> {
    let names = [
        "language_episode_rows",
        "language_episode_relations",
        "language_episode_collision",
        "language_episode_move",
        "language_episode_move_all_relations",
        "language_episode_move_relations",
        "language_episode_insert",
        "language_episode_delete_relations",
        "language_episode_delete",
        "language_episode_empty",
        "language_episode_has_relations",
    ];
    OPS.into_iter()
        .enumerate()
        .map(|(i, op)| (names[i], statement(op).to_owned()))
        .collect()
}
#[cfg(test)]
pub(super) fn schema_parameters(name: &str) -> Option<Vec<sqlx::postgres::PgTypeInfo>> {
    use sqlx::Type;
    let i = <i64 as Type<sqlx::Postgres>>::type_info();
    let a = <Vec<i64> as Type<sqlx::Postgres>>::type_info();
    let j = <Value as Type<sqlx::Postgres>>::type_info();
    Some(match name.strip_prefix("language_episode_")? {
        "rows" | "relations" | "delete_relations" | "delete" | "has_relations" => vec![i],
        "collision" => vec![j, i],
        "move" | "move_all_relations" => vec![i.clone(), i],
        "move_relations" => vec![a, i],
        "insert" => vec![j],
        "empty" => vec![a],
        _ => return None,
    })
}
#[cfg(test)]
pub(super) fn schema_columns(name: &str) -> Option<Vec<(&'static str, &'static str)>> {
    Some(match name.strip_prefix("language_episode_")? {
        "rows" | "relations" | "collision" | "insert" => vec![("item", "JSONB")],
        "has_relations" => vec![("present", "BOOL")],
        "move" | "move_all_relations" | "move_relations" | "delete_relations" | "delete"
        | "empty" => vec![],
        _ => return None,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_source_episode_tie_keeps_original_with_higher_new_series_id() {
        assert_eq!(winner(&[(1, vec![11]), (2, vec![12])]), Some(2));
        assert_eq!(winner(&[(1, vec![11, 13]), (2, vec![12])]), Some(1));
        assert_eq!(winner(&[]), None);
    }
}
