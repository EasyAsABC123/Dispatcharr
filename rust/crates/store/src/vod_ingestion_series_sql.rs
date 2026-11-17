//! Atomic bounded Series list batches from Original process_series_batch.
//! Original Dispatcharr/Django/PostgreSQL authors retain all application credit.
use super::series_values::Series;
use crate::{AuthService, StoreError};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::Row;
use std::{collections::HashMap, sync::Arc};
fn unsupported() -> StoreError {
    StoreError::Unsupported("VOD plain Series batch ownership/domain unavailable")
}
type Identity = (String, Option<i32>);
fn identity(value: &Value) -> Result<Identity, StoreError> {
    let name = value
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(unsupported)?
        .to_owned();
    let year = match value.get("year") {
        Some(Value::Null) => None,
        Some(Value::Number(v)) => Some(
            v.as_i64()
                .and_then(|n| i32::try_from(n).ok())
                .ok_or_else(unsupported)?,
        ),
        _ => return Err(unsupported()),
    };
    Ok((name, year))
}
/// Caller must first prove the bounded catalogue domain; the new metadata
/// extension remains provisional until its full Source batch is reconciled.
/// One batch transaction, with actual session closure before returning to HTTP.
pub(super) async fn apply(
    auth: &AuthService,
    owner: Arc<dyn Send + Sync>,
    account: i64,
    rows: Vec<(Series, i64)>,
    scan: DateTime<Utc>,
) -> Result<(), StoreError> {
    let mut positions = HashMap::<Identity, usize>::new();
    let mut representatives = Vec::<Value>::new();
    for (series, _) in &rows {
        let key = (series.name.clone(), series.year);
        let incoming = json!({"uuid":uuid::Uuid::new_v4(),"name":series.name,"year":series.year,"description":series.description,"rating":series.rating,"genre":series.genre,"properties":series.properties,"logo":series.logo,"stamp":Utc::now()});
        if let Some(index) = positions.get(&key).copied() {
            super::sql::merge_representative(&mut representatives[index], &incoming);
        } else {
            positions.insert(key, representatives.len());
            representatives.push(incoming);
        }
    }
    super::sql::prepare_logos(auth, owner.clone(), account, "series", &mut representatives).await?;
    let session = auth.borrow_task_sql_session(owner).await?;
    let observed = session.run(move |c| Box::pin(async move {
        sqlx::query("BEGIN").execute(&mut *c).await?;
        let observed=async {
            let existing=sqlx::query(ADVANCED_SERIES_EXISTING).fetch_all(&mut *c).await?;
            if existing.len()>128{return Err(unsupported());}
            let mut identities=HashMap::<Identity,i64>::new();
            for row in existing {if identities.insert((row.try_get("name")?,row.try_get("year")?),row.try_get("id")?).is_some(){return Err(unsupported());}}
            let mut new=Vec::new();
            for value in &representatives { if !identities.contains_key(&identity(value)?) { new.push(value.clone()); } }
            let created=sqlx::query(ADVANCED_SERIES_INSERT).bind(json!(new)).fetch_all(&mut*c).await?;
            for row in created{identities.insert((row.try_get("name")?,row.try_get("year")?),row.try_get("id")?);}
            // Source overwrites only nonblank list fields/CP, preserving UUID
            // and model timestamps. Absent/overlong cover clears association.
            sqlx::query(ADVANCED_SERIES_UPDATE).bind(json!(representatives)).execute(&mut*c).await?;
            let mut relations=Vec::new();
            for(series,category)in rows {
                let id=*identities.get(&(series.name.clone(),series.year)).ok_or_else(unsupported)?;
                relations.push(json!({"series":id,"category":category,"external":series.external,"basic":super::series_values::basic(&series,category),"stamp":Utc::now()}));
            }
            let relations=json!(relations);
            sqlx::query(ADVANCED_SERIES_RELATION_INSERT).bind(account).bind(&relations).bind(scan).execute(&mut*c).await?;
            sqlx::query("UPDATE vod_m3useriesrelation r SET series_id=(v->>'series')::bigint,category_id=(v->>'category')::bigint,custom_properties=COALESCE(r.custom_properties,'{}'::jsonb)||jsonb_build_object('basic_data',v->'basic')||CASE WHEN r.series_id IS DISTINCT FROM (v->>'series')::bigint THEN jsonb_build_object('detailed_fetched',false,'episodes_fetched',false) ELSE '{}'::jsonb END,last_seen=$3 FROM jsonb_array_elements($2) v WHERE r.m3u_account_id=$1 AND r.external_series_id=v->>'external'").bind(account).bind(&relations).bind(scan).execute(&mut*c).await?;
            Ok::<_,StoreError>(())
        }.await;
        match observed {Ok(())=>{sqlx::query("COMMIT").execute(c).await?;Ok(())},Err(error)=>{sqlx::query("ROLLBACK").execute(c).await?;Err(error)}}
    })).await;
    session.close().await?;
    observed?
}

const ADVANCED_SERIES_EXISTING: &str = "SELECT id,name,year FROM vod_series WHERE tmdb_id IS NULL AND imdb_id IS NULL AND language='' ORDER BY id LIMIT 129";
const ADVANCED_SERIES_INSERT: &str = "INSERT INTO vod_series(uuid,name,description,year,rating,genre,logo_id,tmdb_id,imdb_id,language,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name',v->>'description',(v->>'year')::integer,v->>'rating',v->>'genre',(v->>'logo_id')::bigint,NULL,NULL,'',NULLIF(v->'properties','null'::jsonb),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM jsonb_array_elements($1) v RETURNING id,name,year";
const ADVANCED_SERIES_UPDATE: &str = "WITH projected AS MATERIALIZED (SELECT s.id,CASE WHEN v->>'description'<>'' THEN v->>'description' ELSE s.description END AS description, COALESCE(v->>'rating',s.rating) AS rating, CASE WHEN v->>'genre'<>'' THEN v->>'genre' ELSE s.genre END AS genre, CASE WHEN (COALESCE(s.custom_properties,'{}'::jsonb)||COALESCE((SELECT jsonb_object_agg(p.key,p.value) FROM jsonb_each(COALESCE(NULLIF(v->'properties','null'::jsonb),'{}'::jsonb)) p WHERE NOT (p.value IN ('null'::jsonb,'\"\"'::jsonb,'[]'::jsonb) OR (jsonb_typeof(p.value)='array' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.value)='array' THEN p.value ELSE '[]'::jsonb END) element WHERE element NOT IN ('null'::jsonb,'\"\"'::jsonb))))),'{}'::jsonb)) IS DISTINCT FROM COALESCE(s.custom_properties,'{}'::jsonb) THEN COALESCE(s.custom_properties,'{}'::jsonb)||COALESCE((SELECT jsonb_object_agg(p.key,p.value) FROM jsonb_each(COALESCE(NULLIF(v->'properties','null'::jsonb),'{}'::jsonb)) p WHERE NOT (p.value IN ('null'::jsonb,'\"\"'::jsonb,'[]'::jsonb) OR (jsonb_typeof(p.value)='array' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.value)='array' THEN p.value ELSE '[]'::jsonb END) element WHERE element NOT IN ('null'::jsonb,'\"\"'::jsonb))))),'{}'::jsonb) ELSE s.custom_properties END AS custom_properties, (v->>'logo_id')::bigint AS logo_id FROM vod_series s CROSS JOIN jsonb_array_elements($1) v WHERE s.name=v->>'name' AND s.year IS NOT DISTINCT FROM (v->>'year')::integer AND s.tmdb_id IS NULL AND s.imdb_id IS NULL AND s.language='') UPDATE vod_series s SET description=p.description,rating=p.rating,genre=p.genre,custom_properties=p.custom_properties,logo_id=p.logo_id FROM projected p WHERE s.id=p.id AND ROW(s.description,s.rating,s.genre,s.custom_properties,s.logo_id) IS DISTINCT FROM ROW(p.description,p.rating,p.genre,p.custom_properties,p.logo_id)";

#[cfg(test)]
pub(super) fn schema_statements() -> Vec<(&'static str, String)> {
    vec![
        (
            "advanced_series_relation_insert",
            ADVANCED_SERIES_RELATION_INSERT.to_owned(),
        ),
        (
            "advanced_series_existing",
            ADVANCED_SERIES_EXISTING.to_owned(),
        ),
        ("advanced_series_insert", ADVANCED_SERIES_INSERT.to_owned()),
        ("advanced_series_update", ADVANCED_SERIES_UPDATE.to_owned()),
    ]
}

const ADVANCED_SERIES_RELATION_INSERT: &str = "INSERT INTO vod_m3useriesrelation(m3u_account_id,series_id,category_id,external_series_id,custom_properties,created_at,updated_at,last_episode_refresh,last_seen) SELECT $1::bigint,(v->>'series')::bigint,(v->>'category')::bigint,v->>'external',jsonb_build_object('basic_data',v->'basic','detailed_fetched',false,'episodes_fetched',false),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz,NULL,$3 FROM jsonb_array_elements($2) v WHERE NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.m3u_account_id::bigint=$1 AND r.external_series_id=v->>'external') ON CONFLICT(m3u_account_id,external_series_id) DO NOTHING";

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn global_identity_distinguishes_details_year_from_yearless_list_row() {
        let detailed = identity(&json!({"name":"Owned","year":2010})).unwrap();
        let yearless = identity(&json!({"name":"Owned","year":null})).unwrap();
        assert_ne!(detailed, yearless);
        assert_eq!(
            detailed,
            identity(&json!({"name":"Owned","year":2010})).unwrap()
        );
        for bad in [
            json!({"name":"Owned"}),
            json!({"name":"Owned","year":"2010"}),
            json!({"name":"Owned","year":2147483648i64}),
        ] {
            assert!(identity(&bad).is_err());
        }
    }
    #[test]
    fn source_coalesced_provider_occurrences_fill_only_missing_nonblank_keys() {
        let mut representative =
            json!({"properties":{"category_id":"11","releaseDate":"2010-01-01","empty":[]}});
        super::super::sql::merge_representative(
            &mut representative,
            &json!({"properties":{"category_id":"12","releaseDate":"2010-02-01","empty":"filled","blank":"","zero":0}}),
        );
        assert_eq!(
            representative["properties"],
            json!({"category_id":"11","releaseDate":"2010-01-01","empty":"filled","zero":0})
        );
        let mut absent = json!({"properties":null});
        super::super::sql::merge_representative(
            &mut absent,
            &json!({"properties":{"releaseDate":"2010-01-01"}}),
        );
        super::super::sql::merge_representative(
            &mut absent,
            &json!({"properties":{"category_id":"11"}}),
        );
        assert_eq!(
            absent["properties"],
            json!({"releaseDate":"2010-01-01","category_id":"11"})
        );
    }
}
