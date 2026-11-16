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
/// Source merges only nonblank missing CP keys across provider occurrences of
/// one global identity; a later category must not replace the first category.
fn merge_properties(existing: &mut Value, incoming: Option<&Value>) {
    let Some(incoming) = incoming.and_then(Value::as_object) else {
        return;
    };
    if existing.is_null() {
        *existing = json!({});
    }
    let Some(existing) = existing.as_object_mut() else {
        return;
    };
    let blank = |v: &Value| {
        v.is_null()
            || v.as_str() == Some("")
            || v.as_array()
                .is_some_and(|xs| xs.iter().all(|x| x.is_null() || x.as_str() == Some("")))
    };
    for (key, value) in incoming {
        if !blank(value) && existing.get(key).is_none_or(blank) {
            existing.insert(key.clone(), value.clone());
        }
    }
}
/// Caller must first prove the bounded catalogue domain; the current broad
/// guard is intentionally unchanged while advanced Source observations are pending.
/// One batch transaction, with actual session closure before returning to HTTP.
pub(super) async fn apply(
    auth: &AuthService,
    owner: Arc<dyn Send + Sync>,
    account: i64,
    rows: Vec<(Series, i64)>,
    scan: DateTime<Utc>,
) -> Result<(), StoreError> {
    let session = auth.borrow_task_sql_session(owner).await?;
    let observed = session.run(move |c| Box::pin(async move {
        sqlx::query("BEGIN").execute(&mut *c).await?;
        let observed=async {
            let existing=sqlx::query(ADVANCED_SERIES_EXISTING).fetch_all(&mut *c).await?;
            if existing.len()>128{return Err(unsupported());}
            let mut identities=HashMap::<Identity,i64>::new();
            for row in existing {if identities.insert((row.try_get("name")?,row.try_get("year")?),row.try_get("id")?).is_some(){return Err(unsupported());}}
            let mut positions=HashMap::<Identity,usize>::new();let mut representatives=Vec::<Value>::new();
            for(series,_)in &rows {
                let key=(series.name.clone(),series.year);
                if let Some(index)=positions.get(&key).copied(){
                    // Source merges the first provided category metadata for a global identity.
                    merge_properties(&mut representatives[index]["properties"],series.properties.as_ref());
                }else{positions.insert(key,representatives.len());representatives.push(json!({"uuid":uuid::Uuid::new_v4(),"name":series.name,"year":series.year,"properties":series.properties,"stamp":Utc::now()}));}
            }
            let mut new=Vec::new();
            for value in &representatives { if !identities.contains_key(&identity(value)?) { new.push(value.clone()); } }
            let created=sqlx::query(ADVANCED_SERIES_INSERT).bind(json!(new)).fetch_all(&mut*c).await?;
            for row in created{identities.insert((row.try_get("name")?,row.try_get("year")?),row.try_get("id")?);}
            // Sparse list fields preserve detail metadata and timestamps. Source
            // clears an existing logo when the provider list omits cover.
            // All nonblank provider description/rating/genre/cover stays refused.
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
const ADVANCED_SERIES_INSERT: &str = "INSERT INTO vod_series(uuid,name,description,year,rating,genre,logo_id,tmdb_id,imdb_id,language,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name','',(v->>'year')::integer,NULL,'',NULL,NULL,NULL,'',NULLIF(v->'properties','null'::jsonb),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM jsonb_array_elements($1) v RETURNING id,name,year";
const ADVANCED_SERIES_UPDATE: &str = "UPDATE vod_series s SET custom_properties=CASE WHEN v->'properties'='null'::jsonb THEN s.custom_properties ELSE COALESCE(s.custom_properties,'{}'::jsonb)||(v->'properties') END,logo_id=NULL FROM jsonb_array_elements($1) v WHERE s.name=v->>'name' AND s.year IS NOT DISTINCT FROM (v->>'year')::integer AND s.tmdb_id IS NULL AND s.imdb_id IS NULL AND s.language='' AND (s.logo_id IS NOT NULL OR (v->'properties'<>'null'::jsonb AND s.custom_properties IS DISTINCT FROM (COALESCE(s.custom_properties,'{}'::jsonb)||(v->'properties'))))";

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
        let mut properties = json!({"category_id":"11","releaseDate":"2010-01-01","empty":[]});
        merge_properties(
            &mut properties,
            Some(
                &json!({"category_id":"12","releaseDate":"2010-02-01","empty":"filled","blank":"","zero":0}),
            ),
        );
        assert_eq!(
            properties,
            json!({"category_id":"11","releaseDate":"2010-01-01","empty":"filled","zero":0})
        );
        let mut absent = Value::Null;
        merge_properties(&mut absent, Some(&json!({"releaseDate":"2010-01-01"})));
        merge_properties(&mut absent, Some(&json!({"category_id":"11"})));
        assert_eq!(
            absent,
            json!({"releaseDate":"2010-01-01","category_id":"11"})
        );
    }
}
