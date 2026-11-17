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
#[cfg(test)]
type Identity = (String, Option<i32>);
#[cfg(test)]
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
    let mut positions = HashMap::<String, usize>::new();
    let mut representatives = Vec::<Value>::new();
    let mut occurrences = Vec::<Vec<usize>>::new();
    for (ordinal, (item, _)) in rows.iter().enumerate() {
        let incoming = json!({"uuid":uuid::Uuid::new_v4(),"name":item.name,"year":item.year,"tmdb_id":item.tmdb_id,"imdb_id":item.imdb_id,"description":item.description,"rating":item.rating,"genre":item.genre,"properties":item.properties,"logo":item.logo,"stamp":Utc::now()});
        let key = super::sql::list_identity(&incoming);
        if let Some(index) = positions.get(&key).copied() {
            super::sql::merge_representative(&mut representatives[index], &incoming);
            if !occurrences[index]
                .iter()
                .any(|old| rows[*old].0.external == item.external)
            {
                occurrences[index].push(ordinal);
            }
        } else {
            positions.insert(key, representatives.len());
            representatives.push(incoming);
            occurrences.push(vec![ordinal]);
        }
    }
    // Cross-key duplicate provider identities were not in Source9. Refuse them
    // before logo writes rather than import an arbitrary SQL UPDATE winner.
    let mut provider_keys = std::collections::HashSet::new();
    for group in &occurrences {
        for ordinal in group {
            if !provider_keys.insert(rows[*ordinal].0.external.clone()) {
                return Err(unsupported());
            }
        }
    }
    super::sql::prepare_logos(auth, owner.clone(), &mut representatives).await?;
    let session = auth.borrow_task_sql_session(owner).await?;
    let observed=session.run(move|c|Box::pin(async move{
        sqlx::query("BEGIN").execute(&mut*c).await?;
        let result=async{
            let initial=super::sql::list_snapshot(sqlx::query(ADVANCED_SERIES_EXISTING).fetch_all(&mut*c).await?)?;
            for row in &mut representatives {row["initial_id"]=json!(super::sql::list_existing(row,&initial,false));row["resolved_id"]=row["initial_id"].clone();}
            let mut initial_targets=std::collections::HashSet::new();
            for row in &representatives { if let Some(id)=row["initial_id"].as_i64() { if !initial_targets.insert(id) { return Err(unsupported()); } } }
            // Fresh database snapshot, not a map extended with other candidates.
            let second=super::sql::list_snapshot(sqlx::query(ADVANCED_SERIES_EXISTING).fetch_all(&mut*c).await?)?;
            for row in &mut representatives {if row["initial_id"].is_null() {row["resolved_id"]=json!(super::sql::list_existing(row,&second,true));}}
            let new_rows:Vec<Value>=representatives.iter().filter(|row|row["resolved_id"].is_null()).cloned().collect();
            let created=sqlx::query(ADVANCED_SERIES_INSERT).bind(json!(new_rows)).fetch_all(&mut*c).await?;
            for row in created {let token:uuid::Uuid=row.try_get("uuid")?;let id:i64=row.try_get("id")?;let representative=representatives.iter_mut().find(|v|v["uuid"].as_str()==Some(token.to_string().as_str())).ok_or_else(unsupported)?;representative["resolved_id"]=json!(id);}
            let updates:Vec<Value>=representatives.iter().filter(|v|!v["initial_id"].is_null()).cloned().collect();
            sqlx::query(ADVANCED_SERIES_UPDATE).bind(json!(updates)).execute(&mut*c).await?;
            let mut relations=Vec::new();
            for (representative,group) in representatives.iter().zip(&occurrences) {
                let id=representative["resolved_id"].as_i64().ok_or_else(unsupported)?;
                for ordinal in group {let(item,category)=&rows[*ordinal];relations.push(json!({"series":id,"category":category,"external":item.external,"basic":super::series_values::basic(item,*category),"candidate":representative["initial_id"].is_null(),"stamp":Utc::now()}));}
            }
            let relations=json!(relations);
            sqlx::query(ADVANCED_SERIES_RELATION_INSERT).bind(account).bind(&relations).bind(scan).execute(&mut*c).await?;
            sqlx::query(ADVANCED_SERIES_RELATION_UPDATE).bind(account).bind(&relations).bind(scan).execute(&mut*c).await?;
            Ok::<_,StoreError>(())
        }.await;
        match result{Ok(())=>{sqlx::query("COMMIT").execute(c).await?;Ok(())},Err(error)=>{sqlx::query("ROLLBACK").execute(c).await?;match error{StoreError::Database(_)=>Ok(()),other=>Err(other)}}}
    })).await;
    session.close().await?;
    observed?
}

const ADVANCED_SERIES_EXISTING:&str="SELECT id::bigint,name,year,tmdb_id,imdb_id FROM vod_series WHERE language='' ORDER BY id LIMIT 129";
const ADVANCED_SERIES_INSERT: &str = "INSERT INTO vod_series(uuid,name,description,year,rating,genre,logo_id,tmdb_id,imdb_id,language,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name',v->>'description',(v->>'year')::integer,v->>'rating',v->>'genre',(v->>'logo_id')::bigint,v->>'tmdb_id',v->>'imdb_id','',NULLIF(v->'properties','null'::jsonb),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM jsonb_array_elements($1) v RETURNING id::bigint,uuid";
const ADVANCED_SERIES_UPDATE: &str = "WITH projected AS MATERIALIZED (SELECT s.id,COALESCE((v->>'year')::integer,s.year) AS year,COALESCE(v->>'tmdb_id',s.tmdb_id) AS tmdb_id,COALESCE(v->>'imdb_id',s.imdb_id) AS imdb_id,CASE WHEN v->>'description'<>'' THEN v->>'description' ELSE s.description END AS description, COALESCE(v->>'rating',s.rating) AS rating, CASE WHEN v->>'genre'<>'' THEN v->>'genre' ELSE s.genre END AS genre, CASE WHEN (COALESCE(s.custom_properties,'{}'::jsonb)||COALESCE((SELECT jsonb_object_agg(p.key,p.value) FROM jsonb_each(COALESCE(NULLIF(v->'properties','null'::jsonb),'{}'::jsonb)) p WHERE NOT (p.value IN ('null'::jsonb,'\"\"'::jsonb,'[]'::jsonb) OR (jsonb_typeof(p.value)='array' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.value)='array' THEN p.value ELSE '[]'::jsonb END) element WHERE element NOT IN ('null'::jsonb,'\"\"'::jsonb))))),'{}'::jsonb)) IS DISTINCT FROM COALESCE(s.custom_properties,'{}'::jsonb) THEN COALESCE(s.custom_properties,'{}'::jsonb)||COALESCE((SELECT jsonb_object_agg(p.key,p.value) FROM jsonb_each(COALESCE(NULLIF(v->'properties','null'::jsonb),'{}'::jsonb)) p WHERE NOT (p.value IN ('null'::jsonb,'\"\"'::jsonb,'[]'::jsonb) OR (jsonb_typeof(p.value)='array' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.value)='array' THEN p.value ELSE '[]'::jsonb END) element WHERE element NOT IN ('null'::jsonb,'\"\"'::jsonb))))),'{}'::jsonb) ELSE s.custom_properties END AS custom_properties, (v->>'logo_id')::bigint AS logo_id FROM vod_series s CROSS JOIN jsonb_array_elements($1) v WHERE s.id::bigint=(v->>'initial_id')::bigint) UPDATE vod_series s SET year=p.year,tmdb_id=p.tmdb_id,imdb_id=p.imdb_id,description=p.description,rating=p.rating,genre=p.genre,custom_properties=p.custom_properties,logo_id=p.logo_id FROM projected p WHERE s.id=p.id AND ROW(s.year,s.tmdb_id,s.imdb_id,s.description,s.rating,s.genre,s.custom_properties,s.logo_id) IS DISTINCT FROM ROW(p.year,p.tmdb_id,p.imdb_id,p.description,p.rating,p.genre,p.custom_properties,p.logo_id)";

#[cfg(test)]
pub(super) fn schema_statements() -> Vec<(&'static str, String)> {
    vec![
        (
            "advanced_series_relation_update",
            ADVANCED_SERIES_RELATION_UPDATE.to_owned(),
        ),
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

const ADVANCED_SERIES_RELATION_INSERT: &str = "INSERT INTO vod_m3useriesrelation(m3u_account_id,series_id,category_id,external_series_id,custom_properties,created_at,updated_at,last_episode_refresh,last_seen) SELECT $1::bigint,(v->>'series')::bigint,(v->>'category')::bigint,v->>'external',jsonb_build_object('basic_data',v->'basic','detailed_fetched',false,'episodes_fetched',false),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz,NULL,$3 FROM jsonb_array_elements($2) WITH ORDINALITY AS input(v,ordinal) WHERE NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.m3u_account_id::bigint=$1 AND r.external_series_id=v->>'external') ORDER BY input.ordinal ON CONFLICT(m3u_account_id,external_series_id) DO NOTHING";

const ADVANCED_SERIES_RELATION_UPDATE:&str="UPDATE vod_m3useriesrelation r SET series_id=(v->>'series')::bigint,category_id=(v->>'category')::bigint,custom_properties=COALESCE(r.custom_properties,'{}'::jsonb)||jsonb_build_object('basic_data',v->'basic')||CASE WHEN COALESCE((v->>'candidate')::boolean,false) OR r.series_id IS DISTINCT FROM (v->>'series')::bigint THEN jsonb_build_object('detailed_fetched',false,'episodes_fetched',false) ELSE '{}'::jsonb END,last_seen=$3 FROM jsonb_array_elements($2) v WHERE r.m3u_account_id=$1 AND r.external_series_id=v->>'external'";

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
