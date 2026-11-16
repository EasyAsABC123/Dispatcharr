//! Private uncompiled atomic plain-Series batch from Original process_series_batch.
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
/// Caller must first prove the bounded existing plain-Series catalogue domain.
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
            let existing=sqlx::query("SELECT id,name FROM vod_series WHERE year IS NULL ORDER BY id LIMIT 129").fetch_all(&mut *c).await?;
            if existing.len()>128{return Err(unsupported());}
            let mut identities=HashMap::<String,i64>::new();
            for row in existing {if identities.insert(row.try_get("name")?,row.try_get("id")?).is_some(){return Err(unsupported());}}
            let mut positions=HashMap::<String,usize>::new();let mut representatives=Vec::<Value>::new();
            for(series,_)in &rows {
                if let Some(index)=positions.get(&series.name).copied(){
                    // Source merges the first provided category metadata for a global identity.
                    if representatives[index]["properties"].is_null()&&series.properties.is_some(){representatives[index]["properties"]=json!(series.properties);}
                }else{positions.insert(series.name.clone(),representatives.len());representatives.push(json!({"uuid":uuid::Uuid::new_v4(),"name":series.name,"properties":series.properties,"stamp":Utc::now()}));}
            }
            let new:Vec<_>=representatives.iter().filter(|v|!identities.contains_key(v["name"].as_str().unwrap_or_default())).cloned().collect();
            let created=sqlx::query("INSERT INTO vod_series(uuid,name,description,year,rating,genre,logo_id,tmdb_id,imdb_id,language,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name','',NULL,NULL,'',NULL,NULL,NULL,'',NULLIF(v->'properties','null'::jsonb),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM jsonb_array_elements($1) v RETURNING id,name").bind(json!(new)).fetch_all(&mut*c).await?;
            for row in created{identities.insert(row.try_get("name")?,row.try_get("id")?);}
            // Source bulk_update leaves timestamps alone and merges provided list CP.
            sqlx::query("UPDATE vod_series s SET custom_properties=COALESCE(s.custom_properties,'{}'::jsonb)||(v->'properties') FROM jsonb_array_elements($1) v WHERE s.name=v->>'name' AND s.year IS NULL AND v->'properties'<>'null'::jsonb AND s.custom_properties IS DISTINCT FROM (COALESCE(s.custom_properties,'{}'::jsonb)||(v->'properties'))").bind(json!(representatives)).execute(&mut*c).await?;
            let mut relations=Vec::new();
            for(series,category)in rows {
                let id=*identities.get(&series.name).ok_or_else(unsupported)?;
                relations.push(json!({"series":id,"category":category,"external":series.external,"basic":super::series_values::basic(&series,category),"stamp":Utc::now()}));
            }
            let relations=json!(relations);
            sqlx::query("INSERT INTO vod_m3useriesrelation(m3u_account_id,series_id,category_id,external_series_id,custom_properties,created_at,updated_at,last_episode_refresh,last_seen) SELECT $1::bigint,(v->>'series')::bigint,(v->>'category')::bigint,v->>'external',jsonb_build_object('basic_data',v->'basic','detailed_fetched',false,'episodes_fetched',false),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz,NULL,$3 FROM jsonb_array_elements($2) v ON CONFLICT(m3u_account_id,external_series_id) DO NOTHING").bind(account).bind(&relations).bind(scan).execute(&mut*c).await?;
            sqlx::query("UPDATE vod_m3useriesrelation r SET series_id=(v->>'series')::bigint,category_id=(v->>'category')::bigint,custom_properties=COALESCE(r.custom_properties,'{}'::jsonb)||jsonb_build_object('basic_data',v->'basic')||CASE WHEN r.series_id IS DISTINCT FROM (v->>'series')::bigint THEN jsonb_build_object('detailed_fetched',false,'episodes_fetched',false) ELSE '{}'::jsonb END,last_seen=$3 FROM jsonb_array_elements($2) v WHERE r.m3u_account_id=$1 AND r.external_series_id=v->>'external'").bind(account).bind(&relations).bind(scan).execute(&mut*c).await?;
            Ok::<_,StoreError>(())
        }.await;
        match observed {Ok(())=>{sqlx::query("COMMIT").execute(c).await?;Ok(())},Err(error)=>{sqlx::query("ROLLBACK").execute(c).await?;Err(error)}}
    })).await;
    session.close().await?;
    observed?
}
