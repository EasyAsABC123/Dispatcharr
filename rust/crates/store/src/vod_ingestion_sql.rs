//! Short existing-Main SQL scopes for initial VOD list ingestion.
//! Original Dispatcharr/Django/PostgreSQL authors retain credit.
use super::{unsupported, values, Account, Owner};
use crate::{AuthService, StoreError};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, Row};
use std::collections::HashMap;

async fn scope<T, F>(auth: &AuthService, owner: Owner, work: F) -> Result<T, StoreError>
where
    T: Send + 'static,
    F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, StoreError>>
        + Send
        + 'static,
{
    let session = auth.borrow_task_sql_session(owner).await?;
    let observed = session.run(work).await;
    session.close().await?;
    observed?
}
pub(super) async fn account(
    auth: &AuthService,
    id: i64,
    owner: Owner,
) -> Result<Option<Account>, StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{
        let row=sqlx::query("SELECT a.name,a.account_type,a.is_active,CASE WHEN COALESCE(octet_length(a.last_message),0)<=65536 THEN a.last_message END AS last_message,CASE WHEN COALESCE(octet_length(a.server_url),0)<=65536 THEN a.server_url END AS server_url,CASE WHEN COALESCE(octet_length(a.username),0)<=65536 THEN a.username END AS username,CASE WHEN COALESCE(octet_length(a.password),0)<=65536 THEN a.password END AS password,CASE WHEN COALESCE(octet_length(u.user_agent),0)<=65536 THEN u.user_agent END AS user_agent,CASE WHEN COALESCE(octet_length(a.custom_properties::text),0)<=65536 THEN a.custom_properties END AS custom_properties,(COALESCE(octet_length(a.last_message),0)<=65536 AND COALESCE(octet_length(a.server_url),0)<=65536 AND COALESCE(octet_length(a.username),0)<=65536 AND COALESCE(octet_length(a.password),0)<=65536 AND COALESCE(octet_length(u.user_agent),0)<=65536 AND COALESCE(octet_length(a.custom_properties::text),0)<=65536) AS fits FROM m3u_m3uaccount a LEFT JOIN core_useragent u ON u.id=a.user_agent_id WHERE a.id::bigint=$1").bind(id).fetch_optional(c).await?;
        let Some(row)=row else{return Ok(None)};
        if !row.try_get::<bool,_>("fits")?{return Ok(None);}
        if row.try_get::<String,_>("account_type")?!="XC"||!row.try_get::<bool,_>("is_active")?{return Ok(None);}
        let properties=row.try_get::<Option<Value>,_>("custom_properties")?.unwrap_or(Value::Null);
        let props=if crate::xc_live_provider::truthy(&properties){properties.as_object().ok_or_else(unsupported)?.clone()}else{serde_json::Map::new()};
        if !props.get("enable_vod").is_some_and(crate::xc_live_provider::truthy){return Ok(None);}
        let flag=|key:&str|match props.get(key){None=>Ok(true),Some(Value::Bool(value))=>Ok(*value),_=>Err(unsupported())};
        let movie_enabled=flag("auto_enable_new_groups_vod")?;let series_enabled=flag("auto_enable_new_groups_series")?;
        let text=|key:&str|->Result<String,StoreError>{let value=row.try_get::<Option<String>,_>(key)?.unwrap_or_default();if value.len()>65536{return Err(unsupported());}Ok(value)};
        let account=Account{id,name:row.try_get("name")?,base:text("server_url")?,username:text("username")?,password:text("password")?,agent:row.try_get("user_agent")?,message:row.try_get("last_message")?,movie_enabled,series_enabled};
        if account.base.is_empty()||account.username.is_empty()||account.password.is_empty()||!account.movie_enabled||!account.series_enabled{return Ok(None);}
        Ok(Some(account))
    })).await
}
pub(super) async fn supported_catalog(
    auth: &AuthService,
    owner: Owner,
) -> Result<bool, StoreError> {
    let valid=scope(auth,owner.clone(),|c|Box::pin(async move{
        Ok(sqlx::query_scalar::<_,bool>("SELECT NOT EXISTS(SELECT 1 FROM vod_episode) AND NOT EXISTS(SELECT 1 FROM vod_m3uepisoderelation) AND (SELECT count(*) FROM (SELECT 1 FROM vod_series LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3useriesrelation LIMIT 257)s)<=256 AND NOT EXISTS(SELECT 1 FROM vod_series WHERE year IS NOT NULL OR tmdb_id IS NOT NULL OR imdb_id IS NOT NULL OR language<>'' OR logo_id IS NOT NULL OR COALESCE(description,'')<>'' OR rating IS NOT NULL OR COALESCE(genre,'')<>'' OR (custom_properties IS NOT NULL AND CASE WHEN jsonb_typeof(custom_properties)='object' THEN (custom_properties-'category_id')<>'{}'::jsonb OR octet_length(custom_properties::text)>65536 OR (custom_properties?'category_id' AND CASE jsonb_typeof(custom_properties->'category_id') WHEN 'string' THEN length(custom_properties->>'category_id')>255 WHEN 'number' THEN (custom_properties->>'category_id')!~'^-?[0-9]+$' ELSE true END) ELSE true END)) AND NOT EXISTS(SELECT 1 FROM vod_series GROUP BY name,year HAVING count(*)>1) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND (SELECT count(*) FROM (SELECT 1 FROM vod_movie LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3umovierelation LIMIT 257)s)<=256 AND (SELECT count(*) FROM (SELECT 1 FROM vod_vodcategory LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3uvodcategoryrelation LIMIT 257)s)<=256 AND NOT EXISTS(SELECT 1 FROM vod_movie WHERE tmdb_id IS NOT NULL OR imdb_id IS NOT NULL OR language<>'' OR logo_id IS NOT NULL OR COALESCE(description,'')<>'' OR rating IS NOT NULL OR COALESCE(genre,'')<>'' OR duration_secs IS NOT NULL OR (custom_properties IS NOT NULL AND custom_properties<>'{}'::jsonb)) AND NOT EXISTS(SELECT 1 FROM vod_movie GROUP BY name,year HAVING count(*)>1) AND NOT EXISTS(SELECT 1 FROM vod_m3umovierelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation WHERE NOT enabled OR (custom_properties IS NOT NULL AND custom_properties<>'{}'::jsonb))").fetch_one(c).await?)
    })).await?;
    if valid {
        return Ok(true);
    }
    supported_sparse_advanced_catalog(auth, owner).await
}
/// Provisional next-domain branch; reconcile with actual Source advanced4
/// before installation. Plain admission above remains the original predicate.
/// Each Series has one provider relation, so crossaccount descendant ownership
/// stays refused. Metadata is opaque, bounded, and preserved by sparse updates.
async fn supported_sparse_advanced_catalog(
    auth: &AuthService,
    owner: Owner,
) -> Result<bool, StoreError> {
    scope(auth, owner, |c| {
        Box::pin(async move {
            Ok(sqlx::query_scalar::<_, bool>(ADVANCED_GUARD)
                .fetch_one(c)
                .await?)
        })
    })
    .await
}
/// Preserve per-bulk-call commits rather than an encompassing root transaction.
pub(super) async fn categories(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    kind: &'static str,
    rows: &[values::Category],
    enabled: bool,
) -> Result<HashMap<String, i64>, StoreError> {
    let names: Vec<String> = rows.iter().map(|row| row.name.clone()).collect();
    let wanted = names.clone();
    let stamp = Utc::now();
    scope(auth, owner.clone(), move |c| {
        Box::pin(async move {
            sqlx::query(ADVANCED_CATEGORY_INSERT)
                .bind(json!(names))
                .bind(kind)
                .bind(stamp)
                .execute(c)
                .await?;
            Ok(())
        })
    })
    .await?;
    let mapping=scope(auth,owner.clone(),move|c|Box::pin(async move{
        let rows=sqlx::query("SELECT id::bigint,name FROM vod_vodcategory WHERE category_type=$2 AND name=ANY($1) ORDER BY name").bind(&wanted).bind(kind).fetch_all(c).await?;
        rows.into_iter().map(|row|Ok((row.try_get::<String,_>("name")?,row.try_get::<i64,_>("id")?))).collect::<Result<HashMap<_,_>,StoreError>>()
    })).await?;
    let ids: Vec<i64> = rows.iter().map(|row| mapping[&row.name]).collect();
    scope(auth,owner,move|c|Box::pin(async move{
        sqlx::query("INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) SELECT $1::bigint,c.id,$3,'{}'::jsonb,$4,$4 FROM vod_vodcategory c WHERE c.id::bigint=ANY($2) ORDER BY array_position($2,c.id::bigint) ON CONFLICT(m3u_account_id,category_id) DO NOTHING").bind(account).bind(&ids).bind(enabled).bind(stamp).execute(&mut*c).await?;
        let removed:Vec<i64>=sqlx::query_scalar("DELETE FROM vod_m3uvodcategoryrelation r USING vod_vodcategory c WHERE r.category_id=c.id AND r.m3u_account_id::bigint=$1 AND c.category_type=$2 AND c.name<>'Uncategorized' AND NOT(c.id::bigint=ANY($3)) RETURNING r.category_id::bigint").bind(account).bind(kind).bind(&ids).fetch_all(&mut*c).await?;
        let mut delete_tx=c.begin().await?;
        let orphaned:Vec<i64>=sqlx::query_scalar("SELECT c.id FROM vod_vodcategory c WHERE c.id::bigint=ANY($1) AND NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation r WHERE r.category_id=c.id)").bind(&removed).fetch_all(&mut*delete_tx).await?;
        sqlx::query("UPDATE vod_m3umovierelation SET category_id=NULL WHERE category_id=ANY($1)").bind(&orphaned).execute(&mut*delete_tx).await?;
        sqlx::query("UPDATE vod_m3useriesrelation SET category_id=NULL WHERE category_id=ANY($1)").bind(&orphaned).execute(&mut*delete_tx).await?;
        sqlx::query("DELETE FROM vod_vodcategory WHERE id=ANY($1)").bind(&orphaned).execute(&mut*delete_tx).await?;delete_tx.commit().await?;Ok(())
    })).await?;
    rows.iter()
        .map(|row| {
            mapping
                .get(&row.name)
                .copied()
                .map(|id| (row.provider.clone(), id))
                .ok_or_else(unsupported)
        })
        .collect()
}
pub(super) async fn uncategorized(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    kind: &'static str,
    enabled: bool,
) -> Result<i64, StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{
        let stamp=Utc::now();
        sqlx::query(ADVANCED_UNCATEGORIZED_INSERT).bind(kind).bind(stamp).execute(&mut*c).await?;
        let id:i64=sqlx::query_scalar("SELECT id::bigint FROM vod_vodcategory WHERE name='Uncategorized' AND category_type=$1").bind(kind).fetch_one(&mut*c).await?;
        sqlx::query(ADVANCED_UNCATEGORIZED_RELATION_INSERT).bind(account).bind(id).bind(enabled).bind(stamp).execute(c).await?;Ok(id)
    })).await
}
pub(super) async fn enabled(
    auth: &AuthService,
    owner: Owner,
    account: i64,
) -> Result<HashMap<i64, bool>, StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{let rows=sqlx::query("SELECT category_id::bigint,enabled FROM vod_m3uvodcategoryrelation WHERE m3u_account_id::bigint=$1 LIMIT 129").bind(account).fetch_all(c).await?;if rows.len()>128{return Err(unsupported());}rows.into_iter().map(|row|Ok((row.try_get("category_id")?,row.try_get("enabled")?))).collect()})).await
}
pub(super) async fn movies(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    movies: Vec<(values::Movie, i64)>,
    scan: DateTime<Utc>,
) -> Result<(), StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{
        sqlx::query("BEGIN").execute(&mut*c).await?;
        let result=async{
            let existing=sqlx::query("SELECT id,name,year FROM vod_movie ORDER BY id LIMIT 129").fetch_all(&mut*c).await?;
            if existing.len()>128{return Err(unsupported());}
            let mut identities:HashMap<(String,Option<i32>),i64>=HashMap::new();
            for row in existing{if identities.insert((row.try_get("name")?,row.try_get("year")?),row.try_get("id")?).is_some(){return Err(unsupported());}}
            let mut representatives=Vec::new();let mut positions=HashMap::new();
            for(movie,_)in &movies {
                let key=(movie.name.clone(),movie.year);
                if let Some(index)=positions.get(&key).copied(){
                    let row:&mut Value=&mut representatives[index];
                    if row["adult"].is_null()&&movie.adult.is_some(){row["adult"]=json!(movie.adult);}
                }else{positions.insert(key,representatives.len());representatives.push(json!({"uuid":uuid::Uuid::new_v4(),"name":movie.name,"year":movie.year,"adult":movie.adult,"stamp":Utc::now()}));}
            }
            let new_rows:Vec<Value>=representatives.iter().filter(|row|!identities.contains_key(&(row["name"].as_str().unwrap_or_default().to_owned(),row["year"].as_i64().and_then(|v|i32::try_from(v).ok())))).cloned().collect();
            let created=sqlx::query("INSERT INTO vod_movie(uuid,name,description,year,rating,genre,duration_secs,logo_id,tmdb_id,imdb_id,language,is_adult,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name','',(v->>'year')::integer,NULL,'',NULL,NULL,NULL,NULL,'',COALESCE((v->>'adult')::boolean,false),NULL,(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM jsonb_array_elements($1) v RETURNING id,name,year").bind(json!(new_rows)).fetch_all(&mut*c).await?;
            for row in created{identities.insert((row.try_get("name")?,row.try_get("year")?),row.try_get("id")?);}
            // Source bulk_update does not touch updated_at; absent adult is not a false reset.
            sqlx::query("UPDATE vod_movie m SET is_adult=(v->>'adult')::boolean FROM jsonb_array_elements($1) v WHERE m.name=v->>'name' AND m.year IS NOT DISTINCT FROM (v->>'year')::integer AND v->>'adult' IS NOT NULL AND m.is_adult IS DISTINCT FROM (v->>'adult')::boolean").bind(json!(representatives)).execute(&mut*c).await?;
            let mut relations=Vec::new();
            for(movie,category)in movies{
                let id=*identities.get(&(movie.name.clone(),movie.year)).ok_or_else(unsupported)?;
                relations.push(json!({"movie":id,"category":category,"stream":movie.stream,"extension":movie.extension,"basic":values::basic(&movie,category),"stamp":Utc::now()}));
            }
            let rows=json!(relations);
            sqlx::query("INSERT INTO vod_m3umovierelation(m3u_account_id,movie_id,category_id,stream_id,container_extension,custom_properties,created_at,updated_at,last_advanced_refresh,last_seen) SELECT $1::bigint,(v->>'movie')::bigint,(v->>'category')::bigint,v->>'stream',v->>'extension',jsonb_build_object('basic_data',v->'basic','detailed_fetched',false),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz,NULL,$3 FROM jsonb_array_elements($2) v ON CONFLICT(m3u_account_id,stream_id) DO NOTHING").bind(account).bind(&rows).bind(scan).execute(&mut*c).await?;
            sqlx::query("UPDATE vod_m3umovierelation r SET movie_id=(v->>'movie')::bigint,category_id=(v->>'category')::bigint,container_extension=v->>'extension',custom_properties=COALESCE(r.custom_properties,'{}'::jsonb)||jsonb_build_object('basic_data',v->'basic')||CASE WHEN r.movie_id IS DISTINCT FROM (v->>'movie')::bigint THEN jsonb_build_object('detailed_fetched',false) ELSE '{}'::jsonb END,last_seen=$3 FROM jsonb_array_elements($2) v WHERE r.m3u_account_id=$1 AND r.stream_id=v->>'stream'").bind(account).bind(&rows).bind(scan).execute(&mut*c).await?;
            Ok::<_,StoreError>(())
        }.await;
        match result{Ok(())=>{sqlx::query("COMMIT").execute(c).await?;Ok(())},Err(error)=>{sqlx::query("ROLLBACK").execute(c).await?;Err(error)}}
    })).await
}
pub(super) async fn cleanup(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    scan: DateTime<Utc>,
) -> Result<(), StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{
        sqlx::query("DELETE FROM vod_m3umovierelation WHERE m3u_account_id::bigint=$1 AND last_seen<$2").bind(account).bind(scan).execute(&mut*c).await?;
        delete_stale_series_relations(c,account,scan).await?;
        sqlx::query("DELETE FROM vod_movie m WHERE NOT EXISTS(SELECT 1 FROM vod_m3umovierelation r WHERE r.movie_id=m.id)").execute(&mut*c).await?;
        delete_orphaned_series(c).await?;Ok(())
    })).await
}

/// Django's Series-relation deletion collects its Episode relations in the
/// same delete transaction. Deleting a stale provider must not delete Episode
/// objects still owned by the same global Series through another account.
async fn delete_stale_series_relations(
    c: &mut PgConnection,
    account: i64,
    scan: DateTime<Utc>,
) -> Result<(), StoreError> {
    let mut tx = c.begin().await?;
    let relations: Vec<i64> = sqlx::query_scalar(ADVANCED_STALE_SERIES)
        .bind(account)
        .bind(scan)
        .fetch_all(&mut *tx)
        .await?;
    if relations.len() > 256 {
        return Err(unsupported());
    }
    let children: Vec<i64> = sqlx::query_scalar(ADVANCED_STALE_CHILDREN)
        .bind(&relations)
        .fetch_all(&mut *tx)
        .await?;
    if children.len() > 4096 {
        return Err(unsupported());
    }
    sqlx::query(ADVANCED_DELETE_EPISODE_RELATIONS)
        .bind(&children)
        .execute(&mut *tx)
        .await?;
    sqlx::query(ADVANCED_DELETE_SERIES_RELATIONS)
        .bind(&relations)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
/// Collect only globally orphaned Series, then their full Episode descendants,
/// including Episodes with no stream relation. Preserve VODLogo rows. The
/// parent row locks prevent a newly attached provider from being silently lost.
/// Source catches orphan-delete IntegrityError; use one transaction for the
/// collected deletion, and roll it back on a foreign-key conflict only.
async fn delete_orphaned_series(c: &mut PgConnection) -> Result<(), StoreError> {
    let mut tx = c.begin().await?;
    let ids: Vec<i64> = sqlx::query_scalar(ADVANCED_ORPHAN_SERIES)
        .fetch_all(&mut *tx)
        .await?;
    if ids.len() > 128 {
        return Err(unsupported());
    }
    // SELECT FOR UPDATE may wait behind an older FK insertion. Recheck the
    // orphan predicate in a fresh statement AFTER all parent locks are held,
    // before deleting any descendant of a newly referenced Series.
    let ids: Vec<i64> = sqlx::query_scalar(ADVANCED_ORPHAN_RECHECK)
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
    let episodes: Vec<i64> = sqlx::query_scalar(ADVANCED_ORPHAN_EPISODES)
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
    if episodes.len() > 1024 {
        return Err(unsupported());
    }
    let children: Vec<i64> = sqlx::query_scalar(ADVANCED_ORPHAN_CHILDREN)
        .bind(&episodes)
        .fetch_all(&mut *tx)
        .await?;
    if children.len() > 4096 {
        return Err(unsupported());
    }
    let deleted = async {
        sqlx::query(ADVANCED_DELETE_EPISODE_RELATIONS)
            .bind(&children)
            .execute(&mut *tx)
            .await?;
        sqlx::query(ADVANCED_DELETE_EPISODES)
            .bind(&episodes)
            .execute(&mut *tx)
            .await?;
        sqlx::query(ADVANCED_DELETE_SERIES)
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        Ok::<_, sqlx::Error>(())
    }
    .await;
    match deleted {
        Ok(()) => {
            tx.commit().await?;
            Ok(())
        }
        Err(error) => {
            let conflict = error
                .as_database_error()
                .is_some_and(|e| e.code().as_deref() == Some("23503"));
            tx.rollback().await?;
            if conflict {
                Ok(())
            } else {
                Err(error.into())
            }
        }
    }
}

pub(super) async fn verify_untagged(auth: &AuthService, owner: Owner) -> Result<(), StoreError> {
    scope(auth,owner,|c|Box::pin(async move {
        // Current initial domain proves language '' throughout; no migration,
        // split or metadata merge is silently skipped for a tagged relation.
        if sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM vod_movie WHERE language<>'') OR EXISTS(SELECT 1 FROM vod_series WHERE language<>'') OR EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation WHERE custom_properties IS NOT NULL AND custom_properties<>'{}'::jsonb)").fetch_one(&mut*c).await?{return Err(unsupported());}
        Ok(())
    })).await
}

/// Ordinary get_object and natural REST refusal order, before runtime admission.
pub(super) async fn api_target(
    auth: &AuthService,
    id: i64,
    owner: Owner,
) -> Result<String, StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{
  let row=sqlx::query("SELECT name,account_type,custom_properties FROM m3u_m3uaccount WHERE id::bigint=$1").bind(id).fetch_optional(c).await?.ok_or(StoreError::NotFound)?;
  if row.try_get::<String,_>("account_type")?!="XC"{return Err(StoreError::StatusBody(400,json!({"error":"VOD refresh is only available for XtreamCodes accounts"})));}
  let props=row.try_get::<Option<Value>,_>("custom_properties")?.unwrap_or(Value::Null);
  if !props.as_object().and_then(|p|p.get("enable_vod")).is_some_and(crate::xc_live_provider::truthy){return Err(StoreError::StatusBody(400,json!({"error":"VOD is not enabled for this account"})));}
  Ok(row.try_get("name")?)
 })).await
}

pub(super) async fn has_selected_categories(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    kind: &'static str,
) -> Result<bool, StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation r JOIN vod_vodcategory c ON c.id=r.category_id WHERE r.m3u_account_id=$1 AND c.category_type=$2 AND c.name<>'Uncategorized')").bind(account).bind(kind).fetch_one(c).await?)})).await
}

const ADVANCED_GUARD: &str = "SELECT (SELECT count(*) FROM (SELECT 1 FROM vod_episode LIMIT 1025)s)<=1024 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3uepisoderelation LIMIT 4097)s)<=4096 AND NOT EXISTS(SELECT 1 FROM vod_episode WHERE octet_length(name)>255 OR COALESCE(octet_length(description),0)>65536 OR COALESCE(octet_length(rating),0)>10 OR (custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536))) AND NOT EXISTS(SELECT 1 FROM vod_series s WHERE (SELECT count(*) FROM vod_m3useriesrelation r WHERE r.series_id=s.id)<>1) AND NOT EXISTS(SELECT 1 FROM vod_m3uepisoderelation e WHERE e.series_relation_id IS NULL OR NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r JOIN vod_episode p ON p.id=e.episode_id WHERE r.id=e.series_relation_id AND r.series_id=p.series_id AND r.m3u_account_id=e.m3u_account_id)) AND NOT EXISTS(SELECT 1 FROM vod_m3uepisoderelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND (SELECT count(*) FROM (SELECT 1 FROM vod_series LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3useriesrelation LIMIT 257)s)<=256 AND NOT EXISTS(SELECT 1 FROM vod_series WHERE tmdb_id IS NOT NULL OR imdb_id IS NOT NULL OR language<>'' OR name ~ '[^ -~]' OR octet_length(name)>255 OR COALESCE(octet_length(description),0)>65536 OR COALESCE(octet_length(genre),0)>255 OR COALESCE(octet_length(rating),0)>10 OR (custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536))) AND NOT EXISTS(SELECT 1 FROM vod_series GROUP BY name,year HAVING count(*)>1) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND (SELECT count(*) FROM (SELECT 1 FROM vod_movie LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3umovierelation LIMIT 257)s)<=256 AND (SELECT count(*) FROM (SELECT 1 FROM vod_vodcategory LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3uvodcategoryrelation LIMIT 257)s)<=256 AND NOT EXISTS(SELECT 1 FROM vod_movie WHERE tmdb_id IS NOT NULL OR imdb_id IS NOT NULL OR language<>'' OR logo_id IS NOT NULL OR COALESCE(description,'')<>'' OR rating IS NOT NULL OR COALESCE(genre,'')<>'' OR duration_secs IS NOT NULL OR (custom_properties IS NOT NULL AND custom_properties<>'{}'::jsonb)) AND NOT EXISTS(SELECT 1 FROM vod_movie GROUP BY name,year HAVING count(*)>1) AND NOT EXISTS(SELECT 1 FROM vod_m3umovierelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation WHERE NOT enabled OR (custom_properties IS NOT NULL AND custom_properties<>'{}'::jsonb))";
const ADVANCED_STALE_SERIES: &str = "SELECT id::bigint FROM vod_m3useriesrelation WHERE m3u_account_id::bigint=$1 AND last_seen<$2 ORDER BY id LIMIT 257 FOR UPDATE";
const ADVANCED_STALE_CHILDREN: &str = "SELECT id::bigint FROM vod_m3uepisoderelation WHERE series_relation_id::bigint=ANY($1) ORDER BY id LIMIT 4097 FOR UPDATE";
const ADVANCED_DELETE_EPISODE_RELATIONS: &str =
    "DELETE FROM vod_m3uepisoderelation WHERE id::bigint=ANY($1)";
const ADVANCED_DELETE_SERIES_RELATIONS: &str =
    "DELETE FROM vod_m3useriesrelation WHERE id::bigint=ANY($1)";
const ADVANCED_ORPHAN_SERIES: &str = "SELECT s.id::bigint FROM vod_series s WHERE NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.series_id=s.id) ORDER BY s.id LIMIT 129 FOR UPDATE";
const ADVANCED_ORPHAN_RECHECK: &str = "SELECT s.id::bigint FROM vod_series s WHERE s.id::bigint=ANY($1) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.series_id=s.id) ORDER BY s.id";
const ADVANCED_ORPHAN_EPISODES: &str = "SELECT id::bigint FROM vod_episode WHERE series_id::bigint=ANY($1) ORDER BY id LIMIT 1025 FOR UPDATE";
const ADVANCED_ORPHAN_CHILDREN: &str = "SELECT id::bigint FROM vod_m3uepisoderelation WHERE episode_id::bigint=ANY($1) ORDER BY id LIMIT 4097 FOR UPDATE";
const ADVANCED_DELETE_EPISODES: &str = "DELETE FROM vod_episode WHERE id::bigint=ANY($1)";
const ADVANCED_DELETE_SERIES: &str = "DELETE FROM vod_series s WHERE s.id::bigint=ANY($1) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.series_id=s.id)";

#[cfg(test)]
pub(super) fn schema_statements() -> Vec<(&'static str, String)> {
    vec![
        (
            "advanced_uncategorized_relation_insert",
            ADVANCED_UNCATEGORIZED_RELATION_INSERT.to_owned(),
        ),
        (
            "advanced_uncategorized_insert",
            ADVANCED_UNCATEGORIZED_INSERT.to_owned(),
        ),
        (
            "advanced_category_insert",
            ADVANCED_CATEGORY_INSERT.to_owned(),
        ),
        ("advanced_guard", ADVANCED_GUARD.to_owned()),
        ("advanced_stale_series", ADVANCED_STALE_SERIES.to_owned()),
        (
            "advanced_stale_children",
            ADVANCED_STALE_CHILDREN.to_owned(),
        ),
        (
            "advanced_delete_episode_relations",
            ADVANCED_DELETE_EPISODE_RELATIONS.to_owned(),
        ),
        (
            "advanced_delete_series_relations",
            ADVANCED_DELETE_SERIES_RELATIONS.to_owned(),
        ),
        ("advanced_orphan_series", ADVANCED_ORPHAN_SERIES.to_owned()),
        (
            "advanced_orphan_recheck",
            ADVANCED_ORPHAN_RECHECK.to_owned(),
        ),
        (
            "advanced_orphan_episodes",
            ADVANCED_ORPHAN_EPISODES.to_owned(),
        ),
        (
            "advanced_orphan_children",
            ADVANCED_ORPHAN_CHILDREN.to_owned(),
        ),
        (
            "advanced_delete_episodes",
            ADVANCED_DELETE_EPISODES.to_owned(),
        ),
        ("advanced_delete_series", ADVANCED_DELETE_SERIES.to_owned()),
    ]
}

const ADVANCED_CATEGORY_INSERT: &str = "INSERT INTO vod_vodcategory(name,category_type,created_at,updated_at) SELECT incoming.value,$2,$3,$3 FROM jsonb_array_elements_text($1) AS incoming(value) WHERE NOT EXISTS(SELECT 1 FROM vod_vodcategory c WHERE c.name=incoming.value AND c.category_type=$2) ON CONFLICT(name,category_type) DO NOTHING";

const ADVANCED_UNCATEGORIZED_INSERT: &str = "INSERT INTO vod_vodcategory(name,category_type,created_at,updated_at) SELECT 'Uncategorized',$1,$2,$2 WHERE NOT EXISTS(SELECT 1 FROM vod_vodcategory c WHERE c.name='Uncategorized' AND c.category_type=$1) ON CONFLICT(name,category_type) DO NOTHING";

const ADVANCED_UNCATEGORIZED_RELATION_INSERT: &str = "INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) SELECT $1::bigint,$2::bigint,$3,'{}'::jsonb,$4,$4 WHERE NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation r WHERE r.m3u_account_id::bigint=$1 AND r.category_id::bigint=$2) ON CONFLICT(m3u_account_id,category_id) DO NOTHING";
