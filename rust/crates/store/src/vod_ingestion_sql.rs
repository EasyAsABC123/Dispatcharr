//! Short existing-Main SQL scopes for initial VOD list ingestion.
//! Original Dispatcharr/Django/PostgreSQL authors retain credit.
use super::{unsupported, values, Account, Owner};
use crate::{AuthService, StoreError};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use sqlx::{PgConnection, Row};
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
        let row=sqlx::query("SELECT a.name,a.account_type,a.is_active,CASE WHEN COALESCE(octet_length(a.last_message),0)<=65536 THEN a.last_message END AS last_message,CASE WHEN COALESCE(octet_length(a.server_url),0)<=65536 THEN a.server_url END AS server_url,CASE WHEN COALESCE(octet_length(a.username),0)<=65536 THEN a.username END AS username,CASE WHEN COALESCE(octet_length(a.password),0)<=65536 THEN a.password END AS password,CASE WHEN COALESCE(octet_length(u.user_agent),0)<=65536 THEN u.user_agent END AS user_agent,CASE WHEN COALESCE(octet_length(a.custom_properties::text),0)<=65536 THEN a.custom_properties END AS custom_properties,(COALESCE(octet_length(a.last_message),0)<=65536 AND COALESCE(octet_length(a.server_url),0)<=65536 AND COALESCE(octet_length(a.username),0)<=65536 AND COALESCE(octet_length(a.password),0)<=65536 AND COALESCE(octet_length(u.user_agent),0)<=65536 AND COALESCE(octet_length(a.custom_properties::text),0)<=65536) AS fits FROM m3u_m3uaccount a LEFT JOIN core_useragent u ON u.id=a.user_agent_id WHERE a.id::bigint=$1 AND NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation r WHERE r.m3u_account_id=a.id)").bind(id).fetch_optional(c).await?;
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
pub(super) async fn fresh(auth: &AuthService, owner: Owner) -> Result<bool, StoreError> {
    let valid=scope(auth,owner,|c|Box::pin(async move{
        Ok(sqlx::query_scalar::<_,bool>("SELECT NOT EXISTS(SELECT 1 FROM vod_movie) AND NOT EXISTS(SELECT 1 FROM vod_series) AND NOT EXISTS(SELECT 1 FROM vod_episode) AND NOT EXISTS(SELECT 1 FROM vod_m3umovierelation) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation) AND NOT EXISTS(SELECT 1 FROM vod_m3uepisoderelation) AND (SELECT count(*) FROM (SELECT 1 FROM vod_vodcategory LIMIT 129)s)<=128 AND NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation WHERE NOT enabled OR (custom_properties IS NOT NULL AND custom_properties<>'{}'::jsonb))").fetch_one(c).await?)
    })).await?;
    Ok(valid)
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
    scope(auth,owner.clone(),move|c|Box::pin(async move{
        sqlx::query("INSERT INTO vod_vodcategory(name,category_type,created_at,updated_at) SELECT value,$2,$3,$3 FROM jsonb_array_elements_text($1) ON CONFLICT(name,category_type) DO NOTHING").bind(json!(names)).bind(kind).bind(stamp).execute(c).await?;Ok(())
    })).await?;
    let mapping=scope(auth,owner.clone(),move|c|Box::pin(async move{
        let rows=sqlx::query("SELECT id::bigint,name FROM vod_vodcategory WHERE category_type=$2 AND name=ANY($1) ORDER BY name").bind(&wanted).bind(kind).fetch_all(c).await?;
        rows.into_iter().map(|row|Ok((row.try_get::<String,_>("name")?,row.try_get::<i64,_>("id")?))).collect::<Result<HashMap<_,_>,StoreError>>()
    })).await?;
    let ids: Vec<i64> = rows.iter().map(|row| mapping[&row.name]).collect();
    scope(auth,owner,move|c|Box::pin(async move{
        sqlx::query("INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) SELECT $1::bigint,c.id,$3,'{}'::jsonb,$4,$4 FROM vod_vodcategory c WHERE c.id::bigint=ANY($2) ORDER BY array_position($2,c.id::bigint) ON CONFLICT(m3u_account_id,category_id) DO NOTHING").bind(account).bind(&ids).bind(enabled).bind(stamp).execute(&mut*c).await?;
        let removed:Vec<i64>=sqlx::query_scalar("DELETE FROM vod_m3uvodcategoryrelation r USING vod_vodcategory c WHERE r.category_id=c.id AND r.m3u_account_id::bigint=$1 AND c.category_type=$2 AND c.name<>'Uncategorized' AND NOT(c.id::bigint=ANY($3)) RETURNING r.category_id::bigint").bind(account).bind(kind).bind(&ids).fetch_all(&mut*c).await?;
        sqlx::query("DELETE FROM vod_vodcategory c WHERE c.id::bigint=ANY($1) AND NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation r WHERE r.category_id=c.id)").bind(&removed).execute(c).await?;Ok(())
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
        sqlx::query("INSERT INTO vod_vodcategory(name,category_type,created_at,updated_at) VALUES('Uncategorized',$1,$2,$2) ON CONFLICT(name,category_type) DO NOTHING").bind(kind).bind(stamp).execute(&mut*c).await?;
        let id:i64=sqlx::query_scalar("SELECT id::bigint FROM vod_vodcategory WHERE name='Uncategorized' AND category_type=$1").bind(kind).fetch_one(&mut*c).await?;
        sqlx::query("INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) VALUES($1::bigint,$2::bigint,$3,'{}'::jsonb,$4,$4) ON CONFLICT(m3u_account_id,category_id) DO NOTHING").bind(account).bind(id).bind(enabled).bind(stamp).execute(c).await?;Ok(id)
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
            if sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM vod_movie) OR EXISTS(SELECT 1 FROM vod_m3umovierelation)").fetch_one(&mut*c).await?{return Err(unsupported());}
            let mut identities:HashMap<(String,Option<i32>),i64>=HashMap::new();
            let mut new_rows=Vec::new();let mut unique=std::collections::HashSet::new();
            for(movie,_)in &movies {
                if unique.insert((movie.name.clone(),movie.year)){
                    new_rows.push(json!({"uuid":uuid::Uuid::new_v4(),"name":movie.name,"year":movie.year,"adult":movie.adult,"stamp":Utc::now()}));
                }
            }
            let created=sqlx::query("INSERT INTO vod_movie(uuid,name,description,year,rating,genre,duration_secs,logo_id,tmdb_id,imdb_id,language,is_adult,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name','',(v->>'year')::integer,NULL,'',NULL,NULL,NULL,NULL,'',(v->>'adult')::boolean,NULL,(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM jsonb_array_elements($1) v RETURNING id::bigint,name,year").bind(json!(new_rows)).fetch_all(&mut*c).await?;
            for row in created{identities.insert((row.try_get("name")?,row.try_get("year")?),row.try_get("id")?);}
            let mut relations=Vec::new();
            for(movie,category)in movies{
                let id=*identities.get(&(movie.name.clone(),movie.year)).ok_or_else(unsupported)?;
                relations.push(json!({"movie":id,"category":category,"stream":movie.stream,"extension":movie.extension,"props":{"basic_data":values::basic(&movie,category),"detailed_fetched":false},"stamp":Utc::now()}));
            }
            sqlx::query("INSERT INTO vod_m3umovierelation(m3u_account_id,movie_id,category_id,stream_id,container_extension,custom_properties,created_at,updated_at,last_advanced_refresh,last_seen) SELECT $1::bigint,(v->>'movie')::bigint,(v->>'category')::bigint,v->>'stream',v->>'extension',v->'props',(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz,NULL,$3 FROM jsonb_array_elements($2) v ON CONFLICT(m3u_account_id,stream_id) DO NOTHING").bind(account).bind(json!(relations)).bind(scan).execute(&mut*c).await?;
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
        sqlx::query("DELETE FROM vod_m3useriesrelation WHERE m3u_account_id::bigint=$1 AND last_seen<$2").bind(account).bind(scan).execute(&mut*c).await?;
        sqlx::query("DELETE FROM vod_movie m WHERE NOT EXISTS(SELECT 1 FROM vod_m3umovierelation r WHERE r.movie_id=m.id)").execute(&mut*c).await?;
        sqlx::query("DELETE FROM vod_series s WHERE NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.series_id=s.id)").execute(c).await?;Ok(())
    })).await
}

pub(super) async fn verify_untagged(auth: &AuthService, owner: Owner) -> Result<(), StoreError> {
    scope(auth,owner,|c|Box::pin(async move {
        // Current initial domain proves language '' throughout; no migration,
        // split or metadata merge is silently skipped for a tagged relation.
        if sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM vod_movie WHERE language<>'') OR EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation WHERE custom_properties IS NOT NULL AND custom_properties<>'{}'::jsonb)").fetch_one(&mut*c).await?{return Err(unsupported());}
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
