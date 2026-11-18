//! Short existing-Main SQL scopes for initial VOD list ingestion.
//! Original Dispatcharr/Django/PostgreSQL authors retain credit.
use super::{unsupported, values, Account, Owner};
use crate::{AuthService, StoreError};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use futures_util::TryStreamExt;
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
        if account.base.is_empty()||account.username.is_empty()||account.password.is_empty(){return Ok(None);}
        Ok(Some(account))
    })).await
}
pub(super) async fn supported_catalog(
    auth: &AuthService,
    owner: Owner,
) -> Result<bool, StoreError> {
    let valid=scope(auth,owner.clone(),|c|Box::pin(async move{
        Ok(sqlx::query_scalar::<_,bool>("SELECT NOT EXISTS(SELECT 1 FROM vod_episode) AND NOT EXISTS(SELECT 1 FROM vod_m3uepisoderelation) AND (SELECT count(*) FROM (SELECT 1 FROM vod_series LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3useriesrelation LIMIT 257)s)<=256 AND NOT EXISTS(SELECT 1 FROM vod_series WHERE year IS NOT NULL OR tmdb_id IS NOT NULL OR imdb_id IS NOT NULL OR language<>'' OR logo_id IS NOT NULL OR COALESCE(description,'')<>'' OR rating IS NOT NULL OR COALESCE(genre,'')<>'' OR (custom_properties IS NOT NULL AND CASE WHEN jsonb_typeof(custom_properties)='object' THEN (custom_properties-'category_id')<>'{}'::jsonb OR octet_length(custom_properties::text)>65536 OR (custom_properties?'category_id' AND CASE jsonb_typeof(custom_properties->'category_id') WHEN 'string' THEN length(custom_properties->>'category_id')>255 WHEN 'number' THEN (custom_properties->>'category_id')!~'^-?[0-9]+$' ELSE true END) ELSE true END)) AND NOT EXISTS(SELECT 1 FROM vod_series GROUP BY name,year,language HAVING count(*)>1) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND (SELECT count(*) FROM (SELECT 1 FROM vod_movie LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3umovierelation LIMIT 257)s)<=256 AND (SELECT count(*) FROM (SELECT 1 FROM vod_vodcategory LIMIT 129)s)<=128 AND (SELECT count(*) FROM (SELECT 1 FROM vod_m3uvodcategoryrelation LIMIT 257)s)<=256 AND NOT EXISTS(SELECT 1 FROM vod_movie WHERE tmdb_id IS NOT NULL OR imdb_id IS NOT NULL OR language<>'' OR logo_id IS NOT NULL OR COALESCE(description,'')<>'' OR rating IS NOT NULL OR COALESCE(genre,'')<>'' OR duration_secs IS NOT NULL OR (custom_properties IS NOT NULL AND custom_properties<>'{}'::jsonb)) AND NOT EXISTS(SELECT 1 FROM vod_movie GROUP BY name,year,language HAVING count(*)>1) AND NOT EXISTS(SELECT 1 FROM vod_m3umovierelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation WHERE NOT enabled OR (custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536 OR custom_properties-'language'<>'{}'::jsonb OR (custom_properties?'language' AND custom_properties->'language'<>'null'::jsonb AND (jsonb_typeof(custom_properties->'language')<>'string' OR custom_properties->>'language'!~'^[A-Za-z]{2}$')))))").fetch_one(c).await?)
    })).await?;
    if valid {
        return Ok(true);
    }
    supported_sparse_advanced_catalog(auth, owner).await
}
/// Bounded metadata list domain; reconcile the next extension against Source
/// list-metadata captures before installation. Plain admission stays unchanged.
/// Source globals can be shared by multiple provider accounts. Each Episode relation
/// must still bind its actual provider Series relation and the same global Series.
/// The shared-account extension remains provisional until actual Source4 acceptance.
async fn supported_sparse_advanced_catalog(
    auth: &AuthService,
    owner: Owner,
) -> Result<bool, StoreError> {
    scope(auth, owner, |c| {
        Box::pin(async move {
            if !sqlx::query_scalar::<_,bool>(ADVANCED_GUARD).fetch_one(&mut *c).await? {
                return Ok(false);
            }
            // Admission bounds all subsequent category maps, including rows
            // returned by stale-relation collection, before any provider effect.
            // SQL aggregates scan without fetching unbounded rows into Rust.
            let(category_bytes,relation_bytes):(i64,i64)=sqlx::query_as(
                "SELECT COALESCE((SELECT sum(256::bigint+8::bigint*octet_length(name)) FROM vod_vodcategory),0)::bigint,COALESCE((SELECT sum(256::bigint+4::bigint*COALESCE(octet_length(custom_properties->>'language'),0)) FROM vod_m3uvodcategoryrelation),0)::bigint"
            ).fetch_one(c).await?;
            Ok((0..=values::CATEGORY_WORK_BYTES as i64).contains(&category_bytes)&&(0..=values::CATEGORY_WORK_BYTES as i64).contains(&relation_bytes))
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
    // Source distinguishes already-global categories from newly bulk-created
    // categories. Existing relation inputs retain provider order; new inputs
    // follow the Meta.ordering=name refetch, preserving serial allocation.
    let prior = wanted.clone();
    let existing_names=scope(auth,owner.clone(),move|c|Box::pin(async move{
        let mut stream=sqlx::query("SELECT name FROM vod_vodcategory WHERE category_type=$2 AND name=ANY($1) ORDER BY name").bind(&prior).bind(kind).fetch(c);
        let mut names=std::collections::HashSet::new();let mut work=0usize;
        while let Some(row)=stream.try_next().await?{
            let name:String=row.try_get("name")?;values::category_charge(&mut work,name.len())?;names.insert(name);
        }
        Ok(names)
    })).await?;
    let previously_present = existing_names.clone();
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
    let (mapping,new_ids)=scope(auth,owner.clone(),move|c|Box::pin(async move{
        let mut stream=sqlx::query("SELECT id::bigint,name FROM vod_vodcategory WHERE category_type=$2 AND name=ANY($1) ORDER BY name").bind(&wanted).bind(kind).fetch(c);
        let mut result=HashMap::new();let mut new_ids=Vec::new();let mut work=0usize;
        while let Some(row)=stream.try_next().await?{
            let name:String=row.try_get("name")?;
            values::category_charge(&mut work,name.len())?;
            let id:i64=row.try_get("id")?;
            if !previously_present.contains(&name){new_ids.push(id);}
            result.insert(name,id);
        }
        Ok((result,new_ids))
    })).await?;
    let mut ids: Vec<i64> = rows
        .iter()
        .filter(|row| existing_names.contains(&row.name))
        .map(|row| mapping[&row.name])
        .collect();
    ids.extend(new_ids);
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
) -> Result<(i64, String, bool), StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{
        let stamp=Utc::now();
        sqlx::query(ADVANCED_UNCATEGORIZED_INSERT).bind(kind).bind(stamp).execute(&mut*c).await?;
        let id:i64=sqlx::query_scalar("SELECT id::bigint FROM vod_vodcategory WHERE name='Uncategorized' AND category_type=$1").bind(kind).fetch_one(&mut*c).await?;
        sqlx::query(ADVANCED_UNCATEGORIZED_RELATION_INSERT).bind(account).bind(id).bind(enabled).bind(stamp).execute(&mut*c).await?;let(language,actual_enabled):(String,bool)=sqlx::query_as(LIST_UNCATEGORIZED_LANGUAGE).bind(account).bind(id).fetch_one(c).await?;Ok((id,language,actual_enabled))
    })).await
}
pub(super) async fn enabled(
    auth: &AuthService,
    owner: Owner,
    account: i64,
) -> Result<HashMap<i64, bool>, StoreError> {
    scope(auth,owner,move|c|Box::pin(async move{
        let mut stream=sqlx::query("SELECT category_id::bigint,enabled FROM vod_m3uvodcategoryrelation WHERE m3u_account_id::bigint=$1").bind(account).fetch(c);
        let mut result=HashMap::new();let mut work=0usize;
        while let Some(row)=stream.try_next().await?{
            values::category_charge(&mut work,16)?;
            result.insert(row.try_get::<i64,_>("category_id")?,row.try_get::<bool,_>("enabled")?);
        }
        Ok(result)
    })).await
}
/// One Source category-relation snapshot after both category refreshes.
/// The caller then replaces only each get/create Uncategorized entry.
pub(super) async fn category_languages(
    auth: &AuthService,
    owner: Owner,
    account: i64,
) -> Result<HashMap<i64, String>, StoreError> {
    scope(auth, owner, move |c| {
        Box::pin(async move {
            let mut stream = sqlx::query(LIST_CATEGORY_LANGUAGES).bind(account).fetch(c);
            let mut result = HashMap::new();
            let mut work = 0usize;
            while let Some(row) = stream.try_next().await? {
                let language: String = row.try_get("language")?;
                values::category_charge(&mut work, language.len())?;
                result.insert(row.try_get::<i64, _>("category")?, language);
            }
            Ok(result)
        })
    })
    .await
}

pub(super) struct ListPins {
    pub(super) targets: HashMap<String, i64>,
    pub(super) languages: HashMap<String, String>,
}
/// Source snapshots the provider relations before choosing language-aware keys.
/// A later category retag is applied by reconciliation, never by list identity.
pub(super) async fn list_pins(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    providers: Vec<String>,
    relation_sql: &'static str,
) -> Result<ListPins, StoreError> {
    scope(auth, owner, move |c| {
        Box::pin(async move {
            let rows = sqlx::query(relation_sql)
                .bind(account)
                .bind(&providers)
                .fetch_all(&mut *c)
                .await?;
            let mut targets = HashMap::new();
            let mut languages = HashMap::new();
            for row in rows {
                let provider: String = row.try_get("provider")?;
                targets.insert(provider.clone(), row.try_get("target")?);
                languages.insert(provider, row.try_get("language")?);
            }
            Ok(ListPins { targets, languages })
        })
    })
    .await
}

pub(super) async fn movies(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    movies: Vec<(values::Movie, i64)>,
    scan: DateTime<Utc>,
    category_languages: &HashMap<i64, String>,
) -> Result<(), StoreError> {
    let providers = movies.iter().map(|(item, _)| item.stream.clone()).collect();
    let pins = list_pins(
        auth,
        owner.clone(),
        account,
        providers,
        LIST_MOVIE_RELATION_EXISTING,
    )
    .await?;
    let mut positions = HashMap::<String, usize>::new();
    let mut representatives = Vec::<Value>::new();
    let mut occurrences = Vec::<Vec<usize>>::new();
    for (ordinal, (item, category)) in movies.iter().enumerate() {
        let language = pins
            .languages
            .get(&item.stream)
            .or_else(|| category_languages.get(category))
            .map(String::as_str)
            .unwrap_or("");
        let incoming = json!({"uuid":uuid::Uuid::new_v4(),"name":item.name,"year":item.year,"tmdb_id":item.tmdb_id,"imdb_id":item.imdb_id,"language":language,"description":item.description,"rating":item.rating,"genre":item.genre,"properties":item.properties,"logo":item.logo,"stamp":Utc::now(),"adult":item.adult,"duration":item.duration});
        let key = list_identity(&incoming);
        if let Some(index) = positions.get(&key).copied() {
            merge_representative(&mut representatives[index], &incoming);
            if !occurrences[index]
                .iter()
                .any(|old| movies[*old].0.stream == item.stream)
            {
                occurrences[index].push(ordinal);
            }
        } else {
            positions.insert(key, representatives.len());
            representatives.push(incoming);
            occurrences.push(vec![ordinal]);
        }
    }
    prepare_logos(auth, owner.clone(), &mut representatives).await?;
    let session = auth.borrow_task_sql_session(owner).await?;
    let observed=session.run(move|c|Box::pin(async move{
        sqlx::query("BEGIN").execute(&mut*c).await?;
        let result=async{
            let prefetched=pins.targets;
            let initial=list_snapshot(sqlx::query(LIST_MOVIE_EXISTING).bind(json!(representatives)).fetch_all(&mut*c).await?)?;
            for row in &mut representatives {row["initial_id"]=json!(list_existing(row,&initial,false));row["resolved_id"]=row["initial_id"].clone();}
            // Fresh database snapshot, not a map extended with other candidates.
            let second=list_snapshot(sqlx::query(LIST_MOVIE_EXISTING).bind(json!(representatives)).fetch_all(&mut*c).await?)?;
            for row in &mut representatives {if row["initial_id"].is_null() {row["resolved_id"]=json!(list_existing(row,&second,true));}}
            let new_rows:Vec<Value>=representatives.iter().filter(|row|row["resolved_id"].is_null()).cloned().collect();
            let created=sqlx::query(LIST_MOVIE_INSERT).bind(json!(new_rows)).fetch_all(&mut*c).await?;
            for row in created {let token:uuid::Uuid=row.try_get("uuid")?;let id:i64=row.try_get("id")?;let representative=representatives.iter_mut().find(|v|v["uuid"].as_str()==Some(token.to_string().as_str())).ok_or_else(unsupported)?;representative["resolved_id"]=json!(id);}
            let updates:Vec<Value>=representatives.iter().filter(|v|!v["initial_id"].is_null()).cloned().collect();
            sqlx::query(LIST_MOVIE_UPDATE).bind(json!(updates)).execute(&mut*c).await?;
            let mut relations=Vec::new();
            for (representative,group) in representatives.iter().zip(&occurrences) {
                let id=representative["resolved_id"].as_i64().ok_or_else(unsupported)?;
                for ordinal in group {let(item,category)=&movies[*ordinal];relations.push(json!({"movie":id,"category":category,"stream":item.stream,"basic":values::basic(item,*category),"candidate":representative["initial_id"].is_null(),"stamp":Utc::now(),"extension":item.extension}));}
            }
            let updates=existing_relation_updates(&relations,&prefetched,"stream","movie");
            let relations=json!(relations);
            sqlx::query(LIST_MOVIE_RELATION_INSERT).bind(account).bind(&relations).bind(scan).execute(&mut*c).await?;
            sqlx::query(LIST_MOVIE_RELATION_UPDATE).bind(account).bind(json!(updates)).bind(scan).execute(&mut*c).await?;
            Ok::<_,StoreError>(())
        }.await;
        match result{Ok(())=>{sqlx::query("COMMIT").execute(c).await?;Ok(())},Err(error)=>{sqlx::query("ROLLBACK").execute(c).await?;match error{StoreError::Database(_)=>Ok(()),other=>Err(other)}}}
    })).await;
    session.close().await?;
    observed?
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

const ADVANCED_GUARD: &str = "SELECT NOT EXISTS(SELECT 1 FROM vod_episode WHERE octet_length(name)>255 OR COALESCE(octet_length(description),0)>65536 OR COALESCE(octet_length(rating),0)>10 OR (custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536))) AND NOT EXISTS(SELECT 1 FROM vod_m3uepisoderelation e WHERE e.series_relation_id IS NULL OR NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r JOIN vod_episode p ON p.id=e.episode_id WHERE r.id=e.series_relation_id AND r.series_id=p.series_id AND r.m3u_account_id=e.m3u_account_id)) AND NOT EXISTS(SELECT 1 FROM vod_m3uepisoderelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND NOT EXISTS(SELECT 1 FROM vod_series WHERE COALESCE(char_length(tmdb_id),0)>50 OR COALESCE(octet_length(tmdb_id),0)>200 OR COALESCE(char_length(imdb_id),0)>50 OR COALESCE(octet_length(imdb_id),0)>200 OR COALESCE(tmdb_id,'') ~ U&'[\\0001-\\001F\\007F-\\009F]' OR COALESCE(imdb_id,'') ~ U&'[\\0001-\\001F\\007F-\\009F]' OR language !~ '^([a-z]{2})?$' OR char_length(name)>255 OR COALESCE(octet_length(description),0)>65536 OR COALESCE(octet_length(genre),0)>255 OR COALESCE(octet_length(rating),0)>10 OR (custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536))) AND NOT EXISTS(SELECT 1 FROM vod_series WHERE tmdb_id IS NULL AND imdb_id IS NULL GROUP BY name,year,language HAVING count(*)>1) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND NOT EXISTS(SELECT 1 FROM vod_movie WHERE COALESCE(char_length(tmdb_id),0)>50 OR COALESCE(octet_length(tmdb_id),0)>200 OR COALESCE(char_length(imdb_id),0)>50 OR COALESCE(octet_length(imdb_id),0)>200 OR COALESCE(tmdb_id,'') ~ U&'[\\0001-\\001F\\007F-\\009F]' OR COALESCE(imdb_id,'') ~ U&'[\\0001-\\001F\\007F-\\009F]' OR language !~ '^([a-z]{2})?$' OR char_length(name)>255 OR COALESCE(octet_length(description),0)>65536 OR COALESCE(octet_length(genre),0)>255 OR COALESCE(octet_length(rating),0)>10 OR (custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536))) AND NOT EXISTS(SELECT 1 FROM vod_movie WHERE tmdb_id IS NULL AND imdb_id IS NULL GROUP BY name,year,language HAVING count(*)>1) AND NOT EXISTS(SELECT 1 FROM vod_m3umovierelation WHERE custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536)) AND NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation WHERE (custom_properties IS NOT NULL AND (jsonb_typeof(custom_properties)<>'object' OR octet_length(custom_properties::text)>65536 OR (custom_properties?'language' AND custom_properties->'language'<>'null'::jsonb AND (jsonb_typeof(custom_properties->'language')<>'string' OR custom_properties->>'language'!~'^[A-Za-z]{2}$'))))) AND NOT EXISTS(SELECT 1 FROM vod_vodlogo l WHERE (EXISTS(SELECT 1 FROM vod_movie m WHERE m.logo_id=l.id) OR EXISTS(SELECT 1 FROM vod_series s WHERE s.logo_id=l.id)) AND (char_length(l.url)>500 OR char_length(l.name)>255))";
const ADVANCED_STALE_SERIES: &str = "SELECT id::bigint FROM vod_m3useriesrelation WHERE m3u_account_id::bigint=$1 AND last_seen<$2 ORDER BY id FOR UPDATE";
const ADVANCED_STALE_CHILDREN: &str = "SELECT id::bigint FROM vod_m3uepisoderelation WHERE series_relation_id::bigint=ANY($1) ORDER BY id LIMIT 4097 FOR UPDATE";
const ADVANCED_DELETE_EPISODE_RELATIONS: &str =
    "DELETE FROM vod_m3uepisoderelation WHERE id::bigint=ANY($1)";
const ADVANCED_DELETE_SERIES_RELATIONS: &str =
    "DELETE FROM vod_m3useriesrelation WHERE id::bigint=ANY($1)";
const ADVANCED_ORPHAN_SERIES: &str = "SELECT s.id::bigint FROM vod_series s WHERE NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.series_id=s.id) ORDER BY s.id FOR UPDATE";
const ADVANCED_ORPHAN_RECHECK: &str = "SELECT s.id::bigint FROM vod_series s WHERE s.id::bigint=ANY($1) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.series_id=s.id) ORDER BY s.id";
const ADVANCED_ORPHAN_EPISODES: &str = "SELECT id::bigint FROM vod_episode WHERE series_id::bigint=ANY($1) ORDER BY id LIMIT 1025 FOR UPDATE";
const ADVANCED_ORPHAN_CHILDREN: &str = "SELECT id::bigint FROM vod_m3uepisoderelation WHERE episode_id::bigint=ANY($1) ORDER BY id LIMIT 4097 FOR UPDATE";
const ADVANCED_DELETE_EPISODES: &str = "DELETE FROM vod_episode WHERE id::bigint=ANY($1)";
const ADVANCED_DELETE_SERIES: &str = "DELETE FROM vod_series s WHERE s.id::bigint=ANY($1) AND NOT EXISTS(SELECT 1 FROM vod_m3useriesrelation r WHERE r.series_id=s.id)";

#[cfg(test)]
pub(super) fn schema_statements() -> Vec<(&'static str, String)> {
    vec![
        (
            "list_movie_relation_existing",
            LIST_MOVIE_RELATION_EXISTING.to_owned(),
        ),
        (
            "list_movie_relation_update",
            LIST_MOVIE_RELATION_UPDATE.to_owned(),
        ),
        ("list_movie_existing", LIST_MOVIE_EXISTING.to_owned()),
        (
            "list_category_languages",
            LIST_CATEGORY_LANGUAGES.to_owned(),
        ),
        (
            "list_uncategorized_language",
            LIST_UNCATEGORIZED_LANGUAGE.to_owned(),
        ),
        ("list_logo_insert", LIST_LOGO_INSERT.to_owned()),
        ("list_logo_select", LIST_LOGO_SELECT.to_owned()),
        ("list_movie_insert", LIST_MOVIE_INSERT.to_owned()),
        ("list_movie_update", LIST_MOVIE_UPDATE.to_owned()),
        (
            "list_movie_relation_insert",
            LIST_MOVIE_RELATION_INSERT.to_owned(),
        ),
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

const ADVANCED_CATEGORY_INSERT: &str = "INSERT INTO vod_vodcategory(name,category_type,created_at,updated_at) SELECT incoming.value,$2,$3,$3 FROM jsonb_array_elements_text($1) WITH ORDINALITY AS incoming(value,ordinal) WHERE NOT EXISTS(SELECT 1 FROM vod_vodcategory c WHERE c.name=incoming.value AND c.category_type=$2) ORDER BY incoming.ordinal ON CONFLICT(name,category_type) DO NOTHING";

const ADVANCED_UNCATEGORIZED_INSERT: &str = "INSERT INTO vod_vodcategory(name,category_type,created_at,updated_at) SELECT 'Uncategorized',$1,$2,$2 WHERE NOT EXISTS(SELECT 1 FROM vod_vodcategory c WHERE c.name='Uncategorized' AND c.category_type=$1) ON CONFLICT(name,category_type) DO NOTHING";

const ADVANCED_UNCATEGORIZED_RELATION_INSERT: &str = "INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) SELECT $1::bigint,$2::bigint,$3,'{}'::jsonb,$4,$4 WHERE NOT EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation r WHERE r.m3u_account_id::bigint=$1 AND r.category_id::bigint=$2) ON CONFLICT(m3u_account_id,category_id) DO NOTHING";

/// Match Source merge_blank_vod_list_props across occurrences of one identity.
/// False and zero are nonblank; empty strings/null/empty-only arrays are blank.
fn list_blank(value: &Value) -> bool {
    value.is_null()
        || value.as_str() == Some("")
        || value
            .as_array()
            .is_some_and(|xs| xs.iter().all(|v| v.is_null() || v.as_str() == Some("")))
}
pub(super) fn merge_representative(existing: &mut Value, incoming: &Value) {
    for key in [
        "description",
        "rating",
        "genre",
        "duration",
        "adult",
        "year",
        "tmdb_id",
        "imdb_id",
    ] {
        if list_blank(&existing[key]) && !list_blank(&incoming[key]) {
            existing[key] = incoming[key].clone();
        }
    }
    if let Some(properties) = incoming["properties"].as_object() {
        if existing["properties"].is_null() {
            existing["properties"] = json!({});
        }
        if let Some(target) = existing["properties"].as_object_mut() {
            for (key, value) in properties {
                if !list_blank(value) && target.get(key).is_none_or(list_blank) {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
    }
    if existing["properties"]
        .as_object()
        .is_some_and(serde_json::Map::is_empty)
    {
        existing["properties"] = Value::Null;
    }
    if existing["logo"].as_str().is_none_or(str::is_empty)
        && incoming["logo"].as_str().is_some_and(|v| !v.is_empty())
    {
        existing["logo"] = incoming["logo"].clone();
    }
}
/// Same Source URL lifecycle: missing-only global logos, first identity name,
/// retained orphan rows and NULL association for absent/overlong raw URLs.
pub(super) async fn prepare_logos(
    auth: &AuthService,
    owner: Owner,
    rows: &mut [Value],
) -> Result<(), StoreError> {
    let mut seen = std::collections::HashSet::new();
    let mut logos = Vec::new();
    for row in rows.iter() {
        if let Some(url) = row["logo"]
            .as_str()
            .filter(|url| !url.is_empty() && url.chars().count() <= 500)
        {
            if seen.insert(url.to_owned()) {
                logos.push(json!({"url":url,"name":row["name"].as_str().unwrap_or_default().chars().take(255).collect::<String>()}));
            }
        }
    }
    let ids = scope(auth, owner, move |c| {
        Box::pin(async move {
            let urls: Vec<String> = logos
                .iter()
                .filter_map(|v| v["url"].as_str().map(str::to_owned))
                .collect();
            let found = sqlx::query(LIST_LOGO_SELECT)
                .bind(&urls)
                .fetch_all(&mut *c)
                .await?;
            let mut ids = found
                .into_iter()
                .map(|r| Ok((r.try_get::<String, _>("url")?, r.try_get::<i64, _>("id")?)))
                .collect::<Result<HashMap<_, _>, StoreError>>()?;
            let missing: Vec<Value> = logos
                .into_iter()
                .filter(|row| {
                    row["url"]
                        .as_str()
                        .is_some_and(|url| !ids.contains_key(url))
                })
                .collect();
            // Qualified Source lifecycle allocates multiple fresh URL rows from an
            // unordered set. Preserve URL/name identity and every association;
            // fresh row IDs may follow this runtime's representative order.
            if !missing.is_empty() {
                // Source catches logo bulk-create/refetch failures and retains the
                // initially fetched map. An SQL error outside BEGIN does not roll
                // back logos already committed by a successful bulk insert.
                if sqlx::query(LIST_LOGO_INSERT)
                    .bind(json!(missing))
                    .execute(&mut *c)
                    .await
                    .is_ok()
                {
                    if let Ok(found) = sqlx::query(LIST_LOGO_SELECT)
                        .bind(&urls)
                        .fetch_all(&mut *c)
                        .await
                    {
                        for row in found {
                            ids.insert(
                                row.try_get::<String, _>("url")?,
                                row.try_get::<i64, _>("id")?,
                            );
                        }
                    }
                }
            }
            Ok(ids)
        })
    })
    .await?;
    for row in rows {
        row["logo_id"] = row["logo"]
            .as_str()
            .and_then(|url| ids.get(url))
            .map_or(Value::Null, |id| json!(id));
    }
    Ok(())
}

const LIST_LOGO_INSERT:&str="INSERT INTO vod_vodlogo(name,url) SELECT v->>'name',v->>'url' FROM jsonb_array_elements($1) v WHERE NOT EXISTS(SELECT 1 FROM vod_vodlogo l WHERE l.url=v->>'url') ON CONFLICT(url) DO NOTHING";
const LIST_LOGO_SELECT: &str = "SELECT id::bigint,url FROM vod_vodlogo WHERE url=ANY($1)";
const LIST_MOVIE_INSERT:&str="INSERT INTO vod_movie(uuid,name,description,year,rating,genre,duration_secs,logo_id,tmdb_id,imdb_id,language,is_adult,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name',v->>'description',(v->>'year')::integer,v->>'rating',v->>'genre',(v->>'duration')::bigint,(v->>'logo_id')::bigint,v->>'tmdb_id',v->>'imdb_id',COALESCE(v->>'language',''),COALESCE((v->>'adult')::boolean,false),NULLIF(v->'properties','null'::jsonb),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM jsonb_array_elements($1) v RETURNING id::bigint,uuid";
const LIST_MOVIE_UPDATE:&str= "WITH projected AS MATERIALIZED (SELECT m.id,input.ordinal,(v->>'name' IS DISTINCT FROM m.name) AS queue_name_change,COALESCE((v->>'year')::integer,m.year) AS year,COALESCE(v->>'tmdb_id',m.tmdb_id) AS tmdb_id,COALESCE(v->>'imdb_id',m.imdb_id) AS imdb_id,CASE WHEN v->>'description'<>'' THEN v->>'description' ELSE m.description END AS description, COALESCE(v->>'rating',m.rating) AS rating, CASE WHEN v->>'genre'<>'' THEN v->>'genre' ELSE m.genre END AS genre, COALESCE((v->>'duration')::bigint,m.duration_secs) AS duration_secs, COALESCE((v->>'adult')::boolean,m.is_adult) AS is_adult, (v->>'logo_id')::bigint AS logo_id, CASE WHEN (COALESCE(m.custom_properties,'{}'::jsonb)||COALESCE((SELECT jsonb_object_agg(p.key,p.value) FROM jsonb_each(COALESCE(NULLIF(v->'properties','null'::jsonb),'{}'::jsonb)) p WHERE CASE WHEN p.key IN ('director','actors','release_date') THEN m.custom_properties->p.key IS NULL OR m.custom_properties->p.key IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'\"\"'::jsonb,'[]'::jsonb,'{}'::jsonb) ELSE NOT (p.value IN ('null'::jsonb,'\"\"'::jsonb,'[]'::jsonb) OR (jsonb_typeof(p.value)='array' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.value)='array' THEN p.value ELSE '[]'::jsonb END) element WHERE element NOT IN ('null'::jsonb,'\"\"'::jsonb)))) END),'{}'::jsonb)) IS DISTINCT FROM COALESCE(m.custom_properties,'{}'::jsonb) THEN COALESCE(m.custom_properties,'{}'::jsonb)||COALESCE((SELECT jsonb_object_agg(p.key,p.value) FROM jsonb_each(COALESCE(NULLIF(v->'properties','null'::jsonb),'{}'::jsonb)) p WHERE CASE WHEN p.key IN ('director','actors','release_date') THEN m.custom_properties->p.key IS NULL OR m.custom_properties->p.key IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'\"\"'::jsonb,'[]'::jsonb,'{}'::jsonb) ELSE NOT (p.value IN ('null'::jsonb,'\"\"'::jsonb,'[]'::jsonb) OR (jsonb_typeof(p.value)='array' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.value)='array' THEN p.value ELSE '[]'::jsonb END) element WHERE element NOT IN ('null'::jsonb,'\"\"'::jsonb)))) END),'{}'::jsonb) ELSE m.custom_properties END AS custom_properties FROM vod_movie m CROSS JOIN jsonb_array_elements($1) WITH ORDINALITY AS input(v,ordinal) WHERE m.id::bigint=(v->>'initial_id')::bigint), changed AS MATERIALIZED (SELECT DISTINCT ON(p.id) p.*,CASE WHEN EXISTS(SELECT 1 FROM projected q WHERE q.id=p.id AND q.logo_id IS DISTINCT FROM m.logo_id) THEN (SELECT q.logo_id FROM projected q WHERE q.id=p.id AND q.logo_id IS DISTINCT FROM m.logo_id ORDER BY q.ordinal DESC LIMIT 1) ELSE m.logo_id END AS final_logo_id FROM projected p JOIN vod_movie m ON m.id=p.id WHERE (p.queue_name_change OR ROW(m.year,m.tmdb_id,m.imdb_id,m.description,m.rating,m.genre,m.duration_secs,m.is_adult,m.logo_id,m.custom_properties) IS DISTINCT FROM ROW(p.year,p.tmdb_id,p.imdb_id,p.description,p.rating,p.genre,p.duration_secs,p.is_adult,p.logo_id,p.custom_properties)) ORDER BY p.id,p.ordinal) UPDATE vod_movie m SET year=p.year,tmdb_id=p.tmdb_id,imdb_id=p.imdb_id,description=p.description,rating=p.rating,genre=p.genre,duration_secs=p.duration_secs,is_adult=p.is_adult,logo_id=p.final_logo_id,custom_properties=p.custom_properties FROM changed p WHERE m.id=p.id AND ROW(m.year,m.tmdb_id,m.imdb_id,m.description,m.rating,m.genre,m.duration_secs,m.is_adult,m.logo_id,m.custom_properties) IS DISTINCT FROM ROW(p.year,p.tmdb_id,p.imdb_id,p.description,p.rating,p.genre,p.duration_secs,p.is_adult,p.final_logo_id,p.custom_properties)";

const LIST_MOVIE_RELATION_INSERT:&str="INSERT INTO vod_m3umovierelation(m3u_account_id,movie_id,category_id,stream_id,container_extension,custom_properties,created_at,updated_at,last_advanced_refresh,last_seen) SELECT $1::bigint,(v->>'movie')::bigint,(v->>'category')::bigint,v->>'stream',v->>'extension',jsonb_build_object('basic_data',v->'basic','detailed_fetched',false),(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz,NULL,$3 FROM jsonb_array_elements($2) WITH ORDINALITY AS input(v,ordinal) WHERE NOT EXISTS(SELECT 1 FROM vod_m3umovierelation r WHERE r.m3u_account_id::bigint=$1 AND r.stream_id=v->>'stream') ORDER BY input.ordinal ON CONFLICT(m3u_account_id,stream_id) DO NOTHING";

/// Source key includes the pinned/category language; missing language is legacy ''.
fn list_language(value: &Value) -> &str {
    value["language"].as_str().unwrap_or("")
}
pub(super) fn list_identity(value: &Value) -> String {
    let language = list_language(value);
    if value["tmdb_id"].as_str().is_some_and(|s| !s.is_empty()) {
        json!(["tmdb", value["tmdb_id"], language]).to_string()
    } else if value["imdb_id"].as_str().is_some_and(|s| !s.is_empty()) {
        json!(["imdb", value["imdb_id"], language]).to_string()
    } else {
        json!(["name", value["name"], value["year"], language]).to_string()
    }
}
/// Initial lookup uses selected key exact then untagged. The second recheck
/// first tries exact TMDB/IMDb (in that order), then only primary untagged.
pub(super) fn list_existing(value: &Value, existing: &[Value], second: bool) -> Option<i64> {
    let language = list_language(value);
    let find = |field: &str, language: &str| {
        existing
            .iter()
            .find(|row| row[field] == value[field] && list_language(row) == language)
            .and_then(|row| row["id"].as_i64())
    };
    let name = |language: &str| {
        existing
            .iter()
            .find(|row| {
                row["tmdb_id"].is_null()
                    && row["imdb_id"].is_null()
                    && row["name"] == value["name"]
                    && row["year"] == value["year"]
                    && list_language(row) == language
            })
            .and_then(|row| row["id"].as_i64())
    };
    if !second {
        let exact = if value["tmdb_id"].is_string() {
            find("tmdb_id", language)
        } else if value["imdb_id"].is_string() {
            find("imdb_id", language)
        } else {
            name(language)
        };
        if exact.is_some() || language.is_empty() {
            return exact;
        }
    } else {
        if value["tmdb_id"].is_string() {
            if let Some(id) = find("tmdb_id", language) {
                return Some(id);
            }
        }
        if value["imdb_id"].is_string() {
            if let Some(id) = find("imdb_id", language) {
                return Some(id);
            }
        }
        if !value["tmdb_id"].is_string() && !value["imdb_id"].is_string() {
            if let Some(id) = name(language) {
                return Some(id);
            }
        }
        if language.is_empty() {
            return None;
        }
    }
    if value["tmdb_id"].is_string() {
        find("tmdb_id", "")
    } else if value["imdb_id"].is_string() {
        find("imdb_id", "")
    } else {
        name("")
    }
}
/// Existing provider objects are shared within a Source batch: later occurrences
/// mutate the same object; once a transition clears detail flags they stay clear.
/// Newly constructed duplicate providers are NOT in the prefetched map, so both
/// INSERT attempts remain ordered and burn their sequences before conflict-ignore.
pub(super) fn existing_relation_updates(
    rows: &[Value],
    prefetched: &HashMap<String, i64>,
    provider: &str,
    target: &str,
) -> Vec<Value> {
    let mut positions = HashMap::<String, usize>::new();
    let mut updates = Vec::<Value>::new();
    for row in rows {
        let Some(key) = row[provider].as_str() else {
            continue;
        };
        let Some(original) = prefetched.get(key) else {
            continue;
        };
        let index = positions.get(key).copied();
        let previous = index.map_or(*original, |i| {
            updates[i][target].as_i64().unwrap_or(*original)
        });
        let reset = row["candidate"].as_bool().unwrap_or(false)
            || row[target].as_i64() != Some(previous)
            || index.is_some_and(|i| updates[i]["reset"] == true);
        let mut next = row.clone();
        next["reset"] = json!(reset);
        if let Some(i) = index {
            updates[i] = next;
        } else {
            positions.insert(key.to_owned(), updates.len());
            updates.push(next);
        }
    }
    updates
}

pub(super) fn list_snapshot(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Value>, StoreError> {
    if rows.len() > 2000 {
        return Err(unsupported());
    }
    rows.into_iter().map(|row| Ok(json!({"id":row.try_get::<i64,_>("id")?,"name":row.try_get::<String,_>("name")?,"year":row.try_get::<Option<i32>,_>("year")?,"tmdb_id":row.try_get::<Option<String>,_>("tmdb_id")?,"imdb_id":row.try_get::<Option<String>,_>("imdb_id")?,"language":row.try_get::<String,_>("language")?}))).collect()
}

const LIST_UNCATEGORIZED_LANGUAGE: &str = "SELECT COALESCE(lower(CASE WHEN jsonb_typeof(custom_properties->'language')='string' THEN custom_properties->>'language' ELSE '' END),''),enabled FROM vod_m3uvodcategoryrelation WHERE m3u_account_id::bigint=$1 AND category_id::bigint=$2";

const LIST_CATEGORY_LANGUAGES: &str = "SELECT category_id::bigint AS category,COALESCE(lower(CASE WHEN jsonb_typeof(custom_properties->'language')='string' THEN custom_properties->>'language' ELSE '' END),'') AS language FROM vod_m3uvodcategoryrelation WHERE m3u_account_id::bigint=$1";

const LIST_MOVIE_RELATION_EXISTING:&str="SELECT r.stream_id AS provider,r.movie_id::bigint AS target,m.language FROM vod_m3umovierelation r JOIN vod_movie m ON m.id=r.movie_id WHERE r.m3u_account_id::bigint=$1 AND r.stream_id=ANY($2)";

const LIST_MOVIE_EXISTING:&str="WITH wanted AS MATERIALIZED (SELECT v FROM jsonb_array_elements($1) v), matches AS (SELECT m.id::bigint,m.name,m.year,m.tmdb_id,m.imdb_id,m.language FROM vod_movie m JOIN wanted w ON m.tmdb_id=w.v->>'tmdb_id' WHERE m.language IN(COALESCE(w.v->>'language',''),'') UNION SELECT m.id::bigint,m.name,m.year,m.tmdb_id,m.imdb_id,m.language FROM vod_movie m JOIN wanted w ON m.imdb_id=w.v->>'imdb_id' WHERE m.language IN(COALESCE(w.v->>'language',''),'') UNION SELECT m.id::bigint,m.name,m.year,m.tmdb_id,m.imdb_id,m.language FROM vod_movie m JOIN wanted w ON m.name=w.v->>'name' AND m.year IS NOT DISTINCT FROM (w.v->>'year')::integer WHERE m.language IN(COALESCE(w.v->>'language',''),'') AND m.tmdb_id IS NULL AND m.imdb_id IS NULL AND w.v->>'tmdb_id' IS NULL AND w.v->>'imdb_id' IS NULL) SELECT id,name,year,tmdb_id,imdb_id,language FROM matches ORDER BY id LIMIT 2001";

const LIST_MOVIE_RELATION_UPDATE:&str="UPDATE vod_m3umovierelation r SET movie_id=(v->>'movie')::bigint,category_id=(v->>'category')::bigint,container_extension=v->>'extension',custom_properties=COALESCE(r.custom_properties,'{}'::jsonb)||jsonb_build_object('basic_data',v->'basic')||CASE WHEN COALESCE((v->>'reset')::boolean,false) OR r.movie_id IS DISTINCT FROM (v->>'movie')::bigint THEN jsonb_build_object('detailed_fetched',false) ELSE '{}'::jsonb END,last_seen=$3 FROM jsonb_array_elements($2) v WHERE r.m3u_account_id=$1 AND r.stream_id=v->>'stream'";

#[cfg(test)]
mod list_metadata_tests {
    use super::*;
    #[test]
    fn language_identity_prefers_exact_then_untagged_without_crossing_tagged_rows() {
        let incoming = json!({"name":"Owned","year":2010,"tmdb_id":"96001","imdb_id":"tt-owned","language":"fr"});
        let untagged = json!({"id":1,"name":"Owned","year":2010,"tmdb_id":"96001","imdb_id":"tt-owned","language":""});
        let english = json!({"id":2,"name":"Owned","year":2010,"tmdb_id":"96001","imdb_id":"tt-owned","language":"en"});
        let french = json!({"id":3,"name":"Owned","year":2010,"tmdb_id":"96001","imdb_id":"tt-owned","language":"fr"});
        assert_eq!(
            list_existing(&incoming, &[untagged.clone(), english.clone()], false),
            Some(1)
        );
        assert_eq!(
            list_existing(&incoming, &[untagged.clone(), french], false),
            Some(3)
        );
        assert_eq!(
            list_existing(&incoming, std::slice::from_ref(&english), false),
            None
        );
        assert_ne!(list_identity(&incoming), list_identity(&english));
        let secondary = json!({"id":4,"name":"Secondary","year":2010,"tmdb_id":null,"imdb_id":"tt-owned","language":"fr"});
        assert_eq!(
            list_existing(&incoming, &[untagged.clone(), secondary.clone()], false),
            Some(1)
        );
        assert_eq!(
            list_existing(&incoming, &[untagged, secondary], true),
            Some(4)
        );
        let no_ids =
            json!({"name":"Owned","year":2010,"tmdb_id":null,"imdb_id":null,"language":"fr"});
        let no_ids_untagged =
            json!({"id":5,"name":"Owned","year":2010,"tmdb_id":null,"imdb_id":null,"language":""});
        assert_eq!(list_existing(&no_ids, &[no_ids_untagged], false), Some(5));
    }
    #[test]
    fn prefetched_relation_mutation_keeps_last_payload_and_sticky_reset() {
        let prefetched = HashMap::from([("existing".to_owned(), 7)]);
        let occurrences = vec![
            json!({"stream":"existing","movie":8,"candidate":false,"basic":"first"}),
            json!({"stream":"new","movie":8,"candidate":true,"basic":"new first"}),
            json!({"stream":"existing","movie":7,"candidate":false,"basic":"last"}),
            json!({"stream":"new","movie":9,"candidate":true,"basic":"new last"}),
        ];
        let actual = existing_relation_updates(&occurrences, &prefetched, "stream", "movie");
        assert_eq!(
            actual,
            vec![
                json!({"stream":"existing","movie":7,"candidate":false,"basic":"last","reset":true})
            ]
        );
        // An unsaved candidate resets details even if second lookup reuses the same PK.
        assert_eq!(
            existing_relation_updates(
                &[json!({"external":"existing","series":7,"candidate":true})],
                &prefetched,
                "external",
                "series"
            )[0]["reset"],
            true
        );
        assert_eq!(
            existing_relation_updates(
                &[json!({"stream":"existing","movie":7,"candidate":false})],
                &prefetched,
                "stream",
                "movie"
            )[0]["reset"],
            false
        );
    }

    #[test]
    fn repeated_identity_preserves_first_nonblank_and_fills_missing_fields() {
        let mut first = json!({"description":"First","rating":null,"genre":"","duration":0,"adult":false,"properties":{"director":"first","empty":[null,""]},"logo":"x".repeat(501)});
        merge_representative(
            &mut first,
            &json!({"description":"Later","rating":"7.5","genre":"Drama","duration":90,"adult":true,"properties":{"director":"later","empty":"filled","zero":0},"logo":"https://fixture.invalid/later"}),
        );
        assert_eq!(first["description"], "First");
        assert_eq!(first["rating"], "7.5");
        assert_eq!(first["genre"], "Drama");
        assert_eq!(first["duration"], 0);
        assert_eq!(first["adult"], false);
        assert_eq!(
            first["properties"],
            json!({"director":"first","empty":"filled","zero":0})
        );
        assert_eq!(first["logo"].as_str().unwrap().len(), 501);
    }
    #[test]
    fn first_nonempty_logo_is_selected_independently_from_field_merging() {
        let mut first = json!({"properties":null,"logo":null});
        merge_representative(
            &mut first,
            &json!({"description":"Filled","properties":{"releaseDate":"2010-01-01","blank":[null,""]},"logo":"https://fixture.invalid/first"}),
        );
        merge_representative(
            &mut first,
            &json!({"description":"Later","properties":{"releaseDate":"2020-01-01"},"logo":"https://fixture.invalid/second"}),
        );
        assert_eq!(first["description"], "Filled");
        assert_eq!(first["properties"], json!({"releaseDate":"2010-01-01"}));
        assert_eq!(first["logo"], "https://fixture.invalid/first");
    }

    #[test]
    fn external_id_second_lookup_reuses_only_after_primary_misses() {
        let rows =
            vec![json!({"id":16,"name":"Old","year":2009,"tmdb_id":null,"imdb_id":"tt7101"})];
        let incoming = json!({"name":"New","year":2010,"tmdb_id":"7199","imdb_id":"tt7101"});
        assert_eq!(list_existing(&incoming, &rows, false), None);
        assert_eq!(list_existing(&incoming, &rows, true), Some(16));
        let sparse = json!({"name":"Old","year":2009,"tmdb_id":null,"imdb_id":null});
        assert_eq!(list_existing(&sparse, &rows, true), None);
        assert_ne!(list_identity(&incoming), list_identity(&sparse));
        let both = vec![
            json!({"id":1,"name":"TMDB","year":2010,"tmdb_id":"7199","imdb_id":null}),
            rows[0].clone(),
        ];
        assert_eq!(list_existing(&incoming, &both, true), Some(1));
    }
    #[test]
    fn external_primary_duplicate_fills_missing_year_without_changing_first_name() {
        let mut first = json!({"name":"First","year":null,"tmdb_id":"7101","imdb_id":null});
        let later = json!({"name":"Later","year":2010,"tmdb_id":"7101","imdb_id":"tt7101"});
        assert_eq!(list_identity(&first), list_identity(&later));
        merge_representative(&mut first, &later);
        assert_eq!(first["year"], 2010);
        assert_eq!(first["name"], "First");
        assert_eq!(first["imdb_id"], "tt7101");
        // Source is_blank_vod_value is not Python truthiness: zero is nonblank.
        let mut zero = json!({"year":0});
        merge_representative(&mut zero, &json!({"year":2010}));
        assert_eq!(zero["year"], 0);
        let mut missing = json!({"year":null});
        merge_representative(&mut missing, &json!({"year":0}));
        assert_eq!(missing["year"], 0);
    }
}
