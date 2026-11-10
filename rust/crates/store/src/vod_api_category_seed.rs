//! UNREGISTERED/UNCOMPILED lazy category-write helper draft. No HTTP/SQL gate.
//! Original Dispatcharr authors retain credit. Call only after Standard/UI
//! authentication and physical admission, before category visibility filters.
//! No cross-request metadata cache; no blanket transaction/rollback of earlier
//! successful get_or_create operations. Existing defaults are never overwritten.
use super::vod_api_owned_read::{OwnedConnection,ReadError,ReadWork};
use chrono::Utc;
use sqlx::Row;
#[derive(Debug)]
pub(crate) struct SeedResult {pub movie_category:i64,pub series_category:i64,pub examined_accounts:usize,pub created_links:usize}

async fn category(work:&ReadWork,connection:&mut OwnedConnection,kind:&str)->Result<i64,ReadError> {
    work.check()?;connection.configure(work).await?;
    if let Some(row)=sqlx::query("SELECT id::bigint AS id FROM vod_vodcategory WHERE name='Uncategorized' AND category_type=$1")
        .bind(kind).fetch_optional(connection.raw()).await? {return Ok(row.try_get("id")?);}
    let now=Utc::now();
    // Mirrors get_or_create's unique-conflict retry. It does NOT update the
    // properties/timestamps of an existing row, including a concurrent winner.
    work.check()?;connection.configure(work).await?;
    if let Some(row)=sqlx::query("INSERT INTO vod_vodcategory(name,category_type,created_at,updated_at) VALUES('Uncategorized',$1,$2,$2) ON CONFLICT(name,category_type) DO NOTHING RETURNING id::bigint AS id")
        .bind(kind).bind(now).fetch_optional(connection.raw()).await? {return Ok(row.try_get("id")?);}
    work.check()?;connection.configure(work).await?;
    Ok(sqlx::query("SELECT id::bigint AS id FROM vod_vodcategory WHERE name='Uncategorized' AND category_type=$1")
        .bind(kind).fetch_one(connection.raw()).await?.try_get("id")?)
}

pub(crate) async fn prepare(work:&ReadWork,connection:&mut OwnedConnection)->Result<SeedResult,ReadError> {
    // Preserve Source ordering: global category writes happen even if the
    // eventual user flags return an empty list, and before account processing.
    let movie=category(work,connection,"movie").await?;
    let series=category(work,connection,"series").await?;
    work.check()?;connection.configure(work).await?;
    // Minimal scalar metadata only. JSONB equality classifies JSON falsy
    // values without sending huge unrelated properties/values to the client.
    // Bad nonmapping/default types are explicit unsupported fault branches;
    // they are not silently treated as disabled/default-false accounts.
    let accounts=sqlx::query("SELECT id::bigint AS id, COALESCE(custom_properties IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'\"\"'::jsonb,'[]'::jsonb,'{}'::jsonb),true) AS properties_falsy, jsonb_typeof(custom_properties) AS properties_type, COALESCE((custom_properties->'enable_vod') NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'\"\"'::jsonb,'[]'::jsonb,'{}'::jsonb),false) AS vod_enabled, COALESCE(custom_properties ? 'auto_enable_new_groups_vod',false) AS auto_present, jsonb_typeof(custom_properties->'auto_enable_new_groups_vod') AS auto_type, CASE WHEN jsonb_typeof(custom_properties->'auto_enable_new_groups_vod')='boolean' THEN (custom_properties->>'auto_enable_new_groups_vod')::boolean END AS auto_value FROM m3u_m3uaccount WHERE is_active=true AND account_type='XC' LIMIT 4097")
        .fetch_all(connection.raw()).await?;
    // Explicit draft resource guard, never truncate to the first4096. Source's
    // unbounded account iteration requires a separate large-catalogue design
    // and concurrency/fault gate before claiming full category parity.
    if accounts.len()>4096{return Err(ReadError::Capacity);}
    let mut result=SeedResult {movie_category:movie,series_category:series,examined_accounts:accounts.len(),created_links:0};
    for account in accounts {
        work.check()?;
        if account.try_get::<bool,_>("properties_falsy")?{continue;}
        if account.try_get::<Option<String>,_>("properties_type")?.as_deref()!=Some("object") {
            return Err(ReadError::Unsupported("Truthy nonmapping account properties need Source lazy-write fault gate"));
        }
        if !account.try_get::<bool,_>("vod_enabled")?{continue;}
        let id=account.try_get::<i64,_>("id")?;
        for category in [movie,series] {
            connection.configure(work).await?;
            let exists=sqlx::query("SELECT 1 FROM vod_m3uvodcategoryrelation WHERE m3u_account_id=$1 AND category_id=$2")
                .bind(id).bind(category).fetch_optional(connection.raw()).await?.is_some();
            if exists {continue;}
            // Existing links must bypass defaults validation, as get_or_create
            // does. Only missing rows need BooleanField conversion. Source1
            // exercised boolean/default values; other conversions stay gated.
            let enabled=if !account.try_get::<bool,_>("auto_present")? {true}
                else if account.try_get::<Option<String>,_>("auto_type")?.as_deref()==Some("boolean") {
                    account.try_get::<bool,_>("auto_value")?
                }else{return Err(ReadError::Unsupported("Uncaptured category BooleanField coercion"));};
            work.check()?;connection.configure(work).await?;
            let now=Utc::now();
            let created=sqlx::query("INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) VALUES($1,$2,$3,'{}'::jsonb,$4,$4) ON CONFLICT(m3u_account_id,category_id) DO NOTHING")
                .bind(id).bind(category).bind(enabled).bind(now).execute(connection.raw()).await?.rows_affected();
            result.created_links+=created as usize;
        }
    }
    Ok(result)
}
