//! Real Source rehash batches, token handoff/renewal and duplicate relationships.
//! Original Dispatcharr/Django authors retain algorithm/application credit.
use crate::{AuthService, StoreError};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_ingest_core::{stream_hash_nullable, NullableStreamIdentity};
use serde_json::{json, Value};
use sqlx::{Connection, Postgres, Row, Transaction};
use std::{sync::Arc, time::Duration};
#[derive(Clone, Debug)]
pub struct Request {
    pub keys: Vec<String>,
    pub account: Option<i64>,
    pub preacquired: bool,
    pub ids: Vec<i64>,
    pub token: Option<String>,
}
fn capacity() -> StoreError {
    StoreError::Unsupported("Rehash bounded SQL or Source value domain is unsupported")
}
pub fn available(auth: &AuthService) -> bool {
    auth.notification_cache.is_some()
}
pub async fn preflight(auth: &AuthService) -> Result<(), StoreError> {
    let mut connection = auth.store.pool().acquire().await?;
    let observed = sqlx::query("SELECT id FROM dispatcharr_channels_stream LIMIT 0")
        .execute(&mut *connection)
        .await;
    let closed = connection.close().await;
    observed?;
    closed?;
    Ok(())
}
#[path = "core_task_rehash_state.rs"]
mod state;
/// Same bounded account/override snapshot for the normal Core HTTP producer.
/// The producer owns final physical close; no Worker receipt is fabricated.
pub(crate) async fn prepare_global_targets(
    connection: &mut sqlx::PgConnection,
) -> Result<usize, StoreError> {
    state::prepare(
        connection,
        &Request {
            keys: Vec::new(),
            account: None,
            preacquired: false,
            ids: Vec::new(),
            token: None,
        },
    )
    .await
}
const KEY: &str = "task_lock_refresh_single_m3u_account_";
const RELEASE: &str =
    "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0";
const RENEW:&str="if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('EXPIRE',KEYS[1],300) end return 0";
async fn script(
    cache: &crate::notifications::NotificationCache,
    text: &str,
    id: i64,
    token: &str,
) -> Result<i64, StoreError> {
    let mut cmd = redis::cmd("EVAL");
    cmd.arg(text).arg(1).arg(format!("{KEY}{id}")).arg(token);
    cache.command_settled(cmd).await
}
pub async fn execute(
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    request: Request,
    owner: Arc<dyn Send + Sync>,
) -> Result<Value, StoreError> {
    let cache = auth
        .notification_cache
        .clone()
        .ok_or_else(capacity)?
        .with_retained_scope(owner.clone());
    let token = if request.preacquired {
        request.token.clone().unwrap_or_default()
    } else {
        uuid::Uuid::new_v4().simple().to_string()
    };
    let owned = state::Owned::new(cache, token);
    let mut connection = auth.store.pool().acquire().await?;
    let prepared = state::prepare(&mut connection, &request).await;
    let target_count = match prepared {
        Ok(n) => n,
        Err(error) => {
            connection.close().await?;
            return Err(error);
        }
    };
    let acquired=async{
        let mut blocked=0usize;
        if request.preacquired{
            for id in &request.ids{if !owned.acquire(*id,true).await?{blocked+=1;}}
        }else{
            sqlx::query("DECLARE rust_rehash_lock_targets NO SCROLL CURSOR WITH HOLD FOR SELECT id FROM rust_rehash_targets ORDER BY seq").execute(&mut*connection).await?;
            loop{
                let rows=sqlx::query(&format!("FETCH FORWARD {} FROM rust_rehash_lock_targets",state::PAGE)).fetch_all(&mut*connection).await?;
                if rows.is_empty(){break;}
                for row in rows{if !owned.acquire(row.try_get("id")?,false).await?{blocked+=1;}}
            }
            sqlx::query("CLOSE rust_rehash_lock_targets").execute(&mut*connection).await?;
        }Ok::<_,StoreError>(blocked)
    }.await;
    let blocked = match acquired {
        Ok(0) => 0,
        Ok(blocked) => {
            let released = owned.release().await;
            let closed = connection.close().await;
            released?;
            closed?;
            crate::core_task_offsets::retained_event(events,json!({"type":"update","data":{"success":false,"type":"stream_rehash","action":"blocked","blocked_accounts":blocked,"total_accounts":target_count,"message":format!("Stream rehash blocked: M3U refresh tasks are currently running for {blocked} accounts. Please try again after the refresh finishes.")}}),owner).await?;
            return Ok(json!(format!(
                "Rehash blocked: M3U refresh tasks running for {blocked} accounts"
            )));
        }
        Err(error) => {
            let released = owned.release().await;
            let closed = connection.close().await;
            eprintln!("Rehash acquire failure {error:?}; release {released:?}; close {closed:?}");
            return Err(error);
        }
    };
    debug_assert_eq!(blocked, 0);
    let (stop, mut stopping) = tokio::sync::watch::channel(false);
    let renewal = owned.clone();
    let renew_owner = owner.clone();
    let renewer = tokio::spawn(async move {
        let _owner = renew_owner;
        loop {
            tokio::select! {
                changed=stopping.changed()=>{if changed.is_err()||*stopping.borrow(){break;}},
                _=tokio::time::sleep(Duration::from_secs(120))=>{renewal.renew().await?;}
            }
        }
        Ok::<_, StoreError>(())
    });
    let result = batches(&mut connection, events, &request, owner).await;
    let _ = stop.send(true);
    let joined = renewer.await;
    let released = owned.release().await;
    let ready = connection.ping().await;
    let closed = connection.close().await;
    joined.map_err(|_| capacity())??;
    released?;
    ready?;
    closed?;
    result
}
fn scope() -> &'static str {
    "($1::bigint IS NULL OR s.m3u_account_id=$1) AND ($1::bigint IS NOT NULL OR o.id IS NULL)"
}
async fn batches(
    connection: &mut sqlx::PgConnection,
    events: Arc<dyn UpdateEventSink>,
    request: &Request,
    owner: Arc<dyn Send + Sync>,
) -> Result<Value, StoreError> {
    let initial:i64=sqlx::query_scalar(&format!("SELECT count(*)::bigint FROM dispatcharr_channels_stream s LEFT JOIN rust_rehash_overrides o ON o.id=s.m3u_account_id WHERE {}",scope())).bind(request.account).fetch_one(&mut*connection).await?;
    crate::core_task_offsets::retained_event(events.clone(),json!({"type":"update","data":{"success":true,"type":"stream_rehash","action":"starting","progress":0,"total_records":initial,"message":format!("Starting rehash of {initial} streams")}}),owner.clone()).await?;
    let mut last = 0i64;
    let mut processed = 0usize;
    let mut duplicates = 0usize;
    let mut batch_number = 0usize;
    loop {
        batch_number += 1;
        let mut tx = connection.begin().await?;
        let work=async{
            let rows=sqlx::query(&format!("SELECT s.id::bigint,s.name,s.url,s.tvg_id,s.updated_at,s.stream_hash,s.m3u_account_id::bigint,s.stream_id::bigint,CASE WHEN 'group'=ANY(COALESCE(o.fields,$3::text[])) AND COALESCE(octet_length(g.name),0)<=8192 THEN g.name END AS group_name,('group'<>ALL(COALESCE(o.fields,$3::text[])) OR COALESCE(octet_length(g.name),0)<=8192) AS group_bounded,a.account_type,COALESCE(o.fields,$3::text[]) AS fields FROM dispatcharr_channels_stream s LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=s.channel_group_id LEFT JOIN rust_rehash_overrides o ON o.id=s.m3u_account_id WHERE s.id>$2 AND {} ORDER BY s.id LIMIT 1000 FOR UPDATE OF s SKIP LOCKED",scope())).bind(request.account).bind(last).bind(&request.keys).fetch_all(&mut*tx).await?;
            if rows.is_empty(){return Ok::<_,StoreError>(None);}
            let mut batch_count=0;let mut batch_duplicates=0;
            for row in rows{
                if !row.try_get::<bool,_>("group_bounded")?{return Err(capacity());}
                let id:i64=row.try_get("id")?;last=id;let updated:DateTime<Utc>=row.try_get("updated_at")?;
                let name:Option<String>=row.try_get("name")?;let url:Option<String>=row.try_get("url")?;let tvg:Option<String>=row.try_get("tvg_id")?;
                let account:Option<i64>=row.try_get("m3u_account_id")?;let stream_id:Option<i64>=row.try_get("stream_id")?;let group:Option<String>=row.try_get("group_name")?;let kind:Option<String>=row.try_get("account_type")?;let fields:Vec<String>=row.try_get("fields")?;let keys:Vec<&str>=fields.iter().map(String::as_str).collect();
                let hash=stream_hash_nullable(&NullableStreamIdentity{name:name.as_deref(),url:url.as_deref(),tvg_id:tvg.as_deref(),m3u_id:account,group:group.as_deref(),account_type:kind.as_deref().unwrap_or(""),stream_id},&keys);
                let seen:Option<i64>=sqlx::query_scalar("SELECT id FROM rust_rehash_seen WHERE hash=$1").bind(&hash).fetch_optional(&mut*tx).await?;
                let restore=if let Some(seen)=seen{sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM rust_rehash_deleted WHERE id=$1) OR NOT EXISTS(SELECT 1 FROM dispatcharr_channels_stream WHERE id=$1::bigint)").bind(seen).fetch_one(&mut*tx).await?}else{false};
                let existing=if restore{None}else if let Some(seen)=seen{Some(seen)}else{sqlx::query_scalar::<_,i64>("SELECT id::bigint FROM dispatcharr_channels_stream WHERE stream_hash=$1 AND id<>$2::bigint ORDER BY id LIMIT 1").bind(&hash).bind(id).fetch_optional(&mut*tx).await?};
                let existing=existing.filter(|existing|*existing!=id);
                let existing_hash=if let Some(existing)=existing{sqlx::query_scalar::<_,Option<String>>("SELECT stream_hash FROM dispatcharr_channels_stream WHERE id=$1::bigint").bind(existing).fetch_optional(&mut*tx).await?.flatten()}else{None};
                let keep=if let Some(existing)=existing{
                    let keep=winner(&mut tx,existing,id,updated).await?;let remove=if keep==id{existing}else{id};
                    merge(&mut tx,remove,keep).await?;
                    sqlx::query("INSERT INTO rust_rehash_deleted(id) VALUES($1) ON CONFLICT DO NOTHING").bind(remove).execute(&mut*tx).await?;
                    batch_duplicates+=1;keep
                }else{id};
                let selected_hash:Option<String>=row.try_get("stream_hash")?;
                let should_update=existing.is_none() || if keep==id{selected_hash.as_deref()!=Some(hash.as_str())}else{existing_hash.as_deref()!=Some(hash.as_str())};
                if should_update{
                    let changed=sqlx::query("UPDATE dispatcharr_channels_stream SET stream_hash=$2 WHERE id=$1::bigint").bind(keep).bind(&hash).execute(&mut*tx).await?;
                    if changed.rows_affected()!=1{return Err(capacity());}
                }
                sqlx::query("INSERT INTO rust_rehash_seen(hash,id) VALUES($1,$2) ON CONFLICT(hash) DO UPDATE SET id=EXCLUDED.id").bind(hash).bind(keep).execute(&mut*tx).await?;
                batch_count+=1;
            }Ok(Some((batch_count,batch_duplicates)))
        }.await;
        let outcome = match work {
            Ok(value) => {
                tx.commit().await?;
                value
            }
            Err(error) => {
                let rolled = tx.rollback().await;
                rolled?;
                return Err(error);
            }
        };
        let Some((count, merged)) = outcome else {
            break;
        };
        processed += count;
        duplicates += merged;
        let progress = ((processed as f64 / initial.max(1) as f64 * 100.0) as usize).min(99);
        crate::core_task_offsets::retained_event(events.clone(),json!({"type":"update","data":{"success":true,"type":"stream_rehash","action":"processing","progress":progress,"batch":batch_number,"processed":processed,"duplicates_merged":duplicates,"message":format!("Processed batch {batch_number}: {count} streams, {merged} duplicates merged")}}),owner.clone()).await?;
    }
    crate::core_task_offsets::retained_event(events,json!({"type":"update","data":{"success":true,"type":"stream_rehash","action":"completed","progress":100,"total_processed":processed,"duplicates_merged":duplicates,"final_count":processed-duplicates,"message":format!("Rehashing complete: {processed} streams processed, {duplicates} duplicates merged")}}),owner).await?;
    Ok(json!(format!("Successfully rehashed {processed} streams")))
}
async fn winner(
    tx: &mut Transaction<'_, Postgres>,
    a: i64,
    b: i64,
    b_updated: DateTime<Utc>,
) -> Result<i64, StoreError> {
    let rows=sqlx::query("SELECT s.id::bigint,s.updated_at,(SELECT min(cs.\"order\") FROM dispatcharr_channels_channelstream cs WHERE cs.stream_id=s.id) AS best FROM dispatcharr_channels_stream s WHERE s.id=ANY($1::bigint[])").bind([a,b].as_slice()).fetch_all(&mut**tx).await?;
    let mut values = Vec::new();
    for row in rows {
        values.push((
            row.try_get::<i64, _>("id")?,
            row.try_get::<DateTime<Utc>, _>("updated_at")?,
            row.try_get::<Option<i32>, _>("best")?,
        ));
    }
    let a = values.iter().find(|v| v.0 == a).ok_or_else(capacity)?;
    let b_live = values.iter().find(|v| v.0 == b);
    let b = (b, b_updated, b_live.and_then(|v| v.2));
    Ok(match (a.2, b.2) {
        (Some(_), None) => a.0,
        (None, Some(_)) => b.0,
        (Some(x), Some(y)) if x < y => a.0,
        (Some(x), Some(y)) if y < x => b.0,
        _ => {
            if a.1 > b.1 {
                a.0
            } else if b.1 > a.1 {
                b.0
            } else {
                a.0.min(b.0)
            }
        }
    })
}
async fn merge(
    tx: &mut Transaction<'_, Postgres>,
    remove: i64,
    keep: i64,
) -> Result<(), StoreError> {
    let channels:Vec<i64>=sqlx::query_scalar("SELECT channel_id::bigint FROM dispatcharr_channels_channelstream WHERE stream_id=$1::bigint").bind(remove).fetch_all(&mut**tx).await?;
    sqlx::query("UPDATE dispatcharr_channels_channelstream target SET \"order\"=LEAST(target.\"order\",source.\"order\") FROM dispatcharr_channels_channelstream source WHERE source.stream_id=$1::bigint AND target.stream_id=$2::bigint AND target.channel_id=source.channel_id").bind(remove).bind(keep).execute(&mut**tx).await?;
    sqlx::query("DELETE FROM dispatcharr_channels_channelstream source USING dispatcharr_channels_channelstream target WHERE source.stream_id=$1::bigint AND target.stream_id=$2::bigint AND target.channel_id=source.channel_id").bind(remove).bind(keep).execute(&mut**tx).await?;
    sqlx::query(
        "UPDATE dispatcharr_channels_channelstream SET stream_id=$2::bigint WHERE stream_id=$1::bigint",
    )
    .bind(remove)
    .bind(keep)
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM dispatcharr_channels_stream WHERE id=$1::bigint")
        .bind(remove)
        .execute(&mut **tx)
        .await?;
    crate::channel_writes::rollup_ids(tx, &channels).await?;
    Ok(())
}
