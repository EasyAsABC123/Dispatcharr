//! Original Dispatcharr task algorithms and notifications, ported with full
//! author credit. No scheduler, pool, client or worker is created here.
use crate::{Store, StoreError};
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, Row};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Bulk,
    Names,
    Logos,
    TvgIds,
}
impl Kind {
    pub fn task(self) -> &'static str {
        match self {
            Self::Bulk => "apps.channels.tasks.bulk_create_channels_from_streams",
            Self::Names => "apps.channels.tasks.set_channels_names_from_epg",
            Self::Logos => "apps.channels.tasks.set_channels_logos_from_epg",
            Self::TvgIds => "apps.channels.tasks.set_channels_tvg_ids_from_epg",
        }
    }
    fn progress(self) -> &'static str {
        match self {
            Self::Bulk => "bulk_channel_creation_progress",
            Self::Names => "epg_name_setting_progress",
            Self::Logos => "epg_logo_setting_progress",
            Self::TvgIds => "epg_tvg_id_setting_progress",
        }
    }
}
#[derive(Clone, Debug)]
pub struct Request {
    pub kind: Kind,
    pub ids: Vec<Value>,
    pub source: Option<Value>,
    pub profiles: Option<Vec<Value>>,
    pub starting: Option<Value>,
}
/// Only a settled original body error can become a Source task failure.
/// Socket/Ready/close uncertainty and unsupported bounded domains stay faults.
#[derive(Debug)]
pub enum ExecutionError {
    Raised { error: StoreError, total: usize },
    ValueError { message: String, total: usize },
    TypeError { message: String, total: usize },
    Operational,
}
pub async fn preflight(store: &Store, request: &Request) -> Result<(), StoreError> {
    if request.kind == Kind::Logos && request.ids.is_empty() && request.source.is_none() {
        return Ok(());
    }
    let ids = match query_ids(&request.ids) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let source = match request.source.as_ref().map(query_id).transpose() {
        Ok(v) => v.flatten(),
        Err(_) => return Ok(()),
    };
    let profiles = match request.profiles.as_ref().map(|v| query_ids(v)).transpose() {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let mut c = store.pool().acquire().await?;
    c.close_on_drop();
    let observed=async {
        let oversized:bool=if request.kind==Kind::Bulk {
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_stream WHERE id=ANY($1) AND (octet_length(name)>8192 OR octet_length(tvg_id)>4096 OR stream_chno::text IN ('NaN','Infinity','-Infinity') OR abs(stream_chno)>9007199254000000 OR COALESCE(octet_length((custom_properties->'tvc-guide-stationid')::text),0)>4096 OR COALESCE(jsonb_typeof(custom_properties->'tvc-guide-stationid'),'null') NOT IN ('null','string')))").bind(&ids).fetch_one(&mut *c).await?
        }else{sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON e.id=c.epg_data_id WHERE (c.id=ANY($1) OR ($2::bigint IS NOT NULL AND e.epg_source_id=$2)) AND (octet_length(e.icon_url)>65536 OR octet_length(e.name)>8192 OR octet_length(e.tvg_id)>4096))").bind(&ids).bind(source).fetch_one(&mut *c).await?};
        if oversized{return Err(StoreError::Unsupported("Channel task field capacity/types"));}
        if request.kind==Kind::Bulk{let n:i64=sqlx::query_scalar("SELECT count(*) FROM dispatcharr_channels_channelprofile WHERE $1::bigint[] IS NULL OR $2 OR id=ANY($1)").bind(&profiles).bind(all_profiles(request)).fetch_one(&mut *c).await?;if n>4096||n.saturating_mul(request.ids.len()as i64)>65536{return Err(StoreError::Unsupported("Channel profile membership capacity"));}}
        Ok::<(),StoreError>(())
    }.await;
    let ready = c.ping().await;
    let close = c.close().await;
    observed?;
    ready?;
    close?;
    Ok(())
}
/// Owned worker calls this inside its supervised future. One exact checked-out SQL
/// session spans every Source page, event and original inner transaction. No
/// outer transaction changes the Source's earlier-page/logos partial effects.
pub async fn execute(
    store: Store,
    events: Arc<dyn UpdateEventSink>,
    request: Request,
    task: Uuid,
    owner: Arc<dyn Send + Sync>,
) -> Result<Value, ExecutionError> {
    if request.kind == Kind::Logos && request.ids.is_empty() && request.source.is_none() {
        return Err(ExecutionError::ValueError {
            message: "channel_ids or epg_source_id is required".into(),
            total: 0,
        });
    }
    let mut c = store
        .pool()
        .acquire()
        .await
        .map_err(|_| ExecutionError::Operational)?;
    c.close_on_drop();
    let mut total = if request.kind == Kind::Logos {
        0
    } else {
        request.ids.len()
    };
    let observed = if request.kind == Kind::Bulk {
        bulk(&mut c, &events, &request, task, owner.clone()).await
    } else {
        epg(&mut c, &events, &request, task, owner.clone(), &mut total).await
    };
    let ready = c.ping().await;
    let close = c.close().await;
    if ready.is_err() || close.is_err() {
        return Err(ExecutionError::Operational);
    }
    match observed {
        Ok(value) => Ok(value),
        Err(BodyError::Store(error @ StoreError::Database(_))) => {
            Err(ExecutionError::Raised { error, total })
        }
        Err(BodyError::Lookup(error)) if error.class == "ValueError" => {
            Err(ExecutionError::ValueError {
                message: error.message,
                total,
            })
        }
        Err(BodyError::Lookup(error)) if error.class == "TypeError" => {
            Err(ExecutionError::TypeError {
                message: error.message,
                total,
            })
        }
        Err(_) => Err(ExecutionError::Operational),
    }
}
/// The worker calls this only after classifying the actual settled error, never
/// a fabricated SQL error. Source sends this event before Celery FAILURE.
pub async fn publish_failed(
    events: Arc<dyn UpdateEventSink>,
    kind: Kind,
    task: Uuid,
    total: usize,
    message: &str,
    owner: Arc<dyn Send + Sync>,
) {
    let _=event(&events,json!({"type":kind.progress(),"task_id":task,"progress":0,"total":total,"status":"failed","message":format!("Task failed: {message}"),"error":message}),owner).await;
}

async fn event(
    events: &Arc<dyn UpdateEventSink>,
    data: Value,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), StoreError> {
    // core.utils.send_websocket_update catches send exceptions, so a settled
    // unavailable sink does not roll back or fail these task bodies.
    let _ = events
        .send_retained("updates", json!({"type":"update","data":data}), owner)
        .await;
    Ok(())
}
#[derive(Debug)]
struct LookupFailure {
    class: &'static str,
    message: String,
}
#[derive(Debug)]
enum BodyError {
    Store(StoreError),
    Lookup(LookupFailure),
}
impl From<StoreError> for BodyError {
    fn from(v: StoreError) -> Self {
        Self::Store(v)
    }
}
impl From<sqlx::Error> for BodyError {
    fn from(v: sqlx::Error) -> Self {
        Self::Store(v.into())
    }
}
impl From<LookupFailure> for BodyError {
    fn from(v: LookupFailure) -> Self {
        Self::Lookup(v)
    }
}
fn query_id(value: &Value) -> Result<Option<i64>, LookupFailure> {
    let unknown = || LookupFailure {
        class: "Unqualified",
        message: String::new(),
    };
    match value {
        Value::Null => return Ok(None),
        Value::Bool(v) => return Ok(Some(i64::from(*v))),
        Value::Number(v) => {
            return v
                .as_i64()
                .or_else(|| {
                    v.as_f64()
                        .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v < i64::MAX as f64)
                        .map(|v| v.trunc() as i64)
                })
                .map(Some)
                .ok_or_else(unknown)
        }
        Value::String(v) => {
            if let Some(s) = dispatcharr_ingest_core::source_integer_literal(v) {
                return s.parse::<i64>().map(Some).map_err(|_| unknown());
            }
        }
        _ => (),
    }
    let representation = crate::python_action::collection_text(value, 8192).ok_or_else(unknown)?;
    Err(LookupFailure {
        class: if value.is_array() || value.is_object() {
            "TypeError"
        } else {
            "ValueError"
        },
        message: format!("Field 'id' expected a number but got {representation}."),
    })
}
fn query_ids(values: &[Value]) -> Result<Vec<i64>, LookupFailure> {
    values
        .iter()
        .map(query_id)
        .collect::<Result<Vec<_>, _>>()
        .map(|v| v.into_iter().flatten().collect())
}
pub fn source_truthy(value: &Value) -> bool {
    crate::channels::truthy(Some(value))
}
fn all_profiles(r: &Request) -> bool {
    r.profiles.as_ref().is_none_or(|v| {
        v.iter()
            .any(|v| matches!(v, Value::Bool(false)) || v.as_f64() == Some(0.0))
    })
}
fn original_stream_key(v: &Value) -> Option<i64> {
    match v {
        Value::Bool(v) => Some(i64::from(*v)),
        Value::Number(n) => n.as_i64().or_else(|| {
            n.as_f64()
                .filter(|v| v.is_finite() && v.fract() == 0.0 && v.abs() < 9_007_199_254_000_000.0)
                .map(|v| v as i64)
        }),
        _ => None,
    }
}
/// Source API publishes first, THEN prepares this actual FK count lookup.
/// A malformed source is a post-publication generic HTTP500, not early501.
pub async fn source_logo_count(store: &Store, value: &Value) -> Result<usize, StoreError> {
    let source = query_id(value).map_err(|_| StoreError::UnhandledRestFault)?;
    let mut c = store.pool().acquire().await?;
    c.close_on_drop();
    let observed=sqlx::query_scalar::<_,i64>("SELECT count(*) FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON e.id=c.epg_data_id WHERE e.epg_source_id=$1 AND e.icon_url IS NOT NULL AND e.icon_url<>''").bind(source).fetch_one(&mut *c).await;
    let ready = c.ping().await;
    let close = c.close().await;
    let value = observed?;
    ready?;
    close?;
    Ok(value as usize)
}
async fn epg(
    c: &mut PgConnection,
    events: &Arc<dyn UpdateEventSink>,
    r: &Request,
    task: Uuid,
    owner: Arc<dyn Send + Sync>,
    total_seen: &mut usize,
) -> Result<Value, BodyError> {
    let logo = r.kind == Kind::Logos;
    let source = r.source.as_ref().filter(|_| logo);
    let total = if let Some(source) = source {
        sqlx::query_scalar::<_,i64>("SELECT count(*) FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON e.id=c.epg_data_id WHERE e.epg_source_id=$1 AND e.icon_url IS NOT NULL AND e.icon_url<>''").bind(query_id(source)?).fetch_one(&mut*c).await? as usize
    } else {
        r.ids.len()
    };
    *total_seen = total;
    let label = if logo {
        "logo"
    } else if r.kind == Kind::Names {
        "name"
    } else {
        "TVG-ID"
    };
    event(events,json!({"type":r.kind.progress(),"task_id":task,"progress":0,"total":total,"status":"running","message":format!("Starting EPG {label} setting...")}),owner.clone()).await?;
    let mut updated = 0usize;
    let mut created = 0usize;
    let mut errors: Vec<String> = Vec::new();
    let mut progress = 0usize;
    let mut last = 0i64;
    let batch = if logo { 500 } else { 100 };
    loop {
        let ids = if let Some(source) = source {
            sqlx::query_scalar::<_,i64>("SELECT c.id::bigint FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON e.id=c.epg_data_id WHERE e.epg_source_id=$1 AND e.icon_url IS NOT NULL AND e.icon_url<>'' AND c.id>$2 ORDER BY c.id LIMIT 500").bind(query_id(source)?).bind(last).fetch_all(&mut*c).await?.into_iter().map(Value::from).collect::<Vec<_>>()
        } else {
            r.ids
                .iter()
                .skip(progress)
                .take(batch)
                .cloned()
                .collect::<Vec<_>>()
        };
        if ids.is_empty() {
            break;
        }
        let ids = query_ids(&ids)?;
        if logo {
            let stats = logos(c, &ids).await?;
            updated += stats.0;
            created += stats.1;
            let remaining = 100usize.saturating_sub(errors.len());
            errors.extend(stats.2.into_iter().take(remaining));
        } else {
            // Django's original default queryset has no custom ordering here.
            // A single batch UPDATE is atomic, but prior batches stay committed.
            let field = if r.kind == Kind::Names {
                "name"
            } else {
                "tvg_id"
            };
            let sql=format!("UPDATE dispatcharr_channels_channel c SET {field}=e.{field} FROM epg_epgdata e LEFT JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE c.id=ANY($1) AND c.epg_data_id=e.id AND s.source_type IS DISTINCT FROM 'dummy' AND e.{field} IS NOT NULL AND e.{field}<>'' AND c.{field} IS DISTINCT FROM e.{field}");
            updated += sqlx::query(&sql)
                .bind(&ids)
                .execute(&mut *c)
                .await?
                .rows_affected() as usize;
        }
        progress += if source.is_some() {
            ids.len()
        } else {
            batch.min(total.saturating_sub(progress))
        };
        last = *ids.iter().max().unwrap_or(&last);
        // Source source-id branch emits every FULL500 batch but not final tail.
        if !(source.is_some() && ids.len() < batch) {
            let mut data = json!({"type":r.kind.progress(),"task_id":task,"progress":progress,"total":total,"status":"running","updated_count":updated,"message":if logo{format!("Updated {updated} channel logos, created {created} new logos...")}else if r.kind==Kind::Names{format!("Updated {updated} channel names...")}else{format!("Updated {updated} channel TVG-IDs...")}});
            if logo {
                data["created_logos_count"] = json!(created);
            }
            event(events, data, owner.clone()).await?;
        }
        if source.is_none() && progress >= total {
            break;
        }
    }
    let mut result = json!({"status":"completed","updated_count":updated,"error_count":errors.len(),"errors":errors});
    if logo {
        result["created_logos_count"] = json!(created);
    }
    let mut done = result.clone();
    done["type"] = json!(r.kind.progress());
    done["task_id"] = json!(task);
    done["progress"] = json!(total);
    done["total"] = json!(total);
    done["message"] = json!(if logo {
        format!("Successfully updated {updated} channel logos and created {created} new logos from EPG data")
    } else if r.kind == Kind::Names {
        format!("Successfully updated {updated} channel names from EPG data")
    } else {
        format!("Successfully updated {updated} channel TVG-IDs from EPG data")
    });
    event(events, done, owner).await?;
    Ok(result)
}
async fn logos(
    c: &mut PgConnection,
    ids: &[i64],
) -> Result<(usize, usize, Vec<String>), StoreError> {
    let rows=sqlx::query("SELECT c.id::bigint,e.icon_url,e.name,e.tvg_id,CASE WHEN octet_length(l.url)<=65536 THEN l.url END AS current_url FROM dispatcharr_channels_channel c LEFT JOIN epg_epgdata e ON e.id=c.epg_data_id LEFT JOIN dispatcharr_channels_logo l ON l.id=c.logo_id WHERE c.id=ANY($1)").bind(ids).fetch_all(&mut*c).await?;
    let mut work = Vec::new();
    let mut unique = Vec::new();
    let mut seen = HashMap::new();
    for row in rows {
        let icon: Option<String> = row.try_get("icon_url")?;
        let Some(icon) = icon else { continue };
        let icon = icon
            .trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
            .to_owned();
        if icon.is_empty() {
            continue;
        }
        let old: Option<String> = row.try_get("current_url")?;
        if old.as_deref() == Some(&icon) {
            continue;
        }
        let id: i64 = row.try_get("id")?;
        work.push((id, icon.clone()));
        if !seen.contains_key(&icon) {
            let name: Option<String> = row.try_get("name")?;
            let tvg: Option<String> = row.try_get("tvg_id")?;
            let name = name
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| format!("Logo for {}", tvg.unwrap_or_else(|| "None".into())));
            seen.insert(icon.clone(), name);
            unique.push(icon);
        }
    }
    if work.is_empty() {
        return Ok((0, 0, Vec::new()));
    }
    let rows =
        sqlx::query("SELECT id::bigint,url FROM dispatcharr_channels_logo WHERE url=ANY($1)")
            .bind(&unique)
            .fetch_all(&mut *c)
            .await?;
    let mut found = HashMap::new();
    for row in rows {
        found.insert(
            row.try_get::<String, _>("url")?,
            row.try_get::<i64, _>("id")?,
        );
    }
    let missing = unique
        .iter()
        .filter(|u| !found.contains_key(*u))
        .cloned()
        .collect::<Vec<_>>();
    let created = missing.len();
    // Source Logo.bulk_create is one transaction separate from channel updates.
    if !missing.is_empty() {
        let mut tx = c.begin().await?;
        for u in &missing {
            sqlx::query("INSERT INTO dispatcharr_channels_logo(name,url) VALUES($1,$2) ON CONFLICT DO NOTHING").bind(&seen[u]).bind(u).execute(&mut*tx).await?;
        }
        tx.commit().await?;
        for row in
            sqlx::query("SELECT id::bigint,url FROM dispatcharr_channels_logo WHERE url=ANY($1)")
                .bind(&unique)
                .fetch_all(&mut *c)
                .await?
        {
            found.insert(
                row.try_get::<String, _>("url")?,
                row.try_get::<i64, _>("id")?,
            );
        }
    }
    let mut changes = Vec::new();
    let mut errors = Vec::new();
    for (id, u) in work {
        if let Some(logo) = found.get(&u) {
            changes.push((id, *logo));
        } else {
            errors.push(format!("Channel {id}: Logo not found for {u}"));
        }
    }
    if !changes.is_empty() {
        let mut tx = c.begin().await?;
        for (id, logo) in &changes {
            sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=$2 WHERE id=$1")
                .bind(id)
                .bind(logo)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
    }
    Ok((changes.len(), created, errors))
}
async fn bulk(
    c: &mut PgConnection,
    events: &Arc<dyn UpdateEventSink>,
    r: &Request,
    task: Uuid,
    owner: Arc<dyn Send + Sync>,
) -> Result<Value, BodyError> {
    let invalid_reserved:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel WHERE channel_number::text IN ('NaN','Infinity','-Infinity') OR abs(channel_number)>9007199254000000 UNION ALL SELECT 1 FROM dispatcharr_channels_channeloverride WHERE channel_number::text IN ('NaN','Infinity','-Infinity') OR abs(channel_number)>9007199254000000)").fetch_one(&mut*c).await?;
    if invalid_reserved {
        return Err(
            StoreError::Unsupported("Channel reserved numbering outside exact profile").into(),
        );
    }
    let total = r.ids.len();
    let mut processed = 0usize;
    event(events,json!({"type":r.kind.progress(),"task_id":task,"progress":0,"total":total,"status":"starting","message":format!("Starting bulk creation of {total} channels...")}),owner.clone()).await?;
    // The Source set can contain all reserved values. A session-private SQL set
    // keeps application RAM bounded while retaining this one-time snapshot.
    sqlx::query("CREATE TEMP TABLE rust_channel_reserved ON COMMIT PRESERVE ROWS AS SELECT channel_number AS number FROM dispatcharr_channels_channel WHERE channel_number IS NOT NULL UNION SELECT channel_number FROM dispatcharr_channels_channeloverride WHERE channel_number IS NOT NULL").execute(&mut*c).await?;
    sqlx::query("CREATE INDEX ON rust_channel_reserved(number)")
        .execute(&mut *c)
        .await?;
    let highest: Option<f64> = sqlx::query_scalar("SELECT max(number) FROM rust_channel_reserved")
        .fetch_one(&mut *c)
        .await?;
    let mut next = match r.starting.as_ref() {
        None => 1.0,
        Some(Value::Bool(_)) => 1.0,
        Some(Value::Number(n)) => {
            let v = n
                .as_f64()
                .filter(|v| v.is_finite() && v.abs() < 9_007_199_254_000_000.0)
                .ok_or_else(|| {
                    BodyError::Store(StoreError::Unsupported(
                        "Channel start outside bounded finite profile",
                    ))
                })?;
            if v == 0.0 {
                1.0
            } else if v == -1.0 {
                highest.map(|h| h.trunc() + 1.0).unwrap_or(1.0)
            } else {
                v
            }
        }
        _ => {
            return Err(StoreError::Unsupported(
                "Channel start type outside measured numeric profile",
            )
            .into())
        }
    };
    let mut channel_data = Vec::new();
    let mut stream_links = Vec::new();
    let mut logo_urls = Vec::new();
    let mut logo_names = Vec::new();
    let mut errors: Vec<Value> = Vec::new();
    for ids in r.ids.chunks(100) {
        let query = query_ids(ids)?;
        let rows=sqlx::query("SELECT id::bigint,name,tvg_id,channel_group_id::bigint,stream_chno,CASE WHEN octet_length(logo_url)<=2000 THEN logo_url END AS logo_url,is_adult,is_radio,custom_properties->'tvc-guide-stationid' AS station FROM dispatcharr_channels_stream WHERE id=ANY($1) ORDER BY updated_at DESC").bind(&query).fetch_all(&mut*c).await?;

        event(events,json!({"type":r.kind.progress(),"task_id":task,"progress":processed,"total":total,"status":"processing","message":format!("Processing streams {}-{} of {total}...",processed+1,(processed+100).min(total))}),owner.clone()).await?;
        let mut streams = HashMap::new();
        for row in rows {
            streams.insert(row.try_get::<i64, _>("id")?, row);
        }
        for original in ids {
            let Some(id) = original_stream_key(original) else {
                continue;
            };
            let Some(row) = streams.get(&id) else {
                continue;
            };
            let provider: Option<f64> = row.try_get("stream_chno")?;
            let mut number = if r.starting.is_none() { provider } else { None };
            if let Some(v) = number {
                if sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM rust_channel_reserved WHERE number=$1)",
                )
                .bind(v)
                .fetch_one(&mut *c)
                .await?
                {
                    number = None;
                }
            }
            if number.is_none() {
                loop {
                    let v = next;
                    if !sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM rust_channel_reserved WHERE number=$1)",
                    )
                    .bind(v)
                    .fetch_one(&mut *c)
                    .await?
                    {
                        number = Some(v);
                        break;
                    }
                    next += 1.0;
                    if !next.is_finite() || next.abs() >= 9_007_199_254_000_000.0 {
                        return Err(StoreError::Unsupported("Channel numbering capacity").into());
                    }
                }
            }
            let number = number.unwrap();
            sqlx::query("INSERT INTO rust_channel_reserved(number) VALUES($1)")
                .bind(number)
                .execute(&mut *c)
                .await?;
            let name: String = row.try_get("name")?;
            let tvg: Option<String> = row.try_get("tvg_id")?;
            let station: Option<Value> = row.try_get("station")?;
            let station = station.unwrap_or(Value::Null);
            let epg = if let Some(tvg) = &tvg {
                sqlx::query_scalar::<_, i64>(
                    "SELECT id::bigint FROM epg_epgdata WHERE tvg_id=$1 ORDER BY id LIMIT 1",
                )
                .bind(tvg)
                .fetch_optional(&mut *c)
                .await?
            } else {
                sqlx::query_scalar::<_, i64>(
                    "SELECT id::bigint FROM epg_epgdata WHERE tvg_id IS NULL ORDER BY id LIMIT 1",
                )
                .fetch_optional(&mut *c)
                .await?
            };
            channel_data.push(json!({"channel_number":number,"name":name,"tvg_id":tvg,"tvc_guide_stationid":station,"is_adult":row.try_get::<bool,_>("is_adult")?,"is_radio":row.try_get::<bool,_>("is_radio")?,"channel_group_id":row.try_get::<Option<i64>,_>("channel_group_id")?,"epg_data_id":epg,"uuid":Uuid::new_v4()}));
            stream_links.push(id);
            let url: Option<String> = row.try_get("logo_url")?;
            let url = url.filter(|u| !u.is_empty() && u.len() <= 2000);
            logo_urls.push(url.clone());
            if let Some(url) = url {
                logo_names.push((
                    url,
                    if name.is_empty() {
                        tvg.unwrap_or_default()
                    } else {
                        name
                    },
                ));
            }
            processed += 1;
        }
    }
    if !logo_names.is_empty() {
        event(events,json!({"type":r.kind.progress(),"task_id":task,"progress":processed,"total":total,"status":"creating_logos","message":format!("Creating {} logos...",logo_names.len())}),owner.clone()).await?;
        let mut tx = c.begin().await?;
        for (url, name) in &logo_names {
            sqlx::query("INSERT INTO dispatcharr_channels_logo(name,url) VALUES($1,$2) ON CONFLICT DO NOTHING").bind(name).bind(url).execute(&mut*tx).await?;
        }
        tx.commit().await?;
    }
    let urls = logo_urls.iter().flatten().cloned().collect::<Vec<_>>();
    let mut logos = HashMap::new();
    for row in sqlx::query("SELECT id::bigint,url FROM dispatcharr_channels_logo WHERE url=ANY($1)")
        .bind(&urls)
        .fetch_all(&mut *c)
        .await?
    {
        logos.insert(
            row.try_get::<String, _>("url")?,
            row.try_get::<i64, _>("id")?,
        );
    }
    if !channel_data.is_empty() {
        event(events,json!({"type":r.kind.progress(),"task_id":task,"progress":processed,"total":total,"status":"creating_channels","message":format!("Creating {} channels in database...",channel_data.len())}),owner.clone()).await?;
        let mut tx = c.begin().await?;
        let mut created = Vec::new();
        for rows in channel_data.chunks(100) {
            let values = Value::Array(rows.to_vec());
            let ids=sqlx::query_scalar::<_,i64>("INSERT INTO dispatcharr_channels_channel(channel_number,name,tvg_id,tvc_guide_stationid,is_adult,is_radio,channel_group_id,epg_data_id,uuid,user_level,auto_created,is_catchup,catchup_days,hidden_from_output,created_at,updated_at) SELECT x.channel_number,x.name,x.tvg_id,x.tvc_guide_stationid,x.is_adult,x.is_radio,x.channel_group_id,x.epg_data_id,x.uuid,0,false,false,0,false,$2,$2 FROM jsonb_populate_recordset(NULL::dispatcharr_channels_channel,$1) x RETURNING id::bigint").bind(values).bind(chrono::Utc::now()).fetch_all(&mut*tx).await?;
            created.extend(ids);
        }
        for (index, id) in created.iter().enumerate() {
            if let Some(logo) = logo_urls[index].as_ref().and_then(|u| logos.get(u)) {
                sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=$2 WHERE id=$1")
                    .bind(id)
                    .bind(logo)
                    .execute(&mut *tx)
                    .await?;
            }
            sqlx::query("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") VALUES($1,$2,0) ON CONFLICT DO NOTHING").bind(id).bind(stream_links[index]).execute(&mut*tx).await?;
        }
        sqlx::query("UPDATE dispatcharr_channels_channel c SET is_catchup=EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active),catchup_days=COALESCE((SELECT max(s.catchup_days) FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active),0) WHERE c.id=ANY($1)").bind(&created).execute(&mut*tx).await?;
        for id in created {
            let profiles = if all_profiles(r) {
                None
            } else {
                match r.profiles.as_ref().map(|v| query_ids(v)).transpose() {
                    Ok(v) => v,
                    Err(error) if error.class != "Unqualified" => {
                        errors.push(json!({"channel_id":id,"error":format!("Failed to add to profiles: {}",error.message)}));
                        continue;
                    }
                    Err(_) => {
                        return Err(StoreError::Unsupported(
                            "Profile conversion outside bounded domain",
                        )
                        .into())
                    }
                }
            };
            sqlx::query("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_profile_id,channel_id,enabled) SELECT id,$1,true FROM dispatcharr_channels_channelprofile WHERE $2::bigint[] IS NULL OR id=ANY($2) ON CONFLICT DO NOTHING").bind(id).bind(profiles).execute(&mut*tx).await?;
        }
        tx.commit().await?;
    }
    let count = channel_data.len();
    event(events,json!({"type":r.kind.progress(),"task_id":task,"progress":total,"total":total,"status":"completed","message":format!("Successfully created {count} channels"),"created_count":count,"error_count":errors.len(),"errors":errors.iter().take(10).collect::<Vec<_>>()}),owner.clone()).await?;
    event(
        events,
        json!({"type":"channels_created","count":count}),
        owner,
    )
    .await?;
    Ok(
        json!({"status":"completed","created_count":count,"error_count":errors.len(),"errors":errors}),
    )
}
impl crate::ProducerIdentitySource {
    /// API delay arguments are a tuple, unlike Beat's nonempty JSON list.
    pub fn channel_task_representations(
        &self,
        args: &Value,
        kwargs: &Value,
    ) -> Result<(String, String), StoreError> {
        Ok((
            crate::celery_metadata::arguments(args)?,
            crate::celery_metadata::keywords(kwargs)?,
        ))
    }
}
#[cfg(test)]
mod django_lookup_contract {
    use super::*;
    #[test]
    fn deferred_lookup_preserves_source_coercion_and_original_bulk_dictionary_keys() {
        // Grounded in pinned Django6.0.9 IntegerField.get_prep_value/In, not
        // another Source execution. Raw Source98 established API acceptance.
        assert_eq!(
            query_ids(&[Value::Null, json!(true), json!("١_٢"), json!(1.9)]).unwrap(),
            vec![1, 12, 1]
        );
        let error = query_ids(&[json!("not-an-integer")]).unwrap_err();
        assert_eq!(error.class, "ValueError");
        assert_eq!(
            error.message,
            "Field 'id' expected a number but got 'not-an-integer'."
        );
        assert_eq!(original_stream_key(&json!("1")), None);
        assert_eq!(original_stream_key(&json!(1.5)), None);
        assert_eq!(original_stream_key(&json!(1.0)), Some(1));
        let error = query_ids(&[json!({})]).unwrap_err();
        assert_eq!(error.class, "TypeError");
        assert_eq!(error.message, "Field 'id' expected a number but got {}.");
        assert_eq!(
            query_id(&json!("9223372036854775808")).unwrap_err().class,
            "Unqualified"
        );
    }
}
