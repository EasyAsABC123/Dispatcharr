//! Bounded ordinary single-stream creation on an already admitted copied SQLite runtime.
//! Original Dispatcharr authors retain application, serializer and signal credit.
use super::CopiedSqliteRuntime;
use crate::{RetainedSqliteRestReads, StoreError, User};
use axum::{
    extract::{ConnectInfo, OriginalUri, State},
    http::HeaderMap,
    response::Response,
    Json, Router,
};
use serde_json::{json, Value};
use sqlx::{Connection, Row, SqliteConnection};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use url::Url;

pub(crate) type Sink = Arc<dyn dispatcharr_contracts::UpdateEventSink>;
pub(crate) type Body = Result<Json<Value>, axum::extract::rejection::JsonRejection>;
const PATH: &str = "/api/channels/channels/from-stream/";

// Production resolves the already admitted runtime and its bounded request slot.
// Tests inject only this boundary; they do not fabricate runtime admission.
pub(crate) trait Owner: Clone + Send + Sync + 'static {
    fn request(&self) -> Result<RetainedSqliteRestReads, StoreError>;
    fn sink(&self) -> Option<Sink>;
}
impl Owner for CopiedSqliteRuntime {
    fn request(&self) -> Result<RetainedSqliteRestReads, StoreError> {
        self.allocate()
    }
    fn sink(&self) -> Option<Sink> {
        self.ip_event_sink.clone()
    }
}
pub(crate) fn router() -> Router<CopiedSqliteRuntime> {
    Router::new().route(PATH, axum::routing::post(handle::<CopiedSqliteRuntime>))
}
pub(crate) async fn handle<O: Owner>(
    State(owner): State<O>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Body,
) -> Result<Response, StoreError> {
    owner
        .request()?
        .single_from_stream(peer, headers, uri, body, owner.sink())
        .await
}
fn outside() -> StoreError {
    StoreError::Unsupported("Copied SQLite single-create supports a bounded stream without logo/Guide work and default profiles, no profiles, or one existing selected profile")
}
fn no_stream() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No Stream matches the given query."}))
}
fn bad_number() -> StoreError {
    StoreError::StatusBody(400, json!({"error":"channel_number must be an integer."}))
}

enum ProfileSelection {
    All,
    None,
    One(i64),
}
fn profile_selection(value: Option<&Value>) -> Result<ProfileSelection, StoreError> {
    match value.filter(|v| !v.is_null()) {
        None => Ok(ProfileSelection::All),
        Some(Value::Array(values)) if values.is_empty() => Ok(ProfileSelection::None),
        Some(Value::Array(values)) if values.len() == 1 => values[0]
            .as_i64()
            .filter(|id| *id > 0)
            .map(ProfileSelection::One)
            .ok_or_else(outside),
        _ => Err(outside()),
    }
}

async fn number(
    db: &mut SqliteConnection,
    supplied: Option<&Value>,
    provider: Option<f64>,
) -> Result<f64, StoreError> {
    let reserved: Vec<f64> = sqlx::query_scalar("SELECT channel_number FROM dispatcharr_channels_channel WHERE channel_number IS NOT NULL UNION ALL SELECT channel_number FROM dispatcharr_channels_channeloverride WHERE channel_number IS NOT NULL LIMIT 4097")
        .fetch_all(db).await?;
    if reserved.len() > 4096 || reserved.iter().any(|n| !n.is_finite() || n.abs() > 1e12) {
        return Err(outside());
    }
    let supplied = supplied.filter(|v| !v.is_null());
    let mut candidate = match supplied {
        None => provider.unwrap_or(1.0),
        Some(v) if crate::json_compat::python_equal(v, &json!(0)) => 1.0,
        Some(v) if crate::json_compat::python_equal(v, &json!(-1)) => reserved
            .iter()
            .copied()
            .reduce(f64::max)
            .map_or(1.0, |v| v.trunc() + 1.0),
        Some(v) => crate::channel_writes::decimal("channel_number", v, None)
            .map_err(|_| bad_number())?
            .as_f64()
            .ok_or_else(bad_number)?,
    };
    if !candidate.is_finite() || candidate.abs() > 1e12 {
        return Err(outside());
    }
    while reserved.contains(&candidate) {
        candidate += 1.0;
    }
    Ok(candidate)
}

async fn mark_custom(db: &mut SqliteConnection, group: i64, custom: i64) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO dispatcharr_channels_channelgroupm3uaccount(channel_group_id,m3u_account_id,custom_properties,enabled,auto_channel_sync,auto_sync_channel_start,auto_sync_channel_end,last_seen,is_stale) SELECT ?1,?2,'{}',true,false,NULL,NULL,?3,false WHERE NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount WHERE channel_group_id=?1 AND m3u_account_id=?2)")
        .bind(group).bind(custom).bind(chrono::Utc::now().naive_utc()).execute(db).await?;
    Ok(())
}

pub(crate) struct Created {
    pub response: Vec<u8>,
    pub event: Value,
}

/// All unsupported work is rejected before any DML. BEGIN IMMEDIATE holds the
/// number reservation, preflight and all selected-path writes on one connection.
/// Commit includes group, channel, stream relation, rollup and all memberships.
pub(crate) async fn command(
    db: &mut SqliteConnection,
    user: &User,
    body: Body,
    base: &Url,
    limit: usize,
) -> Result<Created, StoreError> {
    let body = crate::admin_writes::body_object(body)?;
    let stream_id = body
        .get("stream_id")
        .filter(|v| crate::channels::truthy(Some(v)))
        .ok_or_else(|| StoreError::StatusBody(400, json!({"error":"Missing stream_id"})))?;
    // The selected React path supplies a positive integer, not coercion-dependent IDs.
    let stream_id = stream_id
        .as_i64()
        .filter(|id| *id > 0)
        .ok_or_else(outside)?;
    if body.keys().any(|k| {
        !matches!(
            k.as_str(),
            "stream_id" | "name" | "channel_number" | "channel_profile_ids"
        )
    }) || serde_json::to_vec(&body).map_err(|_| outside())?.len() > 16384
    {
        return Err(outside());
    }
    if body.get("channel_number").is_some_and(|v| {
        !v.is_null() && !v.as_f64().is_some_and(|n| n.is_finite() && n.abs() <= 1e12)
    }) {
        return Err(outside());
    }
    let selection = profile_selection(body.get("channel_profile_ids"))?;
    let mut tx = db.begin_with("BEGIN IMMEDIATE").await?;
    let row_bytes:Option<i64>=sqlx::query_scalar("SELECT COALESCE(length(CAST(s.name AS BLOB)),0)+COALESCE(length(CAST(s.url AS BLOB)),0)+COALESCE(length(CAST(s.logo_url AS BLOB)),0)+COALESCE(length(CAST(s.tvg_id AS BLOB)),0)+COALESCE(length(CAST(s.local_file AS BLOB)),0)+COALESCE(length(CAST(s.stream_hash AS BLOB)),0)+COALESCE(length(CAST(s.stream_stats AS BLOB)),0)+COALESCE(length(CAST(s.custom_properties AS BLOB)),0)+COALESCE(length(CAST(s.updated_at AS BLOB)),0)+COALESCE(length(CAST(s.last_seen AS BLOB)),0)+COALESCE(length(CAST(s.stream_stats_updated_at AS BLOB)),0)+COALESCE(length(CAST(a.name AS BLOB)),0) FROM dispatcharr_channels_stream s LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE s.id=?")
        .bind(stream_id).fetch_optional(&mut *tx).await?;
    let row_bytes = row_bytes.ok_or_else(no_stream)?;
    if row_bytes > 65536 {
        return Err(outside());
    }
    let stream=sqlx::query("SELECT name,tvg_id,channel_group_id,logo_url,custom_properties,is_adult,is_radio,stream_chno FROM dispatcharr_channels_stream WHERE id=?")
        .bind(stream_id).fetch_one(&mut *tx).await?;
    let title: String = stream.try_get("name")?;
    let tvg: Option<String> = stream.try_get("tvg_id")?;
    let logo: Option<String> = stream.try_get("logo_url")?;
    let properties: Option<sqlx::types::Json<Value>> = stream.try_get("custom_properties")?;
    let properties = properties.map_or(json!({}), |v| v.0);
    if logo.is_some_and(|v| !v.is_empty()) || (!properties.is_null() && !properties.is_object()) {
        return Err(outside());
    }
    let epg: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM epg_epgdata WHERE tvg_id IS ?)")
            .bind(tvg.as_deref())
            .fetch_one(&mut *tx)
            .await?;
    if epg {
        return Err(outside());
    } // Includes dummy Guide: no cache/producer bypass.
    let tvg = tvg
        .map(|v| crate::admin_writes::character("tvg_id", &json!(v), Some(255), true))
        .transpose()?;
    let triggers:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='trigger' AND tbl_name IN ('dispatcharr_channels_channel','dispatcharr_channels_channelstream','dispatcharr_channels_channelprofilemembership','dispatcharr_channels_channelgroup','dispatcharr_channels_channelgroupm3uaccount'))").fetch_one(&mut *tx).await?;
    if triggers {
        return Err(outside());
    }
    let name = crate::admin_writes::character(
        "name",
        body.get("name")
            .filter(|v| !v.is_null())
            .unwrap_or(&json!(title)),
        Some(512),
        false,
    )?;
    let station = match properties
        .get("tvc-guide-stationid")
        .filter(|v| !v.is_null())
    {
        None => None,
        Some(v) => Some(crate::admin_writes::character(
            "tvc_guide_stationid",
            v,
            Some(255),
            true,
        )?),
    };
    let profiles = match selection {
        ProfileSelection::None => Vec::new(),
        ProfileSelection::All => {
            let profiles: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM dispatcharr_channels_channelprofile ORDER BY id LIMIT 129",
            )
            .fetch_all(&mut *tx)
            .await?;
            if profiles.len() > 128 {
                return Err(outside());
            }
            profiles
        }
        ProfileSelection::One(id) => {
            let found: Option<i64> =
                sqlx::query_scalar("SELECT id FROM dispatcharr_channels_channelprofile WHERE id=?")
                    .bind(id)
                    .fetch_optional(&mut *tx)
                    .await?;
            // Source's missing/duplicate profile error branches can commit the
            // preceding channel. Keep them unsupported BEFORE DML here.
            vec![found.ok_or_else(outside)?]
        }
    };
    let custom: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM m3u_m3uaccount WHERE name='custom' AND locked LIMIT 2")
            .fetch_all(&mut *tx)
            .await?;
    let [custom] = custom.as_slice() else {
        return Err(outside());
    };
    let mut group: Option<i64> = stream.try_get("channel_group_id")?;
    if let Some(id) = group {
        let bytes: Option<i64> = sqlx::query_scalar(
            "SELECT length(CAST(name AS BLOB)) FROM dispatcharr_channels_channelgroup WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        if !bytes.is_some_and(|n| n <= 65536) {
            return Err(outside());
        }
    }
    let channel_number = number(
        &mut tx,
        body.get("channel_number"),
        stream.try_get("stream_chno")?,
    )
    .await?;
    let adult: bool = stream.try_get("is_adult")?;
    let radio: bool = stream.try_get("is_radio")?;
    let default_group = group.is_none();
    // All remaining branches are qualified selected-path DML, after preflight.
    if group.is_none() {
        group = sqlx::query_scalar(
            "SELECT id FROM dispatcharr_channels_channelgroup WHERE name='Default Group'",
        )
        .fetch_optional(&mut *tx)
        .await?;
        if group.is_none() {
            group=Some(sqlx::query_scalar("INSERT INTO dispatcharr_channels_channelgroup(name) VALUES('Default Group') RETURNING id").fetch_one(&mut *tx).await?);
        }
    }
    let group = group.expect("selected existing or created default group");
    // Source serializer marks Default Group before the channel insert.
    if default_group {
        mark_custom(&mut tx, group, *custom).await?;
    }
    let stamp = chrono::Utc::now().naive_utc();
    let id:i64=sqlx::query_scalar("INSERT INTO dispatcharr_channels_channel(channel_number,name,channel_group_id,tvg_id,tvc_guide_stationid,uuid,user_level,is_adult,auto_created,is_catchup,catchup_days,is_radio,hidden_from_output,created_at,updated_at)
        VALUES(?,?,?,?,?,?,0,?,false,false,0,?,false,?,?) RETURNING id")
        .bind(channel_number).bind(&name).bind(group).bind(tvg).bind(station).bind(uuid::Uuid::new_v4().simple().to_string()).bind(adult).bind(radio).bind(stamp).bind(stamp).fetch_one(&mut *tx).await?;
    // Channel.post_save marks the manual group after insertion (idempotent
    // when serializer.get_or_create_manual already marked the default group).
    mark_custom(&mut tx, group, *custom).await?;
    sqlx::query("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") VALUES(?,?,0)").bind(id).bind(stream_id).execute(&mut *tx).await?;
    crate::channel_writes::sqlite_relation_effects::after_link_change(&mut tx, id).await?;
    for profile in profiles {
        sqlx::query("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_profile_id,channel_id,enabled) VALUES(?,?,true)").bind(profile).bind(id).execute(&mut *tx).await?;
    }
    let mut response =
        crate::channels::sqlite_list::read(&mut tx, user, &HashMap::new(), base, Some(id)).await?;
    // Source keeps its pre-rollup serializer instance for the immediate response.
    response["is_catchup"] = json!(false);
    response["catchup_days"] = json!(0);
    let encoded = crate::json_compat::compact_json_bytes(&response).map_err(|_| outside())?;
    let encoded = String::from_utf8(encoded)
        .map_err(|_| outside())?
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
        .into_bytes();
    if encoded.len() > limit {
        return Err(outside());
    }
    tx.commit().await?;
    Ok(Created {
        response: encoded,
        event: json!({"type":"update","data":{"type":"channels_created","count":1,"channel_id":id,"channel_name":name,"channel_number":channel_number}}),
    })
}
