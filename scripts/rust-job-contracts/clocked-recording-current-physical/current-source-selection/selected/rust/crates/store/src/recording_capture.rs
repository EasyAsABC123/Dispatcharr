//! Inert normal DVR snapshot groundwork, derived from actual Source3.
//! Original Dispatcharr authors retain application/algorithm/license credit.
//! PRIVATE UNCOMPILED: no registration, ACK, lease, media or terminal result.
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{pool::PoolConnection, Connection, PgConnection, PgPool, Postgres, Row};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Acquire,
    Context,
    Recording,
    Poster,
    MutationRead,
    MutationWrite,
    Audit,
    Ready,
    Close,
}
#[derive(Debug)]
pub enum Error {
    Unsupported(&'static str),
    Sql {
        stage: Stage,
        source: sqlx::Error,
        settlement: Option<sqlx::Error>,
    },
    Supervisor,
}
#[derive(Clone, Debug)]
pub struct RecordingIdentity {
    pub recording: i64,
    pub channel: i64,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}
/// Actual bounded database data only. A service must still validate filesystem,
/// cache/active-key/domain and due admission; this is NOT a runnable capability.
#[derive(Debug)]
pub struct CaptureSnapshot {
    pub identity: RecordingIdentity,
    pub channel_uuid: Uuid,
    pub channel_name: String,
    pub poster_logo_id: i64,
    pub owned_logo_path: String,
    pub periodic_name: String,
    pub properties: Value,
    pub dvr_settings: Value,
    /// SQL projection only, not the effective Source cache-authoritative group.
    /// Service admission must reconcile actual bounded cache observations first.
    pub system_settings_sql: Option<Value>,
    pub system_settings_row_present: bool,
    pub audit_rows: i64,
    pub actual_read_utc: DateTime<Utc>,
    _store_identity: Arc<()>,
}
pub struct CaptureStore {
    pool: PgPool,
    identity: Arc<()>,
    owned_logo_path: String,
}

/// Data-only Source whitelist; does not certify media work or permit a task ACK.
pub enum PropertyWrite {
    Prime(Value),
    RemovedOwnedHls,
    CompletedNormal {
        actual_ended: DateTime<Utc>,
        bytes_written: u64,
    },
}

// One actual settlement implementation for every local SQL operation. The
// surrounding supervised task still owns its original caller throughout await.
async fn settle<T>(
    mut connection: PoolConnection<Postgres>,
    result: Result<T, Error>,
) -> Result<T, Error> {
    let ready = connection.ping().await;
    let closed = connection.close().await;
    match (result, ready, closed) {
        (Ok(value), Ok(()), Ok(())) => Ok(value),
        (Err(Error::Sql { stage, source, .. }), ready, closed) => Err(Error::Sql {
            stage,
            source,
            settlement: ready.err().or_else(|| closed.err()),
        }),
        (_, Err(source), closed) => Err(Error::Sql {
            stage: Stage::Ready,
            source,
            settlement: closed.err(),
        }),
        (_, Ok(()), Err(source)) => Err(Error::Sql {
            stage: Stage::Close,
            source,
            settlement: None,
        }),
        (Err(error), Ok(()), Ok(())) => Err(error),
    }
}

impl CaptureStore {
    pub fn source3_local(pool: PgPool) -> Self {
        Self {
            pool,
            identity: Arc::new(()),
            owned_logo_path: "/app/media/rust-owned-clocked/logo.png".into(),
        }
    }
    pub async fn snapshot(
        self: Arc<Self>,
        expected: RecordingIdentity,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<CaptureSnapshot, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&expected.recording)
            || !(1..=i64::from(i32::MAX)).contains(&expected.channel)
            || expected.end <= expected.start
        {
            return Err(Error::Unsupported("recording identity"));
        }
        // If the caller future disappears, the actual SQL/Ready/close task still
        // owns its original caller guard. No timeout drops a raw pool operation.
        tokio::spawn(async move {
            let _caller = owner;
            let mut connection = self.pool.acquire().await.map_err(|source| Error::Sql {
                stage: Stage::Acquire,
                source,
                settlement: None,
            })?;
            let result = self.read_inner(&mut connection, expected).await;
            // SQLx can return an ErrorResponse before trailing ReadyForQuery.
            // Always fence it on the SAME physical connection before owner drop.
            settle(connection, result).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    async fn read_inner(
        &self,
        connection: &mut PgConnection,
        expected: RecordingIdentity,
    ) -> Result<CaptureSnapshot, Error> {
        // Stable empty Connect/plugin/short history is the captured first normal
        // domain. Counts do not fetch arbitrary config text or claim bounded DB
        // scan/CPU. Concurrent configuration mutation is outside this snapshot.
        let context: (bool, bool, bool, bool, i64) = sqlx::query_as(
            "SELECT NOT EXISTS(SELECT 1 FROM dispatcharr_connect_integration),NOT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription),NOT EXISTS(SELECT 1 FROM dispatcharr_connect_deliverylog),NOT EXISTS(SELECT 1 FROM plugins_pluginconfig),(SELECT count(*)::bigint FROM core_systemevent)"
        ).fetch_one(&mut *connection).await.map_err(|source| Error::Sql {
            stage: Stage::Context, source, settlement: None,
        })?;
        if !(context.0 && context.1 && context.2 && context.3 && (0..=98).contains(&context.4)) {
            return Err(Error::Unsupported(
                "empty integration/plugin and bounded audit context",
            ));
        }
        // Read actual text size at projection, never transfer unbounded settings
        // then reject them. Absence is retained; it is not default resolution.
        let system: Option<(Option<Value>, bool)> = sqlx::query_as(
            "SELECT CASE WHEN octet_length(value::text)<=8192 THEN value ELSE NULL END,(value IS NULL OR octet_length(value::text)<=8192) FROM core_coresettings WHERE key='system_settings' LIMIT 1"
        ).fetch_optional(&mut *connection).await.map_err(|source| Error::Sql {
            stage: Stage::Context, source, settlement: None,
        })?;
        if system.as_ref().is_some_and(|(_, supported)| !supported) {
            return Err(Error::Unsupported("system settings SQL wire bound"));
        }
        let row = sqlx::query(
            "SELECT r.id::bigint AS recording,r.channel_id::bigint AS channel,r.start_time,r.end_time,r.task_id,CASE WHEN octet_length(r.custom_properties::text)<=8192 THEN r.custom_properties ELSE NULL END AS properties,(r.custom_properties IS NOT NULL AND octet_length(r.custom_properties::text)<=8192) AS properties_supported,c.uuid AS channel_uuid,CASE WHEN octet_length(c.name)<=255 THEN c.name ELSE NULL END AS channel_name,c.epg_data_id::bigint AS epg_data_id,(c.stream_profile_id IS NULL AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.channel_id=c.id)) AS profile_supported,CASE WHEN octet_length(s.value::text)<=8192 THEN s.value ELSE NULL END AS dvr_settings,(s.value IS NOT NULL AND octet_length(s.value::text)<=8192) AS settings_supported FROM dispatcharr_channels_recording r JOIN dispatcharr_channels_channel c ON c.id=r.channel_id LEFT JOIN core_coresettings s ON s.key='dvr_settings' WHERE r.id=$1 LIMIT 1"
        ).bind(expected.recording).fetch_optional(&mut *connection).await
            .map_err(|source| Error::Sql { stage: Stage::Recording, source, settlement: None })?
            .ok_or(Error::Unsupported("missing recording"))?;
        let decode = |source| Error::Sql {
            stage: Stage::Recording,
            source,
            settlement: None,
        };
        let recording: i64 = row.try_get("recording").map_err(decode)?;
        let channel: i64 = row.try_get("channel").map_err(decode)?;
        let start: DateTime<Utc> = row.try_get("start_time").map_err(decode)?;
        let end: DateTime<Utc> = row.try_get("end_time").map_err(decode)?;
        let task: Option<String> = row.try_get("task_id").map_err(decode)?;
        let properties: Option<Value> = row.try_get("properties").map_err(decode)?;
        let settings: Option<Value> = row.try_get("dvr_settings").map_err(decode)?;
        let epg: Option<i64> = row.try_get("epg_data_id").map_err(decode)?;
        let name: Option<String> = row.try_get("channel_name").map_err(decode)?;
        let properties_supported: bool = row.try_get("properties_supported").map_err(decode)?;
        let settings_supported: Option<bool> = row.try_get("settings_supported").map_err(decode)?;
        let profile_supported: bool = row.try_get("profile_supported").map_err(decode)?;
        let periodic_name = format!("dvr-recording-{recording}");
        if recording != expected.recording
            || channel != expected.channel
            || start != expected.start
            || end != expected.end
            || epg.is_some()
            || task.as_deref() != Some(periodic_name.as_str())
            || !properties_supported
            || settings_supported != Some(true)
            || !profile_supported
        {
            return Err(Error::Unsupported("captured manual recording snapshot"));
        }
        let properties = properties.ok_or(Error::Unsupported("properties bound"))?;
        let settings = settings.ok_or(Error::Unsupported("settings bound"))?;
        if !properties.is_object() || properties["status"] != "scheduled" || !settings.is_object() {
            return Err(Error::Unsupported("scheduled bounded object data"));
        }
        let title = properties
            .get("program")
            .and_then(|value| value.get("title"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.len() <= 128)
            .ok_or(Error::Unsupported("bounded manual title"))?;
        let logo = properties
            .get("poster_logo_id")
            .and_then(Value::as_i64)
            .filter(|value| (1..=i64::from(i32::MAX)).contains(value))
            .ok_or(Error::Unsupported("prequalified local poster id"))?;
        // This is the captured Movie/local-VODLogo resolution branch only.
        // Refuse ties/provider/null/oversize; do not query a network fallback.
        let posters:Vec<(Option<i64>,Option<String>,bool)>=sqlx::query_as(
            "SELECT l.id::bigint,CASE WHEN octet_length(l.url)<=1024 THEN l.url ELSE NULL END,(l.url IS NOT NULL AND octet_length(l.url)<=1024) FROM vod_movie m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE UPPER(m.name::text)=UPPER($1) ORDER BY m.name LIMIT 2"
        ).bind(title).fetch_all(&mut *connection).await.map_err(|source|Error::Sql {stage:Stage::Poster,source,settlement:None})?;
        if !matches!(posters.as_slice(),[(Some(id),Some(path),true)] if *id==logo&&path==&self.owned_logo_path)
        {
            return Err(Error::Unsupported("unique captured local Movie poster"));
        }
        let system_settings_row_present = system.is_some();
        Ok(CaptureSnapshot {
            identity: expected,
            channel_uuid: row.try_get("channel_uuid").map_err(decode)?,
            channel_name: name.ok_or(Error::Unsupported("channel name"))?,
            poster_logo_id: logo,
            owned_logo_path: self.owned_logo_path.clone(),
            periodic_name: task.ok_or(Error::Unsupported("periodic task name"))?,
            properties,
            dvr_settings: settings,
            system_settings_sql: system.and_then(|(value, _)| value),
            system_settings_row_present,
            audit_rows: context.4,
            actual_read_utc: Utc::now(),
            _store_identity: self.identity.clone(),
        })
    }

    /// Normal scoped autocommit write. No retry or generic SQL->terminal mapping.
    /// The caller guard outlives actual ErrorResponse/Ready/close, even if the
    /// observation future disappears. Post-ACK uncertainty stays operational.
    pub async fn write_properties(
        self: Arc<Self>,
        recording: i64,
        write: PropertyWrite,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Value, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&recording) {
            return Err(Error::Unsupported("recording identity"));
        }
        tokio::spawn(async move {
            let _caller = owner;
            let mut connection = self.pool.acquire().await.map_err(|source| Error::Sql {
                stage: Stage::Acquire,
                source,
                settlement: None,
            })?;
            let result = Self::write_properties_inner(&mut connection, recording, write).await;
            settle(connection, result).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }

    async fn write_properties_inner(
        connection: &mut PgConnection,
        recording: i64,
        write: PropertyWrite,
    ) -> Result<Value, Error> {
        let row:Option<(Option<Value>,bool)>=sqlx::query_as(
            "SELECT CASE WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,(custom_properties IS NOT NULL AND octet_length(custom_properties::text)<=8192) FROM dispatcharr_channels_recording WHERE id=$1 LIMIT 1"
        ).bind(recording).fetch_optional(&mut *connection).await.map_err(|source|Error::Sql {stage:Stage::MutationRead,source,settlement:None})?;
        let before = match row {
            Some((Some(value), true)) if value.is_object() => value,
            _ => {
                return Err(Error::Unsupported(
                    "fresh bounded existing recording properties",
                ))
            }
        };
        let after = Self::merge_properties(recording, &before, write)?;
        if after == before {
            return Ok(before);
        }
        // Stable normal context is Source-authorized. An observed concurrent
        // change is operational, never silently overwritten/retried as normal.
        let affected=sqlx::query("UPDATE dispatcharr_channels_recording SET custom_properties=$2 WHERE id=$1 AND custom_properties=$3")
            .bind(recording).bind(&after).bind(&before).execute(&mut *connection).await
            .map_err(|source|Error::Sql {stage:Stage::MutationWrite,source,settlement:None})?.rows_affected();
        if affected != 1 {
            return Err(Error::Unsupported("recording changed during fresh merge"));
        }
        Ok(after)
    }

    /// The single metadata engine used by real SQL and Source-backed pure tests.
    /// It certifies only a bounded transformation, never physical media effects.
    pub fn merge_properties(
        recording: i64,
        before: &Value,
        write: PropertyWrite,
    ) -> Result<Value, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&recording)
            || serde_json::to_vec(before)
                .map_err(|_| Error::Unsupported("property serialization"))?
                .len()
                > 8192
        {
            return Err(Error::Unsupported("property input bound"));
        }
        let mut after = before.clone();
        let properties = after
            .as_object_mut()
            .ok_or(Error::Unsupported("fresh object"))?;
        match write {
            PropertyWrite::Prime(patch) => {
                if properties.get("status").and_then(Value::as_str) != Some("scheduled") {
                    return Err(Error::Unsupported("prime status changed"));
                }
                let patch = patch
                    .as_object()
                    .ok_or(Error::Unsupported("prime patch object"))?;
                // These are exactly the keys Source merges after refresh_from_db.
                const ALLOWED: [&str; 10] = [
                    "status",
                    "started_at",
                    "file_url",
                    "output_file_url",
                    "file_name",
                    "file_path",
                    "_hls_dir",
                    "program",
                    "poster_logo_id",
                    "poster_url",
                ];
                if patch.keys().any(|key| !ALLOWED.contains(&key.as_str()))
                    || patch.get("status").and_then(Value::as_str) != Some("recording")
                    || [
                        "started_at",
                        "file_url",
                        "output_file_url",
                        "file_name",
                        "file_path",
                        "_hls_dir",
                    ]
                    .iter()
                    .any(|key| patch.get(*key).and_then(Value::as_str).is_none())
                {
                    return Err(Error::Unsupported("Source prime whitelist"));
                }
                for (key, value) in patch {
                    properties.insert(key.clone(), value.clone());
                }
            }
            PropertyWrite::RemovedOwnedHls => {
                if properties.get("status").and_then(Value::as_str) != Some("recording") {
                    return Err(Error::Unsupported("HLS cleanup status changed"));
                }
                if properties.remove("_hls_dir").is_none() {
                    return Ok(before.clone());
                }
            }
            PropertyWrite::CompletedNormal {
                actual_ended,
                bytes_written,
            } => {
                if properties.get("status").and_then(Value::as_str) != Some("recording")
                    || properties.contains_key("_hls_dir")
                    || bytes_written == 0
                {
                    return Err(Error::Unsupported("normal completion scope"));
                }
                properties.insert("status".into(), Value::String("completed".into()));
                let ended = if actual_ended.timestamp_subsec_micros() == 0 {
                    actual_ended.format("%Y-%m-%d %H:%M:%S").to_string()
                } else {
                    format!(
                        "{}.{:06}",
                        actual_ended.format("%Y-%m-%d %H:%M:%S"),
                        actual_ended.timestamp_subsec_micros()
                    )
                };
                properties.insert("ended_at".into(), Value::String(ended));
                properties.insert("bytes_written".into(), Value::from(bytes_written));
                properties.insert("remux_success".into(), Value::Bool(true));
                let url = format!("/api/channels/recordings/{recording}/file/");
                properties.insert("file_url".into(), Value::String(url.clone()));
                properties.insert("output_file_url".into(), Value::String(url));
                properties.remove("interrupted_reason");
            }
        }
        if serde_json::to_vec(&after)
            .map_err(|_| Error::Unsupported("property serialization"))?
            .len()
            > 8192
        {
            return Err(Error::Unsupported("merged property byte bound"));
        }
        Ok(after)
    }

    /// Only the observed normal recording_start/end rows, injected stable empty
    /// Connect/plugin context and nontrimming history. Does not borrow another
    /// family's caught-P0001 authority or turn unknown audit errors into success.
    pub async fn insert_normal_event(
        self: Arc<Self>,
        kind: NormalEvent,
        channel: Uuid,
        channel_name: String,
        details: Value,
        max_events: u64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<i64, Error> {
        if channel_name.len() > 255
            || !details.is_object()
            || serde_json::to_vec(&details)
                .map_err(|_| Error::Unsupported("audit details"))?
                .len()
                > 8192
            || !(2..=100).contains(&max_events)
        {
            return Err(Error::Unsupported("normal event bounds"));
        }
        tokio::spawn(async move {
            let _caller = owner;
            let mut connection = self.pool.acquire().await.map_err(|source| Error::Sql {
                stage: Stage::Acquire,
                source,
                settlement: None,
            })?;
            let result = Self::insert_normal_event_inner(
                &mut connection,
                kind,
                channel,
                channel_name,
                details,
                max_events,
            )
            .await;
            settle(connection, result).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    async fn insert_normal_event_inner(
        connection: &mut PgConnection,
        kind: NormalEvent,
        channel: Uuid,
        channel_name: String,
        details: Value,
        max_events: u64,
    ) -> Result<i64, Error> {
        // Recheck actual context at the effect boundary. Counts bound neither
        // table scanning nor concurrent DB CPU; stable-context parity only.
        let context:(bool,bool,bool,bool,i64)=sqlx::query_as("SELECT NOT EXISTS(SELECT 1 FROM dispatcharr_connect_integration),NOT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription),NOT EXISTS(SELECT 1 FROM dispatcharr_connect_deliverylog),NOT EXISTS(SELECT 1 FROM plugins_pluginconfig),(SELECT count(*)::bigint FROM core_systemevent)")
            .fetch_one(&mut *connection).await.map_err(|source|Error::Sql {stage:Stage::Audit,source,settlement:None})?;
        if !(context.0 && context.1 && context.2 && context.3)
            || context.4 < 0
            || context.4 as u64 >= max_events
        {
            return Err(Error::Unsupported(
                "audit context changed/nontrimming scope",
            ));
        }
        let event_type = match kind {
            NormalEvent::Started => "recording_start",
            NormalEvent::Ended => "recording_end",
        };
        sqlx::query_scalar("INSERT INTO core_systemevent (event_type,timestamp,channel_id,channel_name,details) VALUES ($1,$2,$3,$4,$5) RETURNING id::bigint")
            .bind(event_type).bind(Utc::now()).bind(channel).bind(channel_name).bind(details)
            .fetch_one(&mut *connection).await.map_err(|source|Error::Sql {stage:Stage::Audit,source,settlement:None})
    }
}

pub enum NormalEvent {
    Started,
    Ended,
}
