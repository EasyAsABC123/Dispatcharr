//! Inert normal DVR snapshot groundwork, derived from actual Source3.
//! Original Dispatcharr authors retain application/algorithm/license credit.
//! PRIVATE UNCOMPILED: no registration, ACK, lease, media or terminal result.
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{pool::PoolConnection, Connection, PgConnection, PgPool, Postgres, Row};
use std::sync::Arc;
use uuid::Uuid;
#[path = "recording_connect_events.rs"]
mod connect;
#[path = "recording_event_enrichment.rs"]
pub(crate) mod enrichment;

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
    Scoped(Box<crate::StoreError>),
    Connect(Box<crate::epg_connect::Error>),
}
/// A prime-write rejection may be consumed only after this module awaited
/// the existing same-connection Ready/ping AND explicit physical close.
#[derive(Debug)]
pub struct SettledPrimeRejection {
    source: sqlx::Error,
}
impl SettledPrimeRejection {
    pub fn code(&self) -> &'static str {
        "P0001"
    }
    pub fn diagnostic(&self) -> &sqlx::Error {
        &self.source
    }
}
#[derive(Debug)]
pub enum PrimeWriteOutcome {
    Applied(Value),
    Rejected(SettledPrimeRejection),
}
#[derive(Clone, Debug)]
pub struct RecordingIdentity {
    pub recording: i64,
    pub channel: i64,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}
// Data-only rejection policy; real Ready/close authority comes exclusively
// from prime_properties awaiting the existing supervised write_properties.
fn prime_rejection_scope(stage: Stage, code: Option<&str>, settled: bool) -> bool {
    settled && stage == Stage::MutationWrite && code == Some("P0001")
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
/// Actual normal row context. Optional artwork/profile/EPG remains actual data;
/// no captured poster/filepath or absence of an input profile is fabricated.
#[derive(Clone, Debug)]
pub struct NormalCaptureSnapshot {
    pub identity: RecordingIdentity,
    pub channel_uuid: Uuid,
    pub channel_name: String,
    pub channel_epg_data_id: Option<i64>,
    pub channel_stream_profile_id: Option<i64>,
    pub channel_has_override: bool,
    pub periodic_name: Option<String>,
    pub properties: Value,
    pub programme_custom_properties: Option<Value>,
    pub actual_read_utc: DateTime<Utc>,
}
#[derive(Clone, Debug)]
pub struct NormalRecordingState {
    pub stopped: bool,
    pub property_object: bool,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}
#[derive(Clone, Debug)]
pub struct RuntimeFinalize {
    pub ended: DateTime<Utc>,
    pub bytes_written: u64,
    pub remux_success: bool,
    pub interrupted_reason: Option<String>,
    pub server_shutdown: bool,
    pub stream_info: Option<Value>,
}
#[derive(Clone, Debug)]
pub enum RuntimeFinalizeOutcome {
    Missing,
    Applied(Value),
    Caught {
        intended: Value,
        code: Option<String>,
    },
}
pub struct CaptureStore {
    pool: PgPool,
    identity: Arc<()>,
    owned_logo_path: String,
    recording_connect: std::sync::OnceLock<connect::Ports>,
}

/// Data-only Source whitelist; does not certify media work or permit a task ACK.
pub enum PropertyWrite {
    Prime(Value),
    PrimeRuntime(Value),
    PermanentRuntimeUrls,
    RemovedOwnedHls,
    RemovedRuntimeHls,
    /// Source's pre-rmtree URL update when the caught prime write left scheduled CP.
    PermanentUrlsAfterRejectedPrime,
    CompletedAfterRejectedPrime {
        actual_ended: DateTime<Utc>,
        bytes_written: u64,
    },
    CompletedNormal {
        actual_ended: DateTime<Utc>,
        bytes_written: u64,
    },
}

impl PropertyWrite {
    fn normalizes_runtime_properties(&self) -> bool {
        matches!(
            self,
            Self::PrimeRuntime(_) | Self::PermanentRuntimeUrls | Self::RemovedRuntimeHls
        )
    }
}

// Keep the original SQL NULL distinct from JSON null for exact compare-and-set.
const BOUNDED_PROPERTY_ROW: &str = "SELECT CASE WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,custom_properties IS NULL OR octet_length(custom_properties::text)<=8192 FROM dispatcharr_channels_recording WHERE id=$1 LIMIT 1";

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
    pub fn normal(pool: PgPool) -> Self {
        Self {
            pool,
            identity: Arc::new(()),
            owned_logo_path: String::new(),
            recording_connect: std::sync::OnceLock::new(),
        }
    }
    pub fn source3_local(pool: PgPool) -> Self {
        Self {
            pool,
            identity: Arc::new(()),
            owned_logo_path: "/app/media/rust-owned-clocked/logo.png".into(),
            recording_connect: std::sync::OnceLock::new(),
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
    /// Real normal row read and idempotency guard. None is the Source no-op for
    /// a missing row or an already recording/completed/stopped status. This
    /// method alone grants no queue claim, process or output-path authority.
    pub async fn normal_snapshot(
        self: Arc<Self>,
        recording_id: i64,
        argument_channel: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<NormalCaptureSnapshot>, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&recording_id)
            || !(1..=i64::from(i32::MAX)).contains(&argument_channel)
        {
            return Err(Error::Unsupported("normal recording identity"));
        }
        tokio::spawn(async move {
            let _owner = owner;
            let mut connection = self.pool.acquire().await.map_err(|source| Error::Sql {
                stage: Stage::Acquire,
                source,
                settlement: None,
            })?;
            let result = Self::read_normal_inner(
                &mut connection,
                recording_id,
                argument_channel,
                self.recording_connect_bound(),
            )
            .await;
            settle(connection, result).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    async fn read_normal_inner(
        connection: &mut PgConnection,
        recording_id: i64,
        argument_channel: i64,
        connect_bound: bool,
    ) -> Result<Option<NormalCaptureSnapshot>, Error> {
        let row=sqlx::query("SELECT r.id::bigint AS recording,r.channel_id::bigint AS channel,r.start_time,r.end_time,CASE WHEN octet_length(r.task_id)<=255 THEN r.task_id ELSE NULL END AS task_id,CASE WHEN r.custom_properties IS NULL THEN '{}'::jsonb WHEN octet_length(r.custom_properties::text)<=8192 THEN r.custom_properties ELSE NULL END AS properties,c.uuid AS channel_uuid,CASE WHEN octet_length(c.name)<=255 THEN c.name ELSE NULL END AS channel_name,c.epg_data_id::bigint AS epg_data_id,c.stream_profile_id::bigint AS stream_profile_id,EXISTS(SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.channel_id=c.id) AS has_override,c.id IS NOT NULL AS argument_channel_exists FROM dispatcharr_channels_recording r LEFT JOIN dispatcharr_channels_channel c ON c.id=$2 WHERE r.id=$1 LIMIT 1")
            .bind(recording_id).bind(argument_channel).fetch_optional(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::Recording,source,settlement:None})?;
        let Some(row) = row else {
            return Ok(None);
        };
        let decode = |source| Error::Sql {
            stage: Stage::Recording,
            source,
            settlement: None,
        };
        let properties: Option<Value> = row.try_get("properties").map_err(decode)?;
        let properties =
            properties.ok_or(Error::Unsupported("normal property transfer capacity"))?;
        let properties = normal_object(&properties)
            .map_err(|_| Error::Unsupported("normal object properties"))?;
        if matches!(
            properties.get("status").and_then(Value::as_str),
            Some("recording" | "completed" | "stopped")
        ) {
            return Ok(None);
        }
        if !row
            .try_get::<bool, _>("argument_channel_exists")
            .map_err(decode)?
        {
            return Err(Error::Unsupported(
                "missing argument Channel requires actual Source task-failure port",
            ));
        }
        let supported:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM plugins_pluginconfig) AND CASE WHEN $1 THEN NOT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event IN('recording_start','recording_end') AND i.type='script') ELSE NOT EXISTS(SELECT 1 FROM dispatcharr_connect_integration) AND NOT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription) AND NOT EXISTS(SELECT 1 FROM dispatcharr_connect_deliverylog) END").bind(connect_bound).fetch_one(&mut*connection).await.map_err(|source|Error::Sql{stage:Stage::Context,source,settlement:None})?;
        if !supported {
            return Err(Error::Unsupported(
                "normal system-event integration ports required",
            ));
        }
        let recording: i64 = row.try_get("recording").map_err(decode)?;
        let channel: i64 = row.try_get("channel").map_err(decode)?;
        let start: DateTime<Utc> = row.try_get("start_time").map_err(decode)?;
        let end: DateTime<Utc> = row.try_get("end_time").map_err(decode)?;
        // Original accepts surviving published arguments after a booking update.
        // Row identity/window is a NEW actual observation, never overwritten by
        // the producer registry's historical publication/adoption snapshot.
        if recording != recording_id {
            return Err(Error::Unsupported("normal Recording identity changed"));
        }
        let program = properties.get("program");
        let program_id = program.and_then(|p| p.get("id"));
        let parse_id = |value: &Value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
        };
        let id = program_id
            .filter(|v| {
                !v.is_null() && *v != &serde_json::json!(false) && *v != &serde_json::json!(0)
            })
            .map(|v| parse_id(v).ok_or(Error::Unsupported("normal programme identity scalar")))
            .transpose()?;
        let mut selected: Option<(Option<Value>, bool)> = if let Some(id) = id {
            sqlx::query_as("SELECT CASE WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,custom_properties IS NULL OR octet_length(custom_properties::text)<=8192 FROM epg_programdata WHERE id=$1 LIMIT 1").bind(id).fetch_optional(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::Recording,source,settlement:None})?
        } else {
            None
        };
        if selected.is_none() {
            let parse_time = |value: &Value| -> Option<DateTime<Utc>> {
                let text = value.as_str()?;
                let normalized = text.replacen(' ', "T", 1);
                DateTime::parse_from_rfc3339(&normalized)
                    .ok()
                    .map(|v| v.with_timezone(&Utc))
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%dT%H:%M:%S%.f")
                            .ok()
                            .map(|v| v.and_utc())
                    })
            };
            if let (Some(tvg), Some(start), Some(end)) = (
                program
                    .and_then(|v| v.get("tvg_id"))
                    .and_then(Value::as_str),
                program
                    .and_then(|v| v.get("start_time"))
                    .and_then(parse_time),
                program.and_then(|v| v.get("end_time")).and_then(parse_time),
            ) {
                selected=sqlx::query_as("SELECT CASE WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,custom_properties IS NULL OR octet_length(custom_properties::text)<=8192 FROM epg_programdata WHERE tvg_id=$1 AND start_time=$2 AND end_time=$3 ORDER BY id ASC LIMIT 1").bind(tvg).bind(start).bind(end).fetch_optional(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::Recording,source,settlement:None})?;
            }
        }
        let programme_custom_properties = match selected {
            Some((_, false)) => {
                return Err(Error::Unsupported(
                    "normal programme property transfer capacity",
                ))
            }
            Some((value, true)) => value,
            None => None,
        };
        let name: Option<String> = row.try_get("channel_name").map_err(decode)?;
        Ok(Some(NormalCaptureSnapshot {
            identity: RecordingIdentity {
                recording,
                channel,
                start,
                end,
            },
            channel_uuid: row.try_get("channel_uuid").map_err(decode)?,
            channel_name: name
                .ok_or(Error::Unsupported("normal channel name transfer capacity"))?,
            channel_epg_data_id: row.try_get("epg_data_id").map_err(decode)?,
            channel_stream_profile_id: row.try_get("stream_profile_id").map_err(decode)?,
            channel_has_override: row.try_get("has_override").map_err(decode)?,
            periodic_name: row.try_get("task_id").map_err(decode)?,
            properties,
            programme_custom_properties,
            actual_read_utc: Utc::now(),
        }))
    }
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

    /// Additive, prime-only outcome API. No raw inner error reaches this
    /// classification: write_properties returns only after settle ping+close.
    pub async fn prime_properties(
        self: Arc<Self>,
        recording: i64,
        patch: Value,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<PrimeWriteOutcome, Error> {
        match self
            .write_properties(recording, PropertyWrite::Prime(patch), owner)
            .await
        {
            Ok(value) => Ok(PrimeWriteOutcome::Applied(value)),
            Err(Error::Sql {
                stage: Stage::MutationWrite,
                source,
                settlement: None,
            }) if prime_rejection_scope(
                Stage::MutationWrite,
                source.as_database_error().and_then(|e| e.code()).as_deref(),
                true,
            ) =>
            {
                Ok(PrimeWriteOutcome::Rejected(SettledPrimeRejection {
                    source,
                }))
            }
            Err(error) => Err(error),
        }
    }

    /// Actual effective StreamProfile.is_redirect (locked + name), rather
    /// than rejecting all noncaptured profile ids or silently choosing DVR UA.
    pub async fn normal_requires_provider_ua(
        self: Arc<Self>,
        channel: i64,
        default_profile: Option<i64>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        tokio::spawn(async move{
            let _owner=owner;let mut connection=self.pool.acquire().await.map_err(|source|Error::Sql{stage:Stage::Acquire,source,settlement:None})?;
            let result=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id JOIN core_streamprofile p ON p.id=COALESCE(o.stream_profile_id,c.stream_profile_id,$2) WHERE c.id=$1 AND p.locked AND p.name='Redirect')")
                .bind(channel).bind(default_profile).fetch_one(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::Recording,source,settlement:None});
            settle(connection,result).await
        }).await.map_err(|_|Error::Supervisor)?
    }
    /// Source's two-second stop/deletion/end-extension observation transfers
    /// only derived booleans/timestamps, never the growing recording JSON.
    pub async fn normal_state(
        self: Arc<Self>,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<NormalRecordingState>, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&recording) {
            return Err(Error::Unsupported("normal state identity"));
        }
        tokio::spawn(async move{
            let _owner=owner;let mut connection=self.pool.acquire().await.map_err(|source|Error::Sql{stage:Stage::Acquire,source,settlement:None})?;
            let result=async{
                let row:Option<(bool,bool,DateTime<Utc>,DateTime<Utc>)>=sqlx::query_as("SELECT COALESCE(custom_properties->>'status'='stopped',false),COALESCE(jsonb_typeof(custom_properties)='object' OR custom_properties IN('null'::jsonb,'false'::jsonb,'0'::jsonb,'\"\"'::jsonb,'[]'::jsonb),true),start_time,end_time FROM dispatcharr_channels_recording WHERE id=$1 LIMIT 1").bind(recording).fetch_optional(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::Recording,source,settlement:None})?;
                Ok(row.map(|(stopped,property_object,start,end)|NormalRecordingState{stopped,property_object,start,end}))
            }.await;settle(connection,result).await
        }).await.map_err(|_|Error::Supervisor)?
    }
    /// Fresh final status is always authoritative (stopped wins). Shutdown
    /// preserves all HLS/file URLs and marks interrupted before returning.
    pub async fn finalize_runtime(
        self: Arc<Self>,
        recording: i64,
        finalize: RuntimeFinalize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RuntimeFinalizeOutcome, Error> {
        self.finalize_runtime_retry(recording, finalize, false, owner)
            .await
    }
    /// Startup expired rows preserve previous ended_at/bytes_written. Source
    /// writes only interrupted/reason/remux, plus URLs after actual remux success.
    pub async fn finalize_expired_runtime(
        self: Arc<Self>,
        recording: i64,
        remux_success: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RuntimeFinalizeOutcome, Error> {
        self.finalize_runtime_retry(
            recording,
            RuntimeFinalize {
                ended: Utc::now(),
                bytes_written: 0,
                remux_success,
                interrupted_reason: None,
                server_shutdown: false,
                stream_info: None,
            },
            true,
            owner,
        )
        .await
    }
    async fn finalize_runtime_retry(
        self: Arc<Self>,
        recording: i64,
        finalize: RuntimeFinalize,
        expired: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RuntimeFinalizeOutcome, Error> {
        for attempt in 0..3u32 {
            let outcome = self
                .clone()
                .finalize_runtime_mode(recording, finalize.clone(), expired, owner.clone())
                .await?;
            let transient = matches!(&outcome,RuntimeFinalizeOutcome::Caught{code:Some(code),..} if normal_transient_sqlstate(code));
            if !transient || attempt == 2 {
                return Ok(outcome);
            }
            // Retry only a real SQL ErrorResponse after Ready+close. A lost
            // write/commit reply, supervisor loss or unknown fate never retries.
            tokio::time::sleep(std::time::Duration::from_secs(1u64 << attempt)).await;
        }
        unreachable!("finite Source DB retry loop returns final outcome")
    }
    async fn finalize_runtime_mode(
        self: Arc<Self>,
        recording: i64,
        finalize: RuntimeFinalize,
        expired: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RuntimeFinalizeOutcome, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&recording)
            || finalize
                .interrupted_reason
                .as_ref()
                .is_some_and(|s| s.len() > 4096)
        {
            return Err(Error::Unsupported("normal finalization capacity"));
        }
        tokio::spawn(async move{
            let _owner=owner;let mut connection=self.pool.acquire().await.map_err(|source|Error::Sql{stage:Stage::Acquire,source,settlement:None})?;
            let result=async{
                let raw:Option<(Option<Value>,bool)>=sqlx::query_as(BOUNDED_PROPERTY_ROW).bind(recording).fetch_optional(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::MutationRead,source,settlement:None})?;
                let before=match raw {None=>return Ok(RuntimeFinalizeOutcome::Missing),Some((before,true))=>before,_=>return Err(Error::Unsupported("normal final property capacity"))};
                let mut after=normal_object(before.as_ref().unwrap_or(&Value::Null))?;let properties=after.as_object_mut().ok_or(Error::Unsupported("normal fresh object properties"))?;
                let ended=if finalize.ended.timestamp_subsec_micros()==0{finalize.ended.format("%Y-%m-%d %H:%M:%S").to_string()}else{format!("{}.{:06}",finalize.ended.format("%Y-%m-%d %H:%M:%S"),finalize.ended.timestamp_subsec_micros())};
                if !expired {properties.insert("ended_at".into(),Value::String(ended));}
                if expired {
                    properties.insert("status".into(),Value::String("interrupted".into()));
                    properties.insert("interrupted_reason".into(),Value::String("server_restarted_after_end".into()));
                    properties.insert("remux_success".into(),Value::Bool(finalize.remux_success));
                    if finalize.remux_success {
                        properties.remove("_hls_dir");
                        let url=format!("/api/channels/recordings/{recording}/file/");
                        properties.insert("file_url".into(),Value::String(url.clone()));
                        properties.insert("output_file_url".into(),Value::String(url));
                    }
                }else if finalize.server_shutdown{
                    properties.insert("status".into(),Value::String("interrupted".into()));properties.insert("interrupted_reason".into(),Value::String("server_shutdown".into()));
                }else{
                    if properties.get("status").and_then(Value::as_str)==Some("stopped"){properties.remove("interrupted_reason");}
                    else if let Some(reason)=finalize.interrupted_reason {properties.insert("status".into(),Value::String("interrupted".into()));properties.insert("interrupted_reason".into(),Value::String(reason));}
                    else{properties.insert("status".into(),Value::String("completed".into()));properties.remove("interrupted_reason");}
                    if let Some(stream_info)=finalize.stream_info{properties.insert("stream_info".into(),stream_info);}
                    let url=format!("/api/channels/recordings/{recording}/file/");properties.insert("file_url".into(),Value::String(url.clone()));properties.insert("output_file_url".into(),Value::String(url));properties.insert("bytes_written".into(),Value::from(finalize.bytes_written));properties.insert("remux_success".into(),Value::Bool(finalize.remux_success));
                }
                if serde_json::to_vec(&after).map_err(|_|Error::Unsupported("normal final serialization"))?.len()>8192{return Err(Error::Unsupported("normal merged final capacity"));}
                let changed=sqlx::query("UPDATE dispatcharr_channels_recording SET custom_properties=$2 WHERE id=$1 AND custom_properties IS NOT DISTINCT FROM $3").bind(recording).bind(&after).bind(&before).execute(&mut *connection).await;
                match changed{Ok(value)if value.rows_affected()==1=>Ok(RuntimeFinalizeOutcome::Applied(after)),Ok(_)=>Err(Error::Unsupported("normal final row changed")),Err(source)=>Ok(RuntimeFinalizeOutcome::Caught{intended:after,code:source.as_database_error().and_then(|e|e.code()).map(|v|v.into_owned())})}
            }.await;
            // Caught SQL outcomes also cross actual Ready/close before their
            // intended in-memory payload can reach Source's best-effort event.
            settle(connection,result).await
        }).await.map_err(|_|Error::Supervisor)?
    }
    pub async fn publish_normal_permanent_urls(
        self: Arc<Self>,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.runtime_hls_patch(recording, false, owner).await
    }
    pub async fn clear_runtime_hls(
        self: Arc<Self>,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.runtime_hls_patch(recording, true, owner).await
    }
    async fn runtime_hls_patch(
        self: Arc<Self>,
        recording: i64,
        clear: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if recording <= 0 || recording > i64::from(i32::MAX) {
            return Err(Error::Unsupported("normal HLS SQL identity"));
        }
        tokio::spawn(async move{
            let _owner=owner;let mut connection=self.pool.acquire().await.map_err(|source|Error::Sql{stage:Stage::Acquire,source,settlement:None})?;
            let query=if clear {"UPDATE dispatcharr_channels_recording SET custom_properties=COALESCE(custom_properties,'{}'::jsonb)-'_hls_dir' WHERE id=$1"}
                else {"UPDATE dispatcharr_channels_recording SET custom_properties=COALESCE(custom_properties,'{}'::jsonb)||jsonb_build_object('file_url','/api/channels/recordings/'||id||'/file/','output_file_url','/api/channels/recordings/'||id||'/file/') WHERE id=$1"};
            let result=sqlx::query(query).bind(recording).execute(&mut *connection).await.map(|r|r.rows_affected()==1).map_err(|source|Error::Sql{stage:Stage::MutationWrite,source,settlement:None});
            settle(connection,result).await
        }).await.map_err(|_|Error::Supervisor)?
    }
    pub async fn prime_normal_properties(
        self: Arc<Self>,
        recording: i64,
        patch: Value,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<PrimeWriteOutcome, Error> {
        match self
            .write_properties(recording, PropertyWrite::PrimeRuntime(patch), owner)
            .await
        {
            Ok(value) => Ok(PrimeWriteOutcome::Applied(value)),
            Err(Error::Sql {
                stage: Stage::MutationWrite,
                source,
                settlement: None,
            }) if prime_rejection_scope(
                Stage::MutationWrite,
                source.as_database_error().and_then(|e| e.code()).as_deref(),
                true,
            ) =>
            {
                Ok(PrimeWriteOutcome::Rejected(SettledPrimeRejection {
                    source,
                }))
            }
            Err(error) => Err(error),
        }
    }
    async fn write_properties_inner(
        connection: &mut PgConnection,
        recording: i64,
        write: PropertyWrite,
    ) -> Result<Value, Error> {
        let row: Option<(Option<Value>, bool)> = sqlx::query_as(BOUNDED_PROPERTY_ROW)
            .bind(recording)
            .fetch_optional(&mut *connection)
            .await
            .map_err(|source| Error::Sql {
                stage: Stage::MutationRead,
                source,
                settlement: None,
            })?;
        let stored = match row {
            Some((value, true))
                if write.normalizes_runtime_properties()
                    || value.as_ref().is_some_and(Value::is_object) =>
            {
                value
            }
            _ => {
                return Err(Error::Unsupported(
                    "fresh bounded existing recording properties",
                ))
            }
        };
        let before = stored.as_ref().unwrap_or(&Value::Null);
        let after = Self::merge_properties(recording, before, write)?;
        if &after == before {
            return Ok(after);
        }
        // Stable normal context is Source-authorized. An observed concurrent
        // change is operational, never silently overwritten/retried as normal.
        let affected=sqlx::query("UPDATE dispatcharr_channels_recording SET custom_properties=$2 WHERE id=$1 AND custom_properties IS NOT DISTINCT FROM $3")
            .bind(recording).bind(&after).bind(&stored).execute(&mut *connection).await
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
        let rejected_completion =
            matches!(&write, PropertyWrite::CompletedAfterRejectedPrime { .. });
        let mut after = if write.normalizes_runtime_properties() {
            normal_object(before)?
        } else {
            before.clone()
        };
        let properties = after
            .as_object_mut()
            .ok_or(Error::Unsupported("fresh object"))?;
        let runtime_prime = matches!(&write, PropertyWrite::PrimeRuntime(_));
        match write {
            PropertyWrite::Prime(patch) | PropertyWrite::PrimeRuntime(patch) => {
                if (!runtime_prime
                    && properties.get("status").and_then(Value::as_str) != Some("scheduled"))
                    || (runtime_prime
                        && matches!(
                            properties.get("status").and_then(Value::as_str),
                            Some("recording" | "completed" | "stopped")
                        ))
                {
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
            PropertyWrite::PermanentRuntimeUrls => {
                let url = format!("/api/channels/recordings/{recording}/file/");
                properties.insert("file_url".into(), Value::String(url.clone()));
                properties.insert("output_file_url".into(), Value::String(url));
            }
            PropertyWrite::RemovedRuntimeHls => {
                properties.remove("_hls_dir");
            }
            PropertyWrite::RemovedOwnedHls => {
                if properties.get("status").and_then(Value::as_str) != Some("recording") {
                    return Err(Error::Unsupported("HLS cleanup status changed"));
                }
                if properties.remove("_hls_dir").is_none() {
                    return Ok(before.clone());
                }
            }
            PropertyWrite::PermanentUrlsAfterRejectedPrime => {
                if properties.get("status").and_then(Value::as_str) != Some("scheduled")
                    || properties.contains_key("_hls_dir")
                {
                    return Err(Error::Unsupported("rejected prime URL scope"));
                }
                let url = format!("/api/channels/recordings/{recording}/file/");
                properties.insert("file_url".into(), Value::String(url.clone()));
                properties.insert("output_file_url".into(), Value::String(url));
            }
            PropertyWrite::CompletedNormal {
                actual_ended,
                bytes_written,
            }
            | PropertyWrite::CompletedAfterRejectedPrime {
                actual_ended,
                bytes_written,
            } => {
                if properties.get("status").and_then(Value::as_str)
                    != Some(if rejected_completion {
                        "scheduled"
                    } else {
                        "recording"
                    })
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
    /// Source's normal best-effort audit insertion/history trimming, physically
    /// settled before returning. Hook integrations are checked in normal row
    /// preflight until their real ports are composed, never silently discarded.
    pub async fn insert_runtime_event(
        self: Arc<Self>,
        kind: NormalEvent,
        channel: Uuid,
        channel_name: String,
        details: Value,
        max_events: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<i64>, Error> {
        if channel_name.len() > 255
            || !details.is_object()
            || serde_json::to_vec(&details)
                .map_err(|_| Error::Unsupported("audit details"))?
                .len()
                > 8192
        {
            return Err(Error::Unsupported("normal event capacity"));
        }
        let result=tokio::spawn(async move {
            let _owner=owner;let mut connection=self.pool.acquire().await.map_err(|source|Error::Sql{stage:Stage::Acquire,source,settlement:None})?;
            let result=async {
                // Legacy/cancellation/recovery callers have no claimed hook context.
                Self::read_unbound_event_domain(&mut connection,kind).await?;
                let kind=kind.as_str();
                let id:i64=sqlx::query_scalar("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,$2,$3,$4,$5)RETURNING id::bigint").bind(kind).bind(Utc::now()).bind(channel).bind(channel_name).bind(details).fetch_one(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::Audit,source,settlement:None})?;
                // An nth row exists iff count>max. This avoids Source's full
                // COUNT scan while preserving its exact cutoff/id predicate.
                // Negative queryset indices throw inside Source's caught logger;
                // insertion remains, no trim occurs.
                if max_events>=0 {
                    let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(max_events).fetch_optional(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::Audit,source,settlement:None})?;
                    if let Some(cutoff)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(&mut *connection).await.map_err(|source|Error::Sql{stage:Stage::Audit,source,settlement:None})?;}
                }Ok(id)
            }.await;settle(connection,result).await
        }).await.map_err(|_|Error::Supervisor)?;
        match result {
            Ok(id) => Ok(Some(id)),
            Err(Error::Sql {
                stage: Stage::Audit,
                settlement: None,
                ..
            }) => Ok(None),
            Err(error) => Err(error),
        }
    }
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

#[derive(Clone, Copy)]
pub enum NormalEvent {
    Started,
    Ended,
}
impl NormalEvent {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Started => "recording_start",
            Self::Ended => "recording_end",
        }
    }
}

fn normal_object(value: &Value) -> Result<Value, Error> {
    let falsy = match value {
        Value::Null => true,
        Value::Bool(v) => !*v,
        Value::Number(n) => n.as_f64() == Some(0.0),
        Value::String(s) => s.is_empty(),
        Value::Array(v) => v.is_empty(),
        Value::Object(v) => v.is_empty(),
    };
    if falsy {
        Ok(serde_json::json!({}))
    } else if value.is_object() {
        Ok(value.clone())
    } else {
        Err(Error::Unsupported("truthy nonobject recording properties"))
    }
}

fn normal_transient_sqlstate(code: &str) -> bool {
    // psycopg2 operational transaction/resource/connection failures. Source
    // Integrity/Data/Programming/RaiseException (P0001) are never retried here.
    ["08", "40", "53", "54", "55", "57", "58"]
        .iter()
        .any(|class| code.starts_with(class))
}

#[cfg(test)]
mod prime_rejected_source_controls {
    use super::*;
    fn source() -> Value {
        serde_json::from_str(include_str!("../datasets/dvr-prime-rejected-source.json"))
            .expect("Source fixture JSON")
    }
    #[test]
    fn actual_source_rejected_prime_does_not_backfill_unsaved_fields() {
        let data = source();
        let before = &data["before_properties"];
        let permanent = CaptureStore::merge_properties(
            1,
            before,
            PropertyWrite::PermanentUrlsAfterRejectedPrime,
        )
        .expect("Source pre-rmtree URL merge");
        assert_eq!(permanent["status"], "scheduled");
        let expected = &data["after_properties"];
        let ended = chrono::NaiveDateTime::parse_from_str(
            expected["ended_at"].as_str().expect("Source ended string"),
            "%Y-%m-%d %H:%M:%S%.f",
        )
        .expect("Source observed UTC")
        .and_utc();
        let merged = CaptureStore::merge_properties(
            1,
            &permanent,
            PropertyWrite::CompletedAfterRejectedPrime {
                actual_ended: ended,
                bytes_written: expected["bytes_written"]
                    .as_u64()
                    .expect("Source byte count"),
            },
        )
        .expect("Source completion merge");
        assert_eq!(&merged, expected);
        for key in ["started_at", "file_name", "file_path", "_hls_dir"] {
            assert!(merged.get(key).is_none());
        }
        assert!(data["actual_end_audit"]["file_path"].is_string());
        assert!(data["actual_end_audit"]["file_name"].is_string());
        assert_eq!(data["actual_recording_result"]["status"], "SUCCESS");
        assert!(data["actual_recording_result"]["result"].is_null());
    }
    #[test]
    fn only_measured_prime_stage_code_after_successful_settlement_is_in_scope() {
        assert!(prime_rejection_scope(
            Stage::MutationWrite,
            Some("P0001"),
            true
        ));
        for stage in [
            Stage::Acquire,
            Stage::Context,
            Stage::Recording,
            Stage::Poster,
            Stage::MutationRead,
            Stage::Audit,
            Stage::Ready,
            Stage::Close,
        ] {
            assert!(!prime_rejection_scope(stage, Some("P0001"), true));
        }
        for code in [None, Some("23505"), Some("57014"), Some("08006")] {
            assert!(!prime_rejection_scope(Stage::MutationWrite, code, true));
        }
        assert!(!prime_rejection_scope(
            Stage::MutationWrite,
            Some("P0001"),
            false
        ));
    }
    #[test]
    fn normal_completion_scope_is_not_broadened_to_scheduled_rows() {
        let data = source();
        assert!(CaptureStore::merge_properties(
            1,
            &data["before_properties"],
            PropertyWrite::CompletedNormal {
                actual_ended: Utc::now(),
                bytes_written: 1
            }
        )
        .is_err());
        let mut changed = data["before_properties"].clone();
        changed["status"] = serde_json::json!("recording");
        assert!(CaptureStore::merge_properties(
            1,
            &changed,
            PropertyWrite::PermanentUrlsAfterRejectedPrime
        )
        .is_err());
        changed["status"] = serde_json::json!("scheduled");
        changed["_hls_dir"] = serde_json::json!("foreign");
        assert!(CaptureStore::merge_properties(
            1,
            &changed,
            PropertyWrite::PermanentUrlsAfterRejectedPrime
        )
        .is_err());
    }
}
