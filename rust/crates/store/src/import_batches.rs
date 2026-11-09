//! Unwired M3U batch sink derived from original process_m3u_batch_direct.
//! Original Dispatcharr authors retain full application and algorithm credit.
use crate::{Store, StoreError};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, Row};
use std::{
    collections::{HashMap, HashSet},
    io,
};

pub const MAX_IMPORT_BATCH_ROWS: usize = 2000;
pub const MAX_IMPORT_BATCH_BYTES: usize = 8 * 1024 * 1024;

/// Already-projected import row. The caller owns source filtering, character
/// truncation, provider-number parsing, enabled groups and stream hashing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImportedStream {
    pub stream_hash: String,
    pub name: String,
    pub url: String,
    pub logo_url: String,
    pub tvg_id: String,
    pub channel_group_id: i64,
    pub custom_properties: Value,
    pub is_adult: bool,
    pub stream_id: Option<i64>,
    pub stream_chno: Option<f64>,
    pub is_catchup: bool,
    pub catchup_days: i64,
    pub is_radio: bool,
}
impl From<dispatcharr_ingest_core::ProjectedStream> for ImportedStream {
    fn from(row: dispatcharr_ingest_core::ProjectedStream) -> Self {
        Self {
            stream_hash: row.stream_hash,
            name: row.name,
            url: row.url,
            logo_url: row.logo_url,
            tvg_id: row.tvg_id,
            channel_group_id: row.channel_group_id,
            custom_properties: row.custom_properties,
            is_adult: row.is_adult,
            stream_id: row.stream_id,
            stream_chno: row.stream_chno,
            is_catchup: row.is_catchup,
            catchup_days: row.catchup_days,
            is_radio: row.is_radio,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ImportCounts {
    pub created: usize,
    pub updated: usize,
    pub unchanged: usize,
}
impl ImportCounts {
    pub fn source_message(self, account_id: i64) -> String {
        format!(
            "M3U account: {account_id}, Batch processed: {} created, {} updated, {} unchanged.",
            self.created, self.updated, self.unchanged
        )
    }
}
struct Budget {
    remaining: usize,
}
impl io::Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("Import batch byte capacity"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn invalid() -> StoreError {
    StoreError::InvalidInput(json!({"detail":"Import batch capacity or identity is invalid."}))
}

fn changed(old: &PgRow, new: &ImportedStream) -> Result<bool, sqlx::Error> {
    Ok(old.try_get::<String, _>("name")? != new.name
        || old.try_get::<Option<String>, _>("url")?.as_deref() != Some(new.url.as_str())
        || old.try_get::<Option<String>, _>("logo_url")?.as_deref() != Some(new.logo_url.as_str())
        || old.try_get::<Option<String>, _>("tvg_id")?.as_deref() != Some(new.tvg_id.as_str())
        || !crate::json_compat::python_equal(
            old.try_get::<Option<Value>, _>("custom_properties")?
                .as_ref()
                .unwrap_or(&Value::Null),
            &new.custom_properties,
        )
        || old.try_get::<bool, _>("is_adult")? != new.is_adult
        || old.try_get::<Option<i32>, _>("stream_id")?.map(i64::from) != new.stream_id
        || old.try_get::<Option<f64>, _>("stream_chno")? != new.stream_chno
        || old.try_get::<Option<i64>, _>("channel_group_id")? != Some(new.channel_group_id)
        || old.try_get::<bool, _>("is_catchup")? != new.is_catchup
        || i64::from(old.try_get::<i32, _>("catchup_days")?) != new.catchup_days
        || old.try_get::<bool, _>("is_radio")? != new.is_radio)
}
fn record(row: &ImportedStream, account_id: i64) -> Value {
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
    let channel_number = row.stream_chno.map(|value| {
        if value.is_finite() {
            json!(value)
        } else {
            json!(if value.is_nan() {
                "NaN"
            } else if value.is_sign_positive() {
                "Infinity"
            } else {
                "-Infinity"
            })
        }
    });
    json!({"stream_hash":row.stream_hash,"name":row.name,"url":row.url,"logo_url":row.logo_url,
        "tvg_id":row.tvg_id,"m3u_account_id":account_id,"channel_group_id":row.channel_group_id,
        "custom_properties":row.custom_properties,"is_adult":row.is_adult,"stream_id":row.stream_id,
        "stream_chno":channel_number,"is_catchup":row.is_catchup,"catchup_days":row.catchup_days,
        "is_radio":row.is_radio,"current_viewers":0,"is_custom":false,"is_stale":false,
        "local_file":"","stream_profile_id":null,"stream_stats":null,"stream_stats_updated_at":null,
        "updated_at":timestamp,"last_seen":timestamp})
}
const COLUMNS:&str="name,url,m3u_account_id,logo_url,tvg_id,local_file,current_viewers,updated_at,channel_group_id,stream_profile_id,is_custom,stream_hash,last_seen,is_stale,is_adult,custom_properties,stream_id,stream_chno,stream_stats,stream_stats_updated_at,is_catchup,catchup_days,is_radio";
const CHANGED_FIELDS: &[&str] = &[
    "name",
    "url",
    "logo_url",
    "tvg_id",
    "custom_properties",
    "is_adult",
    "last_seen",
    "updated_at",
    "is_stale",
    "stream_id",
    "stream_chno",
    "channel_group_id",
    "is_catchup",
    "catchup_days",
    "is_radio",
];

impl Store {
    /// Bounded transactional sink only; no public refresh route calls this yet.
    /// Preserve first-row hash deduplication and source attempted-create counts,
    /// even when a concurrent unique-hash insertion makes ON CONFLICT skip it.
    pub async fn import_m3u_batch(
        &self,
        account_id: i64,
        rows: Vec<ImportedStream>,
    ) -> Result<ImportCounts, StoreError> {
        if rows.len() > MAX_IMPORT_BATCH_ROWS {
            return Err(invalid());
        }
        let mut budget = Budget {
            remaining: MAX_IMPORT_BATCH_BYTES,
        };
        for row in &rows {
            if row.stream_hash.len() != 64
                || !row.stream_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(invalid());
            }
            serde_json::to_writer(&mut budget, row).map_err(|_| invalid())?;
        }
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE id=$1)")
                .bind(account_id)
                .fetch_one(&self.pool)
                .await?;
        if !exists {
            return Err(StoreError::NotFound);
        }
        let mut seen = HashSet::new();
        let unique = rows
            .into_iter()
            .filter(|row| seen.insert(row.stream_hash.clone()))
            .collect::<Vec<_>>();
        if unique.is_empty() {
            return Ok(ImportCounts::default());
        }
        let hashes = unique
            .iter()
            .map(|row| row.stream_hash.as_str())
            .collect::<Vec<_>>();
        // Deliberately global: the original lookup is not scoped by account.
        let existing=sqlx::query("SELECT id,stream_hash,name,url,logo_url,tvg_id,custom_properties,is_adult,stream_id,stream_chno,channel_group_id,is_catchup,catchup_days,is_radio FROM dispatcharr_channels_stream WHERE stream_hash=ANY($1)")
            .bind(hashes).fetch_all(&self.pool).await?;
        let existing = existing
            .into_iter()
            .map(|row| Ok((row.try_get::<String, _>("stream_hash")?, row)))
            .collect::<Result<HashMap<_, _>, sqlx::Error>>()?;
        let mut creates = Vec::new();
        let mut updates = Vec::new();
        let mut touches = Vec::new();
        for row in &unique {
            let mut data = record(row, account_id);
            if let Some(old) = existing.get(&row.stream_hash) {
                data["id"] = json!(old.try_get::<i64, _>("id")?);
                if changed(old, row)? {
                    updates.push(data);
                } else {
                    touches.push(data);
                }
            } else {
                creates.push(data);
            }
        }
        let counts = ImportCounts {
            created: creates.len(),
            updated: updates.len(),
            unchanged: touches.len(),
        };
        let mut transaction = self.pool.begin().await?;
        if !creates.is_empty() {
            sqlx::query(&format!("INSERT INTO dispatcharr_channels_stream ({COLUMNS}) SELECT {COLUMNS} FROM jsonb_populate_recordset(NULL::dispatcharr_channels_stream,$1) ON CONFLICT DO NOTHING"))
                .bind(json!(creates)).execute(&mut *transaction).await?;
        }
        if !touches.is_empty() {
            sqlx::query("UPDATE dispatcharr_channels_stream AS target SET last_seen=incoming.last_seen,is_stale=false FROM jsonb_populate_recordset(NULL::dispatcharr_channels_stream,$1) AS incoming WHERE target.id=incoming.id")
                .bind(json!(touches)).execute(&mut *transaction).await?;
        }
        if !updates.is_empty() {
            let assignments = CHANGED_FIELDS
                .iter()
                .map(|field| format!("{field}=incoming.{field}"))
                .collect::<Vec<_>>()
                .join(",");
            // Keep existing account ownership/profile/stats/local-file fields;
            // the source changes only its explicit bulk-update field list.
            sqlx::query(&format!("UPDATE dispatcharr_channels_stream AS target SET {assignments} FROM jsonb_populate_recordset(NULL::dispatcharr_channels_stream,$1) AS incoming WHERE target.id=incoming.id"))
                .bind(json!(updates)).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(counts)
    }
}
