//! Original Dispatcharr staged programme swap with owned physical work.
//! Application and algorithm credit belongs to the original authors.
use crate::{EpgHistoryPolicy, Store, StoreError};
use chrono::{DateTime, Duration, Utc};
use dispatcharr_ingest_core::XmltvProgramme;
use futures_util::TryStreamExt;
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_ROWS: usize = 5_000_000;
const MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_BATCH_ROWS: usize = 2500;
const MAX_MAPPED_IDS: usize = 100_000;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GuideSwapCounts {
    pub deleted: u64,
    pub inserted: u64,
    pub orphaned: u64,
}
#[derive(Debug)]
pub struct GuideMapping {
    pub epg_ids: Vec<i64>,
    pub by_tvg_id: HashMap<String, i64>,
}
pub struct GuideStaging {
    connection: PgConnection,
    _admission: OwnedSemaphorePermit,
    _retained: Arc<dyn Send + Sync>,
    source_id: i64,
    rows: usize,
    bytes: usize,
    backend_pid: i32,
}
async fn owned<T: Send + 'static>(
    work: impl Future<Output = Result<T, StoreError>> + Send + 'static,
) -> Result<T, StoreError> {
    tokio::spawn(work)
        .await
        .map_err(|_| StoreError::Unsupported("EPG staging worker failed"))?
}
impl Store {
    /// The caller supplies its owned job/lock guard; it remains retained until
    /// actual database replies, even when the awaiting caller disappears.
    pub async fn begin_guide_staging(
        &self,
        source_id: i64,
        retained: Arc<dyn Send + Sync>,
    ) -> Result<GuideStaging, StoreError> {
        static ADMISSION: OnceLock<Arc<Semaphore>> = OnceLock::new();
        let admission = ADMISSION
            .get_or_init(|| Arc::new(Semaphore::new(4)))
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StoreError::Unsupported("EPG staging stopped"))?;
        let store = self.clone();
        owned(async move{
            // A private session makes temporary-table cleanup unambiguous:
            // closing it removes staging data instead of returning it to a pool.
            let mut connection=store.pool.acquire().await?.detach();
            let backend_pid=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut connection).await?;
            sqlx::query("CREATE TEMP TABLE epg_program_staging (epg_id bigint NOT NULL,start_time timestamptz NOT NULL,end_time timestamptz NOT NULL,title varchar(255) NOT NULL,sub_title text,description text,tvg_id varchar(255),custom_properties jsonb) ON COMMIT PRESERVE ROWS")
                .execute(&mut connection).await?;
            Ok(GuideStaging{connection,_admission:admission,_retained:retained,source_id,rows:0,bytes:0,backend_pid})
        }).await
    }
}
impl GuideStaging {
    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }
    /// Snapshot the original base/override mappings once, before the file pass.
    /// Nullable/empty identifiers still count as mapped IDs for cleanup but are
    /// excluded from programme lookup, matching the original source loop.
    pub async fn mapping_snapshot(self) -> Result<(Self, GuideMapping), StoreError> {
        owned(async move {
            let mut this=self;
            let mut mapping=GuideMapping{epg_ids:Vec::new(),by_tvg_id:HashMap::new()};
            let mut bytes=0usize;
            {
                let mut rows=sqlx::query("SELECT e.id,e.tvg_id FROM epg_epgdata e WHERE e.epg_source_id=$1 AND (EXISTS (SELECT 1 FROM dispatcharr_channels_channel c WHERE c.epg_data_id=e.id) OR EXISTS (SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.epg_data_id=e.id)) LIMIT 100001")
                    .bind(this.source_id).fetch(&mut this.connection);
                while let Some(row)=rows.try_next().await? {
                    let id:i64=row.try_get("id")?;
                    let tvg:Option<String>=row.try_get("tvg_id")?;
                    bytes=bytes.saturating_add(32+tvg.as_ref().map_or(0,String::len));
                    if mapping.epg_ids.len()>=MAX_MAPPED_IDS||bytes>16*1024*1024 {
                        return Err(StoreError::Unsupported("EPG mapping snapshot capacity"));
                    }
                    mapping.epg_ids.push(id);
                    if let Some(tvg)=tvg.filter(|value|!value.is_empty()){mapping.by_tvg_id.insert(tvg,id);}
                }
            }
            Ok((this,mapping))
        }).await
    }
    /// Original import status uses the source's total entry count sampled after
    /// mapping lookup. Retain the same private session/job through its SQL reply.
    pub async fn source_entry_count(self) -> Result<(Self, i64), StoreError> {
        owned(async move {
            let mut this = self;
            let count = sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM epg_epgdata WHERE epg_source_id=$1",
            )
            .bind(this.source_id)
            .fetch_one(&mut this.connection)
            .await?;
            Ok((this, count))
        })
        .await
    }
    /// Ownership moves into the worker. Dropping its waiter cannot free the
    /// connection, admission or retained job guard while SQL is still running.
    pub async fn push(self, rows: Vec<XmltvProgramme>) -> Result<Self, StoreError> {
        if rows.len() > MAX_BATCH_ROWS {
            return Err(StoreError::Unsupported("EPG staging batch capacity"));
        }
        let mut bytes = 0usize;
        for row in &rows {
            let json = row
                .custom_properties
                .as_ref()
                .map(serde_json::to_vec)
                .transpose()
                .map_err(|_| StoreError::Unsupported("EPG staging JSON"))?;
            bytes = bytes.saturating_add(
                row.title.len()
                    + row.tvg_id.len()
                    + row.sub_title.as_ref().map_or(0, String::len)
                    + row.description.as_ref().map_or(0, String::len)
                    + json.as_ref().map_or(0, Vec::len)
                    + 64,
            );
        }
        if bytes > MAX_BATCH_BYTES
            || self.bytes.saturating_add(bytes) > MAX_BYTES
            || self.rows.saturating_add(rows.len()) > MAX_ROWS
        {
            return Err(StoreError::Unsupported("EPG staging payload capacity"));
        }
        owned(async move{
            let mut this=self;
            if !rows.is_empty(){
                let mut query=QueryBuilder::<Postgres>::new("INSERT INTO epg_program_staging (epg_id,start_time,end_time,title,sub_title,description,tvg_id,custom_properties) ");
                query.push_values(&rows,|mut values,row|{
                    values.push_bind(row.epg_id).push_bind(row.start_time).push_bind(row.end_time).push_bind(&row.title)
                        .push_bind(&row.sub_title).push_bind(&row.description).push_bind(&row.tvg_id)
                        .push_bind(row.custom_properties.as_ref().map(sqlx::types::Json));
                });
                query.build().execute(&mut this.connection).await?;
            }
            this.rows+=rows.len();this.bytes+=bytes;Ok(this)
        }).await
    }
    pub async fn swap(
        self,
        mapped: Vec<i64>,
        now: DateTime<Utc>,
        policy: EpgHistoryPolicy,
        insert_batch: usize,
    ) -> Result<GuideSwapCounts, StoreError> {
        self.swap_cancellable(
            mapped,
            now,
            policy,
            insert_batch,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }
    /// Cancellation is checked on the private transaction before commit. This
    /// boundary is not a cross-Redis/PostgreSQL fencing guarantee.
    pub async fn swap_cancellable(
        self,
        mapped: Vec<i64>,
        now: DateTime<Utc>,
        policy: EpgHistoryPolicy,
        insert_batch: usize,
        cancelled: Arc<AtomicBool>,
    ) -> Result<GuideSwapCounts, StoreError> {
        if cancelled.load(Ordering::Acquire) {
            return Err(StoreError::Unsupported("EPG staging swap cancelled"));
        }
        if mapped.len() > MAX_MAPPED_IDS
            || !(1..=5000).contains(&insert_batch)
            || policy.min_days < 0
            || policy.max_days > 365
            || policy.min_days > policy.max_days
        {
            return Err(StoreError::Unsupported("EPG swap configuration capacity"));
        }
        owned(async move{
            let mut this=self;let mut transaction=this.connection.begin().await?;
            if cancelled.load(Ordering::Acquire) {
                transaction.rollback().await?;
                return Err(StoreError::Unsupported("EPG staging swap cancelled"));
            }
            sqlx::query("SET LOCAL statement_timeout = '10min'").execute(&mut *transaction).await?;
            let ids:Vec<i64>=sqlx::query_scalar("SELECT DISTINCT epg_id FROM epg_program_staging WHERE epg_id=ANY($1)")
                .bind(&mapped).fetch_all(&mut *transaction).await?;
            let mut counts=GuideSwapCounts::default();
            for chunk in ids.chunks(1000){
                let mut days:HashMap<i64,i64>=HashMap::new();
                let values=sqlx::query("SELECT c.epg_data_id AS epg_id,MAX(c.catchup_days) AS days FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.is_catchup AND c.epg_data_id=ANY($1) AND (o.id IS NULL OR o.epg_data_id IS NULL) GROUP BY c.epg_data_id UNION ALL SELECT o.epg_data_id AS epg_id,MAX(c.catchup_days) AS days FROM dispatcharr_channels_channel c JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.is_catchup AND o.epg_data_id=ANY($1) GROUP BY o.epg_data_id")
                    .bind(chunk).fetch_all(&mut *transaction).await?;
                for row in values{
                    let id:i64=row.try_get("epg_id")?;let value:Option<i32>=row.try_get("days")?;
                    if let Some(value)=value{days.entry(id).and_modify(|old|*old=(*old).max(i64::from(value))).or_insert(i64::from(value));}
                }
                let cutoffs:Vec<_>=chunk.iter().map(|id|(*id,now-Duration::days(days.get(id).copied().unwrap_or(0).clamp(policy.min_days,policy.max_days)))).collect();
                let mut query=QueryBuilder::<Postgres>::new("WITH cutoffs (epg_id,cutoff) AS (");
                query.push_values(&cutoffs,|mut value,(id,cutoff)|{value.push_bind(*id).push_bind(*cutoff);});
                query.push("), firsts AS (SELECT c.epg_id,c.cutoff,MIN(s.start_time) AS first_start FROM cutoffs c LEFT JOIN epg_program_staging s ON s.epg_id=c.epg_id AND s.start_time>=c.cutoff AND s.end_time>=c.cutoff GROUP BY c.epg_id,c.cutoff) DELETE FROM epg_programdata pd USING firsts f WHERE pd.epg_id=f.epg_id AND (pd.end_time<f.cutoff OR pd.end_time>f.first_start)");
                counts.deleted+=query.build().execute(&mut *transaction).await?.rows_affected();
                let mut query=QueryBuilder::<Postgres>::new("WITH cutoffs (epg_id,cutoff) AS (");
                query.push_values(&cutoffs,|mut value,(id,cutoff)|{value.push_bind(*id).push_bind(*cutoff);});
                query.push(") DELETE FROM epg_program_staging s USING cutoffs c WHERE s.epg_id=c.epg_id AND (s.end_time<c.cutoff OR (s.start_time<c.cutoff AND EXISTS (SELECT 1 FROM epg_programdata pd WHERE pd.epg_id=s.epg_id AND pd.start_time=s.start_time)))");
                query.build().execute(&mut *transaction).await?;
            }
            counts.orphaned=sqlx::query("DELETE FROM epg_programdata p USING epg_epgdata e WHERE p.epg_id=e.id AND e.epg_source_id=$1 AND NOT EXISTS (SELECT 1 FROM dispatcharr_channels_channel c WHERE c.epg_data_id=e.id) AND NOT EXISTS (SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.epg_data_id=e.id)")
                .bind(this.source_id).execute(&mut *transaction).await?.rows_affected();
            loop{
                let moved=sqlx::query("WITH moved AS (DELETE FROM epg_program_staging WHERE ctid IN (SELECT ctid FROM epg_program_staging LIMIT $1) RETURNING epg_id,start_time,end_time,title,sub_title,description,tvg_id,custom_properties) INSERT INTO epg_programdata (epg_id,start_time,end_time,title,sub_title,description,tvg_id,custom_properties) SELECT epg_id,start_time,end_time,title,sub_title,description,tvg_id,custom_properties FROM moved")
                    .bind(insert_batch as i64).execute(&mut *transaction).await?.rows_affected();
                if moved==0{break;}counts.inserted+=moved;
            }
            if cancelled.load(Ordering::Acquire) {
                transaction.rollback().await?;
                return Err(StoreError::Unsupported("EPG staging swap cancelled"));
            }
            transaction.commit().await?;Ok(counts)
        }).await
    }
}
