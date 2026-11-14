//! PRIVATE normal per-channel programme Store composition; uncompiled/unqualified.
//! Original Dispatcharr authors retain application/SQL algorithm credit.
//! Own one actual connection and caller through rollback/Ready/physical close.
use crate::Store;
use chrono::{DateTime, Utc};
use dispatcharr_ingest_core::XmltvProgramme;
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row};
use std::sync::Arc;
#[derive(Debug)]
pub enum Error {
    Unsupported,
    Sql {
        stage: &'static str,
        source: sqlx::Error,
        rollback: Option<sqlx::Error>,
        ready: Option<sqlx::Error>,
        close: Option<sqlx::Error>,
    },
    Settlement {
        ready: Option<sqlx::Error>,
        close: Option<sqlx::Error>,
    },
    Capacity {
        rollback: Option<sqlx::Error>,
        ready: Option<sqlx::Error>,
        close: Option<sqlx::Error>,
    },
    Supervisor,
}
#[derive(Debug)]
pub struct Context {
    pub epg_id: i64,
    pub tvg_id: String,
    pub source_id: i64,
    pub source_type: String,
    pub file_path: Option<String>,
    pub extracted_file_path: Option<String>,
    pub mapped: bool,
}
pub const CONTEXT_SQL: &str = "SELECT e.id::bigint AS epg_id,CASE WHEN octet_length(e.tvg_id)<=1024 THEN e.tvg_id ELSE NULL END AS tvg_id,s.id::bigint AS source_id,s.source_type,CASE WHEN octet_length(s.file_path)<=4096 THEN s.file_path ELSE NULL END AS file_path,CASE WHEN octet_length(s.extracted_file_path)<=4096 THEN s.extracted_file_path ELSE NULL END AS extracted_file_path,(octet_length(e.tvg_id)>1024 OR coalesce(octet_length(s.file_path)>4096,false) OR coalesce(octet_length(s.extracted_file_path)>4096,false)) AS oversized,EXISTS(SELECT 1 FROM dispatcharr_channels_channel c WHERE c.epg_data_id=e.id) OR EXISTS(SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.epg_data_id=e.id) AS mapped FROM epg_epgdata e JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE e.id=$1";
pub const DELETE_SQL: &str = "DELETE FROM epg_programdata WHERE epg_id=$1 AND (end_time<$2 OR ($3::timestamptz IS NOT NULL AND end_time>$3))";
pub const KEPT_SQL: &str = "SELECT start_time FROM epg_programdata WHERE epg_id=$1 AND start_time<$2 AND end_time>=$2 LIMIT 4097";
enum Fault {
    Sql(&'static str, sqlx::Error),
    Capacity,
}
impl From<(&'static str, sqlx::Error)> for Fault {
    fn from((stage, error): (&'static str, sqlx::Error)) -> Self {
        Self::Sql(stage, error)
    }
}
async fn finish<T>(
    mut connection: PgConnection,
    outcome: Result<T, Fault>,
    transaction_open: bool,
) -> Result<T, Error> {
    let rollback = if outcome.is_err() && transaction_open {
        sqlx::query("ROLLBACK").execute(&mut connection).await.err()
    } else {
        None
    };
    // SQLx ErrorResponse precedes Ready. Only a real ping and physical close
    // settle this local owner. Caller cancellation never drops this task.
    let ready = connection.ping().await.err();
    let close = connection.close().await.err();
    match outcome {
        Err(Fault::Sql(stage, source)) => Err(Error::Sql {
            stage,
            source,
            rollback,
            ready,
            close,
        }),
        Err(Fault::Capacity) => Err(Error::Capacity {
            rollback,
            ready,
            close,
        }),
        Ok(value) => {
            if ready.is_some() || close.is_some() {
                Err(Error::Settlement { ready, close })
            } else {
                Ok(value)
            }
        }
    }
}
pub const CATCHUP_SQL:&str="SELECT coalesce(MAX(days),0)::bigint FROM (SELECT MAX(c.catchup_days) AS days FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.is_catchup AND c.epg_data_id=$1 AND (o.id IS NULL OR o.epg_data_id IS NULL) UNION ALL SELECT MAX(c.catchup_days) AS days FROM dispatcharr_channels_channel c JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.is_catchup AND o.epg_data_id=$1) AS selected";
impl Store {
    pub async fn tvg_retention_cutoff(
        &self,
        epg_id: i64,
        now: DateTime<Utc>,
        policy: crate::EpgHistoryPolicy,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<DateTime<Utc>, Error> {
        if epg_id <= 0
            || policy.min_days < 0
            || policy.max_days > 365
            || policy.min_days > policy.max_days
        {
            return Err(Error::Unsupported);
        }
        let pool = self.pool().clone();
        tokio::spawn(async move {
            let _owner = owner;
            let mut c = pool
                .acquire()
                .await
                .map_err(|source| Error::Sql {
                    stage: "acquire",
                    source,
                    rollback: None,
                    ready: None,
                    close: None,
                })?
                .detach();
            let result = sqlx::query_scalar::<_, i64>(CATCHUP_SQL)
                .bind(epg_id)
                .fetch_one(&mut c)
                .await
                .map_err(|e| Fault::Sql("catchup", e));
            let days = finish(c, result, false)
                .await?
                .clamp(policy.min_days, policy.max_days);
            Ok(now - chrono::Duration::days(days))
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub async fn tvg_programme_context(
        &self,
        epg_id: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Context>, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&epg_id) {
            return Err(Error::Unsupported);
        }
        let pool = self.pool().clone();
        tokio::spawn(async move {
            let _owner = owner;
            let mut c = pool
                .acquire()
                .await
                .map_err(|source| Error::Sql {
                    stage: "acquire",
                    source,
                    rollback: None,
                    ready: None,
                    close: None,
                })?
                .detach();
            let result = async {
                let row = sqlx::query(CONTEXT_SQL)
                    .bind(epg_id)
                    .fetch_optional(&mut c)
                    .await?;
                row.map(|row| {
                    if row.try_get::<bool, _>("oversized")? {
                        return Ok((true, None));
                    }
                    Ok((
                        false,
                        Some(Context {
                            epg_id: row.try_get("epg_id")?,
                            tvg_id: row.try_get("tvg_id")?,
                            source_id: row.try_get("source_id")?,
                            source_type: row.try_get("source_type")?,
                            file_path: row.try_get("file_path")?,
                            extracted_file_path: row.try_get("extracted_file_path")?,
                            mapped: row.try_get("mapped")?,
                        }),
                    ))
                })
                .transpose()
            }
            .await
            .map_err(|e| Fault::Sql("context", e));
            let result = finish(c, result, false).await?;
            match result {
                Some((true, _)) => Err(Error::Unsupported),
                other => Ok(other.and_then(|(_, context)| context)),
            }
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    /// No task terminal/result policy here. SQL, rollback and Ready fate are
    /// retained for the family worker's later Source-backed classifier.
    pub async fn swap_tvg_programmes(
        &self,
        epg_id: i64,
        cutoff: DateTime<Utc>,
        rows: Vec<XmltvProgramme>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(u64, u64), Error> {
        if rows.len() > 4096 || rows.iter().any(|r| r.epg_id != epg_id) {
            return Err(Error::Unsupported);
        }
        let bytes = rows
            .iter()
            .map(|r| {
                r.title.len()
                    + r.tvg_id.len()
                    + r.description.as_ref().map_or(0, String::len)
                    + r.sub_title.as_ref().map_or(0, String::len)
                    + r.custom_properties
                        .as_ref()
                        .map_or(0, |v| v.to_string().len())
            })
            .sum::<usize>();
        if bytes > 4 * 1024 * 1024 || epg_id <= 0 {
            return Err(Error::Unsupported);
        }
        let pool = self.pool().clone();
        tokio::spawn(async move {
            let _owner=owner;let mut c=pool.acquire().await.map_err(|source|Error::Sql {stage:"acquire",source,rollback:None,ready:None,close:None})?.detach();
            let mut transaction_open=false;
            let result=async {
                sqlx::query("BEGIN").execute(&mut c).await.map_err(|e|("begin",e))?;transaction_open=true;
                let mut deleted=0;let mut inserted=0;
                if !rows.is_empty() {
                    let mut rows:Vec<_>=rows.into_iter().filter(|r|r.end_time>=cutoff).collect();
                    let first=rows.iter().filter(|r|r.start_time>=cutoff).map(|r|r.start_time).min();
                    deleted=sqlx::query(DELETE_SQL).bind(epg_id).bind(cutoff).bind(first).execute(&mut c).await.map_err(|e|("delete",e))?.rows_affected();
                    let kept=sqlx::query_scalar::<_,DateTime<Utc>>(KEPT_SQL).bind(epg_id).bind(cutoff).fetch_all(&mut c).await.map_err(|e|("kept-starts",e))?;
                    if kept.len()>4096 { return Err(Fault::Capacity); }
                    rows.retain(|r|r.start_time>=cutoff||!kept.contains(&r.start_time));
                    for chunk in rows.chunks(2500) {
                        let mut q:QueryBuilder<Postgres>=QueryBuilder::new("INSERT INTO epg_programdata(epg_id,start_time,end_time,title,description,sub_title,tvg_id,custom_properties) ");
                        q.push_values(chunk,|mut b,r|{b.push_bind(r.epg_id).push_bind(r.start_time).push_bind(r.end_time).push_bind(&r.title).push_bind(&r.description).push_bind(&r.sub_title).push_bind(&r.tvg_id).push_bind(&r.custom_properties);});
                        inserted+=q.build().execute(&mut c).await.map_err(|e|("insert",e))?.rows_affected();
                    }
                }
                sqlx::query("COMMIT").execute(&mut c).await.map_err(|e|("commit",e))?;transaction_open=false;Ok((deleted,inserted))
            }.await;
            finish(c,result,transaction_open).await
        }).await.map_err(|_|Error::Supervisor)?
    }
}
