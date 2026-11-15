//! SQL-backed per-channel XML programme replacement on one actual Auth writer.
//! Original Dispatcharr authors retain all application/retention algorithm credit.
//! Parsing stages rows outside the atomic replacement; only complete input seals it.
use crate::{AuthService, StoreError, TaskSqlSession};
use dispatcharr_ingest_core::XmltvProgramme;
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;
#[derive(Debug)]
pub enum Error {
    Store(StoreError),
    Unsupported,
    Sql {
        stage: &'static str,
        source: sqlx::Error,
        rollback: Option<sqlx::Error>,
    },
    Supervisor,
}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}
#[derive(Clone)]
pub struct Service {
    auth: AuthService,
}
pub struct Stage {
    auth: AuthService,
    session: TaskSqlSession,
    table: String,
    epg: i64,
    rows: u64,
    _owner: Owner,
}
impl Service {
    pub fn prepare(auth: AuthService) -> Self {
        Self { auth }
    }
    /// Call before raw-head CAS. Capacity leaves that frame untouched.
    pub async fn admit(&self, epg: i64, owner: Owner) -> Result<Stage, Error> {
        let session = self.auth.borrow_task_sql_session(owner.clone()).await?;
        self.on_session(epg, session, owner).await
    }
    /// Consumes an actual existing session from the SAME downloader admission.
    /// No checkout, writer admission or replacement connection is performed.
    pub async fn on_session(
        &self,
        epg: i64,
        session: TaskSqlSession,
        owner: Owner,
    ) -> Result<Stage, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&epg) {
            session.close().await?;
            return Err(Error::Unsupported);
        }
        let table = format!("xml_tvg_{}", uuid::Uuid::new_v4().simple());
        let create = table.clone();
        let outcome = session.run(move |c| Box::pin(async move {
            sqlx::query(&format!("CREATE TEMP TABLE {create}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,epg_id bigint NOT NULL,start_time timestamptz NOT NULL,end_time timestamptz NOT NULL,title text NOT NULL,description text,sub_title text,tvg_id text NOT NULL,custom_properties jsonb) ON COMMIT PRESERVE ROWS"))
                .execute(c).await.map(|_| ()).map_err(StoreError::from)
        })).await;
        match outcome {
            Ok(Ok(())) => Ok(Stage {
                auth: self.auth.clone(),
                session: session.clone(),
                table,
                epg,
                rows: 0,
                _owner: Arc::new((owner, session)),
            }),
            other => {
                session.close().await?;
                Err(Error::Store(match other {
                    Ok(Err(e)) | Err(e) => e,
                    Ok(Ok(())) => unreachable!(),
                }))
            }
        }
    }
}
impl Stage {
    pub fn session(&self) -> TaskSqlSession {
        self.session.clone()
    }
    /// One bounded parser page. This is staging only: stored programmes cannot
    /// change until finish has observed the parser's actual EOF and FD identity.
    pub async fn append(&mut self, rows: Vec<XmltvProgramme>) -> Result<(), Error> {
        if rows.len() > 256 || rows.iter().any(|row| row.epg_id != self.epg) {
            return Err(Error::Unsupported);
        }
        let bytes = rows
            .iter()
            .try_fold(0usize, |n, row| {
                n.checked_add(
                    row.title.len()
                        + row.tvg_id.len()
                        + row.description.as_ref().map_or(0, String::len)
                        + row.sub_title.as_ref().map_or(0, String::len)
                        + row
                            .custom_properties
                            .as_ref()
                            .map_or(0, |v| v.to_string().len()),
                )
            })
            .ok_or(Error::Unsupported)?;
        if bytes > 4 * 1024 * 1024 + 512 * 1024 {
            return Err(Error::Unsupported);
        }
        if rows.is_empty() {
            return Ok(());
        }
        let count = rows.len() as u64;
        let table = self.table.clone();
        self.session.run(move |c| Box::pin(async move {
            let mut query: QueryBuilder<Postgres> = QueryBuilder::new(format!("INSERT INTO {table}(epg_id,start_time,end_time,title,description,sub_title,tvg_id,custom_properties) "));
            query.push_values(rows, |mut b,row| { b.push_bind(row.epg_id).push_bind(row.start_time).push_bind(row.end_time).push_bind(row.title).push_bind(row.description).push_bind(row.sub_title).push_bind(row.tvg_id).push_bind(row.custom_properties); });
            query.build().execute(c).await.map(|_| ()).map_err(StoreError::from)
        })).await??;
        self.rows = self.rows.checked_add(count).ok_or(Error::Unsupported)?;
        Ok(())
    }
    /// Parser errors/cancellation close the same physical session and discard
    /// its temporary rows. No partial parsed guide is installed.
    pub async fn discard(self) -> Result<(), Error> {
        self.session.close().await?;
        Ok(())
    }
    /// Caller must have joined the parser successfully before invoking this.
    /// The entire Source atomic block (including rollback) is ONE actor command;
    /// its post-operation Ready fence therefore never runs in an aborted tx.
    pub async fn finish(self) -> Result<(u64, u64), Error> {
        tokio::spawn(async move {
            let table = self.table.clone();
            let epg = self.epg;
            let original_count = self.rows;
            let auth = self.auth.clone();
            let result = self.session.run(move |c| Box::pin(async move {
                let mut transaction_open = false;
                let outcome = async {
                    sqlx::query("BEGIN").execute(&mut *c).await.map_err(|e| ("begin",e))?;
                    transaction_open = true;
                    let result = if original_count == 0 { (0,0) } else {
                        let now = chrono::Utc::now();
                        let policy = history_on(&auth, &mut *c).await.map_err(|e| ("history", e))?;
                        let days: i64 = sqlx::query_scalar(crate::tvg_programme::CATCHUP_SQL).bind(epg).fetch_one(&mut *c).await.map_err(|e| ("catchup",e))?;
                        let cutoff = now - chrono::Duration::days(days.clamp(policy.min_days,policy.max_days));
                        let first: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(&format!("SELECT min(start_time) FROM {table} WHERE end_time>=$1 AND start_time>=$1")).bind(cutoff).fetch_one(&mut *c).await.map_err(|e| ("first-start",e))?;
                        let deleted = sqlx::query(crate::tvg_programme::DELETE_SQL).bind(epg).bind(cutoff).bind(first).execute(&mut *c).await.map_err(|e| ("delete",e))?.rows_affected();
                        // Source materializes the kept-starts set AFTER deletion
                        // and BEFORE insertion. Mark staging eligibility now so
                        // later insert pages cannot deduplicate sibling pull rows.
                        sqlx::query(&format!("DELETE FROM {table} x WHERE x.end_time<$1 OR (x.start_time<$1 AND EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id=$2 AND p.start_time=x.start_time AND p.start_time<$1 AND p.end_time>=$1))")).bind(cutoff).bind(epg).execute(&mut *c).await.map_err(|e| ("kept-starts",e))?;
                        let mut after=0i64;
                        let mut inserted=0u64;
                        loop {
                            // Source groups the FILTERED input in batches of
                            // 5000, retaining original XML order and duplicates.
                            let end: Option<i64> = sqlx::query_scalar(&format!("SELECT max(ordinal) FROM(SELECT ordinal FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 5000) page")).bind(after).fetch_one(&mut *c).await.map_err(|e| ("page",e))?;
                            let Some(end)=end else { break };
                            inserted+=sqlx::query(&format!("INSERT INTO epg_programdata(epg_id,start_time,end_time,title,description,sub_title,tvg_id,custom_properties) SELECT epg_id,start_time,end_time,title,description,sub_title,tvg_id,custom_properties FROM {table} WHERE ordinal>$1 AND ordinal<=$2 ORDER BY ordinal")).bind(after).bind(end).execute(&mut *c).await.map_err(|e| ("insert",e))?.rows_affected();
                            after=end;
                        }
                        (deleted,inserted)
                    };
                    sqlx::query("COMMIT").execute(&mut *c).await.map_err(|e| ("commit",e))?;
                    transaction_open=false;
                    Ok::<_,(&'static str,sqlx::Error)>(result)
                }.await;
                let rollback=if outcome.is_err()&&transaction_open { sqlx::query("ROLLBACK").execute(&mut *c).await.err() }else{None};
                outcome.map_err(|(stage,source)|Error::Sql{stage,source,rollback})
            })).await;
            // The actor observes Ready before sending result, and close waits
            // the actual physical connection. Owner/writer survive observer loss.
            let closed=self.session.close().await;
            closed?;
            result?
        }).await.map_err(|_|Error::Supervisor)?
    }
}
async fn history_on(
    auth: &AuthService,
    c: &mut PgConnection,
) -> Result<crate::EpgHistoryPolicy, sqlx::Error> {
    let load = || async move {
        let row=sqlx::query("SELECT octet_length(value::text)<=65536 AS fits,CASE WHEN octet_length(value::text)<=65536 THEN value END AS value FROM core_coresettings WHERE key='epg_settings'").fetch_optional(c).await?;
        let Some(row) = row else {
            return Ok(crate::settings_groups::default_epg());
        };
        if !row.try_get::<bool, _>("fits")? {
            return Err(StoreError::Unsupported("XML history settings capacity"));
        }
        let value: Value = row.try_get("value")?;
        Ok(if value.as_object().is_some_and(|v| !v.is_empty()) {
            value
        } else {
            crate::settings_groups::default_epg()
        })
    };
    let value = match &auth.notification_cache {
        Some(cache) => cache.settings_group("epg_settings", load).await,
        None => load().await,
    };
    let value = value.map_err(|e| match e {
        StoreError::Database(e) => e,
        _ => sqlx::Error::Protocol("XML history policy unavailable".into()),
    })?;
    crate::source_epg_history_policy(&value)
        .map_err(|_| sqlx::Error::Protocol("XML history policy unsupported".into()))
}
