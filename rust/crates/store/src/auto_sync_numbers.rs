//! Source auto-sync number reservations over the SAME physically owned session.
//! Application and algorithm credit belongs to Original Dispatcharr authors.
//! Draft component, not registered or compiled; full sync orchestration pending.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use sqlx::Row;
use std::sync::Arc;

/// The table is session-private, so even caller loss leaves its lifetime tied to
/// the actual TaskSqlSession/connection. No global ID set, client or pool exists.
pub(crate) struct Numbers {
    session: Arc<TaskSqlSession>,
    table: String,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Auto-sync channel number cannot be advanced as a finite Source float")
}
fn finite(value: f64) -> Result<f64, StoreError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(unsupported())
    }
}
impl Numbers {
    pub(crate) async fn prepare(
        session: Arc<TaskSqlSession>,
        account: i64,
    ) -> Result<Self, StoreError> {
        let table = format!("native_auto_numbers_{}", uuid::Uuid::new_v4().simple());
        let relation = table.clone();
        session.run(move |c| Box::pin(async move {
            // Both snapshots are Source's actual initial set. Advisory group
            // ranges are deliberately absent; overrides reserve globally and
            // hidden account channels remain occupied.
            sqlx::query(&format!("CREATE TEMP TABLE {relation}(number double precision PRIMARY KEY) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {relation}(number) SELECT channel_number::double precision FROM dispatcharr_channels_channel WHERE channel_number IS NOT NULL AND NOT(auto_created AND COALESCE(auto_created_by_id=$1,false) AND NOT hidden_from_output) UNION SELECT channel_number::double precision FROM dispatcharr_channels_channeloverride WHERE channel_number IS NOT NULL ON CONFLICT DO NOTHING")).bind(account).execute(&mut*c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(Self { session, table })
    }
    pub(crate) async fn reserve(&self, value: f64) -> Result<(), StoreError> {
        let value = finite(value)?;
        let table = self.table.clone();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!(
                        "INSERT INTO {table}(number) VALUES($1) ON CONFLICT DO NOTHING"
                    ))
                    .bind(value)
                    .execute(&mut *c)
                    .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
    /// Mirrors Source set.discard during overflow pruning; no special override
    /// re-reservation is invented after the Source set has discarded a value.
    pub(crate) async fn discard(&self, value: f64) -> Result<(), StoreError> {
        let value = finite(value)?;
        let table = self.table.clone();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("DELETE FROM {table} WHERE number=$1"))
                        .bind(value)
                        .execute(c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
    /// Source provider numbers override even a stale group End. next_available
    /// starts at1 and ignores End. Other mode strings use the fixed cursor.
    /// Picking does not reserve; Source callers reserve after actual selection.
    pub(crate) async fn pick(
        &self,
        mode: &str,
        provider: Option<f64>,
        fixed: f64,
        fallback: f64,
        end: Option<f64>,
    ) -> Result<Option<f64>, StoreError> {
        let (preferred, start, end) = match mode {
            "provider" => (
                provider.map(finite).transpose()?,
                finite(fallback)?,
                end.map(finite).transpose()?,
            ),
            "next_available" => (None, 1.0, None),
            _ => (None, finite(fixed)?, end.map(finite).transpose()?),
        };
        let table = self.table.clone();
        self.session.run(move |c| Box::pin(async move {
            if let Some(value) = preferred {
                let used:bool=sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE number=$1)")).bind(value).fetch_one(&mut*c).await?;
                if !used { return Ok(Some(value)); }
            }
            // Indexed lookup per occupied step; there is no all-channel host
            // allocation or arbitrary stream-count refusal. Preserve fractional
            // start +1 progression exactly as Source's helper actually does.
            let row=sqlx::query(&format!("WITH RECURSIVE candidate(n) AS(SELECT $1::double precision UNION ALL SELECT n+1 FROM candidate WHERE EXISTS(SELECT 1 FROM {table} u WHERE u.number=n) AND ($2::double precision IS NULL OR n<=$2) AND n+1<>n) SELECT n,EXISTS(SELECT 1 FROM {table} u WHERE u.number=n) AS used FROM candidate WHERE NOT EXISTS(SELECT 1 FROM {table} u WHERE u.number=n) OR n+1=n OR ($2::double precision IS NOT NULL AND n>$2) LIMIT 1")).bind(start).bind(end).fetch_optional(&mut*c).await?;
            let Some(row)=row else{return Err(unsupported())};
            let number:f64=row.try_get("n")?;
            if end.is_some_and(|end|number>end){return Ok(None)}
            if row.try_get::<bool,_>("used")?{return Err(unsupported())}
            Ok(Some(finite(number)?))
        })).await?
    }
    pub(crate) async fn close(self) -> Result<(), StoreError> {
        let table = self.table;
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("DROP TABLE {table}"))
                        .execute(c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
}
