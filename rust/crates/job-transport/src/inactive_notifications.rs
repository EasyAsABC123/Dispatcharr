//! Inactive-profile stale notification collector, derived from Original tasks.py.
//! Original Dispatcharr/Django/Channels authors retain code and behavior credit.
//! No active profile evaluation. Bounded 128 notices/1024 dismissals; no claim
//! for concurrent mutations between Source's key SELECT and collector transaction.
use crate::default_domains::Error;
use crate::notification_terminal::{settled, Effect, ReportError};
use dispatcharr_contracts::{UpdateEventError, UpdateEventSink};
use serde_json::json;
use sqlx::{Connection, PgPool};
use std::sync::Arc;
use tokio::sync::oneshot;
const FILTER: &str = "is_active AND (notification_key LIKE 'xc-exp-warning-%' OR notification_key LIKE 'xc-exp-expired-%')";
struct Receipt {
    done: Option<oneshot::Sender<()>>,
    _owner: Arc<dyn Send + Sync>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
async fn eligibility(connection: &mut sqlx::PgConnection) -> Result<(), Error> {
    let active:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL)").fetch_one(&mut *connection).await.map_err(Error::Sql)?;
    if active {
        return Err(Error::Unsupported);
    }
    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM core_systemnotification WHERE {FILTER}"
    ))
    .fetch_one(&mut *connection)
    .await
    .map_err(Error::Sql)?;
    let related:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM core_notificationdismissal WHERE notification_id IN (SELECT id FROM core_systemnotification WHERE {FILTER})")).fetch_one(&mut *connection).await.map_err(Error::Sql)?;
    if count > 128 || related > 1024 {
        return Err(Error::Unsupported);
    }
    Ok(())
}
pub(crate) async fn preflight(pool: PgPool, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
    tokio::spawn(async move {
        let _owner = owner;
        let mut connection = pool.acquire().await.map_err(Error::Sql)?;
        let result = eligibility(&mut connection).await;
        let closed = connection.close().await;
        result?;
        closed.map_err(Error::Sql)
    })
    .await
    .map_err(|_| Error::Panic)?
}
pub(crate) async fn execute(
    pool: PgPool,
    sink: Arc<dyn UpdateEventSink>,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), Error> {
    execute_report(pool, sink, owner)
        .await
        .map_err(|report| report.error)
}
pub(crate) async fn execute_report(
    pool: PgPool,
    sink: Arc<dyn UpdateEventSink>,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), ReportError> {
    tokio::spawn(async move {
 let _owner=owner.clone();let mut connection=pool.acquire().await.map_err(Error::Sql)?;
 let observed=async {
 eligibility(&mut connection).await?;
 // Source observes ordered keys before its independent atomic Collector.delete.
 let rows:Vec<(i64,Option<String>)>=sqlx::query_as(&format!("SELECT id::bigint,CASE WHEN octet_length(notification_key)<=1020 THEN notification_key ELSE NULL END FROM core_systemnotification WHERE {FILTER} ORDER BY priority DESC,created_at DESC LIMIT 129")).fetch_all(&mut *connection).await.map_err(Error::Sql)?;
 if rows.len()>128 || rows.iter().any(|(_,key)|key.as_ref().is_none_or(|v|v.chars().count()>255)){return Err(Error::Unsupported.into());}
 let ids:Vec<i64>=rows.iter().map(|v|v.0).collect();
 let mut transaction=connection.begin().await.map_err(Error::Sql)?;
 let deleted=async {
 // Lock each bounded parent before rechecking related count: FK insertions
 // cannot expand the admitted collector while DELETE is in progress.
 let locked:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemnotification WHERE id=ANY($1) FOR UPDATE").bind(&ids).fetch_all(&mut *transaction).await.map_err(Error::Sql)?;
 if locked.len()>128 { return Err(Error::Unsupported.into()); }
 let related:i64=sqlx::query_scalar("SELECT count(*) FROM core_notificationdismissal WHERE notification_id=ANY($1)").bind(&ids).fetch_one(&mut *transaction).await.map_err(Error::Sql)?;
 if related>1024 { return Err(Error::Unsupported.into()); }

 // Django CASCADE is an application collector, not database ON DELETE CASCADE.
 // Both statements are inside the one collector transaction, children first.
 sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id=ANY($1)").bind(&ids).execute(&mut *transaction).await.map_err(|error|ReportError::effect(error,Effect::FinalCollector))?;
 sqlx::query("DELETE FROM core_systemnotification WHERE id=ANY($1)").bind(&ids).execute(&mut *transaction).await.map_err(|error|ReportError::effect(error,Effect::FinalCollector))?;
 Ok::<_,ReportError>(())
 }.await;
 match deleted {Ok(())=>transaction.commit().await.map_err(Error::Sql)?,Err(error)=>{transaction.rollback().await.map_err(Error::Sql)?;return Err(error);}}
 Ok::<_,ReportError>(rows.into_iter().map(|v|v.1.expect("bounded keys checked")).collect::<Vec<_>>())
 }.await;
 let closed=connection.close().await;let keys=settled(observed,closed)?;
 for key in keys {
 let (done,settled)=oneshot::channel();let receipt=Arc::new(Receipt{done:Some(done),_owner:owner.clone()});
 let sent=sink.send_retained("updates",json!({"type":"update","data":{"type":"notification_dismissed","notification_key":key}}),receipt.clone()).await;
 drop(receipt);settled.await.map_err(|_|Error::Event)?;
 // Source catches Channels send exceptions after the collector commit.
 match sent{Ok(())|Err(UpdateEventError::Unavailable)=>{},Err(_)=>return Err(Error::Event.into())}
 }
 Ok(())
 }).await.map_err(|_|Error::Panic)?
}
