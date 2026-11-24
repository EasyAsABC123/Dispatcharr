//! Bounded, opt-in active-profile expiration evaluation from Original tasks.py.
//! Original Dispatcharr/Django/Channels authors retain behavior and code credit.
//! Up to32 eligible profiles/64KiB fetched input, 128 collector rows/1024 dismissals.
//! Exact Original SELECT/unsorted iteration; no concurrency or
//! mixed-live process-local proxy-manager side-effect parity is claimed.
use crate::default_domains::Error;
use crate::notification_terminal::{settled, Effect, ReportError};
use chrono::{DateTime, Datelike, SecondsFormat, Timelike, Utc};
use dispatcharr_contracts::{UpdateEventError, UpdateEventSink};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::sync::Arc;
use tokio::sync::{oneshot, Semaphore};
/// A per-evaluation clock. Existing inactive constructors do not use this API.
pub trait NotificationClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}
pub struct UtcNotificationClock;
impl NotificationClock for UtcNotificationClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
#[derive(Clone)]
pub(crate) struct Service {
    pool: PgPool,
    sink: Arc<dyn UpdateEventSink>,
    clock: Arc<dyn NotificationClock>,
    slots: Arc<Semaphore>,
}
pub(crate) type Profile = (i64, i64, String, String, DateTime<Utc>);
const SOURCE_PROFILE_QUERY: &str = r#"SELECT "m3u_m3uaccountprofile"."id", "m3u_m3uaccountprofile"."m3u_account_id", "m3u_m3uaccountprofile"."name", "m3u_m3uaccountprofile"."is_default", "m3u_m3uaccountprofile"."max_streams", "m3u_m3uaccountprofile"."is_active", "m3u_m3uaccountprofile"."search_pattern", "m3u_m3uaccountprofile"."replace_pattern", "m3u_m3uaccountprofile"."current_viewers", "m3u_m3uaccountprofile"."custom_properties", "m3u_m3uaccountprofile"."exp_date", "m3u_m3uaccount"."id", "m3u_m3uaccount"."name", "m3u_m3uaccount"."server_url", "m3u_m3uaccount"."file_path", "m3u_m3uaccount"."server_group_id", "m3u_m3uaccount"."max_streams", "m3u_m3uaccount"."is_active", "m3u_m3uaccount"."created_at", "m3u_m3uaccount"."updated_at", "m3u_m3uaccount"."status", "m3u_m3uaccount"."last_message", "m3u_m3uaccount"."user_agent_id", "m3u_m3uaccount"."locked", "m3u_m3uaccount"."stream_profile_id", "m3u_m3uaccount"."account_type", "m3u_m3uaccount"."username", "m3u_m3uaccount"."password", "m3u_m3uaccount"."custom_properties", "m3u_m3uaccount"."refresh_interval", "m3u_m3uaccount"."refresh_task_id", "m3u_m3uaccount"."stale_stream_days", "m3u_m3uaccount"."priority" FROM "m3u_m3uaccountprofile" INNER JOIN "m3u_m3uaccount" ON ("m3u_m3uaccountprofile"."m3u_account_id" = "m3u_m3uaccount"."id") WHERE ("m3u_m3uaccountprofile"."exp_date" IS NOT NULL AND "m3u_m3uaccountprofile"."is_active" AND "m3u_m3uaccount"."is_active")"#;
const FILTER:&str="is_active AND (notification_key LIKE 'xc-exp-warning-%' OR notification_key LIKE 'xc-exp-expired-%')";
fn candidate_counts_sql() -> String {
    format!(
        "WITH candidates AS (
            SELECT id FROM core_systemnotification
            WHERE ({FILTER})
               OR notification_key IN (
                    SELECT 'xc-exp-warning-'||p.id::text
                    FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id
                    WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL
               )
               OR notification_key IN (
                    SELECT 'xc-exp-expired-'||p.id::text
                    FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id
                    WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL
               )
            LIMIT 129
        )
        SELECT (SELECT count(*) FROM candidates),
               (SELECT count(*) FROM (
                    SELECT 1 FROM core_notificationdismissal d
                    JOIN candidates n ON n.id=d.notification_id
                    LIMIT 1025
               ) AS related)"
    )
}
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
impl Service {
    pub(crate) fn new(
        pool: PgPool,
        sink: Arc<dyn UpdateEventSink>,
        clock: Arc<dyn NotificationClock>,
    ) -> Self {
        Self {
            pool,
            sink,
            clock,
            slots: Arc::new(Semaphore::new(4)),
        }
    }
    pub(crate) fn with_clock(mut self, clock: Arc<dyn NotificationClock>) -> Self {
        self.clock = clock;
        self
    }
    async fn send(&self, event: Value, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        match send_observed(self.sink.clone(), event, owner).await? {
            Ok(()) | Err(UpdateEventError::Unavailable) => Ok(()),
            Err(_) => Err(Error::Event),
        }
    }
    async fn inspect(connection: &mut PgConnection) -> Result<Vec<Profile>, Error> {
        // Count and byte aggregation execute inside the same read-only repeatable
        // snapshot as the exact Source SELECT. No LIMIT/projection/order change
        // is allowed to invent a different multi-profile iteration order.
        let mut tx = connection.begin().await.map_err(Error::Sql)?;
        let observed=async {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY").execute(&mut *tx).await.map_err(Error::Sql)?;
            let count:i64=sqlx::query_scalar("SELECT count(*)::bigint FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL").fetch_one(&mut *tx).await.map_err(Error::Sql)?;
            if count>32 {return Err(Error::Unsupported);}
            let rows:Vec<Profile>=if count<=1 {
                // Preserve the qualified0/1 projection: unused JSON was never
                // fetched or capped in that existing domain; row order cannot
                // differ with a unique eligible profile.
                sqlx::query_as("SELECT p.id::bigint,a.id::bigint,p.name,a.name,p.exp_date FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL LIMIT 2").fetch_all(&mut *tx).await.map_err(Error::Sql)?
            }else{
                let bytes:i64=sqlx::query_scalar("SELECT COALESCE(sum(octet_length(row_to_json(p)::text)+octet_length(row_to_json(a)::text)),0)::bigint FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL").fetch_one(&mut *tx).await.map_err(Error::Sql)?;
                if bytes>65536{return Err(Error::Unsupported);}
                let selected=sqlx::query(SOURCE_PROFILE_QUERY).persistent(false).fetch_all(&mut *tx).await.map_err(Error::Sql)?;
                let mut rows=Vec::with_capacity(selected.len());
                for row in selected {rows.push((row.try_get(0).map_err(Error::Sql)?,row.try_get(1).map_err(Error::Sql)?,row.try_get(2).map_err(Error::Sql)?,row.try_get(12).map_err(Error::Sql)?,row.try_get(10).map_err(Error::Sql)?));}
                rows
            };
            if rows.len()!=count as usize || rows.len()>32 {return Err(Error::Unsupported);}
            for p in &rows {
                if p.2.chars().count()>237 || p.3.chars().count()>255 || !(1970..=9999).contains(&p.4.year()){return Err(Error::Unsupported);}
            }
            Ok::<_,Error>(rows)
        }.await;
        let rows = match observed {
            Ok(rows) => {
                tx.commit().await.map_err(Error::Sql)?;
                rows
            }
            Err(e) => {
                tx.rollback().await.map_err(Error::Sql)?;
                return Err(e);
            }
        };
        // Only expiration candidates can reach the collectors. A dismissal
        // of an unrelated notice cannot consume their admission budget.
        // Count both bounded sets in one snapshot; deletes still recheck
        // the exact related rows under their parent locks.
        let (notices, dismissals): (i64, i64) = sqlx::query_as(&candidate_counts_sql())
            .fetch_one(&mut *connection)
            .await
            .map_err(Error::Sql)?;
        if notices > 128 || dismissals > 1024 {
            return Err(Error::Unsupported);
        }
        Ok(rows)
    }
    pub(crate) async fn preflight(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Unsupported)?;
        let this = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _owner = owner;
            let mut c = this.pool.acquire().await.map_err(Error::Sql)?;
            let observed = Self::inspect(&mut c).await;
            let closed = c.close().await;
            observed?;
            closed.map_err(Error::Sql)
        })
        .await
        .map_err(|_| Error::Panic)?
    }
    async fn collect(
        &self,
        keys: Option<Vec<String>>,
        keep: Option<Vec<String>>,
        effect: Effect,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), ReportError> {
        let mut c = self.pool.acquire().await.map_err(Error::Sql)?;
        let observed = collect_on_connection(&mut c, keys, keep, effect).await;
        let closed = c.close().await;
        let keys = settled(observed, closed)?;
        for key in keys {
            self.send(json!({"type":"update","data":{"type":"notification_dismissed","notification_key":key}}),owner.clone()).await?;
        }
        Ok(())
    }
    async fn upsert(
        &self,
        p: &Profile,
        now: DateTime<Utc>,
        expired: bool,
        key: &str,
    ) -> Result<Value, ReportError> {
        let title = format!(
            "Account {}: {}",
            if expired { "Expired" } else { "Expiring" },
            p.2
        );
        if title.chars().count() > 255 {
            return Err(Error::Unsupported.into());
        }
        let mut c = self.pool.acquire().await.map_err(Error::Sql)?;
        let observed = upsert_on_connection(&mut c, p, now, expired, key).await;
        let closed = c.close().await;
        settled(observed, closed)
    }
    pub(crate) async fn execute(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        self.execute_report(owner)
            .await
            .map_err(|report| report.error)
    }
    pub(crate) async fn execute_report(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), ReportError> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Unsupported)?;
        let this = self.clone();
        tokio::spawn(async move{
            let _permit=permit;let _owner=owner.clone();let mut c=this.pool.acquire().await.map_err(Error::Sql)?;
            let observed=Self::inspect(&mut c).await;let closed=c.close().await;let profiles=observed?;closed.map_err(Error::Sql)?;
            let mut keep=Vec::new();
            for p in profiles{
                let now=this.clock.now();if !(1970..=9999).contains(&now.year()){return Err(Error::Unsupported.into());}
                let plan=profile_plan(p.0,Some(p.4),now);
                this.collect(Some(plan.remove),None,Effect::OppositeCollector,owner.clone()).await?;
                if let Some(key)=plan.key {
                    let row=this.upsert(&p,now,plan.expired,&key).await?;
                    this.send(json!({"type":"update","data":{"type":"system_notification","notification":row}}),owner.clone()).await?;keep.push(key);
                }
            }
            this.collect(None,Some(keep),Effect::FinalCollector,owner).await
        }).await.map_err(|_|Error::Panic)?
    }
}
fn iso(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(
        if value.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        false,
    )
}

/// Exact legacy collector SQL/inner transaction on the caller's session.
pub(crate) async fn collect_on_connection(
    connection: &mut PgConnection,
    keys: Option<Vec<String>>,
    keep: Option<Vec<String>>,
    effect: Effect,
) -> Result<Vec<String>, ReportError> {
    // Preserve accepted post-save/task query and transaction ordering.
    let rows = collector_rows(connection, keys, keep).await?;
    let mut tx = connection.begin().await.map_err(Error::Sql)?;
    let deleted = delete_collector_rows(&mut tx, &rows, effect).await;
    match deleted {
        Ok(()) => tx.commit().await.map_err(Error::Sql)?,
        Err(error) => {
            tx.rollback().await.map_err(Error::Sql)?;
            return Err(error);
        }
    }
    Ok(rows
        .into_iter()
        .map(|v| v.1.expect("bounded key checked"))
        .collect())
}
/// Same Collector core for Source's existing raw BEGIN owner. This verifies
/// physical transaction status; it does not open, commit or detach a TX.
pub(crate) async fn collect_in_open_transaction(
    c: &mut PgConnection,
    keys: Option<Vec<String>>,
    keep: Option<Vec<String>>,
    effect: Effect,
) -> Result<Vec<String>, ReportError> {
    if c.protocol_transaction_is_idle().await.map_err(Error::Sql)? {
        return Err(Error::Unsupported.into());
    }
    let rows = collector_rows(c, keys, keep).await?;
    delete_collector_rows(c, &rows, effect).await?;
    Ok(rows
        .into_iter()
        .map(|v| v.1.expect("bounded key checked"))
        .collect())
}
async fn collector_rows(
    c: &mut PgConnection,
    keys: Option<Vec<String>>,
    keep: Option<Vec<String>>,
) -> Result<Vec<(i64, Option<String>)>, ReportError> {
    let rows: Vec<(i64, Option<String>)> = if let Some(keys) = keys {
        sqlx::query_as("SELECT id::bigint,CASE WHEN octet_length(notification_key)<=1020 THEN notification_key ELSE NULL END FROM core_systemnotification WHERE notification_key=ANY($1) ORDER BY priority DESC,created_at DESC LIMIT 129").bind(keys).fetch_all(&mut *c).await.map_err(Error::Sql)?
    } else {
        sqlx::query_as(&format!("SELECT id::bigint,CASE WHEN octet_length(notification_key)<=1020 THEN notification_key ELSE NULL END FROM core_systemnotification WHERE {FILTER} AND ($1::text[] IS NULL OR NOT(notification_key=ANY($1))) ORDER BY priority DESC,created_at DESC LIMIT 129")).bind(keep).fetch_all(&mut *c).await.map_err(Error::Sql)?
    };
    if rows.len() > 128
        || rows
            .iter()
            .any(|v| v.1.as_ref().is_none_or(|k| k.chars().count() > 255))
    {
        return Err(Error::Unsupported.into());
    }
    Ok(rows)
}
async fn delete_collector_rows(
    c: &mut PgConnection,
    rows: &[(i64, Option<String>)],
    effect: Effect,
) -> Result<(), ReportError> {
    let ids: Vec<i64> = rows.iter().map(|v| v.0).collect();
    sqlx::query_scalar::<_, i64>(
        "SELECT id::bigint FROM core_systemnotification WHERE id=ANY($1) FOR UPDATE",
    )
    .bind(&ids)
    .fetch_all(&mut *c)
    .await
    .map_err(Error::Sql)?;
    let related: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core_notificationdismissal WHERE notification_id=ANY($1)",
    )
    .bind(&ids)
    .fetch_one(&mut *c)
    .await
    .map_err(Error::Sql)?;
    if related > 1024 {
        return Err(Error::Unsupported.into());
    }
    delete_dismissal_rows(c, &ids, effect).await?;
    sqlx::query("DELETE FROM core_systemnotification WHERE id=ANY($1)")
        .bind(&ids)
        .execute(&mut *c)
        .await
        .map_err(|error| ReportError::effect(error, effect))?;
    Ok(())
}

async fn delete_dismissal_rows(
    c: &mut PgConnection,
    ids: &[i64],
    effect: Effect,
) -> Result<(), ReportError> {
    sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id=ANY($1)")
        .bind(ids)
        .execute(&mut *c)
        .await
        .map_err(|error| ReportError::effect(error, effect))?;
    Ok(())
}

/// The bounded standalone deactivation holds the account and task first.
/// Legacy deletes can own notice rows first, so a held row here rejects
/// the transaction. Deletion and event capture use the same locked rows.
pub(crate) async fn collect_deactivation_in_transaction(
    c: &mut PgConnection,
    keys: Vec<String>,
) -> Result<Vec<String>, ReportError> {
    if c.protocol_transaction_is_idle().await.map_err(Error::Sql)? {
        return Err(Error::Unsupported.into());
    }
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT id::bigint FROM core_systemnotification WHERE notification_key=ANY($1) \
         ORDER BY id LIMIT 129 FOR UPDATE NOWAIT",
    )
    .bind(keys)
    .fetch_all(&mut *c)
    .await
    .map_err(Error::Sql)?;
    if ids.len() > 128 {
        return Err(Error::Unsupported.into());
    }
    let dismissals: Vec<i64> = sqlx::query_scalar(
        "SELECT id::bigint FROM core_notificationdismissal WHERE notification_id=ANY($1) \
         ORDER BY id LIMIT 1025 FOR UPDATE NOWAIT",
    )
    .bind(&ids)
    .fetch_all(&mut *c)
    .await
    .map_err(Error::Sql)?;
    if dismissals.len() > 1024 {
        return Err(Error::Unsupported.into());
    }
    delete_dismissal_rows(c, &ids, Effect::OppositeCollector).await?;
    sqlx::query_scalar(
        "DELETE FROM core_systemnotification WHERE id=ANY($1) RETURNING notification_key",
    )
    .bind(&ids)
    .fetch_all(&mut *c)
    .await
    .map_err(|error| ReportError::effect(error, Effect::OppositeCollector))
}
/// Exact legacy notification upsert on the caller's session. Legacy caller
/// retains its title-domain guard; signal caller lets actual PG reject an
/// overlong model title, as the caught Source signal does.
pub(crate) async fn upsert_on_connection(
    connection: &mut PgConnection,
    p: &Profile,
    now: DateTime<Utc>,
    expired: bool,
    key: &str,
) -> Result<Value, ReportError> {
    let when = if expired {
        "has expired".to_string()
    } else {
        let days = (p.4 - now).num_days();
        format!(
            "expires {}",
            match days {
                0 => "today".to_string(),
                1 => "in 1 day".to_string(),
                n => format!("in {n} days"),
            }
        )
    };
    let title = format!(
        "Account {}: {}",
        if expired { "Expired" } else { "Expiring" },
        p.2
    );
    let expiration_text = format!(
        "{}-{:02}-{:02} {:02}:{:02} UTC",
        p.4.year(),
        p.4.month(),
        p.4.day(),
        p.4.hour(),
        p.4.minute()
    );
    let message = format!(
        "Profile \"{}\" on M3U account \"{}\" {when} ({} {expiration_text}).",
        p.2,
        p.3,
        if expired { "expired" } else { "expires" }
    );
    let action = json!({"profile_id":p.0,"account_id":p.1,"account_name":p.3,"profile_name":p.2,"exp_date":iso(p.4)});
    let c = connection;
    async{
            let mut tx=c.begin().await.map_err(Error::Sql)?;
            let written=async{
                let existing:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemnotification WHERE notification_key=$1 FOR UPDATE").bind(key).fetch_optional(&mut *tx).await.map_err(Error::Sql)?;
                let row:(i64,DateTime<Utc>)=if let Some(id)=existing{
                    sqlx::query_as("UPDATE core_systemnotification SET notification_type='warning',priority=$2,title=$3,message=$4,action_data=$5,is_active=true,admin_only=true,updated_at=$6 WHERE id=$1 RETURNING id::bigint,created_at").bind(id).bind(if expired{"high"}else{"normal"}).bind(&title).bind(&message).bind(&action).bind(now).fetch_one(&mut *tx).await.map_err(|error|ReportError::effect(error,Effect::ProfileUpsert))?
                }else{
                    sqlx::query_as("INSERT INTO core_systemnotification(notification_key,notification_type,priority,title,message,action_data,is_active,admin_only,source,created_at,updated_at,expires_at) VALUES($1,'warning',$2,$3,$4,$5,true,true,'system',$6,$6,NULL) RETURNING id::bigint,created_at").bind(key).bind(if expired{"high"}else{"normal"}).bind(&title).bind(&message).bind(&action).bind(now).fetch_one(&mut *tx).await.map_err(|error|ReportError::effect(error,Effect::ProfileUpsert))?
                };Ok::<_,ReportError>(json!({"id":row.0,"notification_key":key,"notification_type":"warning","priority":if expired{"high"}else{"normal"},"title":title,"message":message,"action_data":action,"is_active":true,"admin_only":true,"created_at":iso(row.1)}))
            }.await;
            match written{Ok(row)=>{tx.commit().await.map_err(Error::Sql)?;Ok(row)},Err(e)=>{tx.rollback().await.map_err(Error::Sql)?;Err(e)}}
        }.await
}
/// The returned inner error has actually released the retained event owner.
/// Signal helpers catch all settled send errors; legacy bulk callers preserve
/// their narrower error policy. Neither returns while physical work survives.
pub(crate) async fn send_observed(
    sink: Arc<dyn UpdateEventSink>,
    event: Value,
    owner: Arc<dyn Send + Sync>,
) -> Result<Result<(), UpdateEventError>, Error> {
    let (done, settled) = oneshot::channel();
    let receipt = Arc::new(Receipt {
        done: Some(done),
        _owner: owner,
    });
    let sent = sink.send_retained("updates", event, receipt.clone()).await;
    drop(receipt);
    settled.await.map_err(|_| Error::Event)?;
    Ok(sent)
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProfilePlan {
    pub key: Option<String>,
    pub remove: Vec<String>,
    pub expired: bool,
}
pub(crate) fn profile_plan(
    id: i64,
    expiration: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> ProfilePlan {
    let warning = format!("xc-exp-warning-{id}");
    let expired = format!("xc-exp-expired-{id}");
    match expiration {
        Some(value) if value <= now => ProfilePlan {
            key: Some(expired),
            remove: vec![warning],
            expired: true,
        },
        Some(value) if value <= now + chrono::Duration::days(7) => ProfilePlan {
            key: Some(warning),
            remove: vec![expired],
            expired: false,
        },
        _ => ProfilePlan {
            key: None,
            remove: vec![warning, expired],
            expired: false,
        },
    }
}

#[cfg(test)]
mod shared_profile_plan_tests {
    use super::*;
    #[test]
    fn source_signal_none_expired_warning_and_seven_day_boundary() {
        let now = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for expiration in [
            None,
            Some(now + chrono::Duration::days(7) + chrono::Duration::microseconds(1)),
        ] {
            assert_eq!(
                profile_plan(42, expiration, now),
                ProfilePlan {
                    key: None,
                    remove: vec!["xc-exp-warning-42".into(), "xc-exp-expired-42".into()],
                    expired: false
                }
            );
        }
        assert_eq!(
            profile_plan(42, Some(now), now),
            ProfilePlan {
                key: Some("xc-exp-expired-42".into()),
                remove: vec!["xc-exp-warning-42".into()],
                expired: true
            }
        );
        assert_eq!(
            profile_plan(42, Some(now + chrono::Duration::days(7)), now),
            ProfilePlan {
                key: Some("xc-exp-warning-42".into()),
                remove: vec!["xc-exp-expired-42".into()],
                expired: false
            }
        );
    }
}
