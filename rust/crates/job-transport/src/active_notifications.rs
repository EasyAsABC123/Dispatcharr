//! Bounded, opt-in active-profile expiration evaluation from Original tasks.py.
//! Original Dispatcharr/Django/Channels authors retain behavior and code credit.
//! One eligible profile, 128 collector rows/1024 dismissals. No concurrency or
//! mixed-live process-local proxy-manager side-effect parity is claimed.
use crate::default_domains::Error;
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use dispatcharr_contracts::{UpdateEventError, UpdateEventSink};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};
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
type Profile = (i64, i64, String, String, DateTime<Utc>);
const FILTER:&str="is_active AND (notification_key LIKE 'xc-exp-warning-%' OR notification_key LIKE 'xc-exp-expired-%')";
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
        let (done, settled) = oneshot::channel();
        let receipt = Arc::new(Receipt {
            done: Some(done),
            _owner: owner,
        });
        let sent = self
            .sink
            .send_retained("updates", event, receipt.clone())
            .await;
        drop(receipt);
        settled.await.map_err(|_| Error::Event)?;
        match sent {
            Ok(()) | Err(UpdateEventError::Unavailable) => Ok(()),
            Err(_) => Err(Error::Event),
        }
    }
    async fn inspect(connection: &mut PgConnection) -> Result<Option<Profile>, Error> {
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL").fetch_one(&mut *connection).await.map_err(Error::Sql)?;
        if count > 1 {
            return Err(Error::Unsupported);
        }
        // LIMIT 2 also rejects growth after the scalar preflight; no invented
        // profile ordering is used for the qualified single-profile domain.
        let rows:Vec<Profile>=sqlx::query_as("SELECT p.id::bigint,a.id::bigint,p.name,a.name,p.exp_date FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL LIMIT 2").fetch_all(&mut *connection).await.map_err(Error::Sql)?;
        if rows.len() > 1 {
            return Err(Error::Unsupported);
        }
        if let Some(row) = rows.first() {
            if row.2.chars().count() > 237
                || row.3.chars().count() > 255
                || !(1970..=9999).contains(&row.4.year())
            {
                return Err(Error::Unsupported);
            }
        }
        let notices:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM core_systemnotification WHERE ({FILTER}) OR notification_key IN (SELECT 'xc-exp-warning-'||p.id::text FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL) OR notification_key IN (SELECT 'xc-exp-expired-'||p.id::text FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.is_active AND p.is_active AND p.exp_date IS NOT NULL)")).fetch_one(&mut *connection).await.map_err(Error::Sql)?;
        // Global bounded dismissal count is conservative; no unbounded related
        // expansion is materialized before the admitted collector transaction.
        let dismissals: i64 = sqlx::query_scalar("SELECT count(*) FROM core_notificationdismissal")
            .fetch_one(&mut *connection)
            .await
            .map_err(Error::Sql)?;
        if notices > 128 || dismissals > 1024 {
            return Err(Error::Unsupported);
        }
        Ok(rows.into_iter().next())
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
        keep: Option<String>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        let mut c = self.pool.acquire().await.map_err(Error::Sql)?;
        let observed=async {
            let rows:Vec<(i64,Option<String>)>=if let Some(keys)=keys {
                sqlx::query_as("SELECT id::bigint,CASE WHEN octet_length(notification_key)<=1020 THEN notification_key ELSE NULL END FROM core_systemnotification WHERE notification_key=ANY($1) ORDER BY priority DESC,created_at DESC LIMIT 129").bind(keys).fetch_all(&mut *c).await.map_err(Error::Sql)?
            }else{
                sqlx::query_as(&format!("SELECT id::bigint,CASE WHEN octet_length(notification_key)<=1020 THEN notification_key ELSE NULL END FROM core_systemnotification WHERE {FILTER} AND ($1::text IS NULL OR notification_key<>$1) ORDER BY priority DESC,created_at DESC LIMIT 129")).bind(keep).fetch_all(&mut *c).await.map_err(Error::Sql)?
            };
            if rows.len()>128 || rows.iter().any(|v|v.1.as_ref().is_none_or(|k|k.chars().count()>255)){return Err(Error::Unsupported);}
            let ids:Vec<i64>=rows.iter().map(|v|v.0).collect();let mut tx=c.begin().await.map_err(Error::Sql)?;
            let deleted=async {
                sqlx::query_scalar::<_,i64>("SELECT id::bigint FROM core_systemnotification WHERE id=ANY($1) FOR UPDATE").bind(&ids).fetch_all(&mut *tx).await.map_err(Error::Sql)?;
                let related:i64=sqlx::query_scalar("SELECT count(*) FROM core_notificationdismissal WHERE notification_id=ANY($1)").bind(&ids).fetch_one(&mut *tx).await.map_err(Error::Sql)?;
                if related>1024{return Err(Error::Unsupported);}
                sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id=ANY($1)").bind(&ids).execute(&mut *tx).await.map_err(Error::Sql)?;
                sqlx::query("DELETE FROM core_systemnotification WHERE id=ANY($1)").bind(&ids).execute(&mut *tx).await.map_err(Error::Sql)?;Ok::<_,Error>(())
            }.await;
            match deleted{Ok(())=>tx.commit().await.map_err(Error::Sql)?,Err(e)=>{tx.rollback().await.map_err(Error::Sql)?;return Err(e);}}
            Ok::<_,Error>(rows.into_iter().map(|v|v.1.expect("bounded key checked")).collect::<Vec<_>>())
        }.await;
        let closed = c.close().await;
        let keys = observed?;
        closed.map_err(Error::Sql)?;
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
    ) -> Result<Value, Error> {
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
        if title.chars().count() > 255 {
            return Err(Error::Unsupported);
        }
        let message = format!(
            "Profile \"{}\" on M3U account \"{}\" {when} ({} {}).",
            p.2,
            p.3,
            if expired { "expired" } else { "expires" },
            p.4.format("%Y-%m-%d %H:%M UTC")
        );
        let action = json!({"profile_id":p.0,"account_id":p.1,"account_name":p.3,"profile_name":p.2,"exp_date":iso(p.4)});
        let mut c = self.pool.acquire().await.map_err(Error::Sql)?;
        let observed=async{
            let mut tx=c.begin().await.map_err(Error::Sql)?;
            let written=async{
                let existing:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemnotification WHERE notification_key=$1 FOR UPDATE").bind(key).fetch_optional(&mut *tx).await.map_err(Error::Sql)?;
                let row:(i64,DateTime<Utc>)=if let Some(id)=existing{
                    sqlx::query_as("UPDATE core_systemnotification SET notification_type='warning',priority=$2,title=$3,message=$4,action_data=$5,is_active=true,admin_only=true,updated_at=$6 WHERE id=$1 RETURNING id::bigint,created_at").bind(id).bind(if expired{"high"}else{"normal"}).bind(&title).bind(&message).bind(&action).bind(now).fetch_one(&mut *tx).await.map_err(Error::Sql)?
                }else{
                    sqlx::query_as("INSERT INTO core_systemnotification(notification_key,notification_type,priority,title,message,action_data,is_active,admin_only,source,created_at,updated_at,expires_at) VALUES($1,'warning',$2,$3,$4,$5,true,true,'system',$6,$6,NULL) RETURNING id::bigint,created_at").bind(key).bind(if expired{"high"}else{"normal"}).bind(&title).bind(&message).bind(&action).bind(now).fetch_one(&mut *tx).await.map_err(Error::Sql)?
                };Ok::<_,Error>(json!({"id":row.0,"notification_key":key,"notification_type":"warning","priority":if expired{"high"}else{"normal"},"title":title,"message":message,"action_data":action,"is_active":true,"admin_only":true,"created_at":iso(row.1)}))
            }.await;
            match written{Ok(row)=>{tx.commit().await.map_err(Error::Sql)?;Ok(row)},Err(e)=>{tx.rollback().await.map_err(Error::Sql)?;Err(e)}}
        }.await;
        let closed = c.close().await;
        let row = observed?;
        closed.map_err(Error::Sql)?;
        Ok(row)
    }
    pub(crate) async fn execute(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Unsupported)?;
        let this = self.clone();
        tokio::spawn(async move{
            let _permit=permit;let _owner=owner.clone();let mut c=this.pool.acquire().await.map_err(Error::Sql)?;
            let observed=Self::inspect(&mut c).await;let closed=c.close().await;let profile=observed?;closed.map_err(Error::Sql)?;
            let mut keep=None;
            if let Some(p)=profile{
                let now=this.clock.now();if !(1970..=9999).contains(&now.year()){return Err(Error::Unsupported);}
                let warning=format!("xc-exp-warning-{}",p.0);let expired=format!("xc-exp-expired-{}",p.0);
                if p.4<=now+chrono::Duration::days(7){
                    let is_expired=p.4<=now;let(key,opposite)=if is_expired{(expired,warning)}else{(warning,expired)};
                    this.collect(Some(vec![opposite]),None,owner.clone()).await?;
                    let row=this.upsert(&p,now,is_expired,&key).await?;
                    this.send(json!({"type":"update","data":{"type":"system_notification","notification":row}}),owner.clone()).await?;keep=Some(key);
                }else{this.collect(Some(vec![warning,expired]),None,owner.clone()).await?;}
            }
            this.collect(None,keep,owner).await
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
