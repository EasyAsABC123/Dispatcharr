//! Safe network-reset operations derived from the Original Dispatcharr commands.
//! Original Dispatcharr authors retain full application and algorithm credit.
//! CLI-only additive component; no HTTP router, destructive command or settings import.
use crate::Store;
use dispatcharr_contracts::UpdateEventSink;
use futures_util::TryStreamExt;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, Semaphore};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperatorCommand {
    ResetNetwork,
    CreateSuperuser(crate::operator_create::CreateSuperuser),
    Help(&'static str),
    ResetUser(Option<String>),
}
#[derive(Debug)]
pub enum OperatorError {
    Arguments,
    Command(String),
    UnsupportedCreateInput,
    MissingNetwork,
    UserSaveNoRows,
    MalformedProperties(&'static str),
    Capacity,
    UnsupportedSignal,
    Database(sqlx::Error),
    Cache(redis::RedisError),
    Unavailable,
}
impl From<sqlx::Error> for OperatorError {
    fn from(e: sqlx::Error) -> Self {
        Self::Database(e)
    }
}
impl std::fmt::Display for OperatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Command(message) => write!(f, "CommandError: {message}"),
            Self::UnsupportedCreateInput => f.write_str("Unsupported createsuperuser input domain"),
            Self::Arguments => f.write_str("Unsupported operator arguments"),
            Self::MissingNetwork => f.write_str("Network access setting does not exist"),
            Self::UserSaveNoRows => f.write_str("User save did not affect any rows"),
            Self::MalformedProperties(s) => write!(f, "Invalid allowed_networks container: {s}"),
            Self::Capacity => f.write_str("Operator resource limit exceeded"),
            Self::UnsupportedSignal => f.write_str(
                "Notification condition invalidation needs an unsupported cache fallback",
            ),
            Self::Database(_) => f.write_str("Operator database operation failed"),
            Self::Cache(_) => f.write_str("Operator cache operation failed"),
            Self::Unavailable => f.write_str("Operator unavailable"),
        }
    }
}
impl OperatorCommand {
    /// Deliberately narrow grammar. Django global flags are unsupported; no fake
    /// Python traceback or claim that native help is manage.py help.
    pub fn parse(args: &[String]) -> Result<Self, OperatorError> {
        if args.first().is_some_and(|s| s == "createsuperuser") {
            if args.len() == 2 && args[1] == "--help" {
                return Ok(Self::Help("createsuperuser"));
            }
            return crate::operator_create::CreateSuperuser::parse(&args[1..], |key| {
                std::env::var(key).ok()
            })
            .map(Self::CreateSuperuser);
        }
        match args {
            [name, flag] if flag == "--help" && name == "reset_network_access" => {
                Ok(Self::Help("reset_network_access"))
            }
            [name, flag] if flag == "--help" && name == "reset_user_network" => {
                Ok(Self::Help("reset_user_network"))
            }
            [name] if name == "reset_network_access" => Ok(Self::ResetNetwork),
            [name] if name == "reset_user_network" => Ok(Self::ResetUser(None)),
            [name, flag, user]
                if name == "reset_user_network" && flag == "--user" && user.len() <= 1024 =>
            {
                Ok(Self::ResetUser(Some(user.clone())))
            }
            _ => Err(OperatorError::Arguments),
        }
    }
}
#[derive(Clone)]
pub struct OperatorCommands {
    store: Store,
    redis_url: String,
    events: Option<Arc<dyn UpdateEventSink>>,
    slot: Arc<Semaphore>,
}
impl OperatorCommands {
    pub fn new(store: Store, redis_url: String, events: Option<Arc<dyn UpdateEventSink>>) -> Self {
        Self {
            store,
            redis_url,
            events,
            slot: Arc::new(Semaphore::new(1)),
        }
    }
    /// Whole operation is supervised. Aborting the observer never cancels its SQL,
    /// Redis or event future and never releases the physical admission/caller early.
    pub async fn run(
        &self,
        command: OperatorCommand,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<String, OperatorError> {
        self.supervised(Ok(command), caller).await
    }
    pub async fn run_arguments(
        &self,
        args: &[String],
        caller: Arc<dyn Send + Sync>,
    ) -> Result<String, OperatorError> {
        self.supervised(OperatorCommand::parse(args), caller).await
    }
    async fn supervised(
        &self,
        command: Result<OperatorCommand, OperatorError>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<String, OperatorError> {
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| OperatorError::Capacity)?;
        let own = self.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            // Carry both budgets through retained publication, whose logical
            // deadline may precede the actual physical Redis reply.
            let physical: Arc<dyn Send + Sync> = Arc::new((permit, caller));
            // Original CoreConfig initializes before argument parsing.
            own.sync_notifications(physical.clone()).await;
            let result = match command {
                Ok(command) => own.execute(command, physical.clone()).await,
                Err(error) => Err(error),
            };
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| OperatorError::Unavailable)?
    }
    pub async fn shutdown(&self, deadline: Duration) -> bool {
        self.slot.close();
        tokio::time::timeout(deadline, async {
            while self.slot.available_permits() != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }
    async fn execute(
        &self,
        command: OperatorCommand,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<String, OperatorError> {
        match command {
            OperatorCommand::CreateSuperuser(input) => input.execute(&self.store, caller).await,
            OperatorCommand::Help(name)=>Ok(format!("Native safe operator: {name}\nDjango global flags and help formatting are not emulated.\n")),
            OperatorCommand::ResetUser(username) => {
                let filter = username.filter(|s| !s.is_empty());
                // Original evaluates its queryset before iterating, then saves each user
                // independently. Do not wrap all users in a new atomic transaction.
                let projection="SELECT id::bigint AS id, octet_length(custom_properties::text)::bigint AS property_bytes, CASE WHEN octet_length(custom_properties::text)<=1048576 THEN custom_properties::text ELSE NULL END AS properties_text FROM accounts_user";
                let sql = if filter.is_some() {
                    format!("{projection} WHERE username=$1 LIMIT 4097")
                } else {
                    format!("{projection} LIMIT 4097")
                };
                let mut query = sqlx::query(&sql);
                if let Some(name) = &filter {
                    query = query.bind(name)
                }
                let mut rows = query.fetch(self.store.pool());
                let mut snapshots = Vec::new();
                let mut bytes = 0usize;
                while let Some(row) = rows.try_next().await? {
                    let length = row
                        .try_get::<Option<i64>, _>("property_bytes")?
                        .unwrap_or(0);
                    bytes = bytes.saturating_add(
                        usize::try_from(length).map_err(|_| OperatorError::Capacity)?,
                    );
                    if snapshots.len() == 4096 || length > 1048576 || bytes > 4 * 1024 * 1024 {
                        return Err(OperatorError::Capacity);
                    }
                    let text: Option<String> = row.try_get("properties_text")?;
                    let value = text
                        .map(|v| serde_json::from_str(&v))
                        .transpose()
                        .map_err(|_| OperatorError::Unavailable)?
                        .unwrap_or(Value::Null);
                    snapshots.push((row.try_get::<i64, _>("id")?, value));
                }
                drop(rows);
                let mut count = 0;
                for (id, mut props) in snapshots {
                    if remove_allowed(&mut props)? {
                        let saved=sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2")
                            .bind(props)
                            .bind(id)
                            .execute(self.store.pool())
                            .await?;
                        if saved.rows_affected()!=1 {return Err(OperatorError::UserSaveNoRows)}
                        count += 1;
                    }
                }
                Ok(format!("Reset allowed_networks for {count} user(s).\n"))
            }
            OperatorCommand::ResetNetwork => {
                // Source .get caches name/key, then .save writes that full snapshot.
                // Initial absence differs from a deletion after this successful get.
                let row = sqlx::query(
                    "SELECT id::bigint AS id,key,name FROM core_coresettings WHERE key='network_access'",
                )
                .fetch_optional(self.store.pool())
                .await?
                .ok_or(OperatorError::MissingNetwork)?;
                let id:i64=row.try_get("id")?;
                let key:String=row.try_get("key")?;
                let name:String=row.try_get("name")?;
                let changed=sqlx::query("UPDATE core_coresettings SET key=$1,name=$2,value='{}'::jsonb WHERE id=$3")
                    .bind(&key).bind(&name).bind(id).execute(self.store.pool()).await?;
                if changed.rows_affected()==0 {
                    // Model.save's update-then-insert path after concurrent deletion.
                    // Reuse the cached PK; never rewind or advance its sequence.
                    sqlx::query("INSERT INTO core_coresettings(id,key,name,value) VALUES($1,$2,$3,'{}'::jsonb)")
                        .bind(id).bind(key).bind(name).execute(self.store.pool()).await?;
                }
                self.invalidate_and_sync(caller).await?;
                Ok(String::new())
            }
        }
    }
    async fn connection(&self) -> Result<ConnectionManager, redis::RedisError> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        client
            .get_connection_manager_with_config(
                ConnectionManagerConfig::new()
                    .set_number_of_retries(0)
                    .set_connection_timeout(Duration::from_secs(5)),
            )
            .await
    }
    async fn invalidate_and_sync(&self, caller: Arc<dyn Send + Sync>) -> Result<(), OperatorError> {
        // Connectivity is ignored by Source group invalidation, but settled auth /
        // server errors propagate after the SQL save. No uncertain command replay.
        let connection = match self.connection().await {
            Ok(conn) => Some(conn),
            Err(error) if error.is_io_error() => None,
            Err(error) => return Err(OperatorError::Cache(error)),
        };
        if let Some(mut conn) = connection {
            let version = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| OperatorError::Unavailable)?
                .as_nanos()
                .to_string();
            for cmd in [
                redis::cmd("DEL")
                    .arg(":1:coresettings:group:network_access")
                    .to_owned(),
                redis::cmd("SET")
                    .arg(":1:coresettings:groupver:network_access")
                    .arg(version)
                    .to_owned(),
            ] {
                if let Err(error) = cmd.query_async::<redis::Value>(&mut conn).await {
                    if !error.is_io_error() {
                        return Err(OperatorError::Cache(error));
                    }
                }
            }
            // Source delete_pattern signal is best effort. Bound scanner/retained keys;
            // FLUSHDB fallback is deliberately not exposed by this safe CLI.
            let mut cursor = 0u64;
            let mut keys = 0usize;
            let mut rounds = 0usize;
            loop {
                rounds += 1;
                if rounds > 8192 {
                    return Err(OperatorError::Capacity);
                }
                let result: Result<(u64, Vec<String>), _> = redis::cmd("EVAL")
                    .arg("local r=redis.call('SCAN',ARGV[1],'MATCH',':1:dev_notif_condition_*','COUNT',128);local n=0;if #r[2]>256 then return redis.error_reply('operator key batch exceeds limit') end;for _,k in ipairs(r[2]) do n=n+#k;if #k>1024 or n>131072 then return redis.error_reply('operator key bytes exceed limit') end end;return r")
                    .arg(0)
                    .arg(cursor)
                    .query_async(&mut conn)
                    .await;
                let Ok((next, batch)) = result else {
                    return Err(OperatorError::UnsupportedSignal);
                };
                keys += batch.len();
                if keys > 4096 {
                    return Err(OperatorError::Capacity);
                }
                if !batch.is_empty() {
                    redis::cmd("DEL")
                        .arg(batch)
                        .query_async::<i64>(&mut conn)
                        .await
                        .map_err(|_| OperatorError::UnsupportedSignal)?;
                }
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
        }
        self.sync_notifications(caller).await;
        Ok(())
    }
    async fn sync_notifications(&self, caller: Arc<dyn Send + Sync>) {
        // Existing pinned developer definition. Conditions are evaluated at read time,
        // not during sync. Preserve developer upsert and obsolete-row dismissal cascade.
        let action = json!({"action_url":"/settings#network-access","action_text":"Review Settings","condition":["m3u_epg_network_insecure"],"min_version":null,"max_version":null,"user_level":"admin"});
        let _=async {
            self.save_pinned_notification(action).await?;
            let mut tx=self.store.pool().begin().await?;
            let removal=async {
                sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN (SELECT id FROM core_systemnotification WHERE source='developer' AND notification_key<>'network_security_m3u_epg')").execute(&mut *tx).await?;
                sqlx::query("DELETE FROM core_systemnotification WHERE source='developer' AND notification_key<>'network_security_m3u_epg'").execute(&mut *tx).await?;
                Ok::<(),sqlx::Error>(())
            }.await;
            match removal {Ok(())=>tx.commit().await?,Err(error)=>{let _=tx.rollback().await;return Err(error)}}
            if let Some(events)=&self.events {let _=events.send_retained("updates",json!({"type":"update","data":{"type":"notifications_cleared"}}),caller).await;}
            Ok::<(),sqlx::Error>(())
        }.await;
    }
    async fn save_pinned_notification(&self, action: Value) -> Result<(), sqlx::Error> {
        let mut tx = self.store.pool().begin().await?;
        let saved=async {
            // update_or_create first locks/reads the existing row. A normal update
            // must not attempt INSERT or consume a notification sequence value.
            let mut existing=sqlx::query_as::<_,(i64,chrono::DateTime<chrono::Utc>)>("SELECT id::bigint,created_at FROM core_systemnotification WHERE notification_key='network_security_m3u_epg' FOR UPDATE").fetch_optional(&mut *tx).await?;
            if existing.is_none(){
                let created=chrono::Utc::now();let updated=chrono::Utc::now();
                let inserted=sqlx::query("INSERT INTO core_systemnotification(notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,expires_at,created_at,updated_at) VALUES('network_security_m3u_epg','warning','high','developer','Network Access Security Warning','Your EPG/M3U output is accessible from any network. Consider restricting access to improve security.',$1,true,true,NULL,$2,$3) ON CONFLICT(notification_key) DO NOTHING")
                    .bind(&action).bind(created).bind(updated).execute(&mut *tx).await?;
                if inserted.rows_affected()==0 {
                    // Only an absent-row creation race attempts an INSERT, matching
                    // get_or_create's sequence consumption on its losing create.
                    existing=Some(sqlx::query_as::<_,(i64,chrono::DateTime<chrono::Utc>)>("SELECT id::bigint,created_at FROM core_systemnotification WHERE notification_key='network_security_m3u_epg' FOR UPDATE").fetch_one(&mut *tx).await?);
                }
            }
            if let Some((id,created))=existing {
                let updated=chrono::Utc::now();
                sqlx::query("UPDATE core_systemnotification SET notification_type='warning',priority='high',source='developer',title='Network Access Security Warning',message='Your EPG/M3U output is accessible from any network. Consider restricting access to improve security.',action_data=$1,is_active=true,admin_only=true,expires_at=NULL,created_at=$2,updated_at=$3 WHERE id=$4")
                    .bind(action).bind(created).bind(updated).bind(id).execute(&mut *tx).await?;
            }
            Ok::<(),sqlx::Error>(())
        }.await;
        match saved {
            Ok(()) => tx.commit().await,
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}
fn remove_allowed(value: &mut Value) -> Result<bool, OperatorError> {
    match value {
        Value::Object(props) => Ok(props.remove("allowed_networks").is_some()),
        Value::Null => Ok(false),
        Value::Array(items) if items.is_empty() => Ok(false),
        Value::Array(items) => {
            if items.iter().any(|v| v == "allowed_networks") {
                Err(OperatorError::MalformedProperties("list"))
            } else {
                Ok(false)
            }
        }
        Value::String(s) => {
            if s.contains("allowed_networks") {
                Err(OperatorError::MalformedProperties("str"))
            } else {
                Ok(false)
            }
        }
        Value::Bool(false) => Ok(false),
        Value::Number(n) if n.as_f64() == Some(0.0) => Ok(false),
        Value::Bool(true) => Err(OperatorError::MalformedProperties("bool")),
        Value::Number(_) => Err(OperatorError::MalformedProperties("number")),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_membership() {
        for value in [
            json!(null),
            json!(false),
            json!(0),
            json!([]),
            json!("other"),
            json!(["other"]),
        ] {
            assert!(!remove_allowed(&mut value.clone()).unwrap())
        }
        for value in [
            json!(true),
            json!(2),
            json!(["allowed_networks"]),
            json!("allowed_networks"),
        ] {
            assert!(remove_allowed(&mut value.clone()).is_err())
        }
        let mut v = json!({"allowed_networks":null,"keep":[true,1,null]});
        assert!(remove_allowed(&mut v).unwrap());
        assert_eq!(v, json!({"keep":[true,1,null]}));
    }
    #[test]
    fn grammar() {
        assert_eq!(
            OperatorCommand::parse(&["reset_user_network".into(), "--user".into(), "".into()])
                .unwrap(),
            OperatorCommand::ResetUser(Some("".into()))
        );
        assert!(OperatorCommand::parse(&[
            "reset_network_access".into(),
            "--user".into(),
            "x".into()
        ])
        .is_err());
    }
}
