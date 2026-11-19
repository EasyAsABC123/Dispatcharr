//! Source booking signal boundary after an actual committed Recording mutation.
//! Original Dispatcharr/Celery/django-celery-beat authors retain all credit.
//! Inputs identify rows, never caller-defined frames/times/parent capabilities.
use super::*;
#[derive(Debug)]
pub enum BookingTask {
    Unchanged { task_key: Option<String> },
    Periodic { name: String, periodic_id: i64 },
    Immediate { task_key: String },
}
#[derive(Debug)]
pub struct BookingPublication {
    pub task: BookingTask,
    pub artwork: Option<Uuid>,
}
#[derive(Clone)]
struct BookingRow {
    recording: i64,
    channel: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    task_key: Option<String>,
    artwork: bool,
    recurring_times: Option<(String, String)>,
}
impl BookingRow {
    fn arguments(&self) -> (String, String) {
        self.recurring_times.clone().unwrap_or_else(|| {
            (
                python_datetime(self.start, false),
                python_datetime(self.end, false),
            )
        })
    }
}

impl NormalRecordingPublisher {
    /// Reread the real committed row, schedule future Beat or publish a unique
    /// immediate named root, then persist the actual resulting task_id. Physical
    /// SQL/broker work retains this caller even after HTTP cancellation.
    pub async fn schedule_booking(
        &self,
        recording: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<BookingPublication, Error> {
        self.schedule_booking_context(recording, None, false, caller)
            .await
    }
    /// The recurring planner stores Source's local ISO values in program JSON.
    /// Retain those offsets in Beat arguments after verifying the persisted UTC instants.
    pub async fn schedule_recurring_booking(
        &self,
        recording: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<BookingPublication, Error> {
        self.schedule_booking_context(recording, None, true, caller)
            .await
    }
    pub async fn schedule_booking_child(
        &self,
        recording: i64,
        parent: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<BookingPublication, Error> {
        self.schedule_booking_context(recording, Some(parent), false, caller)
            .await
    }
    async fn schedule_booking_context(
        &self,
        recording: i64,
        parent: Option<Uuid>,
        recurring: bool,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<BookingPublication, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let owner = Arc::new(Owner {
            _slot: slot,
            _caller: caller,
            _producer: self.inner.clone(),
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let row = booking_row(inner.pool.clone(), recording, recurring, owner.clone()).await?;
            let now = Utc::now();
            let task = if row.task_key.as_ref().is_some_and(|key| !key.is_empty())
                || !(row.start > now - chrono::Duration::seconds(1) || row.end > now)
            {
                BookingTask::Unchanged {
                    task_key: row.task_key.clone(),
                }
            } else if row.start > now {
                future(inner.pool.clone(), row.clone(), owner.clone()).await?
            } else {
                // Source immediate branch removes a leftover Beat schedule.
                let retained = inner
                    .registry
                    .inner
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Capacity)?;
                drop_future(inner.pool.clone(), recording, owner.clone()).await?;
                let key = format!("dvr-now-{recording}-{}", Uuid::new_v4().simple());
                let bytes =
                    booking_lineage(immediate_frame(&row, &key, reply_oid(&inner)?)?, parent)?;
                let message = crate::normal_named_recording::parse_head(&bytes)
                    .map_err(|_| Error::Unsupported("immediate booking envelope"))?;
                let reply = inner
                    .jobs
                    .publish_named_recording(&message, owner.clone())
                    .await
                    .map_err(Error::Transport)?;
                inner
                    .named
                    .record_actual(message, row.start, row.end, retained, reply)
                    .map_err(|_| Error::Unsupported("immediate booking receipt"))?;
                assign(inner.pool.clone(), recording, key.clone(), owner.clone()).await?;
                BookingTask::Immediate { task_key: key }
            };
            // This is the actual Source post_save branch even when task_id was
            // already present or the capture window ended. Status alone skips
            // eligible artwork, never presence of a previous capture task.
            let artwork = if row.artwork {
                let publisher = inner
                    .booking_artwork
                    .lock()
                    .map_err(|_| Error::Supervisor)?
                    .clone()
                    .ok_or(Error::Unsupported(
                        "booking root artwork publisher not composed",
                    ))?;
                let raw = booking_lineage(artwork_frame(recording, reply_oid(&inner)?)?, parent)?;
                Some(
                    publisher
                        .publish_when_ready(&raw, owner, &inner.booking_backpressure_closed)
                        .await
                        .map_err(Error::Transport)?
                        .task_id,
                )
            } else {
                None
            };
            Ok(BookingPublication { task, artwork })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    /// Cache-authoritative Source DVR group for the HTTP serializer's EPG padding.
    /// A bare SQL setting read is deliberately insufficient. Use the same Store
    /// and Redis URL as Main; all transferred policy operations physically settle.
    pub async fn booking_dvr_settings(
        &self,
        store: dispatcharr_store::Store,
        cache: &str,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Value, Error> {
        if !self.uses_pool(store.pool()) {
            return Err(Error::Unsupported("booking settings pool identity"));
        }
        let cache = cache.to_string();
        tokio::spawn(async move {
            let policy =
                dispatcharr_ingest_epg::cache_policy::CachedSwapPolicy::new(store, Some(cache));
            let (done, settled) = tokio::sync::oneshot::channel();
            let receipt = Arc::new(PolicyReceipt {
                _owner: caller,
                done: Some(done),
            });
            let result = policy
                .retained_configured_dvr_settings(dispatcharr_ingest_epg::SwapPolicyContext {
                    cancelled: Arc::new(AtomicBool::new(false)),
                    retained: receipt.clone(),
                })
                .await;
            drop(receipt);
            settled.await.map_err(|_| Error::Supervisor)?;
            result.map_err(Error::Policy)
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
}
struct PolicyReceipt {
    _owner: Arc<dyn Send + Sync>,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Drop for PolicyReceipt {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
fn immediate_frame(row: &BookingRow, key: &str, reply: Uuid) -> Result<Vec<u8>, Error> {
    let raw = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        Uuid::new_v4(),
        reply,
        Uuid::new_v4(),
        &origin()?,
    )
    .map_err(|_| Error::Unsupported("booking common envelope"))?;
    let mut value: Value =
        serde_json::from_slice(&raw).map_err(|_| Error::Unsupported("booking JSON"))?;
    let (start, end) = row.arguments();
    value["headers"]["task"] = json!("apps.channels.tasks.run_recording");
    value["headers"]["id"] = json!(key);
    value["headers"]["root_id"] = json!(key);
    value["headers"]["parent_id"] = Value::Null;
    value["headers"]["argsrepr"] = json!(format!(
        "[{}, {}, '{start}', '{end}']",
        row.recording, row.channel
    ));
    value["properties"]["correlation_id"] = json!(key);
    value["properties"]["delivery_info"]["routing_key"] = json!("dvr");
    value["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[row.recording,row.channel,start,end],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|Error::Unsupported("booking body"))?));
    serde_json::to_vec(&value).map_err(|_| Error::Unsupported("booking frame"))
}
fn recurring_time(
    properties: &Value,
    field: &str,
    instant: DateTime<Utc>,
) -> Result<String, Error> {
    let value = properties["program"][field]
        .as_str()
        .filter(|raw| raw.len() <= 40 && raw.as_bytes().get(10) == Some(&b'T'))
        .ok_or(Error::Unsupported("recurring booking program clock"))?;
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| Error::Unsupported("recurring booking program clock"))?;
    if parsed.with_timezone(&Utc) != instant {
        return Err(Error::Unsupported(
            "recurring booking persisted clock disagreement",
        ));
    }
    Ok(value.replacen('T', " ", 1))
}

async fn booking_row(
    pool: PgPool,
    recording: i64,
    recurring: bool,
    owner: Arc<dyn Send + Sync>,
) -> Result<BookingRow, Error> {
    if recording <= 0 {
        return Err(Error::Unsupported("booking recording identity"));
    }
    tokio::spawn(async move {let _owner=owner;let mut connection=pool.acquire().await.map_err(database)?.detach();
  let observed=async {let row=sqlx::query("SELECT r.channel_id::bigint,r.start_time,r.end_time,CASE WHEN COALESCE(octet_length(r.task_id),0)<=255 THEN r.task_id END AS task_key,CASE WHEN r.custom_properties IS NULL THEN '{}'::jsonb WHEN octet_length(r.custom_properties::text)<=8192 THEN r.custom_properties END AS properties,(COALESCE(octet_length(r.task_id),0)<=255 AND (r.custom_properties IS NULL OR octet_length(r.custom_properties::text)<=8192)) AS fits FROM dispatcharr_channels_recording r JOIN dispatcharr_channels_channel c ON c.id=r.channel_id WHERE r.id=$1").bind(recording).fetch_optional(&mut connection).await.map_err(database)?.ok_or(Error::Unsupported("booking row missing"))?;
   if !row.try_get::<bool,_>("fits").map_err(database)?{return Err(Error::Capacity);}
   let properties=row.try_get::<Option<Value>,_>("properties").map_err(database)?.ok_or(Error::Capacity)?;
   let object=properties.as_object();
   let truthy=match &properties{Value::Null=>false,Value::Bool(v)=>*v,Value::String(v)=>!v.is_empty(),Value::Array(v)=>!v.is_empty(),Value::Object(v)=>!v.is_empty(),Value::Number(v)=>v.as_f64()!=Some(0.0)};
   let artwork=(!truthy||object.is_some())&&!object.and_then(|v|v.get("status")).and_then(Value::as_str).is_some_and(|status|matches!(status,"recording"|"completed"|"stopped"|"interrupted"));
   let start = row.try_get("start_time").map_err(database)?;
   let end = row.try_get("end_time").map_err(database)?;
   if end <= start {return Err(Error::Unsupported("booking recording window"));}
   let recurring_times = if recurring {
       if properties["rule"]["type"] != "recurring"
        || properties["rule"]["id"].as_i64().is_none_or(|id| id <= 0)
    {
           return Err(Error::Unsupported("recurring booking persisted rule identity"));
       }
       Some((recurring_time(&properties, "start_time", start)?, recurring_time(&properties, "end_time", end)?))
   } else {
       None
   };
   Ok(BookingRow{recording,channel:row.try_get("channel_id").map_err(database)?, start, end,
       task_key:row.try_get("task_key").map_err(database)?, artwork, recurring_times})
  }.await;
  let ready=connection.ping().await.err().map(Box::new);let close=connection.close().await.err().map(Box::new);settle(observed,ready,close)
 }).await.map_err(|_|Error::Supervisor)?
}
fn settle<T>(
    observed: Result<T, Error>,
    ready: Option<Box<sqlx::Error>>,
    close: Option<Box<sqlx::Error>>,
) -> Result<T, Error> {
    match observed {
        Err(Error::Database { primary, .. }) => Err(Error::Database {
            primary,
            ready,
            close,
        }),
        other => {
            if ready.is_some() || close.is_some() {
                Err(Error::Database {
                    primary: None,
                    ready,
                    close,
                })
            } else {
                other
            }
        }
    }
}
async fn marker(connection: &mut sqlx::PgConnection) -> Result<(), Error> {
    sqlx::query("INSERT INTO django_celery_beat_periodictasks (ident,last_update) VALUES (1,$1) ON CONFLICT (ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(connection).await.map_err(database)?;
    Ok(())
}
async fn future(
    pool: PgPool,
    row: BookingRow,
    owner: Arc<dyn Send + Sync>,
) -> Result<BookingTask, Error> {
    tokio::spawn(async move {let _owner=owner;let mut connection=pool.acquire().await.map_err(database)?.detach();
  let observed=async {
   let clocked:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_clockedschedule WHERE clocked_time=$1 LIMIT 2").bind(row.start).fetch_all(&mut connection).await.map_err(database)?;if clocked.len()>1{return Err(Error::Unsupported("multiple booking clocked schedules"));}
   let mut tx=connection.begin().await.map_err(database)?;let name=format!("dvr-recording-{}",row.recording);
   let result=async {
    let clocked=match clocked.first(){Some(value)=>*value,None=>{let id=sqlx::query_scalar::<_,i64>("INSERT INTO django_celery_beat_clockedschedule (clocked_time) VALUES ($1) RETURNING id::bigint").bind(row.start).fetch_one(&mut *tx).await.map_err(database)?;marker(&mut tx).await?;id}};
    let (start, end) = row.arguments();
    let args = if row.recurring_times.is_some() {
        format!("[{}, {}, {}, {}]", row.recording, row.channel, json!(start), json!(end))
    } else {
        serde_json::to_string(&json!([row.recording, row.channel, start, end]))
            .map_err(|_| Error::Unsupported("booking args"))?
    };
    let id=sqlx::query_scalar::<_,i64>("INSERT INTO django_celery_beat_periodictask (name,task,interval_id,crontab_id,solar_id,clocked_id,args,kwargs,queue,exchange,routing_key,headers,priority,expires,expire_seconds,one_off,start_time,enabled,last_run_at,total_run_count,date_changed,description) VALUES ($1,'apps.channels.tasks.run_recording',NULL,NULL,NULL,$2,$3,'{}',NULL,NULL,NULL,'{}',NULL,NULL,NULL,true,NULL,true,NULL,0,$4,'') ON CONFLICT (name) DO UPDATE SET task=EXCLUDED.task,clocked_id=EXCLUDED.clocked_id,args=EXCLUDED.args,one_off=true,enabled=true,interval_id=NULL,crontab_id=NULL,solar_id=NULL,date_changed=EXCLUDED.date_changed RETURNING id::bigint").bind(&name).bind(clocked).bind(args).bind(Utc::now()).fetch_one(&mut *tx).await.map_err(database)?;
    marker(&mut tx).await?;Ok::<i64,Error>(id)
   }.await;
   let periodic_id=match result{Ok(id)=>{tx.commit().await.map_err(database)?;id},Err(error)=>{if let Err(rollback)=tx.rollback().await{return Err(database(rollback));}return Err(error)}};
   let updated=sqlx::query("UPDATE dispatcharr_channels_recording SET task_id=$1 WHERE id=$2").bind(&name).bind(row.recording).execute(&mut connection).await.map_err(database)?;
   if updated.rows_affected()!=1{return Err(Error::Unsupported("booking row removed before task assignment"));}Ok(BookingTask::Periodic{name,periodic_id})
  }.await;
  let ready=connection.ping().await.err().map(Box::new);let close=connection.close().await.err().map(Box::new);settle(observed,ready,close)
 }).await.map_err(|_|Error::Supervisor)?
}
async fn drop_future(
    pool: PgPool,
    recording: i64,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), Error> {
    tokio::spawn(async move {let _owner=owner;let mut connection=pool.acquire().await.map_err(database)?.detach();let observed=async{
  let removed:Vec<Option<i64>>=sqlx::query_scalar("DELETE FROM django_celery_beat_periodictask WHERE name=$1 RETURNING clocked_id::bigint").bind(format!("dvr-recording-{recording}")).fetch_all(&mut connection).await.map_err(database)?;
  if !removed.is_empty(){marker(&mut connection).await?;}
  for clocked in removed.into_iter().flatten(){let removed=sqlx::query("DELETE FROM django_celery_beat_clockedschedule WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE clocked_id=$1)").bind(clocked).execute(&mut connection).await.map_err(database)?.rows_affected();if removed>0{marker(&mut connection).await?;}}
  Ok(())}.await;
  let ready=connection.ping().await.err().map(Box::new);let close=connection.close().await.err().map(Box::new);settle(observed,ready,close)
 }).await.map_err(|_|Error::Supervisor)?
}
async fn assign(
    pool: PgPool,
    recording: i64,
    key: String,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), Error> {
    tokio::spawn(async move {
        let _owner = owner;
        let mut connection = pool.acquire().await.map_err(database)?.detach();
        let observed =
            sqlx::query("UPDATE dispatcharr_channels_recording SET task_id=$1 WHERE id=$2")
                .bind(key)
                .bind(recording)
                .execute(&mut connection)
                .await
                .map_err(database)
                .and_then(|value| {
                    if value.rows_affected() == 1 {
                        Ok(())
                    } else {
                        Err(Error::Unsupported("booking row removed after publication"))
                    }
                });
        let ready = connection.ping().await.err().map(Box::new);
        let close = connection.close().await.err().map(Box::new);
        settle(observed, ready, close)
    })
    .await
    .map_err(|_| Error::Supervisor)?
}

fn artwork_frame(recording: i64, reply: Uuid) -> Result<Vec<u8>, Error> {
    let raw = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        Uuid::new_v4(),
        reply,
        Uuid::new_v4(),
        &origin()?,
    )
    .map_err(|_| Error::Unsupported("artwork booking envelope"))?;
    let mut frame: Value =
        serde_json::from_slice(&raw).map_err(|_| Error::Unsupported("artwork booking JSON"))?;
    frame["headers"]["task"] = json!("apps.channels.tasks.prefetch_recording_artwork");
    frame["headers"]["argsrepr"] = json!(format!("[{recording}]"));
    frame["headers"]["eta"] = json!((Utc::now() + chrono::Duration::seconds(1))
        .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
        .to_string());
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[recording],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| Error::Unsupported("artwork booking body"))?
    ));
    serde_json::to_vec(&frame).map_err(|_| Error::Unsupported("artwork booking frame"))
}

impl NormalRecordingPublisher {
    /// Source cancelled recording_end audit. Read only the actual system group
    /// through its retained cache-authoritative getter; no unrelated DVR/stream
    /// cache reads or fixture max-event value are introduced.
    #[allow(clippy::too_many_arguments)]
    pub async fn booking_cancelled_event(
        &self,
        store: dispatcharr_store::Store,
        cache: &str,
        details: Value,
        channel: Uuid,
        name: String,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.uses_pool(store.pool()) {
            return Err(Error::Unsupported("booking audit pool identity"));
        }
        // Active Connect cancellation has no actual HTTP hook context yet.
        // Refuse before cache fills, audit/status writes, revokes or deletion;
        // no-Connect cleanup remains available even with a populated plugin dir.
        Arc::new(dispatcharr_store::recording_capture::CaptureStore::normal(
            store.pool().clone(),
        ))
        .require_unbound_event_domain(
            dispatcharr_store::recording_capture::NormalEvent::Ended,
            caller.clone(),
        )
        .await
        .map_err(|error| Error::Audit(Box::new(error)))?;
        let cache = cache.to_owned();
        tokio::spawn(async move {
            let settings = crate::recording_settings::Settings::new(store.clone(), cache);
            let system = settings
                .normal_effect(true, caller.clone())
                .await
                .map_err(Error::Settings)?;
            let groups = crate::recording_normal::EffectiveGroups {
                system,
                dvr: Value::Null,
                stream: Value::Null,
            };
            let maximum = crate::recording_normal_policy::max_events(&groups)
                .map_err(|_| Error::Unsupported("booking audit maximum policy"))?;
            Arc::new(dispatcharr_store::recording_capture::CaptureStore::normal(
                store.pool().clone(),
            ))
            .insert_runtime_event(
                dispatcharr_store::recording_capture::NormalEvent::Ended,
                channel,
                name,
                details,
                maximum,
                caller,
            )
            .await
            .map_err(|error| Error::Audit(Box::new(error)))?;
            Ok(())
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
}

fn booking_lineage(raw: Vec<u8>, parent: Option<Uuid>) -> Result<Vec<u8>, Error> {
    let Some(parent) = parent else {
        return Ok(raw);
    };
    if parent.is_nil() {
        return Err(Error::Unsupported("booking task parent identity"));
    }
    let mut frame: Value =
        serde_json::from_slice(&raw).map_err(|_| Error::Unsupported("booking task frame"))?;
    frame["headers"]["root_id"] = json!(parent.to_string());
    frame["headers"]["parent_id"] = json!(parent.to_string());
    serde_json::to_vec(&frame).map_err(|_| Error::Unsupported("booking task lineage"))
}

#[path = "normal_booking_session.rs"]
mod normal_booking_session;
