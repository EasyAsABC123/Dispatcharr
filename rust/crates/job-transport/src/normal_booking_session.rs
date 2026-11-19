//! Booking save signals on the task's existing physical SQL session.
//! Original Dispatcharr/Django/Celery authors retain all application credit.
use super::*;
use sqlx::PgConnection;
impl NormalRecordingPublisher {
    /// Called only inside an actual supervised task owning this connection.
    /// No pool checkout or detached borrowed future can deadlock its HOLD cursor.
    pub async fn schedule_booking_child_on(
        &self,
        recording: i64,
        parent: Uuid,
        revoked_before_date_save: bool,
        connection: &mut PgConnection,
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
        let persisted = read(connection, recording).await?;
        // Source pre_save clears only the in-memory instance after revocation.
        // save(update_fields=[start_time,end_time]) preserves the stored task_id
        // until post_save actually assigns a replacement schedule/publication.
        let row = save_signal_row(persisted, revoked_before_date_save);
        let now = Utc::now();
        let task = if !needs_booking(&row, now) {
            BookingTask::Unchanged {
                task_key: row.task_key.clone(),
            }
        } else if row.start > now {
            future_on(connection, &row).await?
        } else {
            let retained = self
                .inner
                .registry
                .inner
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Capacity)?;
            dispatcharr_store::recording_beat_cleanup::Store::normal(self.inner.pool.clone())
                .delete_by_name_on(
                    connection,
                    &format!("dvr-recording-{recording}"),
                    owner.clone(),
                )
                .await
                .map_err(|error| Error::Cleanup(Box::new(error)))?;
            let key = format!("dvr-now-{recording}-{}", Uuid::new_v4().simple());
            let bytes = booking_lineage(
                immediate_frame(&row, &key, reply_oid(&self.inner)?)?,
                Some(parent),
            )?;
            let message = crate::normal_named_recording::parse_head(&bytes)
                .map_err(|_| Error::Unsupported("immediate booking envelope"))?;
            let reply = self
                .inner
                .jobs
                .publish_named_recording(&message, owner.clone())
                .await
                .map_err(Error::Transport)?;
            self.inner
                .named
                .record_actual(message, row.start, row.end, retained, reply)
                .map_err(|_| Error::Unsupported("immediate booking receipt"))?;
            let updated = sqlx::query(
                "UPDATE dispatcharr_channels_recording SET task_id=$1 WHERE id=$2::bigint",
            )
            .bind(&key)
            .bind(recording)
            .execute(&mut *connection)
            .await
            .map_err(database)?;
            if updated.rows_affected() != 1 {
                return Err(Error::Unsupported("booking row removed after publication"));
            }
            BookingTask::Immediate { task_key: key }
        };
        let artwork = if row.artwork {
            let publisher = self
                .inner
                .booking_artwork
                .lock()
                .map_err(|_| Error::Supervisor)?
                .clone()
                .ok_or(Error::Unsupported(
                    "booking root artwork publisher not composed",
                ))?;
            let bytes = booking_lineage(
                artwork_frame(recording, reply_oid(&self.inner)?)?,
                Some(parent),
            )?;
            Some(
                publisher
                    .publish_when_ready(&bytes, owner, &self.inner.booking_backpressure_closed)
                    .await
                    .map_err(Error::Transport)?
                    .task_id,
            )
        } else {
            None
        };
        Ok(BookingPublication { task, artwork })
    }
}
async fn read(connection: &mut PgConnection, recording: i64) -> Result<BookingRow, Error> {
    if recording <= 0 {
        return Err(Error::Unsupported("booking recording identity"));
    }
    let row=sqlx::query(r#"SELECT r.channel_id::bigint,r.start_time,r.end_time,CASE WHEN COALESCE(octet_length(r.task_id),0)<=255 THEN r.task_id END AS task_key,COALESCE(octet_length(r.task_id),0)<=255 AS fits,((r.custom_properties IS NULL OR jsonb_typeof(r.custom_properties)='object' OR r.custom_properties IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'""'::jsonb,'[]'::jsonb,'{}'::jsonb)) AND NOT COALESCE(r.custom_properties->'status' IN ('"recording"'::jsonb,'"completed"'::jsonb,'"stopped"'::jsonb,'"interrupted"'::jsonb),false)) AS artwork FROM dispatcharr_channels_recording r JOIN dispatcharr_channels_channel c ON c.id=r.channel_id WHERE r.id=$1::bigint"#).bind(recording).fetch_optional(&mut*connection).await.map_err(database)?.ok_or(Error::Unsupported("booking row missing"))?;
    if !row.try_get::<bool, _>("fits").map_err(database)? {
        return Err(Error::Capacity);
    }
    let artwork: bool = row.try_get("artwork").map_err(database)?;
    let row = BookingRow {
        recording,
        channel: row.try_get("channel_id").map_err(database)?,
        start: row.try_get("start_time").map_err(database)?,
        end: row.try_get("end_time").map_err(database)?,
        task_key: row.try_get("task_key").map_err(database)?,
        artwork,
        recurring_times: None,
    };
    if row.end <= row.start {
        return Err(Error::Unsupported("booking recording window"));
    }
    Ok(row)
}
async fn future_on(connection: &mut PgConnection, row: &BookingRow) -> Result<BookingTask, Error> {
    let clocked: Vec<i64> = sqlx::query_scalar(
        "SELECT id::bigint FROM django_celery_beat_clockedschedule WHERE clocked_time=$1 LIMIT 2",
    )
    .bind(row.start)
    .fetch_all(&mut *connection)
    .await
    .map_err(database)?;
    if clocked.len() > 1 {
        return Err(Error::Unsupported("multiple booking clocked schedules"));
    }
    let mut tx = connection.begin().await.map_err(database)?;
    let name = format!("dvr-recording-{}", row.recording);
    let result=async{
        let clocked=match clocked.first(){Some(value)=>*value,None=>{let id=sqlx::query_scalar::<_,i64>("INSERT INTO django_celery_beat_clockedschedule (clocked_time) VALUES ($1) RETURNING id::bigint").bind(row.start).fetch_one(&mut*tx).await.map_err(database)?;marker(&mut tx).await?;id}};
        let args=serde_json::to_string(&json!([row.recording,row.channel,python_datetime(row.start,false),python_datetime(row.end,false)])).map_err(|_|Error::Unsupported("booking args"))?;
        let id=sqlx::query_scalar::<_,i64>("INSERT INTO django_celery_beat_periodictask (name,task,interval_id,crontab_id,solar_id,clocked_id,args,kwargs,queue,exchange,routing_key,headers,priority,expires,expire_seconds,one_off,start_time,enabled,last_run_at,total_run_count,date_changed,description) VALUES ($1,'apps.channels.tasks.run_recording',NULL,NULL,NULL,$2,$3,'{}',NULL,NULL,NULL,'{}',NULL,NULL,NULL,true,NULL,true,NULL,0,$4,'') ON CONFLICT (name) DO UPDATE SET task=EXCLUDED.task,clocked_id=EXCLUDED.clocked_id,args=EXCLUDED.args,one_off=true,enabled=true,interval_id=NULL,crontab_id=NULL,solar_id=NULL,date_changed=EXCLUDED.date_changed RETURNING id::bigint").bind(&name).bind(clocked).bind(args).bind(Utc::now()).fetch_one(&mut*tx).await.map_err(database)?;
        marker(&mut tx).await?;Ok::<_,Error>(id)
    }.await;
    let periodic_id = match result {
        Ok(id) => {
            tx.commit().await.map_err(database)?;
            id
        }
        Err(error) => {
            tx.rollback().await.map_err(database)?;
            return Err(error);
        }
    };
    let updated =
        sqlx::query("UPDATE dispatcharr_channels_recording SET task_id=$1 WHERE id=$2::bigint")
            .bind(&name)
            .bind(row.recording)
            .execute(&mut *connection)
            .await
            .map_err(database)?;
    if updated.rows_affected() != 1 {
        return Err(Error::Unsupported(
            "booking row removed before task assignment",
        ));
    }
    Ok(BookingTask::Periodic { name, periodic_id })
}

fn save_signal_row(mut row: BookingRow, revoked_before_date_save: bool) -> BookingRow {
    if revoked_before_date_save {
        row.task_key = None;
    }
    row
}
fn needs_booking(row: &BookingRow, now: DateTime<Utc>) -> bool {
    row.task_key.as_ref().is_none_or(|key| key.is_empty())
        && (row.start > now - chrono::Duration::seconds(1) || row.end > now)
}
#[cfg(test)]
mod save_signal_tests {
    use super::*;
    fn row(now: DateTime<Utc>, start: i64, end: i64) -> BookingRow {
        BookingRow {
            recording: 41,
            channel: 9,
            start: now + chrono::Duration::seconds(start),
            end: now + chrono::Duration::seconds(end),
            task_key: Some("dvr-recording-41".into()),
            artwork: true,
            recurring_times: None,
        }
    }
    #[test]
    fn offset_date_only_save_preserves_persisted_key_when_shifted_entirely_past() {
        let now = Utc::now();
        let persisted = row(now, -600, -300);
        let instance = save_signal_row(persisted.clone(), true);
        assert!(!needs_booking(&instance, now));
        assert!(instance.artwork, "Source still dispatches eligible artwork");
        assert_eq!(persisted.task_key.as_deref(), Some("dvr-recording-41"));
        assert_eq!(instance.task_key, None);
    }
    #[test]
    fn offset_date_only_save_rebooks_future_and_running_windows_after_revoke_only() {
        let now = Utc::now();
        for (start, end) in [(600, 1200), (-600, 300)] {
            let persisted = row(now, start, end);
            assert!(!needs_booking(
                &save_signal_row(persisted.clone(), false),
                now
            ));
            let instance = save_signal_row(persisted.clone(), true);
            assert!(needs_booking(&instance, now));
            assert_eq!(instance.start > now, start > 0);
            assert_eq!(persisted.task_key.as_deref(), Some("dvr-recording-41"));
        }
    }
}
