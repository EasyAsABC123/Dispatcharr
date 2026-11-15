//! SQL-backed SD stations, schedule deltas and actual publication work state.
//! Original Dispatcharr/Django/PostgreSQL authors retain algorithm credit.
use crate::{StoreError, TaskSqlSession};
use chrono::NaiveDate;
use serde_json::{json, Value};
#[derive(Debug)]
pub(super) enum Error {
    Store(StoreError),
    Shape,
    Capacity,
}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self::Store(StoreError::Database(e))
    }
}
/// One operation's actual SQL-backed state, on the already admitted session.
/// Temp storage is physically reclaimed with that session even after failure.
pub(super) struct State {
    pub session: TaskSqlSession,
    pub source: i64,
    pub table: String,
    pub first: NaiveDate,
    pub single: Option<i64>,
    pub days: Vec<NaiveDate>,
}
impl State {
    pub async fn begin(
        session: TaskSqlSession,
        source: i64,
        single: Option<i64>,
        first: NaiveDate,
    ) -> Result<Self, Error> {
        let days = (0..20)
            .map(|n| {
                first
                    .checked_add_signed(chrono::Duration::days(n))
                    .ok_or(Error::Shape)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let table = format!("native_sd_{}", uuid::Uuid::new_v4().simple());
        let name = table.clone();
        session.run(move|c|Box::pin(async move{
   sqlx::query(&format!("CREATE TEMP TABLE {name}_mapped(id bigint PRIMARY KEY,station text) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;

   sqlx::query(&format!("CREATE TEMP TABLE {name}_stations(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,station text UNIQUE,epg_id bigint,label text) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
   // Source single lookup is independent of channel mapping; mapped batch uses
   // effective base plus profile override EPG identities, not fixture IDs.
   // Dictionary insertion order follows first observed EPG row; duplicate TVG
   // keys overwrite the selected EPG with the last row without moving position.
   // Keep the complete selection in SQL, never an account-sized Rust Vec.

   sqlx::query(&format!("CREATE TEMP TABLE {name}_days(station text,day date,md5 text,last_modified timestamptz,changed boolean NOT NULL DEFAULT false,PRIMARY KEY(station,day)) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
   sqlx::query(&format!("CREATE TEMP TABLE {name}_airings(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,station text,payload jsonb,program_id text,md5 text) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
   sqlx::query(&format!("CREATE TEMP TABLE {name}_programs(program_id text PRIMARY KEY,md5 text,payload jsonb,failed boolean NOT NULL DEFAULT false,cached jsonb,downloaded boolean NOT NULL DEFAULT false,has_data boolean NOT NULL DEFAULT false,cached_md5 text) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
   sqlx::query(&format!("CREATE TEMP TABLE {name}_cache_updates(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,station text,day date,md5 text,modified timestamptz,existed boolean) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
   sqlx::query(&format!("CREATE TEMP TABLE {name}_projected(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,payload jsonb) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
   Ok::<_,StoreError>(())
  })).await??;
        Ok(Self {
            session,
            source,
            table,
            first,
            single,
            days,
        })
    }
    pub async fn refresh_mappings(&self) -> Result<(), Error> {
        let source = self.source;
        let single = self.single;
        let name = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
sqlx::query(&format!("INSERT INTO {name}_mapped(id,station) SELECT e.id::bigint,e.tvg_id FROM epg_epgdata e WHERE e.epg_source_id=$1 AND e.id IN(SELECT epg_data_id FROM dispatcharr_channels_channel WHERE epg_data_id IS NOT NULL UNION SELECT epg_data_id FROM dispatcharr_channels_channeloverride WHERE epg_data_id IS NOT NULL)")).bind(source).execute(&mut*c).await?;sqlx::query(&format!("WITH candidates AS MATERIALIZED(SELECT row_number() OVER() AS observed,e.id::bigint AS epg_id,e.tvg_id AS station,CASE WHEN e.name='' THEN e.tvg_id ELSE e.name END AS label FROM epg_epgdata e WHERE e.epg_source_id=$1 AND(($2::bigint IS NOT NULL AND e.id=$2) OR($2::bigint IS NULL AND e.id IN(SELECT epg_data_id FROM dispatcharr_channels_channel WHERE epg_data_id IS NOT NULL UNION SELECT epg_data_id FROM dispatcharr_channels_channeloverride WHERE epg_data_id IS NOT NULL)))),first_seen AS(SELECT station,min(observed) AS observed FROM candidates WHERE station IS NOT NULL AND station<>'' GROUP BY station),last_seen AS(SELECT DISTINCT ON(station) station,epg_id,label FROM candidates WHERE station IS NOT NULL AND station<>'' ORDER BY station,observed DESC) INSERT INTO {name}_stations(station,epg_id,label) SELECT l.station,l.epg_id,l.label FROM first_seen f JOIN last_seen l USING(station) ORDER BY f.observed")).bind(source).bind(single).execute(&mut*c).await?;Ok::<_,StoreError>(())})).await??;
        Ok(())
    }
    pub fn reset_window(&mut self, first: NaiveDate) -> Result<(), Error> {
        self.first = first;
        self.days = (0..20)
            .map(|n| {
                first
                    .checked_add_signed(chrono::Duration::days(n))
                    .ok_or(Error::Shape)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }
    pub async fn station_page(&self, after: i64) -> Result<Vec<(i64, String, i64)>, Error> {
        let table = self.table.clone();
        Ok(self.session.run(move|c|Box::pin(async move{sqlx::query_as(&format!("SELECT ordinal,station,epg_id FROM {table}_stations WHERE ordinal>$1 AND epg_id IN(SELECT id FROM {table}_mapped) ORDER BY ordinal LIMIT 5000")).bind(after).fetch_all(c).await.map_err(StoreError::from)})).await??)
    }
    pub async fn mapped_page(&self, after: i64) -> Result<Vec<(i64, String, i64)>, Error> {
        let table = self.table.clone();
        Ok(self.session.run(move|c|Box::pin(async move{sqlx::query_as(&format!("SELECT id,COALESCE(station,''),id FROM {table}_mapped WHERE id>$1 ORDER BY id LIMIT 5000")).bind(after).fetch_all(c).await.map_err(StoreError::from)})).await??)
    }
    pub async fn mapping_counts(&self) -> Result<(i64, i64), Error> {
        let table = self.table.clone();
        Ok(self.session.run(move|c|Box::pin(async move{sqlx::query_as(&format!("SELECT(SELECT count(*) FROM {table}_mapped),(SELECT count(*) FROM {table}_stations)")).fetch_one(c).await.map_err(StoreError::from)})).await??)
    }
    pub async fn station_label(&self) -> Result<String, Error> {
        let table = self.table.clone();
        Ok(self
            .session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar(&format!(
                        "SELECT label FROM {table}_stations ORDER BY ordinal LIMIT 1"
                    ))
                    .fetch_one(c)
                    .await
                    .map_err(StoreError::from)
                })
            })
            .await??)
    }
    pub async fn mapped_station_count(&self) -> Result<i64, Error> {
        let table = self.table.clone();
        Ok(self
            .session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar(&format!(
                        "SELECT count(*) FROM(SELECT DISTINCT station FROM {table}_mapped) s"
                    ))
                    .fetch_one(c)
                    .await
                    .map_err(StoreError::from)
                })
            })
            .await??)
    }
    pub async fn fetch_station_count(&self) -> Result<i64, Error> {
        let table = self.table.clone();
        Ok(self.session.run(move|c|Box::pin(async move{sqlx::query_scalar(&format!("SELECT count(*) FROM {table}_stations WHERE epg_id IN(SELECT id FROM {table}_mapped)")).fetch_one(c).await.map_err(StoreError::from)})).await??)
    }
    pub async fn changed_station_count(&self) -> Result<i64, Error> {
        let table = self.table.clone();
        Ok(self
            .session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar(&format!(
                        "SELECT count(DISTINCT station) FROM {table}_days WHERE changed"
                    ))
                    .fetch_one(c)
                    .await
                    .map_err(StoreError::from)
                })
            })
            .await??)
    }
    pub async fn needed_program_count(&self) -> Result<i64, Error> {
        let table = self.table.clone();
        Ok(self.session.run(move|c|Box::pin(async move{sqlx::query_scalar(&format!("SELECT count(*) FROM {table}_programs WHERE cached_md5 IS DISTINCT FROM md5 OR NOT has_data")).fetch_one(c).await.map_err(StoreError::from)})).await??)
    }
    pub fn md5_request(&self, stations: &[(i64, String, i64)]) -> Value {
        Value::Array(stations.iter().map(|(_,id,_)|json!({"stationID":id,"date":self.days.iter().map(ToString::to_string).collect::<Vec<_>>()})).collect())
    }
    pub async fn prune_schedule_cache(&self) -> Result<(), Error> {
        let source = self.source;
        let first = self.first;
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{sqlx::query(&format!("DELETE FROM epg_sdschedulemd5 WHERE epg_source_id=$1 AND(date<$2 OR station_id NOT IN(SELECT station FROM {table}_mapped WHERE station IS NOT NULL))")).bind(source).bind(first).execute(c).await.map(|_|()).map_err(StoreError::from)})).await??;
        Ok(())
    }
    pub async fn record_schedule_md5(&self, payload: Value) -> Result<(), Error> {
        let object = payload.as_object().ok_or(Error::Shape)?;
        for dates in object.values() {
            for info in dates.as_object().ok_or(Error::Shape)?.values() {
                let info = info.as_object().ok_or(Error::Shape)?;
                if info
                    .get("md5")
                    .is_some_and(|v| !v.is_null() && !v.is_string())
                {
                    return Err(Error::Shape);
                }
            }
        }
        let days = self
            .days
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
   // SD returns object keyed station, then date. No host-sized flattened map.
   // Codes and scalar shape are validated by the caller before this stage.
   sqlx::query(&format!("INSERT INTO {table}_days(station,day,md5,last_modified) SELECT s.key,d.key::date,CASE WHEN d.value?'md5' THEN d.value->>'md5' ELSE '' END,NULL::timestamptz FROM jsonb_each($1) s CROSS JOIN LATERAL jsonb_each(s.value) d WHERE d.key=ANY($2::text[]) AND(NOT(d.value?'code') OR d.value->'code' IN('0'::jsonb,'false'::jsonb)) ON CONFLICT(station,day) DO UPDATE SET md5=excluded.md5,last_modified=excluded.last_modified")).bind(payload).bind(days).execute(c).await?;Ok::<_,StoreError>(())
  })).await??;
        Ok(())
    }
    pub async fn select_changes(&self) -> Result<i64, Error> {
        let source = self.source;
        let first = self.first;
        let end = self.days.last().copied().ok_or(Error::Shape)?;
        let table = self.table.clone();
        let raw=self.session.run(move|c|Box::pin(async move{
  sqlx::query(&format!("UPDATE {table}_days d SET changed=true WHERE d.day BETWEEN $2 AND $3 AND NOT EXISTS(SELECT 1 FROM epg_sdschedulemd5 m WHERE m.epg_source_id=$1 AND m.station_id=d.station AND m.date=d.day AND m.md5 IS NOT DISTINCT FROM d.md5)")).bind(source).bind(first).bind(end).execute(&mut*c).await?;
  let raw_changed:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM {table}_days WHERE changed")).fetch_one(&mut*c).await?;
  // Backfill missing days only where the server advertised a key and Source
  // cache does not suppress it; a completely empty station overrides cache.
  sqlx::query(&format!("UPDATE {table}_days d SET changed=true FROM {table}_stations s WHERE s.station=d.station AND d.day BETWEEN $2 AND $3 AND NOT EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id=s.epg_id AND(p.start_time AT TIME ZONE 'UTC')::date=d.day) AND(NOT EXISTS(SELECT 1 FROM epg_sdschedulemd5 m WHERE m.epg_source_id=$1 AND m.station_id=d.station AND m.date=d.day) OR NOT EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id=s.epg_id AND p.end_time>=($2::date::timestamp AT TIME ZONE 'UTC')))")).bind(source).bind(first).bind(end).execute(&mut*c).await?;
  sqlx::query(&format!("CREATE TEMP TABLE {table}_cache_existing ON COMMIT PRESERVE ROWS AS SELECT station_id,date FROM epg_sdschedulemd5 WHERE epg_source_id=$1 AND station_id IN(SELECT station FROM {table}_days WHERE changed)")).bind(source).execute(c).await?;Ok::<_,StoreError>(raw_changed)
 })).await??;
        Ok(raw)
    }
    pub async fn changed_dates(&self, station: &str) -> Result<Vec<NaiveDate>, Error> {
        let table = self.table.clone();
        let station = station.to_owned();
        Ok(self.session.run(move|c|Box::pin(async move{sqlx::query_scalar(&format!("SELECT day FROM {table}_days WHERE station=$1 AND changed ORDER BY day LIMIT 20")).bind(station).fetch_all(c).await.map_err(StoreError::from)})).await??)
    }
    pub async fn stage_schedules(&self, payload: Value) -> Result<(), Error> {
        let table = self.table.clone();
        let array = payload.as_array().ok_or(Error::Shape)?;
        let mut updates = Vec::new();
        for schedule in array {
            let station = schedule
                .get("stationID")
                .and_then(Value::as_str)
                .unwrap_or("");
            let metadata = schedule.get("metadata").and_then(Value::as_object);
            let date = metadata
                .and_then(|m| m.get("startDate"))
                .and_then(Value::as_str);
            let hash = metadata
                .and_then(|m| m.get("md5"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if station.is_empty() || hash.is_empty() {
                continue;
            }
            let Some(date) = date.and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
            else {
                continue;
            };
            let modified = match metadata
                .and_then(|m| m.get("modified"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                None => Some(chrono::Utc::now()),
                Some(s) => chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|v| v.with_timezone(&chrono::Utc)),
            };
            updates.push(json!({"station":station,"day":date,"md5":hash,"modified":modified}));
        }
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("INSERT INTO {table}_airings(station,payload,program_id,md5) SELECT s->>'stationID',p,p->>'programID',p->>'md5' FROM jsonb_array_elements($1) s CROSS JOIN LATERAL jsonb_array_elements(COALESCE(s->'programs','[]'::jsonb)) p WHERE COALESCE(s->>'stationID','')<>''")).bind(payload).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {table}_cache_updates(station,day,md5,modified,existed) SELECT x.station,x.day,x.md5,x.modified,EXISTS(SELECT 1 FROM {table}_cache_existing m WHERE m.station_id=x.station AND m.date=x.day) FROM jsonb_to_recordset($1) x(station text,day date,md5 text,modified timestamptz)")).bind(Value::Array(updates)).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(())
    }
    pub async fn select_programs(&self) -> Result<(), Error> {
        let table = self.table.clone();
        let source = self.source;
        self.session.run(move|c|Box::pin(async move{
  sqlx::query(&format!("CREATE TEMP TABLE {table}_ordered_airings(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,airing bigint) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
  sqlx::query(&format!("INSERT INTO {table}_ordered_airings(airing) SELECT a.ordinal FROM {table}_stations s JOIN {table}_airings a ON a.station=s.station ORDER BY s.ordinal,a.ordinal")).execute(&mut*c).await?;
  // Dict overwrite keeps the final airing's MD5; cached Programme snapshot is
  // captured BEFORE the surgical delete and remains available on fetch error.
  sqlx::query(&format!("INSERT INTO {table}_programs(program_id,md5,has_data,cached_md5) SELECT DISTINCT ON(a.program_id) a.program_id,(SELECT h.md5 FROM {table}_airings h LEFT JOIN {table}_stations hs ON hs.station=h.station WHERE h.program_id=a.program_id AND COALESCE(h.md5,'')<>'' ORDER BY hs.ordinal DESC NULLS FIRST,h.ordinal DESC LIMIT 1),EXISTS(SELECT 1 FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE e.epg_source_id=$1 AND p.program_id=a.program_id),(SELECT m.md5 FROM epg_sdprogrammd5 m WHERE m.epg_source_id=$1 AND m.program_id=a.program_id) FROM {table}_airings a WHERE a.program_id IS NOT NULL AND a.program_id<>'' ORDER BY a.program_id,a.ordinal DESC")).bind(source).execute(c).await?;Ok::<_,StoreError>(())
 })).await??;
        Ok(())
    }
    pub async fn downloaded_program_count(&self) -> Result<i64, Error> {
        let table = self.table.clone();
        Ok(self
            .session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar(&format!(
                        "SELECT count(*) FROM {table}_programs WHERE downloaded"
                    ))
                    .fetch_one(c)
                    .await
                    .map_err(StoreError::from)
                })
            })
            .await??)
    }
    pub async fn programme_id_count(&self) -> Result<u64, Error> {
        let table = self.table.clone();
        let n:i64=self.session.run(move|c|Box::pin(async move{sqlx::query_scalar(&format!("SELECT count(DISTINCT program_id) FROM {table}_airings WHERE program_id IS NOT NULL AND program_id<>''")).fetch_one(c).await.map_err(StoreError::from)})).await??;
        u64::try_from(n).map_err(|_| Error::Shape)
    }
    pub async fn has_programs(&self) -> Result<bool, Error> {
        let table = self.table.clone();
        Ok(self
            .session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {table}_programs)"))
                        .fetch_one(c)
                        .await
                        .map_err(StoreError::from)
                })
            })
            .await??)
    }
    pub async fn needed_programs(&self, after: &str) -> Result<Vec<String>, Error> {
        let table = self.table.clone();
        let after = after.to_owned();
        Ok(self.session.run(move|c|Box::pin(async move{sqlx::query_scalar(&format!("SELECT p.program_id FROM {table}_programs p WHERE p.program_id>$1 AND(p.cached_md5 IS DISTINCT FROM p.md5 OR NOT p.has_data) ORDER BY p.program_id LIMIT 5000")).bind(after).fetch_all(c).await.map_err(StoreError::from)})).await??)
    }
    pub async fn stage_programs(
        &self,
        requested: Vec<String>,
        payload: Option<Value>,
    ) -> Result<(), Error> {
        if requested.len() > 5000 {
            return Err(Error::Capacity);
        }
        if let Some(payload) = &payload {
            for entry in payload.as_array().ok_or(Error::Shape)? {
                let object = entry.as_object().ok_or(Error::Shape)?;
                if object
                    .get("programID")
                    .is_some_and(|v| crate::xc_live_provider::truthy(v) && !v.is_string())
                {
                    return Err(Error::Shape);
                }
            }
        }
        let table = self.table.clone();
        let failed = payload.is_none();
        self.session.run(move|c|Box::pin(async move{
  sqlx::query(&format!("UPDATE {table}_programs SET failed=$2 WHERE program_id=ANY($1)")).bind(requested).bind(failed).execute(&mut*c).await?;
  if let Some(payload)=payload{sqlx::query(&format!("UPDATE {table}_programs p SET payload=x.value,failed=false,downloaded=true FROM(SELECT DISTINCT ON(value->>'programID') value FROM jsonb_array_elements($1) WITH ORDINALITY x(value,ordinal) ORDER BY value->>'programID',ordinal DESC) x WHERE p.program_id=x.value->>'programID'")).bind(payload).execute(&mut*c).await?;}Ok::<_,StoreError>(())
 })).await??;
        Ok(())
    }
    pub async fn capture_cached_programmes(&self) -> Result<(), Error> {
        let table = self.table.clone();
        let source = self.source;
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("UPDATE {table}_programs x SET cached=(SELECT jsonb_build_object('title',p.title,'description',p.description,'sub_title',p.sub_title,'custom_properties',p.custom_properties) FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE e.epg_source_id=$1 AND p.program_id=x.program_id LIMIT 1) WHERE x.payload IS NULL")).bind(source).execute(c).await.map(|_|()).map_err(StoreError::from)
        })).await??;
        Ok(())
    }
    pub async fn skipped_unmapped(&self) -> Result<u64, Error> {
        let table = self.table.clone();
        let count:i64=self.session.run(move|c|Box::pin(async move{
            sqlx::query_scalar(&format!("SELECT count(*) FROM {table}_airings a WHERE NOT EXISTS(SELECT 1 FROM {table}_mapped s WHERE s.station=a.station)")).fetch_one(c).await.map_err(StoreError::from)
        })).await??;
        u64::try_from(count).map_err(|_| Error::Shape)
    }
    pub async fn airing_page(
        &self,
        after: i64,
    ) -> Result<Vec<(i64, i64, String, Value, Option<Value>, Option<Value>, bool)>, Error> {
        let table = self.table.clone();
        Ok(self.session.run(move|c|Box::pin(async move{
            sqlx::query_as(&format!("SELECT o.ordinal,s.epg_id,a.station,a.payload,p.payload,CASE WHEN p.payload IS NULL THEN p.cached END,COALESCE(p.failed,false) FROM {table}_ordered_airings o JOIN {table}_airings a ON a.ordinal=o.airing JOIN {table}_stations s ON s.station=a.station LEFT JOIN {table}_programs p ON p.program_id=a.program_id WHERE o.ordinal>$1 ORDER BY o.ordinal LIMIT 1")).bind(after).fetch_all(c).await.map_err(StoreError::from)
        })).await??)
    }
    /// One projected Programme at a time: provider metadata is never repeated
    /// into a500-row host buffer. SQL temp state replaces Source's whole list.
    pub async fn stage_projection(&self, payload: Value) -> Result<(), Error> {
        let table = self.table.clone();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("INSERT INTO {table}_projected(payload)VALUES($1)"))
                        .bind(payload)
                        .execute(c)
                        .await
                        .map(|_| ())
                        .map_err(StoreError::from)
                })
            })
            .await??;
        Ok(())
    }
    pub async fn projected_count(&self) -> Result<u64, Error> {
        let table = self.table.clone();
        let n: i64 = self
            .session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}_projected"))
                        .fetch_one(c)
                        .await
                        .map_err(StoreError::from)
                })
            })
            .await??;
        u64::try_from(n).map_err(|_| Error::Shape)
    }
    pub async fn insert_projected_batch(&self, after: i64) -> Result<Option<(i64, u64)>, Error> {
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
            let high:Option<i64>=sqlx::query_scalar(&format!("SELECT max(ordinal) FROM(SELECT ordinal FROM {table}_projected WHERE ordinal>$1 ORDER BY ordinal LIMIT 1000)x")).bind(after).fetch_one(&mut*c).await?;
            let Some(high)=high else{return Ok(None)};
            let expected:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM {table}_projected WHERE ordinal>$1 AND ordinal<=$2")).bind(after).bind(high).fetch_one(&mut*c).await?;
            let rows:Vec<i64>=sqlx::query_scalar(&format!("INSERT INTO epg_programdata(epg_id,start_time,end_time,title,sub_title,description,tvg_id,program_id,custom_properties) SELECT r.epg_id,r.start_time,r.end_time,r.title,r.sub_title,r.description,r.tvg_id,r.program_id,r.custom_properties FROM {table}_projected p CROSS JOIN LATERAL jsonb_to_record(p.payload) r(epg_id bigint,start_time timestamptz,end_time timestamptz,title text,sub_title text,description text,tvg_id text,program_id text,custom_properties jsonb) WHERE p.ordinal>$1 AND p.ordinal<=$2 ORDER BY p.ordinal RETURNING id::bigint")).bind(after).bind(high).fetch_all(c).await?;
            if expected<0||rows.len()as i64!=expected{return Err(StoreError::Unsupported("SD Programme bulk insert returned an incomplete model identity set"))}
            Ok::<_,StoreError>(Some((high,rows.len()as u64)))
        })).await?.map_err(Error::Store)
    }
    pub async fn commit_schedule_md5(&self) -> Result<(), Error> {
        let table = self.table.clone();
        let source = self.source;
        self.session.run(move|c|Box::pin(async move{
            // Persist schedule deltas before programme transaction, as Source.
            // Actual schedule metadata supplies md5/modified; no advertised
            // schedules/md5 value is substituted for a downloaded schedule.
            sqlx::query(&format!("INSERT INTO epg_sdschedulemd5(epg_source_id,station_id,date,md5,last_modified) SELECT $1,d.station,d.day,d.md5,d.modified FROM(SELECT DISTINCT ON(station,day) * FROM {table}_cache_updates WHERE NOT existed ORDER BY station,day,ordinal) d ON CONFLICT(epg_source_id,station_id,date) DO NOTHING")).bind(source).execute(&mut*c).await?;
            sqlx::query(&format!("UPDATE epg_sdschedulemd5 m SET md5=d.md5,last_modified=d.modified FROM(SELECT DISTINCT ON(station,day) * FROM {table}_cache_updates WHERE existed ORDER BY station,day,ordinal DESC) d WHERE m.epg_source_id=$1 AND m.station_id=d.station AND m.date=d.day")).bind(source).execute(c).await.map(|_|()).map_err(StoreError::from)
        })).await??;
        Ok(())
    }
    pub async fn begin_surgical_write(&self) -> Result<(), Error> {
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
            sqlx::query("BEGIN").execute(&mut*c).await?;
            sqlx::query("SET LOCAL statement_timeout='10min'").execute(&mut*c).await?;
            sqlx::query(&format!("DELETE FROM epg_programdata p USING {table}_stations s,{table}_days d WHERE p.epg_id=s.epg_id AND s.station=d.station AND d.changed AND p.start_time>=(d.day::timestamp AT TIME ZONE 'UTC') AND p.start_time<((d.day+1)::timestamp AT TIME ZONE 'UTC')")).execute(c).await?;Ok::<_,StoreError>(())
        })).await??;
        Ok(())
    }
    pub async fn settle_surgical_write(&self, commit: bool) -> Result<(), Error> {
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(if commit { "COMMIT" } else { "ROLLBACK" })
                        .execute(c)
                        .await
                        .map(|_| ())
                        .map_err(StoreError::from)
                })
            })
            .await??;
        Ok(())
    }
    pub async fn commit_program_md5(&self) -> Result<(), Error> {
        let table = self.table.clone();
        let source = self.source;
        self.session.run(move|c|Box::pin(async move{
            // Only metadata actually downloaded enters Source's programme cache,
            // and only after actual Programme transaction COMMIT.
            sqlx::query(&format!("INSERT INTO epg_sdprogrammd5(epg_source_id,program_id,md5) SELECT $1,program_id,md5 FROM {table}_programs WHERE downloaded AND COALESCE(md5,'')<>'' ON CONFLICT(epg_source_id,program_id) DO UPDATE SET md5=excluded.md5")).bind(source).execute(c).await.map(|_|()).map_err(StoreError::from)
        })).await??;
        Ok(())
    }
}
