//! Source backups.scheduler over actual settings/Beat rows; no timer starts here.
//! Original Dispatcharr/django-celery-beat authors retain application credit.
use crate::recording_settings::Settings;
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{
    AuthService, BackupScheduleDisableApi, ProducerIdentitySource, Store, StoreError,
};
use serde_json::{json, Value};
use sqlx::{Connection, Row};
use std::sync::Arc;
fn defaults() -> Value {
    json!({"schedule_enabled":true,"schedule_frequency":"daily","schedule_time":"03:00","schedule_day_of_week":0,"retention_count":3,"schedule_cron_expression":""})
}
fn failed(detail: &str) -> StoreError {
    StoreError::StatusBody(500, json!({"detail":detail}))
}
fn invalid(detail: &str) -> StoreError {
    StoreError::StatusBody(400, json!({"detail":detail}))
}
fn truth(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
fn text(v: &Value, identity: &ProducerIdentitySource) -> Result<String, StoreError> {
    if let Some(v) = v.as_str() {
        return Ok(v.to_string());
    }
    let (args, _) = identity.periodic_representations(&json!([v]), &json!({}))?;
    Ok(args[1..args.len() - 1].to_string())
}
fn integer(v: &Value) -> Result<i64, StoreError> {
    match v {
        Value::Bool(v) => Ok(i64::from(*v)),
        Value::Number(v) => v
            .as_i64()
            .or_else(|| {
                v.as_f64()
                    .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v <= i64::MAX as f64)
                    .map(|v| v.trunc() as i64)
            })
            .ok_or_else(|| invalid("Schedule integer exceeds native capacity")),
        Value::String(v) => dispatcharr_ingest_core::source_integer_literal(v)
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| invalid("Invalid schedule integer")),
        _ => Err(invalid("Invalid schedule integer")),
    }
}
fn normalized(v: &Value, identity: &ProducerIdentitySource) -> Result<Value, StoreError> {
    let d = defaults();
    let get = |key: &str| v.get(key).unwrap_or(&d[key]);
    Ok(
        json!({"enabled":truth(get("schedule_enabled")),"frequency":text(get("schedule_frequency"),identity)?,"time":text(get("schedule_time"),identity)?,"day_of_week":integer(get("schedule_day_of_week"))?,"retention_count":integer(get("retention_count"))?,"cron_expression":text(get("schedule_cron_expression"),identity)?}),
    )
}
#[derive(Clone)]
pub(crate) struct Schedule {
    store: Store,
    cache: String,
    identity: ProducerIdentitySource,
}
impl Schedule {
    pub(crate) fn new(store: Store, cache: String, identity: ProducerIdentitySource) -> Self {
        Self {
            store,
            cache,
            identity,
        }
    }
    pub(crate) async fn initialize(&self, owner: Arc<dyn Send + Sync>) -> Result<(), StoreError> {
        self.apply(None, owner, None, false).await.map(|_| ())
    }
    pub(crate) async fn update(
        &self,
        data: Value,
        auth: &AuthService,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Value, StoreError> {
        self.apply(Some(data), owner, Some(auth), false).await
    }
    async fn apply(
        &self,
        data: Option<Value>,
        owner: Arc<dyn Send + Sync>,
        backup_cache_after_save: Option<&AuthService>,
        ordinary_disable: bool,
    ) -> Result<Value, StoreError> {
        // Source reads malformed stored values as DEFAULTS, but PUT merges
        // updates into an empty object. Preserve that distinction before data moves.
        let updating = data.is_some();
        let held = owner.clone();
        let mut connection = self.store.pool().acquire().await?;
        let observed=async{
   let row=sqlx::query("SELECT CASE WHEN value IS NULL OR octet_length(value::text)<=65536 THEN value ELSE NULL END AS value,value IS NULL OR octet_length(value::text)<=65536 AS bounded FROM core_coresettings WHERE key='backup_settings' LIMIT 1").fetch_optional(&mut *connection).await?;
   let create_ordinary_row = row.is_none() && ordinary_disable;
   let mut value=match row{Some(row)=>{if !row.try_get::<bool,_>("bounded")?{return Err(failed("Backup settings capacity unavailable"))}row.try_get::<Option<Value>,_>("value")?.filter(Value::is_object).unwrap_or_else(||if updating{json!({})}else{defaults()})},
    None if create_ordinary_row=>{
     if !data.as_ref().is_some_and(|request| ordinary_disable_request(request, true)){
      return Err(disable_unavailable());
     }
     defaults()
    },
    None=>{let d=defaults();sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('backup_settings','Backup Settings',$1) ON CONFLICT(key) DO NOTHING").bind(&d).execute(&mut *connection).await?;d}};
   if let Some(data)=data{
    let fields=data.as_object().ok_or_else(||invalid("Expected an object"))?;
    if let Some(v)=fields.get("frequency"){if !matches!(v.as_str(),Some("daily"|"weekly")){return Err(invalid("frequency must be 'daily' or 'weekly'"))}}
    if let Some(v)=fields.get("time"){
     let parts=v.as_str().map(|s|s.split(':').collect::<Vec<_>>()).filter(|p|p.len()==2).ok_or_else(||invalid("time must be in HH:MM format"))?;
     for p in parts{dispatcharr_ingest_core::source_integer_literal(p).ok_or_else(||invalid("time must be in HH:MM format"))?;}
    }
    if let Some(v)=fields.get("day_of_week"){let n=integer(v)?;if !(0..=6).contains(&n){return Err(invalid("day_of_week must be 0-6 (Sunday-Saturday)"))}}
    if let Some(v)=fields.get("retention_count"){if integer(v)?<0{return Err(invalid("retention_count must be >= 0"))}}
    let map=value.as_object_mut().ok_or_else(||failed("Backup settings unavailable"))?;
    for(from,to)in[("enabled","schedule_enabled"),("frequency","schedule_frequency"),("time","schedule_time"),("day_of_week","schedule_day_of_week"),("retention_count","retention_count"),("cron_expression","schedule_cron_expression")]{
     if let Some(v)=fields.get(from){let converted=match from{"enabled"=>json!(truth(v)),"day_of_week"|"retention_count"=>json!(integer(v)?),_=>json!(text(v,&self.identity)?)};map.insert(to.to_string(),converted);}
    }
    // Source commits the setting before periodic sync; a later invalid cron is
    // not grounds to fabricate rollback of the already committed setting.
    let changed = if create_ordinary_row {
     // Persist the final disabled state without exposing a default-enabled row.
     // Another request may have created the group after the initial SELECT.
     // In that case preserve its unknown fields and existing display name.
     sqlx::query(concat!(
      "INSERT INTO core_coresettings(key,name,value) VALUES('backup_settings','Backup Settings',$1) ",
      "ON CONFLICT(key) DO UPDATE SET value=CASE WHEN jsonb_typeof(core_coresettings.value)='object' ",
      "THEN core_coresettings.value || EXCLUDED.value ELSE EXCLUDED.value END ",
      "WHERE core_coresettings.value IS NULL OR octet_length(core_coresettings.value::text)<=65536"
     )).bind(&value)
     .execute(&mut *connection).await?
    } else {
     sqlx::query("UPDATE core_coresettings SET value=$1 WHERE key='backup_settings'").bind(&value).execute(&mut *connection).await?
    };
    if ordinary_disable && changed.rows_affected()!=1 { return Err(disable_unavailable()); }
   }
   Ok::<_,StoreError>(value)
  }.await;
        let ready = connection.ping().await;
        let close = connection.close().await;
        ready?;
        close?;
        let value = observed?;
        if let Some(auth) = backup_cache_after_save {
            auth.backup_schedule_settings_saved().await?;
        }
        let config = normalized(&value, &self.identity)?;
        let canonical = if config["enabled"] == json!(false) {
            "UTC"
        } else {
            let system = Settings::new(self.store.clone(), self.cache.clone())
                .normal_effect(true, owner)
                .await
                .map_err(|_| failed("Backup timezone policy did not settle"))?;
            let zone = system
                .get("time_zone")
                .and_then(Value::as_str)
                .unwrap_or("UTC");
            chrono_tz::TZ_VARIANTS
                .iter()
                .find(|v| v.name().eq_ignore_ascii_case(zone))
                .ok_or_else(|| failed("Invalid backup schedule timezone"))?
                .name()
        };
        let mut connection = self.store.pool().acquire().await?;
        let observed=async{
   let mut tx=connection.begin().await?;
   let old:Option<(i64,Option<i64>,Option<i64>)>=sqlx::query_as("SELECT id::bigint,interval_id::bigint,crontab_id::bigint FROM django_celery_beat_periodictask WHERE name='backup-scheduled-task' FOR UPDATE").fetch_optional(&mut *tx).await?;
   // Source delete_periodic_task returns immediately when the named task is
   // absent. Disabled startup must not invalidate Beat for a nonexistent row.
   if config["enabled"]==json!(false)&&old.is_none(){tx.commit().await?;return Ok(config)}
   if config["enabled"]==json!(false){sqlx::query("DELETE FROM django_celery_beat_periodictask WHERE name='backup-scheduled-task'").execute(&mut *tx).await?;}
   else{
    let advanced=config["cron_expression"].as_str().unwrap_or("");let expression=if !advanced.is_empty(){advanced.to_string()}else{let time=config["time"].as_str().unwrap_or("");let parts=time.split(':').collect::<Vec<_>>();if parts.len()!=2{return Err(failed("Invalid stored backup schedule time"))}format!("{} {} * * {}",parts[1],parts[0],if config["frequency"]==json!("daily"){ "*".to_string()}else{config["day_of_week"].to_string()})};
    let parts=expression.split_whitespace().collect::<Vec<_>>();if parts.len()!=5{return Err(invalid("Cron expression must have 5 parts: minute hour day month weekday"))}
    let found:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_crontabschedule WHERE minute=$1 AND hour=$2 AND day_of_month=$3 AND month_of_year=$4 AND day_of_week=$5 AND timezone=$6 ORDER BY id LIMIT 1").bind(parts[0]).bind(parts[1]).bind(parts[2]).bind(parts[3]).bind(parts[4]).bind(canonical).fetch_optional(&mut *tx).await?;
    let cron=match found{Some(id)=>id,None=>sqlx::query_scalar("INSERT INTO django_celery_beat_crontabschedule(minute,hour,day_of_month,month_of_year,day_of_week,timezone) VALUES($1,$2,$3,$4,$5,$6) RETURNING id::bigint").bind(parts[0]).bind(parts[1]).bind(parts[2]).bind(parts[3]).bind(parts[4]).bind(canonical).fetch_one(&mut *tx).await?};
    let kwargs=serde_json::to_string(&json!({"retention_count":config["retention_count"]})).map_err(|_|failed("Backup schedule serialization"))?;
    sqlx::query("INSERT INTO django_celery_beat_periodictask(name,task,crontab_id,args,kwargs,headers,enabled,total_run_count,date_changed,description,one_off) VALUES('backup-scheduled-task','apps.backups.tasks.scheduled_backup_task',$1,'[]',$2,'{}',true,0,$3,'',false) ON CONFLICT(name) DO UPDATE SET task=EXCLUDED.task,crontab_id=EXCLUDED.crontab_id,interval_id=NULL,kwargs=EXCLUDED.kwargs,enabled=true,date_changed=EXCLUDED.date_changed").bind(cron).bind(kwargs).bind(chrono::Utc::now()).execute(&mut *tx).await?;
   }
   if let Some((_,interval,cron))=old{
    if let Some(id)=interval{sqlx::query("DELETE FROM django_celery_beat_intervalschedule s WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask p WHERE p.interval_id=s.id)").bind(id).execute(&mut *tx).await?;}
    if let Some(id)=cron{sqlx::query("DELETE FROM django_celery_beat_crontabschedule s WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask p WHERE p.crontab_id=s.id)").bind(id).execute(&mut *tx).await?;}
   }
   sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(chrono::Utc::now()).execute(&mut *tx).await?;tx.commit().await?;Ok(config)
  }.await;
        let ready = connection.ping().await;
        let close = connection.close().await;
        drop(held);
        ready?;
        close?;
        observed
    }
}

fn disable_unavailable() -> StoreError {
    StoreError::StatusBody(
        501,
        json!({"detail":"Backup schedule consumer is not prepared"}),
    )
}

fn ordinary_disable_request(data: &Value, missing_row: bool) -> bool {
    let Some(fields) = data.as_object() else {
        return false;
    };
    if !matches!(fields.get("enabled"), Some(Value::Bool(false)))
        && data["enabled"].as_str() != Some("")
    {
        return false;
    }
    if fields.len() == 1 {
        return !missing_row;
    }
    let form_integer = |key: &str| {
        let value = fields.get(key)?;
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|_| integer(value).ok()))
    };
    fields.len() == 6
        && matches!(data["frequency"].as_str(), Some("daily" | "weekly"))
        && data["time"].as_str().is_some_and(|time| {
            time.len() == 5 && chrono::NaiveTime::parse_from_str(time, "%H:%M").is_ok()
        })
        && form_integer("day_of_week").is_some_and(|day| (0..=6).contains(&day))
        && form_integer("retention_count").is_some_and(|count| count >= 0)
        && data["cron_expression"].is_string()
}

/// Owned settings-only adapter. Its constructor does not start a worker or
/// synchronize schedules on startup.
pub struct BackupScheduleDisable(Schedule);

impl BackupScheduleDisable {
    pub fn new(store: Store, cache: String, identity: ProducerIdentitySource) -> Self {
        Self(Schedule::new(store, cache, identity))
    }
}

impl BackupScheduleDisableApi for BackupScheduleDisable {
    fn disable<'a>(
        &'a self,
        data: Value,
        auth: &'a AuthService,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Value, StoreError>> {
        Box::pin(async move {
            if !ordinary_disable_request(&data, false) {
                return Err(disable_unavailable());
            }
            self.0.apply(Some(data), caller, Some(auth), true).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_ordinary_row_requires_the_full_stock_disabled_save() {
        let mut request = json!({"enabled":false, "frequency":"weekly", "time":"04:10",
                                 "day_of_week":2, "retention_count":3, "cron_expression":""});
        assert!(ordinary_disable_request(&request, true));
        assert!(!ordinary_disable_request(&json!({"enabled":false}), true));
        request["enabled"] = json!(true);
        assert!(!ordinary_disable_request(&request, true));
        request["enabled"] = json!(false);
        request.as_object_mut().unwrap().remove("cron_expression");
        assert!(!ordinary_disable_request(&request, true));
    }

    #[test]
    fn ordinary_scope_only_admits_a_stock_disabled_save() {
        let mut full = json!({"enabled":false, "frequency":"weekly", "time":"04:10",
                              "day_of_week":2, "retention_count":3, "cron_expression":""});
        assert!(ordinary_disable_request(&full, false));
        assert!(ordinary_disable_request(&json!({"enabled":false}), false));
        for (key, value) in [
            ("enabled", json!(true)),
            ("enabled", json!("false")),
            ("time", json!("27:15")),
            ("day_of_week", json!(7)),
            ("retention_count", json!(-1)),
        ] {
            let old = std::mem::replace(&mut full[key], value);
            assert!(!ordinary_disable_request(&full, false));
            full[key] = old;
        }
        assert!(!ordinary_disable_request(
            &json!({"enabled":false,"unknown":1}),
            false
        ));
    }

    #[test]
    fn ordinary_disable_accepts_the_stock_html_form_strings() {
        let mut request = json!({"enabled":"", "frequency":"weekly", "time":"04:10",
                                 "day_of_week":"2", "retention_count":"3", "cron_expression":""});
        assert!(ordinary_disable_request(&request, false));
        assert!(ordinary_disable_request(&request, true));
        request["enabled"] = json!("false");
        assert!(!ordinary_disable_request(&request, false));
        request["enabled"] = json!("");
        request["retention_count"] = json!("1.0");
        assert!(!ordinary_disable_request(&request, false));
    }
}
