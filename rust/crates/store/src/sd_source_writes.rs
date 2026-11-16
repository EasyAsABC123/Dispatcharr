//! Source-backed narrow SD source creation and lineup-management persistence.
//! Original Dispatcharr/Django authors retain model, signal and API credit.
use crate::{AuthService, StoreError, TaskSqlSession};
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::Connection;
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;
pub(super) struct Create {
    pub name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub active: bool,
    pub priority: i32,
    pub properties: Value,
    pub url: Option<String>,
    pub file_path: Option<String>,
}
fn field(name: &str, message: &str) -> StoreError {
    StoreError::StatusBody(400, json!({name:[message]}))
}
fn strip(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
}
fn chars(value: Option<&Value>, name: &str, required: bool) -> Result<Option<String>, StoreError> {
    let Some(value) = value else {
        return if required {
            Err(field(name, "This field is required."))
        } else {
            Ok(None)
        };
    };
    if value.is_null() {
        return if required {
            Err(field(name, "This field may not be null."))
        } else {
            Ok(None)
        };
    }
    let text = match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return Err(field(name, "Not a valid string.")),
    };
    let text = strip(&text).to_owned();
    if required && text.is_empty() {
        return Err(field(name, "This field may not be blank."));
    }
    if text.chars().count() > 255 {
        return Err(field(
            name,
            "Ensure this field has no more than 255 characters.",
        ));
    }
    if text.contains('\0') {
        return Err(field(name, "Null characters are not allowed."));
    }
    Ok(Some(text))
}
pub(super) fn validate(value: &Value) -> Result<Create, StoreError> {
    let object = value.as_object().ok_or_else(|| {
        StoreError::StatusBody(
            400,
            json!({"non_field_errors":["Invalid data. Expected a dictionary, but got list."]}),
        )
    })?;
    let name = chars(object.get("name"), "name", true)?.ok_or(StoreError::UnhandledRestFault)?;
    match object.get("source_type") {
        None => return Err(field("source_type", "This field is required.")),
        Some(Value::String(s)) if s == "schedules_direct" => {}
        Some(_) => {
            return Err(StoreError::Unsupported(
                "Only the Source-qualified SD creation domain is admitted",
            ))
        }
    }
    // Explicit first slice: normal interval-zero source creation. Other source
    // types, uploaded XMLTV, cron and positive periodic scheduling need their own
    // actual Source scheduling characterization, never an ignored field.
    if object
        .get("refresh_interval")
        .is_some_and(|v| v != &json!(0))
        || object
            .get("cron_expression")
            .is_some_and(|v| v != &json!(""))
        || ["url", "file_path"].iter().any(|k| {
            object
                .get(*k)
                .is_some_and(|v| !v.is_null() && v != &json!(""))
        })
        || object.get("status").is_some_and(|v| v != &json!("idle"))
        || object.get("last_message").is_some_and(|v| !v.is_null())
    {
        return Err(StoreError::Unsupported(
            "SD source scheduling/upload/status write domain unqualified",
        ));
    }
    let active = match object.get("is_active") {
        None => true,
        Some(Value::Bool(b)) => *b,
        _ => return Err(field("is_active", "Must be a valid boolean.")),
    };
    let priority = match object.get("priority") {
        None => 0,
        Some(v) => v
            .as_i64()
            .filter(|v| (0..=i32::MAX as i64).contains(v))
            .ok_or_else(|| field("priority", "A valid integer is required."))?
            as i32,
    };
    let properties = object
        .get("custom_properties")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !properties.is_null() && !properties.is_object() {
        return Err(StoreError::Unsupported(
            "SD source nonobject properties need Source characterization",
        ));
    }
    Ok(Create {
        name,
        username: chars(object.get("username"), "username", false)?,
        password: chars(object.get("password"), "password", false)?,
        active,
        priority,
        properties,
        url: object.get("url").and_then(Value::as_str).map(str::to_owned),
        file_path: object
            .get("file_path")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}
/// Source interval0 still creates a DISABLED one-hour Beat task and links it.
/// All writes commit on the same real admitted SQL session before publication.
pub(super) async fn create(
    auth: AuthService,
    input: Create,
    owner: Owner,
) -> Result<(i64, bool, Value), StoreError> {
    let session = auth.borrow_task_sql_session(owner).await?;
    let active = input.active;
    let result=session.run(move|c|Box::pin(async move{
  let mut tx=c.begin().await?;
  let custom:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN('epg_epgsource'::regclass,'django_celery_beat_periodictask'::regclass,'django_celery_beat_intervalschedule'::regclass,'django_celery_beat_periodictasks'::regclass))").fetch_one(&mut*tx).await?;
  if custom{return Err(StoreError::Unsupported("SD source custom SQL trigger domain requires Source characterization"))}
  let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE name=$1)").bind(&input.name).fetch_one(&mut*tx).await?;
  if exists{return Err(field("name","epg source with this name already exists."))}
  let created=Utc::now();let updated:Option<chrono::DateTime<Utc>>=None;
  let id:i64=sqlx::query_scalar("INSERT INTO epg_epgsource(name,source_type,url,username,password,is_active,file_path,extracted_file_path,refresh_interval,refresh_task_id,created_at,updated_at,status,last_message,custom_properties,priority) VALUES($1,'schedules_direct',$9,$2,$3,$4,$10,NULL,0,NULL,$5,$6,'idle',NULL,$7,$8) RETURNING id::bigint").bind(input.name).bind(input.username).bind(input.password).bind(active).bind(created).bind(updated).bind(if input.properties.is_null(){None}else{Some(input.properties)}).bind(input.priority).bind(input.url).bind(input.file_path).fetch_one(&mut*tx).await?;
  if !(1..=i64::from(i32::MAX)).contains(&id){return Err(StoreError::Unsupported("SD station task identity capacity"))}
  let name=format!("epg_source-refresh-{id}");
  let stale:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE name=$1)").bind(&name).fetch_one(&mut*tx).await?;
  if stale{return Err(StoreError::Unsupported("SD creation with existing orphan Beat name requires Source characterization"))}
  let intervals:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_intervalschedule WHERE every=1 AND period='hours' LIMIT 2").fetch_all(&mut*tx).await?;
  let interval=match intervals.as_slice(){[]=>sqlx::query_scalar("INSERT INTO django_celery_beat_intervalschedule(every,period) VALUES(1,'hours') RETURNING id::bigint").fetch_one(&mut*tx).await?,[id]=>*id,_=>return Err(StoreError::UnhandledRestFault)};
  let periodic:i64=sqlx::query_scalar("INSERT INTO django_celery_beat_periodictask(name,task,interval_id,crontab_id,solar_id,clocked_id,args,kwargs,queue,exchange,routing_key,headers,priority,expires,expire_seconds,one_off,start_time,enabled,last_run_at,total_run_count,date_changed,description) VALUES($1,'apps.epg.tasks.refresh_epg_data',$2,NULL,NULL,NULL,'[]',$3,NULL,NULL,NULL,'{}',NULL,NULL,NULL,false,NULL,false,NULL,0,$4,'') RETURNING id::bigint").bind(name).bind(i32::try_from(interval).map_err(|_|StoreError::UnhandledRestFault)?).bind(format!("{{\"source_id\": {id}}}")).bind(Utc::now()).fetch_one(&mut*tx).await?;
  sqlx::query("UPDATE epg_epgsource SET refresh_task_id=$2 WHERE id=$1").bind(id).bind(i32::try_from(periodic).map_err(|_|StoreError::UnhandledRestFault)?).execute(&mut*tx).await?;
  sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(&mut*tx).await?;
  let row=sqlx::query(crate::provider_reads::epg_read_query(true)).bind(Some(id)).bind(None::<i64>).fetch_one(&mut*tx).await?;
  let response=crate::provider_reads::epg_read_projection(row,true)?;
  tx.commit().await?;
  Ok::<_,StoreError>((id,active,response))
 })).await;
    let closed = session.close().await;
    closed?;
    result?
}
pub(super) async fn close_result<T>(
    session: &TaskSqlSession,
    result: Result<T, StoreError>,
) -> Result<T, StoreError> {
    session.close().await?;
    result
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn creation_retains_source_text_trimming_and_password_null() {
        let v=validate(&json!({"name":" \u{1c}Guide\u{1f}","source_type":"schedules_direct","username":" user ","password":null})).unwrap();
        assert_eq!(v.name, "Guide");
        assert_eq!(v.username.as_deref(), Some("user"));
        assert_eq!(v.password, None);
        assert!(v.active);
    }
    #[test]
    fn wider_schedule_is_not_silently_ignored() {
        assert!(validate(
            &json!({"name":"Guide","source_type":"schedules_direct","refresh_interval":1})
        )
        .is_err());
        assert!(validate(&json!({"name":"Guide","source_type":"xmltv"})).is_err());
    }
}
