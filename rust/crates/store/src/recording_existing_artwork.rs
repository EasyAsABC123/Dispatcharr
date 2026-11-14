//! Existing Movie/Series VODLogo resolution from pinned Original artwork logic.
//! Original Dispatcharr/Django authors retain all algorithm/application credit.
//! No fixture titles, paths, IDs or season numbers define this production domain.
use crate::json_compat::python_equal;
use serde_json::{json, Value};
use sqlx::{Connection, PgPool};
use std::sync::Arc;
#[derive(Debug)]
pub enum Error {
    Unsupported(&'static str),
    Sql {
        primary: Option<Box<sqlx::Error>>,
        ready: Option<Box<sqlx::Error>>,
        close: Option<Box<sqlx::Error>>,
    },
    Supervisor,
}
pub struct Prepared {
    origin: Arc<()>,
    recording: i64,
    resolved: Value,
    changed: bool,
    skip: Option<String>,
}
pub struct Completed {
    pub recording: i64,
    pub changed: bool,
    pub value: String,
}
#[derive(Clone)]
pub struct Store {
    pool: PgPool,
    origin: Arc<()>,
}
fn sql(e: sqlx::Error) -> Error {
    Error::Sql {
        primary: Some(Box::new(e)),
        ready: None,
        close: None,
    }
}
fn truth(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
        Value::Number(v) => v.as_f64() != Some(0.0),
    }
}
fn strip(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{001c}'..='\u{001f}'))
}
fn normalized(s: &str) -> String {
    strip(&s.to_lowercase().replace('*', "").replace('-', " ")).to_owned()
}
impl Store {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            origin: Arc::new(()),
        }
    }
    pub async fn prepare(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Prepared, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&recording) {
            return Err(Error::Unsupported("recording id"));
        }
        let pool = self.pool.clone();
        let origin = self.origin.clone();
        tokio::spawn(async move{
   let _owner=owner;let mut c=pool.acquire().await.map_err(sql)?.detach();let result=async{
    let triggers:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN ('dispatcharr_channels_recording'::regclass,'dispatcharr_channels_channel'::regclass,'epg_programdata'::regclass,'vod_movie'::regclass,'vod_series'::regclass,'vod_vodlogo'::regclass))").fetch_one(&mut c).await.map_err(sql)?;if triggers{return Err(Error::Unsupported("unmeasured artwork model trigger"));}
    let row:Option<(i64,Option<Value>,bool)>=sqlx::query_as("SELECT channel_id::bigint,CASE WHEN custom_properties IS NULL THEN '{}'::jsonb WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,(custom_properties IS NULL OR octet_length(custom_properties::text)<=8192) FROM dispatcharr_channels_recording WHERE id=$1").bind(recording).fetch_optional(&mut c).await.map_err(sql)?;
    let Some((channel,Some(properties),true))=row else{return Err(Error::Unsupported("recording snapshot capacity/missing"));};let fields=properties.as_object().ok_or(Error::Unsupported("recording properties object"))?;
    if let Some(status)=fields.get("status").and_then(Value::as_str){if matches!(status,"recording"|"completed"|"stopped"|"interrupted"){let skip=format!("skipped: status is {status}");return Ok(Prepared{origin,recording,resolved:properties,changed:false,skip:Some(skip)});}}
    let program=fields.get("program").and_then(Value::as_object).ok_or(Error::Unsupported("manual programme enrichment requires batch coverage"))?;let title=program.get("title").and_then(Value::as_str).map(strip).filter(|v|!v.is_empty()&&v.len()<=1024).ok_or(Error::Unsupported("programme title"))?.to_owned();
    let program_id=program.get("id").and_then(Value::as_i64).filter(|v|*v>0).ok_or(Error::Unsupported("manual recording timeslot branch"))?;
    let channel_name:Option<String>=sqlx::query_scalar("SELECT CASE WHEN octet_length(name)<=1024 THEN name ELSE NULL END FROM dispatcharr_channels_channel WHERE id=$1").bind(channel).fetch_optional(&mut c).await.map_err(sql)?.flatten();let channel_name=channel_name.ok_or(Error::Unsupported("channel name capacity/missing"))?;
    if normalized(&title)==normalized(&channel_name){return Err(Error::Unsupported("channel-name title fallback branches"));}
    let epg:Option<(Option<Value>,bool)>=sqlx::query_as("SELECT CASE WHEN custom_properties IS NULL THEN '{}'::jsonb WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,(custom_properties IS NULL OR octet_length(custom_properties::text)<=8192) FROM epg_programdata WHERE id=$1").bind(program_id).fetch_optional(&mut c).await.map_err(sql)?;
    let epg=match epg{None=>json!({}),Some((Some(value),true))=>value,_=>return Err(Error::Unsupported("programme metadata capacity"))};let props=epg.as_object().ok_or(Error::Unsupported("programme metadata object"))?;
    // Existing VOD logo is Source stage2. Any possible higher-priority EPG
    // image must wait for that branch's adapter, never silently lose precedence.
    if props.keys().any(|k|!matches!(k.as_str(),"season"|"episode"|"rating"|"rating_system"|"onscreen_episode")){return Err(Error::Unsupported("EPG artwork precedence branch"));}
    let mut logo=None;
    for query in ["SELECT l.id::bigint FROM vod_movie m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE UPPER(m.name::text)=UPPER($1) ORDER BY m.name LIMIT 2","SELECT l.id::bigint FROM vod_series m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE UPPER(m.name::text)=UPPER($1) ORDER BY m.name LIMIT 2"]{
     let rows:Vec<Option<i64>>=sqlx::query_scalar(query).bind(&title).fetch_all(&mut c).await.map_err(sql)?;if rows.len()>1{return Err(Error::Unsupported("ambiguous equal-name ordering"));}logo=rows.into_iter().next().flatten();if logo.is_some(){break;}
    }
    let logo=logo.filter(|v|*v>0).ok_or(Error::Unsupported("provider/direct/channel artwork fallback"))?;
    let mut resolved=properties.clone();let out=resolved.as_object_mut().ok_or(Error::Unsupported("recording object"))?;let mut changed=false;
    let value=json!(logo);if out.get("poster_logo_id").is_none_or(|old|!python_equal(old,&value)){out.insert("poster_logo_id".into(),value);changed=true;}
    for key in ["rating","rating_system","season","episode","onscreen_episode"]{if let Some(value)=props.get(key){let selected=if matches!(key,"season"|"episode"){!value.is_null()}else{truth(value)};if selected&&out.get(key).is_none_or(|old|!python_equal(old,value)){out.insert(key.into(),value.clone());changed=true;}}}
    Ok(Prepared{origin,recording,resolved,changed,skip:None})
   }.await;let ready=c.ping().await.err().map(Box::new);let close=c.close().await.err().map(Box::new);match result{Err(Error::Sql{primary,..})=>Err(Error::Sql{primary,ready,close}),other=>{if ready.is_some()||close.is_some(){Err(Error::Sql{primary:None,ready,close})}else{other}}}
  }).await.map_err(|_|Error::Supervisor)?
    }
    pub async fn execute(
        &self,
        prepared: Prepared,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Completed, Error> {
        if !Arc::ptr_eq(&self.origin, &prepared.origin) {
            return Err(Error::Unsupported("foreign preparation"));
        }
        if let Some(value) = prepared.skip {
            return Ok(Completed {
                recording: prepared.recording,
                changed: false,
                value,
            });
        }
        if !prepared.changed {
            return Ok(Completed {
                recording: prepared.recording,
                changed: false,
                value: "ok".into(),
            });
        }
        let pool = self.pool.clone();
        tokio::spawn(async move{
   let _owner=owner;let mut c=pool.acquire().await.map_err(sql)?.detach();let result=async{
    let fresh:Option<Value>=sqlx::query_scalar("SELECT CASE WHEN custom_properties IS NULL THEN '{}'::jsonb WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END FROM dispatcharr_channels_recording WHERE id=$1").bind(prepared.recording).fetch_optional(&mut c).await.map_err(sql)?.flatten();let fresh=fresh.ok_or(Error::Unsupported("fresh recording capacity/missing"))?;
    let mut merged=fresh.as_object().cloned().ok_or(Error::Unsupported("fresh recording object"))?;let resolved=prepared.resolved.as_object().ok_or(Error::Unsupported("resolved object"))?;
    for key in ["program","poster_logo_id","poster_url","rating","rating_system","season","episode","onscreen_episode"]{if let Some(value)=resolved.get(key){merged.insert(key.into(),value.clone());}}
    let merged=Value::Object(merged);if serde_json::to_vec(&merged).map_err(|_|Error::Unsupported("metadata encoding"))?.len()>8192{return Err(Error::Unsupported("merged metadata capacity"));}
    let changed=sqlx::query("UPDATE dispatcharr_channels_recording SET custom_properties=$1 WHERE id=$2 AND COALESCE(custom_properties,'{}'::jsonb)=$3").bind(merged).bind(prepared.recording).bind(fresh).execute(&mut c).await.map_err(sql)?.rows_affected();if changed!=1{return Err(Error::Unsupported("concurrent metadata write"));}Ok(Completed{recording:prepared.recording,changed:true,value:"ok".into()})
   }.await;let ready=c.ping().await.err().map(Box::new);let close=c.close().await.err().map(Box::new);match result{Err(Error::Sql{primary,..})=>Err(Error::Sql{primary,ready,close}),other=>{if ready.is_some()||close.is_some(){Err(Error::Sql{primary:None,ready,close})}else{other}}}
  }).await.map_err(|_|Error::Supervisor)?
    }
}

#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn source_title_normalization_is_generic() {
        assert_eq!(
            normalized("  A*-Different Programme  "),
            "a different programme"
        );
        assert_ne!(normalized("Other"), normalized("Channel"));
    }
    #[test]
    fn metadata_truth_and_equality_follow_source_python() {
        assert!(!truth(&Value::Null));
        assert!(!truth(&json!([])));
        assert!(!truth(&json!(0)));
        assert!(truth(&json!([0])));
        assert!(python_equal(&json!(true), &json!(1)));
        assert!(python_equal(&json!(2), &json!(2.0)));
    }
}
