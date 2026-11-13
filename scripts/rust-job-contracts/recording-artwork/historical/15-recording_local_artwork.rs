//! Bounded inert local-VODLogo artwork persistence.
//! Original Dispatcharr authors retain all application/algorithm/license credit.
//! No network fallback, task registration, SQL-failure return authority or Main.
use serde_json::{json, Value};
use sqlx::{Connection, PgPool};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Acquire,
    Schema,
    Recording,
    Channel,
    Programme,
    Movie,
    FreshRecording,
    Update,
    Ready,
    Close,
}
#[derive(Debug)]
pub enum Error {
    Unsupported(&'static str),
    Physical {
        stage: Stage,
        source: sqlx::Error,
        settlement: Option<sqlx::Error>,
    },
    Supervisor,
}
fn physical(stage: Stage, source: sqlx::Error) -> Error {
    Error::Physical {
        stage,
        source,
        settlement: None,
    }
}
/// Exact positive reference domain. This deliberately does not imply arbitrary
/// title/case/fallback qualification from one captured Movie/title.
#[derive(Clone)]
pub struct Domain {
    owned_logo_path: String,
}
impl Domain {
    pub fn captured_local(owned_logo_path: String) -> Option<Self> {
        if !owned_logo_path.starts_with("/app/media/rust-owned-parented-artwork/")
            || owned_logo_path != "/app/media/rust-owned-parented-artwork/logo.png"
        {
            return None;
        }
        Some(Self { owned_logo_path })
    }
}
#[derive(Clone, Debug)]
struct Snapshot {
    recording: i64,
    properties: Value,
    logo: i64,
    programme: Value,
}
pub struct Prepared {
    origin: Arc<()>,
    snapshot: Snapshot,
}
pub struct Completed {
    recording: i64,
    changed: bool,
}
impl Completed {
    pub fn recording(&self) -> i64 {
        self.recording
    }
    pub fn changed(&self) -> bool {
        self.changed
    }
}
type ChannelRow = (Option<String>, Option<i64>, Option<i64>, bool);
pub struct LocalArtworkStore {
    pool: PgPool,
    domain: Domain,
    identity: Arc<()>,
}
impl LocalArtworkStore {
    pub fn new(pool: PgPool, domain: Domain) -> Self {
        Self {
            pool,
            domain,
            identity: Arc::new(()),
        }
    }
    pub async fn prepare(
        self: Arc<Self>,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Prepared, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&recording) {
            return Err(Error::Unsupported("recording id"));
        }
        let identity = self.identity.clone();
        let result = self.operation(recording, None, owner).await?;
        match result {
            Outcome::Snapshot(snapshot) => Ok(Prepared {
                origin: identity,
                snapshot,
            }),
            _ => Err(Error::Supervisor),
        }
    }
    pub async fn execute(
        self: Arc<Self>,
        prepared: Prepared,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Completed, Error> {
        if !Arc::ptr_eq(&self.identity, &prepared.origin) {
            return Err(Error::Unsupported("foreign preparation"));
        }
        match self
            .operation(prepared.snapshot.recording, Some(prepared.snapshot), owner)
            .await?
        {
            Outcome::Completed(value) => Ok(value),
            _ => Err(Error::Supervisor),
        }
    }
    async fn operation(
        self: Arc<Self>,
        recording: i64,
        expected: Option<Snapshot>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Outcome, Error> {
        tokio::spawn(async move {
            let _owner=owner;
            let mut connection=self.pool.acquire().await.map_err(|e|physical(Stage::Acquire,e))?;
            let observed=async {
                let custom:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN ('dispatcharr_channels_recording'::regclass,'dispatcharr_channels_channel'::regclass,'epg_programdata'::regclass,'vod_movie'::regclass,'vod_vodlogo'::regclass))")
                    .fetch_one(&mut *connection).await.map_err(|e|physical(Stage::Schema,e))?;
                if custom {return Err(Error::Unsupported("unqualified artwork model trigger"));}
                let rows:Vec<(i64,Option<Value>,bool)>=sqlx::query_as("SELECT channel_id::bigint,CASE WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,(custom_properties IS NOT NULL AND octet_length(custom_properties::text)<=8192) FROM dispatcharr_channels_recording WHERE id=$1 LIMIT 2")
                    .bind(recording).fetch_all(&mut *connection).await.map_err(|e|physical(Stage::Recording,e))?;
                let [(channel,Some(properties),true)]=rows.as_slice() else{return Err(Error::Unsupported("recording snapshot"));};
                if !metadata_supported(properties) {return Err(Error::Unsupported("recording optional metadata domain"));}
                let Some(program)=properties.get("program").and_then(Value::as_object)else{return Err(Error::Unsupported("manual recording"));};
                if properties.as_object().is_none() || properties.get("status").is_some() || program.get("title").and_then(Value::as_str)!=Some("Owned Parented Artwork") {
                    return Err(Error::Unsupported("recording title/status domain"));
                }
                let programme=program.get("id").and_then(Value::as_i64).filter(|id|*id>0).ok_or(Error::Unsupported("programme id"))?;
                let channels:Vec<ChannelRow>=sqlx::query_as("SELECT CASE WHEN octet_length(name)<=255 THEN name ELSE NULL END,logo_id::bigint,epg_data_id::bigint,(octet_length(name)<=255) FROM dispatcharr_channels_channel WHERE id=$1 LIMIT 2")
                    .bind(channel).fetch_all(&mut *connection).await.map_err(|e|physical(Stage::Channel,e))?;
                if !matches!(channels.as_slice(),[(Some(name),None,Some(_),true)] if name=="Owned Parented Channel") {return Err(Error::Unsupported("channel fallback/domain"));}
                let programmes:Vec<(Option<Value>,bool)>=sqlx::query_as("SELECT CASE WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,(custom_properties IS NOT NULL AND octet_length(custom_properties::text)<=8192) FROM epg_programdata WHERE id=$1 LIMIT 2")
                    .bind(programme).fetch_all(&mut *connection).await.map_err(|e|physical(Stage::Programme,e))?;
                let [(Some(props),true)]=programmes.as_slice()else{return Err(Error::Unsupported("programme properties"));};
                if *props!=json!({"season":1,"episode":1}) {return Err(Error::Unsupported("unqualified EPG image/rating metadata"));}
                // Source orders Movie by name and takes first. Refuse ambiguous
                // case-folded matches rather than invent an equal-name tie order.
                let movies:Vec<(Option<i64>,Option<String>,bool)>=sqlx::query_as("SELECT l.id::bigint,CASE WHEN octet_length(l.url)<=1024 THEN l.url ELSE NULL END,(l.url IS NOT NULL AND octet_length(l.url)<=1024) FROM vod_movie m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE UPPER(m.name::text)=UPPER($1) ORDER BY m.name LIMIT 2")
                    .bind("Owned Parented Artwork").fetch_all(&mut *connection).await.map_err(|e|physical(Stage::Movie,e))?;
                let [(Some(logo),Some(path),true)]=movies.as_slice()else{return Err(Error::Unsupported("missing/ambiguous local Movie logo"));};
                if *logo<=0 || path!=&self.domain.owned_logo_path {return Err(Error::Unsupported("nonlocal/unqualified logo"));}
                let snapshot=Snapshot {recording,properties:properties.clone(),logo:*logo,programme:props.clone()};
                let Some(expected)=expected else{return Ok(Outcome::Snapshot(snapshot));};
                if expected.logo!=snapshot.logo || expected.programme!=snapshot.programme || expected.properties!=snapshot.properties {return Err(Error::Unsupported("post-admission snapshot changed"));}
                let mut merged=snapshot.properties.clone();
                let fields=merged.as_object_mut().ok_or(Error::Unsupported("properties object"))?;
                fields.insert("poster_logo_id".into(),json!(snapshot.logo));fields.insert("season".into(),json!(1));fields.insert("episode".into(),json!(1));
                if merged==snapshot.properties {return Ok(Outcome::Completed(Completed {recording,changed:false}));}
                // Source refreshes properties before its metadata-only save. The
                // serial corpus has no concurrent writer. Preserve fresh status/data
                // in the merge, without claiming general concurrent parity.
                let fresh:Vec<(Option<Value>,bool)>=sqlx::query_as("SELECT CASE WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties ELSE NULL END,(custom_properties IS NOT NULL AND octet_length(custom_properties::text)<=8192) FROM dispatcharr_channels_recording WHERE id=$1 LIMIT 2")
                    .bind(recording).fetch_all(&mut *connection).await.map_err(|e|physical(Stage::FreshRecording,e))?;
                let [(Some(fresh),true)]=fresh.as_slice() else{return Err(Error::Unsupported("fresh recording bounds"));};
                // Source copies its own metadata keys into the freshly read row;
                // preserve status and unrelated fresh fields, then use CAS to
                // refuse a further concurrent write rather than overwrite it.
                merged=merge_fresh(fresh,&merged)?;
                let result=sqlx::query("UPDATE dispatcharr_channels_recording SET custom_properties=$1 WHERE id=$2 AND custom_properties=$3")
                    .bind(merged).bind(recording).bind(fresh).execute(&mut *connection).await.map_err(|e|physical(Stage::Update,e))?;
                if result.rows_affected()!=1 {return Err(Error::Unsupported("concurrent metadata change"));}
                Ok(Outcome::Completed(Completed {recording,changed:true}))
            }.await;
            // Success, unsupported and actual ErrorResponse exits all retain the
            // same owner until explicit Sync/Ready and physical close settle.
            let ready=connection.ping().await;let closed=connection.close().await;
            match observed {
                Err(Error::Physical {stage,source,..})=>Err(Error::Physical {stage,source,settlement:ready.err().or_else(||closed.err())}),
                other=>{ready.map_err(|e|physical(Stage::Ready,e))?;closed.map_err(|e|physical(Stage::Close,e))?;other}
            }
        }).await.map_err(|_|Error::Supervisor)?
    }
}
enum Outcome {
    Snapshot(Snapshot),
    Completed(Completed),
}

fn merge_fresh(fresh: &Value, resolved: &Value) -> Result<Value, Error> {
    let mut out = fresh
        .as_object()
        .cloned()
        .ok_or(Error::Unsupported("fresh properties object"))?;
    let resolved = resolved
        .as_object()
        .ok_or(Error::Unsupported("resolved properties object"))?;
    for key in [
        "program",
        "poster_logo_id",
        "poster_url",
        "rating",
        "rating_system",
        "season",
        "episode",
        "onscreen_episode",
    ] {
        if let Some(value) = resolved.get(key) {
            out.insert(key.to_owned(), value.clone());
        }
    }
    let out = Value::Object(out);
    if serde_json::to_vec(&out)
        .map_err(|_| Error::Unsupported("properties encoding"))?
        .len()
        > 8192
    {
        return Err(Error::Unsupported("merged properties capacity"));
    }
    Ok(out)
}
#[cfg(test)]
mod merge_controls {
    use super::*;
    #[test]
    fn source_metadata_merge_preserves_new_status_and_unowned_fields() {
        let fresh = json!({"status":"recording","stop_requested":true,"program":{"title":"new"},"unrelated":[1]});
        let resolved = json!({"program":{"title":"Owned Parented Artwork"},"poster_logo_id":7,"season":1,"episode":1});
        let got = merge_fresh(&fresh, &resolved).unwrap();
        assert_eq!(got["status"], "recording");
        assert_eq!(got["stop_requested"], true);
        assert_eq!(got["unrelated"], json!([1]));
        assert_eq!(got["program"], resolved["program"]);
        assert_eq!(got["poster_logo_id"], 7);
        assert_eq!(merge_fresh(&got, &resolved).unwrap(), got);
    }
    #[test]
    fn merge_refuses_nonobjects_and_combined_overflow() {
        for fresh in [Value::Null, json!([])] {
            assert!(merge_fresh(&fresh, &json!({})).is_err());
        }
        let fresh = json!({"unrelated":"x".repeat(8100)});
        let resolved = json!({"program":{"title":"x".repeat(200)}});
        assert!(merge_fresh(&fresh, &resolved).is_err());
    }
}

// Python bool/float equality with integer1 is outside the captured domain;
// reject it before ACK instead of silently changing an event/no-op decision.
fn metadata_supported(value: &Value) -> bool {
    let Some(fields) = value.as_object() else {
        return false;
    };
    fields
        .keys()
        .all(|key| ["program", "poster_logo_id", "season", "episode"].contains(&key.as_str()))
        && ["season", "episode"]
            .iter()
            .all(|key| fields.get(*key).is_none_or(|v| v.as_i64() == Some(1)))
        && fields.get("poster_logo_id").is_none_or(|v| {
            v.as_i64()
                .is_some_and(|n| (1..=i64::from(i32::MAX)).contains(&n))
        })
}
#[cfg(test)]
mod optional_metadata_controls {
    use super::*;
    #[test]
    fn initial_and_resolved_integer_fields_admit_but_python_equality_coercions_do_not() {
        assert!(metadata_supported(&json!({"program":{}})));
        assert!(metadata_supported(
            &json!({"program":{},"season":1,"episode":1,"poster_logo_id":1})
        ));
        for value in [json!(true), json!(1.0), json!("1"), Value::Null, json!([])] {
            for field in ["season", "episode", "poster_logo_id"] {
                let mut props = json!({"program":{}});
                props[field] = value.clone();
                assert!(!metadata_supported(&props));
            }
        }
        for props in [
            json!({"program":{},"poster_url":"https://unqualified.invalid"}),
            json!({"program":{},"status":"recording"}),
            json!({"program":{},"rating":5}),
        ] {
            assert!(!metadata_supported(&props));
        }
    }
}
