//! Stable-build version notices, derived from the pinned Original task/model.
//! Original Dispatcharr/Django/Requests/Channels authors retain all credit.
//! Source4 supplies update/no-update/repeat/malformed reference behavior.
use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};
use std::{
    cmp::Ordering,
    io::Read,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;

const BODY: usize = 65536;
#[derive(Clone)]
pub struct Options {
    pub version: String,
    pub endpoint: String,
    pub ca_bundle: PathBuf,
}
#[derive(Debug)]
pub enum Error {
    Unsupported(&'static str),
    Capacity,
    Supervisor,
    Http(String),
    Event,
    Sql {
        primary: Option<Box<sqlx::Error>>,
        rollback: Option<Box<sqlx::Error>>,
        ready: Option<Box<sqlx::Error>>,
        close: Option<Box<sqlx::Error>>,
    },
}
#[derive(Clone, Debug)]
pub struct Report {
    pub caught_source_exception: Option<String>,
}
#[derive(Clone)]
pub struct Service {
    pool: PgPool,
    sink: Arc<dyn UpdateEventSink>,
    options: Options,
    uncertain: Arc<Mutex<Option<Arc<dyn Send + Sync>>>>,
}
struct Receipt {
    owner: Option<Arc<dyn Send + Sync>>,
    done: Option<oneshot::Sender<()>>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        drop(self.owner.take());
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
fn now() -> DateTime<Utc> {
    let n = Utc::now();
    n.with_nanosecond(n.nanosecond() / 1000 * 1000)
        .expect("microsecond clock")
}
fn iso(n: DateTime<Utc>) -> String {
    n.to_rfc3339_opts(
        if n.nanosecond() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        false,
    )
}
fn stable(raw: &str) -> Option<Vec<String>> {
    if raw.is_empty() || raw.len() > 128 {
        return None;
    }
    let mut parts = Vec::new();
    for part in raw.split('.') {
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let n = part.trim_start_matches('0');
        parts.push(if n.is_empty() {
            "0".to_owned()
        } else {
            n.to_owned()
        });
    }
    if parts.len() > 16 {
        return None;
    }
    while parts.len() > 1 && parts.last().is_some_and(|p| p == "0") {
        parts.pop();
    }
    Some(parts)
}
fn compare(a: &[String], b: &[String]) -> Ordering {
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).map(String::as_str).unwrap_or("0");
        let y = b.get(i).map(String::as_str).unwrap_or("0");
        let c = x.len().cmp(&y.len()).then_with(|| x.cmp(y));
        if c != Ordering::Equal {
            return c;
        }
    }
    Ordering::Equal
}
impl Service {
    /// Production URL is literal Source GitHub. Dependency substitution is
    /// limited to an explicitly owned copied-fixture localhost endpoint.
    pub fn prepare(
        pool: PgPool,
        sink: Arc<dyn UpdateEventSink>,
        options: Options,
    ) -> Result<Self, Error> {
        if stable(&options.version).is_none() {
            return Err(Error::Unsupported("Stable source build version required"));
        }
        let production = options.endpoint
            == "https://api.github.com/repos/Dispatcharr/Dispatcharr/releases/latest";
        if !production {
            let url = reqwest::Url::parse(&options.endpoint)
                .map_err(|_| Error::Unsupported("Version endpoint"))?;
            let owned = std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref()
                == Ok("playback-fixture-only")
                && std::env::var("POSTGRES_DB")
                    .ok()
                    .is_some_and(|v| v.starts_with("rust_probe_backup_jobs_"));
            if !owned
                || url.scheme() != "http"
                || url.host_str() != Some("127.0.0.1")
                || url.port().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.path() != "/owned-version"
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(Error::Unsupported("Owned Source HTTP dependency only"));
            }
        }
        if production {
            let file = std::fs::File::open(&options.ca_bundle)
                .map_err(|_| Error::Unsupported("Installed Source CA bundle required"))?;
            let mut bytes = Vec::new();
            file.take(1048577)
                .read_to_end(&mut bytes)
                .map_err(|_| Error::Unsupported("Readable Source CA bundle required"))?;
            if bytes.len() > 1048576
                || reqwest::Certificate::from_pem_bundle(&bytes)
                    .map_err(|_| Error::Unsupported("Source CA certificate bundle"))?
                    .is_empty()
            {
                return Err(Error::Unsupported("Source CA certificate capacity"));
            }
        }
        Ok(Self {
            pool,
            sink,
            options,
            uncertain: Arc::new(Mutex::new(None)),
        })
    }
    async fn emit(&self, value: Value, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let receipt = Arc::new(Receipt {
            owner: Some(owner),
            done: Some(tx),
        });
        let observed = self
            .sink
            .send_retained("updates", value, receipt.clone())
            .await;
        drop(receipt);
        rx.await.map_err(|_| Error::Event)?;
        // Source catches ordinary Channels delivery failure. Completion of its
        // original physical owner is still required before returning SUCCESS.
        match observed {
            Ok(()) | Err(dispatcharr_contracts::UpdateEventError::Unavailable) => Ok(()),
            Err(_) => Err(Error::Event),
        }
    }
    async fn cleared(&self, count: u64, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        if count > 0 {
            self.emit(json!({"type":"update","data":{"success":true,"type":"notifications_cleared","count":count}}),owner).await?;
        }
        Ok(())
    }
    pub async fn execute(&self, owner: Arc<dyn Send + Sync>) -> Result<Report, Error> {
        match self.execute_inner(owner.clone()).await {
            Ok(()) => Ok(Report {
                caught_source_exception: None,
            }),
            Err(error @ Error::Sql { ready: Some(_), .. })
            | Err(error @ Error::Sql { close: Some(_), .. }) => {
                // A failed settlement probe cannot establish remote SQL fate.
                // Retain this original admission until owned process exit;
                // live reconciliation is outside the current Source scope.
                *self.uncertain.lock().map_err(|_| Error::Supervisor)? = Some(owner);
                Err(error)
            }
            // Pinned Source catches requests and general task exceptions. Only
            // failures of original operations qualify, after owned SQL Ready/
            // close or dedicated HTTP runtime join have physically completed.
            Err(
                error @ Error::Sql {
                    primary: Some(_), ..
                },
            )
            | Err(error @ Error::Http(_)) => {
                let diagnostic = format!("{error:?}").chars().take(4096).collect();
                Ok(Report {
                    caught_source_exception: Some(diagnostic),
                })
            }
            Err(error) => Err(error),
        }
    }
    async fn execute_inner(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let current = format!("version-{}", self.options.version);
        // Source clears the CURRENT version before its first HTTP request.
        let count = delete(self.pool.clone(), Delete::Current(current), owner.clone()).await?;
        self.cleared(count, owner.clone()).await?;
        let Some(data) = fetch(self.options.clone(), owner.clone()).await? else {
            return Ok(());
        };
        let Some(object) = data.as_object() else {
            return Ok(());
        };
        let tag = match object.get("tag_name") {
            None => return Ok(()),
            Some(Value::String(tag)) => tag.trim_start_matches('v'),
            Some(_) => return Ok(()),
        };
        if tag.is_empty() {
            return Ok(());
        }
        // PEP440 needs a numeric release component. Known invalid nonnumeric
        // tags are caught by Source; plausible prerelease/dev forms remain an
        // explicit unsupported domain instead of silently comparing semver.
        if !tag.bytes().any(|b| b.is_ascii_digit()) {
            return Ok(());
        }
        let Some(latest) = stable(tag) else {
            return Err(Error::Unsupported(
                "Unmeasured PEP440 prerelease/dev/local tag",
            ));
        };
        let current =
            stable(&self.options.version).ok_or(Error::Unsupported("Stable source version"))?;
        if compare(&latest, &current) != Ordering::Greater {
            let count = delete(self.pool.clone(), Delete::All, owner.clone()).await?;
            return self.cleared(count, owner).await;
        }
        if tag.len() > 247 {
            return Err(Error::Capacity);
        }
        let release_url = object.get("html_url").cloned().unwrap_or(json!(""));
        if release_url.to_string().len() > 8192 {
            return Err(Error::Capacity);
        }
        let count = delete(
            self.pool.clone(),
            Delete::Other(format!("version-{tag}")),
            owner.clone(),
        )
        .await?;
        self.cleared(count, owner.clone()).await?;
        let (created, notice) = upsert(
            self.pool.clone(),
            tag.to_owned(),
            release_url,
            owner.clone(),
        )
        .await?;
        if created {
            self.emit(json!({"type":"update","data":{"type":"system_notification","notification":notice}}),owner).await?;
        }
        Ok(())
    }
}
async fn fetch(options: Options, owner: Arc<dyn Send + Sync>) -> Result<Option<Value>, Error> {
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        let mut builder = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::limited(30))
            .cookie_store(true)
            .pool_max_idle_per_host(0)
            .user_agent(format!("Dispatcharr/{}", options.version));
        if options.endpoint.starts_with("https:") {
            let file =
                std::fs::File::open(&options.ca_bundle).map_err(|e| Error::Http(e.to_string()))?;
            let mut pem = Vec::new();
            file.take(1048577)
                .read_to_end(&mut pem)
                .map_err(|e| Error::Http(e.to_string()))?;
            if pem.len() > 1048576 {
                return Err(Error::Capacity);
            }
            builder = builder.tls_built_in_root_certs(false);
            for certificate in reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|e| Error::Http(e.to_string()))?
            {
                builder = builder.add_root_certificate(certificate);
            }
        } else {
            builder = builder.no_proxy();
        }
        // Dedicated blocking client Drop joins its runtime thread in pinned
        // reqwest. Response/decoder/client drop BEFORE the retained caller.
        let client = builder.build().map_err(|e| Error::Http(e.to_string()))?;
        let output = (|| {
            let mut response = match client
                .get(&options.endpoint)
                .header("Accept", "application/vnd.github.v3+json")
                .header("Accept-Encoding", "gzip, deflate")
                .header("Connection", "keep-alive")
                .send()
            {
                Ok(r) => r,
                Err(_) => return Ok(None),
            };
            if response.status().as_u16() != 200 {
                // requests.get eagerly receives its body before Source checks
                // status. Preserve complete bounded upstream consumption.
                let mut ignored = Vec::new();
                response
                    .by_ref()
                    .take(BODY as u64 + 1)
                    .read_to_end(&mut ignored)
                    .map_err(|e| Error::Http(e.to_string()))?;
                if ignored.len() > BODY {
                    return Err(Error::Capacity);
                }
                return Ok(None);
            }
            let encoding = response
                .headers()
                .get("Content-Encoding")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .to_ascii_lowercase();
            let mut body = Vec::new();
            let reader: &mut dyn Read = match encoding.as_str() {
                "" | "identity" => &mut response,
                _ => {
                    return decode_response(response, &encoding);
                }
            };
            reader
                .take(BODY as u64 + 1)
                .read_to_end(&mut body)
                .map_err(|e| Error::Http(e.to_string()))?;
            if body.len() > BODY {
                return Err(Error::Capacity);
            }
            Ok(serde_json::from_slice(&body).ok())
        })();
        drop(client);
        output
    })
    .await
    .map_err(|_| Error::Supervisor)?
}
fn decode_response(
    mut response: reqwest::blocking::Response,
    encoding: &str,
) -> Result<Option<Value>, Error> {
    let mut input = Vec::new();
    response
        .by_ref()
        .take(BODY as u64 + 1)
        .read_to_end(&mut input)
        .map_err(|e| Error::Http(e.to_string()))?;
    if input.len() > BODY {
        return Err(Error::Capacity);
    }
    drop(response);
    let mut output = Vec::new();
    match encoding {
        "gzip" => {
            flate2::read::MultiGzDecoder::new(input.as_slice())
                .take(BODY as u64 + 1)
                .read_to_end(&mut output)
                .map_err(|e| Error::Http(e.to_string()))?;
        }
        "deflate" => {
            if flate2::read::ZlibDecoder::new(input.as_slice())
                .take(BODY as u64 + 1)
                .read_to_end(&mut output)
                .is_err()
            {
                output.clear();
                flate2::read::DeflateDecoder::new(input.as_slice())
                    .take(BODY as u64 + 1)
                    .read_to_end(&mut output)
                    .map_err(|e| Error::Http(e.to_string()))?;
            }
        }
        _ => return Err(Error::Unsupported("HTTP content encoding")),
    };
    if output.len() > BODY {
        return Err(Error::Capacity);
    }
    Ok(serde_json::from_slice(&output).ok())
}

enum Delete {
    Current(String),
    All,
    Other(String),
}
async fn finish<T>(
    mut connection: PgConnection,
    result: Result<T, Error>,
    transaction_open: bool,
) -> Result<T, Error> {
    let rollback = if result.is_err() && transaction_open {
        sqlx::query("ROLLBACK")
            .execute(&mut connection)
            .await
            .err()
            .map(Box::new)
    } else {
        None
    };
    // Query errors can precede Ready. A real ping and physical close settle the
    // owned connection before any result or caught failure releases its caller.
    let ready = connection.ping().await.err().map(Box::new);
    let close = connection.close().await.err().map(Box::new);
    match result {
        Err(Error::Sql { primary, .. }) => Err(Error::Sql {
            primary,
            rollback,
            ready,
            close,
        }),
        Err(other) => {
            if rollback.is_some() || ready.is_some() || close.is_some() {
                Err(Error::Sql {
                    primary: None,
                    rollback,
                    ready,
                    close,
                })
            } else {
                Err(other)
            }
        }
        Ok(value) => {
            if ready.is_some() || close.is_some() {
                Err(Error::Sql {
                    primary: None,
                    rollback,
                    ready,
                    close,
                })
            } else {
                Ok(value)
            }
        }
    }
}
fn sql(error: sqlx::Error) -> Error {
    Error::Sql {
        primary: Some(Box::new(error)),
        rollback: None,
        ready: None,
        close: None,
    }
}
async fn delete(
    pool: PgPool,
    selection: Delete,
    owner: Arc<dyn Send + Sync>,
) -> Result<u64, Error> {
    tokio::spawn(async move{
        let _owner=owner;let mut connection=pool.acquire().await.map_err(sql)?.detach();let mut open=false;
        let result=async{
            sqlx::query("BEGIN").execute(&mut connection).await.map_err(sql)?;open=true;
            let(mode,key)=match selection{Delete::Current(key)=>(0,Some(key)),Delete::All=>(1,None),Delete::Other(key)=>(2,Some(key))};
            let ids:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemnotification WHERE notification_type='version_update' AND ($1::integer=1 OR ($1=0 AND notification_key=$2) OR ($1=2 AND notification_key<>$2)) ORDER BY priority DESC,created_at DESC LIMIT 129 FOR UPDATE").bind(mode).bind(key).fetch_all(&mut connection).await.map_err(sql)?;
            if ids.len()>128{return Err(Error::Capacity);}
            let related:i64=sqlx::query_scalar("SELECT count(*) FROM core_notificationdismissal WHERE notification_id=ANY($1)").bind(&ids).fetch_one(&mut connection).await.map_err(sql)?;if related>1024{return Err(Error::Capacity);}
            let children=sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id=ANY($1)").bind(&ids).execute(&mut connection).await.map_err(sql)?.rows_affected();
            let parents=sqlx::query("DELETE FROM core_systemnotification WHERE id=ANY($1)").bind(&ids).execute(&mut connection).await.map_err(sql)?.rows_affected();
            sqlx::query("COMMIT").execute(&mut connection).await.map_err(sql)?;open=false;Ok(children+parents)
        }.await;finish(connection,result,open).await
    }).await.map_err(|_|Error::Supervisor)?
}
async fn upsert(
    pool: PgPool,
    version: String,
    release_url: Value,
    owner: Arc<dyn Send + Sync>,
) -> Result<(bool, Value), Error> {
    tokio::spawn(async move{
        let _owner=owner;let mut connection=pool.acquire().await.map_err(sql)?.detach();let mut open=false;
        let result=async{
            sqlx::query("BEGIN").execute(&mut connection).await.map_err(sql)?;open=true;
            let key=format!("version-{version}");let title=format!("Version {version} Available");let message=format!("A new version of Dispatcharr ({version}) is available.");let action=json!({"version":version,"release_url":release_url,"release_notes":null});
            let existing:Option<(i64,DateTime<Utc>)>=sqlx::query_as("SELECT id::bigint,created_at FROM core_systemnotification WHERE notification_key=$1 FOR UPDATE").bind(&key).fetch_optional(&mut connection).await.map_err(sql)?;
            let mut created=false;let mut row=existing;
            if row.is_none(){
                // Sample both Django auto fields independently; never set an
                // existing created_at to NOW or hide update timestamps in tests.
                let created_at=now();let updated_at=now();
                row=sqlx::query_as("INSERT INTO core_systemnotification(notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,expires_at,created_at,updated_at) VALUES($1,'version_update','high','system',$2,$3,$4,true,true,NULL,$5,$6) ON CONFLICT(notification_key) DO NOTHING RETURNING id::bigint,created_at").bind(&key).bind(&title).bind(&message).bind(&action).bind(created_at).bind(updated_at).fetch_optional(&mut connection).await.map_err(sql)?;
                created=row.is_some();
                if row.is_none(){row=sqlx::query_as("SELECT id::bigint,created_at FROM core_systemnotification WHERE notification_key=$1 FOR UPDATE").bind(&key).fetch_optional(&mut connection).await.map_err(sql)?;}
            }
            let(id,created_at)=row.ok_or(Error::Unsupported("Version notice disappeared"))?;
            if !created {
                // update_or_create preserves source/expiry/created_at while
                // reactivating and updating all declared defaults and auto_now.
                sqlx::query("UPDATE core_systemnotification SET notification_type='version_update',priority='high',title=$2,message=$3,action_data=$4,is_active=true,admin_only=true,updated_at=$5 WHERE id=$1").bind(id).bind(&title).bind(&message).bind(&action).bind(now()).execute(&mut connection).await.map_err(sql)?;
            }
            let notice=json!({"id":id,"notification_key":key,"notification_type":"version_update","priority":"high","title":title,"message":message,"action_data":action,"is_active":true,"admin_only":true,"created_at":iso(created_at)});
            sqlx::query("COMMIT").execute(&mut connection).await.map_err(sql)?;open=false;Ok((created,notice))
        }.await;finish(connection,result,open).await
    }).await.map_err(|_|Error::Supervisor)?
}
