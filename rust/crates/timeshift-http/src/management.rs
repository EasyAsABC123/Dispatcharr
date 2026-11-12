//! Original Timeshift admin routes ported from apps/timeshift/stats_views.py.
//! All original application credit belongs to Dispatcharr's authors/contributors.
use crate::{management_data, management_programmes, CatchupMedia};
use axum::{
    body::Body,
    extract::{ConnectInfo, OriginalUri, State},
    http::{Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{AuthService, Store, StoreError};
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

struct Inner {
    store: Store,
    auth: AuthService,
    transport: RedisTransport,
    media: CatchupMedia,
    tasks: Arc<Semaphore>,
    stopped: AtomicBool,
}
#[derive(Clone)]
pub struct CatchupManagement {
    inner: Arc<Inner>,
    events: Option<Arc<dyn UpdateEventSink>>,
}
fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Timeshift management unavailable."}))
}
impl CatchupManagement {
    pub async fn new(
        store: Store,
        auth: AuthService,
        media: CatchupMedia,
        redis_url: &str,
    ) -> Result<Self, StoreError> {
        let transport = RedisTransport::connect(
            redis_url,
            TransportConfig {
                max_operations: 16,
                max_queued_operations: 128,
                command_timeout: Duration::from_secs(3),
            },
        )
        .await
        .map_err(|_| unavailable())?;
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                auth,
                transport,
                media,
                tasks: Arc::new(Semaphore::new(16)),
                stopped: AtomicBool::new(false),
            }),
            events: None,
        })
    }
    pub fn with_events(mut self, events: Arc<dyn UpdateEventSink>) -> Self {
        self.events = Some(events);
        self
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/proxy/catchup/stats/", any(dispatch))
            .route("/proxy/catchup/programs/", any(dispatch))
            .route("/proxy/catchup/stop_client/", any(dispatch))
            .with_state(self)
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.stopped.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.inner.tasks.available_permits() != 16 {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
        })
        .await
        .is_ok()
    }
    pub async fn statistics(&self) -> Value {
        management_data::stats(&self.inner.store, &self.inner.transport, now()).await
    }
    /// Same Source catch-up projection, retaining the caller through actual
    /// Redis work. An owning supervisor retains SQL on observer cancellation.
    pub async fn statistics_retained(&self, caller: Arc<dyn Send + Sync>) -> Value {
        management_data::stats_guarded(&self.inner.store, &self.inner.transport, now(), caller)
            .await
    }
    async fn publish(&self) {
        if let Some(events) = &self.events {
            let stats = self.statistics().await;
            let message = json!({"type":"update","data":{"success":true,"type":"timeshift_stats","stats":stats.to_string()}});
            let _ =
                tokio::time::timeout(Duration::from_secs(3), events.send("updates", message)).await;
        }
    }
    async fn stop(&self, sid: String) -> Result<bool, StoreError> {
        if self.inner.stopped.load(Ordering::SeqCst) {
            return Err(unavailable());
        }
        let permit = self
            .inner
            .tasks
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let this = self.clone();
        // Mutation, local wakeup and event dispatch survive HTTP cancellation.
        tokio::spawn(async move {
            let _permit = permit;
            let (found, _): (i64, ()) = this
                .inner
                .transport
                .command(STOP, &[], &[sid.as_bytes()], ())
                .await
                .map_err(|_| StoreError::StatusBody(500, json!({"error":"Stop failed"})))?;
            if found == 0 {
                return Ok(false);
            }
            this.inner.media.signal_management_stop(&sid);
            this.publish().await;
            Ok(true)
        })
        .await
        .map_err(|_| unavailable())?
    }
}
const STOP: &str = r#"
local sid=ARGV[1];local cursor='0';local scanned=0;local id=nil
repeat
 local result=redis.call('SCAN',cursor,'MATCH','timeshift:channel:*:clients:'..sid,'COUNT',100);cursor=result[1]
 for _,key in ipairs(result[2]) do id=string.match(key,'^timeshift:channel:(.+):clients:.+$');if id then break end end
 scanned=scanned+#result[2];if scanned>50000 then return redis.error_reply('session scan limit') end
until id or cursor=='0'
if not id then return 0 end
local client='timeshift:channel:'..id..':clients:'..sid
local vid=redis.call('HGET',client,'programme_vid') or id
local channels={vid};local generations={};cursor='0';scanned=0
repeat
 local result=redis.call('SCAN',cursor,'MATCH','timeshift:stream_gen:*:'..sid,'COUNT',100);cursor=result[1]
 for _,key in ipairs(result[2]) do
  generations[#generations+1]=key
  local channel=string.sub(key,#'timeshift:stream_gen:'+1,#key-#sid-1)
  if channel~='' then channels[#channels+1]=channel end
 end
 scanned=scanned+#result[2];if scanned>50000 then return redis.error_reply('generation scan limit') end
until cursor=='0'
for _,channel in ipairs(channels) do redis.call('SET','timeshift:channel:'..channel..':client:'..sid..':stop','admin','EX',60) end
redis.call('DEL','timeshift:grace:'..id..':'..sid,client,'timeshift:session:'..sid,'timeshift:pool:'..sid..':superseded')
redis.call('SREM','timeshift:channel:'..id..':clients',sid)
if redis.call('SCARD','timeshift:channel:'..id..':clients')==0 then redis.call('DEL','timeshift:channel:'..id..':clients','timeshift:channel:'..id..':metadata') end
for _,key in ipairs(generations) do redis.call('DEL',key) end
return 1
"#;
fn response(value: Value, status: StatusCode, allow: &str) -> Response {
    let mut response = (status, axum::Json(value)).into_response();
    response
        .headers_mut()
        .insert("allow", allow.parse().unwrap());
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    response
}
fn error(error: StoreError, allow: &str) -> Response {
    let mut response = error.into_response();
    response
        .headers_mut()
        .insert("allow", allow.parse().unwrap());
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    response
}
async fn dispatch(
    State(engine): State<CatchupManagement>,
    OriginalUri(uri): OriginalUri,
    request: Request<Body>,
) -> Response {
    let stats = uri.path().ends_with("/stats/");
    let programs = uri.path().ends_with("/programs/");
    let allow = if stats {
        "GET, OPTIONS"
    } else {
        "POST, OPTIONS"
    };
    let Some(peer) = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|value| value.0)
    else {
        return error(unavailable(), allow);
    };
    if let Err(failure) = engine
        .inner
        .auth
        .account_user(peer, request.headers().clone(), true)
        .await
    {
        return error(failure, allow);
    }
    let method = request.method().clone();
    if method == Method::OPTIONS {
        let (name, description) = if stats {
            (
                "Timeshift Stats",
                "Return active catch-up viewer sessions for the stats page.",
            )
        } else if programs {
            (
                "Catchup Programmes",
                "Return EPG metadata for active catch-up sessions (batch).",
            )
        } else {
            (
                "Stop Timeshift Session",
                "Stop a catch-up viewer by session id (one session = one stats channel).",
            )
        };
        return response(
            json!({"name":name,"description":description,"renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}),
            StatusCode::OK,
            allow,
        );
    }
    if method != if stats { Method::GET } else { Method::POST } {
        return response(
            json!({"detail":format!("Method \"{method}\" not allowed.")}),
            StatusCode::METHOD_NOT_ALLOWED,
            allow,
        );
    }
    if stats {
        return response(engine.statistics().await, StatusCode::OK, allow);
    }
    let bytes = match axum::body::to_bytes(request.into_body(), 64 * 1024).await {
        Ok(value) => value,
        Err(_) => {
            return error(
                StoreError::Unsupported("management body limit exceeded"),
                allow,
            )
        }
    };
    let body: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(error) => {
            let value = if programs {
                let message = error.to_string();
                let reason = if message.starts_with("key must be a string")
                    || message.starts_with("EOF while parsing an object")
                {
                    "Expecting property name enclosed in double quotes"
                } else {
                    "Expecting value"
                };
                // serde reports EOF at the previous byte; Python reports the
                // insertion position after the input, counted in characters.
                let input = String::from_utf8_lossy(&bytes);
                let (line, column, offset) = if error.is_eof() {
                    (
                        input.bytes().filter(|byte| *byte == b'\n').count() + 1,
                        input.rsplit('\n').next().unwrap_or("").chars().count() + 1,
                        input.chars().count(),
                    )
                } else {
                    let line = error.line().max(1);
                    let prefix = input
                        .split_inclusive('\n')
                        .take(line - 1)
                        .collect::<String>();
                    let current = input.lines().nth(line - 1).unwrap_or("");
                    let current_prefix = current
                        .get(..error.column().saturating_sub(1).min(current.len()))
                        .unwrap_or("");
                    (
                        line,
                        current_prefix.chars().count() + 1,
                        prefix.chars().count() + current_prefix.chars().count(),
                    )
                };
                json!({"detail":format!("JSON parse error - {reason}: line {line} column {column} (char {offset})")})
            } else {
                json!({"error":"Invalid JSON"})
            };
            return response(value, StatusCode::BAD_REQUEST, allow);
        }
    };
    if programs {
        let Some(sessions) = body.get("sessions").filter(|v| !v.is_null()) else {
            return response(
                json!({"error":"sessions is required"}),
                StatusCode::BAD_REQUEST,
                allow,
            );
        };
        let Some(sessions) = sessions.as_array() else {
            return response(
                json!({"error":"sessions must be an array"}),
                StatusCode::BAD_REQUEST,
                allow,
            );
        };
        return match management_programmes::programmes(
            &engine.inner.store,
            &engine.inner.transport,
            sessions,
            now(),
        )
        .await
        {
            Ok(value) => response(value, StatusCode::OK, allow),
            Err(failure) => error(failure, allow),
        };
    }
    let Some(sid) = body
        .get("session_id")
        .filter(|v| management_programmes::truthy(v))
    else {
        return response(
            json!({"error":"No session_id provided"}),
            StatusCode::BAD_REQUEST,
            allow,
        );
    };
    match engine.stop(management_programmes::string(sid)).await {
        Ok(true) => response(
            json!({"message":"Timeshift session stop signal sent","session_id":sid}),
            StatusCode::OK,
            allow,
        ),
        Ok(false) => response(
            json!({"error":"Connection not found"}),
            StatusCode::NOT_FOUND,
            allow,
        ),
        Err(failure) => error(failure, allow),
    }
}
