//! Inert generic CoreSettings API, ported from pinned Original Source32.
//! Original Dispatcharr authors retain model/serializer/signal/application credit.
//! No Main registration, new pool, default worker or task-coupled write exists.
use crate::{admin_writes::character, AuthService, RestReadLimits, StoreError, XcUserReadLimits};
use axum::{
    body::HttpBody,
    extract::{ConnectInfo, Path, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::{Connection, Row};
use std::{
    io::{self, Write},
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Notify, OwnedSemaphorePermit};
const RAW_LIMIT: usize = 65536;
const OUTPUT_LIMIT: usize = 1024 * 1024;
// Each of the SAME four existing administrative writer slots can retain two
// separately preflighted4MiB JSON quotes +one bounded1MiB response/scratch.
// This is a conservative component work bound, not a process RSS allowance.
const JSON_WORK: usize = 4 * 1024 * 1024;
const OBSERVER: Duration = Duration::from_secs(15);
#[derive(Clone)]
pub struct CoreSettingsWrites {
    state: Arc<StateData>,
}
/// Bounded internal group result retaining the SAME real request owner.
/// It is not a Main route, static observation cache or uncharged JSON value.
pub struct CoreSettingsGroupRead {
    value: Value,
    _owner: Arc<Lease>,
}
impl CoreSettingsGroupRead {
    pub fn value(&self) -> &Value {
        &self.value
    }
}
struct StateData {
    auth: AuthService,
    active: AtomicUsize,
    stopped: AtomicBool,
    done: Notify,
    admission: std::sync::Mutex<()>,
    special: Option<crate::core_settings_special_effects::CoreSettingsSpecialEffects>,
}
struct Lease {
    _permit: Arc<OwnedSemaphorePermit>,
    state: Arc<StateData>,
    scratch: crate::core_settings_special_budget::Scratch,
    _emergency: Arc<crate::core_settings_special_budget::Charge>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::AcqRel);
        self.state.done.notify_waiters();
    }
}
struct Frame {
    bytes: Bytes,
    _lease: Arc<Lease>,
    _charge: Option<Arc<crate::core_settings_special_budget::Charge>>,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Setting {
    id: i64,
    key: String,
    name: String,
    value: Value,
}
#[derive(Clone, Copy)]
enum Kind {
    Create,
    Read,
    Update(bool),
    Delete,
    Check,
}
impl CoreSettingsWrites {
    pub fn new(auth: AuthService) -> Result<Self, StoreError> {
        if auth.notification_cache.is_none() {
            return Err(StoreError::Unsupported(
                "CoreSettings requires the explicit shared cache adapter",
            ));
        }
        Ok(Self {
            state: Arc::new(StateData {
                auth,
                active: AtomicUsize::new(0),
                stopped: AtomicBool::new(false),
                done: Notify::new(),
                admission: std::sync::Mutex::new(()),
                special: None,
            }),
        })
    }
    /// Explicit component capability. Default construction retains all six
    /// guards; no Main router or default worker is enabled by this method.
    pub fn with_special_effects(
        mut self,
        effects: crate::CoreSettingsSpecialEffects,
    ) -> Result<Self, StoreError> {
        let state = Arc::get_mut(&mut self.state).ok_or(StoreError::Unsupported(
            "Configure special groups before cloning service",
        ))?;
        if state.auth.event_sink.is_none() {
            return Err(StoreError::Unsupported(
                "Special groups require a real Channels publisher",
            ));
        }
        state.special = Some(effects);
        Ok(self)
    }
    /// Deliberately unassembled. A future Main must reconcile the existing read
    /// router's detail GET instead of merging duplicate method handlers blindly.
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/core/settings/", post(create))
            .route("/api/core/settings/check/", post(check))
            .route(
                "/api/core/settings/{id}/",
                get(read).put(put).patch(patch).delete(delete),
            )
            .with_state(self)
    }
    pub fn active_requests(&self) -> usize {
        self.state.active.load(Ordering::Acquire)
    }
    pub async fn settings_group(&self, key: &str) -> Result<CoreSettingsGroupRead, StoreError> {
        if crate::core_settings_special::Group::parse(key).is_none() {
            return Err(StoreError::Unsupported("Only six reviewed special groups"));
        }
        let lease = self.reserve()?;
        let auth = self
            .state
            .auth
            .clone()
            .with_request_writer_permit(lease._permit.clone())?
            .with_retained_scope(lease.clone());
        let key = key.to_owned();
        let task = tokio::spawn(async move {
            let value =
                crate::core_settings_special_effects::load_group(&auth, &key, json!({})).await?;
            Ok::<_, StoreError>(CoreSettingsGroupRead {
                value,
                _owner: lease,
            })
        });
        task.await
            .map_err(|_| StoreError::Unsupported("Owned group loader did not settle"))?
    }
    pub async fn default_user_agent(&self) -> Result<CoreSettingsGroupRead, StoreError> {
        let lease = self.reserve()?;
        let charge = lease.scratch.charge(512 * 1024)?;
        let auth = self
            .state
            .auth
            .clone()
            .with_request_writer_permit(lease._permit.clone())?
            .with_retained_scope(lease.clone())
            .with_retained_scope(charge);
        let task = tokio::spawn(async move {
            let value = crate::core_settings_special_effects::default_user_agent(&auth).await?;
            Ok::<_, StoreError>(CoreSettingsGroupRead {
                value: Value::String(value),
                _owner: lease,
            })
        });
        task.await
            .map_err(|_| StoreError::Unsupported("Owned default-UA loader did not settle"))?
    }
    pub fn cached_proxy_settings(&self) -> Option<crate::ProxySettingsRead> {
        self.state.special.as_ref()?.cached_proxy_read()
    }
    /// Literal component ownership state, not a resource sampler or Main API.
    pub fn ownership_state(&self) -> Value {
        json!({"active":self.active_requests(),"available_writer_permits":self.state.auth.user_agent_workers.available_permits(),"retained_proxy_entries":self.state.special.as_ref().map_or(0,|s|s.retained_cache_entries()),"admission_closed":self.state.stopped.load(Ordering::Acquire),"display_zone":self.state.auth.display_timezone_name()})
    }
    /// Trusted internal consumer seam. This reserves an existing real token;
    /// it does not authenticate an HTTP user or register a public route.
    pub async fn proxy_settings(&self) -> Result<crate::ProxySettingsRead, StoreError> {
        {
            let _admission = self
                .state
                .admission
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if self.state.stopped.load(Ordering::Acquire) {
                return Err(capacity());
            }
            if let Some(read) = self
                .state
                .special
                .as_ref()
                .and_then(|s| s.fresh_proxy_read())
            {
                return Ok(read);
            }
        }
        let lease = self.reserve()?;
        let auth = self
            .state
            .auth
            .clone()
            .with_request_writer_permit(lease._permit.clone())?
            .with_retained_scope(lease);
        self.state
            .special
            .as_ref()
            .ok_or(StoreError::Unsupported("Special context not configured"))?
            .proxy_settings(&auth)
            .await
    }
    pub async fn proxy_health_knobs(&self) -> Result<crate::ProxyHealthKnobs, StoreError> {
        let read = self.proxy_settings().await?;
        let copy = read.health_knobs()?;
        drop(read);
        Ok(copy)
    }
    /// The Source TSConfig loader calls CoreSettings directly, not BaseConfig's
    /// separate process cache. Return only copied scalars after physical Ready.
    /// TS may inherit a fresh Base slot until it performs its own cold load.
    /// Copy that inherited value without installing a separate TS slot/token.
    pub fn inherited_proxy_health_knobs(
        &self,
    ) -> Result<Option<crate::ProxyHealthKnobs>, StoreError> {
        let _admission = self
            .state
            .admission
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.state.stopped.load(Ordering::Acquire) {
            return Err(capacity());
        }
        self.state
            .special
            .as_ref()
            .ok_or(StoreError::Unsupported("Special context not configured"))?
            .fresh_proxy_read()
            .map(|read| read.health_knobs())
            .transpose()
    }
    pub fn proxy_health_reads_open(&self) -> bool {
        !self.state.stopped.load(Ordering::Acquire)
    }
    pub async fn proxy_ts_health_knobs(&self) -> Result<crate::ProxyHealthKnobs, StoreError> {
        let lease = self.reserve()?;
        let auth = self
            .state
            .auth
            .clone()
            .with_request_writer_permit(lease._permit.clone())?
            .with_retained_scope(lease);
        let task = tokio::spawn(async move {
            let value = crate::core_settings_special_effects::load_proxy(&auth).await?;
            let copied = crate::ProxyHealthKnobs::from_value(&value)?;
            drop(value);
            Ok::<_, StoreError>(copied)
        });
        task.await
            .map_err(|_| StoreError::Unsupported("Owned TS health loader did not settle"))?
    }
    /// Component-only drain, never a claim that unrelated AuthService users stopped.
    pub async fn shutdown(&self) -> bool {
        {
            let _admission = self
                .state
                .admission
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.state.stopped.store(true, Ordering::Release);
            if let Some(effects) = &self.state.special {
                effects.clear_proxy();
            }
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.state.done.notified();
                if self.active_requests() == 0 && self.state.special.as_ref().is_none_or(|s|s.retained_cache_entries()==0) {
                    break;
                }
                tokio::select!{_ = notified => {}, _ = tokio::time::sleep(Duration::from_millis(10)) => {}}
            }
        })
        .await
        .is_ok()
    }
    fn reserve(&self) -> Result<Arc<Lease>, StoreError> {
        let _admission = self
            .state
            .admission
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.state.stopped.load(Ordering::Acquire) {
            return Err(capacity());
        }
        let permit = self
            .state
            .auth
            .user_agent_workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        let scratch = crate::core_settings_special_budget::Scratch::new();
        let emergency = scratch.charge(256)?;
        self.state.active.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::new(Lease {
            _permit: Arc::new(permit),
            state: self.state.clone(),
            scratch,
            _emergency: emergency,
        }))
    }
    async fn run(
        &self,
        peer: SocketAddr,
        raw: Option<String>,
        request: Request,
        kind: Kind,
    ) -> Result<Response, StoreError> {
        // Native capacity admission is distinct from login failure. Within admitted
        // work, real authentication/network/role checks precede lookup/body parsing.
        let lease = self.reserve()?;
        let auth = match self
            .state
            .auth
            .clone()
            .with_request_writer_permit(lease._permit.clone())
        {
            Ok(auth) => auth.with_retained_scope(lease.clone()),
            Err(error) => return Ok(retained_error(error, lease).await),
        };
        let actor_lease = lease.clone();
        let actor = tokio::spawn(async move {
            let result = async {
                auth.request_started_display_timezone().await;
                let reads = auth.retained_rest_reads(RestReadLimits::default())?;
                let subject = reads
                    .account_user(peer, request.headers().clone(), !matches!(kind, Kind::Read))
                    .await?;
                if matches!(kind, Kind::Read) && subject.user().user_level < 1 {
                    return Err(StoreError::PermissionDenied);
                }
                // Retire the bounded user/property allocation before allocating
                // metadata. Reads and the actor share the SAME original permit.
                drop(subject);
                drop(reads);
                act(auth, peer, raw, request, kind, actor_lease.clone()).await
            }
            .await;
            match result {
                Ok(response) => response,
                Err(error) => retained_error(error, actor_lease).await,
            }
        });
        // Observer loss detaches the same actor. An admitted observer error has
        // its own frame reference to that SAME lease, never a fifth allowance.
        match tokio::time::timeout(OBSERVER, actor).await {
            Ok(Ok(response)) => Ok(response),
            _ => Ok(retained_error(capacity(), lease).await),
        }
    }
}
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"CoreSettings resource capacity is unavailable."}),
    )
}
async fn retained_error(error: StoreError, lease: Arc<Lease>) -> Response {
    retained_error_response(error.into_response(), lease).await
}
async fn retained_error_response(response: Response, lease: Arc<Lease>) -> Response {
    let (parts, body) = response.into_parts();
    let bound = body
        .size_hint()
        .upper()
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n <= OUTPUT_LIMIT);
    let reserved = bound.and_then(|n| lease.scratch.charge(n).ok());
    let (parts, bytes, charge) = match reserved {
        Some(charge) => match axum::body::to_bytes(body, bound.unwrap()).await {
            Ok(bytes) => (parts, bytes, Some(charge)),
            Err(_) => {
                let (parts, _) = capacity().into_response().into_parts();
                (
                    parts,
                    Bytes::from_static(
                        b"{\"detail\":\"CoreSettings resource capacity is unavailable.\"}",
                    ),
                    None,
                )
            }
        },
        None => {
            let (parts, _) = capacity().into_response().into_parts();
            (
                parts,
                Bytes::from_static(
                    b"{\"detail\":\"CoreSettings resource capacity is unavailable.\"}",
                ),
                None,
            )
        }
    };
    // Error and late physical transport buffers share one existing scratch
    // quote. The emergency static body is covered by this lease's 256 bytes.
    let bytes = Bytes::from_owner(Frame {
        bytes,
        _lease: lease,
        _charge: charge,
    });
    Response::from_parts(parts, axum::body::Body::from(bytes))
}

fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No CoreSettings matches the given query."}),
    )
}
fn guarded(key: &str) -> bool {
    [
        "system_settings",
        "network_access",
        "reverse_proxy_auth",
        "proxy_settings",
        "stream_settings",
        "dvr_settings",
    ]
    .contains(&key)
}
fn supported(key: &str) -> Result<(), StoreError> {
    if guarded(key) {
        Err(StoreError::Unsupported(
            "Task-coupled or global CoreSettings writes have not completed their Source gates",
        ))
    } else {
        Ok(())
    }
}
fn limits() -> XcUserReadLimits {
    XcUserReadLimits {
        max_raw_bytes: RAW_LIMIT,
        max_nodes: 2048,
        max_depth: 32,
        work_bytes: JSON_WORK,
    }
}
fn decode(raw: &str, input: bool, scalar: usize) -> Result<Value, StoreError> {
    match crate::xc_bounded_user::checked_properties(raw, scalar, limits()) {
        Ok((mut v, _)) => {
            crate::json_compat::normalize_input_numbers(&mut v).map_err(|_| {
                StoreError::Unsupported("CoreSettings stored number grammar is not qualified")
            })?;
            Ok(v)
        }
        Err(StoreError::StatusBody(503, _)) => Err(capacity()),
        Err(_) if input => Err(StoreError::InvalidInput(
            json!({"detail":"JSON parse error"}),
        )),
        Err(_) => Err(StoreError::Unsupported(
            "CoreSettings stored JSON grammar is not qualified",
        )),
    }
}
async fn body(request: Request) -> Result<Map<String, Value>, StoreError> {
    let mime = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if mime != "application/json" {
        return Err(StoreError::Unsupported(
            "Only Source32 JSON ingress has completed this component's definition",
        ));
    }
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        axum::body::to_bytes(request.into_body(), RAW_LIMIT),
    )
    .await
    .map_err(|_| capacity())?
    .map_err(|_| capacity())?;
    let v = if bytes.is_empty() {
        Value::Object(Map::new())
    } else {
        decode(
            std::str::from_utf8(&bytes)
                .map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?,
            true,
            0,
        )?
    };
    match v {
        Value::Object(v) => Ok(v),
        Value::Null => Err(StoreError::InvalidInput(
            json!({"non_field_errors":["No data provided"]}),
        )),
        _ => Err(StoreError::Unsupported(
            "Uncaptured CoreSettings top-level input shape",
        )),
    }
}
async fn finish(mut c: sqlx::pool::PoolConnection<sqlx::Postgres>) -> Result<(), StoreError> {
    if let Err(error) = Connection::ping(&mut *c).await {
        let _closed = c.close().await;
        return Err(error.into());
    }
    Connection::shrink_buffers(&mut *c);
    // PoolConnection.drop would spawn this work and lose our physical owner.
    // Await the actual return while the admitted actor/response scope remains.
    c.return_to_pool().await;
    drop(c);
    Ok(())
}
async fn existing(auth: &AuthService, raw: &str) -> Result<Setting, StoreError> {
    let id = dispatcharr_ingest_core::source_integer_literal(raw)
        .and_then(|n| n.parse::<i64>().ok())
        .ok_or_else(missing)?;
    let mut c = auth.store.pool().acquire().await?;
    let result=sqlx::query("SELECT id::bigint AS id,key,name,octet_length(value::text) AS bytes,CASE WHEN octet_length(value::text)<=65536 THEN value::text END AS value_text FROM core_coresettings WHERE id::bigint=$1").bind(id).fetch_optional(&mut*c).await;
    finish(c).await?;
    let row = result?.ok_or_else(missing)?;
    let bytes: i32 = row.try_get("bytes")?;
    if bytes < 0 || bytes as usize > RAW_LIMIT {
        return Err(capacity());
    }
    let key: String = row.try_get("key")?;
    let name: String = row.try_get("name")?;
    let text: Option<String> = row.try_get("value_text")?;
    let text = text.ok_or_else(capacity)?;
    let value = decode(&text, false, key.len() + name.len())?;
    Ok(Setting {
        id,
        key,
        name,
        value,
    })
}
#[derive(Default)]
struct Draft {
    key: Option<String>,
    name: Option<String>,
    value: Option<Value>,
    errors: Map<String, Value>,
}
fn fields(mut input: Map<String, Value>, partial: bool) -> Draft {
    let mut d = Draft::default();
    for field in ["key", "name"] {
        match input.remove(field) {
            None => {
                if !partial {
                    d.errors
                        .insert(field.into(), json!(["This field is required."]));
                }
            }
            Some(v) => match character(field, &v, Some(255), false) {
                Ok(v) => {
                    if field == "key" {
                        d.key = Some(v)
                    } else {
                        d.name = Some(v)
                    }
                }
                Err(StoreError::InvalidInput(Value::Object(mut e))) => {
                    if let Some(v) = e.remove(field) {
                        d.errors.insert(field.into(), v);
                    }
                }
                Err(_) => {
                    d.errors
                        .insert(field.into(), json!(["Not a valid string."]));
                }
            },
        }
    }
    if let Some(v) = input.remove("value") {
        if v.is_null() {
            d.errors
                .insert("value".into(), json!(["This field may not be null."]));
        } else {
            d.value = Some(v)
        }
    }
    d
}
#[cfg(test)]
fn complete(d: Draft, old: Option<Setting>, duplicate: bool) -> Result<Setting, StoreError> {
    complete_inner(d, old, duplicate, false)
}
fn complete_inner(
    mut d: Draft,
    old: Option<Setting>,
    duplicate: bool,
    special: bool,
) -> Result<Setting, StoreError> {
    if duplicate {
        d.errors.insert(
            "key".into(),
            json!(["core settings with this key already exists."]),
        );
    }
    if !d.errors.is_empty() {
        let mut ordered = Map::new();
        for k in ["key", "name", "value"] {
            if let Some(v) = d.errors.remove(k) {
                ordered.insert(k.into(), v);
            }
        }
        return Err(StoreError::InvalidInput(Value::Object(ordered)));
    }
    let creation = old.is_none();
    let mut row = old.unwrap_or(Setting {
        id: 0,
        key: String::new(),
        name: String::new(),
        value: json!({}),
    });
    let oldkey = row.key.clone();
    if let Some(key) = d.key {
        if !special {
            supported(&key)?;
        }
        row.key = key
    }
    if let Some(name) = d.name {
        row.name = name
    }
    if let Some(mut v) = d.value {
        if special && !creation {
            if let Some(group) = crate::core_settings_special::Group::parse(&oldkey) {
                crate::core_settings_special::normalize_update(group, &mut v, clamped)?;
            }
        }
        if (creation && row.key == "epg_settings") || (!creation && oldkey == "epg_settings") {
            clamp_epg(&mut v)
        }
        row.value = v
    }
    Ok(row)
}
fn clamped(v: Option<&Value>, default: i64, hi: i64) -> i64 {
    let Some(v) = v else { return default };
    let n = match v {
        Value::Bool(v) => BigInt::from(i64::from(*v)),
        Value::Number(n) if n.is_f64() => {
            let Some(n) = n.as_f64().filter(|n| n.is_finite()) else {
                return default;
            };
            return if n < 0.0 {
                0
            } else if n > hi as f64 {
                hi
            } else {
                n.trunc() as i64
            };
        }
        Value::Number(n) => match BigInt::from_str(&n.to_string()) {
            Ok(n) => n,
            Err(_) => return default,
        },
        Value::String(s) => match dispatcharr_ingest_core::source_integer_literal(s)
            .and_then(|s| BigInt::from_str(&s).ok())
        {
            Some(n) => n,
            None => return default,
        },
        _ => return default,
    };
    if n < BigInt::from(0) {
        0
    } else if n > BigInt::from(hi) {
        hi
    } else {
        n.to_string().parse().unwrap_or(default)
    }
}
fn clamp_epg(v: &mut Value) {
    let Some(m) = v.as_object_mut() else { return };
    if m.contains_key("epg_history_max_days") {
        let n = clamped(m.get("epg_history_max_days"), 30, 365);
        m.insert("epg_history_max_days".into(), json!(n));
    }
    let max = clamped(m.get("epg_history_max_days"), 30, 365);
    for (k, d, h) in [
        ("epg_history_min_days", 1, max),
        ("epg_default_prev_days", 0, max),
        ("epg_default_days", 0, 365),
    ] {
        if m.contains_key(k) {
            let n = clamped(m.get(k), d, h);
            m.insert(k.into(), json!(n));
        }
    }
}
async fn validate(
    auth: &AuthService,
    input: Map<String, Value>,
    old: Option<Setting>,
    partial: bool,
    special: bool,
) -> Result<Setting, StoreError> {
    let d = fields(input, partial);
    let duplicate = if let Some(key) = d.key.as_ref() {
        let mut c = auth.store.pool().acquire().await?;
        let r=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM core_coresettings WHERE key=$1 AND($2::bigint IS NULL OR id::bigint<>$2))").bind(key).bind(old.as_ref().map(|r|r.id)).fetch_one(&mut*c).await;
        finish(c).await?;
        r?
    } else {
        false
    };
    complete_inner(d, old, duplicate, special)
}
fn checked_network(
    auth: &AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    body: Map<String, Value>,
) -> Result<Value, StoreError> {
    if body.get("key").and_then(Value::as_str) != Some("network_access") {
        return Ok(json!({}));
    }
    let request = crate::auth::request(peer, headers);
    let ip = auth.client_ip(&request);
    let empty = Map::new();
    let groups = match body.get("value") {
        None => &empty,
        Some(Value::Object(v)) => v,
        _ => {
            return Err(StoreError::Unsupported(
                "Uncaptured network-check value shape",
            ))
        }
    };
    let mut in_network = Map::new();
    let mut invalid = Vec::new();
    for (k, v) in groups {
        let Some(v) = v.as_str() else {
            return Err(StoreError::Unsupported(
                "Uncaptured network-check CIDR type",
            ));
        };
        let mut outside = Vec::new();
        for part in v.split(',') {
            match crate::xc_auth::network(part, true) {
                Ok(net) => {
                    if net.contains(&ip) {
                        outside.clear();
                        break;
                    } else {
                        outside.push(part)
                    }
                }
                Err(_) => invalid.push(part),
            }
        }
        in_network.insert(k.clone(), json!(outside));
    }
    if invalid.is_empty() {
        in_network.insert("client_ip".into(), json!(ip.to_string()));
        Ok(Value::Object(in_network))
    } else {
        Ok(json!({"error":true,"message":"Invalid CIDR(s)","data":invalid}))
    }
}
async fn invalidate(auth: &AuthService, key: &str) -> Result<(), StoreError> {
    let cache = auth
        .notification_cache
        .as_ref()
        .ok_or(StoreError::Unsupported("CoreSettings cache not configured"))?;
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| capacity())?
        .as_nanos();
    let n = i64::try_from(n).map_err(|_| {
        StoreError::Unsupported("CoreSettings clock outside qualified integer range")
    })?;
    cache.invalidate_settings_group(key, &n.to_string()).await
}
struct Count(usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= OUTPUT_LIMIT)
            .ok_or_else(|| io::Error::other("capacity"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Output {
    bytes: Vec<u8>,
}
impl Write for Output {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(b.len())
            .is_none_or(|n| n > OUTPUT_LIMIT)
        {
            return Err(io::Error::other("capacity"));
        }
        self.bytes
            .try_reserve_exact(b.len())
            .map_err(io::Error::other)?;
        self.bytes.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encoded(v: &Value, out: &mut impl Write) -> Result<(), StoreError> {
    let io = |_: io::Error| capacity();
    match v {
        Value::Number(n) => {
            let s = if n.is_f64() {
                crate::json_compat::float_string(
                    n.as_f64().filter(|n| n.is_finite()).ok_or_else(capacity)?,
                )
            } else {
                let s = n.to_string();
                if s.len() > 4301 {
                    return Err(StoreError::Unsupported(
                        "CoreSettings number exceeds captured integer grammar",
                    ));
                }
                s
            };
            out.write_all(s.as_bytes()).map_err(io)?
        }
        Value::Array(a) => {
            out.write_all(b"[").map_err(io)?;
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.write_all(b",").map_err(io)?
                }
                encoded(v, out)?
            }
            out.write_all(b"]").map_err(io)?
        }
        Value::Object(m) => {
            out.write_all(b"{").map_err(io)?;
            for (i, (k, v)) in m.iter().enumerate() {
                if i > 0 {
                    out.write_all(b",").map_err(io)?
                }
                serde_json::to_writer(&mut *out, k).map_err(|_| capacity())?;
                out.write_all(b":").map_err(io)?;
                encoded(v, out)?
            }
            out.write_all(b"}").map_err(io)?
        }
        _ => serde_json::to_writer(out, v).map_err(|_| capacity())?,
    }
    Ok(())
}
fn reply(
    status: StatusCode,
    value: Option<Value>,
    lease: Arc<Lease>,
) -> Result<Response, StoreError> {
    if let Some(v) = value {
        let mut counter = Count(0);
        encoded(&v, &mut counter)?;
        let charge = lease.scratch.charge(counter.0)?;
        let mut out = Output {
            bytes: Vec::with_capacity(counter.0),
        };
        encoded(&v, &mut out)?;
        let bytes = Bytes::from_owner(Frame {
            bytes: Bytes::from(out.bytes),
            _lease: lease,
            _charge: Some(charge),
        });
        Ok((
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response())
    } else {
        drop(lease);
        Ok((
            status,
            [(axum::http::header::CONTENT_LENGTH, "0")],
            axum::body::Body::empty(),
        )
            .into_response())
    }
}
async fn act(
    auth: AuthService,
    peer: SocketAddr,
    raw: Option<String>,
    request: Request,
    kind: Kind,
    lease: Arc<Lease>,
) -> Result<Response, StoreError> {
    if matches!(kind, Kind::Check) {
        let headers = request.headers().clone();
        let v = checked_network(&auth, peer, headers, body(request).await?)?;
        return reply(StatusCode::OK, Some(v), lease);
    }
    let old = if matches!(kind, Kind::Create) {
        None
    } else {
        Some(existing(&auth, raw.as_deref().ok_or_else(missing)?).await?)
    };
    if matches!(kind, Kind::Read) {
        drop(request);
        return reply(
            StatusCode::OK,
            Some(serde_json::to_value(old.ok_or_else(missing)?).map_err(|_| capacity())?),
            lease,
        );
    }
    let special = lease.state.special.clone();
    if special.is_none() {
        if let Some(row) = old.as_ref() {
            supported(&row.key)?;
        }
    }
    if matches!(kind, Kind::Delete) {
        drop(request);
        let row = old.ok_or_else(missing)?;
        let mut c = auth.store.pool().acquire().await?;
        let begin = sqlx::query("BEGIN").execute(&mut *c).await;
        if let Err(error) = begin {
            finish(c).await?;
            return Err(error.into());
        }
        let deleted = sqlx::query("DELETE FROM core_coresettings WHERE id::bigint=$1")
            .bind(row.id)
            .execute(&mut *c)
            .await;
        let effect = async {
            match deleted {
                Ok(result) if result.rows_affected() == 1 => {
                    invalidate(&auth, &row.key).await?;
                    if row.key == "proxy_settings" {
                        if let Some(effects) = &special {
                            effects.clear_proxy();
                        }
                    }
                    if special.is_some() && row.key == "stream_settings" {
                        invalidate_user_agent(&auth).await?;
                    }
                    Ok(())
                }
                Ok(_) => Err(StoreError::Unsupported(
                    "Concurrent CoreSettings deletion is outside Source32",
                )),
                Err(error) => Err(error.into()),
            }
        }
        .await;
        if let Err(error) = effect {
            let rollback = sqlx::query("ROLLBACK").execute(&mut *c).await;
            if let Err(rollback_error) = rollback {
                let _closed = c.close().await;
                return Err(rollback_error.into());
            }
            finish(c).await?;
            return Err(error);
        }
        let committed = sqlx::query("COMMIT").execute(&mut *c).await;
        if let Err(error) = committed {
            // An uncertain terminal transaction is closed/settled, never
            // replayed as a mutation or drained by Transaction.drop.
            let _closed = c.close().await;
            return Err(error.into());
        }
        finish(c).await?;
        return reply(StatusCode::NO_CONTENT, None, lease);
    }
    let partial = matches!(kind, Kind::Update(true));
    let input = body(request).await?;
    let old_key = old.as_ref().map(|r| r.key.clone());
    let new_key = input
        .get("key")
        .and_then(Value::as_str)
        .or(old_key.as_deref());
    let dispatch = crate::core_settings_special::Dispatch::new(old_key.as_deref(), new_key);
    let empty = json!({});
    let incoming = input.get("value").unwrap_or(&empty);
    let dvr = special.is_some()
        && crate::core_settings_special::dvr_publish(
            dispatch,
            old.as_ref().map(|r| &r.value),
            incoming,
        );
    let mut pending = None;
    let mut keys = None;
    if special.is_some() && dispatch.old == Some(crate::core_settings_special::Group::Stream) {
        if let (Some(old_value), Some(new_value)) = (
            old.as_ref().and_then(|r| r.value.as_object()),
            incoming.as_object(),
        ) {
            let before = crate::core_settings_special::hash_fields(old_value.get("m3u_hash_key"))?;
            let after = crate::core_settings_special::hash_fields(new_value.get("m3u_hash_key"))?;
            if crate::core_settings_special_broker::changed(&before, &after) {
                pending = Some(
                    crate::core_settings_special_broker::RehashLocks::acquire(
                        &auth,
                        &lease.scratch,
                    )
                    .await?,
                );
                keys = Some(after);
            }
        }
    }
    let outcome=async {
    let row = validate(&auth, input, old, partial,special.is_some()).await?;
    let creation = matches!(kind, Kind::Create);
    let mut row = row;
    let mut c = auth.store.pool().acquire().await?;
    if creation {
        let result = sqlx::query_scalar::<_, i64>(
            "INSERT INTO core_coresettings(key,name,value)VALUES($1,$2,$3)RETURNING id::bigint",
        )
        .bind(&row.key)
        .bind(&row.name)
        .bind(sqlx::types::Json(&row.value))
        .fetch_one(&mut *c)
        .await;
        finish(c).await?;
        row.id = result?;
    } else {
        let r =
            sqlx::query("UPDATE core_coresettings SET key=$2,name=$3,value=$4 WHERE id::bigint=$1")
                .bind(row.id)
                .bind(&row.key)
                .bind(&row.name)
                .bind(sqlx::types::Json(&row.value))
                .execute(&mut *c)
                .await;
        finish(c).await?;
        if r?.rows_affected() != 1 {
            return Err(StoreError::Unsupported(
                "Concurrent CoreSettings row disappearance is outside Source32",
            ));
        }
    }
    // Source post_save runs after an autocommitted save; a cache command error
    // must not invent a rollback of the already saved row.
    if let Some(effects)=&special {
        if row.key=="system_settings" {effects.system_save(&auth,&row.value,&lease.scratch).await?;}
    }
    invalidate(&auth, &row.key).await?;
    if let Some(effects)=&special {
        match row.key.as_str() {
            "proxy_settings"=>effects.clear_proxy(),
            "stream_settings"=>invalidate_user_agent(&auth).await?,
            "network_access"=>crate::core_settings_special_network::save(&auth,&lease.scratch).await,
            _=>{}
        }
        if let Some(fields)=keys {
            let locks=pending.as_ref().ok_or_else(capacity)?;
            crate::core_settings_special_broker::publish(&auth,&lease.scratch,"core.tasks.rehash_streams",json!([fields]),json!({"locks_preacquired":true,"preacquired_lock_ids":locks.ids,"preacquired_lock_token":locks.token})).await?;
            pending.take(); // only actual successful LPUSH hands off owned locks
        }
        if dvr {
            // Source's synchronous reschedule fallback on publication error is
            // a separate unqualified downstream gate. Never invent its effects.
            crate::core_settings_special_broker::publish(&auth,&lease.scratch,"apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change",json!([]),json!({})).await?;
        }
    }
    reply(
        if creation {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Some(serde_json::to_value(row).map_err(|_| capacity())?),
        lease.clone(),
    )
    }.await;
    if let Some(locks) = pending {
        locks.release(&auth).await?;
    }
    outcome
}
async fn invalidate_user_agent(auth: &AuthService) -> Result<(), StoreError> {
    let version = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| capacity())?
        .as_nanos()
        .to_string();
    auth.notification_cache
        .as_ref()
        .ok_or_else(capacity)?
        .invalidate_user_agent(&version)
        .await
}
async fn create(
    State(s): State<CoreSettingsWrites>,
    ConnectInfo(p): ConnectInfo<SocketAddr>,
    r: Request,
) -> Result<Response, StoreError> {
    s.run(p, None, r, Kind::Create).await
}
async fn read(
    State(s): State<CoreSettingsWrites>,
    ConnectInfo(p): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    r: Request,
) -> Result<Response, StoreError> {
    s.run(p, Some(id), r, Kind::Read).await
}
async fn put(
    State(s): State<CoreSettingsWrites>,
    ConnectInfo(p): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    r: Request,
) -> Result<Response, StoreError> {
    s.run(p, Some(id), r, Kind::Update(false)).await
}
async fn patch(
    State(s): State<CoreSettingsWrites>,
    ConnectInfo(p): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    r: Request,
) -> Result<Response, StoreError> {
    s.run(p, Some(id), r, Kind::Update(true)).await
}
async fn delete(
    State(s): State<CoreSettingsWrites>,
    ConnectInfo(p): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    r: Request,
) -> Result<Response, StoreError> {
    s.run(p, Some(id), r, Kind::Delete).await
}
async fn check(
    State(s): State<CoreSettingsWrites>,
    ConnectInfo(p): ConnectInfo<SocketAddr>,
    r: Request,
) -> Result<Response, StoreError> {
    s.run(p, None, r, Kind::Check).await
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reference() -> Value {
        serde_json::from_str(include_str!("../datasets/core-settings-source32.json")).unwrap()
    }
    fn old(case: &Value) -> Option<Setting> {
        let id = case["target_id"].as_str()?.parse::<i64>().ok()?;
        case["before_settings"]
            .as_array()?
            .iter()
            .find(|v| v["id"].as_i64() == Some(id))
            .map(|v| serde_json::from_value(v.clone()).unwrap())
    }
    #[test]
    fn actual_source32_payload_projection_and_field_errors() {
        let source = reference();
        let mut checked = 0;
        for case in source["cases"].as_array().unwrap() {
            let method = case["method"].as_str().unwrap();
            let status = case["status"].as_u64().unwrap();
            if !["POST", "PATCH", "PUT"].contains(&method)
                || case["target_alias"] == "check"
                || ![200, 201, 400].contains(&status)
            {
                continue;
            }
            let body: Value = serde_json::from_str(case["body"].as_str().unwrap()).unwrap();
            let fields = fields(body.as_object().unwrap().clone(), method == "PATCH");
            let existing = old(case);
            let duplicate = fields.key.as_ref().is_some_and(|key| {
                case["before_settings"].as_array().unwrap().iter().any(|r| {
                    r["key"].as_str() == Some(key)
                        && r["id"].as_i64() != existing.as_ref().map(|r| r.id)
                })
            });
            match complete(fields, existing, duplicate) {
                Ok(mut row) => {
                    assert!(status == 200 || status == 201, "case{}", case["id"]);
                    if status == 201 {
                        row.id = case["json"]["id"].as_i64().unwrap()
                    }
                    let value = serde_json::to_value(row).unwrap();
                    assert_eq!(value, case["json"], "case{}", case["id"]);
                    let mut out = Output { bytes: Vec::new() };
                    encoded(&value, &mut out).unwrap();
                    assert_eq!(
                        hex(&out.bytes),
                        case["response_body_hex"],
                        "source response bytes case{}",
                        case["id"]
                    );
                }
                Err(StoreError::InvalidInput(error)) => {
                    assert_eq!(status, 400);
                    assert_eq!(error, case["json"], "case{}", case["id"]);
                }
                Err(error) => panic!("unexpected case{}: {error:?}", case["id"]),
            }
            checked += 1;
        }
        assert_eq!(checked, 17);
    }
    fn hex(v: &[u8]) -> String {
        v.iter().map(|b| format!("{b:02x}")).collect()
    }
    #[test]
    fn epg_clamp_keeps_nonmapping_and_ordered_dependencies() {
        let mut v = json!({"epg_history_max_days":999,"epg_history_min_days":999,"epg_default_prev_days":999,"epg_default_days":999});
        clamp_epg(&mut v);
        assert_eq!(
            v,
            json!({"epg_history_max_days":365,"epg_history_min_days":365,"epg_default_prev_days":365,"epg_default_days":365})
        );
        for mut v in [json!([1, true]), json!(false), Value::Null] {
            let before = v.clone();
            clamp_epg(&mut v);
            assert_eq!(v, before)
        }
        assert_eq!(clamped(Some(&json!(true)), 30, 365), 1);
        assert_eq!(clamped(Some(&json!("  +7_0  ")), 30, 365), 70);
        assert_eq!(clamped(Some(&json!(7.9)), 30, 365), 7);
        assert_eq!(clamped(Some(&json!("bad")), 30, 365), 30);
    }
    #[test]
    fn coupled_old_and_new_keys_are_guarded_without_mutation() {
        for key in [
            "system_settings",
            "network_access",
            "reverse_proxy_auth",
            "proxy_settings",
            "stream_settings",
            "dvr_settings",
        ] {
            assert!(supported(key).is_err());
            let d = fields(
                serde_json::from_value(json!({"key":key,"name":"not applied"})).unwrap(),
                false,
            );
            assert!(complete(d, None, false).is_err());
        }
        assert!(supported("owned.settings.any").is_ok());
        assert!(supported("epg_settings").is_ok());
    }
    #[test]
    fn bounded_json_refuses_raw_amplification_and_oversize_before_value() {
        assert!(matches!(
            decode(r#"{"$serde_json::private::RawValue":"[1,2]"}"#, true, 0),
            Err(StoreError::StatusBody(503, _))
        ));
        assert!(matches!(
            decode(&"x".repeat(RAW_LIMIT + 1), true, 0),
            Err(StoreError::StatusBody(503, _))
        ));
        assert!(matches!(
            decode("{", true, 0),
            Err(StoreError::InvalidInput(_))
        ));
        assert_eq!(
            decode(r#"{"literal":true}"#, true, 0).unwrap(),
            json!({"literal":true})
        );
    }
    #[test]
    fn bounded_writer_checks_before_emission_and_retains_finite_grammar() {
        let mut out = Output { bytes: Vec::new() };
        assert!(encoded(&json!({"utf8":"Café","float":1.0}), &mut out).is_ok());
        assert_eq!(
            std::str::from_utf8(&out.bytes).unwrap(),
            "{\"utf8\":\"Café\",\"float\":1.0}"
        );
        let mut out = Output {
            bytes: vec![0; OUTPUT_LIMIT],
        };
        assert!(out.write_all(b"x").is_err());
        assert_eq!(out.bytes.len(), OUTPUT_LIMIT);
    }
    #[test]
    fn source_case_counts_stay_framework_only_and_complete() {
        let r = reference();
        assert_eq!(r["cases"].as_array().unwrap().len(), 32);
        assert!(r["scope"].as_str().unwrap().contains("no Native"));
    }
    #[test]
    fn actual_source51_old_key_serializer_creation_and_errors() {
        let source: Value =
            serde_json::from_str(include_str!("core_settings_special_vectors.json")).unwrap();
        let mut checked = 0;
        for case in source["cases"].as_array().unwrap() {
            let status = case["status"].as_u64().unwrap();
            if ![200, 201, 400].contains(&status)
                || !["PUT", "PATCH", "POST"].contains(&case["method"].as_str().unwrap())
            {
                continue;
            }
            let old = case
                .get("old")
                .filter(|v| !v.is_null())
                .map(|v| serde_json::from_value::<Setting>(v.clone()).unwrap());
            let draft = fields(
                case["input"].as_object().unwrap().clone(),
                case["method"] == "PATCH",
            );
            match complete_inner(draft, old, false, true) {
                Ok(mut row) => {
                    assert_ne!(status, 400, "case{}", case["id"]);
                    if status == 201 {
                        row.id = case["json"]["id"].as_i64().unwrap();
                    }
                    assert_eq!(
                        serde_json::to_value(row).unwrap(),
                        case["json"],
                        "case{}",
                        case["id"]
                    );
                }
                Err(StoreError::InvalidInput(value)) => {
                    assert_eq!(status, 400, "case{}", case["id"]);
                    assert_eq!(value, case["json"], "case{}", case["id"]);
                }
                Err(error) => panic!("case{}: {error:?}", case["id"]),
            }
            checked += 1;
        }
        assert_eq!(checked, 30);
    }
    fn local_service() -> CoreSettingsWrites {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect_lazy("postgres://unit:unit@127.0.0.1:9/not_contacted")
            .unwrap();
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("local-core-tests-only-secret-32".to_owned()).unwrap(),
        );
        // No constructor/cache claim: this only exercises actual shared permit
        // and response-byte ownership, with no authentication/SQL/cache calls.
        CoreSettingsWrites {
            state: Arc::new(StateData {
                auth,
                active: AtomicUsize::new(0),
                stopped: AtomicBool::new(false),
                done: Notify::new(),
                admission: std::sync::Mutex::new(()),
                special: None,
            }),
        }
    }
    #[tokio::test]
    async fn internal_group_read_keeps_original_quote_until_retired() {
        let service = local_service();
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(CoreSettingsGroupRead {
                value: json!({"owned":true}),
                _owner: service.reserve().unwrap(),
            });
        }
        assert_eq!(service.active_requests(), 4);
        assert!(service.reserve().is_err());
        assert_eq!(held[0].value(), &json!({"owned":true}));
        drop(held.remove(0));
        let replaced = service.reserve().unwrap();
        assert_eq!(service.active_requests(), 4);
        drop(replaced);
        drop(held);
        assert_eq!(service.active_requests(), 0);
        assert_eq!(service.state.auth.user_agent_workers.available_permits(), 4);
    }
    #[tokio::test]
    async fn shared_budget_refuses_plugin_with_one_external_and_three_core_owners() {
        use tower::ServiceExt;
        let core = local_service();
        let auth = core.state.auth.clone();
        let plugin = crate::PluginRepositories::new(auth.clone());
        // A REAL existing semaphore owner, not a dummy cache observation.
        // Actual cache-warm composition still requires the physical fixture.
        let external = auth.user_agent_workers.clone().try_acquire_owned().unwrap();
        let mut held = (0..3).map(|_| core.reserve().unwrap()).collect::<Vec<_>>();
        assert_eq!(auth.user_agent_workers.available_permits(), 0);
        let mut request = Request::builder()
            .uri("/api/plugins/repos/")
            .body(axum::body::Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 2], 12345))));
        let response = plugin.router().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(core.state.auth.store.pool().size(), 0);
        assert_eq!(core.active_requests(), 3);
        assert_eq!(auth.user_agent_workers.available_permits(), 0);
        drop(held.pop());
        let reused = core.reserve().unwrap();
        drop(reused);
        drop(held);
        drop(external);
        assert_eq!(auth.user_agent_workers.available_permits(), 4);
    }
    #[tokio::test]
    async fn same_actual_writer_token_and_response_owner_survive_shutdown() {
        let service = local_service();
        let lease = service.reserve().unwrap();
        let scoped = service
            .state
            .auth
            .clone()
            .with_request_writer_permit(lease._permit.clone())
            .unwrap()
            .with_retained_scope(lease.clone());
        assert_eq!(service.state.auth.user_agent_workers.available_permits(), 3);
        let response = reply(StatusCode::OK, Some(json!({"owned":true})), lease.clone()).unwrap();
        drop(lease);
        drop(scoped);
        let bytes = axum::body::to_bytes(response.into_body(), OUTPUT_LIMIT)
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"{\"owned\":true}");
        assert_eq!(service.active_requests(), 1);
        assert!(!service.shutdown().await);
        drop(bytes);
        assert!(service.shutdown().await);
        assert_eq!(service.active_requests(), 0);
        assert_eq!(service.state.auth.user_agent_workers.available_permits(), 4);
    }
    #[tokio::test]
    async fn captured_network_check_values_and_bytes_match_without_sql_or_auth() {
        let service = local_service();
        let cases = reference();
        let peer = SocketAddr::from(([127, 0, 0, 1], 12345));
        let mut count = 0;
        for row in cases["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["target_alias"] == "check" && r["status"] == 200)
        {
            let body: Map<String, Value> =
                serde_json::from_str(row["body"].as_str().unwrap()).unwrap();
            let actual =
                checked_network(&service.state.auth, peer, HeaderMap::new(), body).unwrap();
            assert_eq!(actual, row["json"]);
            let mut out = Output { bytes: Vec::new() };
            encoded(&actual, &mut out).unwrap();
            assert_eq!(hex(&out.bytes), row["response_body_hex"]);
            count += 1;
        }
        assert_eq!(count, 3);
    }
    #[tokio::test]
    async fn queued_admission_cannot_cross_successful_close() {
        let service = local_service();
        {
            let gate = service.state.admission.lock().unwrap();
            let clone = service.clone();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let thread = std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                done_tx.send(clone.reserve().is_err()).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(
                done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "reservation must not bypass the actual admission epoch lock"
            );
            service.state.stopped.store(true, Ordering::Release);
            drop(gate);
            assert!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap());
            thread.join().unwrap();
        }
        assert_eq!(service.active_requests(), 0);
        assert!(service.shutdown().await);
        assert!(service.reserve().is_err());
    }
    #[tokio::test]
    async fn admitted_error_preserves_exact_response_and_clone_owns_real_lease() {
        let service = local_service();
        let original = StoreError::Unauthorized("owned error").into_response();
        let expected_status = original.status();
        let expected_headers = original.headers().clone();
        let expected = axum::body::to_bytes(original.into_body(), OUTPUT_LIMIT)
            .await
            .unwrap();
        let lease = service.reserve().unwrap();
        let response = retained_error(StoreError::Unauthorized("owned error"), lease).await;
        assert_eq!(response.status(), expected_status);
        assert_eq!(response.headers(), &expected_headers);
        let bytes = axum::body::to_bytes(response.into_body(), OUTPUT_LIMIT)
            .await
            .unwrap();
        assert_eq!(bytes, expected);
        let cloned = bytes.clone();
        drop(bytes);
        assert_eq!(service.active_requests(), 1);
        assert_eq!(service.state.auth.user_agent_workers.available_permits(), 3);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), service.shutdown())
                .await
                .is_err()
        );
        drop(cloned);
        assert!(service.shutdown().await);
        assert_eq!(service.active_requests(), 0);
        assert_eq!(service.state.auth.user_agent_workers.available_permits(), 4);
    }
    #[tokio::test]
    async fn four_error_frames_refuse_fifth_before_auth_and_reuse_after_last_clone() {
        let service = local_service();
        let mut held = Vec::new();
        for _ in 0..4 {
            let lease = service.reserve().unwrap();
            let response = retained_error(StoreError::PermissionDenied, lease).await;
            held.push(
                axum::body::to_bytes(response.into_body(), OUTPUT_LIMIT)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(service.active_requests(), 4);
        assert_eq!(service.state.auth.user_agent_workers.available_permits(), 0);
        let fifth = service
            .run(
                SocketAddr::from(([127, 0, 0, 1], 12345)),
                Some("1002".into()),
                Request::new(axum::body::Body::empty()),
                Kind::Read,
            )
            .await;
        assert!(matches!(fifth, Err(StoreError::StatusBody(503, _))));
        assert_eq!(service.state.auth.store.pool().size(), 0);
        assert_eq!(service.active_requests(), 4);
        let first = held.remove(0);
        let cloned = first.clone();
        drop(first);
        assert!(service.reserve().is_err());
        drop(cloned);
        let reused = service.reserve().unwrap();
        assert_eq!(service.active_requests(), 4);
        drop(reused);
        drop(held);
        assert_eq!(service.active_requests(), 0);
        assert_eq!(service.state.auth.user_agent_workers.available_permits(), 4);
    }
    #[tokio::test]
    async fn over_limit_error_surface_is_bounded_and_still_retains_same_lease() {
        let service = local_service();
        let lease = service.reserve().unwrap();
        let response = (StatusCode::BAD_REQUEST, vec![b'x'; OUTPUT_LIMIT + 1]).into_response();
        let response = retained_error_response(response, lease).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), OUTPUT_LIMIT)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!({"detail":"CoreSettings resource capacity is unavailable."})
        );
        assert_eq!(service.active_requests(), 1);
        drop(bytes);
        assert_eq!(service.active_requests(), 0);
    }
    #[tokio::test]
    async fn successful_empty204_retires_without_fake_frame() {
        let service = local_service();
        let lease = service.reserve().unwrap();
        let response = reply(StatusCode::NO_CONTENT, None, lease).unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(service.active_requests(), 0);
        assert_eq!(service.state.auth.user_agent_workers.available_permits(), 4);
        assert!(axum::body::to_bytes(response.into_body(), OUTPUT_LIMIT)
            .await
            .unwrap()
            .is_empty());
    }
}

#[cfg(test)]
#[path = "core_settings_physical_tests.rs"]
mod physical_tests;
