//! Owned native orchestration of pinned Source EPG matching tasks.
//! Original Dispatcharr, Django and RapidFuzz authors retain all algorithm credit.
use crate::channel_epg_matching::{self as algorithm, Decision, EpgCandidate};
use crate::{AuthService, StoreError};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;
#[path = "channel_epg_match_sql.rs"]
mod sql;
pub type Owner = Arc<dyn Send + Sync>;
#[derive(Clone, Debug)]
pub enum Request {
    All,
    Selected(Value),
    Single(Value),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchingChild {
    ParsePrograms(i64),
    SdMapped(i64),
}
pub trait MatchingEffects: Send + Sync {
    /// Implemented only by the actual claimed task publisher. UUID/root/parent
    /// and real child settlement remain owned by that publisher, never by JSON.
    fn publish<'a>(
        &'a self,
        child: MatchingChild,
        owner: Owner,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}
/// Actual normal-worker admission/drain state, never a request metadata flag.
pub trait MatchingCancellation: Send + Sync {
    fn is_draining(&self) -> bool;
}
#[derive(Debug)]
pub enum ExecutionError {
    Raised { class: String, message: String },
    Operational(StoreError),
}
#[derive(Clone, Debug)]
struct Failure {
    class: String,
    message: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ChannelRow {
    id: i64,
    name: String,
    tvg: Option<String>,
    station: Option<String>,
    current: Option<i64>,
    auto_created: bool,
    group: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct EpgRow {
    id: i64,
    name: String,
    tvg: Option<String>,
    source: Option<i64>,
    priority: i64,
    source_type: Option<String>,
    has_programs: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Snapshot {
    channels: Vec<ChannelRow>,
    epgs: Vec<EpgRow>,
    region: Option<Value>,
    settings: Value,
    single: bool,
    single_exact: Option<(i64, bool)>,
}
#[derive(Clone, Debug)]
struct Choice {
    channel: ChannelRow,
    epg: Option<EpgRow>,
    via: Option<String>,
}
/// No public constructor or caller-supplied boolean/path can assert absence.
#[derive(Debug)]
pub struct UnavailableWitness {
    _private: (),
}
#[derive(Debug)]
pub enum ModelAvailability {
    Unavailable(UnavailableWitness),
    Unbound,
}
pub async fn observe_model_availability(owner: Owner) -> Result<ModelAvailability, StoreError> {
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        if !std::env::var("DISABLE_ML_DOWNLOADS").is_ok_and(|v| v.eq_ignore_ascii_case("true")) {
            return Ok(ModelAvailability::Unbound);
        }
        match std::fs::metadata("/data/models/models--sentence-transformers--all-MiniLM-L6-v2") {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(ModelAvailability::Unavailable(UnavailableWitness {
                    _private: (),
                }))
            }
            Ok(_) => Ok(ModelAvailability::Unbound),
            Err(_) => Err(unsupported("Matching model cache availability is unknown")),
        }
    })
    .await
    .map_err(|_| unsupported("Matching model observation owner unsettled"))?
}
#[derive(Debug)]
struct PluginIdentity {
    configured: std::path::PathBuf,
    path: std::path::PathBuf,
    held: std::fs::File,
    dev: u64,
    ino: u64,
}
impl PluginIdentity {
    fn observe() -> Result<Self, StoreError> {
        let configured =
            std::env::var("DISPATCHARR_PLUGINS_DIR").unwrap_or_else(|_| "/data/plugins".into());
        if configured.is_empty() || configured.len() > 4096 {
            return Err(unsupported("Matching plugin path capacity"));
        }
        let path = std::fs::canonicalize(&configured)
            .map_err(|_| unsupported("Matching plugin directory unavailable"))?;
        let held = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|_| unsupported("Matching plugin directory identity unavailable"))?;
        let meta = held
            .metadata()
            .map_err(|_| unsupported("Matching plugin directory metadata unavailable"))?;
        let value = Self {
            configured: configured.into(),
            path,
            held,
            dev: meta.dev(),
            ino: meta.ino(),
        };
        value.recheck()?;
        Ok(value)
    }
    fn recheck(&self) -> Result<(), StoreError> {
        let configured = std::fs::canonicalize(&self.configured)
            .map_err(|_| unsupported("Matching plugin path changed"))?;
        let before = std::fs::symlink_metadata(&self.path)
            .map_err(|_| unsupported("Matching plugin directory changed"))?;
        let held = self
            .held
            .metadata()
            .map_err(|_| unsupported("Matching plugin handle unavailable"))?;
        if configured != self.path
            || before.file_type().is_symlink()
            || !before.is_dir()
            || (before.dev(), before.ino()) != (self.dev, self.ino)
            || (held.dev(), held.ino()) != (self.dev, self.ino)
        {
            return Err(unsupported("Matching plugin namespace changed"));
        }
        if std::fs::read_dir(&self.path)
            .map_err(|_| unsupported("Matching plugin directory read unavailable"))?
            .next()
            .is_some()
        {
            return Err(unsupported(
                "Matching general Python plugin hooks require adapter",
            ));
        }
        let after = std::fs::symlink_metadata(&self.path)
            .map_err(|_| unsupported("Matching plugin namespace changed"))?;
        if (after.dev(), after.ino()) != (self.dev, self.ino) {
            return Err(unsupported("Matching plugin namespace changed"));
        }
        Ok(())
    }
}
async fn observe_plugins(owner: Owner) -> Result<PluginIdentity, StoreError> {
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        PluginIdentity::observe()
    })
    .await
    .map_err(|_| unsupported("Matching plugin observation unsettled"))?
}
pub async fn require_supported_domain(auth: &AuthService, owner: Owner) -> Result<(), StoreError> {
    match observe_model_availability(owner.clone()).await? {
        ModelAvailability::Unavailable(_) => {}
        ModelAvailability::Unbound => {
            return Err(unsupported(
                "Matching model port unbound outside actual no-download/absent-cache domain",
            ))
        }
    }
    let _identity = observe_plugins(owner.clone()).await?;
    let session = auth.borrow_task_sql_session(owner).await?;
    let result = session
        .run(|c| {
            Box::pin(async move {
                sqlx::query_scalar::<_, bool>(PLUGIN_CONFIG_SQL)
                    .fetch_one(c)
                    .await
                    .map_err(StoreError::from)
            })
        })
        .await;
    let closed = session.close().await;
    closed?;
    if result?? {
        return Err(unsupported(
            "Matching Python plugin configuration requires signal/task adapter",
        ));
    }
    Ok(())
}
pub struct PreparedMatching {
    request: Request,
    snapshot: Option<Snapshot>,
    choices: Vec<Choice>,
    failure: Option<Failure>,
    witness: Option<UnavailableWitness>,
    plugin: Option<PluginIdentity>,
    cancellation: Arc<dyn MatchingCancellation>,
}
fn unsupported(s: &'static str) -> StoreError {
    StoreError::Unsupported(s)
}
fn strip(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
}
fn normalized(s: Option<&str>) -> String {
    strip(s.unwrap_or("")).to_lowercase()
}
fn failure(class: &str, message: String) -> Failure {
    Failure {
        class: class.into(),
        message,
    }
}
fn id(value: &Value) -> Result<Option<i64>, Failure> {
    match value {
        Value::Null => return Ok(None),
        Value::Bool(v) => return Ok(Some(i64::from(*v))),
        Value::Number(v) => {
            if let Some(n) = v.as_i64().or_else(|| {
                v.as_f64()
                    .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v < i64::MAX as f64)
                    .map(|v| v.trunc() as i64)
            }) {
                return Ok(Some(n));
            }
        }
        Value::String(v) => {
            if let Some(n) = dispatcharr_ingest_core::source_integer_literal(v)
                .and_then(|v| v.parse::<i64>().ok())
            {
                return Ok(Some(n));
            }
        }
        _ => {}
    }
    let repr = crate::python_action::collection_text(value, 8192)
        .unwrap_or_else(|| "<unrepresentable>".into());
    Err(failure(
        if value.is_array() || value.is_object() {
            "TypeError"
        } else {
            "ValueError"
        },
        format!("Field 'id' expected a number but got {repr}."),
    ))
}
fn ids(request: &Request) -> Result<Option<Vec<i64>>, Failure> {
    let values = match request {
        Request::All => return Ok(None),
        Request::Single(v) => return id(v).map(|v| Some(v.into_iter().collect())),
        Request::Selected(Value::Array(v)) => v.clone(),
        Request::Selected(Value::String(v)) => v.chars().map(|c| json!(c.to_string())).collect(),
        Request::Selected(Value::Object(v)) => v.keys().map(|s| json!(s)).collect(),
        Request::Selected(v) => {
            return Err(failure(
                "TypeError",
                format!(
                    "'{}' object is not iterable",
                    crate::channel_memberships::datatype(v)
                ),
            ))
        }
    };
    values
        .iter()
        .map(id)
        .collect::<Result<Vec<_>, _>>()
        .map(|v| Some(v.into_iter().flatten().collect()))
}
fn known(error: &StoreError) -> Option<Failure> {
    let StoreError::Database(sqlx::Error::Database(db)) = error else {
        return None;
    };
    let pg = db.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()?;
    if pg.severity() != sqlx::postgres::PgSeverity::Error || pg.position().is_some() {
        return None;
    }
    // Same measured Source9 mapping/diagnostic fields as normal_channel_tasks.
    let class = match pg.code() {
        "P0001" => "ProgrammingError",
        "22001" => "DataError",
        "23505" | "23514" => "IntegrityError",
        _ => return None,
    };
    if [Some(pg.message()), pg.detail(), pg.hint(), pg.r#where()]
        .into_iter()
        .flatten()
        .any(|v| v.len() > 8192)
    {
        return None;
    }
    let mut message = pg.message().to_owned();
    for (label, value) in [
        ("DETAIL", pg.detail()),
        ("HINT", pg.hint()),
        ("CONTEXT", pg.r#where()),
    ] {
        if let Some(value) = value {
            message.push_str(&format!("\n{label}:  {value}"))
        }
    }
    Some(failure(class, strip(&message).to_owned()))
}
pub async fn preflight(
    auth: &AuthService,
    request: &Request,
    owner: Owner,
    cancellation: Arc<dyn MatchingCancellation>,
) -> Result<Option<PreparedMatching>, StoreError> {
    if cancellation.is_draining() {
        return Ok(None);
    }
    let ids = match ids(request) {
        Ok(v) => v,
        Err(e) => {
            return Ok(Some(PreparedMatching {
                request: request.clone(),
                snapshot: None,
                choices: Vec::new(),
                failure: Some(e),
                witness: None,
                plugin: None,
                cancellation: cancellation.clone(),
            }))
        }
    };
    require_supported_domain(auth, owner.clone()).await?;
    let plugin = observe_plugins(owner.clone()).await?;
    let witness = match observe_model_availability(owner.clone()).await? {
        ModelAvailability::Unavailable(v) => v,
        ModelAvailability::Unbound => return Err(unsupported("Matching model domain unbound")),
    };
    let snapshot = match sql::snapshot(auth, request, ids, owner.clone()).await {
        Ok(v) => v,
        Err(error) => {
            if let Some(e) = known(&error) {
                return Ok(Some(PreparedMatching {
                    request: request.clone(),
                    snapshot: None,
                    choices: Vec::new(),
                    failure: Some(e),
                    witness: Some(witness),
                    plugin: Some(plugin),
                    cancellation: cancellation.clone(),
                }));
            } else {
                return Err(error);
            }
        }
    };
    if cancellation.is_draining() {
        return Ok(None);
    }
    // Preclaim is read-only domain/snapshot admission. Fuzzy CPU and actual
    // progress run only AFTER claim, without retaining any SQL writer/session.
    Ok(Some(PreparedMatching {
        request: request.clone(),
        snapshot: Some(snapshot),
        choices: Vec::new(),
        failure: None,
        witness: Some(witness),
        plugin: Some(plugin),
        cancellation,
    }))
}
enum PlanError {
    Known(Failure),
    Draining,
    Capacity,
    Bridge,
}
type ProgressCallback<'a> = dyn FnMut(usize, usize, &str, &str) -> Result<(), PlanError> + 'a;
fn plan(
    request: &Request,
    s: &Snapshot,
    cancellation: &dyn MatchingCancellation,
    notice: &mut ProgressCallback<'_>,
) -> Result<Vec<Choice>, PlanError> {
    if cancellation.is_draining() {
        return Err(PlanError::Draining);
    }
    let single = matches!(request, Request::Single(_));
    if single {
        if let Some(channel) = s.channels.first() {
            notice(1, 0, &channel.name, "matching")?;
        }
    }
    if let Some(region) = &s.region {
        if !region.is_string() {
            return Err(PlanError::Known(failure(
                "AttributeError",
                format!(
                    "'{}' object has no attribute 'strip'",
                    crate::channel_memberships::datatype(region)
                ),
            )));
        }
    }
    let region = s
        .region
        .as_ref()
        .and_then(Value::as_str)
        .map(|v| strip(v).to_lowercase());
    let mut catalog = Vec::new();
    for e in &s.epgs {
        if cancellation.is_draining() {
            return Err(PlanError::Draining);
        }
        if e.name.is_empty() {
            continue;
        }
        catalog.push(EpgCandidate {
            id: e.id,
            name: e.name.clone(),
            tvg_id: normalized(e.tvg.as_deref()),
            norm_name: algorithm::normalize(&e.name, &s.settings),
            priority: e.priority,
        });
    }
    if !matches!(request, Request::Single(_)) {
        catalog.sort_by(|a, b| b.priority.cmp(&a.priority))
    }
    let by_id = s.epgs.iter().map(|e| (e.id, e)).collect::<HashMap<_, _>>();
    let mut exact = HashMap::with_capacity(catalog.len());
    for epg in &catalog {
        if !epg.tvg_id.is_empty() {
            exact.entry(epg.tvg_id.as_str()).or_insert(epg.id);
        }
    }
    let mut choices = Vec::with_capacity(s.channels.len());
    let total = s.channels.len();
    let mut resolved = 0;
    let mut cells = 0usize;
    if !single {
        notice(total, 0, "", "starting")?;
    }
    let lengths = catalog
        .iter()
        .map(|e| e.norm_name.chars().count())
        .collect::<Vec<_>>();
    for (index, channel) in s.channels.iter().enumerate() {
        if cancellation.is_draining() {
            return Err(PlanError::Draining);
        }
        if !single && (index < 5 || index % 5 == 0 || index == total - 1) {
            let short = channel.name.chars().take(50).collect::<String>();
            notice(total, resolved, &short, "matching")?;
        }
        let tvg = normalized(channel.tvg.as_deref());
        let station = normalized(channel.station.as_deref());
        let mut choice = Choice {
            channel: channel.clone(),
            epg: None,
            via: None,
        };
        // Exact SINGLE IEXACT/first-PK resolution is supplied by the SQL helper;
        // never substitute the bulk normalized index for padded stored strings.
        if matches!(request, Request::Single(_)) {
            if let Some((id, gracenote)) = s.single_exact {
                if let Some(epg) = by_id.get(&id) {
                    choice.epg = Some((**epg).clone());
                    let value = normalized(epg.tvg.as_deref());
                    choice.via = Some(if gracenote {
                        format!("gracenote:{value}")
                    } else {
                        value
                    });
                }
            }
        } else {
            for key in [&tvg, &station] {
                if !key.is_empty() {
                    if let Some(id) = exact.get(key.as_str()) {
                        choice.epg = by_id.get(id).map(|v| (**v).clone());
                        break;
                    }
                }
            }
        }
        if choice.epg.is_none() {
            let query = algorithm::normalize(&channel.name, &s.settings);
            if !query.is_empty() {
                let mut scanner = algorithm::Scanner::new(&query, region.as_deref());
                let query_len = query.chars().count();
                for (index, epg) in catalog.iter().enumerate() {
                    if cancellation.is_draining() {
                        return Err(PlanError::Draining);
                    }
                    let work = query_len.saturating_mul(lengths[index]);
                    cells = cells.saturating_add(work);
                    if work > 1_000_000 || cells > 32_000_000 {
                        return Err(PlanError::Capacity);
                    }
                    scanner.push(index, epg);
                }
                let scan = scanner.finish();
                if matches!(
                    algorithm::selection(
                        &scan,
                        s.channels.len() > 1 && !matches!(request, Request::Single(_)),
                        true
                    ),
                    Decision::AcceptBest
                ) {
                    if let Some(best) = scan.best {
                        choice.epg = by_id.get(&best.candidate.id).map(|v| (**v).clone());
                        choice.via = Some(best.candidate.tvg_id)
                    }
                }
            }
            // NeedBestModel/NeedCandidateModel under the sealed actual
            // unavailable domain follows Source get_sentence_transformer None.
            // Unbound adapters never enter this planner.
        }
        if choice.epg.is_some() {
            resolved += 1;
        }
        choices.push(choice);
    }
    if !single {
        notice(total, resolved, "", "completed")?;
    }
    Ok(choices)
}
async fn event(events: &Arc<dyn UpdateEventSink>, payload: Value, owner: Owner) {
    let _ = events
        .send_retained("updates", json!({"type":"update","data":payload}), owner)
        .await;
}
async fn progress(
    events: &Arc<dyn UpdateEventSink>,
    total: usize,
    matched: usize,
    name: &str,
    stage: &str,
    owner: Owner,
) {
    let percent = if total == 0 {
        0.0
    } else {
        ((matched as f64 / total as f64 * 1000.0).round_ties_even()) / 10.0
    };
    event(events,json!({"type":"epg_matching_progress","total":total,"matched":matched,"remaining":total.saturating_sub(matched),"current_channel":name,"stage":stage,"progress_percent":percent}),owner).await;
}
struct CpuNotice {
    total: usize,
    matched: usize,
    name: String,
    stage: String,
    ack: std::sync::mpsc::SyncSender<()>,
}
async fn run_plan(
    request: Request,
    snapshot: Snapshot,
    cancellation: Arc<dyn MatchingCancellation>,
    events: Arc<dyn UpdateEventSink>,
    owner: Owner,
) -> Result<Vec<Choice>, PlanError> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<CpuNotice>(1);
    let worker_owner = owner.clone();
    let task = tokio::task::spawn_blocking(move || {
        let _owner = worker_owner;
        plan(
            &request,
            &snapshot,
            cancellation.as_ref(),
            &mut |total, matched, name, stage| {
                let (ack, received) = std::sync::mpsc::sync_channel(1);
                sender
                    .blocking_send(CpuNotice {
                        total,
                        matched,
                        name: name.to_owned(),
                        stage: stage.to_owned(),
                        ack,
                    })
                    .map_err(|_| PlanError::Bridge)?;
                received.recv().map_err(|_| PlanError::Bridge)?;
                Ok(())
            },
        )
    });
    while let Some(notice) = receiver.recv().await {
        progress(
            &events,
            notice.total,
            notice.matched,
            &notice.name,
            &notice.stage,
            owner.clone(),
        )
        .await;
        let _ = notice.ack.send(());
    }
    // Every return observes the ACTUAL CPU join. Cancellation/drop cannot free
    // the real worker owner while blocking work or callbacks still exist.
    task.await.map_err(|_| PlanError::Bridge)?
}
async fn fill_normalization_cache(
    auth: &AuthService,
    settings: &Value,
    owner: Owner,
) -> Result<(), StoreError> {
    let Some(cache) = &auth.notification_cache else {
        return Ok(());
    };
    let value = settings.clone();
    let loaded = cache
        .clone()
        .with_retained_scope(owner)
        .settings_group_bounded("epg_settings", move || async move { Ok(value) })
        .await?;
    if loaded != *settings {
        return Err(unsupported("Matching normalization settings changed"));
    }
    Ok(())
}
fn normalization_needed(request: &Request, snapshot: &Snapshot) -> bool {
    let channel_name = snapshot.channels.iter().any(|c| !c.name.is_empty());
    match request {
        // Source's single exact lookup never normalizes the EPG display name.
        Request::Single(_) => channel_name,
        Request::Selected(_) if snapshot.channels.is_empty() => false,
        // Bulk builds its named catalog even with no unassigned channels.
        Request::All | Request::Selected(_) => {
            channel_name || snapshot.epgs.iter().any(|e| !e.name.is_empty())
        }
    }
}
fn to_error(error: StoreError) -> ExecutionError {
    if let Some(e) = known(&error) {
        ExecutionError::Raised {
            class: e.class,
            message: e.message,
        }
    } else {
        ExecutionError::Operational(error)
    }
}
pub async fn execute(
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    mut prepared: PreparedMatching,
    _task: Uuid,
    effects: Arc<dyn MatchingEffects>,
    owner: Owner,
) -> Result<Value, ExecutionError> {
    if let Some(error) = prepared.failure {
        if matches!(&prepared.request, Request::Single(_)) {
            require_supported_domain(&auth, owner.clone())
                .await
                .map_err(ExecutionError::Operational)?;
            if let Some(snapshot) = &prepared.snapshot {
                if let Some(channel) = snapshot.channels.first() {
                    if normalization_needed(&prepared.request, snapshot) {
                        fill_normalization_cache(&auth, &snapshot.settings, owner.clone())
                            .await
                            .map_err(ExecutionError::Operational)?;
                    }
                    progress(&events, 1, 0, &channel.name, "matching", owner.clone()).await;
                }
            }
        }
        return failed_body(&auth, &events, &prepared.request, error, owner).await;
    }
    if prepared.witness.is_none() {
        return Err(ExecutionError::Operational(unsupported(
            "Matching model witness missing",
        )));
    }
    prepared
        .plugin
        .as_ref()
        .ok_or_else(|| ExecutionError::Operational(unsupported("Matching plugin witness missing")))?
        .recheck()
        .map_err(ExecutionError::Operational)?;
    require_supported_domain(&auth, owner.clone())
        .await
        .map_err(ExecutionError::Operational)?;
    let expected = prepared
        .snapshot
        .ok_or_else(|| ExecutionError::Operational(unsupported("Matching snapshot missing")))?;
    let actual = sql::snapshot(
        &auth,
        &prepared.request,
        ids(&prepared.request)
            .map_err(|_| ExecutionError::Operational(unsupported("Matching argument changed")))?,
        owner.clone(),
    )
    .await
    .map_err(to_error)?;
    if actual != expected {
        return Err(ExecutionError::Operational(unsupported(
            "Matching latest worker snapshot changed before effects",
        )));
    }
    if matches!(&prepared.request, Request::Selected(_)) && actual.channels.is_empty() {
        if let Some(region) = &actual.region {
            if !region.is_string() {
                return failed_body(
                    &auth,
                    &events,
                    &prepared.request,
                    failure(
                        "AttributeError",
                        format!(
                            "'{}' object has no attribute 'strip'",
                            crate::channel_memberships::datatype(region)
                        ),
                    ),
                    owner,
                )
                .await;
            }
        }
        event(&events,json!({"success":true,"type":"epg_match","refresh_channels":true,"matches_count":0,"message":"No selected channels found for EPG matching","associations":[]}),owner).await;
        return Ok(json!("No selected channels found for EPG matching."));
    }
    if !matches!(&prepared.request, Request::Single(_)) {
        if let Some(region) = &actual.region {
            if !region.is_string() {
                return failed_body(
                    &auth,
                    &events,
                    &prepared.request,
                    failure(
                        "AttributeError",
                        format!(
                            "'{}' object has no attribute 'strip'",
                            crate::channel_memberships::datatype(region)
                        ),
                    ),
                    owner,
                )
                .await;
            }
        }
    }
    // Source normalization fills the ordinary cache only once actual admission
    // and all supported-domain checks have succeeded. No writer held on CPU.
    if normalization_needed(&prepared.request, &actual) {
        fill_normalization_cache(&auth, &actual.settings, owner.clone())
            .await
            .map_err(ExecutionError::Operational)?;
    }
    prepared.choices = match run_plan(
        prepared.request.clone(),
        actual.clone(),
        prepared.cancellation.clone(),
        events.clone(),
        owner.clone(),
    )
    .await
    {
        Ok(choices) => choices,
        Err(PlanError::Known(error)) => {
            return failed_body(&auth, &events, &prepared.request, error, owner).await
        }
        Err(PlanError::Draining) => {
            return Err(ExecutionError::Operational(unsupported(
                "Matching claimed CPU stopped during actual drain",
            )))
        }
        Err(PlanError::Capacity) => {
            return Err(ExecutionError::Operational(unsupported(
                "Matching CPU work capacity",
            )))
        }
        Err(PlanError::Bridge) => {
            return Err(ExecutionError::Operational(unsupported(
                "Matching CPU/progress physical settlement failed",
            )))
        }
    };
    if matches!(&prepared.request, Request::Single(_)) {
        return single(
            auth,
            events,
            prepared.request,
            prepared.choices,
            &actual,
            effects,
            owner,
        )
        .await;
    }
    let mut unchanged = 0;
    let mut updates = Vec::new();
    for choice in &prepared.choices {
        if let Some(epg) = &choice.epg {
            if choice.channel.current == Some(epg.id) {
                unchanged += 1
            } else {
                updates.push((choice.channel.id, epg.id))
            }
        }
    }
    require_supported_domain(&auth, owner.clone())
        .await
        .map_err(ExecutionError::Operational)?;
    let associations = sql::apply_bulk(&auth, updates, owner.clone())
        .await
        .map_err(to_error)?;
    let epgs = sql::child_epgs(&auth, &associations, owner.clone())
        .await
        .map_err(to_error)?;
    let children = bulk_children(&epgs, &associations);
    for child in children {
        effects
            .publish(child, owner.clone())
            .await
            .map_err(to_error)?;
    }
    let n = associations.len();
    let selected = matches!(prepared.request, Request::Selected(_));
    let adjective = if selected { " selected" } else { "" };
    let message = if n > 0 {
        format!("EPG matching complete: {n}{adjective} channel(s) updated")
    } else if unchanged > 0 {
        format!(
            "EPG matching complete: {unchanged}{adjective} channel(s) already correctly matched"
        )
    } else {
        "EPG matching complete: no matches found".into()
    };
    event(&events,json!({"success":true,"type":"epg_match","refresh_channels":true,"matches_count":n,"message":message,"associations":associations}),owner).await;
    Ok(json!(format!(
        "Done. {n}{adjective} channel(s) updated ({unchanged} unchanged)."
    )))
}
fn bulk_children(epgs: &[EpgRow], associations: &[Value]) -> Vec<MatchingChild> {
    let ids = associations
        .iter()
        .filter_map(|v| v["epg_data_id"].as_i64())
        .collect::<HashSet<_>>();
    let mut result = Vec::new();
    let mut sd: Vec<(i64, Vec<i64>)> = Vec::new();
    for epg in epgs {
        if !ids.contains(&epg.id) {
            continue;
        }
        match epg.source_type.as_deref() {
            Some("dummy") => {}
            Some("schedules_direct") => {
                if !epg.has_programs {
                    if let Some(source) = epg.source {
                        if let Some((_, list)) = sd.iter_mut().find(|(id, _)| *id == source) {
                            list.push(epg.id)
                        } else {
                            sd.push((source, vec![epg.id]))
                        }
                    }
                }
            }
            _ => result.push(MatchingChild::ParsePrograms(epg.id)),
        }
    }
    for (source, ids) in sd {
        if ids.len() >= 3 {
            result.push(MatchingChild::SdMapped(source))
        } else {
            result.extend(ids.into_iter().map(MatchingChild::ParsePrograms))
        }
    }
    result
}
async fn single(
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    request: Request,
    choices: Vec<Choice>,
    snapshot: &Snapshot,
    effects: Arc<dyn MatchingEffects>,
    owner: Owner,
) -> Result<Value, ExecutionError> {
    let requested = match &request {
        Request::Single(v) => v.clone(),
        _ => Value::Null,
    };
    let Some(choice) = choices.first() else {
        single_result(
            &auth,
            &events,
            SingleNotice {
                id: requested,
                matched: false,
                message: "Channel not found",
                channel: None,
                epg: None,
                in_memory_assignment: None,
            },
            owner,
        )
        .await?;
        return Ok(json!({"matched":false,"message":"Channel not found"}));
    };
    let channel = &choice.channel;
    let Some(epg) = &choice.epg else {
        let no_catalog = snapshot.epgs.iter().all(|e| e.name.is_empty())
            && normalized(channel.tvg.as_deref()).is_empty()
            && normalized(channel.station.as_deref()).is_empty();
        let message = if no_catalog {
            "No EPG data available for matching (from active sources)".into()
        } else {
            format!("No suitable EPG match found for channel '{}'", channel.name)
        };
        progress(&events, 1, 0, &channel.name, "completed", owner.clone()).await;
        single_result(
            &auth,
            &events,
            SingleNotice {
                id: json!(channel.id),
                matched: false,
                message: &message,
                channel: Some(channel.id),
                epg: None,
                in_memory_assignment: None,
            },
            owner,
        )
        .await?;
        return Ok(json!({"matched":false,"message":message}));
    };
    let unchanged = channel.current == Some(epg.id);
    if !unchanged {
        require_supported_domain(&auth, owner.clone())
            .await
            .map_err(ExecutionError::Operational)?;
        let session = auth
            .borrow_task_sql_session(owner.clone())
            .await
            .map_err(ExecutionError::Operational)?;
        let action = async {
            sql::update_single(&session, channel.id, epg.id).await?;
            if let Some(cache) = &auth.notification_cache {
                let mut cmd = redis::cmd("INCR");
                cmd.arg("epg_content_generation");
                let _ = cache
                    .clone()
                    .with_retained_scope(owner.clone())
                    .command::<i64>(cmd)
                    .await;
            }
            if epg.source_type.as_deref() != Some("dummy") {
                effects
                    .publish(MatchingChild::ParsePrograms(epg.id), owner.clone())
                    .await?;
            }
            sql::mark_custom(&session, channel).await?;
            Ok::<_, StoreError>(())
        }
        .await;
        let closed = session.close().await;
        closed.map_err(ExecutionError::Operational)?;
        if let Err(error) = action {
            if let Some(error) = known(&error) {
                let message = format!("Error during matching: {}", error.message);
                // Source instance.epg_data was assigned BEFORE save. Its error WS
                // exposes that in-memory value even though actual SQL was refused.
                single_result(
                    &auth,
                    &events,
                    SingleNotice {
                        id: json!(channel.id),
                        matched: false,
                        message: &message,
                        channel: Some(channel.id),
                        epg: None,
                        in_memory_assignment: Some(epg.id),
                    },
                    owner,
                )
                .await?;
                return Ok(json!({"matched":false,"message":message}));
            }
            return Err(ExecutionError::Operational(error));
        }
    }
    let mut message = if unchanged {
        format!(
            "Channel '{}' already matched with EPG '{}'",
            channel.name, epg.name
        )
    } else {
        format!("Channel '{}' matched with EPG '{}'", channel.name, epg.name)
    };
    if let Some(via) = &choice.via {
        if !via.is_empty() {
            message.push_str(&format!(" (matched via: {via})"))
        }
    }
    progress(&events, 1, 1, &channel.name, "completed", owner.clone()).await;
    single_result(
        &auth,
        &events,
        SingleNotice {
            id: json!(channel.id),
            matched: true,
            message: &message,
            channel: Some(channel.id),
            epg: Some(epg),
            in_memory_assignment: None,
        },
        owner,
    )
    .await?;
    let mut result = json!({"matched":true,"message":message,"epg_name":epg.name,"epg_id":epg.id});
    if unchanged {
        result["unchanged"] = json!(true)
    }
    Ok(result)
}
struct SingleNotice<'a> {
    id: Value,
    matched: bool,
    message: &'a str,
    channel: Option<i64>,
    epg: Option<&'a EpgRow>,
    in_memory_assignment: Option<i64>,
}
async fn single_result(
    auth: &AuthService,
    events: &Arc<dyn UpdateEventSink>,
    notice: SingleNotice<'_>,
    owner: Owner,
) -> Result<(), ExecutionError> {
    // Source catches serializer/send failures. Unknown physical SQL closure is
    // still retained by TaskSqlSession; no successful close is fabricated.
    let mut payload = json!({"type":"single_channel_epg_match","channel_id":notice.id,"matched":notice.matched,"message":notice.message});
    if let Some(channel) = notice.channel {
        let session = auth
            .borrow_task_sql_session(owner.clone())
            .await
            .map_err(ExecutionError::Operational)?;
        let value = sql::serialize(&session, channel).await;
        session.close().await.map_err(ExecutionError::Operational)?;
        match value {
            Ok(Some(mut value)) => {
                if let Some(assigned) = notice.in_memory_assignment {
                    value["epg_data_id"] = json!(assigned);
                    if value["override"]["epg_data_id"].is_null() {
                        value["effective_epg_data_id"] = json!(assigned)
                    }
                }
                payload["channel"] = value
            }
            Ok(None) => return Ok(()),
            Err(error) if known(&error).is_some() => return Ok(()),
            Err(error) => return Err(ExecutionError::Operational(error)),
        }
    }
    if let Some(epg) = notice.epg {
        payload["epg_id"] = json!(epg.id);
        payload["epg_name"] = json!(epg.name)
    }
    event(events, payload, owner).await;
    Ok(())
}
async fn failed_body(
    auth: &AuthService,
    events: &Arc<dyn UpdateEventSink>,
    request: &Request,
    error: Failure,
    owner: Owner,
) -> Result<Value, ExecutionError> {
    if let Request::Single(value) = request {
        let message = format!("Error during matching: {}", error.message);
        single_result(
            auth,
            events,
            SingleNotice {
                id: value.clone(),
                matched: false,
                message: &message,
                channel: id(value).ok().flatten(),
                epg: None,
                in_memory_assignment: None,
            },
            owner,
        )
        .await?;
        Ok(json!({"matched":false,"message":message}))
    } else {
        Err(ExecutionError::Raised {
            class: error.class,
            message: error.message,
        })
    }
}

const PLUGIN_CONFIG_SQL: &str = "SELECT EXISTS(SELECT 1 FROM plugins_pluginconfig)";
#[cfg(test)]
pub(crate) fn schema_statements() -> Vec<(&'static str, String)> {
    sql::schema_statements()
}

#[cfg(test)]
mod cache_regression {
    use super::*;

    #[test]
    fn source_empty_names_do_not_load_normalization_settings_early() {
        // Pinned normalize_name returns on empty input before loading settings.
        // Single exact lookup uses SQL, whereas bulk normalizes its catalog.
        let mut snapshot = Snapshot {
            channels: vec![ChannelRow {
                id: 1,
                name: String::new(),
                tvg: Some("exact".into()),
                station: None,
                current: None,
                auto_created: false,
                group: None,
            }],
            epgs: vec![EpgRow {
                id: 2,
                name: "Named EPG".into(),
                tvg: Some("exact".into()),
                source: Some(1),
                priority: 0,
                source_type: Some("xmltv".into()),
                has_programs: false,
            }],
            region: None,
            settings: json!({}),
            single: true,
            single_exact: Some((2, false)),
        };
        assert!(!normalization_needed(&Request::Single(json!(1)), &snapshot));
        assert!(normalization_needed(
            &Request::Selected(json!([1])),
            &snapshot
        ));
        snapshot.channels.clear();
        assert!(!normalization_needed(
            &Request::Selected(json!([])),
            &snapshot
        ));
        assert!(normalization_needed(&Request::All, &snapshot));
        snapshot.epgs[0].name.clear();
        assert!(!normalization_needed(&Request::All, &snapshot));
    }
}
