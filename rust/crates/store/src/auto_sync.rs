//! Normal Source auto-channel synchronization over the actual owned services.
//! Original Dispatcharr authors retain all application/algorithm/protocol credit.
//! DRAFT: orchestration is being joined; this module is not registered in Main.
use crate::{
    auto_sync_config::{Error as ConfigError, Groups},
    auto_sync_create::{ChannelDraft, Pending},
    auto_sync_epg::{Plan, Touched},
    auto_sync_existing::Existing,
    auto_sync_numbers::Numbers,
    auto_sync_orphans::Orphans,
    auto_sync_profiles::Profiles,
    auto_sync_regex::{Renamed, Rules},
    auto_sync_resolve::{Logo, Resolver},
    auto_sync_streams::{Sort, Streams},
    recording_http::{RecordingEffectError, RecordingJobs},
    task_sql_session::TaskSqlSession,
    AuthService, StoreError,
};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Map, Value};
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;
/// Main must supply the real Jobs producer and final child-metadata owner. There
/// is no empty/default implementation or caller-provided parent/child ID list.
pub trait EpgDispatch: Send + Sync {
    /// Bounded optional observer; normal effects and task semantics are unchanged.
    fn observe(&self, _stage: &'static str) {}
    fn dispatch(&self, plan: Plan, owner: Owner)
        -> BoxFuture<'_, Result<(), RecordingEffectError>>;
}
#[derive(Clone)]
pub struct Services {
    pub jobs: Arc<dyn RecordingJobs>,
    pub epg: Arc<dyn EpgDispatch>,
    pub regex: crate::RegexExecutor,
}
pub enum Error {
    Store(StoreError),
    Caught(String),
    Model {
        reason: &'static str,
        message: String,
    },
    Unsupported,
    Unsettled,
}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}
impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self::Store(value.into())
    }
}
#[derive(Default)]
pub struct Report {
    pub created: u64,
    pub updated: u64,
    pub deleted: u64,
    pub failed: u64,
    pub failures: Vec<Value>,
    pub caught: Option<String>,
}
impl Report {
    fn failure(&mut self, name: &str, id: i64, group: &str, reason: &str, error: String) {
        self.failed += 1;
        if self.failures.len() < 1000 {
            self.failures.push(json!({"stream_name":name,"stream_id":id,"group":group,"reason":reason,"error":error}))
        }
    }
    pub fn message_suffix(&self) -> String {
        if let Some(error) = &self.caught {
            return format!(" Auto-sync error: {error}.");
        }
        let mut parts = Vec::with_capacity(4);
        if self.created > 0 {
            parts.push(format!("{} channel(s) created", self.created));
        }
        if self.updated > 0 {
            parts.push(format!("{} updated", self.updated));
        }
        if self.deleted > 0 {
            parts.push(format!("{} deleted", self.deleted));
        }
        if self.failed > 0 {
            parts.push(format!("{} failed", self.failed));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" Auto-sync: {}.", parts.join(", "))
        }
    }
    pub fn value(&self) -> Value {
        let mut result = json!({"status":if self.caught.is_some(){"error"}else{"ok"},"channels_created":self.created,"channels_updated":self.updated,"channels_deleted":self.deleted,"channels_failed":self.failed,"failed_stream_details":self.failures});
        if let Some(error) = &self.caught {
            result["error"] = json!(error)
        }
        result
    }
}
fn unsupported() -> Error {
    Error::Unsupported
}
fn numeric(value: &Value) -> Result<f64, Error> {
    match value {
        Value::Bool(value) => Ok(if *value { 1.0 } else { 0.0 }),
        Value::Number(value) => value
            .as_f64()
            .filter(|n| n.is_finite())
            .ok_or_else(unsupported),
        _ => Err(unsupported()),
    }
}
fn integer_text(value: f64) -> String {
    if value == 0.0 {
        "0".into()
    } else {
        format!("{:.0}", value.trunc())
    }
}
fn advance(cursor: f64) -> Result<f64, Error> {
    let mut next = cursor + 1.0;
    if next.fract() != 0.0 {
        next = next.trunc() + 1.0
    }
    if !next.is_finite() || next == cursor {
        return Err(unsupported());
    }
    Ok(next)
}
fn station(value: &Value) -> Result<Option<String>, Error> {
    if value.is_null() {
        return Ok(None);
    }
    if let Value::String(value) = value {
        return Ok(Some(value.clone()));
    }
    crate::python_action::collection_text(value, 65536)
        .map(Some)
        .ok_or_else(unsupported)
}
fn model_error(error: StoreError) -> Error {
    if let StoreError::Database(error) = &error {
        if let Some(server) = error.as_database_error() {
            if let Some(code) = server.code() {
                if code.starts_with("22") || code.starts_with("23") {
                    let mut message = server.message().to_owned();
                    if let Some(pg) = server.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
                        if let Some(detail) = pg.detail() {
                            message.push_str("\nDETAIL:  ");
                            message.push_str(detail);
                        }
                    }
                    return Error::Model {
                        reason: if code.starts_with("23") {
                            "INTEGRITY_ERROR"
                        } else {
                            "OTHER"
                        },
                        message,
                    };
                }
            }
        }
    }
    Error::Store(error)
}
fn outcome(value: Option<RecordingEffectError>) -> Result<(), Error> {
    match value {
        None | Some(RecordingEffectError::SettledFailure) => Ok(()),
        Some(RecordingEffectError::Unsupported) => Err(Error::Unsupported),
        Some(RecordingEffectError::Unsettled) => Err(Error::Unsettled),
    }
}
async fn dispatch(epg: &Arc<dyn EpgDispatch>, plan: Touched, owner: Owner) -> Result<(), Error> {
    let plan = plan.seal().await?;
    match epg.dispatch(plan, owner).await {
        Ok(()) => Ok(()),
        Err(RecordingEffectError::SettledFailure) => Err(Error::Unsettled),
        Err(RecordingEffectError::Unsupported) => Err(Error::Unsupported),
        Err(RecordingEffectError::Unsettled) => Err(Error::Unsettled),
    }
}
async fn prune(
    auth: &AuthService,
    session: &TaskSqlSession,
    jobs: &Arc<dyn RecordingJobs>,
    owner: Owner,
    existing: &Existing,
    streams: &Streams,
    empty: bool,
) -> Result<u64, Error> {
    let mut selection = auth
        .prepare_task_channel_deletion(session.clone(), jobs.clone(), owner)
        .await?;
    let mut after = 0;
    loop {
        let page = existing.prune_page(streams, after, empty).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().ok_or_else(unsupported)?.0;
        selection
            .append(page.into_iter().map(|(_, id)| id).collect())
            .await?;
    }
    let report = selection.execute().await?;
    outcome(report.effect_outcome)?;
    Ok(report.selected)
}
/// Known unimplemented configurations are fenced BEFORE the normal raw CAS.
/// Provider-dependent operational outcomes still belong to the accepted body.
pub async fn preflight(session: &TaskSqlSession, account: i64) -> Result<(), StoreError> {
    let groups = Groups::capture(session.clone(), account)
        .await
        .map_err(|_| StoreError::Unsupported("Auto-sync group capability preflight unavailable"))?;
    let mut after = 0;
    while let Some(group) = groups
        .next(after)
        .await
        .map_err(|_| StoreError::Unsupported("Auto-sync group capability preflight unavailable"))?
    {
        after = group.ordinal;
        if group
            .properties
            .get("compact_numbering")
            .is_some_and(crate::xc_live_provider::truthy)
        {
            crate::auto_sync_compact::preflight(&group)?;
        }
    }
    groups.close().await.map_err(|_| {
        StoreError::Unsupported("Auto-sync group capability preflight did not drain")
    })?;
    Ok(())
}
/// Actual Source body. Called for BOTH inline successful Single imports and the
/// standalone sync_auto_channels task, using their actual runtime producer scope.
#[allow(clippy::too_many_arguments)]
pub async fn sync(
    session: TaskSqlSession,
    auth: AuthService,
    services: Services,
    account: i64,
    scan_start: DateTime<Utc>,
    owner: Owner,
) -> Result<Report, Error> {
    match body(session, auth, services, account, scan_start, owner).await {
        Ok(report) => Ok(report),
        Err(BodyError::Caught(error)) => Ok(Report {
            caught: Some(error),
            ..Report::default()
        }),
        Err(BodyError::Operational(error)) => Err(error),
    }
}
enum BodyError {
    Caught(String),
    Operational(Error),
}
impl From<Error> for BodyError {
    fn from(value: Error) -> Self {
        match value {
            Error::Caught(message) | Error::Model { message, .. } => Self::Caught(message),
            value => Self::Operational(value),
        }
    }
}
impl From<StoreError> for BodyError {
    fn from(value: StoreError) -> Self {
        Self::Operational(Error::Store(value))
    }
}
impl From<sqlx::Error> for BodyError {
    fn from(value: sqlx::Error) -> Self {
        Self::Operational(Error::Store(value.into()))
    }
}
impl From<ConfigError> for BodyError {
    fn from(value: ConfigError) -> Self {
        match value {
            ConfigError::Caught(value) => Self::Caught(value),
            ConfigError::Unsupported => Self::Operational(Error::Unsupported),
            ConfigError::Store(value) => Self::Operational(Error::Store(value)),
        }
    }
}
#[allow(clippy::too_many_arguments)]
async fn body(
    session: TaskSqlSession,
    auth: AuthService,
    services: Services,
    account: i64,
    scan_start: DateTime<Utc>,
    owner: Owner,
) -> Result<Report, BodyError> {
    let Services { jobs, epg, regex } = services;
    let account_properties = super::auto_sync_config::account_properties(&session, account).await?;
    let groups = Groups::capture(session.clone(), account).await?;
    let numbers = Numbers::prepare(Arc::new(session.clone()), account).await?;
    let mut report = Report::default();
    let mut group_after = 0;
    while let Some(group) = groups.next(group_after).await? {
        group_after = group.ordinal;
        let compact = group
            .properties
            .get("compact_numbering")
            .is_some_and(crate::xc_live_provider::truthy);
        let rules = Rules::new(&group.properties, regex.clone());
        let mode = match group.properties.get("channel_numbering_mode") {
            None => "fixed",
            Some(Value::String(value)) => value,
            Some(_) => "",
        };
        let fallback = numeric(
            group
                .properties
                .get("channel_numbering_fallback")
                .unwrap_or(&json!(1)),
        )?;
        let reverse = group
            .properties
            .get("channel_sort_reverse")
            .is_some_and(crate::xc_live_provider::truthy);
        let sort = match group
            .properties
            .get("channel_sort_order")
            .and_then(Value::as_str)
        {
            Some("name") => Sort::Name,
            Some("tvg_id") => Sort::Tvg,
            Some("updated_at") => Sort::Updated,
            _ => Sort::Provider,
        };
        let streams = Streams::capture(
            session.clone(),
            account,
            group.id,
            scan_start,
            sort,
            reverse,
            rules.may_filter(),
        )
        .await?;
        let filtered = rules.filter(&streams, owner.clone()).await?;
        streams.finish_order(sort, reverse, filtered).await?;
        let existing = Existing::prepare(session.clone(), account, group.id).await?;
        let first = streams.next(0).await?;
        let target_group = match group.optional_fk("group_override")? {
            None => group.id,
            Some(id) => session
                .run(move |c| {
                    Box::pin(async move {
                        sqlx::query_scalar::<_, i64>(
                            "SELECT id::bigint FROM dispatcharr_channels_channelgroup WHERE id=$1",
                        )
                        .bind(id)
                        .fetch_optional(c)
                        .await
                    })
                })
                .await??
                .unwrap_or(group.id),
        };
        let custom_logo = group.optional_fk("custom_logo_id")?;
        let custom_epg = group.optional_fk("custom_epg_id")?;
        let resolver = Resolver::prepare(
            session.clone(),
            &streams,
            custom_logo,
            custom_epg,
            group
                .properties
                .get("force_dummy_epg")
                .is_some_and(crate::xc_live_provider::truthy),
        )
        .await?;
        if first.is_empty() {
            epg.observe("collector_prune_begin");
            report.deleted += prune(
                &auth,
                &session,
                &jobs,
                owner.clone(),
                &existing,
                &streams,
                true,
            )
            .await?;
            resolver.close().await?;
            existing.close().await?;
            streams.close().await?;
            continue;
        }
        let profiles =
            Profiles::capture(session.clone(), group.profiles()?, group.skip_memberships()).await?;
        let profile = match group.optional_fk("stream_profile_id") {
            Ok(Some(id)) => {
                session
                    .run(move |c| {
                        Box::pin(async move {
                            sqlx::query_scalar::<_, i64>(
                                "SELECT id::bigint FROM core_streamprofile WHERE id=$1",
                            )
                            .bind(id)
                            .fetch_optional(c)
                            .await
                        })
                    })
                    .await??
            }
            Ok(None) | Err(ConfigError::Caught(_)) => None,
            Err(error) => return Err(error.into()),
        };
        if !compact {
            let mut after = 0;
            let mut cursor = group.start;
            loop {
                let page = streams.next(after).await?;
                if page.is_empty() {
                    break;
                }
                let mut changes = Vec::new();
                for stream in page {
                    after = stream.ordinal;
                    if let Some((id, current)) = existing.for_stream(stream.id).await? {
                        match numbers
                            .pick(mode, stream.stream_chno, cursor, fallback, group.end)
                            .await?
                        {
                            Some(number) => {
                                numbers.reserve(number).await?;
                                if current["channel_number"].as_f64() != Some(number) {
                                    changes.push((id, number))
                                }
                                if mode == "fixed" {
                                    cursor = advance(cursor)?
                                }
                            }
                            None => {
                                if let Some(number) = current["channel_number"].as_f64() {
                                    numbers.reserve(number).await?
                                }
                            }
                        }
                    }
                }
                existing.stage_numbers(changes).await?;
            }
            existing.flush_numbers().await?;
        }

        if mode == "fixed" {
            if let Some(end) = group.end {
                let mut selection = auth
                    .prepare_task_channel_deletion(session.clone(), jobs.clone(), owner.clone())
                    .await?;
                let mut after = 0;
                loop {
                    let page = existing.overflow_page(after, group.start, end).await?;
                    if page.is_empty() {
                        break;
                    }
                    after = page.last().ok_or_else(unsupported)?.0;
                    for (_, _, number) in &page {
                        numbers.discard(*number).await?
                    }
                    let ids = page.into_iter().map(|(_, id, _)| id).collect::<Vec<_>>();
                    selection.append(ids.clone()).await?;
                    existing.forget(ids).await?;
                }
                let deleted = selection.execute().await?;
                outcome(deleted.effect_outcome)?;
                report.deleted += deleted.selected;
            }
        }
        let pending = Pending::begin(session.clone()).await?;
        let mut cursor = group.start;
        let mut after = 0;
        loop {
            let page = streams.next(after).await?;
            if page.is_empty() {
                break;
            }
            let renamed = rules
                .rename(
                    page.iter().map(|stream| stream.name.clone()).collect(),
                    owner.clone(),
                )
                .await?;
            let mut creates = Vec::new();
            let mut processed = Vec::new();
            for (stream, renamed) in page.into_iter().zip(renamed) {
                after = stream.ordinal;
                processed.push(stream.id);
                let failure_name = stream.name.clone();
                let failure_id = stream.id;
                let applied: Result<(), Error> = async {
                    if !stream.properties_valid {
                        return Err(Error::Caught(format!(
                            "'{}' object has no attribute 'get'",
                            stream.properties_kind
                        )));
                    }
                    let station = station(&stream.station)?;
                    let renamed = match renamed {
                        Renamed::Name(name) => name,
                        Renamed::Failure(message) => return Err(Error::Caught(message)),
                    };
                    let current = existing.for_stream(stream.id).await?;
                    let number = if current.is_none() {
                        match numbers
                            .pick(mode, stream.stream_chno, cursor, fallback, group.end)
                            .await?
                        {
                            Some(number) => {
                                numbers.reserve(number).await?;
                                Some(number)
                            }
                            None => {
                                let end = group.end.ok_or_else(unsupported)?;
                                let start = if mode == "provider" {
                                    fallback
                                } else {
                                    group.start
                                };
                                report.failure(
                                    &stream.name,
                                    stream.id,
                                    &group.name,
                                    "RANGE_EXHAUSTED",
                                    format!(
                                        "Channel number range {}-{} is full",
                                        integer_text(start),
                                        integer_text(end)
                                    ),
                                );
                                return Ok(());
                            }
                        }
                    } else {
                        None
                    };
                    let logo = resolver
                        .logo(
                            stream.logo_url.as_deref(),
                            stream.logo_rejected,
                            &stream.name,
                            stream.tvg_id.as_deref(),
                        )
                        .await
                        .map_err(model_error)?;
                    let guide = resolver.epg(stream.tvg_id.as_deref()).await?;
                    if let Some((id, _)) = current {
                        let mut patch = Map::new();
                        patch.insert("name".into(), json!(renamed));
                        patch.insert("tvg_id".into(), json!(stream.tvg_id));
                        patch.insert("tvc_guide_stationid".into(), json!(station));
                        patch.insert("channel_group_id".into(), json!(target_group));
                        patch.insert("epg_data_id".into(), json!(guide));
                        patch.insert("is_radio".into(), json!(stream.is_radio));
                        match logo {
                            Logo::Absent => {
                                patch.insert("logo_id".into(), Value::Null);
                            }
                            Logo::Use(id) => {
                                patch.insert("logo_id".into(), json!(id));
                            }
                            Logo::Rejected => {}
                        }
                        if let Some(profile) = profile {
                            patch.insert("stream_profile_id".into(), json!(profile));
                        }
                        existing.change(id, patch).await?;
                    } else {
                        creates.push(ChannelDraft {
                            stream: stream.id,
                            account,
                            number: number.ok_or_else(unsupported)?,
                            name: renamed,
                            tvg_id: stream.tvg_id,
                            tvc_guide_stationid: station,
                            group: target_group,
                            logo: match logo {
                                Logo::Use(id) => Some(id),
                                _ => None,
                            },
                            epg: guide,
                            profile,
                            is_radio: stream.is_radio,
                        });
                    }
                    if mode == "fixed" {
                        cursor = advance(cursor)?
                    }
                    Ok(())
                }
                .await;
                match applied {
                    Ok(()) => {}
                    Err(Error::Caught(message)) => {
                        report.failure(&failure_name, failure_id, &group.name, "OTHER", message)
                    }
                    Err(Error::Model { reason, message }) => {
                        report.failure(&failure_name, failure_id, &group.name, reason, message)
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            pending.add(creates).await?;
            streams.processed(processed).await?;
        }
        report.created += pending.create_channels().await.map_err(model_error)?;
        pending.link_streams().await.map_err(model_error)?;
        pending.memberships(&profiles).await.map_err(model_error)?;
        let mut touched = Touched::begin(session.clone()).await?;
        let mut after = 0;
        loop {
            let page = pending.epg_page(after).await?;
            if page.is_empty() {
                break;
            }
            after = page.last().ok_or_else(unsupported)?.0;
            touched
                .add(page.into_iter().filter_map(|(_, id)| id).collect())
                .await?;
        }
        dispatch(&epg, touched, owner.clone()).await?;
        epg.observe("existing_flush_begin");
        report.updated += existing.flush().await.map_err(model_error)?;
        let mut touched = Touched::begin(session.clone()).await?;
        let mut after = 0;
        loop {
            let page = existing.epg_page(after).await?;
            if page.is_empty() {
                break;
            }
            after = page.last().ok_or_else(unsupported)?.0;
            touched
                .add(page.into_iter().filter_map(|(_, id)| id).collect())
                .await?;
        }
        dispatch(&epg, touched, owner.clone()).await?;
        epg.observe("profiles_reconcile_begin");
        profiles.reconcile(&existing).await.map_err(model_error)?;
        epg.observe("profiles_reconcile_complete");
        epg.observe("collector_prune_begin");
        report.deleted += prune(
            &auth,
            &session,
            &jobs,
            owner.clone(),
            &existing,
            &streams,
            false,
        )
        .await?;
        epg.observe("collector_prune_complete");
        if compact {
            let packed = crate::auto_sync_compact::repack(&session, account, &group)
                .await
                .map_err(model_error)?;
            if packed.failed > 0 {
                report.failed += packed.failed;
                if report.failures.len() < 1000 {
                    report.failures.push(json!({"stream_name":null,"stream_id":null,"group":group.name,"reason":"RANGE_EXHAUSTED","error":format!("Compact pack: {} visible channel(s) could not fit in range {}{}",packed.failed,integer_text(group.start),group.end.filter(|n|*n!=0.0).map(|n|format!("-{}",integer_text(n))).unwrap_or_else(||"+".into()))}));
                }
            }
        }
        pending.close().await?;
        profiles.close().await?;
        resolver.close().await?;
        existing.close().await?;
        streams.close().await?;
    }
    epg.observe("groups_complete");
    let cleanup = account_properties.get("orphan_channel_cleanup");
    if cleanup != Some(&json!("never")) {
        let orphans = Orphans::capture(
            session.clone(),
            account,
            cleanup == Some(&json!("preserve_customized")),
        )
        .await?;
        let mut selection = auth
            .prepare_task_channel_deletion(session.clone(), jobs.clone(), owner.clone())
            .await?;
        let mut after = 0;
        loop {
            let page = orphans.next(after).await?;
            if page.is_empty() {
                break;
            }
            after = page.last().ok_or_else(unsupported)?.0;
            selection
                .append(page.into_iter().map(|(_, id)| id).collect())
                .await?;
        }
        let deleted = selection.execute().await?;
        outcome(deleted.effect_outcome)?;
        report.deleted += deleted.selected;
        orphans.close().await?;
    }
    numbers.close().await?;
    groups.close().await?;
    Ok(report)
}
