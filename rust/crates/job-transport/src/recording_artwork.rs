//! Explicit typed artwork publication only; no child consumption or default
//! graph activation. Original Dispatcharr/Celery/Kombu authors retain credit.
use crate::{
    configured_recording_service::{ArtworkError, ArtworkSink},
    protocol::{self, ProtocolError},
    transport::{Config, Jobs},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
#[derive(Clone, Copy, Debug)]
pub struct DirectTask {
    id: Uuid,
    root: Uuid,
    parent: Option<Uuid>,
}
impl DirectTask {
    pub(crate) fn series(message: &crate::series_protocol::SeriesMessage) -> Option<Self> {
        if message.parent.is_some()
            || message.root != message.task_id
            || (message.task_id.get_version_num() != 4
                || message.task_id.get_variant() != uuid::Variant::RFC4122)
        {
            return None;
        }
        Some(Self {
            id: message.task_id,
            root: message.task_id,
            parent: None,
        })
    }
    pub(crate) fn recurring(message: &crate::default_protocol::Message) -> Option<Self> {
        if message.kind != crate::default_protocol::Kind::RecurringMaintenance
            || message.expires.is_some()
            || (message.task_id.get_version_num() != 4
                || message.task_id.get_variant() != uuid::Variant::RFC4122)
        {
            return None;
        }
        Some(Self {
            id: message.task_id,
            root: message.task_id,
            parent: None,
        })
    }
    pub(crate) fn root(self) -> Uuid {
        self.root
    }
    pub(crate) fn parent(self) -> Option<Uuid> {
        self.parent
    }
    pub fn id(self) -> Uuid {
        self.id
    }
}
/// Explicit single-child admission from an actual bounded completed Refresh
/// result and parsed Series frame. This is not a generic parent UUID option.
/// The cached backend and frame are trusted inputs, not cryptographic lineage.
/// Source scope: direct Refresh root=self, immediate Series root=parent=Refresh.
#[derive(Clone, Copy, Debug)]
pub struct RefreshSeriesChild {
    task: DirectTask,
}
impl RefreshSeriesChild {
    pub fn from_completed_refresh(
        refresh: &crate::refresh_protocol::RefreshMessage,
        backend: &[u8],
        child: &crate::series_protocol::SeriesMessage,
    ) -> Option<Self> {
        let canonical =
            |id: Uuid| id.get_version_num() == 4 && id.get_variant() == uuid::Variant::RFC4122;
        let mut expected_call = crate::refresh_protocol::RefreshCall::initial(refresh.source_id);
        // Both actual Source producers are captured: model signal uses tuple
        // argsrepr, repeat apply_async uses list argsrepr. No extra kwargs/args.
        expected_call.list_args = refresh.call.list_args;
        if refresh.parent.is_some()
            || refresh.root != refresh.task_id
            || refresh.retry != 0
            || refresh.eta.is_some()
            || refresh.force
            || !(1..=i64::from(i32::MAX)).contains(&refresh.source_id)
            || refresh.call != expected_call
            || child.root != refresh.task_id
            || child.parent != Some(refresh.task_id)
            || child.task_id == refresh.task_id
            || !canonical(refresh.task_id)
            || !canonical(child.task_id)
        {
            return None;
        }
        let completed =
            crate::refresh_result::parse_success(backend, refresh.task_id, refresh.source_id)
                .ok()?;
        if completed.parent.is_some()
            || completed.missing_source
            || completed
                .children
                .iter()
                .filter(|id| **id == child.task_id)
                .count()
                != 1
        {
            return None;
        }
        Some(Self {
            task: DirectTask {
                id: child.task_id,
                root: child.root,
                parent: child.parent,
            },
        })
    }
    /// Explicit captured API producer spelling; preserves the model/list-only
    /// constructor above. Source import view passes force=False explicitly.
    /// No general kwargs, force, ETA/retry or arbitrary-parent authority.
    pub fn from_completed_refresh_api(
        refresh: &crate::refresh_protocol::RefreshMessage,
        backend: &[u8],
        child: &crate::series_protocol::SeriesMessage,
    ) -> Option<Self> {
        if refresh.force
            || refresh.call.list_args
            || refresh.call.args != vec![json!(refresh.source_id)]
            || refresh.call.kwargs != json!({"force":false}).as_object()?.clone()
        {
            return None;
        }
        // Validation-only clone changes exactly the already-checked redundant
        // false argument. Never rewrites retained FIFO/CAS/publication bytes.
        let mut validation = refresh.clone();
        validation.call = crate::refresh_protocol::RefreshCall::initial(refresh.source_id);
        Self::from_completed_refresh(&validation, backend, child)
    }
    pub(crate) fn admit(
        self,
        message: &crate::series_protocol::SeriesMessage,
    ) -> Option<DirectTask> {
        (message.task_id == self.task.id
            && message.root == self.task.root
            && message.parent == self.task.parent)
            .then_some(self.task)
    }
}
/// Raw spelling is checked separately because legacy protocol UUID parsing
/// intentionally normalizes inputs; this new configured scope must not do so.
pub(crate) fn canonical_frame(raw: &[u8], task: DirectTask) -> bool {
    if raw.len() > protocol::MAX_ENVELOPE {
        return false;
    }
    let Ok(frame) = serde_json::from_slice::<serde_json::Value>(raw) else {
        return false;
    };
    let canonical = task.id.to_string();
    let root = task.root.to_string();
    let parent = task
        .parent
        .map(|id| json!(id.to_string()))
        .unwrap_or(serde_json::Value::Null);
    frame
        .get("headers")
        .and_then(|h| h.get("id"))
        .and_then(serde_json::Value::as_str)
        == Some(canonical.as_str())
        && frame
            .get("headers")
            .and_then(|h| h.get("root_id"))
            .and_then(serde_json::Value::as_str)
            == Some(root.as_str())
        && frame.get("headers").and_then(|h| h.get("parent_id")) == Some(&parent)
}
fn python_utc(t: DateTime<Utc>) -> String {
    let base = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    if t.timestamp_subsec_micros() == 0 {
        format!("{base}+00:00")
    } else {
        format!("{base}.{:06}+00:00", t.timestamp_subsec_micros())
    }
}
pub(crate) fn encode(
    recording: i64,
    task: DirectTask,
    id: Uuid,
    reply: Uuid,
    delivery: Uuid,
    now: DateTime<Utc>,
) -> Result<Vec<u8>, ProtocolError> {
    if !(1..=i64::from(i32::MAX)).contains(&recording) {
        return Err(ProtocolError::InvalidArgument);
    }
    // Reuse the supported common protocol2 envelope, then set ONLY the captured
    // family, one integer argument, direct lineage and one-second countdown ETA.
    let mut frame: serde_json::Value = serde_json::from_slice(&protocol::encode(
        &protocol::Task::CreateBackup,
        id,
        reply,
        delivery,
        "rust@configured-recordings",
    )?)
    .map_err(|_| ProtocolError::InvalidArgument)?;
    frame["headers"]["task"] = json!("apps.channels.tasks.prefetch_recording_artwork");
    frame["headers"]["root_id"] = json!(task.root.to_string());
    frame["headers"]["parent_id"] = json!(task.id.to_string());
    frame["headers"]["argsrepr"] = json!(format!("[{recording}]"));
    frame["headers"]["eta"] = json!(python_utc(now + ChronoDuration::seconds(1)));
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[recording],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ProtocolError::InvalidArgument)?
    ));
    let bytes = serde_json::to_vec(&frame).map_err(|_| ProtocolError::InvalidArgument)?;
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    Ok(bytes)
}
/// Opening this typed publisher starts no intake. Its private Series transport
/// is never exposed as generic default/index/backup publication authority.
pub struct Publisher {
    jobs: Jobs,
    reply: Uuid,
    proofs: Option<
        std::sync::Mutex<
            std::collections::BTreeMap<Uuid, crate::recording_artwork_protocol::Message>,
        >,
    >,
    #[cfg(test)]
    owned_parented_route: bool,
}
impl Publisher {
    pub async fn connect(
        broker: &str,
        backend: &str,
        config: Config,
    ) -> Result<Self, crate::transport::Error> {
        let jobs = Jobs::connect_series(broker, backend, config).await?;
        Ok(Self {
            jobs,
            reply: Uuid::new_v4(),
            proofs: None,
            #[cfg(test)]
            owned_parented_route: false,
        })
    }
    #[cfg(test)]
    pub(crate) async fn connect_parented_fixture(
        broker: &str,
        backend: &str,
        database: &str,
        config: Config,
    ) -> Result<Self, crate::transport::Error> {
        let jobs = Jobs::connect_fixture(broker, backend, database, config).await?;
        jobs.require_parented_fixture("rust-owned-parented-artwork")?;
        Ok(Self {
            jobs,
            reply: Uuid::new_v4(),
            proofs: None,
            owned_parented_route: true,
        })
    }
    pub(crate) fn retain_normal_publications(mut self) -> Self {
        self.proofs = Some(std::sync::Mutex::new(std::collections::BTreeMap::new()));
        self
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), crate::transport::Error> {
        self.jobs.shutdown(timeout).await
    }
}
impl ArtworkSink for Publisher {
    fn take_publication(&self, id: Uuid) -> Option<crate::recording_artwork_protocol::Message> {
        self.proofs.as_ref()?.lock().ok()?.remove(&id)
    }

    fn publish<'a>(
        &'a self,
        recording: i64,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Uuid, ArtworkError>> {
        Box::pin(async move {
            // Python's clock is microsecond precision; trim actual Rust wallclock
            // precision explicitly, never reuse a captured Source timestamp.
            let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
                .ok_or(ArtworkError::Uncertain)?;
            let id = Uuid::new_v4();
            let bytes = encode(recording, task, id, self.reply, Uuid::new_v4(), now)
                .map_err(|_| ArtworkError::Uncertain)?;
            #[cfg(test)]
            if self.owned_parented_route {
                let mut frame: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(|_| ArtworkError::Uncertain)?;
                frame["properties"]["delivery_info"]["routing_key"] =
                    json!("rust-owned-parented-artwork");
                let raw = serde_json::to_vec(&frame).map_err(|_| ArtworkError::Uncertain)?;
                self.jobs
                    .publish_parented_artwork_fixture(&raw, id, owner)
                    .await
                    .map_err(|_| ArtworkError::Uncertain)?;
                return Ok(id);
            }
            let proof = if self.proofs.is_some() && task.parent().is_some() {
                Some(
                    crate::recording_artwork_protocol::parse(&bytes)
                        .ok_or(ArtworkError::Uncertain)?,
                )
            } else {
                None
            };
            self.jobs
                .publish_configured_artwork(&bytes, id, owner)
                .await
                .map_err(|_| ArtworkError::Uncertain)?;
            if let Some(proof) = proof {
                let mut proofs = self
                    .proofs
                    .as_ref()
                    .ok_or(ArtworkError::Uncertain)?
                    .lock()
                    .map_err(|_| ArtworkError::Uncertain)?;
                if proofs.len() >= 64 || proofs.insert(id, proof).is_some() {
                    return Err(ArtworkError::Uncertain);
                }
            }
            Ok(id)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_artwork_protocol_has_direct_parent_and_countdown_one() {
        let id = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let task = DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: parent,
            root: parent,
            parent: None,
        })
        .unwrap();
        let now: DateTime<Utc> = "2026-11-12T23:34:49.548943+00:00".parse().unwrap();
        let raw = encode(1, task, id, Uuid::new_v4(), Uuid::new_v4(), now).unwrap();
        let frame: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&STANDARD.decode(frame["body"].as_str().unwrap()).unwrap())
                .unwrap();
        assert_eq!(
            body,
            json!([[1],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        );
        assert_eq!(
            frame["headers"]["task"],
            "apps.channels.tasks.prefetch_recording_artwork"
        );
        assert_eq!(frame["headers"]["root_id"], parent.to_string());
        assert_eq!(frame["headers"]["parent_id"], parent.to_string());
        assert_eq!(frame["headers"]["eta"], "2026-11-12T23:34:50.548943+00:00");
        assert_eq!(frame["headers"]["ignore_result"], false);
        assert_eq!(
            frame["properties"]["delivery_info"],
            json!({"exchange":"","routing_key":"celery"})
        );
        assert!(frame["headers"]["expires"].is_null());
    }
    #[test]
    fn configured_parented_or_foreign_root_is_not_admitted() {
        let id = Uuid::new_v4();
        let other = Uuid::new_v4();
        assert!(DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: other,
            parent: Some(other)
        })
        .is_none());
        assert!(DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: other,
            parent: None
        })
        .is_none());
        assert!(DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: Uuid::nil(),
            root: Uuid::nil(),
            parent: None
        })
        .is_none());
        assert!(DirectTask::recurring(&crate::default_protocol::Message {
            task_id: id,
            kind: crate::default_protocol::Kind::RedisBackendCleanup,
            expires: None
        })
        .is_none());
    }
    #[test]
    fn publication_refuses_non_recording_identifiers() {
        let id = Uuid::new_v4();
        let task = DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: id,
            parent: None,
        })
        .unwrap();
        for recording in [0, -1, i64::from(i32::MAX) + 1] {
            assert!(encode(
                recording,
                task,
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                Utc::now()
            )
            .is_err());
        }
    }
}

#[cfg(test)]
mod canonical_controls {
    use super::*;
    #[test]
    fn raw_uppercase_or_missing_explicit_null_cannot_gain_configured_authority() {
        let id = Uuid::new_v4();
        let task = DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: id,
            parent: None,
        })
        .unwrap();
        let raw = protocol::encode(
            &protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "owned",
        )
        .unwrap();
        assert!(canonical_frame(&raw, task));
        let mut frame: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        frame["headers"]["id"] = json!(id.to_string().to_uppercase());
        assert!(!canonical_frame(&serde_json::to_vec(&frame).unwrap(), task));
        frame["headers"]["id"] = json!(id.to_string());
        frame["headers"]
            .as_object_mut()
            .unwrap()
            .remove("parent_id");
        assert!(!canonical_frame(&serde_json::to_vec(&frame).unwrap(), task));
    }
    #[test]
    fn version_four_with_wrong_variant_is_not_captured_rfc4122_domain() {
        let mut bytes = *Uuid::new_v4().as_bytes();
        bytes[8] &= 0x3f;
        let id = Uuid::from_bytes(bytes);
        assert_eq!(id.get_version_num(), 4);
        assert!(DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: id,
            parent: None
        })
        .is_none());
        assert!(DirectTask::recurring(&crate::default_protocol::Message {
            task_id: id,
            kind: crate::default_protocol::Kind::RecurringMaintenance,
            expires: None
        })
        .is_none());
    }
}

#[cfg(test)]
mod actual_parented_source_controls {
    use super::*;
    fn captured() -> serde_json::Value {
        serde_json::from_str(include_str!("../datasets/source-parented-recordings.json")).unwrap()
    }
    fn pair(
        source: &serde_json::Value,
        refresh_index: usize,
        series_index: usize,
    ) -> (
        crate::refresh_protocol::RefreshMessage,
        Vec<u8>,
        crate::series_protocol::SeriesMessage,
    ) {
        let observed = &source["observations"];
        let id =
            Uuid::parse_str(observed[refresh_index]["tasks"][0]["id"].as_str().unwrap()).unwrap();
        let child =
            Uuid::parse_str(observed[series_index]["tasks"][0]["id"].as_str().unwrap()).unwrap();
        let raw = STANDARD
            .decode(
                source["refresh_frames_raw_base64"][if refresh_index == 0 { 0 } else { 1 }]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
        let mut frame: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            frame["properties"]["delivery_info"],
            json!({"exchange":"","routing_key":"rust-owned-parented-refresh"})
        );
        frame["properties"]["delivery_info"]["routing_key"] = json!("celery");
        let refresh = crate::refresh_protocol::parse(&serde_json::to_vec(&frame).unwrap()).unwrap();
        assert_eq!(refresh.task_id, id);
        assert_eq!(refresh.call.list_args, refresh_index == 3);
        (
            refresh,
            serde_json::to_vec(&observed[refresh_index]["tasks"][0]["backend"]).unwrap(),
            crate::series_protocol::SeriesMessage {
                task_id: child,
                root: id,
                parent: Some(id),
            },
        )
    }
    #[test]
    fn actual_positive_and_repeat_lineages_keep_refresh_root_for_artwork() {
        let source = captured();
        for (a, b) in [(0, 1), (3, 4)] {
            let (refresh, backend, message) = pair(&source, a, b);
            assert!(DirectTask::series(&message).is_none()); // historical constructor unchanged
            let allowed =
                RefreshSeriesChild::from_completed_refresh(&refresh, &backend, &message).unwrap();
            let task = allowed.admit(&message).unwrap();
            let now: DateTime<Utc> = source["observations"][b]["began_utc"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let raw = encode(1, task, Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), now).unwrap();
            let frame: serde_json::Value = serde_json::from_slice(&raw).unwrap();
            assert_eq!(frame["headers"]["root_id"], refresh.task_id.to_string());
            assert_eq!(frame["headers"]["parent_id"], message.task_id.to_string());
            assert_ne!(frame["headers"]["root_id"], frame["headers"]["parent_id"]);
            let metadata = &source["observations"][b]["tasks"][0]["backend"];
            assert_eq!(metadata["parent_id"], refresh.task_id.to_string());
            let mut wrong = message.clone();
            wrong.task_id = Uuid::new_v4();
            assert!(allowed.admit(&wrong).is_none());
            wrong = message.clone();
            wrong.root = Uuid::new_v4();
            assert!(allowed.admit(&wrong).is_none());
            wrong = message.clone();
            wrong.parent = None;
            assert!(allowed.admit(&wrong).is_none());
        }
    }
    #[test]
    fn completed_parent_is_required_and_cannot_be_replaced_by_arbitrary_uuid() {
        let source = captured();
        let (refresh, backend, message) = pair(&source, 0, 1);
        let mut absent: serde_json::Value = serde_json::from_slice(&backend).unwrap();
        absent["children"] = json!([]);
        assert!(RefreshSeriesChild::from_completed_refresh(
            &refresh,
            &serde_json::to_vec(&absent).unwrap(),
            &message
        )
        .is_none());
        let mut failed: serde_json::Value = serde_json::from_slice(&backend).unwrap();
        failed["status"] = json!("PENDING");
        assert!(RefreshSeriesChild::from_completed_refresh(
            &refresh,
            &serde_json::to_vec(&failed).unwrap(),
            &message
        )
        .is_none());
        let mut another = refresh.clone();
        another.parent = Some(Uuid::new_v4());
        assert!(RefreshSeriesChild::from_completed_refresh(&another, &backend, &message).is_none());
        let mut another_child = message.clone();
        another_child.root = Uuid::new_v4();
        assert!(
            RefreshSeriesChild::from_completed_refresh(&refresh, &backend, &another_child)
                .is_none()
        );
    }
}

#[cfg(test)]
mod parented_refusals {
    use super::*;
    #[test]
    fn completion_cannot_authorize_force_deferred_or_unrelated_raw_lineage() {
        let root = Uuid::parse_str("cb78e036-04f3-40d0-a07c-591ed05ef09c").unwrap();
        let child = Uuid::parse_str("9f7c6077-8205-4e75-883e-098c181747c9").unwrap();
        let refresh = crate::refresh_protocol::RefreshMessage {
            task_id: root,
            source_id: 1,
            force: false,
            retry: 0,
            root,
            parent: None,
            eta: None,
            call: crate::refresh_protocol::RefreshCall::initial(1),
        };
        let message = crate::series_protocol::SeriesMessage {
            task_id: child,
            root,
            parent: Some(root),
        };
        let result = serde_json::to_vec(&json!({"status":"SUCCESS","result":null,"traceback":null,"children":[[[child.to_string(),null],null]],"date_done":"2026-11-13T07:06:36.436298+00:00","task_id":root.to_string()})).unwrap();
        let cap = RefreshSeriesChild::from_completed_refresh(&refresh, &result, &message).unwrap();
        for mode in 0..4 {
            let mut wrong = refresh.clone();
            match mode {
                0 => wrong.force = true,
                1 => wrong.retry = 1,
                2 => wrong.eta = Some(Utc::now()),
                _ => wrong.call = crate::refresh_protocol::RefreshCall::initial(2),
            }
            assert!(
                RefreshSeriesChild::from_completed_refresh(&wrong, &result, &message).is_none()
            );
        }
        let task = cap.admit(&message).unwrap();
        let mut raw: serde_json::Value = serde_json::from_slice(
            &protocol::encode(
                &protocol::Task::CreateBackup,
                child,
                Uuid::new_v4(),
                Uuid::new_v4(),
                "owned",
            )
            .unwrap(),
        )
        .unwrap();
        raw["headers"]["root_id"] = json!(root.to_string());
        raw["headers"]["parent_id"] = json!(root.to_string());
        assert!(canonical_frame(&serde_json::to_vec(&raw).unwrap(), task));
        for key in ["id", "root_id", "parent_id"] {
            let mut altered = raw.clone();
            altered["headers"][key] = json!(raw["headers"][key].as_str().unwrap().to_uppercase());
            assert!(!canonical_frame(
                &serde_json::to_vec(&altered).unwrap(),
                task
            ));
            altered = raw.clone();
            altered["headers"].as_object_mut().unwrap().remove(key);
            assert!(!canonical_frame(
                &serde_json::to_vec(&altered).unwrap(),
                task
            ));
        }
    }
}

#[cfg(test)]
mod api_producer_tests {
    use super::*;
    use serde_json::Value;
    fn source_composition() -> (
        crate::refresh_protocol::RefreshMessage,
        Vec<u8>,
        crate::series_protocol::SeriesMessage,
    ) {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/api-refresh-source37-wire.json"))
                .unwrap();
        let raw = serde_json::to_vec(&source["actual_envelope_projection"]).unwrap();
        let parent = crate::refresh_protocol::parse(&raw).unwrap();
        let mut metadata = source["actual_parented_refresh_backend"].clone();
        metadata["task_id"] = json!(parent.task_id.to_string());
        let child = crate::series_protocol::SeriesMessage {
            task_id: Uuid::parse_str(source["actual_parented_series_task_id"].as_str().unwrap())
                .unwrap(),
            root: parent.task_id,
            parent: Some(parent.task_id),
        };
        (parent, serde_json::to_vec(&metadata).unwrap(), child)
    }
    #[test]
    fn actual_source37_false_kwarg_has_explicit_api_constructor_only() {
        let (parent, backend, child) = source_composition();
        assert!(RefreshSeriesChild::from_completed_refresh(&parent, &backend, &child).is_none());
        assert!(
            RefreshSeriesChild::from_completed_refresh_api(&parent, &backend, &child).is_some()
        );
    }
    #[test]
    fn api_scope_rejects_missing_force_true_and_list_producer() {
        let (parent, backend, child) = source_composition();
        let mut missing = parent.clone();
        missing.call.kwargs.clear();
        assert!(
            RefreshSeriesChild::from_completed_refresh_api(&missing, &backend, &child).is_none()
        );
        let mut forced = parent.clone();
        forced.force = true;
        forced.call.kwargs.insert("force".into(), json!(true));
        assert!(
            RefreshSeriesChild::from_completed_refresh_api(&forced, &backend, &child).is_none()
        );
        let mut list = parent;
        list.call.list_args = true;
        assert!(RefreshSeriesChild::from_completed_refresh_api(&list, &backend, &child).is_none());
    }
}
