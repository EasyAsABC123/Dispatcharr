//! Exact normal UUID/named protocol variants, never a task-id remapping.
//! Original Dispatcharr/Celery/Kombu authors retain protocol/application credit.
use crate::{
    clocked_recording_envelope::ObservedProducerEnvelope,
    normal_named_recording::{
        Message as NamedMessage, NormalNamedRecordingPublication, NormalNamedRecordingPublications,
    },
    normal_recording_publication::{
        NormalRecordingPublication, NormalRecordingPublications, NormalRecordingPublisher,
    },
    recording_dvr_normal::{transport as retained_transport, Error},
    transport::{self, Jobs},
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
#[derive(Clone, Debug)]
pub(crate) enum Message {
    Clocked(ObservedProducerEnvelope),
    Named(NamedMessage),
}
#[derive(Clone, Debug)]
pub enum TaskKey {
    Uuid(uuid::Uuid),
    Named(String),
}
impl Message {
    pub(crate) fn key(&self) -> TaskKey {
        match self {
            Self::Clocked(m) => TaskKey::Uuid(m.task_id()),
            Self::Named(m) => TaskKey::Named(m.task_key().into()),
        }
    }
    pub(crate) fn raw_key(&self) -> String {
        match self {
            Self::Clocked(m) => m.task_id().to_string(),
            Self::Named(m) => m.task_key().into(),
        }
    }
    pub(crate) async fn write_revoked(
        self,
        jobs: Jobs,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        retained_transport(owner, move |guard| async move {
            match self {
                Self::Clocked(m) => jobs.write_normal_recording_revoked(&m, guard).await,
                Self::Named(m) => jobs.write_named_recording_revoked(&m, guard).await,
            }
        })
        .await
    }
    pub(crate) fn recording_id(&self) -> i64 {
        match self {
            Self::Clocked(m) => m.recording_id(),
            Self::Named(m) => m.recording_id(),
        }
    }
    pub(crate) fn channel_id(&self) -> i64 {
        match self {
            Self::Clocked(m) => m.channel_id(),
            Self::Named(m) => m.channel_id(),
        }
    }
    pub(crate) fn start(&self) -> DateTime<Utc> {
        match self {
            Self::Clocked(m) => m.start(),
            Self::Named(m) => m.argument_start(),
        }
    }
    pub(crate) fn end(&self) -> DateTime<Utc> {
        match self {
            Self::Clocked(m) => m.end(),
            Self::Named(m) => m.argument_end(),
        }
    }
    pub(crate) fn root_key(&self) -> String {
        match self {
            Self::Clocked(message) => message.task_id().to_string(),
            Self::Named(message) => message.root().to_owned(),
        }
    }
    pub(crate) async fn write(
        self,
        jobs: Jobs,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        retained_transport(owner, move |guard| async move {
            match self {
                Self::Clocked(m) => jobs.write_recording_dvr(&m, guard).await,
                Self::Named(m) => jobs.write_named_recording_dvr(&m, guard).await,
            }
        })
        .await
    }
    pub(crate) async fn write_with_children(
        self,
        jobs: Jobs,
        children: Vec<uuid::Uuid>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        retained_transport(owner, move |guard| async move {
            match self {
                Self::Clocked(m) => {
                    jobs.write_recording_dvr_with_children(&m, children, guard)
                        .await
                }
                Self::Named(m) => {
                    jobs.write_named_recording_dvr_with_children(&m, children, guard)
                        .await
                }
            }
        })
        .await
    }
}
enum Candidate {
    Clocked(transport::DvrCandidate),
    Named(transport::NamedDvrCandidate),
}
enum Proof {
    Clocked(NormalRecordingPublication),
    Named(NormalNamedRecordingPublication),
}
pub(crate) struct Task {
    pub message: Message,
    proof: Proof,
    candidate: Option<Candidate>,
    claimed: bool,
}
impl Task {
    pub(crate) async fn claim(
        &mut self,
        jobs: Jobs,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        let candidate = self
            .candidate
            .take()
            .ok_or(Error::Unsupported("normal task already claimed"))?;
        retained_transport(owner, move |guard| async move {
            match candidate {
                Candidate::Clocked(c) => jobs.claim_recording_dvr(c, guard).await,
                Candidate::Named(c) => jobs.claim_named_recording_dvr(c, guard).await,
            }
        })
        .await?;
        self.claimed = true;
        Ok(())
    }
    /// Only actual successful CAS can construct this opaque event witness.
    pub(crate) fn event_context(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<ClaimedRecordingEvent, Error> {
        if !self.claimed {
            return Err(Error::Unsupported(
                "Recording hook context before actual claim",
            ));
        }
        let raw = match &self.message {
            Message::Clocked(message) => message.raw_frame(),
            Message::Named(message) => message.raw_frame(),
        };
        let context = dispatcharr_store::PluginHookTaskContext::from_native_claim(raw)
            .map_err(|_| Error::Unsupported("Claimed recording hook frame unsupported"))?;
        if context.task_id() != self.message.raw_key()
            || context.task_name() != "apps.channels.tasks.run_recording"
        {
            return Err(Error::Unsupported(
                "Claimed recording hook identity mismatch",
            ));
        }
        let trail = Arc::new(RecordingTrail {
            id: context.task_id().into(),
            _owner: owner,
        });
        Ok(ClaimedRecordingEvent { context, trail })
    }
    pub(crate) fn complete(
        &self,
        clocked: &NormalRecordingPublications,
        named: &NormalNamedRecordingPublications,
    ) -> Result<(), Error> {
        match &self.proof {
            Proof::Clocked(p) => clocked
                .complete(p)
                .map_err(|_| Error::Unsupported("clocked completion origin/generation")),
            Proof::Named(p) => named
                .complete(p)
                .map_err(|_| Error::Unsupported("named completion origin/generation")),
        }
    }
}
pub(crate) async fn peek(
    jobs: Jobs,
    publisher: &NormalRecordingPublisher,
    clocked: &NormalRecordingPublications,
    named: &NormalNamedRecordingPublications,
    expected: Option<Arc<[u8]>>,
    owner: Arc<dyn Send + Sync>,
) -> Result<Option<Task>, Error> {
    let expected = match expected {
        Some(raw) => raw,
        None => {
            let Some(raw) = publisher
                .peek_existing(owner.clone())
                .await
                .map_err(Error::Publication)?
            else {
                return Ok(None);
            };
            raw
        }
    };
    publisher
        .adopt_existing(expected.clone(), owner.clone())
        .await
        .map_err(Error::Publication)?;
    let observed: serde_json::Value = serde_json::from_slice(&expected)
        .map_err(|_| Error::Unsupported("normal expected JSON"))?;
    let key = observed
        .get("headers")
        .and_then(|h| h.get("id"))
        .and_then(serde_json::Value::as_str)
        .ok_or(Error::Unsupported("normal expected task identity"))?;
    if key.starts_with("dvr-now-") || key.starts_with("dvr-recover-") {
        let read = jobs.clone();
        let raw = expected.clone();
        let candidate = retained_transport(owner, move |guard| async move {
            read.peek_named_recording_dvr(Some(raw.as_ref()), guard)
                .await
        })
        .await?
        .ok_or(Error::Transport(transport::Error::ClaimRace))?;
        let proof = named
            .matching(candidate.message.raw_frame())
            .map_err(|_| Error::Unsupported("normal named publication registry"))?
            .ok_or(Error::Unsupported("unretained normal named publication"))?;
        let message = Message::Named(candidate.message.clone());
        Ok(Some(Task {
            message,
            proof: Proof::Named(proof),
            candidate: Some(Candidate::Named(candidate)),
            claimed: false,
        }))
    } else {
        let read = jobs.clone();
        let raw = expected.clone();
        let candidate = retained_transport(owner, move |guard| async move {
            read.peek_normal_recording_dvr(Some(raw.as_ref()), guard)
                .await
        })
        .await?
        .ok_or(Error::Transport(transport::Error::ClaimRace))?;
        let proof = clocked
            .matching(candidate.message.raw_frame())
            .map_err(|_| Error::Unsupported("normal clocked publication registry"))?
            .ok_or(Error::Unsupported("unretained normal clocked publication"))?;
        let message = Message::Clocked(candidate.message.clone());
        Ok(Some(Task {
            message,
            proof: Proof::Clocked(proof),
            candidate: Some(Candidate::Clocked(candidate)),
            claimed: false,
        }))
    }
}

/// Native run_recording has no Celery child publisher: media processes and
/// permanent-URL SQL patches are not child tasks. This witness is minted only
/// after the real queue CAS above; never from caller JSON or an HTTP context.
/// Its restricted Connect-only use forbids scripts and every configured plugin.
pub(crate) struct ClaimedRecordingEvent {
    context: dispatcharr_store::PluginHookTaskContext,
    trail: Arc<RecordingTrail>,
}
impl ClaimedRecordingEvent {
    pub(crate) fn context(&self) -> dispatcharr_store::PluginHookTaskContext {
        self.context.clone()
    }
    pub(crate) fn trail(&self) -> Arc<dyn dispatcharr_store::NativeTaskTrail> {
        self.trail.clone()
    }
}
struct RecordingTrail {
    id: String,
    _owner: Arc<dyn Send + Sync>,
}
impl dispatcharr_store::NativeTaskTrail for RecordingTrail {
    fn claimed_task_id(&self) -> &str {
        &self.id
    }
    fn settled_after(
        &self,
        cursor: u64,
    ) -> Result<dispatcharr_store::NativeTaskTrailDelta, dispatcharr_store::StoreError> {
        if cursor != 0 {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "Recording task has no native child publication cursor",
            ));
        }
        Ok(dispatcharr_store::NativeTaskTrailDelta {
            next_cursor: 0,
            entries: Vec::new(),
            state: dispatcharr_store::NativeTaskTrailState::Closed,
        })
    }
}
