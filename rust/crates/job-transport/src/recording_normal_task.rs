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
        .await
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
        }))
    }
}
