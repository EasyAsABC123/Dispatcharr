//! Source Stream preview scopes, using the actual existing raw-owner lease.
//! Original Dispatcharr authors retain selection/key/release algorithm credit.
use super::*;
#[derive(Clone)]
pub struct LivePreviewAssignment {
    pub(super) stream: i64,
    pub(super) profile: i64,
    hash: String,
    scoped: bool,
}
impl LivePreviewAssignment {
    pub fn new(stream: i64, hash: &str, profile: i64, scoped: bool) -> Result<Self, Error> {
        if stream <= 0
            || profile <= 0
            || hash.is_empty()
            || hash.len() > 255
            || hash
                .chars()
                .any(|c| c.is_control() || matches!(c, ':' | '/' | '\\'))
            || hash.rsplit_once(".p").is_some_and(|(_, tail)| {
                !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit())
            })
        {
            return Err(Error::InvalidRequest);
        }
        if hash.len()
            + if scoped {
                2 + profile.to_string().len()
            } else {
                0
            }
            > 256
        {
            return Err(Error::InvalidRequest);
        }
        Ok(Self {
            stream,
            profile,
            hash: hash.into(),
            scoped,
        })
    }
    pub fn with_profile(&self, profile: i64) -> Result<Self, Error> {
        if self.scoped && profile != self.profile {
            return Err(Error::InvalidRequest);
        }
        Self::new(self.stream, &self.hash, profile, self.scoped)
    }
    pub fn worker_id(&self) -> String {
        if self.scoped {
            format!("{}.p{}", self.hash, self.profile)
        } else {
            self.hash.clone()
        }
    }
    pub(super) fn alias_key(&self) -> String {
        if self.scoped {
            format!("stream_profile:{}:p{}", self.stream, self.profile)
        } else {
            format!("stream_profile:{}:preview", self.stream)
        }
    }
}
impl Lease {
    /// Returns true only for an actual reusable Source preview assignment.
    /// False includes real stale cleanup; caller then reserves normally.
    pub async fn prepare_preview_assignment(
        &self,
        assignment: LivePreviewAssignment,
    ) -> Result<bool, Error> {
        self.preview_assignment(assignment, None)
            .await
            .map(|reply| reply == 1)
    }
    pub async fn publish_preview_assignment(
        &self,
        assignment: LivePreviewAssignment,
        ttl: Duration,
    ) -> Result<(), Error> {
        if ttl.is_zero() || u64::try_from(ttl.as_millis()).is_err() {
            return Err(Error::InvalidRequest);
        }
        self.preview_assignment(assignment, Some(ttl))
            .await
            .map(|_| ())
    }
    async fn preview_assignment(
        &self,
        assignment: LivePreviewAssignment,
        publish_ttl: Option<Duration>,
    ) -> Result<i64, Error> {
        let worker = self
            .inner
            .keys
            .owner
            .strip_prefix("live:channel:")
            .and_then(|v| v.strip_suffix(":owner"))
            .ok_or(Error::InvalidRequest)?;
        if worker != assignment.worker_id() || self.inner.release_started.load(Ordering::Acquire) {
            return Err(Error::InvalidRequest);
        }
        match lease_mutation_observed(
            &self.inner,
            LeaseMutation::Preview {
                assignment,
                publish_ttl,
            },
            true,
        )
        .await?
        {
            reply @ (0 | 1) => Ok(reply),
            -2 => Err(Error::AlreadyReserved),
            _ => Err(Error::Lost),
        }
    }
}
