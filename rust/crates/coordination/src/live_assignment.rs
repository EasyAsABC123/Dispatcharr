//! Public Source aliases of an already reserved normal Channel worker.
//! Original Dispatcharr authors retain application/provider algorithm credit.
use super::*;
#[derive(Clone, Copy)]
pub struct LiveChannelAssignment {
    pub(super) channel: i64,
    pub(super) stream: i64,
}
impl LiveChannelAssignment {
    pub fn new(channel: i64, stream: i64) -> Result<Self, Error> {
        if channel <= 0 || stream <= 0 {
            Err(Error::InvalidRequest)
        } else {
            Ok(Self { channel, stream })
        }
    }
}
impl Lease {
    /// Borrow ONLY an actual observed channel assignment under this live owner.
    /// The Source slot is shared, not incremented or privately transferred.
    pub async fn adopt_source_assignment(
        &self,
        observed: &RedirectAssignment,
    ) -> Result<(), Error> {
        let Some(channel) = self
            .inner
            .keys
            .owner
            .strip_prefix("live:channel:")
            .and_then(|v| v.strip_suffix(":owner"))
        else {
            return Err(Error::InvalidRequest);
        };
        if !uuid::Uuid::parse_str(channel).is_ok_and(|id| id.to_string() == channel)
            || channel != observed.channel.as_str()
            || self.inner.release_started.load(Ordering::Acquire)
        {
            return Err(Error::InvalidRequest);
        }
        let assignment = LiveChannelAssignment::new(observed.numeric, observed.stream_id())?;
        match lease_mutation_observed(
            &self.inner,
            LeaseMutation::AdoptSourceAssignment {
                assignment,
                profile: observed.profile_id(),
            },
            true,
        )
        .await?
        {
            1 => Ok(()),
            0 => Err(Error::Lost),
            _ => Err(Error::AlreadyReserved),
        }
    }
    /// Publish, with no additional provider operation, only under an actual
    /// live UUID owner and the profile recorded by that lease's current reserve.
    pub async fn publish_source_assignment(
        &self,
        assignment: LiveChannelAssignment,
    ) -> Result<(), Error> {
        self.publish_assignment(assignment, None).await
    }
    pub async fn publish_source_assignment_with_metadata(
        &self,
        assignment: LiveChannelAssignment,
        ttl: Duration,
    ) -> Result<(), Error> {
        if ttl.is_zero() || u64::try_from(ttl.as_millis()).is_err() {
            return Err(Error::InvalidRequest);
        }
        self.publish_assignment(assignment, Some(ttl)).await
    }
    /// Change the active input without rewriting the initial Source alias or
    /// moving capacity. The same local live owner still holds that reservation.
    pub async fn switch_active_source_with_reservation(
        &self,
        stream: i64,
        profile: i64,
    ) -> Result<(), Error> {
        if stream <= 0 || profile <= 0 || self.inner.release_started.load(Ordering::Acquire) {
            return Err(Error::InvalidRequest);
        }
        match lease_mutation_observed(
            &self.inner,
            LeaseMutation::SwitchActiveSource { stream, profile },
            true,
        )
        .await?
        {
            1 => Ok(()),
            _ => {
                self.inner.lost.send_replace(true);
                Err(Error::Lost)
            }
        }
    }
    async fn publish_assignment(
        &self,
        assignment: LiveChannelAssignment,
        metadata_ttl: Option<Duration>,
    ) -> Result<(), Error> {
        let Some(channel) = self
            .inner
            .keys
            .owner
            .strip_prefix("live:channel:")
            .and_then(|v| v.strip_suffix(":owner"))
        else {
            return Err(Error::InvalidRequest);
        };
        if uuid::Uuid::parse_str(channel).is_err()
            || self.inner.release_started.load(Ordering::Acquire)
        {
            return Err(Error::InvalidRequest);
        }
        match lease_mutation_observed(
            &self.inner,
            LeaseMutation::SourceAssignment {
                assignment,
                metadata_ttl,
            },
            true,
        )
        .await?
        {
            1 => Ok(()),
            _ => {
                self.inner.lost.send_replace(true);
                Err(Error::Lost)
            }
        }
    }
}
