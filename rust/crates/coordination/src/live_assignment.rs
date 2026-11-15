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
    /// Publish, with no additional provider operation, only under an actual
    /// live UUID owner and the profile recorded by that lease's current reserve.
    pub async fn publish_source_assignment(
        &self,
        assignment: LiveChannelAssignment,
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
            LeaseMutation::SourceAssignment(assignment),
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
