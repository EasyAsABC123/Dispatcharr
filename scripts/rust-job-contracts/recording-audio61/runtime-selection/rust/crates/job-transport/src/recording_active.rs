//! PRIVATE UNCOMPILED typed DVR active-key primitives, not TaskLease.
//! Original Source uses literal1/NX/EX45, with refresh15. These observations do
//! NOT establish identity against a foreign writer replacing1 with1. No Drop
//! delete, automatic renewal task, acquisition retry or terminal authority.
use chrono::{DateTime, Utc};
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig},
    Script,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const OBSERVE:&str="local t=redis.call('TYPE',KEYS[1]).ok;if t=='none' then return {0,-2} end;if t~='string' or redis.call('STRLEN',KEYS[1])~=1 then return {-1,-1} end;if redis.call('GET',KEYS[1])~='1' then return {-1,-1} end;return {1,redis.call('PTTL',KEYS[1])}";
const REFRESH:&str="if redis.call('GET',KEYS[1])=='1' then return redis.call('EXPIRE',KEYS[1],45) else return 0 end";
const REMOVE: &str =
    "if redis.call('GET',KEYS[1])=='1' then return redis.call('DEL',KEYS[1]) else return 0 end";
#[derive(Debug)]
pub enum Error {
    Unsupported,
    Capacity,
    Backend(redis::RedisError),
    Supervisor,
    Uncertain,
}
#[derive(Debug)]
pub(crate) struct Observation<T> {
    recording: i64,
    origin: Arc<()>,
    pub value: T,
    pub before_utc: DateTime<Utc>,
    pub after_utc: DateTime<Utc>,
    /// Conservative earliest issuance; reply arrival does not reset TTL age.
    pub issued_monotonic: Instant,
    pub physically_completed: Instant,
}
/// Move-only actual literal-value confirmation for explicit captured61 only.
/// No foreign same-value replacement/unique-owner proof is granted.
pub(crate) struct LiteralConfirmation {
    recording: i64,
    origin: Arc<()>,
    issued_monotonic: Instant,
    physically_completed: Instant,
}
pub(crate) struct ConfirmedSet {
    pub observation: Observation<bool>,
    pub confirmation: Option<LiteralConfirmation>,
}
pub(crate) struct ConfirmedRefresh {
    pub observation: Observation<i64>,
    pub confirmation: LiteralConfirmation,
}
fn valid_literal(
    proof: &LiteralConfirmation,
    recording: i64,
    origin: &Arc<()>,
    now: Instant,
) -> Result<(), Error> {
    if proof.recording != recording
        || !Arc::ptr_eq(origin, &proof.origin)
        || proof.physically_completed < proof.issued_monotonic
        || now < proof.physically_completed
        || now
            .checked_duration_since(proof.issued_monotonic)
            .is_none_or(|age| age >= Duration::from_secs(45))
    {
        return Err(Error::Uncertain);
    }
    Ok(())
}
fn initial_literal(
    recording: i64,
    origin: &Arc<()>,
    actual: &Observation<bool>,
    now: Instant,
) -> Result<Option<LiteralConfirmation>, Error> {
    if !actual.value {
        return Ok(None);
    }
    let proof = LiteralConfirmation {
        recording: actual.recording,
        origin: actual.origin.clone(),
        issued_monotonic: actual.issued_monotonic,
        physically_completed: actual.physically_completed,
    };
    valid_literal(&proof, recording, origin, now)?;
    Ok(Some(proof))
}
fn advance_literal(
    prior: LiteralConfirmation,
    recording: i64,
    origin: &Arc<()>,
    actual: &Observation<i64>,
    now: Instant,
) -> Result<LiteralConfirmation, Error> {
    // The prior must remain valid through the actual reply; a late 1 is not
    // authority to reset its age or revive an expired/uncertain operation.
    valid_literal(&prior, recording, origin, now)?;
    if actual.recording != recording
        || !Arc::ptr_eq(origin, &actual.origin)
        || actual.value != 1
        || actual.issued_monotonic < prior.physically_completed
        || actual.physically_completed < actual.issued_monotonic
        || actual.physically_completed > now
    {
        return Err(Error::Uncertain);
    }
    let next = LiteralConfirmation {
        recording: actual.recording,
        origin: actual.origin.clone(),
        // Earliest issuance of the ACTUAL confirmed EXPIRE, never reply time.
        issued_monotonic: actual.issued_monotonic,
        physically_completed: actual.physically_completed,
    };
    valid_literal(&next, recording, origin, now)?;
    Ok(next)
}
#[derive(Debug)]
pub(crate) struct AuxiliaryObservation {
    pub absent: bool,
    pub before_utc: DateTime<Utc>,
    pub after_utc: DateTime<Utc>,
}
impl<T> Observation<T> {
    pub(crate) fn receipt(&self, value: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"recording_id":self.recording,"value":value,
            "before_utc":self.before_utc,"after_utc":self.after_utc,
            "actual_command_elapsed_us":self.physically_completed.duration_since(self.issued_monotonic).as_micros()})
    }
}
impl AuxiliaryObservation {
    pub(crate) fn receipt(&self) -> serde_json::Value {
        serde_json::json!({"absent":self.absent,"before_utc":self.before_utc,"after_utc":self.after_utc})
    }
}
#[derive(Clone)]
pub(crate) struct Operations {
    connection: ConnectionManager,
    commands: Arc<Semaphore>,
    identity: Arc<()>,
}
impl Operations {
    pub(crate) async fn connect(url: String, owner: Arc<dyn Send + Sync>) -> Result<Self, Error> {
        tokio::spawn(async move {
            let _owner = owner;
            let client = redis::Client::open(url).map_err(Error::Backend)?;
            let connection = client
                .get_connection_manager_with_config(
                    ConnectionManagerConfig::new()
                        .set_number_of_retries(0)
                        .set_connection_timeout(Duration::from_secs(2)),
                )
                .await
                .map_err(Error::Backend)?;
            Ok(Self {
                connection,
                commands: Arc::new(Semaphore::new(1)),
                identity: Arc::new(()),
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    fn key(recording: i64) -> Result<String, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&recording) {
            return Err(Error::Unsupported);
        }
        Ok(format!("dvr:recording-active:{recording}"))
    }
    /// Exactly one SET attempt. An error/unknown response is NOT a confirmed miss
    /// and must never trigger another SET, guessed DEL or normal completion.
    pub(crate) async fn set_once(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Observation<bool>, Error> {
        let key = Self::key(recording)?;
        let permit = self
            .commands
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let mut connection = self.connection.clone();
        let origin = self.identity.clone();
        tokio::spawn(async move {
            let (_owner, _permit) = (owner, permit);
            let before_utc = Utc::now();
            let issued_monotonic = Instant::now();
            let reply: Option<String> = redis::cmd("SET")
                .arg(key)
                .arg("1")
                .arg("EX")
                .arg(45)
                .arg("NX")
                .query_async(&mut connection)
                .await
                .map_err(Error::Backend)?;
            let after_utc = Utc::now();
            let value = match reply.as_deref() {
                Some("OK") => true,
                None => false,
                _ => return Err(Error::Uncertain),
            };
            Ok(Observation {
                recording,
                origin,
                value,
                before_utc,
                after_utc,
                issued_monotonic,
                physically_completed: Instant::now(),
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    /// Exactly the existing one SET, then create an unforgeable move-only local
    /// confirmation INSIDE the operation. NX false never produces authority.
    pub(crate) async fn set_once_captured61(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<ConfirmedSet, Error> {
        let observation = self.set_once(recording, owner).await?;
        let confirmation =
            initial_literal(recording, &self.identity, &observation, Instant::now())?;
        Ok(ConfirmedSet {
            observation,
            confirmation,
        })
    }
    /// Explicit61 only. Consume the prior; only one same-origin actual integer1
    /// reply within prior45 can derive the next earliest-issued confirmation.
    pub(crate) async fn refresh_captured61(
        &self,
        recording: i64,
        prior: LiteralConfirmation,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<ConfirmedRefresh, Error> {
        valid_literal(&prior, recording, &self.identity, Instant::now())?;
        let observation = self.script(recording, REFRESH, owner).await?;
        let confirmation = advance_literal(
            prior,
            recording,
            &self.identity,
            &observation,
            Instant::now(),
        )?;
        Ok(ConfirmedRefresh {
            observation,
            confirmation,
        })
    }
    /// Latest actual confirmation, no Drop/delete retry and no TTL increase.
    pub(crate) async fn remove_captured61(
        &self,
        recording: i64,
        prior: LiteralConfirmation,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Observation<i64>, Error> {
        valid_literal(&prior, recording, &self.identity, Instant::now())?;
        let observation = self.script(recording, REMOVE, owner).await?;
        valid_literal(&prior, recording, &self.identity, Instant::now())?;
        if observation.value != 1 {
            return Err(Error::Uncertain);
        }
        Ok(observation)
    }
    /// Atomic bounded TYPE/STRLEN/value/TTL observation. No bulk data beyond the
    /// literal one-byte value is transferred and wrong types aren't projected
    /// as an absent key. This is NOT unique-owner evidence.
    pub(crate) async fn observe(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Observation<(i64, i64)>, Error> {
        self.script(recording, OBSERVE, owner).await
    }
    /// Initial Source3 has no HLS viewers and no live-proxy stream metadata.
    /// Typed two-key observation cannot be supplied as an active-owner receipt.
    /// Key authority: pinned Original tasks.py + live_proxy/redis_keys.py1241.
    pub(crate) async fn auxiliary_absent(
        &self,
        recording: i64,
        channel: uuid::Uuid,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<AuxiliaryObservation, Error> {
        Self::key(recording)?;
        let viewer = format!("dvr:hls_viewer:{recording}");
        let metadata = format!("live:channel:{channel}:metadata");
        let permit = self
            .commands
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let mut connection = self.connection.clone();
        tokio::spawn(async move {
            let (_owner, _permit) = (owner, permit);
            let before_utc = Utc::now();
            let result: i64 = Script::new(
                "if redis.call('EXISTS',KEYS[1],KEYS[2])==0 then return 1 else return 0 end",
            )
            .key(viewer)
            .key(metadata)
            .invoke_async(&mut connection)
            .await
            .map_err(Error::Backend)?;
            let absent = match result {
                0 => false,
                1 => true,
                _ => return Err(Error::Uncertain),
            };
            Ok(AuxiliaryObservation {
                absent,
                before_utc,
                after_utc: Utc::now(),
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    async fn script<T: redis::FromRedisValue + Send + 'static>(
        &self,
        recording: i64,
        script: &'static str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Observation<T>, Error> {
        let key = Self::key(recording)?;
        let permit = self
            .commands
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let mut connection = self.connection.clone();
        let origin = self.identity.clone();
        tokio::spawn(async move {
            let (_owner, _permit) = (owner, permit);
            let before_utc = Utc::now();
            let issued_monotonic = Instant::now();
            let value = Script::new(script)
                .key(key)
                .invoke_async(&mut connection)
                .await
                .map_err(Error::Backend)?;
            Ok(Observation {
                recording,
                origin,
                value,
                before_utc,
                after_utc: Utc::now(),
                issued_monotonic,
                physically_completed: Instant::now(),
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    /// Caller must still hold its live local operation and a prior confirmation
    /// withinTTL. Same-value foreign replacement and TTL lapse remain OPEN;
    /// no API here turns name/value equality into a unique lease token.
    pub(crate) async fn refresh_confirmed(
        &self,
        recording: i64,
        last: &Observation<bool>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Observation<i64>, Error> {
        if !Arc::ptr_eq(&self.identity, &last.origin)
            || last.recording != recording
            || !last.value
            || last.issued_monotonic.elapsed() >= Duration::from_secs(45)
        {
            return Err(Error::Uncertain);
        }
        let result = self.script(recording, REFRESH, owner).await?;
        if last.issued_monotonic.elapsed() >= Duration::from_secs(45) || result.value != 1 {
            return Err(Error::Uncertain);
        }
        Ok(result)
    }
    /// Normal explicit cleanup primitive only; not called from Drop. The service
    /// must refuse uncertainty and unsupported foreign-writer contexts first.
    pub(crate) async fn remove_confirmed(
        &self,
        recording: i64,
        last: &Observation<bool>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Observation<i64>, Error> {
        if !Arc::ptr_eq(&self.identity, &last.origin)
            || last.recording != recording
            || !last.value
            || last.issued_monotonic.elapsed() >= Duration::from_secs(45)
        {
            return Err(Error::Uncertain);
        }
        let result = self.script(recording, REMOVE, owner).await?;
        if result.value != 1 {
            return Err(Error::Uncertain);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_cannot_be_tasklease_or_arbitrary_key() {
        assert_eq!(
            Operations::key(1).expect("Source id"),
            "dvr:recording-active:1"
        );
        for id in [0, -1, i64::from(i32::MAX) + 1, i64::MAX] {
            assert!(Operations::key(id).is_err());
        }
    }
}

#[cfg(test)]
mod captured61_literal_confirmation_controls {
    use super::*;
    fn receipt<T>(
        origin: &Arc<()>,
        recording: i64,
        value: T,
        issue: Instant,
        complete: Instant,
    ) -> Observation<T> {
        Observation {
            recording,
            origin: origin.clone(),
            value,
            before_utc: Utc::now(),
            after_utc: Utc::now(),
            issued_monotonic: issue,
            physically_completed: complete,
        }
    }
    fn seed(origin: &Arc<()>, base: Instant) -> LiteralConfirmation {
        initial_literal(
            1,
            origin,
            &receipt(origin, 1, true, base, base + Duration::from_millis(1)),
            base + Duration::from_millis(2),
        )
        .expect("actual-shape valid initial1")
        .expect("true produces private confirmation")
    }
    #[test]
    fn nx_false_never_produces_confirmation() {
        let origin = Arc::new(());
        let base = Instant::now();
        assert!(
            initial_literal(1, &origin, &receipt(&origin, 1, false, base, base), base)
                .expect("confirmed NX miss")
                .is_none()
        );
    }
    #[test]
    fn initial_wrong_origin_or_record_cannot_create_confirmation() {
        let origin = Arc::new(());
        let foreign = Arc::new(());
        let base = Instant::now();
        assert!(
            initial_literal(1, &origin, &receipt(&foreign, 1, true, base, base), base).is_err()
        );
        assert!(initial_literal(1, &origin, &receipt(&origin, 2, true, base, base), base).is_err());
    }
    #[test]
    fn initial_late_reply_or_reversed_clock_is_uncertain() {
        let origin = Arc::new(());
        let base = Instant::now();
        assert!(initial_literal(
            1,
            &origin,
            &receipt(&origin, 1, true, base, base),
            base + Duration::from_secs(45)
        )
        .is_err());
        assert!(initial_literal(
            1,
            &origin,
            &receipt(&origin, 1, true, base + Duration::from_millis(1), base),
            base + Duration::from_millis(2)
        )
        .is_err());
        assert!(initial_literal(
            1,
            &origin,
            &receipt(&origin, 1, true, base, base + Duration::from_millis(2)),
            base + Duration::from_millis(1)
        )
        .is_err());
    }
    #[test]
    fn exact15_30_45_60_chain_advances_from_actual_issue_not_reply() {
        let origin = Arc::new(());
        let base = Instant::now();
        let mut prior = seed(&origin, base);
        for seconds in [15, 30, 45, 60] {
            let issued = base + Duration::from_secs(seconds);
            let complete = issued + Duration::from_millis(10);
            prior = advance_literal(
                prior,
                1,
                &origin,
                &receipt(&origin, 1, 1, issued, complete),
                complete + Duration::from_millis(1),
            )
            .expect("timely actual1 chain");
            assert_eq!(prior.issued_monotonic, issued);
            assert_ne!(prior.issued_monotonic, complete);
        }
        assert!(valid_literal(&prior, 1, &origin, base + Duration::from_millis(104_999)).is_ok());
        assert!(valid_literal(&prior, 1, &origin, base + Duration::from_secs(105)).is_err());
    }
    #[test]
    fn zero_or_negative_refresh_never_advances() {
        let origin = Arc::new(());
        let base = Instant::now();
        let at = base + Duration::from_secs(15);
        for value in [0, -1] {
            assert!(advance_literal(
                seed(&origin, base),
                1,
                &origin,
                &receipt(&origin, 1, value, at, at),
                at
            )
            .is_err());
        }
    }
    #[test]
    fn foreign_refresh_origin_or_record_never_advances() {
        let origin = Arc::new(());
        let foreign = Arc::new(());
        let base = Instant::now();
        let at = base + Duration::from_secs(15);
        assert!(advance_literal(
            seed(&origin, base),
            1,
            &origin,
            &receipt(&foreign, 1, 1, at, at),
            at
        )
        .is_err());
        assert!(advance_literal(
            seed(&origin, base),
            1,
            &origin,
            &receipt(&origin, 2, 1, at, at),
            at
        )
        .is_err());
        assert!(advance_literal(
            seed(&foreign, base),
            1,
            &origin,
            &receipt(&origin, 1, 1, at, at),
            at
        )
        .is_err());
    }
    #[test]
    fn prior_expired_before_command_cannot_be_revived_by_integer1() {
        let origin = Arc::new(());
        let base = Instant::now();
        let at = base + Duration::from_secs(46);
        assert!(valid_literal(&seed(&origin, base), 1, &origin, at).is_err());
        assert!(advance_literal(
            seed(&origin, base),
            1,
            &origin,
            &receipt(&origin, 1, 1, at, at),
            at
        )
        .is_err());
    }
    #[test]
    fn late_success_after_prior45_is_not_reset_authority() {
        let origin = Arc::new(());
        let base = Instant::now();
        let issued = base + Duration::from_secs(44);
        let complete = base + Duration::from_millis(45_001);
        assert!(advance_literal(
            seed(&origin, base),
            1,
            &origin,
            &receipt(&origin, 1, 1, issued, complete),
            complete
        )
        .is_err());
    }
    #[test]
    fn refresh_bad_clock_or_overlap_before_prior_completion_is_uncertain() {
        let origin = Arc::new(());
        let base = Instant::now();
        let at = base + Duration::from_secs(15);
        assert!(advance_literal(
            seed(&origin, base),
            1,
            &origin,
            &receipt(&origin, 1, 1, base, at),
            at
        )
        .is_err());
        assert!(advance_literal(
            seed(&origin, base),
            1,
            &origin,
            &receipt(&origin, 1, 1, at + Duration::from_millis(1), at),
            at + Duration::from_millis(2)
        )
        .is_err());
        assert!(advance_literal(
            seed(&origin, base),
            1,
            &origin,
            &receipt(&origin, 1, 1, at, at + Duration::from_millis(2)),
            at + Duration::from_millis(1)
        )
        .is_err());
    }
    #[test]
    fn latest_removal_confirmation_still_has_exact45_cap_and_origin() {
        let origin = Arc::new(());
        let foreign = Arc::new(());
        let base = Instant::now();
        let at = base + Duration::from_secs(30);
        let last = advance_literal(
            seed(&origin, base),
            1,
            &origin,
            &receipt(&origin, 1, 1, at, at),
            at,
        )
        .expect("actual1 before prior45");
        assert!(valid_literal(&last, 1, &origin, base + Duration::from_millis(74_999)).is_ok());
        assert!(valid_literal(&last, 1, &origin, base + Duration::from_secs(75)).is_err());
        assert!(valid_literal(&last, 2, &origin, at).is_err());
        assert!(valid_literal(&last, 1, &foreign, at).is_err());
    }
}
