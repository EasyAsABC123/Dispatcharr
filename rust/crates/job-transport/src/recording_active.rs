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
