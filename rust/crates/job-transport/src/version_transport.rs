//! Opaque normal check_for_version_update port. No new generic Jobs enum authority.
use super::*;
#[derive(Clone)]
pub struct VersionJobs {
    jobs: Jobs,
}
impl VersionJobs {
    pub async fn connect(broker: &str, result: &str, config: Config) -> Result<Self, Error> {
        Ok(Self {
            jobs: Jobs::connect_series(broker, result, config).await?,
        })
    }

    pub(crate) fn from_defaults(jobs: Jobs) -> Result<Self, Error> {
        if !jobs.allows_defaults() {
            return Err(Error::Configuration);
        }
        Ok(Self { jobs })
    }

    pub(crate) async fn claim(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::version_protocol::Message>, Error> {
        self.claim_selected(None, caller).await
    }
    pub(crate) async fn claim_selected(
        &self,
        expected: Option<&[u8]>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::version_protocol::Message>, Error> {
        let keys = vec!["celery".to_owned()];
        let lease = self.jobs.lease(caller.clone())?;
        let (reply, held) = self
            .jobs
            .inner
            .broker
            .command_read_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        if let Some(expected) = expected {
            if expected.is_empty() || reply.0 != 1 || reply.1 != expected {
                return Err(Error::ClaimRace);
            }
        }
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        let message =
            crate::version_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
        drop(held);
        match self
            .jobs
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &keys,
                &[reply.1.as_slice()],
                self.jobs.lease(caller)?,
            )
            .await
        {
            Ok((1, _)) => Ok(Some(message)),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(message.task_id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn publish_startup(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let id = crate::version_protocol::parse(raw)
            .map_err(|_| Error::UnsupportedTask)?
            .task_id;
        super::normal_recovery_transport::publish_startup_root(&self.jobs, raw, id, owner).await
    }

    pub(crate) async fn write(
        &self,
        message: &crate::version_protocol::Message,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.jobs
            .write_result(
                message.task_id,
                serde_json::Value::Null,
                None,
                Vec::new(),
                caller,
            )
            .await
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.jobs.shutdown(timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires an isolated Redis service with a two-second idle timeout"]
    async fn version_claim_checks_exact_head_after_normal_idle_expiry() {
        assert_eq!(
            std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let owned_url = std::env::var("REDIS_TEST_URL").unwrap();
        let owned = redis::Client::open(owned_url.as_str()).unwrap();
        let identity = owned.get_connection_info();
        assert_eq!(identity.redis.db, 0);
        assert!(matches!(
            identity.addr,
            redis::ConnectionAddr::Tcp(ref host, _) if host == "127.0.0.1"
        ));
        assert_eq!(
            identity.redis.password.as_deref(),
            Some("playback-fixture-only")
        );

        let prefix = owned_url.strip_suffix("/0").unwrap();
        let broker_url = format!("{prefix}/1");
        let result_url = format!("{prefix}/2");
        let broker = redis::Client::open(broker_url.as_str()).unwrap();
        let mut observer = broker.get_multiplexed_async_connection().await.unwrap();
        let config: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("timeout")
            .query_async(&mut observer)
            .await
            .unwrap();
        assert_eq!(config, ["timeout", "2"]);
        assert_eq!(
            redis::cmd("DBSIZE")
                .query_async::<i64>(&mut observer)
                .await
                .unwrap(),
            0
        );

        let source: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/source-default-stats-wire.json"))
                .unwrap();
        let entry = source["defaults"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["task"] == crate::version_protocol::TASK)
            .unwrap();
        let raw = serde_json::to_vec(&entry["envelope"]).unwrap();
        let expected = crate::version_protocol::parse(&raw).unwrap();
        assert_eq!(
            redis::cmd("LPUSH")
                .arg("celery")
                .arg(raw.as_slice())
                .query_async::<i64>(&mut observer)
                .await
                .unwrap(),
            1
        );
        drop(observer);
        let old = VersionJobs::connect(&broker_url, &result_url, Config::default())
            .await
            .unwrap();
        let guarded = VersionJobs::connect(&broker_url, &result_url, Config::default())
            .await
            .unwrap();

        // Idle expiry comes from the owned server. The two managers remain
        // independent. The guarded production path claims the exact head once.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let keys = ["celery".to_owned()];
        let old_read = old
            .jobs
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &keys,
                &[],
                old.jobs.lease(Arc::new(())).unwrap(),
            )
            .await;
        assert!(matches!(old_read, Err(ClassifiedError::Backend)));
        let claimed = guarded
            .claim_selected(Some(&raw), Arc::new(()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.task_id, expected.task_id);

        let mut observer = broker.get_multiplexed_async_connection().await.unwrap();
        assert_eq!(
            redis::cmd("LLEN")
                .arg("celery")
                .query_async::<i64>(&mut observer)
                .await
                .unwrap(),
            0
        );
    }
}
