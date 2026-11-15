//! Actual configured Beat publication, retaining the shared Jobs physical lease.
//! Original Dispatcharr/Celery/Kombu/Redis authors retain all credit/licenses.
use super::*;
use crate::normal_beat::PreparedPublication;
impl Jobs {
    pub(crate) async fn publish_normal_beat(
        &self,
        publication: &PreparedPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let maximum = self.inner.config.max_queued_jobs.to_string();
        let binding = format!("_kombu.binding.{}", publication.exchange_key());
        let queue = publication.redis_queue();
        let script = r#"local q=redis.call('TYPE',KEYS[1]).ok;local b=redis.call('TYPE',KEYS[2]).ok
if(q~='none'and q~='list')or(b~='none'and b~='set')then return -3 end
if #ARGV[1]>65536 then return -1 end
if redis.call('LLEN',KEYS[1])>=tonumber(ARGV[2])then return -2 end
redis.call('SADD',KEYS[2],ARGV[3]);return redis.call('LPUSH',KEYS[1],ARGV[1])"#;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                script,
                &[queue.to_owned(), binding],
                &[
                    publication.bytes(),
                    maximum.as_bytes(),
                    publication.binding().as_bytes(),
                ],
                self.lease(owner)?,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: publication.id(),
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(publication.id())),
            Err(error) => Err(map(error)),
        }
    }
}
