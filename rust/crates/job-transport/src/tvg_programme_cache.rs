//! Normal per-channel Source cache operations with explicit physical receipts.
//! Original Dispatcharr authors retain key/algorithm credit. No task intake.
use dispatcharr_output_store::transport::{ClassifiedError, RedisTransport, TransportConfig};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;
#[derive(Debug)]
pub enum Error {
    Transport(ClassifiedError),
    Unsupported,
    PhysicalReceipt,
}
#[derive(Clone)]
pub struct Cache {
    transport: RedisTransport,
}
struct Receipt {
    owner: Option<Arc<dyn Send + Sync>>,
    done: Option<oneshot::Sender<()>>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        drop(self.owner.take());
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
pub const HELD:&str="local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return 0 end;if t~='string'then return -1 end;local n=redis.call('STRLEN',KEYS[1]);if n>4096 then return -1 end;return n>0 and 1 or 0";
pub const INCR: &str = "return redis.call('INCR',KEYS[1])";
pub const DECR: &str = "return redis.call('DECR',KEYS[1])";
pub const DELETE: &str = "return redis.call('DEL',KEYS[1])";
impl Cache {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let transport = RedisTransport::connect_classified(
            url,
            TransportConfig {
                max_operations: 2,
                max_queued_operations: 2,
                command_timeout: Duration::from_secs(2),
            },
        )
        .await
        .map_err(Error::Transport)?;
        Ok(Self { transport })
    }
    async fn call(
        &self,
        script: &str,
        key: String,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<i64, Error> {
        let (tx, rx) = oneshot::channel();
        let guard = Receipt {
            owner: Some(owner),
            done: Some(tx),
        };
        let result = self
            .transport
            .command_classified::<i64, _>(script, &[key], &[], guard)
            .await;
        let result = match result {
            Ok((value, guard)) => {
                drop(guard);
                Ok(value)
            }
            Err(error) => Err(Error::Transport(error)),
        };
        // Logical timeout does not let this family publish terminal metadata while
        // the actual operation still owns its original caller/command receipt.
        rx.await.map_err(|_| Error::PhysicalReceipt)?;
        result
    }
    pub async fn source_refresh_held(
        &self,
        source: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        match self
            .call(HELD, format!("task_lock_refresh_epg_data_{source}"), owner)
            .await?
        {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Unsupported),
        }
    }
    pub async fn enter(&self, source: i64, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        self.call(INCR, format!("epg_source_tvg_parse_{source}"), owner)
            .await
            .map(|_| ())
    }
    pub async fn leave(&self, source: i64, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let key = format!("epg_source_tvg_parse_{source}");
        let value = self.call(DECR, key.clone(), owner.clone()).await?;
        // Keep Source's TWO separate commands; no invented atomic DECR+DEL race policy.
        if value <= 0 {
            self.call(DELETE, key, owner).await?;
        };
        Ok(())
    }
    /// Source logs and catches this helper's errors. Expose the literal failure
    /// to the caller for raw diagnostics, after its real physical owner settles.
    pub async fn invalidate_output(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        self.call(INCR, "epg_content_generation".into(), owner)
            .await
            .map(|_| ())
    }
}
