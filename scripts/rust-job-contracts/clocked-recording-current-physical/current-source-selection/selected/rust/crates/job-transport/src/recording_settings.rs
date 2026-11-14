//! PRIVATE UNCOMPILED normal-DVR settings composition. Uses the already qualified
//! configured-recording cache/default/version decisions and SAME-connection SQL
//! ErrorResponse→Ready→close path; does not change any legacy getter.
use super::recording_normal::EffectiveGroups;
use dispatcharr_ingest_epg::{
    cache_policy::{CachedSwapPolicy, ConfiguredSettingsError, DvrSettingsGroup},
    SwapPolicyContext,
};
use dispatcharr_store::Store;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum Error {
    Policy(ConfiguredSettingsError),
    Unsettled,
    Unsupported,
}
struct Receipt {
    done: Option<oneshot::Sender<()>>,
    _original_owner: Arc<dyn Send + Sync>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
pub(crate) struct Settings {
    policy: Arc<CachedSwapPolicy>,
}
impl Settings {
    /// The same injected Store must construct the recording SQL adapter. No
    /// global pool, replacement policy cache or independent caller is created.
    pub(crate) fn new(store: Store, cache: String) -> Self {
        Self {
            policy: Arc::new(CachedSwapPolicy::new(store, Some(cache))),
        }
    }
    async fn group(
        &self,
        group: DvrSettingsGroup,
        readonly: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<serde_json::Value, Error> {
        let (tx, rx) = oneshot::channel();
        let receipt = Arc::new(Receipt {
            done: Some(tx),
            _original_owner: owner,
        });
        let context = SwapPolicyContext {
            // Dropping the observer is not a worker cancellation request.
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: receipt.clone(),
        };
        let observed = if readonly {
            self.policy
                .retained_dvr_preflight_settings(group, context)
                .await
        } else {
            match group {
                DvrSettingsGroup::System => {
                    self.policy
                        .retained_configured_system_settings(context)
                        .await
                }
                DvrSettingsGroup::Dvr => {
                    self.policy.retained_configured_dvr_settings(context).await
                }
                DvrSettingsGroup::Stream => {
                    self.policy
                        .retained_normal_dvr_stream_settings(context)
                        .await
                }
            }
        };
        drop(receipt);
        // A logical result/error alone cannot release the original owner while
        // actual Redis/SQL work still owns a transferred receipt.
        rx.await.map_err(|_| Error::Unsettled)?;
        let value = observed.map_err(Error::Policy)?;
        // This is an additional domain cap AFTER the reused loader's existing
        // wire/structure bounds. It is not a claim of an8192-byte Redis transfer.
        if !value.is_object()
            || serde_json::to_vec(&value)
                .map_err(|_| Error::Unsupported)?
                .len()
                > 8192
        {
            return Err(Error::Unsupported);
        }
        Ok(value)
    }
    pub(crate) async fn admission(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<EffectiveGroups, Error> {
        let dvr = self
            .group(DvrSettingsGroup::Dvr, true, owner.clone())
            .await?;
        let system = self
            .group(DvrSettingsGroup::System, true, owner.clone())
            .await?;
        let stream = self.group(DvrSettingsGroup::Stream, true, owner).await?;
        // Group reads are sequential actual observations. Stable configuration
        // is the Source3 domain; no atomic cross-group/concurrent-update parity.
        Ok(EffectiveGroups {
            dvr,
            system,
            stream,
        })
    }
    /// At Source's post-admission getter boundary; may fill the actual cache.
    /// Re-read value must match the admitted stable group or the task remains
    /// operational. Do not call these methods from unsupported-head preflight.
    pub(crate) async fn normal_effect(
        &self,
        system: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<serde_json::Value, Error> {
        self.group(
            if system {
                DvrSettingsGroup::System
            } else {
                DvrSettingsGroup::Dvr
            },
            false,
            owner,
        )
        .await
    }
    pub(crate) async fn normal_stream_effect(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<serde_json::Value, Error> {
        self.group(DvrSettingsGroup::Stream, false, owner).await
    }
}
