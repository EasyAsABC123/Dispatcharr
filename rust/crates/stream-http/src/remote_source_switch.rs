//! Ordinary signed assigned switching to another Native raw producer.
//! Dispatcharr and dependency authors retain all original application credit.
use super::*;
use dispatcharr_output_store::{
    Error as StoreError, OutputStore, RemoteSwitchIntent, RemoteSwitchSource, RemoteSwitchVerdict,
    RemoteSwitchWake,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(100);
const OWNER_RESOLVE: Duration = Duration::from_secs(3);

pub(crate) enum AssignedSwitch {
    Local(String),
    Remote {
        url: String,
        verdict: Option<RemoteSwitchVerdict>,
    },
}

fn switch_error(_: StoreError) -> PlaybackError {
    PlaybackError::new(503, "Remote source switch unavailable")
}

fn url_digest(url: &str) -> String {
    format!("{:x}", Sha256::digest(url.as_bytes()))
}

fn unique_direct_url(ordered: &[OrderedChannelStream], stream: i64) -> Option<&str> {
    let mut matches = ordered.iter().filter(|assignment| assignment.id == stream);
    let url = matches.next()?.direct_url.as_deref()?;
    if matches.next().is_some()
        || !reqwest::Url::parse(url).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
    {
        return None;
    }
    Some(url)
}

async fn read_remote_target(
    resolver: &dyn PlaybackResolver,
    channel: &str,
    source: &RemoteSwitchSource,
    target: i64,
) -> Result<String, PlaybackError> {
    let rows = resolver
        .ordered_channel_streams(channel, Some((source.profile, 0)))
        .await?;
    let url = unique_direct_url(&rows, source.stream)
        .zip(unique_direct_url(&rows, target))
        .filter(|(current, next)| current != next)
        .map(|(_, next)| next)
        .ok_or_else(|| {
            PlaybackError::new(
                501,
                "Remote switch requires assigned direct streams on the same unlimited profile",
            )
        })?;
    Ok(url.to_owned())
}

impl StreamEngine {
    /// Main binds the pre-engine Store resolver, which cannot retain
    /// EngineControl and form a cycle. Standalone local switch paths omit it.
    pub fn with_remote_source_switches(
        mut self,
        resolver: Arc<dyn PlaybackResolver>,
    ) -> Result<Self, PlaybackError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            PlaybackError::new(
                500,
                "Remote source switches must be bound before engine sharing",
            )
        })?;
        inner.remote_switch_resolver = Some(resolver);
        Ok(self)
    }

    pub(crate) async fn switch_assigned_source(
        &self,
        channel: &str,
        target: i64,
        resolver: &dyn PlaybackResolver,
    ) -> Result<AssignedSwitch, PlaybackError> {
        if self.inner.remote_switch_resolver.is_none()
            || self
                .local_switch_session(channel)
                .is_some_and(|session| !session.following.load(Ordering::Acquire))
        {
            return self
                .switch_assigned_local_source(channel, target, resolver)
                .await
                .map(AssignedSwitch::Local);
        }
        let profiles = self
            .inner
            .profiles
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Remote source switch unavailable"))?;
        let channel_id = Uuid::parse_str(channel)
            .ok()
            .filter(|id| !id.is_nil() && id.to_string() == channel)
            .ok_or_else(|| PlaybackError::new(404, "Channel not found"))?;
        let store = profiles.switch_store();
        let source = store
            .observe_remote_switch(channel_id)
            .await
            .map_err(switch_error)?
            .ok_or_else(|| PlaybackError::new(404, "Channel not found"))?;
        let deadline = Instant::now() + WAIT;
        if source.stream == target {
            return Err(PlaybackError::new(
                501,
                "Remote switch requires a different assigned stream",
            ));
        }
        let url = time::timeout_at(
            deadline,
            read_remote_target(resolver, channel, &source, target),
        )
        .await
        .map_err(|_| PlaybackError::new(504, "Remote switch lookup timed out"))??;
        let wake = RemoteSwitchWake {
            channel: channel_id,
            request: Uuid::new_v4(),
        };
        let intent = RemoteSwitchIntent {
            source,
            target,
            url_sha256: url_digest(&url),
        };
        let status = time::timeout_at(deadline, store.begin_remote_switch(&wake, &intent)).await;
        if matches!(status, Ok(Ok(false))) {
            return Err(PlaybackError::new(
                409,
                "Remote source owner or request changed",
            ));
        }
        // Reconcile the same UUID after an ambiguous publication result.
        // This does not publish again, even if the requester lost the reply.
        let verdict = wait_for_receipt(store, &wake, &intent.source, deadline).await;
        let cleanup = async {
            if verdict.is_some() {
                store.acknowledge_remote_switch(&wake, &intent.source).await
            } else {
                store
                    .cancel_pending_remote_switch(&wake, &intent.source)
                    .await
            }
        };
        let _ = time::timeout(POLL, cleanup).await;
        Ok(AssignedSwitch::Remote { url, verdict })
    }
}

async fn wait_for_receipt(
    store: &OutputStore,
    wake: &RemoteSwitchWake,
    source: &RemoteSwitchSource,
    deadline: Instant,
) -> Option<RemoteSwitchVerdict> {
    time::timeout_at(deadline, async {
        loop {
            if let Ok(Some(confirmed)) = store.read_remote_switch(wake, source).await {
                return Some(confirmed);
            }
            time::sleep(POLL).await;
        }
    })
    .await
    .ok()
    .flatten()
}

#[derive(Default)]
pub(super) struct RemoteInbox {
    active: Option<Uuid>,
    queued: Option<RemoteSwitchWake>,
}

impl RemoteInbox {
    fn offer(&mut self, wake: RemoteSwitchWake) -> Option<RemoteSwitchWake> {
        if self.active.is_none() {
            self.active = Some(wake.request);
            return Some(wake);
        }
        if self.active != Some(wake.request) {
            self.queued = Some(wake);
        }
        None
    }

    fn next(&mut self) -> Option<RemoteSwitchWake> {
        let next = self.queued.take();
        self.active = next.as_ref().map(|wake| wake.request);
        next
    }
}

struct RemoteClaim {
    session: Arc<Session>,
    settled: bool,
}

impl RemoteClaim {
    fn next(&mut self) -> Option<RemoteSwitchWake> {
        let mut inbox = self.session.remote_switch_inbox.lock().unwrap();
        let next = inbox.next();
        self.settled = next.is_none();
        next
    }
}

impl Drop for RemoteClaim {
    fn drop(&mut self) {
        if !self.settled {
            *self.session.remote_switch_inbox.lock().unwrap() = RemoteInbox::default();
        }
    }
}

pub(super) fn dispatch(
    inner: Arc<Inner>,
    store: OutputStore,
    wake: RemoteSwitchWake,
    event_owner: Arc<dyn Send + Sync>,
) -> Result<(), StoreError> {
    let Some(resolver) = inner.remote_switch_resolver.as_ref().cloned() else {
        return Ok(());
    };
    let channel = wake.channel.to_string();
    let (session, fence, worker, mut claim, mut wake) = {
        let sessions = inner.sessions.lock().unwrap();
        let Some(session) = sessions
            .get(&channel)
            .filter(|session| {
                !inner.slots.is_closed()
                    && !session.following.load(Ordering::Acquire)
                    && session.manager_visible.load(Ordering::Acquire)
                    && !*session.stop.borrow()
                    && matches!(*session.startup.borrow(), Startup::Ready)
            })
            .cloned()
        else {
            return Ok(());
        };
        let Some(fence) = session.input_owner.lock().unwrap().clone() else {
            return Ok(());
        };
        let offered = session.remote_switch_inbox.lock().unwrap().offer(wake);
        let Some(wake) = offered else {
            return Ok(());
        };
        let claim = RemoteClaim {
            session: session.clone(),
            settled: false,
        };
        inner.workers.fetch_add(1, Ordering::AcqRel);
        (session, fence, WorkerCount(inner.clone()), claim, wake)
    };
    // Redis I/O and media both stay off the shared reader. The local gate
    // limits any event fanout or duplicate wakes to one task per raw session.
    tokio::spawn(async move {
        let _owners = (worker, event_owner);
        let engine = StreamEngine { inner };
        loop {
            if engine.inner.slots.is_closed() || *session.stop.borrow() {
                return;
            }
            settle_owned(
                &engine,
                &session,
                &store,
                &wake,
                &channel,
                &fence,
                resolver.as_ref(),
            )
            .await;
            let Some(next) = claim.next() else {
                break;
            };
            wake = next;
        }
    });
    Ok(())
}

async fn settle_owned(
    engine: &StreamEngine,
    session: &Arc<Session>,
    store: &OutputStore,
    wake: &RemoteSwitchWake,
    channel: &str,
    fence: &str,
    resolver: &dyn PlaybackResolver,
) {
    let claimant = Uuid::new_v4();
    let claimed = match store.claim_remote_switch(wake, fence, claimant).await {
        Ok(result) => Ok(result),
        Err(_) => store.claim_remote_switch(wake, fence, claimant).await,
    };
    let Ok(Some(intent)) = claimed else {
        return;
    };
    let result = submit_owned(engine, session, &intent, channel, fence, resolver).await;
    let switched = match result {
        Ok(pending) => pending.reply.await.is_ok_and(|result| result.is_ok()),
        Err(_) => false,
    };
    let verdict = if switched {
        RemoteSwitchVerdict::Switched
    } else {
        RemoteSwitchVerdict::Failed
    };
    if store
        .finish_remote_switch(wake, &intent.source, claimant, verdict)
        .await
        .is_err()
    {
        let _ = store
            .finish_remote_switch(wake, &intent.source, claimant, verdict)
            .await;
    }
}

async fn submit_owned(
    engine: &StreamEngine,
    session: &Arc<Session>,
    intent: &RemoteSwitchIntent,
    channel: &str,
    fence: &str,
    resolver: &dyn PlaybackResolver,
) -> Result<PendingSourceSwitch, PlaybackError> {
    let url = time::timeout(
        OWNER_RESOLVE,
        read_remote_target(resolver, channel, &intent.source, intent.target),
    )
    .await
    .map_err(|_| PlaybackError::new(504, "Owner source lookup timed out"))??;
    if url_digest(&url) != intent.url_sha256
        || intent.source.fence() != fence
        || !engine
            .local_switch_session(channel)
            .is_some_and(|active| Arc::ptr_eq(&active, session))
        || session.input_owner.lock().unwrap().as_deref() != Some(fence)
        || engine.inner.slots.is_closed()
        || *session.stop.borrow()
        || session.following.load(Ordering::Acquire)
        || !matches!(*session.startup.borrow(), Startup::Ready)
    {
        return Err(PlaybackError::new(
            409,
            "Remote source owner or target changed",
        ));
    }
    engine.submit_in_local_session(session, None, true, |active| {
        if (source_switch::source(active), active.provider_max_streams)
            != ((intent.source.stream, Some(intent.source.profile)), 0)
        {
            return Err(PlaybackError::new(
                409,
                "Remote source owner or target changed",
            ));
        }
        Ok((intent.target, Some(&url)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_inbox_keeps_a_new_valid_wake_through_prior_receipt_retirement() {
        let channel = Uuid::from_u128(5);
        let first = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(10),
        };
        let next = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(11),
        };
        let latest = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(12),
        };
        let mut inbox = RemoteInbox::default();
        assert_eq!(inbox.offer(first.clone()), Some(first.clone()));
        assert!(inbox.offer(next).is_none());
        assert!(inbox.offer(latest.clone()).is_none());
        assert!(inbox.offer(first).is_none());
        assert_eq!(inbox.next(), Some(latest.clone()));
        assert!(inbox.offer(latest).is_none());
        assert_eq!(inbox.next(), None);
    }
}
