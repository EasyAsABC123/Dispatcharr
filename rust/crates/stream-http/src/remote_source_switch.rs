//! Ordinary signed assigned switching to another Native raw producer.
//! Dispatcharr and dependency authors retain all original application credit.
use super::*;
use dispatcharr_output_store::{
    Error as StoreError, OutputStore, RemoteSwitchIntent, RemoteSwitchKind, RemoteSwitchSource,
    RemoteSwitchVerdict, RemoteSwitchWake,
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

async fn remote_order(
    resolver: &dyn PlaybackResolver,
    channel: &str,
    source: &RemoteSwitchSource,
    capacity: u32,
) -> Result<Vec<OrderedChannelStream>, PlaybackError> {
    resolver
        .ordered_channel_streams(channel, Some((source.profile, capacity)))
        .await
}

fn remote_capacity(kind: RemoteSwitchKind) -> u32 {
    u32::from(kind == RemoteSwitchKind::AssignedOneSlot)
}

fn remote_target_url(
    rows: &[OrderedChannelStream],
    current: i64,
    target: i64,
    kind: RemoteSwitchKind,
) -> Result<&str, PlaybackError> {
    let url = if current == target {
        // A remote assigned reselect may refresh the already active input.
        // The ready owner independently verifies this saved URL against its
        // actual current direct candidate before acknowledging without a dial.
        (kind == RemoteSwitchKind::Assigned)
            .then(|| unique_direct_url(rows, current))
            .flatten()
    } else {
        unique_direct_url(rows, current)
            .zip(unique_direct_url(rows, target))
            .filter(|(current, next)| current != next)
            .map(|(_, next)| next)
    };
    url.ok_or_else(|| {
        PlaybackError::new(
            501,
            if kind == RemoteSwitchKind::AssignedOneSlot {
                "Remote switch requires assigned direct streams on the same capacity-one profile"
            } else {
                "Remote switch requires assigned direct streams on the same unlimited profile"
            },
        )
    })
}

fn owner_target_url(
    rows: &[OrderedChannelStream],
    current: i64,
    target: i64,
    kind: RemoteSwitchKind,
) -> Result<&str, PlaybackError> {
    if kind == RemoteSwitchKind::Next {
        verify_transfer_order(rows, current, target)?;
    }
    remote_target_url(rows, current, target, kind)
}

struct RemoteContext<'a> {
    store: &'a OutputStore,
    channel: Uuid,
    source: RemoteSwitchSource,
    deadline: Instant,
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

    fn is_remote_switch_requester(&self, channel: &str) -> bool {
        self.inner.remote_switch_resolver.is_some()
            && self
                .local_switch_session(channel)
                .is_none_or(|session| session.following.load(Ordering::Acquire))
    }

    async fn observe_remote_switch(
        &self,
        channel: &str,
    ) -> Result<Option<RemoteContext<'_>>, PlaybackError> {
        let profiles = self
            .inner
            .profiles
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Remote source switch unavailable"))?;
        let Some(channel_id) = Uuid::parse_str(channel)
            .ok()
            .filter(|id| !id.is_nil() && id.to_string() == channel)
        else {
            return Ok(None);
        };
        let store = profiles.switch_store();
        Ok(store
            .observe_remote_switch(channel_id)
            .await
            .map_err(switch_error)?
            .map(|source| RemoteContext {
                store,
                channel: channel_id,
                source,
                deadline: Instant::now() + WAIT,
            }))
    }

    pub(crate) async fn switch_assigned_source(
        &self,
        channel: &str,
        target: i64,
        resolver: &dyn PlaybackResolver,
    ) -> Result<AssignedSwitch, PlaybackError> {
        if !self.is_remote_switch_requester(channel) {
            return self
                .switch_assigned_local_source(channel, target, resolver)
                .await
                .map(AssignedSwitch::Local);
        }
        let context = self
            .observe_remote_switch(channel)
            .await?
            .ok_or_else(|| PlaybackError::new(404, "Channel not found"))?;
        let rows = request_order(&context, resolver, channel, RemoteSwitchKind::Assigned).await?;
        let current = context.source.stream;
        let (kind, url) =
            match remote_target_url(&rows, current, target, RemoteSwitchKind::Assigned) {
                Ok(url) => (RemoteSwitchKind::Assigned, url.to_owned()),
                Err(reason) if current == target => return Err(reason),
                Err(original_refusal) => {
                    let kind = RemoteSwitchKind::AssignedOneSlot;
                    let rows = request_order(&context, resolver, channel, kind).await?;
                    let url = remote_target_url(&rows, current, target, kind)
                        .map_err(|_| original_refusal)?;
                    (kind, url.to_owned())
                }
            };
        let verdict = request_remote_switch(context, target, &url, kind).await?;
        Ok(AssignedSwitch::Remote { url, verdict })
    }

    pub(crate) async fn next_source(
        &self,
        channel: &str,
        resolver: &dyn PlaybackResolver,
    ) -> Result<NextStreamOutcome<'_>, PlaybackError> {
        if !self.is_remote_switch_requester(channel) {
            return self.next_local_source(channel, resolver).await;
        }
        let Some(context) = self.observe_remote_switch(channel).await? else {
            return self.next_local_source(channel, resolver).await;
        };
        let worker = self.local_source_switch_worker().ok_or_else(|| {
            PlaybackError::new(501, "Remote source switch requires worker identity")
        })?;
        let rows = request_order(&context, resolver, channel, RemoteSwitchKind::Next).await?;
        let previous = context.source.stream;
        let successor = next_assigned_stream(&rows, previous)?;
        let next = successor.id;
        if successor.missing_active_default_profile {
            return Ok(NextStreamOutcome::MissingActiveDefault { previous, next });
        }
        let url = remote_target_url(&rows, previous, next, RemoteSwitchKind::Next)?.to_owned();
        let verdict = request_remote_switch(context, next, &url, RemoteSwitchKind::Next).await?;
        Ok(NextStreamOutcome::Remote {
            selected: NextStreamSwitch {
                previous,
                next,
                url,
                worker,
            },
            verdict,
        })
    }
}

async fn request_order(
    context: &RemoteContext<'_>,
    resolver: &dyn PlaybackResolver,
    channel: &str,
    kind: RemoteSwitchKind,
) -> Result<Vec<OrderedChannelStream>, PlaybackError> {
    time::timeout_at(
        context.deadline,
        remote_order(resolver, channel, &context.source, remote_capacity(kind)),
    )
    .await
    .map_err(|_| PlaybackError::new(504, "Remote switch lookup timed out"))?
}

async fn request_remote_switch(
    context: RemoteContext<'_>,
    target: i64,
    url: &str,
    kind: RemoteSwitchKind,
) -> Result<Option<RemoteSwitchVerdict>, PlaybackError> {
    let RemoteContext {
        store,
        channel,
        source,
        deadline,
    } = context;
    let wake = RemoteSwitchWake {
        channel,
        request: Uuid::new_v4(),
        kind,
    };
    let intent = RemoteSwitchIntent {
        source,
        target,
        url_sha256: url_digest(url),
    };
    let status = time::timeout_at(deadline, store.begin_remote_switch(&wake, &intent)).await;
    if matches!(status, Ok(Ok(false))) {
        return Err(PlaybackError::new(
            409,
            "Remote source owner or request changed",
        ));
    }
    // Reconcile the same UUID after an ambiguous publication result,
    // without ever publishing a second switch.
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
    Ok(verdict)
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
    let result = submit_owned(
        engine, session, &intent, channel, fence, resolver, wake.kind,
    )
    .await;
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
    kind: RemoteSwitchKind,
) -> Result<PendingSourceSwitch, PlaybackError> {
    let ordered = time::timeout(
        OWNER_RESOLVE,
        remote_order(resolver, channel, &intent.source, remote_capacity(kind)),
    )
    .await
    .map_err(|_| PlaybackError::new(504, "Owner source lookup timed out"))??;
    let url = owner_target_url(&ordered, intent.source.stream, intent.target, kind)?;
    if url_digest(url) != intent.url_sha256
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
    let admission = if kind == RemoteSwitchKind::Assigned && intent.source.stream == intent.target {
        SwitchAdmission::RemoteOwnerReselect
    } else {
        SwitchAdmission::RemoteOwner
    };
    engine.submit_in_local_session(session, None, admission, |active| {
        if (source_switch::source(active), active.provider_max_streams)
            != (
                (intent.source.stream, Some(intent.source.profile)),
                remote_capacity(kind),
            )
        {
            return Err(PlaybackError::new(
                409,
                "Remote source owner or target changed",
            ));
        }
        Ok((intent.target, Some(url)))
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
            kind: RemoteSwitchKind::Assigned,
        };
        let next = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(11),
            kind: RemoteSwitchKind::Assigned,
        };
        let latest = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(12),
            kind: RemoteSwitchKind::Assigned,
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

    #[test]
    fn remote_next_owner_rechecks_the_exact_saved_successor_before_using_the_url() {
        let normal = [11, 12, 13].map(|id| OrderedChannelStream {
            id,
            direct_url: Some(format!("http://owned-origin/{id}.ts")),
            missing_active_default_profile: false,
        });
        assert_eq!(
            owner_target_url(&normal, 11, 12, RemoteSwitchKind::Next).unwrap(),
            "http://owned-origin/12.ts"
        );
        assert_eq!(
            owner_target_url(&normal, 13, 11, RemoteSwitchKind::Next).unwrap(),
            "http://owned-origin/11.ts"
        );

        let mut changed = normal;
        changed.swap(1, 2);
        assert_eq!(
            owner_target_url(&changed, 11, 12, RemoteSwitchKind::Next)
                .unwrap_err()
                .status,
            409
        );
        assert_eq!(
            owner_target_url(&changed, 11, 12, RemoteSwitchKind::Assigned).unwrap(),
            "http://owned-origin/12.ts"
        );
        changed[1].direct_url = None;
        changed[1].missing_active_default_profile = true;
        let immediate = next_assigned_stream(&changed, 11).unwrap();
        assert_eq!(immediate.id, 13);
        assert_eq!(
            owner_target_url(&changed, 11, 13, RemoteSwitchKind::Next)
                .unwrap_err()
                .status,
            501
        );
    }

    #[test]
    fn remote_assigned_reselect_keeps_only_the_exact_fresh_current_direct_url() {
        let mut rows = [11, 12].map(|id| OrderedChannelStream {
            id,
            direct_url: Some(format!("http://owned-origin/{id}.ts")),
            missing_active_default_profile: false,
        });
        let confirmed = owner_target_url(&rows, 11, 11, RemoteSwitchKind::Assigned).unwrap();
        assert_eq!(confirmed, "http://owned-origin/11.ts");
        let original_fingerprint = url_digest(confirmed);
        assert!(owner_target_url(&rows, 11, 11, RemoteSwitchKind::Next).is_err());

        rows[0].direct_url = Some("http://owned-origin/edited.ts".into());
        let changed = owner_target_url(&rows, 11, 11, RemoteSwitchKind::Assigned).unwrap();
        assert_ne!(url_digest(changed), original_fingerprint);
        rows[0].direct_url = None;
        assert_eq!(
            owner_target_url(&rows, 11, 11, RemoteSwitchKind::Assigned)
                .unwrap_err()
                .status,
            501
        );
    }

    #[test]
    fn remote_one_slot_is_only_a_distinct_target_and_retains_a_separate_mode() {
        let rows = [11, 12].map(|id| OrderedChannelStream {
            id,
            direct_url: Some(format!("http://owned-origin/{id}.ts")),
            missing_active_default_profile: false,
        });
        assert_eq!(remote_capacity(RemoteSwitchKind::Assigned), 0);
        assert_eq!(remote_capacity(RemoteSwitchKind::Next), 0);
        assert_eq!(remote_capacity(RemoteSwitchKind::AssignedOneSlot), 1);
        assert_eq!(
            owner_target_url(&rows, 11, 12, RemoteSwitchKind::AssignedOneSlot).unwrap(),
            "http://owned-origin/12.ts"
        );
        assert!(owner_target_url(&rows, 11, 11, RemoteSwitchKind::AssignedOneSlot).is_err());
    }
}
