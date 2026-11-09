//! Acknowledged live-client stats triggers; full original Dispatcharr credit.
use super::*;

#[derive(Default)]
pub(super) struct State {
    trigger: Option<Weak<dyn dispatcharr_contracts::LiveStatsTrigger>>,
    ready: std::collections::HashSet<String>,
    pending: HashMap<String, HashMap<String, Weak<ClientInner>>>,
}
fn trigger(state: &State) -> Option<Arc<dyn dispatcharr_contracts::LiveStatsTrigger>> {
    state.trigger.as_ref().and_then(Weak::upgrade)
}
fn announce(client: &ClientInner, trigger: &dyn dispatcharr_contracts::LiveStatsTrigger) {
    if client.registered.load(Ordering::Acquire)
        && !*client.stop.borrow()
        && !client.released.load(Ordering::Acquire)
        && !client.stats_announced.swap(true, Ordering::AcqRel)
    {
        trigger.changed();
    }
}
pub(super) fn registered(store: &Inner, client: &Arc<ClientInner>, metadata_exists: bool) {
    let notify = {
        let mut state = store.live_stats.lock().unwrap();
        let Some(trigger) = trigger(&state) else {
            return;
        };
        if metadata_exists || state.ready.contains(&client.channel) {
            Some(trigger)
        } else {
            state
                .pending
                .entry(client.channel.clone())
                .or_default()
                .insert(client.id.clone(), Arc::downgrade(client));
            None
        }
    };
    if let Some(trigger) = notify {
        announce(client, trigger.as_ref());
    }
}
pub(super) fn channel_ready(store: &Inner, channel: &str) {
    let (trigger, pending) = {
        let mut state = store.live_stats.lock().unwrap();
        let Some(trigger) = trigger(&state) else {
            return;
        };
        state.ready.insert(channel.to_owned());
        (trigger, state.pending.remove(channel).unwrap_or_default())
    };
    // Upgrading/dropping a final ClientInner can reacquire registry locks;
    // never do either while holding the statistics or client-map mutex.
    for client in pending.into_values().filter_map(|client| client.upgrade()) {
        announce(&client, trigger.as_ref());
    }
}
pub(super) fn channel_closed(store: &Inner, channel: &str) {
    let mut state = store.live_stats.lock().unwrap();
    state.ready.remove(channel);
    state.pending.remove(channel);
}
pub(super) fn released(store: &Inner, channel: &str, id: &str, announced: bool) {
    let trigger = {
        let mut state = store.live_stats.lock().unwrap();
        if let Some(pending) = state.pending.get_mut(channel) {
            pending.remove(id);
            if pending.is_empty() {
                state.pending.remove(channel);
            }
        }
        trigger(&state)
    };
    if announced {
        if let Some(trigger) = trigger {
            trigger.changed();
        }
    }
}
pub(super) fn stopped_channel(store: &Inner, channel: &str, clients: &[Arc<ClientInner>]) {
    let callback = {
        let mut state = store.live_stats.lock().unwrap();
        state.ready.remove(channel);
        state.pending.remove(channel);
        trigger(&state)
    };
    let count = clients
        .iter()
        .filter(|client| {
            client.channel == channel && client.stats_announced.swap(false, Ordering::AcqRel)
        })
        .count();
    if let Some(callback) = callback {
        for _ in 0..count {
            callback.changed();
        }
    }
}
impl ClientStore {
    /// Existing store/lease clones share the callback. The application retains
    /// its owner: weak references avoid engine → store → statistics cycles.
    pub fn set_live_stats_trigger(
        &self,
        trigger: Weak<dyn dispatcharr_contracts::LiveStatsTrigger>,
    ) {
        self.inner.live_stats.lock().unwrap().trigger = Some(trigger);
    }
    /// An observed shared owner metadata barrier also releases a follower's
    /// deferred registrations; ownership or input data is never changed here.
    pub fn announce_channel_statistics(&self, channel: &str) {
        channel_ready(&self.inner, channel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatcharr_contracts::{LiveStatsTrigger, PlaybackViewer, UserLimitPolicy};
    use std::sync::atomic::AtomicUsize;
    struct Counter(AtomicUsize);
    impl LiveStatsTrigger for Counter {
        fn changed(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn viewer() -> PlaybackViewer {
        PlaybackViewer {
            user_id: None,
            username: None,
            ip_address: "127.0.0.1".parse().unwrap(),
            user_agent: "stats-fixture".into(),
            stream_limit: 0,
            limit_policy: UserLimitPolicy::default(),
        }
    }
    async fn fixture() -> (ClientStore, Arc<Counter>) {
        let url = std::env::var("REDIS_TEST_URL").expect("disposable Redis required");
        let store = ClientStore::connect(
            &url,
            ClientConfig {
                max_clients: 2,
                flush_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let count = Arc::new(Counter(AtomicUsize::new(0)));
        let callback: Arc<dyn LiveStatsTrigger> = count.clone();
        store.set_live_stats_trigger(Arc::downgrade(&callback));
        (store, count)
    }
    async fn wait(check: impl Fn() -> bool) {
        time::timeout(Duration::from_secs(3), async {
            while !check() {
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn metadata(store: &ClientStore, channel: &str) {
        store
            .publish_channel(
                channel,
                "",
                "stats-fence",
                &BTreeMap::from([
                    ("state".into(), "connecting".into()),
                    ("init_time".into(), now().to_string()),
                ]),
                0,
            )
            .await
            .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires owned disposable Redis; serial with delayed-command tests"]
    async fn registration_waits_for_metadata_and_releases_exactly_once() {
        let (store, count) = fixture().await;
        let channel = uuid::Uuid::new_v4().to_string();
        let client = store.register(&channel, &viewer(), "ts").await.unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.inner.live_stats.lock().unwrap().pending[&channel].len(),
            1
        );
        store
            .publish_channel(
                &channel,
                "",
                "stats-fence",
                &BTreeMap::from([("state".into(), "initializing".into())]),
                0,
            )
            .await
            .unwrap();
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            0,
            "unadmitted provider initialization must remain deferred"
        );
        metadata(&store, &channel).await;
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        metadata(&store, &channel).await;
        store.announce_channel_statistics(&channel);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        client.release().await.unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 2);
        drop(client);
        assert_eq!(count.0.load(Ordering::SeqCst), 2);
        store.remove_channel(&channel, "stats-fence").await.unwrap();
        assert!(store.inner.live_stats.lock().unwrap().ready.is_empty());
        for _ in 0..8 {
            let channel = uuid::Uuid::new_v4().to_string();
            let client = store.register(&channel, &viewer(), "ts").await.unwrap();
            client.release().await.unwrap();
            drop(client);
            assert!(store.inner.live_stats.lock().unwrap().pending.is_empty());
        }
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            2,
            "unadmitted channel attempts must not produce phantom stats"
        );
        store.shutdown().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires owned disposable Redis"]
    async fn channel_stop_notifies_after_atomic_cleanup_without_waiting_for_held_leases() {
        let (store, count) = fixture().await;
        let channel = uuid::Uuid::new_v4().to_string();
        metadata(&store, &channel).await;
        let first = store.register(&channel, &viewer(), "ts").await.unwrap();
        let second = store.register(&channel, &viewer(), "ts").await.unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 2);
        assert!(store.request_channel_stop(&channel).await.unwrap());
        assert_eq!(count.0.load(Ordering::SeqCst), 4);
        assert!(store.channel_clients(&channel).await.unwrap().is_empty());
        assert_eq!(
            store.inner.slots.available_permits(),
            0,
            "media/frame leases still retain physical admission"
        );
        assert!(store.request_channel_stop(&channel).await.unwrap());
        assert_eq!(count.0.load(Ordering::SeqCst), 4);
        drop((first, second));
        wait(|| store.inner.slots.available_permits() == 2).await;
        assert_eq!(count.0.load(Ordering::SeqCst), 4);
        store.remove_channel(&channel, "stats-fence").await.unwrap();
        let mut connection = store.inner.connection.clone();
        let _: usize = redis::cmd("DEL")
            .arg(format!("live:channel:{channel}:stopping"))
            .query_async(&mut connection)
            .await
            .unwrap();
        store.shutdown().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires owned disposable Redis"]
    async fn abandoned_era_notifies_only_removed_clients_and_preserves_preowner_registration() {
        let (store, count) = fixture().await;
        let channel = uuid::Uuid::new_v4().to_string();
        let mut connection = store.inner.connection.clone();
        let owner = format!("live:channel:{channel}:owner");
        let _: String = redis::cmd("SET")
            .arg(&owner)
            .arg("old-owner")
            .query_async(&mut connection)
            .await
            .unwrap();
        store
            .publish_channel(
                &channel,
                "old-owner",
                "old-owner",
                &BTreeMap::from([("state".into(), "connecting".into())]),
                0,
            )
            .await
            .unwrap();
        let old = store.register(&channel, &viewer(), "ts").await.unwrap();
        let _: usize = redis::cmd("DEL")
            .arg(&owner)
            .query_async(&mut connection)
            .await
            .unwrap();
        let new = store.register(&channel, &viewer(), "ts").await.unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 2);
        assert_eq!(
            store
                .stop_abandoned_channel(&channel, "old-owner")
                .await
                .unwrap(),
            1
        );
        assert_eq!(count.0.load(Ordering::SeqCst), 3);
        assert!(*old.stopped().borrow());
        assert!(!*new.stopped().borrow());
        assert_eq!(store.channel_clients(&channel).await.unwrap().len(), 1);
        assert_eq!(
            store
                .stop_abandoned_channel(&channel, "old-owner")
                .await
                .unwrap(),
            0
        );
        assert_eq!(count.0.load(Ordering::SeqCst), 3);
        old.release().await.unwrap();
        new.release().await.unwrap();
        drop((old, new));
        assert_eq!(count.0.load(Ordering::SeqCst), 4);
        store.shutdown().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires owned disposable Redis; pauses all commands250ms"]
    async fn cancelled_registration_notifies_only_after_actual_redis_replies() {
        let (store, count) = fixture().await;
        let channel = uuid::Uuid::new_v4().to_string();
        metadata(&store, &channel).await;
        let mut connection = store.inner.connection.clone();
        let _: String = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(250)
            .query_async(&mut connection)
            .await
            .unwrap();
        let task = {
            let store = store.clone();
            let channel = channel.clone();
            tokio::spawn(async move { store.register(&channel, &viewer(), "ts").await })
        };
        time::sleep(Duration::from_millis(20)).await;
        task.abort();
        let _ = task.await;
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.inner.slots.available_permits(),
            1,
            "pending physical registration retains admission"
        );
        wait(|| count.0.load(Ordering::SeqCst) == 2 && store.inner.slots.available_permits() == 2)
            .await;
        assert!(store.channel_clients(&channel).await.unwrap().is_empty());
        assert!(store.inner.live_stats.lock().unwrap().pending.is_empty());
        store.remove_channel(&channel, "stats-fence").await.unwrap();
        store.shutdown().await;
    }
}
