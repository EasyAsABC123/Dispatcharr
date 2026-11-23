//! Supervisor admission regression only; not Source task execution authority.
use super::*;
struct Empty;
impl crate::milestone_dispatch::Head for Empty {
    fn queue(&self) -> Queue {
        Queue::Celery
    }
    fn peek(
        &self,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>> {
        Box::pin(async { Ok(None) })
    }
}
struct Due {
    raw: Arc<[u8]>,
    started: AtomicBool,
    count: Arc<std::sync::atomic::AtomicUsize>,
    gate: Arc<Semaphore>,
}
impl Lifecycle for Due {
    fn shutdown(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async { Ok(()) })
    }
}
impl Handler for Due {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn due_head(&self) -> Option<Arc<[u8]>> {
        if self.started.load(Ordering::Acquire) {
            None
        } else {
            Some(self.raw.clone())
        }
    }
    fn run_due(
        &self,
        raw: Arc<[u8]>,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            assert_eq!(raw, self.raw);
            assert!(!self.started.swap(true, Ordering::AcqRel));
            self.count.fetch_add(1, Ordering::AcqRel);
            self.gate.acquire().await.unwrap().forget();
            Ok(Disposition::CompletedWithoutId)
        })
    }
    fn run_once(
        &self,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async { Err(Disposition::UnsupportedHead) })
    }
}
#[tokio::test(start_paused = true)]
async fn already_durable_due_jobs_share_existing_six_global_slots() {
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let mut handlers = BTreeMap::new();
    let mut entries = Vec::new();
    for (i, family) in milestone_dispatch::REQUIRED.into_iter().enumerate() {
        let handler: Arc<dyn Handler> = Arc::new(Due {
            raw: Arc::from(vec![i as u8]),
            started: AtomicBool::new(false),
            count: count.clone(),
            gate: gate.clone(),
        });
        let owned = FamilyAdapter(Arc::new(OwnedFamily {
            inner: handler,
            slot: Arc::new(Semaphore::new(1)),
            closed: AtomicBool::new(false),
        }));
        entries.push((family, Arc::new(owned.clone()) as Arc<dyn Handler>));
        handlers.insert(family, owned);
    }
    let head = Arc::new(Empty);
    let lane = Arc::new(Lane {
        prepared: Prepared::new(head.clone(), entries).unwrap(),
        state: watch::channel(State::Prepared).0,
        done: watch::channel(false).0,
    });
    let (stop, receiver) = watch::channel(false);
    let task = tokio::spawn(celery_lane(
        lane,
        head,
        handlers,
        Arc::new(()),
        receiver,
        Duration::from_millis(100),
        None,
    ));
    for _ in 0..30 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        count.load(Ordering::Acquire),
        6,
        "due work must not bypass existing six-task JoinSet budget"
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(count.load(Ordering::Acquire), 6);
    stop.send_replace(true);
    gate.add_permits(6);
    task.await.unwrap();
    assert_eq!(
        count.load(Ordering::Acquire),
        6,
        "stop cannot admit waiting due work"
    );
}
