//! Actual bounded Celery + separate DVR lanes over a complete selected16 typed registry.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
//! Starts no scheduler; missing families cannot be advertised as prepared.
pub use crate::normal_default_families::{
    prepare_defaults, prepare_stats, Error as FamilyPreparationError, PreparedDefaults,
};
use crate::{
    milestone_dispatch::{self, Disposition, Family, Handler, Lifecycle, Prepared, Queue},
    transport::NormalHead,
};
use dispatcharr_contracts::BoxFuture;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
#[derive(Debug)]
pub enum Error {
    MissingFamily(Family),
    DuplicateFamily(Family),
    Configuration,
    Transport,
    Closed,
    DrainTimeout,
    Poisoned,
}
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Starting,
    Working(usize),
    Observed(Disposition),
    Blocked(Disposition),
    Fault(Disposition),
    Stopped,
}
#[derive(Clone, Debug)]
pub struct Readiness {
    pub scope: &'static str,
    pub celery: State,
    pub dvr: State,
    pub required_families: usize,
    pub prepared_families: usize,
    pub scheduler_enabled: bool,
    pub ready: bool,
}
struct FamilyOwner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
    _family: Arc<OwnedFamily>,
}
struct OwnedFamily {
    inner: Arc<dyn Handler>,
    slot: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
struct FamilyAdapter(Arc<OwnedFamily>);
impl Handler for FamilyAdapter {
    fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.0.inner.head_advance_notifier()
    }
    fn maximum_concurrency(&self) -> usize {
        self.0.inner.maximum_concurrency()
    }
    fn supports_selected_head(&self) -> bool {
        self.0.inner.supports_selected_head()
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational("normal DVR intake closed".into()));
            }
            let slot = self
                .0
                .slot
                .clone()
                .try_acquire_owned()
                .map_err(|_| Disposition::Capacity)?;
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational("normal DVR intake closed".into()));
            }
            let owner: Arc<dyn Send + Sync> = Arc::new(FamilyOwner {
                _slot: slot,
                _caller: caller,
                _family: self.0.clone(),
            });
            self.0.inner.run_selected(expected, owner).await
        })
    }

    fn run_once(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational(
                    "normal family intake closed".into(),
                ));
            }
            let slot = self
                .0
                .slot
                .clone()
                .try_acquire_owned()
                .map_err(|_| Disposition::Capacity)?;
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational(
                    "normal family intake closed".into(),
                ));
            }
            let owner: Arc<dyn Send + Sync> = Arc::new(FamilyOwner {
                _slot: slot,
                _caller: caller,
                _family: self.0.clone(),
            });
            // The owner reaches the physical transport/SQL/media receipts. A logical
            // error cannot release this admission before the real downstream reply.
            self.0.inner.run_once(owner).await
        })
    }
}
impl Lifecycle for FamilyAdapter {
    fn stop_admission(&self) {
        self.0.inner.stop_admission();
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        self.0.inner.shutdown(timeout)
    }
}
struct Lane {
    prepared: Prepared,
    state: watch::Sender<State>,
    done: watch::Sender<bool>,
}
struct Inner {
    celery: Arc<Lane>,
    dvr: Arc<Lane>,
    heads: [Arc<NormalHead>; 2],
    families: BTreeMap<Family, Arc<OwnedFamily>>,
    poll: Duration,
    lifecycle: Mutex<(bool, bool)>,
}
struct Lifetime(watch::Sender<bool>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
#[derive(Clone)]
pub struct Application {
    inner: Arc<Inner>,
    lifetime: Arc<Lifetime>,
}
#[derive(Debug)]
pub struct Drain {
    pub pollers_settled: bool,
    pub admissions_settled: bool,
    pub families: Vec<(Family, Result<(), Disposition>)>,
    pub heads_settled: bool,
}
impl Drain {
    pub fn complete(&self) -> bool {
        self.pollers_settled
            && self.admissions_settled
            && self.heads_settled
            && self.families.iter().all(|(_, value)| value.is_ok())
    }
}
impl Application {
    pub async fn prepare(
        broker: &str,
        backend: &str,
        entries: Vec<(Family, Arc<dyn Handler>)>,
        poll: Duration,
    ) -> Result<Self, Error> {
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&poll) {
            return Err(Error::Configuration);
        }
        let mut seen = BTreeSet::new();
        for (family, _) in &entries {
            if !seen.insert(*family) {
                return Err(Error::DuplicateFamily(*family));
            }
        }
        for family in milestone_dispatch::REQUIRED
            .into_iter()
            .chain([Family::BackupCreate, Family::BackupScheduled])
        {
            if !seen.contains(&family) {
                return Err(Error::MissingFamily(family));
            }
        }
        let mut families = BTreeMap::new();
        for (family, inner) in entries {
            let maximum = inner.maximum_concurrency();
            if maximum == 0
                || maximum > 20
                || (family != Family::Recording && maximum != 1)
                || !inner.supports_selected_head()
            {
                return Err(Error::Configuration);
            }
            families.insert(
                family,
                Arc::new(OwnedFamily {
                    inner,
                    slot: Arc::new(Semaphore::new(maximum)),
                    closed: AtomicBool::new(false),
                }),
            );
        }
        let celery = Arc::new(
            NormalHead::connect(broker, backend, Queue::Celery)
                .await
                .map_err(|_| Error::Transport)?,
        );
        let dvr = match NormalHead::connect(broker, backend, Queue::Dvr).await {
            Ok(head) => Arc::new(head),
            Err(_) => {
                let _ = celery.shutdown(Duration::from_secs(4)).await;
                return Err(Error::Transport);
            }
        };
        let adapters = || {
            families
                .iter()
                .map(|(family, inner)| {
                    (
                        *family,
                        Arc::new(FamilyAdapter(inner.clone())) as Arc<dyn Handler>,
                    )
                })
                .collect::<Vec<_>>()
        };
        let celery_prepared =
            Prepared::new(celery.clone(), adapters()).map_err(|_| Error::Configuration)?;
        let dvr_prepared =
            Prepared::new(dvr.clone(), adapters()).map_err(|_| Error::Configuration)?;
        Ok(Self {
            inner: Arc::new(Inner {
                celery: Arc::new(Lane {
                    prepared: celery_prepared,
                    state: watch::channel(State::Prepared).0,
                    done: watch::channel(true).0,
                }),
                dvr: Arc::new(Lane {
                    prepared: dvr_prepared,
                    state: watch::channel(State::Prepared).0,
                    done: watch::channel(true).0,
                }),
                heads: [celery, dvr],
                families,
                poll,
                lifecycle: Mutex::new((false, false)),
            }),
            lifetime: Arc::new(Lifetime(watch::channel(false).0)),
        })
    }
    pub fn readiness(&self) -> Readiness {
        let celery = self.inner.celery.state.borrow().clone();
        let dvr = self.inner.dvr.state.borrow().clone();
        let active = |state: &State| {
            matches!(
                state,
                State::Starting | State::Working(_) | State::Observed(_)
            )
        };
        Readiness {
            scope: "selected16-migration-families; not all Source tasks",
            ready: active(&celery) && active(&dvr),
            celery,
            dvr,
            required_families: milestone_dispatch::REQUIRED.len() + 2,
            prepared_families: self.inner.celery.prepared.prepared_families(),
            scheduler_enabled: false,
        }
    }
    pub fn start(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let mut lifecycle = self.inner.lifecycle.lock().map_err(|_| Error::Poisoned)?;
        if lifecycle.0 || lifecycle.1 {
            return Err(Error::Closed);
        }
        lifecycle.0 = true;
        {
            let lane = self.inner.celery.clone();
            lane.done.send_replace(false);
            lane.state.send_replace(State::Starting);
            let handlers = self
                .inner
                .families
                .iter()
                .filter(|(family, _)| **family != Family::Recording)
                .map(|(family, value)| (*family, FamilyAdapter(value.clone())))
                .collect();
            tokio::spawn(celery_lane(
                lane,
                self.inner.heads[0].clone(),
                handlers,
                owner.clone(),
                self.lifetime.0.subscribe(),
                self.inner.poll,
            ));
        }
        let lane = self.inner.dvr.clone();
        lane.done.send_replace(false);
        lane.state.send_replace(State::Starting);
        let Some(family) = self.inner.families.get(&Family::Recording).cloned() else {
            return Err(Error::MissingFamily(Family::Recording));
        };
        let head = self.inner.heads[1].clone();
        let stop = self.lifetime.0.subscribe();
        let poll = self.inner.poll;
        tokio::spawn(dvr_lane(
            lane,
            head,
            FamilyAdapter(family),
            owner,
            stop,
            poll,
        ));
        Ok(())
    }
    /// Stop only intake immediately; callers can then settle producer startup
    /// before the full worker/physical-port drain. Existing tasks retain owners.
    pub fn stop_admission(&self) {
        self.lifetime.0.send_replace(true);
    }
    /// Caller stops scheduler/API producers first. Do not close even one shared
    /// family port until both pollers and every actual downstream owner settle.
    pub async fn shutdown(&self, timeout: Duration) -> Drain {
        let deadline = tokio::time::Instant::now() + timeout;
        if let Ok(mut lifecycle) = self.inner.lifecycle.lock() {
            lifecycle.1 = true;
        }
        self.lifetime.0.send_replace(true);
        for family in self.inner.families.values() {
            family.inner.stop_admission();
        }
        let wait = async {
            for lane in [&self.inner.celery, &self.inner.dvr] {
                let mut done = lane.done.subscribe();
                loop {
                    if *done.borrow_and_update() {
                        break;
                    }
                    if done.changed().await.is_err() {
                        return false;
                    }
                }
            }
            true
        };
        let pollers_settled = tokio::time::timeout_at(deadline, wait)
            .await
            .unwrap_or(false);
        if !pollers_settled {
            return Drain {
                pollers_settled: false,
                admissions_settled: false,
                families: vec![],
                heads_settled: false,
            };
        }
        for family in self.inner.families.values() {
            family.closed.store(true, Ordering::Release);
        }
        let quiet = async {
            let mut held = Vec::new();
            for family in self.inner.families.values() {
                let Ok(permit) = family
                    .slot
                    .clone()
                    .acquire_many_owned(family.inner.maximum_concurrency() as u32)
                    .await
                else {
                    return false;
                };
                held.push(permit);
            }
            true
        };
        let admissions_settled = tokio::time::timeout_at(deadline, quiet)
            .await
            .unwrap_or(false);
        if !admissions_settled {
            return Drain {
                pollers_settled: true,
                admissions_settled: false,
                families: vec![],
                heads_settled: false,
            };
        }
        let mut results = Vec::new();
        for (family, handler) in &self.inner.families {
            results.push((
                *family,
                handler
                    .inner
                    .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await,
            ));
        }
        let mut heads_settled = true;
        for head in &self.inner.heads {
            if head
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .is_err()
            {
                heads_settled = false;
            }
        }
        Drain {
            pollers_settled,
            admissions_settled,
            families: results,
            heads_settled,
        }
    }
}

/// Bounded same-unclaimed-head cooldown. A completed handler is not proof of
/// a claim: ETA/capacity/domain refusals must throttle the Redis peek itself.
struct HeadBackoff {
    until: tokio::time::Instant,
    raw: BTreeMap<Arc<[u8]>, tokio::time::Instant>,
    maximum: usize,
}
impl HeadBackoff {
    fn new(maximum: usize) -> Self {
        Self {
            until: tokio::time::Instant::now(),
            raw: BTreeMap::new(),
            maximum,
        }
    }
    fn can_peek(&self) -> bool {
        tokio::time::Instant::now() >= self.until
    }
    fn allow(&mut self, raw: &Arc<[u8]>) -> bool {
        let now = tokio::time::Instant::now();
        self.raw.retain(|_, until| *until > now);
        if let Some(until) = self.raw.get(raw) {
            self.until = *until;
            false
        } else {
            true
        }
    }
    fn outcome(
        &mut self,
        raw: Arc<[u8]>,
        result: &Result<Disposition, Disposition>,
        poll: Duration,
    ) {
        let value = match result {
            Ok(value) | Err(value) => value,
        };
        if matches!(
            value,
            Disposition::Empty
                | Disposition::NotDue
                | Disposition::Capacity
                | Disposition::HeadChanged
                | Disposition::UnsupportedHead
                | Disposition::UnsupportedDomain
        ) {
            let now = tokio::time::Instant::now();
            let until = now + poll;
            self.raw.retain(|_, until| *until > now);
            if self.raw.len() >= self.maximum && !self.raw.contains_key(&raw) {
                if let Some(oldest) = self
                    .raw
                    .iter()
                    .min_by_key(|(_, until)| **until)
                    .map(|(raw, _)| raw.clone())
                {
                    self.raw.remove(&oldest);
                }
            }
            self.raw.insert(raw, until);
            // A real changed-head observation permits one immediate fresh peek;
            // an identical reappearing raw remains cooled by allow().
            self.until = if matches!(value, Disposition::HeadChanged) {
                now
            } else {
                until
            };
        } else {
            self.raw.remove(&raw);
            self.wake();
        }
    }
    fn wake(&mut self) {
        self.until = tokio::time::Instant::now();
    }
    fn deadline(&self, poll: Duration) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        if self.until > now {
            self.until
        } else {
            now + poll
        }
    }
}

async fn dvr_lane(
    lane: Arc<Lane>,
    head: Arc<dyn crate::milestone_dispatch::Head>,
    handler: FamilyAdapter,
    caller: Arc<dyn Send + Sync>,
    mut stop: watch::Receiver<bool>,
    poll: Duration,
) {
    let maximum = handler.maximum_concurrency();
    let advanced = handler.head_advance_notifier();
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = BTreeSet::<Arc<[u8]>>::new();
    let mut fault = None;
    let mut backoff = HeadBackoff::new(maximum);
    loop {
        if *stop.borrow() && tasks.is_empty() {
            break;
        }
        if fault.is_some() && tasks.is_empty() {
            break;
        }
        if !*stop.borrow() && fault.is_none() && tasks.len() < maximum && backoff.can_peek() {
            match head.peek(caller.clone()).await {
                Ok(Some(raw)) => {
                    if milestone_dispatch::classify(&raw) != Some(Family::Recording) {
                        lane.state
                            .send_replace(State::Blocked(Disposition::UnsupportedHead));
                    } else {
                        let expected: Arc<[u8]> = Arc::from(raw);
                        if backoff.allow(&expected) && !pending.contains(&expected) {
                            pending.insert(expected.clone());
                            let selected = expected.clone();
                            let owned = handler.clone();
                            let owner = caller.clone();
                            tasks.spawn(async move {
                                let result = owned.run_selected(selected.clone(), owner).await;
                                (selected, result)
                            });
                            lane.state.send_replace(State::Working(tasks.len()));
                        }
                    }
                }
                Ok(None) => {
                    if tasks.is_empty() {
                        lane.state.send_replace(State::Observed(Disposition::Empty));
                    }
                }
                Err(error @ Disposition::UnsupportedHead)
                | Err(error @ Disposition::UnsupportedDomain) => {
                    lane.state.send_replace(State::Blocked(error));
                }
                Err(error) => {
                    fault = Some(error.clone());
                    lane.state.send_replace(State::Fault(error));
                }
            }
        }
        tokio::select! {
              result=tasks.join_next(),if !tasks.is_empty()=>{
               match result{
                Some(Ok((raw,result)))=>{pending.remove(&raw);backoff.outcome(raw.clone(),&result,poll);match result{Ok(value@Disposition::UnsupportedHead)|Ok(value@Disposition::UnsupportedDomain)=>{if fault.is_none(){lane.state.send_replace(State::Blocked(value));}},Ok(value)=>{if fault.is_none(){lane.state.send_replace(State::Observed(value));}},Err(error@Disposition::UnsupportedHead)|Err(error@Disposition::UnsupportedDomain)=>{if fault.is_none(){lane.state.send_replace(State::Blocked(error));}},Err(error@Disposition::Capacity)|Err(error@Disposition::HeadChanged)|Err(error@Disposition::NotDue)=>{if fault.is_none(){lane.state.send_replace(State::Observed(error));}},Err(error)=>{fault=Some(error.clone());lane.state.send_replace(State::Fault(error));}}},
                Some(Err(_))=>{let error=Disposition::Operational("normal DVR task supervisor failure".into());fault=Some(error.clone());lane.state.send_replace(State::Fault(error));},None=>{}
               }
              },
              _=stop.changed(),if !*stop.borrow()=>{},
              _=async{match &advanced{Some(notify)=>notify.notified().await,None=>std::future::pending::<()>().await}}=>{backoff.wake();},
        _=tokio::time::sleep_until(backoff.deadline(poll))=>{}
             }
    }
    if fault.is_none() {
        lane.state.send_replace(State::Stopped);
    }
    lane.done.send_replace(true);
}

async fn celery_lane(
    lane: Arc<Lane>,
    head: Arc<dyn crate::milestone_dispatch::Head>,
    handlers: BTreeMap<Family, FamilyAdapter>,
    caller: Arc<dyn Send + Sync>,
    mut stop: watch::Receiver<bool>,
    poll: Duration,
) {
    let maximum = 6usize;
    let advanced: Option<Arc<tokio::sync::Notify>> = None;
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = BTreeSet::<Arc<[u8]>>::new();
    let mut fault = None;
    let mut backoff = HeadBackoff::new(maximum);
    loop {
        if *stop.borrow() && tasks.is_empty() {
            break;
        }
        if fault.is_some() && tasks.is_empty() {
            break;
        }
        if !*stop.borrow() && fault.is_none() && tasks.len() < maximum && backoff.can_peek() {
            match head.peek(caller.clone()).await {
                Ok(Some(raw)) => {
                    let selected = milestone_dispatch::classify(&raw)
                        .filter(|family| *family != Family::Recording);
                    if selected.is_none_or(|family| !handlers.contains_key(&family)) {
                        lane.state
                            .send_replace(State::Blocked(Disposition::UnsupportedHead));
                    } else {
                        let handler = handlers
                            .get(&selected.expect("checked selected family"))
                            .expect("checked registered family");
                        if handler.0.slot.available_permits() == 0 {
                            lane.state.send_replace(State::Working(tasks.len()));
                        } else {
                            let expected: Arc<[u8]> = Arc::from(raw);
                            if backoff.allow(&expected) && !pending.contains(&expected) {
                                pending.insert(expected.clone());
                                let selected = expected.clone();
                                let owned = handler.clone();
                                let owner = caller.clone();
                                tasks.spawn(async move {
                                    let result = owned.run_selected(selected.clone(), owner).await;
                                    (selected, result)
                                });
                                lane.state.send_replace(State::Working(tasks.len()));
                            }
                        }
                    }
                }
                Ok(None) => {
                    if tasks.is_empty() {
                        lane.state.send_replace(State::Observed(Disposition::Empty));
                    }
                }
                Err(error @ Disposition::UnsupportedHead)
                | Err(error @ Disposition::UnsupportedDomain) => {
                    lane.state.send_replace(State::Blocked(error));
                }
                Err(error) => {
                    fault = Some(error.clone());
                    lane.state.send_replace(State::Fault(error));
                }
            }
        }
        tokio::select! {
              result=tasks.join_next(),if !tasks.is_empty()=>{
               match result{
                Some(Ok((raw,result)))=>{pending.remove(&raw);backoff.outcome(raw.clone(),&result,poll);match result{Ok(value@Disposition::UnsupportedHead)|Ok(value@Disposition::UnsupportedDomain)=>{if fault.is_none(){lane.state.send_replace(State::Blocked(value));}},Ok(value)=>{if fault.is_none(){lane.state.send_replace(State::Observed(value));}},Err(error@Disposition::UnsupportedHead)|Err(error@Disposition::UnsupportedDomain)=>{if fault.is_none(){lane.state.send_replace(State::Blocked(error));}},Err(error@Disposition::Capacity)|Err(error@Disposition::HeadChanged)|Err(error@Disposition::NotDue)=>{if fault.is_none(){lane.state.send_replace(State::Observed(error));}},Err(error)=>{fault=Some(error.clone());lane.state.send_replace(State::Fault(error));}}},
                Some(Err(_))=>{let error=Disposition::Operational("normal Celery task supervisor failure".into());fault=Some(error.clone());lane.state.send_replace(State::Fault(error));},None=>{}
               }
              },
              _=stop.changed(),if !*stop.borrow()=>{},
              _=async{match &advanced{Some(notify)=>notify.notified().await,None=>std::future::pending::<()>().await}}=>{backoff.wake();},
        _=tokio::time::sleep_until(backoff.deadline(poll))=>{}
             }
    }
    if fault.is_none() {
        lane.state.send_replace(State::Stopped);
    }
    lane.done.send_replace(true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::milestone_dispatch::Head;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    struct Probe {
        queue: Mutex<VecDeque<Vec<u8>>>,
        starts: AtomicUsize,
        claims: AtomicUsize,
        active: AtomicUsize,
        duplicates: AtomicUsize,
        peeked: AtomicUsize,
        claim_gate: Arc<Semaphore>,
        finish_gate: Arc<Semaphore>,
        advanced: Arc<tokio::sync::Notify>,
    }
    impl Head for Probe {
        fn queue(&self) -> Queue {
            Queue::Dvr
        }
        fn peek(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>> {
            Box::pin(async move {
                self.peeked.fetch_add(1, Ordering::SeqCst);
                Ok(self.queue.lock().unwrap().front().cloned())
            })
        }
    }
    impl Lifecycle for Probe {
        fn shutdown(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl Handler for Probe {
        fn maximum_concurrency(&self) -> usize {
            20
        }
        fn supports_selected_head(&self) -> bool {
            true
        }
        fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
            Some(self.advanced.clone())
        }
        fn run_once(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async { Err(Disposition::UnsupportedDomain) })
        }
        fn run_selected(
            &self,
            expected: Arc<[u8]>,
            owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async move {
                let _owner = owner;
                self.starts.fetch_add(1, Ordering::SeqCst);
                self.claim_gate
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap()
                    .forget();
                {
                    let mut queue = self.queue.lock().unwrap();
                    if queue.front().map(Vec::as_slice) != Some(expected.as_ref()) {
                        self.duplicates.fetch_add(1, Ordering::SeqCst);
                        return Ok(Disposition::HeadChanged);
                    }
                    queue.pop_front();
                }
                self.claims.fetch_add(1, Ordering::SeqCst);
                self.active.fetch_add(1, Ordering::SeqCst);
                self.advanced.notify_one();
                self.finish_gate
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap()
                    .forget();
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(Disposition::CompletedWithoutId)
            })
        }
    }
    async fn until(mut test: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !test() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("bounded queue progress");
    }
    #[tokio::test]
    async fn same_selected_head_is_not_launched_twice_and_twenty_real_claims_drain_before_stop() {
        let probe=Arc::new(Probe {queue:Mutex::new((1..=21).map(|n|serde_json::to_vec(&serde_json::json!({"headers":{"task":"apps.channels.tasks.run_recording","id":n}})).unwrap()).collect()),starts:AtomicUsize::new(0),claims:AtomicUsize::new(0),active:AtomicUsize::new(0),duplicates:AtomicUsize::new(0),peeked:AtomicUsize::new(0),claim_gate:Arc::new(Semaphore::new(0)),finish_gate:Arc::new(Semaphore::new(0)),advanced:Arc::new(tokio::sync::Notify::new())});
        let head: Arc<dyn Head> = probe.clone();
        let wrapped = FamilyAdapter(Arc::new(OwnedFamily {
            inner: probe.clone(),
            slot: Arc::new(Semaphore::new(20)),
            closed: AtomicBool::new(false),
        }));
        let entries = milestone_dispatch::REQUIRED
            .into_iter()
            .chain([Family::BackupCreate, Family::BackupScheduled])
            .map(|family| (family, Arc::new(wrapped.clone()) as Arc<dyn Handler>))
            .collect();
        let lane = Arc::new(Lane {
            prepared: Prepared::new(head.clone(), entries).unwrap(),
            state: watch::channel(State::Starting).0,
            done: watch::channel(false).0,
        });
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(dvr_lane(
            lane.clone(),
            head,
            wrapped,
            Arc::new(()),
            rx,
            Duration::from_millis(2),
        ));
        until(|| probe.peeked.load(Ordering::SeqCst) >= 6).await;
        assert_eq!(
            probe.starts.load(Ordering::SeqCst),
            1,
            "same unclaimed raw must stay single in flight"
        );
        probe.claim_gate.add_permits(21);
        until(|| probe.claims.load(Ordering::SeqCst) == 20).await;
        assert_eq!(probe.active.load(Ordering::SeqCst), 20);
        assert_eq!(probe.starts.load(Ordering::SeqCst), 20);
        assert_eq!(probe.queue.lock().unwrap().len(), 1);
        stop.send_replace(true);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !*lane.done.borrow(),
            "stop cannot settle twenty retained physical task owners"
        );
        assert!(!task.is_finished());
        probe.finish_gate.add_permits(20);
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(probe.duplicates.load(Ordering::SeqCst), 0);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.queue.lock().unwrap().len(), 1);
        assert!(*lane.done.borrow());
        assert!(matches!(*lane.state.borrow(), State::Stopped));
    }

    struct UnclaimedProbe {
        queue: Queue,
        raw: Mutex<Vec<u8>>,
        outcome: Mutex<Disposition>,
        peeks: AtomicUsize,
        runs: AtomicUsize,
        advanced: Arc<tokio::sync::Notify>,
    }
    impl Head for UnclaimedProbe {
        fn queue(&self) -> Queue {
            self.queue
        }
        fn peek(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>> {
            Box::pin(async move {
                self.peeks.fetch_add(1, Ordering::SeqCst);
                Ok(Some(self.raw.lock().unwrap().clone()))
            })
        }
    }
    impl Lifecycle for UnclaimedProbe {
        fn shutdown(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl Handler for UnclaimedProbe {
        fn maximum_concurrency(&self) -> usize {
            if self.queue == Queue::Dvr {
                20
            } else {
                1
            }
        }
        fn supports_selected_head(&self) -> bool {
            true
        }
        fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
            Some(self.advanced.clone())
        }
        fn run_once(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async { Err(Disposition::UnsupportedDomain) })
        }
        fn run_selected(
            &self,
            _expected: Arc<[u8]>,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async move {
                self.runs.fetch_add(1, Ordering::SeqCst);
                Ok(self.outcome.lock().unwrap().clone())
            })
        }
    }
    async fn settle_driver() {
        for _ in 0..128 {
            tokio::task::yield_now().await;
        }
    }
    /// Real production lane loops with counted broker Head reads, paused clock
    /// and immediate unclaimed handler completions. This reproduces the actual
    /// ETA hotloop rather than merely testing a cooldown function in isolation.
    #[tokio::test(start_paused = true)]
    async fn both_lanes_bound_actual_peeks_and_relaunches_for_unclaimed_heads() {
        for queue in [Queue::Celery, Queue::Dvr] {
            let task_name = if queue == Queue::Dvr {
                "apps.channels.tasks.run_recording"
            } else {
                "apps.channels.tasks.prefetch_recording_artwork"
            };
            let raw =
                serde_json::to_vec(&serde_json::json!({"headers":{"task":task_name,"id":"first"}}))
                    .unwrap();
            let probe = Arc::new(UnclaimedProbe {
                queue,
                raw: Mutex::new(raw),
                outcome: Mutex::new(Disposition::NotDue),
                peeks: AtomicUsize::new(0),
                runs: AtomicUsize::new(0),
                advanced: Arc::new(tokio::sync::Notify::new()),
            });
            let maximum = probe.maximum_concurrency();
            let handler = FamilyAdapter(Arc::new(OwnedFamily {
                inner: probe.clone(),
                slot: Arc::new(Semaphore::new(maximum)),
                closed: AtomicBool::new(false),
            }));
            let head: Arc<dyn Head> = probe.clone();
            let entries = milestone_dispatch::REQUIRED
                .into_iter()
                .chain([Family::BackupCreate, Family::BackupScheduled])
                .map(|family| (family, Arc::new(handler.clone()) as Arc<dyn Handler>))
                .collect();
            let lane = Arc::new(Lane {
                prepared: Prepared::new(head.clone(), entries).unwrap(),
                state: watch::channel(State::Starting).0,
                done: watch::channel(false).0,
            });
            let (stop, rx) = watch::channel(false);
            let poll = Duration::from_millis(100);
            let running = if queue == Queue::Dvr {
                tokio::spawn(dvr_lane(
                    lane.clone(),
                    head,
                    handler,
                    Arc::new(()),
                    rx,
                    poll,
                ))
            } else {
                let family = Family::Artwork;
                tokio::spawn(celery_lane(
                    lane.clone(),
                    head,
                    [(family, handler)].into_iter().collect(),
                    Arc::new(()),
                    rx,
                    poll,
                ))
            };
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 1);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 1);
            tokio::time::advance(Duration::from_millis(99)).await;
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 1);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 1);
            tokio::time::advance(Duration::from_millis(1)).await;
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 2);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 2);
            *probe.outcome.lock().unwrap() = Disposition::Capacity;
            tokio::time::advance(poll).await;
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 3);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 3);
            *probe.outcome.lock().unwrap() = Disposition::HeadChanged;
            tokio::time::advance(poll).await;
            settle_driver().await;
            // HeadChanged permits one fresh read, but an identical raw cannot
            // create another task/read loop at the same clock instant.
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 5);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 4);
            *probe.outcome.lock().unwrap() = Disposition::NotDue;
            tokio::time::advance(poll).await;
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 6);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 5);
            if queue == Queue::Dvr {
                // Genuine head advance hint follows the old raw's removal.
                // The changed next head progresses without waiting100ms.
                *probe.raw.lock().unwrap() = serde_json::to_vec(
                    &serde_json::json!({"headers":{"task":task_name,"id":"next"}}),
                )
                .unwrap();
                let before = tokio::time::Instant::now();
                probe.advanced.notify_one();
                settle_driver().await;
                assert_eq!(tokio::time::Instant::now(), before);
                assert_eq!(probe.peeks.load(Ordering::SeqCst), 7);
                assert_eq!(probe.runs.load(Ordering::SeqCst), 6);
            }
            stop.send_replace(true);
            running.await.unwrap();
            assert!(*lane.done.borrow());
            assert!(matches!(*lane.state.borrow(), State::Stopped));
        }
    }
}
