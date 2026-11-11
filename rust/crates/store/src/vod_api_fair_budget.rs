//! Private shared VOD budget prototype; no route integration or acceptance yet.
//! Original Dispatcharr authors retain all application/algorithm credit.
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Need {
    pub bytes: usize,
    pub queued_bytes: usize,
    pub workers: usize,
    pub http: usize,
    pub upstream: usize,
    pub continuation: usize,
}
impl Need {
    fn add(self, b: Self) -> Option<Self> {
        Some(Self {
            bytes: self.bytes.checked_add(b.bytes)?,
            queued_bytes: self.queued_bytes.checked_add(b.queued_bytes)?,
            workers: self.workers.checked_add(b.workers)?,
            http: self.http.checked_add(b.http)?,
            upstream: self.upstream.checked_add(b.upstream)?,
            continuation: self.continuation.checked_add(b.continuation)?,
        })
    }
    fn sub(self, b: Self) -> Option<Self> {
        Some(Self {
            bytes: self.bytes.checked_sub(b.bytes)?,
            queued_bytes: self.queued_bytes.checked_sub(b.queued_bytes)?,
            workers: self.workers.checked_sub(b.workers)?,
            http: self.http.checked_sub(b.http)?,
            upstream: self.upstream.checked_sub(b.upstream)?,
            continuation: self.continuation.checked_sub(b.continuation)?,
        })
    }
    fn fits(self, cap: Self) -> bool {
        self.queued_bytes <= self.bytes
            && self.bytes <= cap.bytes
            && self.queued_bytes <= cap.queued_bytes
            && self.workers <= cap.workers
            && self.http <= cap.http
            && self.upstream <= cap.upstream
            && self.continuation <= cap.continuation
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Error {
    Capacity,
    Closed,
    Deadline,
    WrongLedger,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Admission {
    New,
    ExistingContinuation,
}
/// Reserve a complete ordinary lane plus one potential held frame after two
/// full-work upstream waits. All cheap inputs/snapshots share this byte ceiling.
pub(super) fn queue_capacity(
    memory: usize,
    work: usize,
    response: usize,
    input: usize,
) -> Result<usize, Error> {
    let reserved = work
        .checked_mul(3)
        .and_then(|v| response.checked_mul(2).and_then(|n| v.checked_add(n)))
        .and_then(|v| input.checked_mul(4).and_then(|n| v.checked_add(n)))
        .ok_or(Error::Capacity)?;
    memory
        .checked_sub(reserved)
        .map(|n| n.min(16 * 1024 * 1024))
        .filter(|n| *n >= input)
        .ok_or(Error::Capacity)
}
struct Waiting {
    id: u64,
    need: Need,
    admission: Admission,
    send: oneshot::Sender<Result<Grant, Error>>,
}
struct State {
    used: Need,
    next: u64,
    closing: bool,
    waiting: VecDeque<Waiting>,
}
struct Inner {
    cap: Need,
    maximum_queue: usize,
    state: Mutex<State>,
}
#[derive(Clone)]
pub(super) struct Budget {
    inner: Arc<Inner>,
}
pub(super) struct Grant {
    inner: Option<Arc<Inner>>,
    need: Need,
}
impl std::fmt::Debug for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grant")
            .field("need", &self.need)
            .finish_non_exhaustive()
    }
}
struct Pending {
    inner: Arc<Inner>,
    id: u64,
    receive: oneshot::Receiver<Result<Grant, Error>>,
}
impl Drop for Pending {
    fn drop(&mut self) {
        let waiting = {
            let mut state = self.inner.state.lock().expect("budget state");
            state
                .waiting
                .iter()
                .position(|row| row.id == self.id)
                .and_then(|i| state.waiting.remove(i))
        };
        drop(waiting);
        drive(&self.inner);
    }
}
impl Budget {
    pub(super) fn new(cap: Need, maximum_queue: usize) -> Result<Self, Error> {
        if cap.queued_bytes > cap.bytes
            || cap.bytes == 0
            || cap.bytes > 512 * 1024 * 1024
            || cap.workers == 0
            || cap.workers > 16
            || cap.http == 0
            || cap.http > 16
            || cap.upstream > 2
            || cap.continuation != usize::from(cap.upstream > 0)
            || maximum_queue == 0
            || maximum_queue > 256
        {
            return Err(Error::Capacity);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                cap,
                maximum_queue,
                state: Mutex::new(State {
                    used: Need::default(),
                    next: 1,
                    closing: false,
                    waiting: VecDeque::new(),
                }),
            }),
        })
    }
    pub(super) fn same(&self, grant: &Grant) -> bool {
        grant
            .inner
            .as_ref()
            .is_some_and(|inner| Arc::ptr_eq(inner, &self.inner))
    }
    pub(super) fn capacity(&self) -> Need {
        self.inner.cap
    }
    pub(super) fn used(&self) -> Need {
        self.inner.state.lock().expect("budget state").used
    }
    pub(super) fn is_closing(&self) -> bool {
        self.inner.state.lock().expect("budget state").closing
    }
    #[cfg(test)]
    pub(super) fn test_pending_counts(&self) -> [usize; 3] {
        let state = self.inner.state.lock().expect("budget state");
        let mut counts = [0; 3];
        for row in &state.waiting {
            match row.admission {
                Admission::ExistingContinuation => counts[2] += 1,
                Admission::New if row.need.upstream > 0 => counts[0] += 1,
                Admission::New if row.need.http > 0 => counts[1] += 1,
                _ => {}
            }
        }
        counts
    }
    pub(super) fn queued(&self) -> usize {
        self.inner.state.lock().expect("budget state").waiting.len()
    }
    pub(super) fn try_claim(&self, need: Need) -> Result<Grant, Error> {
        if !need.fits(self.inner.cap) {
            return Err(Error::Capacity);
        }
        drive(&self.inner);
        let mut state = self.inner.state.lock().expect("budget state");
        if state.closing {
            return Err(Error::Closed);
        }
        let total = state
            .used
            .add(need)
            .filter(|n| n.fits(self.inner.cap))
            .ok_or(Error::Capacity)?;
        // Existing eligible waiters are driven first; a non-fitting waiter does not
        // reserve half a grant or prevent a smaller eligible continuation progressing.
        state.used = total;
        Ok(Grant {
            inner: Some(self.inner.clone()),
            need,
        })
    }
    pub(super) async fn acquire(
        &self,
        need: Need,
        admission: Admission,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Grant, Error> {
        if !need.fits(self.inner.cap) {
            return Err(Error::Capacity);
        }
        if deadline.is_some_and(|end| tokio::time::Instant::now() >= end) {
            return Err(Error::Deadline);
        }
        let (receive, id) = {
            let mut state = self.inner.state.lock().expect("budget state");
            if state.closing && admission == Admission::New {
                return Err(Error::Closed);
            }
            let same = state
                .waiting
                .iter()
                .filter(|row| row.admission == admission)
                .count();
            let maximum = if admission == Admission::New {
                self.inner.maximum_queue
            } else {
                self.inner.cap.upstream
            };
            if same == maximum {
                return Err(Error::Capacity);
            }
            let id = state.next;
            state.next = state.next.checked_add(1).ok_or(Error::Capacity)?;
            let (send, receive) = oneshot::channel();
            state.waiting.push_back(Waiting {
                id,
                need,
                admission,
                send,
            });
            (receive, id)
        };
        let mut pending = Pending {
            inner: self.inner.clone(),
            id,
            receive,
        };
        drive(&self.inner);
        let result = match deadline {
            Some(end) => tokio::time::timeout_at(end, &mut pending.receive)
                .await
                .map_err(|_| Error::Deadline)?,
            None => (&mut pending.receive).await,
        };
        let grant = result.map_err(|_| Error::Closed)??;
        if deadline.is_some_and(|end| tokio::time::Instant::now() >= end) {
            return Err(Error::Deadline);
        }
        Ok(grant)
    }
    pub(super) fn close_new(&self) {
        let removed = {
            let mut state = self.inner.state.lock().expect("budget state");
            state.closing = true;
            let mut removed = Vec::new();
            let mut keep = VecDeque::new();
            while let Some(waiting) = state.waiting.pop_front() {
                if waiting.admission == Admission::New {
                    removed.push(waiting)
                } else {
                    keep.push_back(waiting)
                }
            }
            state.waiting = keep;
            removed
        };
        for waiting in removed {
            let _ = waiting.send.send(Err(Error::Closed));
        }
        drive(&self.inner);
    }
    /// Atomic same-ledger replacement. Snapshot freeze only shrinks; admission /
    /// resume wait for a complete grant instead of reserving dimensions piecemeal.
    pub(super) fn replace(&self, grant: &mut Grant, need: Need) -> Result<(), Error> {
        if !self.same(grant) {
            return Err(Error::WrongLedger);
        }
        if !need.fits(self.inner.cap) {
            return Err(Error::Capacity);
        }
        let mut state = self.inner.state.lock().expect("budget state");
        let without = state.used.sub(grant.need).expect("owned grant accounting");
        let total = without
            .add(need)
            .filter(|n| n.fits(self.inner.cap))
            .ok_or(Error::Capacity)?;
        if state.closing && !need.fits(grant.need) {
            return Err(Error::Closed);
        }
        state.used = total;
        grant.need = need;
        drop(state);
        drive(&self.inner);
        Ok(())
    }
}
impl Grant {
    pub(super) fn need(&self) -> Need {
        self.need
    }
    /// Pure ownership split: no new budget, no double count, no asynchronous wait.
    pub(super) fn split(&mut self, part: Need) -> Result<Grant, Error> {
        if part.queued_bytes > part.bytes {
            return Err(Error::Capacity);
        }
        let remaining = self.need.sub(part).ok_or(Error::Capacity)?;
        if remaining.queued_bytes > remaining.bytes {
            return Err(Error::Capacity);
        }
        let inner = self.inner.as_ref().ok_or(Error::Closed)?.clone();
        self.need = remaining;
        Ok(Grant {
            inner: Some(inner),
            need: part,
        })
    }
    pub(super) fn combine(&mut self, other: &mut Grant) -> Result<(), Error> {
        let own = self.inner.as_ref().ok_or(Error::Closed)?;
        let incoming = other.inner.as_ref().ok_or(Error::Closed)?;
        if !Arc::ptr_eq(own, incoming) {
            return Err(Error::WrongLedger);
        }
        let sum = self.need.add(other.need).ok_or(Error::Capacity)?;
        self.need = sum;
        other.need = Need::default();
        other.inner = None;
        Ok(())
    }
    fn release_without_drive(&mut self) {
        if let Some(inner) = self.inner.take() {
            let mut state = inner.state.lock().expect("budget state");
            state.used = state.used.sub(self.need).expect("owned grant accounting");
        }
    }
}
impl Drop for Grant {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            {
                let mut state = inner.state.lock().expect("budget state");
                state.used = state.used.sub(self.need).expect("owned grant accounting");
            }
            drive(&inner);
        }
    }
}
fn drive(inner: &Arc<Inner>) {
    loop {
        let delivery = {
            let mut state = inner.state.lock().expect("budget state");
            let at = state
                .waiting
                .iter()
                .position(|row| state.used.add(row.need).is_some_and(|n| n.fits(inner.cap)));
            let Some(at) = at else { return };
            let row = state.waiting.remove(at).expect("queued waiter");
            state.used = state.used.add(row.need).expect("checked grant");
            let grant = Grant {
                inner: Some(inner.clone()),
                need: row.need,
            };
            (row.send, grant)
        };
        // Never deliver while holding the accounting mutex. A cancelled receiver
        // returns its entire grant iteratively, rather than recursive Drop delivery.
        if let Err(Ok(mut grant)) = delivery.0.send(Ok(delivery.1)) {
            grant.release_without_drive();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn budget() -> Budget {
        Budget::new(
            Need {
                bytes: 192,
                queued_bytes: 4,
                workers: 2,
                http: 2,
                upstream: 2,
                continuation: 1,
            },
            8,
        )
        .unwrap()
    }
    fn memory(bytes: usize) -> Need {
        Need {
            bytes,
            ..Need::default()
        }
    }
    async fn queued(b: &Budget, count: usize) {
        for _ in 0..100 {
            if b.queued() == count {
                return;
            }
            tokio::task::yield_now().await
        }
        panic!("waiter not queued")
    }
    #[tokio::test]
    async fn two_paused_work_claims_leave_ordinary_capacity() {
        let b = budget();
        let a = b
            .try_claim(Need {
                bytes: 52,
                upstream: 1,
                ..Need::default()
            })
            .unwrap();
        let c = b
            .try_claim(Need {
                bytes: 52,
                upstream: 1,
                ..Need::default()
            })
            .unwrap();
        let ordinary = b
            .acquire(
                Need {
                    bytes: 68,
                    workers: 1,
                    http: 1,
                    ..Need::default()
                },
                Admission::New,
                None,
            )
            .await
            .unwrap();
        assert_eq!(b.used().bytes, 172);
        assert_eq!(b.used().upstream, 2);
        drop((a, c, ordinary));
        assert_eq!(b.used(), Need::default());
    }
    #[tokio::test]
    async fn nonfitting_ordinary_cannot_deadlock_eligible_resume() {
        let b = budget();
        let used = b.try_claim(memory(172)).unwrap();
        let own = b.clone();
        let large =
            tokio::spawn(async move { own.acquire(memory(68), Admission::New, None).await });
        queued(&b, 1).await;
        let resume = b
            .acquire(
                Need {
                    bytes: 16,
                    workers: 1,
                    http: 1,
                    continuation: 1,
                    ..Need::default()
                },
                Admission::ExistingContinuation,
                None,
            )
            .await
            .unwrap();
        assert_eq!(b.used().bytes, 188);
        drop(resume);
        large.abort();
        let _ = large.await;
        drop(used);
        assert_eq!(b.used(), Need::default());
    }
    #[tokio::test]
    async fn oldest_eligible_is_granted_before_newer() {
        let b = budget();
        let mut used = b.try_claim(memory(192)).unwrap();
        let own = b.clone();
        let first =
            tokio::spawn(async move { own.acquire(memory(16), Admission::New, None).await });
        queued(&b, 1).await;
        let own = b.clone();
        let second =
            tokio::spawn(async move { own.acquire(memory(16), Admission::New, None).await });
        queued(&b, 2).await;
        let available = used.split(memory(16)).unwrap();
        drop(available);
        let a = first.await.unwrap().unwrap();
        assert_eq!(b.queued(), 1);
        assert_eq!(b.used().bytes, 192);
        drop(a);
        let c = second.await.unwrap().unwrap();
        drop((used, c));
        assert_eq!(b.used(), Need::default());
    }

    #[tokio::test]
    async fn cancel_removes_waiter_without_partial_credit() {
        let b = budget();
        let full = b.try_claim(memory(192)).unwrap();
        let own = b.clone();
        let waiting = tokio::spawn(async move {
            own.acquire(
                Need {
                    bytes: 16,
                    workers: 1,
                    http: 1,
                    continuation: 1,
                    ..Need::default()
                },
                Admission::New,
                None,
            )
            .await
        });
        queued(&b, 1).await;
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(b.queued(), 0);
        assert_eq!(b.used(), memory(192));
        drop(full);
        assert_eq!(b.used(), Need::default());
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_does_not_leak_queue_or_resources() {
        let b = budget();
        let full = b.try_claim(memory(192)).unwrap();
        let result = b
            .acquire(
                memory(1),
                Admission::New,
                Some(tokio::time::Instant::now() + std::time::Duration::from_secs(1)),
            )
            .await;
        assert!(matches!(result, Err(Error::Deadline)));
        assert_eq!(b.queued(), 0);
        drop(full);
    }
    #[tokio::test]
    async fn closing_cancels_new_but_allows_existing_physical_continuation() {
        let b = budget();
        let full = b.try_claim(memory(192)).unwrap();
        let own = b.clone();
        let new = tokio::spawn(async move { own.acquire(memory(16), Admission::New, None).await });
        queued(&b, 1).await;
        let own = b.clone();
        let resume = tokio::spawn(async move {
            own.acquire(memory(16), Admission::ExistingContinuation, None)
                .await
        });
        queued(&b, 2).await;
        b.close_new();
        assert!(matches!(new.await.unwrap(), Err(Error::Closed)));
        drop(full);
        let done = resume.await.unwrap().unwrap();
        drop(done);
        assert_eq!(b.used(), Need::default());
    }
    #[test]
    fn ownership_split_and_wrong_ledger_are_checked() {
        let b = budget();
        let other = budget();
        let mut all = b.try_claim(memory(68)).unwrap();
        assert_eq!(other.replace(&mut all, memory(16)), Err(Error::WrongLedger));
        let frame = all.split(memory(16)).unwrap();
        assert_eq!(b.used().bytes, 68);
        b.replace(&mut all, memory(2)).unwrap();
        assert_eq!(b.used().bytes, 18);
        drop((all, frame));
        assert_eq!(b.used(), Need::default());
    }
    #[test]
    fn cheap_queue_leaves_ordinary_room_with_held_frame() {
        let b = budget();
        let paused = b
            .try_claim(Need {
                bytes: 104,
                upstream: 2,
                ..Need::default()
            })
            .unwrap();
        let frame = b
            .try_claim(Need {
                bytes: 16,
                http: 1,
                ..Need::default()
            })
            .unwrap();
        let cheap = b
            .try_claim(Need {
                bytes: 4,
                queued_bytes: 4,
                ..Need::default()
            })
            .unwrap();
        let ordinary = b
            .try_claim(Need {
                bytes: 68,
                workers: 1,
                http: 1,
                ..Need::default()
            })
            .unwrap();
        assert_eq!(b.used().bytes, 192);
        drop((paused, frame, cheap, ordinary));
        assert_eq!(b.used(), Need::default());
    }
    #[test]
    fn oversized_third_snapshot_does_not_take_ordinary_headroom() {
        let b = budget();
        let paused = b
            .try_claim(Need {
                bytes: 104,
                upstream: 2,
                ..Need::default()
            })
            .unwrap();
        let mut preparing = b
            .try_claim(Need {
                bytes: 68,
                workers: 1,
                http: 1,
                ..Need::default()
            })
            .unwrap();
        let cap = b.capacity().queued_bytes;
        let needed = 8;
        assert_eq!(
            b.replace(
                &mut preparing,
                Need {
                    bytes: needed,
                    queued_bytes: needed,
                    ..Need::default()
                }
            ),
            Err(Error::Capacity)
        );
        assert_eq!(cap, 4);
        assert_eq!(preparing.need().bytes, 68);
        drop(preparing);
        let ordinary = b
            .try_claim(Need {
                bytes: 68,
                workers: 1,
                http: 1,
                ..Need::default()
            })
            .unwrap();
        drop((ordinary, paused));
    }
    #[test]
    fn failed_atomic_replace_keeps_old_claim() {
        let b = budget();
        let mut a = b.try_claim(memory(52)).unwrap();
        let _c = b.try_claim(memory(130)).unwrap();
        assert_eq!(b.replace(&mut a, memory(68)), Err(Error::Capacity));
        assert_eq!(a.need().bytes, 52);
        assert_eq!(b.used().bytes, 182);
    }
    #[test]
    fn queued_cap_reserves_full_hot_state() {
        assert_eq!(
            queue_capacity(
                192 * 1024 * 1024,
                52 * 1024 * 1024,
                16 * 1024 * 1024,
                64 * 1024
            ),
            Ok(4 * 1024 * 1024 - 4 * 64 * 1024)
        );
        assert_eq!(queue_capacity(128, 52, 16, 1), Err(Error::Capacity));
    }
    #[test]
    fn wrong_ledger_combine_keeps_both_owners() {
        let b = budget();
        let other = budget();
        let mut a = b.try_claim(memory(52)).unwrap();
        let mut c = other.try_claim(memory(16)).unwrap();
        assert_eq!(a.combine(&mut c), Err(Error::WrongLedger));
        assert_eq!(b.used().bytes, 52);
        assert_eq!(other.used().bytes, 16);
        let mut input = b
            .try_claim(Need {
                bytes: 2,
                queued_bytes: 2,
                ..Need::default()
            })
            .unwrap();
        a.combine(&mut input).unwrap();
        assert_eq!(a.need().bytes, 54);
        drop((a, input, c));
        assert_eq!(b.used(), Need::default());
        assert_eq!(other.used(), Need::default());
    }
    #[tokio::test]
    async fn receiver_abort_after_grant_returns_every_dimension() {
        let b = budget();
        let full = b.try_claim(memory(192)).unwrap();
        let own = b.clone();
        let waiting = tokio::spawn(async move {
            own.acquire(
                Need {
                    bytes: 16,
                    workers: 1,
                    http: 1,
                    continuation: 1,
                    ..Need::default()
                },
                Admission::New,
                None,
            )
            .await
        });
        queued(&b, 1).await;
        drop(full);
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(b.used(), Need::default());
        assert_eq!(b.queued(), 0);
    }

    #[test]
    fn ordinary_only_ledger_does_not_require_refresh_headroom() {
        let b = Budget::new(
            Need {
                bytes: 68 * 1024 * 1024,
                queued_bytes: 64 * 1024,
                workers: 1,
                http: 1,
                upstream: 0,
                continuation: 0,
            },
            8,
        )
        .unwrap();
        let ordinary = b
            .try_claim(Need {
                bytes: 68 * 1024 * 1024,
                workers: 1,
                http: 1,
                ..Need::default()
            })
            .unwrap();
        assert!(matches!(
            b.try_claim(Need {
                upstream: 1,
                ..Need::default()
            }),
            Err(Error::Capacity)
        ));
        assert_eq!(
            queue_capacity(
                68 * 1024 * 1024,
                52 * 1024 * 1024,
                16 * 1024 * 1024,
                64 * 1024
            ),
            Err(Error::Capacity)
        );
        drop(ordinary);
        assert_eq!(b.used(), Need::default());
    }

    #[test]
    fn split_cannot_orphan_queued_bytes_from_their_memory() {
        let b = budget();
        let mut g = b
            .try_claim(Need {
                bytes: 4,
                queued_bytes: 4,
                ..Need::default()
            })
            .unwrap();
        assert!(matches!(g.split(memory(4)), Err(Error::Capacity)));
        assert_eq!(
            g.need(),
            Need {
                bytes: 4,
                queued_bytes: 4,
                ..Need::default()
            }
        );
        drop(g);
        assert_eq!(b.used(), Need::default());
    }
}
