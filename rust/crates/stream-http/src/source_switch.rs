//! A local command slot for one warm TS producer. Original application credit
//! belongs to Dispatcharr's authors.
use dispatcharr_contracts::{PlaybackCandidate, PlaybackError};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use tokio::sync::{oneshot, Notify};

pub(super) type Source = (i64, Option<i64>);
type Receipt = Result<(), PlaybackError>;

pub(super) fn source(candidate: &PlaybackCandidate) -> Source {
    (candidate.stream_id, candidate.provider_profile_id)
}

pub(super) fn supported(candidate: &PlaybackCandidate) -> bool {
    candidate.input_command.is_none()
        && candidate.credential_pool.is_none()
        && (candidate.provider_max_streams == 0
            || (candidate.provider_max_streams == 1 && candidate.provider_profile_id.is_some()))
        && reqwest::Url::parse(&candidate.url)
            .is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
}

pub(super) fn compatible(current: &PlaybackCandidate, target: &PlaybackCandidate) -> bool {
    supported(current)
        && supported(target)
        && target.provider_profile_id == current.provider_profile_id
        && target.provider_max_streams == current.provider_max_streams
        && target.stream_id != current.stream_id
        && target.url != current.url
}

/// Only the signed assigned-stream path may transfer an owned unlimited slot.
pub(super) fn compatible_unlimited_transfer(
    current: &PlaybackCandidate,
    target: &PlaybackCandidate,
) -> bool {
    supported(current)
        && supported(target)
        && current.provider_max_streams == 0
        && target.provider_max_streams == 0
        && current
            .provider_profile_id
            .is_some_and(|profile| profile > 0)
        && target
            .provider_profile_id
            .is_some_and(|profile| profile > 0)
        && current.provider_profile_id != target.provider_profile_id
        && current.stream_id != target.stream_id
        && current.url != target.url
}

#[derive(Clone, Copy)]
struct ReadySource {
    source: Source,
    owns_published_slot: bool,
}

pub(super) struct CurrentUrlEdit {
    pub previous: String,
    pub replacement: String,
}

impl CurrentUrlEdit {
    pub fn matches(&self, candidate: &PlaybackCandidate) -> bool {
        candidate.provider_max_streams == 0
            && candidate
                .provider_profile_id
                .is_some_and(|profile| profile > 0)
            && supported(candidate)
            && candidate.url == self.previous
            && self.previous != self.replacement
            && reqwest::Url::parse(&self.replacement)
                .is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
    }
}

pub(super) struct Request {
    pub target: Source,
    pub current_url_edit: Option<CurrentUrlEdit>,
    reply: oneshot::Sender<Receipt>,
}

enum State {
    Unavailable,
    Ready(ReadySource),
    Queued(ReadySource, Request),
    Switching,
}

pub(super) struct Control {
    state: Mutex<State>,
    queued: Notify,
    last_confirmed_at: AtomicU64,
    last_confirmed_edit_at: AtomicU64,
}

impl Control {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::Unavailable),
            queued: Notify::new(),
            last_confirmed_at: AtomicU64::new(0),
            last_confirmed_edit_at: AtomicU64::new(0),
        }
    }

    /// Only the producer can open admission, after publishing media from its
    /// owned, supported source.
    pub fn arm(&self, current: Source, owns_published_slot: bool) {
        let mut state = self.state.lock().unwrap();
        if matches!(*state, State::Unavailable) {
            *state = State::Ready(ReadySource {
                source: current,
                owns_published_slot,
            });
        }
    }

    pub fn submit(
        &self,
        current: Source,
        target: Source,
    ) -> Result<oneshot::Receiver<Receipt>, PlaybackError> {
        self.submit_with_owner(current, target, false)
    }

    pub fn submit_with_owner(
        &self,
        current: Source,
        target: Source,
        require_owned: bool,
    ) -> Result<oneshot::Receiver<Receipt>, PlaybackError> {
        self.enqueue(current, target, require_owned, None)
    }

    pub fn submit_owned_url_edit(
        &self,
        current: Source,
        edit: CurrentUrlEdit,
    ) -> Result<oneshot::Receiver<Receipt>, PlaybackError> {
        self.enqueue(current, current, true, Some(edit))
    }

    fn enqueue(
        &self,
        current: Source,
        target: Source,
        require_owned: bool,
        current_url_edit: Option<CurrentUrlEdit>,
    ) -> Result<oneshot::Receiver<Receipt>, PlaybackError> {
        let mut state = self.state.lock().unwrap();
        let State::Ready(ready) = *state else {
            return Err(PlaybackError::new(
                409,
                "Local source switch is unavailable",
            ));
        };
        if ready.source != current
            || ((require_owned || current.1 != target.1) && !ready.owns_published_slot)
        {
            return Err(PlaybackError::new(
                409,
                "Local source switch is unavailable",
            ));
        }
        let (reply, result) = oneshot::channel();
        *state = State::Queued(
            ready,
            Request {
                target,
                current_url_edit,
                reply,
            },
        );
        self.queued.notify_one();
        Ok(result)
    }

    pub async fn receive(&self) -> Request {
        loop {
            let wake = self.queued.notified();
            {
                let mut state = self.state.lock().unwrap();
                if matches!(*state, State::Queued(_, _)) {
                    let State::Queued(current, request) =
                        std::mem::replace(&mut *state, State::Switching)
                    else {
                        unreachable!()
                    };
                    if !request.reply.is_closed() {
                        return request;
                    }
                    *state = State::Ready(current);
                }
            }
            wake.await;
        }
    }

    /// Close admission before draining the old attempt, including a normal
    /// fault before the command could be consumed.
    pub fn disarm(&self) {
        let mut state = self.state.lock().unwrap();
        if !matches!(*state, State::Switching) {
            *state = State::Unavailable;
        }
    }

    pub fn last_confirmed_at(&self) -> Option<f64> {
        let at = self.last_confirmed_at.load(Ordering::Acquire);
        (at != 0).then(|| f64::from_bits(at))
    }

    pub fn last_confirmed_edit_at(&self) -> Option<f64> {
        let at = self.last_confirmed_edit_at.load(Ordering::Acquire);
        (at != 0).then(|| f64::from_bits(at))
    }

    pub fn reject_active(
        &self,
        request: Request,
        current: Source,
        owns_published_slot: bool,
        error: PlaybackError,
    ) {
        *self.state.lock().unwrap() = State::Ready(ReadySource {
            source: current,
            owns_published_slot,
        });
        let _ = request.reply.send(Err(error));
    }

    pub fn confirm(&self, request: Request, current: Source, owns_published_slot: bool, at: f64) {
        let mut state = self.state.lock().unwrap();
        if request.current_url_edit.is_some() {
            self.last_confirmed_edit_at
                .store(at.to_bits(), Ordering::Release);
        }
        self.last_confirmed_at
            .store(at.to_bits(), Ordering::Release);
        *state = State::Ready(ReadySource {
            source: current,
            owns_published_slot,
        });
        let _ = request.reply.send(Ok(()));
    }

    pub fn fail(&self, request: Request, error: PlaybackError) {
        let mut state = self.state.lock().unwrap();
        *state = State::Unavailable;
        let _ = request.reply.send(Err(error));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn local_source_switch_keeps_one_ticket_until_owner_acknowledges() {
        let control = Control::new();
        let primary = (5, Some(3));
        let alternate = (8, Some(3));
        assert!(control.submit(primary, alternate).is_err());
        control.arm(primary, false);
        assert!(control.submit(alternate, primary).is_err());
        let result = control.submit(primary, alternate).unwrap();
        assert!(control.submit(primary, alternate).is_err());
        let request = control.receive().await;
        assert_eq!(request.target, alternate);
        control.disarm();
        control.arm(alternate, false);
        assert!(control.submit(alternate, primary).is_err());
        assert_eq!(control.last_confirmed_at(), None);
        control.confirm(request, alternate, false, 100.25);
        assert!(result.await.unwrap().is_ok());
        assert_eq!(control.last_confirmed_at(), Some(100.25));
        let result = control.submit(alternate, primary).unwrap();
        control.disarm();
        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn cancelled_local_source_switch_disarms_without_changing_target() {
        let control = Control::new();
        let primary = (5, None);
        let alternate = (8, None);
        control.arm(primary, false);
        let reply = control.submit(primary, alternate).unwrap();
        drop(reply);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), control.receive())
                .await
                .is_err()
        );
        assert!(control.submit(primary, alternate).is_ok());
    }

    #[tokio::test]
    async fn remote_same_profile_ticket_requires_an_owned_published_slot() {
        let control = Control::new();
        let primary = (5, Some(3));
        let alternate = (8, Some(3));
        control.arm(primary, false);
        assert!(control.submit_with_owner(primary, alternate, true).is_err());
        assert!(control.submit(primary, alternate).is_ok());
        control.disarm();
        control.arm(primary, true);
        let receipt = control.submit_with_owner(primary, alternate, true).unwrap();
        assert!(control.submit(primary, alternate).is_err());
        let request = control.receive().await;
        assert_eq!(request.target, alternate);
        control.confirm(request, alternate, true, 100.25);
        assert!(receipt.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn cross_profile_ticket_requires_published_ownership_on_each_source() {
        let control = Control::new();
        let primary = (5, Some(3));
        let alternate = (8, Some(4));
        control.arm(primary, false);
        assert!(control.submit(primary, alternate).is_err());
        control.disarm();
        control.arm(primary, true);
        let cancelled = control.submit(primary, alternate).unwrap();
        drop(cancelled);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), control.receive())
                .await
                .is_err()
        );
        let reply = control.submit(primary, alternate).unwrap();
        let request = control.receive().await;
        assert_eq!(request.target, alternate);
        control.confirm(request, alternate, false, 100.25);
        assert!(reply.await.unwrap().is_ok());
        assert!(control.submit(alternate, primary).is_err());
    }

    #[tokio::test]
    async fn owned_url_edit_requires_owner_and_media_and_preserves_active_on_rejection() {
        let control = Control::new();
        let source = (5, Some(7));
        let edit = || CurrentUrlEdit {
            previous: "http://owned-origin/old.ts".into(),
            replacement: "http://owned-origin/new.ts".into(),
        };
        control.arm(source, false);
        assert!(control.submit_owned_url_edit(source, edit()).is_err());
        control.disarm();
        control.arm(source, true);

        let failed = control.submit_owned_url_edit(source, edit()).unwrap();
        let request = control.receive().await;
        assert_eq!(request.target, source);
        assert!(request.current_url_edit.is_some());
        control.reject_active(
            request,
            source,
            true,
            PlaybackError::new(409, "old input changed"),
        );
        assert_eq!(failed.await.unwrap().unwrap_err().status, 409);
        assert_eq!(control.last_confirmed_at(), None);

        let mut applied = control.submit_owned_url_edit(source, edit()).unwrap();
        let request = control.receive().await;
        assert!(applied.try_recv().is_err());
        assert!(control.submit(source, source).is_err());
        control.confirm(request, source, true, 21.5);
        assert!(applied.await.unwrap().is_ok());
        assert_eq!(control.last_confirmed_edit_at(), Some(21.5));
        let repeat = control.submit(source, source).unwrap();
        let request = control.receive().await;
        control.confirm(request, source, true, 22.0);
        assert!(repeat.await.unwrap().is_ok());
        assert_eq!(control.last_confirmed_at(), Some(22.0));
        assert_eq!(control.last_confirmed_edit_at(), Some(21.5));
    }
}
