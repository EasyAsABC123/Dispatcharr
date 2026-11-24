//! A local command slot for one warm TS producer. Original application credit
//! belongs to Dispatcharr's authors.
use dispatcharr_contracts::{PlaybackCandidate, PlaybackError};
use std::sync::Mutex;
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

pub(super) struct Request {
    pub target: Source,
    reply: oneshot::Sender<Receipt>,
}

enum State {
    Unavailable,
    Ready(Source),
    Queued(Source, Request),
    Switching,
}

pub(super) struct Control {
    state: Mutex<State>,
    queued: Notify,
}

impl Control {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::Unavailable),
            queued: Notify::new(),
        }
    }

    /// Only the producer can open admission, after publishing media from its
    /// owned, supported source.
    pub fn arm(&self, current: Source) {
        let mut state = self.state.lock().unwrap();
        if matches!(*state, State::Unavailable) {
            *state = State::Ready(current);
        }
    }

    pub fn submit(
        &self,
        current: Source,
        target: Source,
    ) -> Result<oneshot::Receiver<Receipt>, PlaybackError> {
        let mut state = self.state.lock().unwrap();
        if !matches!(*state, State::Ready(ready) if ready == current) {
            return Err(PlaybackError::new(
                409,
                "Local source switch is unavailable",
            ));
        }
        let (reply, result) = oneshot::channel();
        *state = State::Queued(current, Request { target, reply });
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

    pub fn confirm(&self, request: Request, current: Source) {
        let mut state = self.state.lock().unwrap();
        *state = State::Ready(current);
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
        control.arm(primary);
        assert!(control.submit(alternate, primary).is_err());
        let result = control.submit(primary, alternate).unwrap();
        assert!(control.submit(primary, alternate).is_err());
        let request = control.receive().await;
        assert_eq!(request.target, alternate);
        control.disarm();
        control.arm(alternate);
        assert!(control.submit(alternate, primary).is_err());
        control.confirm(request, alternate);
        assert!(result.await.unwrap().is_ok());
        let result = control.submit(alternate, primary).unwrap();
        control.disarm();
        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn cancelled_local_source_switch_disarms_without_changing_target() {
        let control = Control::new();
        let primary = (5, None);
        let alternate = (8, None);
        control.arm(primary);
        let reply = control.submit(primary, alternate).unwrap();
        drop(reply);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), control.receive())
                .await
                .is_err()
        );
        assert!(control.submit(primary, alternate).is_ok());
    }
}
