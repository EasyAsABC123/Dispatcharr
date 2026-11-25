//! Original Dispatcharr VOD provider handoff semantics. See ATTRIBUTION.md.
use reqwest::{StatusCode, Url};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

// Keep one local URL per owned initialized session. Oversized URLs retain the
// ordinary origin path. Never recover a cached destination from public metadata.
const MAX_CACHED_URL_BYTES: usize = 8192;

enum State {
    Unseen,
    Ready(Arc<Url>),
    Disabled,
}

pub(super) struct FinalProviderUrl(Mutex<State>);

impl FinalProviderUrl {
    pub(super) fn new(enabled: bool) -> Self {
        Self(Mutex::new(if enabled {
            State::Unseen
        } else {
            State::Disabled
        }))
    }

    pub(super) fn remember(&self, url: &Url) {
        let mut state = self.0.lock().unwrap();
        if matches!(*state, State::Unseen) {
            *state = if url.as_str().len() <= MAX_CACHED_URL_BYTES {
                State::Ready(Arc::new(url.clone()))
            } else {
                State::Disabled
            };
        }
    }

    pub(super) async fn send<T, E, F>(
        &self,
        original: impl Future<Output = Result<(StatusCode, T), E>>,
        cached: impl FnOnce(Arc<Url>) -> F,
    ) -> Result<T, E>
    where
        F: Future<Output = Result<(StatusCode, T), E>>,
    {
        let target = match &*self.0.lock().unwrap() {
            State::Ready(url) => Some(url.clone()),
            _ => None,
        };
        if let Some(target) = target {
            let (status, response) = cached(target).await?;
            if !status.is_client_error() && !status.is_server_error() {
                return Ok(response);
            }
            // Source closes the cached response before retrying. The clear
            // reaches persistent session state only if the fallback succeeds.
            drop(response);
            let (status, response) = original.await?;
            if !status.is_client_error() && !status.is_server_error() {
                *self.0.lock().unwrap() = State::Disabled;
            }
            return Ok(response);
        }
        original.await.map(|(_, response)| response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, future::ready};

    fn url(path: &str) -> Url {
        Url::parse(&format!("http://provider.invalid/{path}")).unwrap()
    }

    #[tokio::test]
    async fn initialized_session_uses_only_its_first_cached_hop() {
        let cache = FinalProviderUrl::new(true);
        let own = url("fixture-cache");
        cache.remember(&own);
        cache.remember(&url("later-destination"));
        let result = cache
            .send(async { panic!("polled the original request") }, |target| {
                assert_eq!(target.as_ref(), &own);
                // A redirect is returned from the no-redirect cached hop.
                ready(Ok::<_, ()>((StatusCode::FOUND, "cached")))
            })
            .await;
        assert_eq!(result, Ok("cached"));
    }

    #[tokio::test]
    async fn cached_http_failure_drops_response_before_one_origin_fallback_and_clears() {
        struct Body<'a>(&'a Cell<bool>);
        impl Drop for Body<'_> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        for status in [StatusCode::NOT_FOUND, StatusCode::SERVICE_UNAVAILABLE] {
            let cache = FinalProviderUrl::new(true);
            let own = url("fixture-cache");
            cache.remember(&own);
            let dropped = Cell::new(false);
            let fallback = Cell::new(false);
            cache
                .send(
                    async {
                        assert!(dropped.get());
                        assert!(!fallback.replace(true));
                        Ok::<_, ()>((StatusCode::OK, Body(&dropped)))
                    },
                    |target| {
                        assert_eq!(target.as_ref(), &own);
                        ready(Ok((status, Body(&dropped))))
                    },
                )
                .await
                .unwrap();
            assert!(fallback.get());
            cache.remember(&url("disabled-after-failure"));
            assert_eq!(
                cache
                    .send(ready(Ok::<_, ()>((StatusCode::OK, "original"))), |_| {
                        async { panic!("used the discarded destination") }
                    })
                    .await,
                Ok("original")
            );
        }
    }

    #[tokio::test]
    async fn unsuccessful_origin_fallback_does_not_discard_the_previous_destination() {
        let cache = FinalProviderUrl::new(true);
        let own = url("fixture-cache");
        cache.remember(&own);
        let result = cache
            .send(
                ready(Ok::<_, ()>((StatusCode::NOT_FOUND, "original"))),
                |target| {
                    assert_eq!(target.as_ref(), &own);
                    ready(Ok((StatusCode::NOT_FOUND, "cached")))
                },
            )
            .await;
        assert_eq!(result, Ok("original"));
        let result = cache
            .send(async { panic!("polled the original request") }, |target| {
                assert_eq!(target.as_ref(), &own);
                ready(Ok::<_, ()>((StatusCode::OK, "cached")))
            })
            .await;
        assert_eq!(result, Ok("cached"));
    }

    #[tokio::test]
    async fn cold_disabled_oversized_and_transport_error_keep_existing_paths() {
        let cold = FinalProviderUrl::new(true);
        let disabled = FinalProviderUrl::new(false);
        disabled.remember(&url("ignored"));
        let oversized = FinalProviderUrl::new(true);
        oversized.remember(&url(&"x".repeat(MAX_CACHED_URL_BYTES)));
        for cache in [&cold, &disabled, &oversized] {
            assert_eq!(
                cache
                    .send(ready(Ok::<_, ()>((StatusCode::OK, "original"))), |_| {
                        async { panic!("used an unavailable cached destination") }
                    })
                    .await,
                Ok("original")
            );
        }

        cold.remember(&url("fixture-cache"));
        let result = cold
            .send(
                async { panic!("a transport error must not trigger fallback") },
                |_| ready(Err::<(StatusCode, ()), _>("transport")),
            )
            .await;
        assert_eq!(result, Err("transport"));
    }
}
