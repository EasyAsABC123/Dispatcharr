//! Bounded local-cache misses backed by the original shared input history.
//! Original Dispatcharr authors retain full application credit.
use dispatcharr_input_store::{Error, InputBytes, InputSnapshot, InputStore};
use dispatcharr_stream_runtime::{FrameLease, OwnedChunk, OwnedDelivery, Viewer};
use std::{io, time::Duration};
use tokio::time::{self, Instant};

pub(super) struct History {
    pub store: InputStore,
    pub channel: String,
    pub owner: String,
    pub initial_behind_chunks: u64,
    pub read_timeout: Duration,
}

pub(super) enum Payload {
    Cached(OwnedChunk),
    Historical {
        data: InputBytes,
        _lease: FrameLease,
    },
}
impl Payload {
    pub fn discontinuity(&self) -> bool {
        match self {
            Self::Cached(chunk) => chunk.discontinuity,
            Self::Historical { data, .. } => data.discontinuity,
        }
    }
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Cached(chunk) => chunk.as_ref(),
            Self::Historical { data, .. } => data.as_ref(),
        }
    }
}

impl History {
    async fn snapshot(&self, deadline: Instant) -> io::Result<InputSnapshot> {
        loop {
            match self.store.snapshot(&self.channel).await {
                Ok(snapshot) if snapshot.owner.as_deref() == Some(&self.owner) => {
                    return Ok(snapshot)
                }
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "Input owner changed",
                    ))
                }
                Err(Error::Capacity) if Instant::now() < deadline => {
                    time::sleep(Duration::from_millis(25)).await
                }
                Err(_) => return Err(io::Error::other("Shared input history unavailable")),
            }
        }
    }

    pub async fn position(&self, viewer: &mut Viewer, behind: Duration) -> io::Result<()> {
        let snapshot = self.snapshot(Instant::now() + self.read_timeout).await?;
        let cursor = if behind.is_zero() {
            snapshot.index
        } else {
            snapshot
                .cursor_for_age(behind.as_secs_f64(), super::epoch_seconds())
                .unwrap_or(snapshot.index)
        };
        viewer
            .seek_sequence(cursor.saturating_add(1))
            .map_err(|_| io::Error::other("Viewer stopped"))
    }
}

pub(super) async fn next(
    viewer: &mut Viewer,
    history: Option<&History>,
) -> io::Result<Option<Payload>> {
    loop {
        let requested = viewer.next_sequence();
        match viewer.next_owned().await {
            OwnedDelivery::Chunk(chunk) => return Ok(Some(Payload::Cached(chunk))),
            OwnedDelivery::Cancelled | OwnedDelivery::Stopped => return Ok(None),
            OwnedDelivery::Lagged { .. } => {}
        }
        let Some(history) = history else { continue };
        // next_owned reports lag with the old cursor advanced. Restore the
        // requested canonical position before consulting authoritative history.
        viewer
            .seek_sequence(requested)
            .map_err(|_| io::Error::other("Viewer stopped"))?;
        let lease = viewer
            .reserve_frame()
            .await
            .map_err(|_| io::Error::other("Viewer stopped"))?;
        let deadline = Instant::now() + history.read_timeout;
        let data = loop {
            match history.store.get(&history.channel, requested).await {
                Ok(data) => break data,
                Err(Error::Capacity) if Instant::now() < deadline => {
                    time::sleep(Duration::from_millis(25)).await
                }
                Err(_) => return Err(io::Error::other("Shared input history unavailable")),
            }
        };
        if let Some(data) = data {
            if data.owner != history.owner {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Input owner changed",
                ));
            }
            viewer
                .seek_sequence(data.sequence.saturating_add(1))
                .map_err(|_| io::Error::other("Viewer stopped"))?;
            return Ok(Some(Payload::Historical {
                data,
                _lease: lease,
            }));
        }
        drop(lease);
        let snapshot = history.snapshot(deadline).await?;
        // Source generator recovery: first available chunk after the missing
        // cursor, or near the live head when no retained history remains.
        let next = snapshot
            .chunks
            .iter()
            .filter(|stamp| stamp.sequence > requested)
            .map(|stamp| stamp.sequence)
            .min()
            .unwrap_or_else(|| {
                requested.max(
                    snapshot
                        .index
                        .saturating_sub(history.initial_behind_chunks)
                        .saturating_add(1),
                )
            });
        viewer
            .seek_sequence(next)
            .map_err(|_| io::Error::other("Viewer stopped"))?;
        if next == requested {
            time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatcharr_input_store::InputConfig;
    use dispatcharr_stream_runtime::Channel;
    use std::sync::Arc;

    async fn fixture(bytes: usize) -> (History, redis::aio::MultiplexedConnection) {
        fixture_config(InputConfig {
            max_read_bytes: bytes,
            ..Default::default()
        })
        .await
    }

    async fn fixture_config(config: InputConfig) -> (History, redis::aio::MultiplexedConnection) {
        let url = std::env::var("REDIS_TEST_URL").unwrap();
        let mut redis = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let channel = format!("history-{}", uuid::Uuid::new_v4());
        let owner = "history-original-owner".to_owned();
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{channel}:owner"))
            .arg(&owner)
            .arg("EX")
            .arg(120)
            .query_async(&mut redis)
            .await
            .unwrap();
        // Existing canonical index makes global-vs-local confusion visible.
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{channel}:input:buffer:index"))
            .arg(700)
            .query_async(&mut redis)
            .await
            .unwrap();
        let store = InputStore::connect(&url, config).await.unwrap();
        (
            History {
                store,
                channel,
                owner,
                initial_behind_chunks: 4,
                read_timeout: Duration::from_secs(2),
            },
            redis,
        )
    }
    fn packet(counter: u8) -> Arc<[u8]> {
        let mut data = vec![counter; 188];
        data[..4].copy_from_slice(&[0x47, 0, 100, 0x10]);
        data.into()
    }
    async fn fill(history: &History, channel: &Channel) {
        for number in 0..8 {
            let bytes = packet(number);
            let timestamp = super::super::epoch_seconds() - 8.0 + number as f64;
            let sequence = history
                .store
                .put(
                    &history.channel,
                    &history.owner,
                    &bytes,
                    timestamp,
                    number == 3,
                )
                .await
                .unwrap();
            assert_eq!(sequence, 701 + number as u64);
            channel
                .publish_numbered(
                    bytes,
                    number == 3,
                    std::time::Instant::now() - Duration::from_secs(8 - number as u64),
                    sequence,
                )
                .unwrap();
        }
    }
    async fn cleanup(history: &History, redis: &mut redis::aio::MultiplexedConnection) {
        history
            .store
            .cleanup(&history.channel, &history.owner)
            .await
            .unwrap();
        let _: () = redis::cmd("DEL")
            .arg(format!("live:channel:{}:owner", history.channel))
            .query_async(redis)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
    async fn slow_cursor_recovers_exact_history_beyond_cache_and_preserves_flags() {
        let (history, mut redis) = fixture(8 * 1024 * 1024).await;
        let channel = Channel::new(188 * 2, Duration::from_secs(60), 3).unwrap();
        fill(&history, &channel).await;
        let mut viewer = channel.subscribe(Duration::ZERO).unwrap();
        history
            .position(&mut viewer, Duration::from_secs(6))
            .await
            .unwrap();
        assert_eq!(viewer.next_sequence(), 703);
        let first = next(&mut viewer, Some(&history)).await.unwrap().unwrap();
        assert_eq!(first.as_ref(), packet(2).as_ref());
        assert!(!first.discontinuity());
        assert!(
            time::timeout(Duration::from_millis(20), next(&mut viewer, Some(&history)))
                .await
                .is_err()
        );
        drop(first);
        let marked = next(&mut viewer, Some(&history)).await.unwrap().unwrap();
        assert_eq!(marked.as_ref(), packet(3).as_ref());
        assert!(marked.discontinuity());
        drop(marked);
        for number in 4..8 {
            let frame = next(&mut viewer, Some(&history)).await.unwrap().unwrap();
            assert_eq!(frame.as_ref(), packet(number).as_ref());
            drop(frame);
        }
        assert_eq!(channel.retained_bytes(), 376);
        history.position(&mut viewer, Duration::ZERO).await.unwrap();
        assert_eq!(viewer.next_sequence(), 709);
        assert!(
            time::timeout(Duration::from_millis(20), next(&mut viewer, Some(&history)))
                .await
                .is_err()
        );
        cleanup(&history, &mut redis).await;
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
    async fn expired_history_skips_to_first_available_and_rejects_successor_era() {
        let (history, mut redis) = fixture(8 * 1024 * 1024).await;
        let channel = Channel::new(376, Duration::from_secs(60), 2).unwrap();
        fill(&history, &channel).await;
        let _: () = redis::cmd("DEL")
            .arg(format!(
                "live:channel:{}:input:buffer:chunk:701",
                history.channel
            ))
            .query_async(&mut redis)
            .await
            .unwrap();
        let mut viewer = channel.subscribe(Duration::ZERO).unwrap();
        viewer.seek_sequence(701).unwrap();
        let frame = next(&mut viewer, Some(&history)).await.unwrap().unwrap();
        assert_eq!(frame.as_ref(), packet(1).as_ref());
        drop(frame);
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{}:owner", history.channel))
            .arg("successor")
            .query_async(&mut redis)
            .await
            .unwrap();
        viewer.seek_sequence(701).unwrap();
        assert_eq!(
            next(&mut viewer, Some(&history))
                .await
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::ConnectionAborted
        );
        // Restore only our fixture owner so fenced cleanup can remove its rows.
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{}:owner", history.channel))
            .arg(&history.owner)
            .query_async(&mut redis)
            .await
            .unwrap();
        cleanup(&history, &mut redis).await;
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
    async fn hundred_historical_readers_wait_for_one_frame_budget_while_live_cache_continues() {
        let size = 188 * 30;
        let (mut history, mut redis) = fixture_config(InputConfig {
            max_chunk_bytes: size,
            max_chunks: 8,
            max_read_bytes: size,
            ..Default::default()
        })
        .await;
        history.read_timeout = Duration::from_secs(10);
        let channel = Channel::new(size, Duration::from_secs(60), 103).unwrap();
        for number in 0..2 {
            let data: Arc<[u8]> = packet(number).as_ref().repeat(30).into();
            let sequence = history
                .store
                .put(
                    &history.channel,
                    &history.owner,
                    &data,
                    super::super::epoch_seconds(),
                    false,
                )
                .await
                .unwrap();
            channel
                .publish_numbered(data, false, std::time::Instant::now(), sequence)
                .unwrap();
        }
        let history = Arc::new(history);
        let mut held_viewer = channel.subscribe(Duration::ZERO).unwrap();
        held_viewer.seek_sequence(701).unwrap();
        let held = next(&mut held_viewer, Some(&history))
            .await
            .unwrap()
            .unwrap();
        let mut readers = Vec::new();
        for _ in 0..100 {
            let mut viewer = channel.subscribe(Duration::ZERO).unwrap();
            viewer.seek_sequence(701).unwrap();
            let history = history.clone();
            readers.push(tokio::spawn(async move {
                let data = next(&mut viewer, Some(&history)).await.unwrap().unwrap();
                assert_eq!(data.as_ref(), packet(0).as_ref().repeat(30));
            }));
        }
        time::sleep(Duration::from_millis(60)).await;
        assert!(readers.iter().all(|reader| !reader.is_finished()));
        let mut live = channel.subscribe(Duration::ZERO).unwrap();
        live.seek_sequence(702).unwrap();
        let cached = time::timeout(Duration::from_millis(50), next(&mut live, Some(&history)))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(cached.as_ref(), packet(1).as_ref().repeat(30));
        drop(cached);
        // Cancelling a waiter cannot release the held transport frame's bytes.
        let cancelled = readers.pop().unwrap();
        cancelled.abort();
        let _ = cancelled.await;
        assert!(matches!(
            history.store.get(&history.channel, 701).await,
            Err(Error::Capacity)
        ));
        drop(held);
        time::timeout(Duration::from_secs(8), async {
            for reader in readers {
                reader.await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(history
            .store
            .get(&history.channel, 701)
            .await
            .unwrap()
            .is_some());
        cleanup(&history, &mut redis).await;
    }
}
