//! Bounded shared media retention. Transport and channel ownership live above this layer.
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub mod sync;
pub mod ts;

#[derive(Debug, PartialEq, Eq)]
pub enum BufferError {
    InvalidLimits,
    EmptyChunk,
    ChunkTooLarge,
    SequenceExhausted,
    ClockWentBackwards,
    InvalidSequence,
}

#[derive(Clone, Debug)]
pub struct Chunk {
    pub sequence: u64,
    pub received_at: Instant,
    pub data: Arc<[u8]>,
    pub discontinuity: bool,
}

#[derive(Debug)]
pub enum Read {
    Chunk(Chunk),
    /// Next available sequence after the viewer's requested data expired.
    Lagged {
        next_sequence: u64,
    },
    Pending,
}

/// One buffer per channel/output profile, never one per viewer.
/// The owner serializes mutations; readers clone payload references only.
pub struct MediaBuffer {
    chunks: VecDeque<Chunk>,
    bytes: usize,
    max_bytes: usize,
    retention: Duration,
    next_sequence: u64,
    last_write: Option<Instant>,
}

impl MediaBuffer {
    pub fn new(max_bytes: usize, retention: Duration) -> Result<Self, BufferError> {
        if max_bytes == 0 || retention.is_zero() {
            return Err(BufferError::InvalidLimits);
        }
        Ok(Self {
            chunks: VecDeque::new(),
            bytes: 0,
            max_bytes,
            retention,
            next_sequence: 1,
            last_write: None,
        })
    }

    pub fn push(&mut self, data: Arc<[u8]>, now: Instant) -> Result<u64, BufferError> {
        self.push_marked(data, now, false)
    }

    pub fn push_marked(
        &mut self,
        data: Arc<[u8]>,
        now: Instant,
        discontinuity: bool,
    ) -> Result<u64, BufferError> {
        self.push_numbered(data, now, discontinuity, self.next_sequence)
    }

    /// Use an authoritative shared-input sequence. A forward gap invalidates
    /// the contiguous local cache, while history can remain in shared storage.
    /// Replays and backwards sequences never mutate the cache.
    pub fn push_numbered(
        &mut self,
        data: Arc<[u8]>,
        now: Instant,
        discontinuity: bool,
        sequence: u64,
    ) -> Result<u64, BufferError> {
        if data.is_empty() {
            return Err(BufferError::EmptyChunk);
        }
        if data.len() > self.max_bytes {
            return Err(BufferError::ChunkTooLarge);
        }
        if self.last_write.is_some_and(|previous| now < previous) {
            return Err(BufferError::ClockWentBackwards);
        }
        if sequence < self.next_sequence {
            return Err(BufferError::InvalidSequence);
        }
        let next = sequence
            .checked_add(1)
            .ok_or(BufferError::SequenceExhausted)?;
        self.expire(now);
        if sequence != self.next_sequence {
            self.chunks.clear();
            self.bytes = 0;
        }
        while self.bytes > self.max_bytes - data.len() {
            self.evict_front();
        }
        self.bytes += data.len();
        self.chunks.push_back(Chunk {
            sequence,
            received_at: now,
            data,
            discontinuity,
        });
        self.next_sequence = next;
        self.last_write = Some(now);
        Ok(sequence)
    }

    /// Call on the channel timer as well as writes, including while upstream is stalled.
    pub fn expire(&mut self, now: Instant) {
        while self
            .chunks
            .front()
            .is_some_and(|chunk| now.saturating_duration_since(chunk.received_at) >= self.retention)
        {
            self.evict_front();
        }
    }

    fn evict_front(&mut self) {
        if let Some(chunk) = self.chunks.pop_front() {
            self.bytes -= chunk.data.len();
        }
    }

    /// Cursor is the next sequence to consume; it advances only after delivery.
    pub fn read(&self, sequence: u64) -> Read {
        let oldest = self
            .chunks
            .front()
            .map_or(self.next_sequence, |chunk| chunk.sequence);
        if sequence < oldest {
            return Read::Lagged {
                next_sequence: oldest,
            };
        }
        let offset = sequence - oldest;
        match usize::try_from(offset)
            .ok()
            .and_then(|index| self.chunks.get(index))
        {
            Some(chunk) => Read::Chunk(chunk.clone()),
            None => Read::Pending,
        }
    }

    /// Start at the latest chunk at or before the requested delay, or the oldest retained.
    /// Zero delay starts at the live head, matching the existing TS startup behavior.
    pub fn start_sequence(&self, now: Instant, behind: Duration) -> u64 {
        if behind.is_zero() {
            return self.next_sequence;
        }
        self.chunks
            .iter()
            .rev()
            .find(|chunk| now.saturating_duration_since(chunk.received_at) >= behind)
            .or_else(|| self.chunks.front())
            .map_or(self.next_sequence, |chunk| chunk.sequence)
    }

    /// Retained payload bytes, excluding references already held by transports.
    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffer(bytes: usize) -> MediaBuffer {
        MediaBuffer::new(bytes, Duration::from_secs(10)).unwrap()
    }

    #[test]
    fn shared_sequences_preserve_nonzero_cursor_and_clear_only_forward_gaps() {
        let now = Instant::now();
        let mut buffer = buffer(8);
        buffer
            .push_numbered(Arc::from(&b"a"[..]), now, false, 701)
            .unwrap();
        buffer
            .push_numbered(Arc::from(&b"b"[..]), now, true, 702)
            .unwrap();
        assert!(matches!(buffer.read(701), Read::Chunk(c) if &*c.data==b"a"));
        assert!(matches!(buffer.read(702), Read::Chunk(c) if c.discontinuity));
        assert_eq!(
            buffer.push_numbered(Arc::from(&b"c"[..]), now, false, 702),
            Err(BufferError::InvalidSequence)
        );
        assert_eq!(buffer.retained_bytes(), 2);
        buffer
            .push_numbered(Arc::from(&b"d"[..]), now, false, 710)
            .unwrap();
        assert!(matches!(
            buffer.read(702),
            Read::Lagged { next_sequence: 710 }
        ));
        assert!(matches!(buffer.read(710), Read::Chunk(c) if &*c.data==b"d"));
        assert_eq!(buffer.start_sequence(now, Duration::ZERO), 711);
        assert_eq!(buffer.retained_bytes(), 1);
    }

    #[test]
    fn slow_viewer_does_not_block_fast_viewer_or_grow_retention() {
        let now = Instant::now();
        let mut buffer = buffer(4);
        buffer.push(Arc::from(&b"ab"[..]), now).unwrap();
        buffer.push(Arc::from(&b"cd"[..]), now).unwrap();
        buffer.push(Arc::from(&b"ef"[..]), now).unwrap();
        assert_eq!(buffer.retained_bytes(), 4);
        assert!(matches!(buffer.read(1), Read::Lagged { next_sequence: 2 }));
        assert!(matches!(buffer.read(3), Read::Chunk(chunk) if &*chunk.data == b"ef"));
        assert!(matches!(buffer.read(4), Read::Pending));
    }

    #[test]
    fn viewers_share_payload_and_inflight_data_survives_eviction() {
        let now = Instant::now();
        let mut buffer = buffer(2);
        buffer.push(Arc::from(&b"ab"[..]), now).unwrap();
        let Read::Chunk(first) = buffer.read(1) else {
            panic!("missing chunk")
        };
        let Read::Chunk(second) = buffer.read(1) else {
            panic!("missing chunk")
        };
        assert!(Arc::ptr_eq(&first.data, &second.data));
        buffer.expire(now + Duration::from_secs(10));
        assert_eq!(buffer.retained_bytes(), 0);
        assert_eq!(&*first.data, b"ab");
        assert!(matches!(buffer.read(1), Read::Lagged { next_sequence: 2 }));
    }

    #[test]
    fn positioning_matches_time_based_start_and_live_head() {
        let now = Instant::now();
        let mut buffer = buffer(10);
        buffer.push(Arc::from(&b"a"[..]), now).unwrap();
        buffer
            .push(Arc::from(&b"b"[..]), now + Duration::from_secs(4))
            .unwrap();
        let later = now + Duration::from_secs(6);
        assert_eq!(buffer.start_sequence(later, Duration::ZERO), 3);
        assert_eq!(buffer.start_sequence(later, Duration::from_secs(3)), 1);
        assert_eq!(buffer.start_sequence(later, Duration::from_secs(1)), 2);
        assert_eq!(buffer.start_sequence(later, Duration::from_secs(30)), 1);
    }

    #[test]
    fn rejected_writes_leave_existing_data_intact() {
        let now = Instant::now();
        let mut buffer = buffer(2);
        buffer.push(Arc::from(&b"ab"[..]), now).unwrap();
        assert_eq!(
            buffer.push(Arc::from(&b"abc"[..]), now),
            Err(BufferError::ChunkTooLarge)
        );
        assert_eq!(
            buffer.push(Arc::from(&b""[..]), now),
            Err(BufferError::EmptyChunk)
        );
        assert_eq!(buffer.retained_bytes(), 2);
        assert!(matches!(buffer.read(1), Read::Chunk(_)));
    }

    #[test]
    fn invalid_limits_and_clock_regression_are_rejected() {
        assert!(matches!(
            MediaBuffer::new(0, Duration::from_secs(1)),
            Err(BufferError::InvalidLimits)
        ));
        assert!(matches!(
            MediaBuffer::new(1, Duration::ZERO),
            Err(BufferError::InvalidLimits)
        ));
        let now = Instant::now();
        let mut buffer = buffer(4);
        buffer
            .push(Arc::from(&b"a"[..]), now + Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            buffer.push(Arc::from(&b"b"[..]), now),
            Err(BufferError::ClockWentBackwards)
        );
        assert_eq!(buffer.retained_bytes(), 1);
    }

    #[test]
    fn sequence_exhaustion_is_atomic_and_future_cursors_are_pending() {
        let now = Instant::now();
        let mut buffer = buffer(4);
        buffer.next_sequence = u64::MAX - 1;
        buffer.push(Arc::from(&b"a"[..]), now).unwrap();
        assert_eq!(
            buffer.push(Arc::from(&b"b"[..]), now),
            Err(BufferError::SequenceExhausted)
        );
        assert_eq!(buffer.retained_bytes(), 1);
        assert!(matches!(buffer.read(u64::MAX - 1), Read::Chunk(_)));
        assert!(matches!(buffer.read(u64::MAX), Read::Pending));
    }
}
