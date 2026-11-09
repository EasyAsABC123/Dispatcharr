//! Exact legacy sync-chain finder plus an opt-in bounded startup synchronizer.
//! The pinned Python HTTP reader does not currently call `find_ts_sync`; enabling
//! this guard is an explicit validated-TS policy, not literal reader parity.
use crate::ts::{PacketAssembler, PACKET_SIZE};

pub const MAX_PREFIX_BYTES: usize = 64 * 1024;
const SYNC_LOOKAHEAD: usize = 2 * PACKET_SIZE;

/// First 0x47 byte followed by markers exactly 188 and 376 bytes later.
/// Like the original helper, three headers suffice; three full packets are not
/// required. This does not validate PES/PSI syntax or ongoing packet continuity.
pub fn find_ts_sync(bytes: &[u8]) -> Option<usize> {
    (0..bytes.len().saturating_sub(SYNC_LOOKAHEAD)).find(|index| {
        bytes[*index] == 0x47
            && bytes[*index + PACKET_SIZE] == 0x47
            && bytes[*index + SYNC_LOOKAHEAD] == 0x47
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncError {
    InvalidPrefixLimit,
    PrefixBudgetExceeded,
}

/// Startup-only prefix removal. After synchronization, bytes pass through the
/// packet assembler unchanged; reset explicitly before feeding another source.
/// Network startup/read deadlines are the caller's responsibility.
pub struct Synchronizer {
    prefix: Vec<u8>,
    limit: usize,
    scanned: usize,
    synced: bool,
    failed: bool,
    packets: PacketAssembler,
}

impl Default for Synchronizer {
    fn default() -> Self {
        Self::new(MAX_PREFIX_BYTES).unwrap()
    }
}

impl Synchronizer {
    pub fn new(prefix_limit: usize) -> Result<Self, SyncError> {
        if !(SYNC_LOOKAHEAD + 1..=MAX_PREFIX_BYTES).contains(&prefix_limit) {
            return Err(SyncError::InvalidPrefixLimit);
        }
        Ok(Self {
            prefix: Vec::with_capacity(prefix_limit),
            limit: prefix_limit,
            scanned: 0,
            synced: false,
            failed: false,
            packets: PacketAssembler::default(),
        })
    }

    pub fn is_synced(&self) -> bool {
        self.synced
    }
    pub fn buffered_bytes(&self) -> usize {
        self.prefix.len() + self.packets.pending_bytes()
    }

    pub fn reset(&mut self) {
        self.prefix.clear();
        self.scanned = 0;
        self.synced = false;
        self.failed = false;
        self.packets.reset();
    }

    pub fn feed(
        &mut self,
        mut input: &[u8],
        mut emit: impl FnMut(&[u8; PACKET_SIZE]),
    ) -> Result<(), SyncError> {
        if self.failed {
            return Err(SyncError::PrefixBudgetExceeded);
        }
        if self.synced {
            self.packets.feed(input, emit);
            return Ok(());
        }
        while !input.is_empty() {
            let count = input.len().min(self.limit - self.prefix.len());
            self.prefix.extend_from_slice(&input[..count]);
            input = &input[count..];
            if let Some(relative) = find_ts_sync(&self.prefix[self.scanned..]) {
                let start = self.scanned + relative;
                self.synced = true;
                self.packets.feed(&self.prefix[start..], &mut emit);
                self.prefix.clear();
                self.packets.feed(input, emit);
                return Ok(());
            }
            // Previously checked starts cannot change. Keep the suffix whose
            // third marker still needs bytes from a subsequent HTTP frame.
            self.scanned = self.prefix.len().saturating_sub(SYNC_LOOKAHEAD);
            if self.prefix.len() == self.limit {
                self.prefix.clear();
                self.failed = true;
                return Err(SyncError::PrefixBudgetExceeded);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn media(count: usize, value: u8) -> Vec<u8> {
        (0..count)
            .flat_map(|index| {
                let mut packet = [value; PACKET_SIZE];
                packet[..4].copy_from_slice(&[0x47, 0, 100, 0x10 | (index as u8 & 15)]);
                packet
            })
            .collect()
    }

    #[test]
    fn helper_requires_exactly_three_header_markers_and_returns_first_chain() {
        assert_eq!(find_ts_sync(&[]), None);
        let bytes = media(3, 0x33);
        assert_eq!(find_ts_sync(&bytes[..376]), None);
        assert_eq!(find_ts_sync(&bytes[..377]), Some(0));
        let mut prefixed = vec![b'G'; 25];
        prefixed.extend_from_slice(&bytes);
        assert_eq!(find_ts_sync(&prefixed), Some(25));
    }

    #[test]
    fn preface_and_sync_chains_split_across_every_frame_boundary_are_byte_faithful() {
        let media = media(8, 0x33);
        let mut input = b"<html>PHP warning</html>\r\n".to_vec();
        input.extend_from_slice(&media);
        for size in [1, 2, 7, 187, 188, 189, 377, 8192] {
            let mut synchronizer = Synchronizer::default();
            let mut emitted = Vec::new();
            for frame in input.chunks(size) {
                synchronizer
                    .feed(frame, |packet| emitted.extend_from_slice(packet))
                    .unwrap();
                assert!(synchronizer.buffered_bytes() <= MAX_PREFIX_BYTES);
            }
            assert!(synchronizer.is_synced());
            assert_eq!(emitted, media);
            assert_eq!(synchronizer.buffered_bytes(), 0);
        }
    }

    #[test]
    fn prefix_limit_is_hard_and_failure_requires_explicit_source_reset() {
        let mut synchronizer = Synchronizer::new(512).unwrap();
        let mut emitted = Vec::new();
        assert_eq!(
            synchronizer.feed(&[b'X'; 512], |packet| emitted.extend_from_slice(packet)),
            Err(SyncError::PrefixBudgetExceeded)
        );
        assert!(emitted.is_empty());
        assert_eq!(
            synchronizer.feed(&media(4, 0x33), |_| {}),
            Err(SyncError::PrefixBudgetExceeded)
        );
        synchronizer.reset();
        synchronizer
            .feed(&media(4, 0x33), |packet| emitted.extend_from_slice(packet))
            .unwrap();
        assert_eq!(emitted, media(4, 0x33));
    }

    #[test]
    fn synchronization_at_last_budget_byte_works_without_buffering_large_remainder() {
        let media = media(1000, 0x33);
        let mut input = vec![0x55; 512 - 377];
        input.extend_from_slice(&media);
        let mut synchronizer = Synchronizer::new(512).unwrap();
        let mut emitted = Vec::new();
        synchronizer
            .feed(&input, |packet| emitted.extend_from_slice(packet))
            .unwrap();
        assert_eq!(emitted, media);
        assert!(synchronizer.buffered_bytes() < 188);
        let mut too_late = vec![0x55; 512 - 376];
        too_late.extend_from_slice(&media);
        assert_eq!(
            Synchronizer::new(512).unwrap().feed(&too_late, |_| {}),
            Err(SyncError::PrefixBudgetExceeded)
        );
    }

    #[test]
    fn source_reset_drops_old_partial_packet_and_rechecks_new_preamble() {
        let mut synchronizer = Synchronizer::default();
        let old = media(4, 0x11);
        synchronizer.feed(&old, |_| {}).unwrap();
        synchronizer.feed(&[0xaa; 17], |_| {}).unwrap();
        assert_eq!(synchronizer.buffered_bytes(), 17);
        synchronizer.reset();
        let new = media(4, 0x22);
        let mut input = b"provider preamble".to_vec();
        input.extend_from_slice(&new);
        let mut emitted = Vec::new();
        synchronizer
            .feed(&input, |packet| emitted.extend_from_slice(packet))
            .unwrap();
        assert_eq!(emitted, new);
    }

    #[test]
    fn invalid_prefix_limits_are_rejected() {
        for limit in [0, 188, 376, MAX_PREFIX_BYTES + 1] {
            assert!(matches!(
                Synchronizer::new(limit),
                Err(SyncError::InvalidPrefixLimit)
            ));
        }
    }
}
