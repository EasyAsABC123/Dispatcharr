//! Packet alignment and source-switch markers matching the Python stream buffer.
//! This is not a TS validator or sync-byte resynchronizer. The caller must supply
//! a byte stream already starting at a packet boundary, as the current backend does.

pub const PACKET_SIZE: usize = 188;
const NULL_PID: usize = 0x1fff;

/// Retains at most one incomplete packet, independent of upstream read size.
pub struct PacketAssembler {
    partial: [u8; PACKET_SIZE],
    filled: usize,
}

impl Default for PacketAssembler {
    fn default() -> Self {
        Self {
            partial: [0; PACKET_SIZE],
            filled: 0,
        }
    }
}

impl PacketAssembler {
    /// Emits complete packets synchronously; no queue or input-sized allocation.
    pub fn feed(&mut self, mut input: &[u8], mut emit: impl FnMut(&[u8; PACKET_SIZE])) {
        if self.filled > 0 {
            let copied = input.len().min(PACKET_SIZE - self.filled);
            self.partial[self.filled..self.filled + copied].copy_from_slice(&input[..copied]);
            self.filled += copied;
            input = &input[copied..];
            if self.filled == PACKET_SIZE {
                emit(&self.partial);
                self.filled = 0;
            } else {
                return;
            }
        }
        let (packets, remainder) = input.as_chunks::<PACKET_SIZE>();
        for packet in packets {
            emit(packet);
        }
        self.partial[..remainder.len()].copy_from_slice(remainder);
        self.filled = remainder.len();
    }

    /// Drop the old source's incomplete tail before reading the next source.
    pub fn reset(&mut self) {
        self.filled = 0;
    }
    pub fn pending_bytes(&self) -> usize {
        self.filled
    }
}

/// Tracks the first packet of each PID during one source-switch generation.
/// Arm after flushing old complete packets and resetting PacketAssembler;
/// disarm after publishing the first new-source buffer chunk, matching Python.
pub struct DiscontinuityMarker {
    seen: [u64; 128],
    armed: bool,
}

impl Default for DiscontinuityMarker {
    fn default() -> Self {
        Self {
            seen: [0; 128],
            armed: false,
        }
    }
}

impl DiscontinuityMarker {
    pub fn arm(&mut self) {
        self.seen.fill(0);
        self.armed = true;
    }
    pub fn disarm(&mut self) {
        self.armed = false;
    }

    /// Return whether this PID was newly encountered (including packets whose
    /// reserved or empty adaptation layout prevents modifying the indicator).
    pub fn stamp(&mut self, packet: &mut [u8; PACKET_SIZE]) -> bool {
        if !self.armed || packet[0] != 0x47 {
            return false;
        }
        let pid = (((packet[1] & 0x1f) as usize) << 8) | packet[2] as usize;
        let bit = 1u64 << (pid % 64);
        if pid == NULL_PID || self.seen[pid / 64] & bit != 0 {
            return false;
        }
        self.seen[pid / 64] |= bit;
        match (packet[3] >> 4) & 3 {
            2 | 3 if packet[4] >= 1 => packet[5] |= 0x80,
            1 => {
                // Preserve Python behavior: insert a two-byte adaptation field,
                // retain leading payload, drop its final two bytes. This can
                // damage a PES payload; fidelity here is not an endorsement.
                packet.copy_within(4..PACKET_SIZE - 2, 6);
                packet[3] = (packet[3] & 0x0f) | 0x30;
                packet[4] = 1;
                packet[5] = 0x80;
            }
            _ => {}
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(pid: u16, afc: u8) -> [u8; PACKET_SIZE] {
        let mut packet = [0x55; PACKET_SIZE];
        packet[..4].copy_from_slice(&[0x47, (pid >> 8) as u8, pid as u8, afc << 4 | 7]);
        packet
    }

    #[test]
    fn every_fragment_boundary_preserves_original_bytes() {
        let input: Vec<u8> = (0..PACKET_SIZE * 3 + 17).map(|n| n as u8).collect();
        for size in 1..PACKET_SIZE * 2 {
            let mut assembler = PacketAssembler::default();
            let mut output = Vec::new();
            for slice in input.chunks(size) {
                assembler.feed(slice, |packet| output.extend_from_slice(packet));
                assert!(assembler.pending_bytes() < PACKET_SIZE);
            }
            assert_eq!(output, input[..PACKET_SIZE * 3]);
            assert_eq!(assembler.pending_bytes(), 17);
        }
    }

    #[test]
    fn reset_never_splices_old_source_tail_into_new_source() {
        let mut assembler = PacketAssembler::default();
        assembler.feed(&[1; 187], |_| panic!("incomplete packet emitted"));
        assembler.reset();
        assembler.feed(&packet(100, 1), |got| assert_eq!(got, &packet(100, 1)));
        assert_eq!(assembler.pending_bytes(), 0);
    }

    #[test]
    fn first_packet_per_pid_only_and_generation_resets() {
        let mut marker = DiscontinuityMarker::default();
        marker.arm();
        for pid in [1, 64, 8190] {
            let mut first = packet(pid, 1);
            assert!(marker.stamp(&mut first));
            assert_eq!(&first[3..6], &[0x37, 1, 0x80]);
            assert_eq!(&first[6..], &packet(pid, 1)[4..186]);
            let mut second = packet(pid, 1);
            assert!(!marker.stamp(&mut second));
            assert_eq!(second, packet(pid, 1));
        }
        marker.disarm();
        assert!(!marker.stamp(&mut packet(5, 1)));
        marker.arm();
        assert!(marker.stamp(&mut packet(1, 1)));
    }

    #[test]
    fn adaptation_layouts_and_nonmedia_packets_match_python() {
        let mut marker = DiscontinuityMarker::default();
        marker.arm();
        for (pid, afc) in [(1, 2), (2, 3)] {
            let mut p = packet(pid, afc);
            p[4] = 1;
            p[5] = 0x10;
            let original = p;
            assert!(marker.stamp(&mut p));
            assert_eq!(p[5], 0x90);
            p[5] = original[5];
            assert_eq!(p, original);
        }
        for (pid, afc) in [(3, 0), (4, 2), (5, 3)] {
            let mut p = packet(pid, afc);
            p[4] = 0;
            let original = p;
            assert!(marker.stamp(&mut p));
            assert_eq!(p, original);
        }
        let mut null = packet(8191, 1);
        assert!(!marker.stamp(&mut null));
        let mut bad_sync = packet(6, 1);
        bad_sync[0] = 0;
        assert!(!marker.stamp(&mut bad_sync));
    }
}
