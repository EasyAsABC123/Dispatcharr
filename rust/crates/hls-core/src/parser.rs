//! Byte-level helpers preserving the pinned Python segmenter's parsing rules.
use crate::TS_PACKET_SIZE;
use std::collections::BTreeMap;

pub type Packet = [u8; TS_PACKET_SIZE];
pub type ParameterSets = BTreeMap<u8, Vec<u8>>;

pub fn packet_pid(packet: &Packet) -> u16 {
    (u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2])
}
pub fn packet_continuity_counter(packet: &Packet) -> u8 {
    packet[3] & 15
}
pub fn packet_pusi(packet: &Packet) -> bool {
    packet[1] & 0x40 != 0
}

pub fn packet_payload_offset(packet: &Packet) -> Option<usize> {
    match (packet[3] >> 4) & 3 {
        1 => Some(4),
        3 => {
            let offset = 5 + usize::from(packet[4]);
            (offset < TS_PACKET_SIZE).then_some(offset)
        }
        _ => None,
    }
}

pub fn packet_random_access(packet: &Packet) -> bool {
    matches!((packet[3] >> 4) & 3, 2 | 3) && packet[4] > 0 && packet[5] & 0x40 != 0
}

pub fn video_stream_type(stream_type: u8) -> bool {
    matches!(stream_type, 1 | 2 | 0x1b | 0x24)
}
pub fn audio_stream_type(stream_type: u8) -> bool {
    matches!(stream_type, 3 | 4 | 0x0f | 0x11 | 0x81 | 0x87)
}

pub fn parse_pat(packet: &Packet) -> Option<u16> {
    let base = packet_payload_offset(packet)?;
    if base + 1 >= TS_PACKET_SIZE {
        return None;
    }
    let section = base + 1 + usize::from(packet[base]);
    let mut offset = section + 8;
    while offset + 3 < TS_PACKET_SIZE {
        let program = (u16::from(packet[offset]) << 8) | u16::from(packet[offset + 1]);
        let pid = (u16::from(packet[offset + 2] & 0x1f) << 8) | u16::from(packet[offset + 3]);
        if program != 0 {
            return Some(pid);
        }
        offset += 4;
    }
    None
}

pub fn parse_pmt_streams(packet: &Packet) -> Option<Vec<(u8, u16)>> {
    let base = packet_payload_offset(packet)?;
    if base + 1 >= TS_PACKET_SIZE {
        return None;
    }
    let section = base + 1 + usize::from(packet[base]);
    if section + 12 >= TS_PACKET_SIZE || packet[section] != 2 {
        return None;
    }
    let length = (usize::from(packet[section + 1] & 15) << 8) | usize::from(packet[section + 2]);
    if section + 3 + length > TS_PACKET_SIZE {
        return None;
    }
    let info_length =
        (usize::from(packet[section + 10] & 15) << 8) | usize::from(packet[section + 11]);
    let mut offset = section + 12 + info_length;
    let end = section + 3 + length - 4;
    if offset > end {
        return None;
    }
    let mut streams = Vec::new();
    while offset + 4 < end {
        let stream_type = packet[offset];
        let pid = (u16::from(packet[offset + 1] & 0x1f) << 8) | u16::from(packet[offset + 2]);
        let info_length =
            (usize::from(packet[offset + 3] & 15) << 8) | usize::from(packet[offset + 4]);
        let next = offset + 5 + info_length;
        if next > end {
            return None;
        }
        streams.push((stream_type, pid));
        offset = next;
    }
    Some(streams)
}

pub fn parse_pmt(packet: &Packet) -> Option<(u16, u8)> {
    parse_pmt_streams(packet)?
        .into_iter()
        .find_map(|(stream_type, pid)| video_stream_type(stream_type).then_some((pid, stream_type)))
}

pub fn extract_pts(packet: &Packet) -> Option<f64> {
    let base = packet_payload_offset(packet)?;
    if base + 13 >= TS_PACKET_SIZE
        || packet[base..base + 3] != [0, 0, 1]
        || packet[base + 7] & 0x80 == 0
    {
        return None;
    }
    let pts = (u64::from((packet[base + 9] >> 1) & 7) << 30)
        | (u64::from(packet[base + 10]) << 22)
        | (u64::from((packet[base + 11] >> 1) & 0x7f) << 15)
        | (u64::from(packet[base + 12]) << 7)
        | u64::from(packet[base + 13] >> 1);
    Some(pts as f64 / 90_000.0)
}

fn start_offsets(packet: &Packet, start: usize, end: usize) -> Vec<usize> {
    let mut result = Vec::new();
    let mut next = start;
    while next + 3 < end {
        let Some(found) = packet[next..]
            .windows(3)
            .position(|slice| slice == [0, 0, 1])
        else {
            break;
        };
        let header = next + found + 3;
        if header >= end {
            break;
        }
        result.push(header);
        next = header;
    }
    result
}

fn mpeg2_starts_keyframe(packet: &Packet) -> bool {
    let Some(base) = packet_payload_offset(packet) else {
        return false;
    };
    if base + 9 >= TS_PACKET_SIZE {
        return false;
    }
    let start = base + 9 + usize::from(packet[base + 8]);
    for offset in start_offsets(packet, start, TS_PACKET_SIZE) {
        match packet[offset] {
            0xb3 | 0xb8 => return true,
            0 => return offset + 2 < TS_PACKET_SIZE && ((packet[offset + 2] >> 3) & 7) == 1,
            1..=0xaf => return false,
            _ => {}
        }
    }
    false
}

pub fn parameter_types(stream_type: u8) -> &'static [u8] {
    match stream_type {
        0x1b => &[7, 8],
        0x24 => &[32, 33, 34],
        _ => &[],
    }
}

fn nal_type(header: u8, stream_type: u8) -> Option<u8> {
    match stream_type {
        0x1b => Some(header & 31),
        0x24 => Some((header >> 1) & 63),
        _ => None,
    }
}

fn start_code_begin(packet: &Packet, header: usize) -> usize {
    let mut prefix = header - 3;
    if prefix > 0 && packet[prefix - 1] == 0 {
        prefix -= 1;
    }
    prefix
}

fn pes_es_range(packet: &Packet) -> Option<(usize, usize, bool)> {
    let base = packet_payload_offset(packet)?;
    if base + 8 >= TS_PACKET_SIZE || packet[base..base + 3] != [0, 0, 1] {
        return None;
    }
    let length = (usize::from(packet[base + 4]) << 8) | usize::from(packet[base + 5]);
    let start = base + 9 + usize::from(packet[base + 8]);
    if start >= TS_PACKET_SIZE {
        return None;
    }
    let (end, bounded) = if length == 0 {
        let mut end = TS_PACKET_SIZE;
        while end > start && packet[end - 1] == 0xff {
            end -= 1;
        }
        (end, end < TS_PACKET_SIZE)
    } else {
        let end = base + 6 + length;
        (end.min(TS_PACKET_SIZE), end <= TS_PACKET_SIZE)
    };
    (end > start).then_some((start, end, bounded))
}

pub fn inspect_avc_hevc_packet(packet: &Packet, stream_type: u8) -> (bool, ParameterSets) {
    let wanted = parameter_types(stream_type);
    if wanted.is_empty() {
        return (false, ParameterSets::new());
    }
    let Some((start, end, bounded)) = pes_es_range(packet) else {
        return (false, ParameterSets::new());
    };
    let offsets = start_offsets(packet, start, end);
    let mut keyframe = None;
    let mut seen_parameter = false;
    let mut params = ParameterSets::new();
    for (index, offset) in offsets.iter().copied().enumerate() {
        let Some(nal) = nal_type(packet[offset], stream_type) else {
            continue;
        };
        if keyframe.is_none() {
            if stream_type == 0x1b {
                match nal {
                    5 => keyframe = Some(true),
                    1..=4 => keyframe = Some(false),
                    7 => seen_parameter = true,
                    _ => {}
                }
            } else {
                match nal {
                    16..=21 => keyframe = Some(true),
                    0..=15 => keyframe = Some(false),
                    32 | 33 => seen_parameter = true,
                    _ => {}
                }
            }
        }
        if !wanted.contains(&nal) {
            continue;
        }
        let prefix = start_code_begin(packet, offset);
        let nal_end = if let Some(next) = offsets.get(index + 1) {
            start_code_begin(packet, *next)
        } else {
            if !bounded || end - prefix > 512 {
                continue;
            }
            end
        };
        if nal_end > prefix {
            params.insert(nal, packet[prefix..nal_end].to_vec());
        }
    }
    (keyframe.unwrap_or(seen_parameter), params)
}

pub fn starts_keyframe(packet: &Packet, stream_type: u8) -> bool {
    if packet_random_access(packet) {
        return true;
    }
    match stream_type {
        0x1b | 0x24 => inspect_avc_hevc_packet(packet, stream_type).0,
        1 | 2 => mpeg2_starts_keyframe(packet),
        _ => false,
    }
}

pub fn extract_parameter_sets(packet: &Packet, stream_type: u8) -> ParameterSets {
    inspect_avc_hevc_packet(packet, stream_type).1
}

pub fn packet_has_decoder_init(packet: &Packet, stream_type: u8) -> bool {
    let wanted = parameter_types(stream_type);
    if wanted.is_empty() {
        return false;
    }
    let found = extract_parameter_sets(packet, stream_type);
    wanted.iter().all(|kind| found.contains_key(kind))
}

pub fn build_parameter_set_packets(
    pid: u16,
    params: &ParameterSets,
    stream_type: u8,
) -> Vec<Packet> {
    let mut payload = Vec::new();
    for kind in parameter_types(stream_type) {
        let Some(bytes) = params.get(kind) else {
            return Vec::new();
        };
        if bytes.is_empty() {
            return Vec::new();
        }
        payload.extend_from_slice(bytes);
    }
    if payload.is_empty() {
        return Vec::new();
    }
    let mut pes = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0];
    pes.extend_from_slice(&payload);
    let length = pes.len() - 6;
    pes[4] = ((length >> 8) & 255) as u8;
    pes[5] = (length & 255) as u8;
    let mut packets = Vec::new();
    for (index, chunk) in pes.chunks(TS_PACKET_SIZE - 4).enumerate() {
        let mut packet = [0xff; TS_PACKET_SIZE];
        packet[0] = 0x47;
        packet[1] = (if index == 0 { 0x40 } else { 0 }) | ((pid >> 8) as u8 & 31);
        packet[2] = pid as u8;
        let payload_start = if chunk.len() < TS_PACKET_SIZE - 4 {
            let stuffing = TS_PACKET_SIZE - 4 - chunk.len();
            packet[3] = 0x30;
            packet[4] = (stuffing - 1) as u8;
            if stuffing > 1 {
                packet[5] = 0;
            }
            4 + stuffing
        } else {
            packet[3] = 0x10;
            4
        };
        packet[payload_start..].copy_from_slice(chunk);
        packets.push(packet);
    }
    packets
}
