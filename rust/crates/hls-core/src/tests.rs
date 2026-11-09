//! Expectations ported from the original Dispatcharr HLS segmenter test suite.
use super::{parser::*, *};

const PID: u16 = 256;
const PMT: u16 = 4096;

fn packet(pid: u16, payload: &[u8], pusi: bool, rai: bool) -> Packet {
    let mut packet = [0xff; 188];
    packet[..4].copy_from_slice(&[
        0x47,
        (if pusi { 0x40 } else { 0 }) | (pid >> 8) as u8,
        pid as u8,
        if rai { 0x30 } else { 0x10 },
    ]);
    let offset = if rai {
        let padding = 188 - 4 - 2 - payload.len();
        packet[4] = (1 + padding) as u8;
        packet[5] = 0x40;
        6 + padding
    } else {
        4
    };
    packet[offset..offset + payload.len()].copy_from_slice(payload);
    packet
}

fn pat() -> Packet {
    packet(
        0,
        &[
            0,
            0,
            0xb0,
            13,
            0,
            1,
            0xc1,
            0,
            0,
            0,
            1,
            0xe0 | (PMT >> 8) as u8,
            PMT as u8,
            0,
            0,
            0,
            0,
        ],
        true,
        false,
    )
}

fn pmt(kind: u8) -> Packet {
    packet(
        PMT,
        &[
            0,
            2,
            0xb0,
            18,
            0,
            1,
            0xc1,
            0,
            0,
            0xe0 | (PID >> 8) as u8,
            PID as u8,
            0xf0,
            0,
            kind,
            0xe0 | (PID >> 8) as u8,
            PID as u8,
            0xf0,
            0,
            0,
            0,
            0,
            0,
        ],
        true,
        false,
    )
}

fn pes(pts: f64, units: &[u8], rai: bool) -> Packet {
    let pts = (pts * 90_000.0) as u64;
    let mut payload = vec![
        0,
        0,
        1,
        0xe0,
        0,
        0,
        0x80,
        0x80,
        5,
        0x21 | (((pts >> 30) & 7) << 1) as u8,
        (pts >> 22) as u8,
        1 | (((pts >> 15) & 0x7f) << 1) as u8,
        (pts >> 7) as u8,
        1 | ((pts & 0x7f) << 1) as u8,
    ];
    payload.extend_from_slice(units);
    packet(PID, &payload, true, rai)
}

fn avc(pts: f64, nals: &[u8]) -> Packet {
    let units = nals
        .iter()
        .flat_map(|nal| [0, 0, 0, 1, 0x60 | (nal & 31), 1])
        .collect::<Vec<_>>();
    pes(pts, &units, false)
}

fn hevc(pts: f64, nals: &[u8]) -> Packet {
    let units = nals
        .iter()
        .flat_map(|nal| [0, 0, 0, 1, nal << 1, 1])
        .collect::<Vec<_>>();
    pes(pts, &units, false)
}

fn started(kind: u8, target: f64) -> TSSegmenter {
    let mut segmenter = TSSegmenter::new(SegmenterConfig {
        target_duration: target,
        startup_keyframe_cuts: 0,
        ..SegmenterConfig::default()
    })
    .unwrap();
    segmenter.feed(&pat()).unwrap();
    segmenter.feed(&pmt(kind)).unwrap();
    segmenter
}

#[test]
fn pat_pmt_pts_and_truncated_pmt_match_original_expectations() {
    assert_eq!(parse_pat(&pat()), Some(PMT));
    assert_eq!(parse_pmt(&pmt(0x1b)), Some((PID, 0x1b)));
    assert_eq!(parse_pmt_streams(&pmt(0x0f)), Some(vec![(0x0f, PID)]));
    assert_eq!(extract_pts(&avc(1234.5, &[5])), Some(1234.5));
    let mut incomplete = pmt(0x0f);
    incomplete[6] = 0xb0;
    incomplete[7] = 188;
    assert_eq!(parse_pmt_streams(&incomplete), None);
}

#[test]
fn codec_aware_keyframe_detection_does_not_confuse_repeated_parameters_or_mpeg_slices() {
    assert!(starts_keyframe(&avc(0.0, &[9, 7, 8, 5]), 0x1b));
    assert!(starts_keyframe(&avc(0.0, &[9, 7, 8]), 0x1b));
    assert!(!starts_keyframe(&avc(0.0, &[9, 7, 8, 1]), 0x1b));
    assert!(!starts_keyframe(&hevc(0.0, &[34]), 0x24));
    assert!(!starts_keyframe(&hevc(0.0, &[32, 1]), 0x24));
    assert!(starts_keyframe(&hevc(0.0, &[34, 21]), 0x24));
    assert!(starts_keyframe(&pes(0.0, &[0, 0, 1, 0, 0, 8], false), 2));
    assert!(!starts_keyframe(
        &pes(0.0, &[0, 0, 1, 0, 0, 16, 0, 0, 1, 5], false),
        2
    ));
    assert!(starts_keyframe(&pes(0.0, &[0, 0, 1, 0x41], true), 0x1b));
}

#[test]
fn startup_ladder_uses_one_second_floor_then_returns_to_target() {
    let mut segmenter = TSSegmenter::new(SegmenterConfig {
        startup_keyframe_cuts: 3,
        ..SegmenterConfig::default()
    })
    .unwrap();
    segmenter.feed(&[pat(), pmt(0x1b)].concat()).unwrap();
    let mut durations = Vec::new();
    for index in 0..9 {
        durations.extend(
            segmenter
                .feed(&avc(index as f64 * 2.0, &[5]))
                .unwrap()
                .into_iter()
                .map(|s| s.duration),
        );
    }
    assert_eq!(durations, [2.0, 2.0, 2.0, 4.0, 4.0]);
    let mut burst = TSSegmenter::new(SegmenterConfig {
        startup_keyframe_cuts: 2,
        ..SegmenterConfig::default()
    })
    .unwrap();
    burst.feed(&[pat(), pmt(0x1b)].concat()).unwrap();
    for pts in [0.0, 0.033, 0.5, 0.999] {
        assert!(burst.feed(&avc(pts, &[5])).unwrap().is_empty());
    }
    assert_eq!(burst.feed(&avc(1.0, &[5])).unwrap()[0].duration, 1.0);
}

#[test]
fn cached_parameter_sets_are_injected_with_preceding_continuity_counter() {
    let mut segmenter = started(0x1b, 4.0);
    segmenter.feed(&avc(0.0, &[7, 8, 5])).unwrap();
    segmenter.feed(&avc(4.0, &[5])).unwrap();
    let out = segmenter.feed(&avc(8.0, &[5])).unwrap();
    let injected: Packet = out[0].data[376..564].try_into().unwrap();
    let idr: Packet = out[0].data[564..752].try_into().unwrap();
    assert_eq!(packet_pid(&injected), PID);
    assert!(packet_has_decoder_init(&injected, 0x1b));
    assert_eq!(
        (packet_continuity_counter(&injected) + 1) & 15,
        packet_continuity_counter(&idr)
    );
    segmenter.flag_discontinuity();
    assert!(!segmenter.parameter_cache_complete());
}

#[test]
fn hevc_parameter_injection_contains_vps_sps_pps() {
    let mut segmenter = started(0x24, 2.0);
    segmenter.feed(&hevc(0.0, &[32, 33, 34, 21])).unwrap();
    segmenter.feed(&hevc(2.0, &[21])).unwrap();
    let out = segmenter.feed(&hevc(4.0, &[21])).unwrap();
    let injected: Packet = out[0].data[376..564].try_into().unwrap();
    assert!(packet_has_decoder_init(&injected, 0x24));
}

#[test]
fn discontinuity_cuts_measured_tail_and_tags_only_next_playable_segment() {
    let mut segmenter = started(0x1b, 4.0);
    segmenter.feed(&avc(0.0, &[5])).unwrap();
    for pts in [0.5, 1.0, 1.5] {
        segmenter.feed(&avc(pts, &[1])).unwrap();
    }
    let length = segmenter.current_data().len();
    let tail = segmenter.flag_discontinuity().unwrap();
    assert_eq!(tail.duration, 1.5);
    assert_eq!(tail.data.len(), length);
    assert!(!tail.discontinuity);
    assert!(!segmenter.collecting());
    segmenter.feed(&avc(9000.2, &[1])).unwrap();
    assert!(!segmenter.collecting());
    segmenter.feed(&avc(9001.0, &[5])).unwrap();
    assert!(segmenter.feed(&avc(9006.0, &[5])).unwrap()[0].discontinuity);
}

#[test]
fn open_gop_presentation_max_outlives_next_keyframe_boundary() {
    let mut segmenter = started(0x1b, 4.0);
    segmenter.feed(&avc(0.0, &[5])).unwrap();
    for pts in [0.5, 1.0, 3.9, 4.15, 4.28] {
        segmenter.feed(&avc(pts, &[1])).unwrap();
    }
    assert_eq!(segmenter.feed(&avc(4.104, &[5])).unwrap()[0].duration, 4.28);
}

#[test]
fn wrap_is_not_encoder_reset_but_large_backward_pts_is() {
    let mut segmenter = started(0x1b, 2.0);
    segmenter.feed(&avc(PTS_WRAP_SECONDS - 1.0, &[5])).unwrap();
    let wrapped = segmenter.feed(&avc(1.0, &[5])).unwrap();
    assert_eq!(wrapped.len(), 1);
    assert!(!wrapped[0].discontinuity);
    assert_eq!(wrapped[0].duration, 2.0);
    let mut reset = started(0x1b, 2.0);
    reset.feed(&avc(100.0, &[7, 8, 5])).unwrap();
    reset.feed(&avc(101.0, &[1])).unwrap();
    reset.feed(&avc(0.0, &[7, 8, 5])).unwrap();
    assert!(reset.parameter_cache_complete());
    assert!(reset.feed(&avc(2.0, &[5])).unwrap()[0].discontinuity);
}

#[test]
fn audio_only_mode_cuts_on_audio_pts_immediately() {
    let mut segmenter = started(0x0f, 4.0);
    assert!(segmenter.audio_only());
    assert!(!segmenter.video_detected());
    segmenter
        .feed(&pes(10.0, &[0xff, 0xf1, 0x50], false))
        .unwrap();
    assert!(segmenter
        .feed(&pes(12.0, &[0xff, 0xf1, 0x50], false))
        .unwrap()
        .is_empty());
    assert_eq!(
        segmenter
            .feed(&pes(14.0, &[0xff, 0xf1, 0x50], false))
            .unwrap()[0]
            .duration,
        4.0
    );
}

#[test]
fn forced_mid_gop_segment_closes_at_next_keyframe_then_recovers_target() {
    let mut segmenter = TSSegmenter::new(SegmenterConfig {
        target_duration: 3.9,
        max_segment_duration: Some(6.45),
        startup_keyframe_cuts: 0,
        ..SegmenterConfig::default()
    })
    .unwrap();
    segmenter.feed(&[pat(), pmt(0x1b)].concat()).unwrap();
    segmenter.feed(&avc(0.0, &[5])).unwrap();
    assert!(segmenter.feed(&avc(6.44, &[1])).unwrap().is_empty());
    assert_eq!(segmenter.feed(&avc(6.45, &[1])).unwrap()[0].duration, 6.45);
    assert_eq!(segmenter.feed(&avc(6.95, &[5])).unwrap()[0].duration, 0.5);
    assert!(segmenter.feed(&avc(8.95, &[5])).unwrap().is_empty());
    assert!((segmenter.feed(&avc(10.95, &[5])).unwrap()[0].duration - 4.0).abs() < 0.0001);
}

#[test]
fn packet_chunking_and_garbage_resync_do_not_change_segment_bytes() {
    let mut bytes = vec![0xde; 132];
    bytes.extend_from_slice(&[pat(), pmt(0x1b)].concat());
    for index in 0..10 {
        bytes.extend_from_slice(&avc(index as f64 * 2.0, &[5]));
    }
    let mut expected = TSSegmenter::new(SegmenterConfig::default()).unwrap();
    let expected = expected.feed(&bytes).unwrap();
    for size in [1, 7, 187, 188, 189, 377, 8192] {
        let mut segmenter = TSSegmenter::new(SegmenterConfig::default()).unwrap();
        let actual = bytes
            .chunks(size)
            .flat_map(|chunk| segmenter.feed(chunk).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert!(segmenter.pending_bytes() < 188);
    }
}

#[test]
fn resource_limits_reject_invalid_configuration_and_bound_untimed_media() {
    for target in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert!(matches!(
            TSSegmenter::new(SegmenterConfig {
                target_duration: target,
                ..SegmenterConfig::default()
            }),
            Err(Error::InvalidConfiguration)
        ));
    }
    let mut segmenter = TSSegmenter::new(SegmenterConfig {
        startup_keyframe_cuts: 0,
        max_segment_bytes: 188 * 4,
        max_input_bytes: 188,
        ..SegmenterConfig::default()
    })
    .unwrap();
    assert_eq!(segmenter.feed(&[0; 189]).unwrap_err(), Error::InputTooLarge);
    segmenter.feed(&pat()).unwrap();
    segmenter.feed(&pmt(0x1b)).unwrap();
    segmenter.feed(&avc(0.0, &[5])).unwrap();
    segmenter
        .feed(&packet(PID, &[0; 20], false, false))
        .unwrap();
    assert_eq!(
        segmenter
            .feed(&packet(PID, &[0; 20], false, false))
            .unwrap_err(),
        Error::SegmentTooLarge
    );
    assert!(!segmenter.collecting());
    assert!(segmenter.current_data().is_empty());
    segmenter.feed(&avc(10.0, &[5])).unwrap();
    assert!(segmenter.feed(&avc(14.0, &[5])).unwrap()[0].discontinuity);
}

#[test]
fn playlist_keeps_frozen_target_discontinuity_sequence_and_join_offset() {
    let entries = vec![
        SegmentDescriptor {
            seq: 12,
            duration: 4.0,
            discontinuity: true,
        },
        SegmentDescriptor {
            seq: 13,
            duration: 4.6,
            discontinuity: false,
        },
    ];
    let text = render_media_playlist(&entries, 4.0, "{seq}.ts", Some(8), 2, Some(5.0));
    assert!(text.contains("#EXT-X-TARGETDURATION:8\n"));
    assert!(text.contains("#EXT-X-DISCONTINUITY-SEQUENCE:2\n"));
    assert!(text.contains("#EXT-X-START:TIME-OFFSET=-5.000,PRECISE=YES\n"));
    assert!(text.contains("#EXT-X-DISCONTINUITY\n#EXTINF:4.000,\n12.ts\n"));
    assert!(!text.contains("#EXT-X-ENDLIST"));
    assert_eq!(
        render_media_playlist(&[], 4.0, "{seq}.ts", None, 2, Some(5.0)),
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:0\n"
    );
}
