//! Test-only JSONL adapter; production media never uses hex or JSON framing.
use dispatcharr_hls_core::{
    parser::*, render_media_playlist, Segment, SegmentDescriptor, SegmenterConfig, TSSegmenter,
};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};

fn decode(value: &Value) -> Result<Vec<u8>, String> {
    let text = value.as_str().ok_or("hex must be a string")?;
    if text.len() > 8 * 1024 * 1024 || !text.len().is_multiple_of(2) {
        return Err("invalid hex length".into());
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let nibble = |byte: u8| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                _ => None,
            };
            Ok(
                nibble(pair[0]).ok_or("invalid hex")? * 16
                    + nibble(pair[1]).ok_or("invalid hex")?,
            )
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[(byte >> 4) as usize] as char);
        text.push(DIGITS[(byte & 15) as usize] as char);
    }
    text
}

fn segment(segment: Segment) -> Value {
    json!({"data":hex(&segment.data),"duration":segment.duration,"discontinuity":segment.discontinuity})
}

fn state(segmenter: &TSSegmenter) -> Value {
    json!({"collecting":segmenter.collecting(),"current":hex(segmenter.current_data()),
        "video_detected":segmenter.video_detected(),"audio_only":segmenter.audio_only(),
        "param_cache_complete":segmenter.parameter_cache_complete()})
}

fn execute(command: Value, segmenter: &mut TSSegmenter) -> Result<Value, String> {
    match command["op"].as_str().ok_or("missing operation")? {
        "new" => {
            *segmenter = TSSegmenter::new(SegmenterConfig {
                target_duration: command["target"].as_f64().unwrap_or(4.0),
                max_segment_duration: command["ceiling"].as_f64().filter(|value| *value != 0.0),
                startup_keyframe_cuts: command["startup_cuts"].as_u64().unwrap_or(4) as usize,
                max_segment_bytes: command["max_segment_bytes"]
                    .as_u64()
                    .unwrap_or(32 * 1024 * 1024) as usize,
                ..SegmenterConfig::default()
            })
            .map_err(|error| format!("{error:?}"))?;
            Ok(json!({"state":state(segmenter)}))
        }
        "feed" => {
            let bytes = decode(&command["data"])?;
            let segments = segmenter
                .feed(&bytes)
                .map_err(|error| format!("{error:?}"))?;
            Ok(
                json!({"segments":segments.into_iter().map(segment).collect::<Vec<_>>(),"state":state(segmenter)}),
            )
        }
        "discontinuity" => Ok(
            json!({"segment":segmenter.flag_discontinuity().map(segment),"state":state(segmenter)}),
        ),
        "packet" => {
            let bytes = decode(&command["data"])?;
            let packet: Packet = bytes
                .as_slice()
                .try_into()
                .map_err(|_| "packet must contain 188 bytes")?;
            let stream_type = command["stream_type"].as_u64().unwrap_or(0) as u8;
            let params = extract_parameter_sets(&packet, stream_type)
                .into_iter()
                .map(|(kind, bytes)| (kind.to_string(), Value::String(hex(&bytes))))
                .collect::<serde_json::Map<_, _>>();
            let pmt =
                parse_pmt(&packet).map_or(json!([null, null]), |(pid, kind)| json!([pid, kind]));
            Ok(
                json!({"pid":packet_pid(&packet),"cc":packet_continuity_counter(&packet),
                "pat":parse_pat(&packet),"pmt":pmt,"streams":parse_pmt_streams(&packet),
                "pts":extract_pts(&packet),"keyframe":starts_keyframe(&packet,stream_type),
                "parameters":params,"decoder_init":packet_has_decoder_init(&packet,stream_type)}),
            )
        }
        "playlist" => {
            let window = command["window"]
                .as_array()
                .ok_or("window missing")?
                .iter()
                .map(|entry| {
                    Ok(SegmentDescriptor {
                        seq: entry["seq"].as_u64().ok_or("sequence missing")?,
                        duration: entry["dur"].as_f64().ok_or("duration missing")?,
                        discontinuity: entry["disc"].as_bool().unwrap_or(false),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(
                json!({"playlist":render_media_playlist(&window, command["target"].as_f64().unwrap_or(4.0),
                command["segment_name"].as_str().unwrap_or("{seq}.ts"),command["adv_target"].as_u64(),
                command["disc_sequence"].as_u64().unwrap_or(0),command["start_behind_seconds"].as_f64())}),
            )
        }
        other => Err(format!("unknown operation {other}")),
    }
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let mut segmenter = TSSegmenter::new(SegmenterConfig::default()).unwrap();
    for line in stdin.lock().lines() {
        let response = line
            .map_err(|error| error.to_string())
            .and_then(|line| serde_json::from_str(&line).map_err(|error| error.to_string()))
            .and_then(|command| execute(command, &mut segmenter));
        let value = match response {
            Ok(value) => value,
            Err(error) => json!({"error":error}),
        };
        writeln!(stdout, "{value}").unwrap();
        stdout.flush().unwrap();
    }
}
