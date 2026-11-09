//! Component decoder fixture helper, not an HLS session/HTTP implementation.
use dispatcharr_hls_core::{
    render_media_playlist, Segment, SegmentDescriptor, SegmenterConfig, TSSegmenter,
};
use serde_json::json;
use std::{
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

fn save(
    segment: Segment,
    output: &Path,
    descriptors: &mut Vec<SegmentDescriptor>,
    records: &mut Vec<serde_json::Value>,
) -> io::Result<()> {
    let sequence = descriptors.len() as u64 + 1;
    let filename = format!("{sequence}.ts");
    fs::write(output.join(&filename), &segment.data)?;
    records.push(
        json!({"seq":sequence,"duration":segment.duration,"discontinuity":segment.discontinuity,
        "filename":filename,"bytes":segment.data.len()}),
    );
    descriptors.push(SegmentDescriptor {
        seq: sequence,
        duration: segment.duration,
        discontinuity: segment.discontinuity,
    });
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: segment_file INPUT.ts NEW_OUTPUT_DIR".into());
    }
    let output = PathBuf::from(&args[2]);
    if output.exists() && fs::read_dir(&output)?.next().is_some() {
        return Err("output directory must be empty".into());
    }
    fs::create_dir_all(&output)?;
    let mut input = fs::File::open(&args[1])?;
    let mut buffer = [0; 64 * 1024];
    let mut segmenter =
        TSSegmenter::new(SegmenterConfig::default()).map_err(|error| format!("{error:?}"))?;
    let mut descriptors = Vec::new();
    let mut records = Vec::new();
    loop {
        let length = input.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        for segment in segmenter
            .feed(&buffer[..length])
            .map_err(|error| format!("{error:?}"))?
        {
            save(segment, &output, &mut descriptors, &mut records)?;
        }
    }
    // Finite fixture files close their measured tail. Live HLS uses explicit
    // discontinuity/stop lifecycle instead of treating every read as EOF.
    if let Some(tail) = segmenter.flag_discontinuity() {
        save(tail, &output, &mut descriptors, &mut records)?;
    }
    fs::write(
        output.join("index.m3u8"),
        render_media_playlist(&descriptors, 4.0, "{seq}.ts", Some(8), 0, None),
    )?;
    fs::write(
        output.join("segments.json"),
        serde_json::to_vec_pretty(&json!({
        "scope":"TS-HLS-component-segmentation","application_performance_verified":false,"segments":records}))?,
    )?;
    println!(
        "{}",
        json!({"segments":descriptors.len(),"scope":"component-only"})
    );
    Ok(())
}
