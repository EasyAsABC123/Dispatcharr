#[derive(Clone, Debug)]
pub struct SegmentDescriptor {
    pub seq: u64,
    pub duration: f64,
    pub discontinuity: bool,
}

/// Existing TS media playlist layout. `segment_name` supports the internal
/// `{seq}` placeholder used by Dispatcharr's HLS routes, not arbitrary Python
/// format mini-language expressions.
pub fn render_media_playlist(
    window: &[SegmentDescriptor],
    target_duration: f64,
    segment_name: &str,
    advertised_target: Option<u64>,
    discontinuity_sequence: u64,
    start_behind_seconds: Option<f64>,
) -> String {
    let advertised = advertised_target.filter(|target| *target != 0);
    if window.is_empty() {
        let target = advertised.unwrap_or((target_duration.max(1.0) + 0.999) as u64);
        return format!(
            "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:{target}\n#EXT-X-MEDIA-SEQUENCE:0\n"
        );
    }
    // Preserve Python's existing int(value + .999), including its boundary
    // behavior; replacing it with ceil() would change the compatibility output.
    let target = advertised.unwrap_or(
        (window
            .iter()
            .map(|segment| segment.duration)
            .fold(f64::NEG_INFINITY, f64::max)
            + 0.999) as u64,
    );
    let mut lines = vec![
        "#EXTM3U".into(),
        "#EXT-X-VERSION:3".into(),
        format!("#EXT-X-TARGETDURATION:{target}"),
        format!("#EXT-X-MEDIA-SEQUENCE:{}", window[0].seq),
    ];
    if discontinuity_sequence > 0 {
        lines.push(format!(
            "#EXT-X-DISCONTINUITY-SEQUENCE:{discontinuity_sequence}"
        ));
    }
    let total: f64 = window.iter().map(|segment| segment.duration).sum();
    if let Some(behind) = start_behind_seconds.filter(|behind| *behind > 0.0 && total >= *behind) {
        lines.push(format!("#EXT-X-START:TIME-OFFSET=-{behind:.3},PRECISE=YES"));
    }
    for segment in window {
        if segment.discontinuity {
            lines.push("#EXT-X-DISCONTINUITY".into());
        }
        lines.push(format!("#EXTINF:{:.3},", segment.duration));
        lines.push(segment_name.replace("{seq}", &segment.seq.to_string()));
    }
    lines.join("\n") + "\n"
}
