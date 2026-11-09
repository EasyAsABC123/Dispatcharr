use crate::{parser::*, PTS_WRAP_SECONDS, TS_PACKET_SIZE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidConfiguration,
    InputTooLarge,
    SegmentTooLarge,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Segment {
    pub data: Vec<u8>,
    pub duration: f64,
    pub discontinuity: bool,
}

#[derive(Clone, Debug)]
pub struct SegmenterConfig {
    pub target_duration: f64,
    pub max_segment_duration: Option<f64>,
    pub startup_keyframe_cuts: usize,
    pub max_segment_bytes: usize,
    pub max_input_bytes: usize,
}

impl Default for SegmenterConfig {
    fn default() -> Self {
        Self {
            target_duration: 4.0,
            max_segment_duration: None,
            startup_keyframe_cuts: 4,
            max_segment_bytes: 32 * 1024 * 1024,
            max_input_bytes: 4 * 1024 * 1024,
        }
    }
}

pub struct TSSegmenter {
    config: SegmenterConfig,
    ceiling: f64,
    startup_cuts_remaining: usize,
    pending: Vec<u8>,
    current: Vec<u8>,
    pat: Option<Packet>,
    pmt: Option<Packet>,
    pmt_pid: Option<u16>,
    video_pid: Option<u16>,
    video_type: Option<u8>,
    audio_pid: Option<u16>,
    audio_only: bool,
    segment_start_pts: Option<f64>,
    first_pts: Option<f64>,
    last_pts: Option<f64>,
    collecting: bool,
    pending_discontinuity: bool,
    current_discontinuity: bool,
    opened_mid_gop: bool,
    params: ParameterSets,
}

impl TSSegmenter {
    pub fn new(config: SegmenterConfig) -> Result<Self, Error> {
        let ceiling = config
            .max_segment_duration
            .unwrap_or(2.0 * config.target_duration);
        if !config.target_duration.is_finite()
            || config.target_duration <= 0.0
            || !ceiling.is_finite()
            || ceiling <= 0.0
            || config.max_segment_bytes < TS_PACKET_SIZE
            || config.max_input_bytes < TS_PACKET_SIZE
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self {
            ceiling,
            startup_cuts_remaining: config.startup_keyframe_cuts,
            config,
            pending: Vec::new(),
            current: Vec::new(),
            pat: None,
            pmt: None,
            pmt_pid: None,
            video_pid: None,
            video_type: None,
            audio_pid: None,
            audio_only: false,
            segment_start_pts: None,
            first_pts: None,
            last_pts: None,
            collecting: false,
            pending_discontinuity: false,
            current_discontinuity: false,
            opened_mid_gop: false,
            params: ParameterSets::new(),
        })
    }

    pub fn video_detected(&self) -> bool {
        self.video_pid.is_some()
    }
    pub fn audio_only(&self) -> bool {
        self.audio_only
    }
    pub fn collecting(&self) -> bool {
        self.collecting
    }
    pub fn current_data(&self) -> &[u8] {
        &self.current
    }
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }
    pub fn parameter_cache_complete(&self) -> bool {
        let wanted = parameter_types(self.video_type.unwrap_or(0));
        !wanted.is_empty() && wanted.iter().all(|kind| self.params.contains_key(kind))
    }

    pub fn flag_discontinuity(&mut self) -> Option<Segment> {
        let finished = if self.collecting {
            let span = match (self.first_pts, self.last_pts) {
                (Some(first), Some(last)) => elapsed(last, first),
                _ => 0.0,
            };
            (span > 0.0).then(|| self.finish_segment(span))
        } else {
            None
        };
        self.collecting = false;
        self.current.clear();
        self.current_discontinuity = false;
        self.segment_start_pts = None;
        self.first_pts = None;
        self.last_pts = None;
        self.pending_discontinuity = true;
        self.opened_mid_gop = false;
        self.params.clear();
        finished
    }

    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<Segment>, Error> {
        if data.len() > self.config.max_input_bytes {
            return Err(Error::InputTooLarge);
        }
        // Avoid geometric Vec growth beyond the explicitly bounded input plus
        // at most 187 pending bytes when a maximum-size read arrives.
        self.pending.reserve_exact(data.len());
        self.pending.extend_from_slice(data);
        let mut cursor = 0;
        let mut segments = Vec::new();
        while self.pending.len() - cursor >= TS_PACKET_SIZE {
            if self.pending[cursor] != 0x47 {
                match self.pending[cursor..].iter().position(|byte| *byte == 0x47) {
                    Some(sync) => {
                        cursor += sync;
                        continue;
                    }
                    None => {
                        cursor = self.pending.len();
                        break;
                    }
                }
            }
            if self.pending.len() - cursor > TS_PACKET_SIZE
                && self.pending[cursor + TS_PACKET_SIZE] != 0x47
            {
                cursor += 1;
                continue;
            }
            let packet: Packet = self.pending[cursor..cursor + TS_PACKET_SIZE]
                .try_into()
                .unwrap();
            cursor += TS_PACKET_SIZE;
            match self.handle_packet(&packet) {
                Ok(Some(segment)) => segments.push(segment),
                Ok(None) => {}
                Err(error) => {
                    self.pending.clear();
                    self.flag_discontinuity();
                    return Err(error);
                }
            }
        }
        if cursor > 0 {
            self.pending.drain(..cursor);
        }
        Ok(segments)
    }

    fn append(&mut self, data: &[u8]) -> Result<(), Error> {
        if data.len()
            > self
                .config
                .max_segment_bytes
                .saturating_sub(self.current.len())
        {
            return Err(Error::SegmentTooLarge);
        }
        self.current.extend_from_slice(data);
        Ok(())
    }

    fn handle_packet(&mut self, packet: &Packet) -> Result<Option<Segment>, Error> {
        let pid = packet_pid(packet);
        if pid == 0 {
            self.pat = Some(*packet);
            if self.pmt_pid.is_none() {
                self.pmt_pid = parse_pat(packet);
            }
            return Ok(None);
        }
        if self.pmt_pid == Some(pid) {
            self.pmt = Some(*packet);
            let Some(streams) = parse_pmt_streams(packet) else {
                return Ok(None);
            };
            let mut video = None;
            let mut audio = None;
            for (kind, pid) in streams {
                if video.is_none() && video_stream_type(kind) {
                    video = Some((pid, kind));
                } else if audio.is_none() && audio_stream_type(kind) {
                    audio = Some(pid);
                }
            }
            if let Some((pid, kind)) = video {
                if Some(pid) != self.video_pid || Some(kind) != self.video_type {
                    self.params.clear();
                }
                self.video_pid = Some(pid);
                self.video_type = Some(kind);
                self.audio_only = false;
                self.audio_pid = audio;
            } else if audio.is_some() {
                self.audio_pid = audio;
                self.audio_only = true;
                self.video_pid = None;
                self.video_type = None;
                self.params.clear();
            }
            return Ok(None);
        }
        if self.video_pid.is_none() && !self.audio_only {
            return Ok(None);
        }
        if self.audio_only {
            return self.handle_audio_packet(packet, pid);
        }
        let mut finished = None;
        if self.video_pid == Some(pid) && packet_pusi(packet) {
            let pts = extract_pts(packet);
            let kind = self.video_type.unwrap_or(0);
            let (keyframe, found) = if matches!(kind, 0x1b | 0x24) {
                let (keyframe, found) = inspect_avc_hevc_packet(packet, kind);
                self.params.extend(found.clone());
                (packet_random_access(packet) || keyframe, found)
            } else {
                (starts_keyframe(packet, kind), ParameterSets::new())
            };
            if self.collecting
                && pts
                    .zip(self.segment_start_pts)
                    .is_some_and(|(pts, start)| timeline_reset(pts, start))
            {
                finished = self.flag_discontinuity();
                if keyframe {
                    self.begin_segment(pts, Some(packet), &found, false)?;
                    self.append(packet)?;
                }
                return Ok(finished);
            }
            if !self.collecting {
                if keyframe {
                    self.begin_segment(pts, Some(packet), &found, false)?;
                }
            } else if keyframe && pts.is_some() {
                let pts = pts.unwrap();
                if let Some(start) = self.segment_start_pts {
                    let elapsed = elapsed(pts, start);
                    let cut_at = if self.opened_mid_gop {
                        0.0
                    } else if self.startup_cuts_remaining > 0 {
                        1.0
                    } else {
                        self.config.target_duration
                    };
                    if elapsed >= cut_at && elapsed > 0.0 {
                        finished = Some(self.finish_segment(self.extinf_duration(Some(elapsed))));
                        self.begin_segment(Some(pts), Some(packet), &found, false)?;
                    } else {
                        self.note_pts(pts);
                    }
                } else {
                    finished = Some(self.finish_segment(self.extinf_duration(None)));
                    self.begin_segment(Some(pts), Some(packet), &found, false)?;
                }
            } else if let Some(pts) = pts {
                if self.collecting {
                    if let Some(start) = self.segment_start_pts {
                        let elapsed = elapsed(pts, start);
                        if elapsed >= self.ceiling {
                            finished =
                                Some(self.finish_segment(self.extinf_duration(Some(elapsed))));
                            self.begin_segment(Some(pts), None, &ParameterSets::new(), true)?;
                        } else {
                            self.note_pts(pts);
                        }
                    } else {
                        self.note_pts(pts);
                    }
                }
            }
        }
        if self.collecting {
            self.append(packet)?;
        }
        Ok(finished)
    }

    fn handle_audio_packet(&mut self, packet: &Packet, pid: u16) -> Result<Option<Segment>, Error> {
        let mut finished = None;
        if self.audio_pid == Some(pid) && packet_pusi(packet) {
            let pts = extract_pts(packet);
            if self.collecting
                && pts
                    .zip(self.segment_start_pts)
                    .is_some_and(|(pts, start)| timeline_reset(pts, start))
            {
                finished = self.flag_discontinuity();
                self.begin_segment(pts, None, &ParameterSets::new(), false)?;
                self.append(packet)?;
                return Ok(finished);
            }
            if !self.collecting {
                if pts.is_some() {
                    self.begin_segment(pts, None, &ParameterSets::new(), false)?;
                }
            } else if let (Some(pts), Some(start)) = (pts, self.segment_start_pts) {
                let elapsed = elapsed(pts, start);
                if elapsed >= self.config.target_duration && elapsed > 0.0 {
                    finished = Some(self.finish_segment(elapsed));
                    self.begin_segment(Some(pts), None, &ParameterSets::new(), false)?;
                } else {
                    self.note_pts(pts);
                }
            }
        }
        if self.collecting {
            self.append(packet)?;
        }
        Ok(finished)
    }

    fn note_pts(&mut self, pts: f64) {
        if let (Some(first), Some(last)) = (self.first_pts, self.last_pts) {
            if elapsed(pts, first) >= elapsed(last, first) {
                self.last_pts = Some(pts);
            }
        } else {
            self.first_pts = Some(pts);
            self.last_pts = Some(pts);
        }
    }

    fn measured_span(&self) -> Option<f64> {
        let span = elapsed(self.last_pts?, self.first_pts?);
        (span > 0.0 && span <= 4.0 * self.config.target_duration).then_some(span)
    }

    fn extinf_duration(&self, boundary: Option<f64>) -> f64 {
        match (
            self.measured_span(),
            boundary.filter(|elapsed| *elapsed > 0.0),
        ) {
            (Some(span), Some(boundary)) => span.max(boundary),
            (Some(span), None) => span,
            (None, Some(boundary)) => boundary,
            (None, None) => self.config.target_duration,
        }
    }

    fn begin_segment(
        &mut self,
        pts: Option<f64>,
        opening: Option<&Packet>,
        found: &ParameterSets,
        mid_gop: bool,
    ) -> Result<(), Error> {
        self.current.clear();
        self.opened_mid_gop = mid_gop;
        if let Some(packet) = self.pat {
            self.append(&packet)?;
        }
        if let Some(packet) = self.pmt {
            self.append(&packet)?;
        }
        if let Some(opening) = opening {
            self.inject_parameters(opening, found)?;
        }
        self.segment_start_pts = pts;
        self.first_pts = pts;
        self.last_pts = pts;
        self.collecting = true;
        self.current_discontinuity = self.pending_discontinuity;
        self.pending_discontinuity = false;
        Ok(())
    }

    fn inject_parameters(&mut self, opening: &Packet, found: &ParameterSets) -> Result<(), Error> {
        let Some(pid) = self.video_pid else {
            return Ok(());
        };
        let kind = self.video_type.unwrap_or(0);
        if !matches!(kind, 0x1b | 0x24) {
            return Ok(());
        }
        self.params.extend(found.clone());
        let wanted = parameter_types(kind);
        if wanted.iter().all(|kind| found.contains_key(kind)) || !self.parameter_cache_complete() {
            return Ok(());
        }
        let packets = build_parameter_set_packets(pid, &self.params, kind);
        let first_cc =
            (usize::from(packet_continuity_counter(opening)) + 16 - packets.len() % 16) & 15;
        for (index, mut packet) in packets.into_iter().enumerate() {
            packet[3] = (packet[3] & 0xf0) | ((first_cc + index) & 15) as u8;
            self.append(&packet)?;
        }
        Ok(())
    }

    fn finish_segment(&mut self, mut duration: f64) -> Segment {
        if duration <= 0.0 || duration > 4.0 * self.config.target_duration {
            duration = self.config.target_duration;
        }
        let segment = Segment {
            data: std::mem::take(&mut self.current),
            duration,
            discontinuity: self.current_discontinuity,
        };
        self.current_discontinuity = false;
        self.startup_cuts_remaining = self.startup_cuts_remaining.saturating_sub(1);
        segment
    }
}

pub fn elapsed(pts: f64, start: f64) -> f64 {
    let mut delta = pts - start;
    if delta < -PTS_WRAP_SECONDS / 2.0 {
        delta += PTS_WRAP_SECONDS;
    } else if delta > PTS_WRAP_SECONDS / 2.0 {
        delta -= PTS_WRAP_SECONDS;
    }
    delta
}

fn timeline_reset(pts: f64, start: f64) -> bool {
    let raw = pts - start;
    (-PTS_WRAP_SECONDS / 2.0..-2.0).contains(&raw)
}
