//! Source VOD timeshift aliases, provider URL projection and session identity.
//! Dispatcharr's original authors retain application and algorithm credit.
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_ingest_core::source_integer_literal;
use regex::Regex;
use std::{borrow::Cow, collections::HashMap, sync::LazyLock};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Timeshift<T> {
    pub(crate) utc_start: T,
    pub(crate) utc_end: T,
    pub(crate) offset: T,
}

impl Timeshift<String> {
    pub(crate) fn borrowed(&self) -> Timeshift<&str> {
        Timeshift {
            utc_start: &self.utc_start,
            utc_end: &self.utc_end,
            offset: &self.offset,
        }
    }
}

impl Timeshift<&str> {
    pub(crate) fn owned(self) -> Timeshift<String> {
        Timeshift {
            utc_start: self.utc_start.into(),
            utc_end: self.utc_end.into(),
            offset: self.offset.into(),
        }
    }
}

fn first_nonempty<'a>(request: &'a PlaybackRequest, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .filter_map(|name| request.parameter(name))
        .find(|value| !value.is_empty())
}

pub(crate) fn requested(request: &PlaybackRequest) -> Timeshift<&str> {
    let utc_start = first_nonempty(request, &["utc_start", "start", "playliststart"]).unwrap_or("");
    let utc_end = first_nonempty(request, &["utc_end", "end", "playlistend"]).unwrap_or("");
    let mut offset = first_nonempty(request, &["offset", "seek", "t"]);
    if utc_start.is_empty() && offset.is_none() {
        // An explicitly empty timestamp shadows the time alias in Source.
        offset = request
            .parameter("timestamp")
            .or_else(|| request.parameter("time"));
    }
    Timeshift {
        utc_start,
        utc_end,
        offset: offset.unwrap_or(""),
    }
}

static CATCHUP_PATH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"/catchup/\d{4}-\d{2}-\d{2}/\d{2}-\d{2}-\d{2}/")
        .expect("static VOD catchup URL pattern")
});

pub(crate) fn provider_url<'a>(original: &'a str, shift: Timeshift<&str>) -> Cow<'a, str> {
    if shift.utc_start.is_empty() && shift.utc_end.is_empty() && shift.offset.is_empty() {
        return Cow::Borrowed(original);
    }
    let (base, fragment) = original.split_once('#').unwrap_or((original, ""));
    let (path, query) = base.split_once('?').unwrap_or((base, ""));
    let mut keys = HashMap::new();
    let mut values: Vec<(String, Vec<String>)> = Vec::new();
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        // urllib.parse.parse_qs drops empty values, including tokens without '='.
        if value.is_empty() {
            continue;
        }
        let key = key.into_owned();
        let index = *keys.entry(key.clone()).or_insert(values.len());
        if index == values.len() {
            values.push((key, Vec::new()));
        }
        values[index].1.push(value.into_owned());
    }
    let mut set_values = |names: &[&str], value: &str| {
        for name in names {
            if let Some(index) = keys.get(*name).copied() {
                values[index].1 = vec![value.to_owned()];
            } else {
                values.push(((*name).to_owned(), vec![value.to_owned()]));
            }
        }
    };
    if !shift.utc_start.is_empty() {
        set_values(&["utc_start", "start"], shift.utc_start);
    }
    if !shift.utc_end.is_empty() {
        set_values(&["utc_end", "end"], shift.utc_end);
    }
    if let Some(seconds) = source_integer_literal(shift.offset) {
        set_values(&["offset", "seek", "t"], &seconds);
    }
    let path = if !shift.utc_start.is_empty() && CATCHUP_PATH.is_match(path) {
        let normalized = shift.utc_start.replace('Z', "+00:00");
        dispatcharr_store::source_iso_local_datetime(&normalized).map_or(
            Cow::Borrowed(path),
            |time| {
                let part = time.format("/catchup/%Y-%m-%d/%H-%M-%S/").to_string();
                CATCHUP_PATH.replace_all(path, part.as_str())
            },
        )
    } else {
        Cow::Borrowed(path)
    };
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(
            values
                .iter()
                .flat_map(|(name, values)| values.iter().map(move |value| (name, value))),
        )
        .finish()
        .replace('*', "%2A")
        .replace("%7E", "~");
    let mut result = path.into_owned();
    if !query.is_empty() {
        result.push('?');
        result.push_str(&query);
    }
    if !fragment.is_empty() {
        result.push('#');
        result.push_str(fragment);
    }
    Cow::Owned(result)
}

pub(crate) fn idle_score(same_timeshift: bool, same_ip: bool, same_user_agent: bool) -> Option<u8> {
    let score =
        10 + u8::from(same_timeshift) * 7 + u8::from(same_ip) * 5 + u8::from(same_user_agent) * 3;
    (score >= 13).then_some(score)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(pairs: &[(&str, &str)]) -> PlaybackRequest {
        PlaybackRequest {
            channel_id: String::new(),
            query: pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            headers: Vec::new(),
            peer_ip: "127.0.0.1".parse().unwrap(),
        }
    }

    #[test]
    fn source_aliases_preserve_truthy_precedence_and_last_value() {
        let input = request(&[
            ("offset", "5"),
            ("offset", ""),
            ("seek", "٠_١٣"),
            ("t", "82"),
        ]);
        assert_eq!(requested(&input).offset, "٠_١٣");
        let input = request(&[("timestamp", ""), ("time", "14")]);
        assert_eq!(requested(&input).offset, "");
        assert_eq!(requested(&request(&[("time", "14")])).offset, "14");
    }

    #[test]
    fn source_provider_query_order_cast_and_invalid_offset() {
        let location = "http://owned.invalid/movie/u/p/1.mp4?keep=~*&offset=9&blank=&keep=two#mark";
        assert_eq!(
            provider_url(
                location,
                Timeshift {
                    utc_start: "",
                    utc_end: "",
                    offset: "+٠_١٣"
                }
            ),
            "http://owned.invalid/movie/u/p/1.mp4?keep=~%2A&keep=two&offset=13&seek=13&t=13#mark"
        );
        assert_eq!(
            provider_url(
                location,
                Timeshift {
                    utc_start: "",
                    utc_end: "",
                    offset: "1.1"
                }
            ),
            "http://owned.invalid/movie/u/p/1.mp4?keep=~%2A&keep=two&offset=9#mark"
        );
        assert_eq!(
            provider_url(
                location,
                Timeshift {
                    utc_start: "",
                    utc_end: "",
                    offset: ""
                }
            ),
            location
        );
    }

    #[test]
    fn source_calendar_aliases_project_local_clock_verbatim_and_gate_vlc_fallback() {
        let input = request(&[
            ("utc_start", "old"),
            ("utc_start", ""),
            ("start", "2026-03-01T00:03:04+02:30"),
            ("playliststart", "ignored"),
            ("playlistend", "literal end"),
            ("timestamp", "99"),
        ]);
        let shift = requested(&input);
        assert_eq!(shift.utc_start, "2026-03-01T00:03:04+02:30");
        assert_eq!(shift.utc_end, "literal end");
        assert_eq!(shift.offset, "");
        let location =
            "http://owned.invalid/catchup/2000-01-12/13-14-15/movie/u/p/1.mp4?end=old&blank=";
        assert_eq!(
            provider_url(location, shift),
            "http://owned.invalid/catchup/2026-03-01/00-03-04/movie/u/p/1.mp4?end=literal+end&utc_start=2026-03-01T00%3A03%3A04%2B02%3A30&start=2026-03-01T00%3A03%3A04%2B02%3A30&utc_end=literal+end"
        );
        let input = request(&[("end", "later"), ("time", "14")]);
        assert_eq!(requested(&input).offset, "14");
        let input = request(&[("playliststart", "bad"), ("seek", "15")]);
        assert_eq!(
            provider_url(location, requested(&input)),
            "http://owned.invalid/catchup/2000-01-12/13-14-15/movie/u/p/1.mp4?end=old&utc_start=bad&start=bad&offset=15&seek=15&t=15"
        );
    }

    #[test]
    fn source_idle_offset_increases_score_without_requiring_exact_match() {
        assert_eq!(idle_score(true, false, false), Some(17));
        assert_eq!(idle_score(false, true, false), Some(15));
        assert_eq!(idle_score(false, false, true), Some(13));
        assert_eq!(idle_score(false, false, false), None);
    }
}
