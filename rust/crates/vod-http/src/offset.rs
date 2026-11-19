//! Source VOD seek offset aliases, provider query projection and session identity.
//! Dispatcharr's original authors retain application and algorithm credit.
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_ingest_core::source_integer_literal;
use std::{borrow::Cow, collections::HashMap};

pub(crate) fn requested(request: &PlaybackRequest) -> &str {
    ["offset", "seek", "t"]
        .iter()
        .filter_map(|name| request.parameter(name))
        .find(|value| !value.is_empty())
        // An explicitly empty timestamp shadows the time alias in Source.
        .or_else(|| request.parameter("timestamp"))
        .or_else(|| request.parameter("time"))
        .unwrap_or("")
}

pub(crate) fn provider_url<'a>(original: &'a str, offset: &str) -> Cow<'a, str> {
    if offset.is_empty() {
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
    if let Some(seconds) = source_integer_literal(offset) {
        for name in ["offset", "seek", "t"] {
            if let Some(index) = keys.get(name).copied() {
                values[index].1 = vec![seconds.clone()];
            } else {
                values.push((name.to_owned(), vec![seconds.clone()]));
            }
        }
    }
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(
            values
                .iter()
                .flat_map(|(name, values)| values.iter().map(move |value| (name, value))),
        )
        .finish()
        .replace('*', "%2A")
        .replace("%7E", "~");
    let mut result = path.to_owned();
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

pub(crate) fn idle_score(same_offset: bool, same_ip: bool, same_user_agent: bool) -> Option<u8> {
    let score =
        10 + u8::from(same_offset) * 7 + u8::from(same_ip) * 5 + u8::from(same_user_agent) * 3;
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
        assert_eq!(requested(&input), "٠_١٣");
        let input = request(&[("timestamp", ""), ("time", "14")]);
        assert_eq!(requested(&input), "");
        assert_eq!(requested(&request(&[("time", "14")])), "14");
    }

    #[test]
    fn source_provider_query_order_cast_and_invalid_offset() {
        let location = "http://owned.invalid/movie/u/p/1.mp4?keep=~*&offset=9&blank=&keep=two#mark";
        assert_eq!(
            provider_url(location, "+٠_١٣"),
            "http://owned.invalid/movie/u/p/1.mp4?keep=~%2A&keep=two&offset=13&seek=13&t=13#mark"
        );
        assert_eq!(
            provider_url(location, "1.1"),
            "http://owned.invalid/movie/u/p/1.mp4?keep=~%2A&keep=two&offset=9#mark"
        );
        assert_eq!(provider_url(location, ""), location);
    }

    #[test]
    fn source_idle_offset_increases_score_without_requiring_exact_match() {
        assert_eq!(idle_score(true, false, false), Some(17));
        assert_eq!(idle_score(false, true, false), Some(15));
        assert_eq!(idle_score(false, false, true), Some(13));
        assert_eq!(idle_score(false, false, false), None);
    }
}
