//! Source StreamManager URL-type promotion/UDP argv rules.
//! Original Dispatcharr authors retain full detection/profile algorithm credit.
use crate::{Store, StoreError};
use sqlx::Row;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Ts,
    Hls,
    Rtsp,
    Udp,
}
/// URLparse path only; unlike WHATWG Url parsing this MUST NOT remove dot path
/// segments or unescape%encoded names before Source's substring checks.
fn path(url: &str) -> &str {
    let value = url.trim_start_matches(|c: char| c <= ' ');
    let (scheme, mut rest) = if let Some((scheme, tail)) = value.split_once(':') {
        if !scheme.is_empty()
            && scheme.as_bytes()[0].is_ascii_alphabetic()
            && scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b))
        {
            (scheme, tail)
        } else {
            ("", value)
        }
    } else {
        ("", value)
    };
    if let Some(authority) = rest.strip_prefix("//") {
        rest = authority
            .find(['/', '?', '#'])
            .map(|i| &authority[i..])
            .unwrap_or("");
    }
    rest = rest.split(['?', '#']).next().unwrap_or("");
    let after_slash = rest.rfind('/').map_or(0, |i| i + 1);
    if [
        "", "ftp", "hdl", "prospero", "http", "imap", "https", "shttp", "rtsp", "rtsps", "rtspu",
        "sip", "sips", "mms", "sftp", "tel",
    ]
    .contains(&scheme)
    {
        if let Some(i) = rest[after_slash..].find(';') {
            rest = &rest[..after_slash + i]
        }
    }
    rest
}
pub(crate) fn kind(url: &str) -> Kind {
    let lower = url.to_lowercase();
    if lower.starts_with("udp://") {
        return Kind::Udp;
    }
    if lower.starts_with("rtsp://") || lower.starts_with("rtp://") {
        return Kind::Rtsp;
    }
    if lower.ends_with(".m3u8") || lower.contains(".m3u8?") || lower.contains("/playlist.m3u") {
        return Kind::Hls;
    }
    // Python urlparse strips tab/CR/LF even within a URL. Its initial global
    // substring checks above operate on the original string, as Source does.
    let cleaned: String = lower
        .chars()
        .filter(|c| !matches!(c, '\t' | '\r' | '\n'))
        .collect();
    let path = path(&cleaned);
    if ["playlist", "manifest", "master"]
        .iter()
        .any(|s| path.contains(s))
        && path.contains(".m3u")
    {
        Kind::Hls
    } else {
        Kind::Ts
    }
}
/// StreamManager filters TOKENS, not just matched option/value pairs. Preserve
/// the exact Source case-sensitiveUA/insensitiveflag-substring behavior.
pub(crate) fn udp_arguments(command: &mut dispatcharr_contracts::StreamCommand, agent: &str) {
    command.argv.retain(|arg| {
        if arg.contains(agent) {
            return false;
        }
        let lower = arg.to_lowercase();
        !lower.contains("user-agent") && !lower.contains("user_agent")
    });
}
pub(crate) struct Template {
    pub name: String,
    pub command: String,
    pub parameters: String,
}
impl Store {
    pub(crate) async fn source_ffmpeg_template(&self) -> Result<Template, StoreError> {
        let rows=sqlx::query(r#"SELECT CASE WHEN octet_length(name)<=4096 THEN name END AS name,
          CASE WHEN octet_length(command)<=262144 THEN command END AS command,
          CASE WHEN octet_length(parameters)<=262144 THEN parameters END AS parameters,
          (octet_length(name)>4096 OR octet_length(command)>262144 OR octet_length(parameters)>262144) AS oversized
          FROM core_streamprofile WHERE name ILIKE 'FFmpeg' AND locked LIMIT 2"#).fetch_all(self.pool()).await?;
        if rows.len() != 1 {
            return Err(StoreError::Unsupported(
                "Automatic input promotion requires one actual locked FFmpeg profile",
            ));
        }
        let row = &rows[0];
        if row.try_get::<bool, _>("oversized")? {
            return Err(StoreError::StatusBody(
                503,
                serde_json::json!({"error":"Automatic input profile capacity reached"}),
            ));
        }
        // Source get_locked does not require is_active; never silently replace
        // configured Source command/parameters with a hardcoded FFmpeg argv.
        Ok(Template {
            name: row.try_get("name")?,
            command: row.try_get("command")?,
            parameters: row.try_get("parameters")?,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_url_detection_keeps_query_host_params_and_dot_segment_distinctions() {
        let data: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/source-input-url-kinds-host.json"))
                .unwrap();
        for record in data["records"].as_array().unwrap() {
            let url = record["url"].as_str().unwrap();
            assert_eq!(
                format!("{:?}", kind(url)).to_lowercase(),
                record["actual_extracted_function_result"].as_str().unwrap(),
                "{url}"
            );
        }
    }
    #[test]
    fn udp_removes_source_matching_tokens_without_accidentally_selecting_proxy() {
        let mut command = dispatcharr_contracts::StreamCommand {
            argv: vec![
                "ffmpeg".into(),
                "-user_agent".into(),
                "Owned-UA".into(),
                "--USER-AGENT=other".into(),
                "prefixOwned-UAsuffix".into(),
                "-i".into(),
                "udp://host/live".into(),
                "pipe:1".into(),
            ],
        };
        udp_arguments(&mut command, "Owned-UA");
        assert_eq!(
            command.argv,
            vec!["ffmpeg", "-i", "udp://host/live", "pipe:1"]
        );
        let mut unusual = dispatcharr_contracts::StreamCommand {
            argv: vec!["ffmpeg".into()],
        };
        udp_arguments(&mut unusual, "ffmpeg");
        assert!(unusual.argv.is_empty());
        // Caller retains Some(processCommand) even if filtering removes the
        // executable; it must never reinterpret this as lockedProxy/rawHTTP.
    }
}
