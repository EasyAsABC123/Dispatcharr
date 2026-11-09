//! Original M3U text formatting and Python query encoding. Upstream retains credit.
use std::{fmt::Write, net::IpAddr};

pub struct Channel {
    pub id: i64,
    pub uuid: uuid::Uuid,
    pub number: Option<f64>,
    pub name: String,
    pub group: Option<String>,
    pub logo: Option<(i64, String)>,
    pub tvg_id: Option<String>,
    pub guide: Option<String>,
    pub radio: bool,
    pub catchup_days: i32,
    pub direct: Option<Direct>,
}
pub struct Direct {
    pub url: String,
    pub catchup_days: i32,
}
/// No Debug/Serialize: credentials and direct provider URLs can be secrets.
pub struct Context<'a> {
    pub origin: &'a str,
    pub profile: Option<&'a str>,
    pub query: &'a [(String, String)],
    pub xc: Option<(&'a str, &'a str)>,
    pub direct: bool,
    pub restricted_direct: bool,
    pub catchup: bool,
    pub max_bytes: usize,
}
#[derive(Debug)]
pub enum Error {
    Capacity,
    InvalidNumber,
    InvalidProviderUrl,
}
fn parameter<'a>(query: &'a [(String, String)], key: &str) -> Option<&'a str> {
    query
        .iter()
        .rev()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}
fn quote(value: &str, path: bool) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || b"-._~".contains(&byte)
            || (path && b"!$&'()*+,;=:@".contains(&byte))
        {
            out.push(byte as char)
        } else if byte == b' ' && !path {
            out.push('+')
        } else {
            write!(&mut out, "%{byte:02X}").unwrap();
        }
    }
    out
}
pub fn urlencode(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", quote(key, false), quote(value, false)))
        .collect::<Vec<_>>()
        .join("&")
}
fn display_number(value: Option<f64>) -> Result<String, Error> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    if !value.is_finite() {
        return Err(Error::InvalidNumber);
    }
    if value == 0.0 {
        return Ok("0".into());
    }
    if value == value.trunc() {
        return Ok(format!("{value:.0}"));
    }
    if value.abs() < 1e-4 {
        let raw = format!("{value:e}");
        let (mantissa, exponent) = raw.split_once('e').unwrap();
        let exponent: i32 = exponent.parse().unwrap();
        return Ok(format!("{mantissa}e{exponent:+03}"));
    }
    Ok(value.to_string())
}
fn append(output: &mut Vec<u8>, text: &str, limit: usize) -> Result<(), Error> {
    if output
        .len()
        .checked_add(text.len())
        .is_none_or(|size| size > limit)
    {
        return Err(Error::Capacity);
    }
    output.extend_from_slice(text.as_bytes());
    Ok(())
}
fn multicast(url: &str) -> Result<String, Error> {
    if !url.starts_with("udp://") || url.contains("udp://@") {
        return Ok(url.into());
    }
    let parsed = url::Url::parse(url).map_err(|_| Error::InvalidProviderUrl)?;
    let host = parsed
        .host_str()
        .ok_or(Error::InvalidProviderUrl)?
        .trim_matches(['[', ']']);
    if host.parse::<IpAddr>().is_ok_and(|ip| ip.is_multicast()) {
        Ok(url.replacen("udp://", "udp://@", 1))
    } else {
        Ok(url.into())
    }
}
pub fn render(context: &Context<'_>, channels: &[Channel]) -> Result<Vec<u8>, Error> {
    let parameter = |key| parameter(context.query, key);
    let output_format = parameter("output_format")
        .filter(|v| !v.is_empty())
        .or_else(|| parameter("output").filter(|v| !v.is_empty()));
    let mut stream_pairs = Vec::new();
    if let Some(value) = parameter("output_profile").filter(|v| !v.is_empty()) {
        stream_pairs.push(("output_profile".into(), value.into()));
    }
    if let Some(value) = output_format {
        stream_pairs.push(("output_format".into(), value.into()));
    }
    let stream_suffix = if stream_pairs.is_empty() {
        String::new()
    } else {
        format!("?{}", urlencode(&stream_pairs))
    };
    let epg = if let Some((user, password)) = context.xc {
        format!(
            "{}/xmltv.php?username={user}&password={password}",
            context.origin
        )
    } else {
        let path = context.profile.map_or_else(
            || "/output/epg".into(),
            |profile| format!("/output/epg/{}", quote(profile, true)),
        );
        let mut pairs = Vec::new();
        for (key, _) in context.query {
            if ["tvg_id_source", "cachedlogos", "days", "prev_days"].contains(&key.as_str())
                && !pairs.iter().any(|(existing, _)| existing == key)
            {
                pairs.push((key.clone(), parameter(key).unwrap().into()));
            }
        }
        format!(
            "{}{}{}",
            context.origin,
            path,
            if pairs.is_empty() {
                String::new()
            } else {
                format!("?{}", urlencode(&pairs))
            }
        )
    };
    let cached_logos =
        !parameter("cachedlogos").is_some_and(|value| value.eq_ignore_ascii_case("false"));
    let tvg_source = parameter("tvg_id_source")
        .unwrap_or("channel_number")
        .to_lowercase();
    let catchup_source = context
        .xc
        .filter(|_| context.catchup && !context.direct)
        .map(|(user, password)| {
            format!(
                "{}/streaming/timeshift.php?{}&stream=",
                context.origin,
                urlencode(&[
                    ("username".into(), user.into()),
                    ("password".into(), password.into())
                ])
            )
        });
    let mut out = Vec::new();
    append(
        &mut out,
        &format!(
            "#EXTM3U x-tvg-url=\"{epg}\" url-tvg=\"{epg}\"{}\n",
            if catchup_source.is_some() {
                " catchup-timezone=\"UTC\""
            } else {
                ""
            }
        ),
        context.max_bytes,
    )?;
    for channel in channels {
        if context.direct && context.restricted_direct && channel.direct.is_none() {
            continue;
        }
        let number = display_number(channel.number)?;
        let tvg = match tvg_source.as_str() {
            "tvg_id" => channel.tvg_id.as_deref().filter(|v| !v.is_empty()),
            "gracenote" => channel.guide.as_deref().filter(|v| !v.is_empty()),
            _ => None,
        }
        .map(str::to_owned)
        .unwrap_or_else(|| {
            if number.is_empty() {
                channel.id.to_string()
            } else {
                number.clone()
            }
        });
        let logo = channel.logo.as_ref().map_or_else(String::new, |(id, url)| {
            if !cached_logos && (url.starts_with("http://") || url.starts_with("https://")) {
                url.clone()
            } else {
                format!("{}/api/channels/logos/{id}/cache/", context.origin)
            }
        });
        let guide = channel
            .guide
            .as_deref()
            .filter(|v| !v.is_empty())
            .map_or_else(String::new, |value| {
                format!("tvc-guide-stationid=\"{value}\" ")
            });
        let catchup = if context.catchup && context.direct {
            channel
                .direct
                .as_ref()
                .filter(|direct| direct.catchup_days > 0)
                .map_or_else(String::new, |direct| {
                    format!("catchup=\"xc\" catchup-days=\"{}\" ", direct.catchup_days)
                })
        } else if context.catchup && channel.catchup_days > 0 {
            catchup_source.as_ref().map_or_else(String::new,|prefix|format!("catchup=\"default\" catchup-days=\"{}\" catchup-source=\"{prefix}{}&utc={{utc}}&duration={{duration:60}}\" ",channel.catchup_days,channel.id))
        } else {
            String::new()
        };
        append(&mut out,&format!("#EXTINF:-1 tvg-id=\"{tvg}\" tvg-name=\"{}\" tvg-logo=\"{logo}\" tvg-chno=\"{number}\" {}{guide}{catchup}group-title=\"{}\",{}\n",channel.name,if channel.radio{"radio=\"true\" "}else{""},channel.group.as_deref().unwrap_or("Default"),channel.name),context.max_bytes)?;
        let stream = if context.direct {
            channel
                .direct
                .as_ref()
                .map(|direct| multicast(&direct.url))
                .transpose()?
                .unwrap_or_else(|| format!("{}/proxy/ts/stream/{}", context.origin, channel.uuid))
        } else if let Some((user, password)) = context.xc {
            format!(
                "{}/live/{user}/{password}/{}{stream_suffix}",
                context.origin, channel.id
            )
        } else {
            format!(
                "{}/proxy/ts/stream/{}{stream_suffix}",
                context.origin, channel.uuid
            )
        };
        append(&mut out, &stream, context.max_bytes)?;
        append(&mut out, "\n", context.max_bytes)?;
    }
    Ok(out)
}
