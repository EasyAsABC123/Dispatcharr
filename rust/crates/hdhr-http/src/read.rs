//! Source apps/hdhr/api_views.py projections; original Dispatcharr authors retain credit.
use crate::gateway::{unavailable, Endpoint, Inner};
use dispatcharr_store::StoreError;
use futures_util::TryStreamExt;
use serde::Serialize;
use serde_json::{json, ser::Formatter, Value};
use sqlx::Row;
use std::io::{self, Write};

pub(crate) async fn read(
    inner: &Inner,
    endpoint: Endpoint,
    origin: &str,
) -> Result<(&'static str, Vec<u8>), StoreError> {
    match endpoint {
        Endpoint::Root|Endpoint::Devices{..}=>unreachable!(),
        Endpoint::Discover{profile,output}=>{
            let mut base=format!("{origin}/hdhr");let mut slugs=Vec::new();
            if let Some(profile)=profile {base.push('/');base.push_str(&profile);slugs.push(profile)}
            if let Some(output)=output {base.push_str("/output_profile/");base.push_str(&output);slugs.push(output)}
            let row=sqlx::query("SELECT friendly_name,device_id FROM hdhr_hdhrdevice ORDER BY id LIMIT 1").fetch_optional(inner.store.pool()).await?;
            let (name,id)=if let Some(row)=row {(row.try_get::<String,_>("friendly_name")?,row.try_get::<String,_>("device_id")?)} else if slugs.is_empty(){("Dispatcharr HDHomeRun".into(),"12345678".into())}else{(format!("Dispatcharr HDHomeRun - {}",slugs.join(" / ")),format!("dispatcharr-hdhr-{}",slugs.join("-")))};
            let tuner_count=tuners(inner).await.unwrap_or(1);
            encode(&Discovery {
                friendly_name: &name,
                model_number: "HDTC-2US",
                firmware_name: "hdhomerun3_atsc",
                firmware_version: "20200101",
                device_id: &id,
                device_auth: "test_auth_token",
                base_url: &base,
                lineup_url: &format!("{base}/lineup.json"),
                tuner_count,
            })
        },
        Endpoint::Lineup{profile,output}=>lineup(inner,profile,output,origin).await,
        Endpoint::Status => encode(&LineupStatus {
            scan_in_progress: 0,
            scan_possible: 0,
            source: "Cable",
            source_list: ["Cable"],
        }),
        Endpoint::Xml=>Ok(("application/xml",format!("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n        <root>\n            <DeviceID>12345678</DeviceID>\n            <FriendlyName>Dispatcharr HDHomeRun</FriendlyName>\n            <ModelNumber>HDTC-2US</ModelNumber>\n            <FirmwareName>hdhomerun3_atsc</FirmwareName>\n            <FirmwareVersion>20200101</FirmwareVersion>\n            <DeviceAuth>test_auth_token</DeviceAuth>\n            <BaseURL>{origin}/hdhr</BaseURL>\n            <LineupURL>{origin}/hdhr/lineup.json</LineupURL>\n        </root>").into_bytes())),
    }
}
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Discovery<'a> {
    friendly_name: &'a str,
    model_number: &'static str,
    firmware_name: &'static str,
    firmware_version: &'static str,
    #[serde(rename = "DeviceID")]
    device_id: &'a str,
    device_auth: &'static str,
    #[serde(rename = "BaseURL")]
    base_url: &'a str,
    #[serde(rename = "LineupURL")]
    lineup_url: &'a str,
    tuner_count: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct LineupStatus {
    scan_in_progress: u8,
    scan_possible: u8,
    source: &'static str,
    source_list: [&'static str; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct LineupChannel<'a> {
    guide_number: &'a str,
    guide_name: &'a str,
    #[serde(rename = "URL")]
    url: &'a str,
    #[serde(rename = "Guide_ID")]
    guide_id: &'a str,
    station: &'a str,
}

fn encode(value: &impl Serialize) -> Result<(&'static str, Vec<u8>), StoreError> {
    Ok(("application/json", django_json(value)?))
}

fn django_json(value: &impl Serialize) -> Result<Vec<u8>, StoreError> {
    let mut bytes = Vec::new();
    value
        .serialize(&mut serde_json::Serializer::with_formatter(&mut bytes, DjangoJson))
        .map_err(|_| unavailable())?;
    Ok(bytes)
}

// Django JsonResponse uses json.dumps with its default separators and
// ensure_ascii setting. Keep map insertion order from the Source projections.
struct DjangoJson;

impl Formatter for DjangoJson {
    fn begin_array_value<W: Write + ?Sized>(&mut self, writer: &mut W, first: bool) -> io::Result<()> {
        if !first {
            writer.write_all(b", ")?;
        }
        Ok(())
    }

    fn begin_object_key<W: Write + ?Sized>(&mut self, writer: &mut W, first: bool) -> io::Result<()> {
        self.begin_array_value(writer, first)
    }

    fn begin_object_value<W: Write + ?Sized>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b": ")
    }

    fn write_string_fragment<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        let mut start = 0;
        for (offset, character) in fragment.char_indices().filter(|(_, ch)| *ch > '\u{7e}') {
            writer.write_all(&fragment.as_bytes()[start..offset])?;
            for unit in character.encode_utf16(&mut [0; 2]) {
                write!(writer, "\\u{unit:04x}")?;
            }
            start = offset + character.len_utf8();
        }
        writer.write_all(&fragment.as_bytes()[start..])
    }
}

async fn tuners(inner: &Inner) -> Result<i64, sqlx::Error> {
    let row=sqlx::query("SELECT COALESCE(bool_or(p.max_streams=0),false) AS unlimited, COALESCE(sum(p.max_streams) FILTER (WHERE p.max_streams>0),0)::bigint AS limited FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON p.m3u_account_id=a.id WHERE p.id<>1 AND p.is_active AND a.is_active").fetch_one(inner.store.pool()).await?;
    let custom: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dispatcharr_channels_stream WHERE is_custom")
            .fetch_one(inner.store.pool())
            .await?;
    Ok((if row.try_get::<bool, _>("unlimited")? {
        10
    } else {
        row.try_get::<i64, _>("limited")?
    } + custom)
        .max(1))
}
async fn lineup(
    inner: &Inner,
    profile: Option<String>,
    output: Option<String>,
    origin: &str,
) -> Result<(&'static str, Vec<u8>), StoreError> {
    // Source returns an empty lineup for an unknown channel profile before
    // attempting to read the default output-profile setting.
    let profile_id = if let Some(name) = profile {
        let id = sqlx::query_scalar::<_, i64>(
            "SELECT id::bigint FROM dispatcharr_channels_channelprofile WHERE name=$1",
        )
        .bind(name)
        .fetch_optional(inner.store.pool())
        .await?;
        let Some(id) = id else {
            return Ok(("application/json", b"[]".to_vec()));
        };
        Some(id)
    } else {
        None
    };
    let output = match output {
        Some(value) => Some(value),
        None => python_int(
            inner
                .auth
                .source_stream_settings()
                .await?
                .get("hdhr_output_profile_id"),
        ),
    };
    let output = if let Some(value) = output {
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM core_outputprofile WHERE id::text=$1 AND is_active)",
        )
        .bind(&value)
        .fetch_one(inner.store.pool())
        .await?;
        active.then_some(value)
    } else {
        None
    };
    let suffix = output
        .map(|value| format!("?output_profile={value}"))
        .unwrap_or_default();
    let mut rows=sqlx::query("SELECT c.uuid::text AS uuid,COALESCE(o.name,c.name) AS name,COALESCE(o.channel_number,c.channel_number) AS number FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE NOT c.hidden_from_output AND ($1::bigint IS NULL OR EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofilemembership m WHERE m.channel_id=c.id AND m.enabled AND m.channel_profile_id=$1)) ORDER BY COALESCE(o.channel_number,c.channel_number) LIMIT $2")
        .bind(profile_id).bind((inner.config.max_channels+1) as i64).fetch(inner.store.pool());
    let mut metadata = 0usize;
    let mut output = Vec::new();
    output.push(b'[');
    let mut count = 0;
    let mut emitted = false;
    while let Some(row) = rows.try_next().await? {
        count += 1;
        if count > inner.config.max_channels {
            return Err(unavailable());
        }
        let Some(number) = row.try_get::<Option<f64>, _>("number")? else {
            continue;
        };
        let number = display_number(number)?;
        let name: String = row.try_get("name")?;
        let uuid: String = row.try_get("uuid")?;
        metadata = metadata
            .checked_add(name.len() + uuid.len() + number.len() + 128)
            .ok_or_else(unavailable)?;
        if metadata > inner.config.max_metadata_bytes {
            return Err(unavailable());
        }
        let encoded = django_json(&LineupChannel {
            guide_number: &number,
            guide_name: &name,
            url: &format!("{origin}/proxy/ts/stream/{uuid}{suffix}"),
            guide_id: &number,
            station: &number,
        })?;
        let required = output
            .len()
            .checked_add(encoded.len())
            .and_then(|value| value.checked_add(2 * usize::from(emitted) + 1))
            .ok_or_else(unavailable)?;
        if required > inner.config.max_response_bytes {
            return Err(unavailable());
        }
        if emitted {
            output.extend_from_slice(b", ");
        }
        output.extend_from_slice(&encoded);
        emitted = true;
    }
    output.push(b']');
    Ok(("application/json", output))
}
fn python_int(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Bool(value) => Some(if *value { "1" } else { "0" }.into()),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Some(value.to_string())
            } else if let Some(value) = value.as_u64() {
                Some(value.to_string())
            } else {
                let value = value.as_f64()?;
                value.is_finite().then(|| format!("{:.0}", value.trunc()))
            }
        }
        Value::String(value) => dispatcharr_ingest_core::source_integer_literal(value),
        _ => None,
    }
}
fn display_number(value: f64) -> Result<String, StoreError> {
    if !value.is_finite() {
        return Err(StoreError::StatusBody(
            500,
            json!({"detail":"Invalid channel number."}),
        ));
    }
    if value == 0.0 {
        return Ok("0".into());
    }
    if value.fract() == 0.0 {
        return Ok(format!("{value:.0}"));
    }
    if value.abs() < 0.0001 {
        let text = format!("{value:e}");
        let (mantissa, exponent) = text.split_once('e').unwrap();
        let exponent: i32 = exponent.parse().unwrap();
        return Ok(format!("{mantissa}e{exponent:+03}"));
    }
    Ok(value.to_string())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn django_json_preserves_order_and_ascii_escaping() {
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Sample {
            zulu: &'static str,
            alpha: [&'static str; 3],
            count: u8,
        }
        let value = Sample {
            zulu: "ä 🎬\u{7f}",
            alpha: ["quotes: \"", "path/segment", "short\nline"],
            count: 2,
        };
        let actual = django_json(&value).unwrap();
        assert_eq!(
            actual,
            br#"{"Zulu": "\u00e4 \ud83c\udfac\u007f", "Alpha": ["quotes: \"", "path/segment", "short\nline"], "Count": 2}"#
        );
    }

    #[test]
    fn default_profile_python_int() {
        for (input, expected) in [
            (json!(true), Some("1")),
            (json!(" +0002 "), Some("2")),
            (json!("\u{2003}+٠_٠_٢\u{a0}"), Some("2")),
            (json!("１２"), Some("12")),
            (json!("-०_०"), Some("0")),
            (json!("0__2"), None),
            (json!("²"), None),
            (json!(-1.9), Some("-1")),
            (json!("3.0"), None),
            (json!({}), None),
        ] {
            assert_eq!(python_int(Some(&input)).as_deref(), expected)
        }
    }
}
