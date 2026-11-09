//! Source apps/hdhr/api_views.py projections; original Dispatcharr authors retain credit.
use crate::gateway::{unavailable, Endpoint, Inner};
use dispatcharr_store::StoreError;
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::Row;

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
            let value=json!({"FriendlyName":name,"ModelNumber":"HDTC-2US","FirmwareName":"hdhomerun3_atsc","FirmwareVersion":"20200101","DeviceID":id,"DeviceAuth":"test_auth_token","BaseURL":base,"LineupURL":format!("{base}/lineup.json"),"TunerCount":tuner_count});
            encode(value)
        },
        Endpoint::Lineup{profile,output}=>lineup(inner,profile,output,origin).await,
        Endpoint::Status=>encode(json!({"ScanInProgress":0,"ScanPossible":0,"Source":"Cable","SourceList":["Cable"]})),
        Endpoint::Xml=>Ok(("application/xml",format!("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n        <root>\n            <DeviceID>12345678</DeviceID>\n            <FriendlyName>Dispatcharr HDHomeRun</FriendlyName>\n            <ModelNumber>HDTC-2US</ModelNumber>\n            <FirmwareName>hdhomerun3_atsc</FirmwareName>\n            <FirmwareVersion>20200101</FirmwareVersion>\n            <DeviceAuth>test_auth_token</DeviceAuth>\n            <BaseURL>{origin}/hdhr</BaseURL>\n            <LineupURL>{origin}/hdhr/lineup.json</LineupURL>\n        </root>").into_bytes())),
    }
}
fn encode(value: Value) -> Result<(&'static str, Vec<u8>), StoreError> {
    Ok((
        "application/json",
        serde_json::to_vec(&value).map_err(|_| unavailable())?,
    ))
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
    let mut rows=sqlx::query("SELECT c.uuid::text AS uuid,COALESCE(o.name,c.name) AS name,COALESCE(o.channel_number,c.channel_number) AS number FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE NOT c.hidden_from_output AND ($1::text IS NULL OR EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofilemembership m JOIN dispatcharr_channels_channelprofile p ON m.channel_profile_id=p.id WHERE m.channel_id=c.id AND m.enabled AND p.name=$1)) ORDER BY COALESCE(o.channel_number,c.channel_number) LIMIT $2")
        .bind(profile).bind((inner.config.max_channels+1) as i64).fetch(inner.store.pool());
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
        let value = json!({"GuideNumber":number,"GuideName":name,"URL":format!("{origin}/proxy/ts/stream/{uuid}{suffix}"),"Guide_ID":number,"Station":number});
        let encoded = serde_json::to_vec(&value).map_err(|_| unavailable())?;
        let required = output
            .len()
            .checked_add(encoded.len())
            .and_then(|value| value.checked_add(usize::from(emitted) + 1))
            .ok_or_else(unavailable)?;
        if required > inner.config.max_response_bytes {
            return Err(unavailable());
        }
        if emitted {
            output.push(b',')
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
        Value::String(value) => {
            let value = value.trim();
            let (negative, digits) = if let Some(value) = value.strip_prefix('-') {
                (true, value)
            } else {
                (false, value.strip_prefix('+').unwrap_or(value))
            };
            if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            let digits = digits.trim_start_matches('0');
            let digits = if digits.is_empty() { "0" } else { digits };
            Some(format!(
                "{}{}",
                if negative && digits != "0" { "-" } else { "" },
                digits
            ))
        }
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
    fn default_profile_python_int() {
        for (input, expected) in [
            (json!(true), Some("1")),
            (json!(" +0002 "), Some("2")),
            (json!(-1.9), Some("-1")),
            (json!("3.0"), None),
            (json!({}), None),
        ] {
            assert_eq!(python_int(Some(&input)).as_deref(), expected)
        }
    }
}
