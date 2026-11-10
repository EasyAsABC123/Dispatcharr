//! Original output-specific channel visibility and effective values. Upstream credit retained.
use crate::render::{Channel, Direct};
use dispatcharr_store::{Store, StoreError, User};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder, Row};

pub struct Read<'a> {
    pub user: Option<&'a User>,
    pub profile: Option<&'a str>,
    pub direct: bool,
    pub max_channels: usize,
    pub max_metadata_bytes: usize,
}
pub struct Channels {
    pub rows: Vec<Channel>,
    pub restricted_direct: bool,
}
/// Original output channel visibility. Callers apply hidden_from_output and
/// resolve a named profile beforehand; admins still retain the output level cap.
pub fn append_output_visibility(
    query: &mut QueryBuilder<'_, Postgres>,
    user: Option<&User>,
    profile: Option<i64>,
) {
    if let Some(profile) = profile.filter(|_| user.is_none()) {
        query.push(" AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofilemembership m WHERE m.channel_id=c.id AND m.enabled AND m.channel_profile_id=").push_bind(profile).push(")");
    }
    if let Some(user) = user {
        query.push(" AND c.user_level<=").push_bind(user.user_level);
        if user.user_level < 10 {
            if truthy(user.custom_properties.get("hide_adult_content")) {
                query.push(" AND NOT c.is_adult");
            }
            query.push(" AND (NOT EXISTS(SELECT 1 FROM accounts_user_channel_profiles up WHERE up.user_id=").push_bind(user.id).push(") OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles up JOIN dispatcharr_channels_channelprofilemembership m ON m.channel_profile_id=up.channelprofile_id WHERE up.user_id=").push_bind(user.id).push(" AND m.channel_id=c.id AND m.enabled))");
        }
    }
}

pub async fn read(store: &Store, request: Read<'_>) -> Result<Channels, StoreError> {
    let profile = if request.user.is_none() {
        if let Some(profile) = request.profile {
            Some(
                sqlx::query_scalar::<_, i64>(
                    "SELECT id::bigint FROM dispatcharr_channels_channelprofile WHERE name=$1",
                )
                .bind(profile)
                .fetch_optional(store.pool())
                .await?
                .ok_or(StoreError::NotFound)?,
            )
        } else {
            None
        }
    } else {
        None
    };
    let provider_allowlist = request
        .user
        .and_then(|user| user.custom_properties.get("allowed_m3u_profile_ids"))
        .filter(|value| !value.is_null());
    let restricted = request.direct && provider_allowlist.is_some();
    if request.direct
        && provider_allowlist
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    {
        return Err(StoreError::Unsupported(
            "M3U direct provider allowlist transforms are not yet implemented",
        ));
    }
    let mut query=QueryBuilder::<Postgres>::new("SELECT c.id::bigint AS id,c.uuid,COALESCE(o.name,c.name) AS name,COALESCE(o.channel_number,c.channel_number) AS number,COALESCE(o.tvg_id,c.tvg_id) AS tvg_id,COALESCE(o.tvc_guide_stationid,c.tvc_guide_stationid) AS guide,COALESCE(o.is_radio,c.is_radio) AS radio,CASE WHEN octet_length(g.name)>65536 THEN NULL ELSE g.name END AS group_name,l.id::bigint AS logo_id,CASE WHEN octet_length(l.url)>65536 THEN NULL ELSE l.url END AS logo_url,c.is_catchup,c.catchup_days,(COALESCE(octet_length(g.name),0)>65536 OR COALESCE(octet_length(l.url),0)>65536) AS large_metadata");
    if request.direct && !restricted {
        query.push(",CASE WHEN octet_length(s.url)>65536 THEN NULL ELSE s.url END AS direct_url,COALESCE(octet_length(s.url),0)>65536 AS large_direct,s.is_catchup AS direct_catchup,s.catchup_days AS direct_days,s.account_type AS direct_type");
    }
    query.push(" FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=COALESCE(o.channel_group_id,c.channel_group_id) LEFT JOIN dispatcharr_channels_logo l ON l.id=COALESCE(o.logo_id,c.logo_id)");
    if request.direct && !restricted {
        query.push(" LEFT JOIN LATERAL(SELECT s.url,s.is_catchup,s.catchup_days,a.account_type FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id ORDER BY cs.\"order\" LIMIT 1)s ON TRUE");
    }
    query.push(" WHERE NOT c.hidden_from_output");
    append_output_visibility(&mut query, request.user, profile);
    query
        .push(" ORDER BY COALESCE(o.channel_number,c.channel_number) LIMIT ")
        .push_bind((request.max_channels.saturating_add(1)) as i64);
    let mut rows = query.build().fetch(store.pool());
    let mut channels = Vec::new();
    let mut metadata_bytes = 0usize;
    while let Some(row) = rows.try_next().await? {
        if channels.len() >= request.max_channels
            || row.try_get::<bool, _>("large_metadata")?
            || (request.direct && !restricted && row.try_get::<bool, _>("large_direct")?)
        {
            return Err(capacity());
        }
        let logo_id: Option<i64> = row.try_get("logo_id")?;
        let logo = if let Some(id) = logo_id {
            Some((id, row.try_get::<String, _>("logo_url")?))
        } else {
            None
        };
        let direct = if request.direct && !restricted {
            let url: Option<String> = row.try_get("direct_url")?;
            url.filter(|value| !value.is_empty())
                .map(|url| {
                    Ok::<_, sqlx::Error>(Direct {
                        url,
                        catchup_days: if row.try_get::<Option<bool>, _>("direct_catchup")?
                            == Some(true)
                            && row.try_get::<Option<String>, _>("direct_type")?.as_deref()
                                == Some("XC")
                        {
                            row.try_get::<Option<i32>, _>("direct_days")?.unwrap_or(0)
                        } else {
                            0
                        },
                    })
                })
                .transpose()?
        } else {
            None
        };
        let channel = Channel {
            id: row.try_get("id")?,
            uuid: row.try_get("uuid")?,
            number: row.try_get("number")?,
            name: row.try_get("name")?,
            group: row.try_get("group_name")?,
            logo,
            tvg_id: row.try_get("tvg_id")?,
            guide: row.try_get("guide")?,
            radio: row.try_get("radio")?,
            catchup_days: if row.try_get::<bool, _>("is_catchup")? {
                row.try_get("catchup_days")?
            } else {
                0
            },
            direct,
        };
        let row_bytes = std::mem::size_of::<Channel>()
            + channel.name.capacity()
            + channel.group.as_ref().map_or(0, String::capacity)
            + channel.logo.as_ref().map_or(0, |(_, url)| url.capacity())
            + channel.tvg_id.as_ref().map_or(0, String::capacity)
            + channel.guide.as_ref().map_or(0, String::capacity)
            + channel
                .direct
                .as_ref()
                .map_or(0, |value| value.url.capacity());
        metadata_bytes = metadata_bytes
            .checked_add(row_bytes)
            .filter(|bytes| *bytes <= request.max_metadata_bytes)
            .ok_or_else(capacity)?;
        channels.push(channel);
    }
    Ok(Channels {
        rows: channels,
        restricted_direct: restricted,
    })
}
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(v)) => *v,
        Some(Value::Number(v)) => v.as_f64() != Some(0.0),
        Some(Value::String(v)) => !v.is_empty(),
        Some(Value::Array(v)) => !v.is_empty(),
        Some(Value::Object(v)) => !v.is_empty(),
    }
}

fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        serde_json::json!({"detail":"M3U export metadata capacity exceeded."}),
    )
}
