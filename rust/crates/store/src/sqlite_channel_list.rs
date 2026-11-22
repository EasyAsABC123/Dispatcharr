//! Copied SQLite channel collection through the stock channel projection.
//! Original Dispatcharr authors retain full credit.
use crate::{channels, StoreError, User};
use chrono::NaiveDateTime;
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection};
use std::collections::HashMap;
use url::Url;

const CHANNELS: &str = " FROM dispatcharr_channels_channel c
    LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
    LEFT JOIN m3u_m3uaccount a ON a.id=c.auto_created_by_id
    LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=c.channel_group_id
    LEFT JOIN dispatcharr_channels_channelgroup og ON og.id=o.channel_group_id
    LEFT JOIN epg_epgdata e ON e.id=c.epg_data_id
    LEFT JOIN epg_epgdata oe ON oe.id=o.epg_data_id
    LEFT JOIN epg_epgsource es ON es.id=e.epg_source_id
    LEFT JOIN epg_epgsource oes ON oes.id=oe.epg_source_id WHERE true";
const BOOLEAN_FIELDS: &[&str] = &[
    "auto_created",
    "hidden_from_output",
    "is_adult",
    "is_catchup",
    "is_custom",
    "is_radio",
    "is_stale",
];
const STREAM_TIMESTAMPS: &[&str] = &["updated_at", "last_seen", "stream_stats_updated_at"];

pub(crate) fn sqlite_json_object(alias: &str, fields: &[&str]) -> String {
    let items = fields.iter().filter(|field| **field != "streams").map(|field| {
        let column = format!("{alias}.{field}");
        let value = if BOOLEAN_FIELDS.contains(field) {
            format!("CASE WHEN {column} IS NULL THEN NULL WHEN {column} THEN json('true') ELSE json('false') END")
        } else if *field == "stream_stats" {
            format!("json({column})")
        } else {
            column
        };
        format!("'{field}',{value}")
    }).collect::<Vec<_>>().join(",");
    format!("json_object({items})")
}

fn projected_select() -> String {
    let raw = sqlite_json_object("c", channels::FIELDS);
    let overrides = sqlite_json_object("o", channels::OVERRIDE_FIELDS);
    let stream = sqlite_json_object("s", channels::STREAM_FIELDS);
    format!("SELECT {raw} AS raw, CASE WHEN o.id IS NOT NULL THEN {overrides} END AS overrides,
       a.name AS account_name,
       (SELECT json_group_array(json_object('raw', {stream}, 'account_name', ma.name) ORDER BY cs.\"order\")
        FROM dispatcharr_channels_channelstream cs
        JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
        LEFT JOIN m3u_m3uaccount ma ON ma.id=s.m3u_account_id
        WHERE cs.channel_id=c.id) AS streams{CHANNELS}")
}

fn contains(query: &mut QueryBuilder<'_, Sqlite>, expression: &str, value: &str) {
    query
        .push("UPPER(")
        .push(expression)
        .push(") LIKE UPPER(")
        .push_bind(channels::like(value))
        .push(r") ESCAPE '\'");
}

fn filters(
    query: &mut QueryBuilder<'_, Sqlite>,
    params: &HashMap<String, String>,
    user: &User,
    detail: bool,
) -> Result<(), StoreError> {
    if user.user_level < 10 {
        query.push(" AND c.user_level<=").push_bind(user.user_level);
        if channels::truthy(user.custom_properties.get("hide_adult_content")) {
            query.push(" AND NOT c.is_adult");
        }
        query.push(" AND (NOT EXISTS(SELECT 1 FROM accounts_user_channel_profiles up WHERE up.user_id=")
            .push_bind(user.id)
            .push(") OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles up JOIN dispatcharr_channels_channelprofilemembership m ON m.channel_profile_id=up.channelprofile_id WHERE up.user_id=")
            .push_bind(user.id).push(" AND m.channel_id=c.id AND m.enabled))");
    }
    if !detail {
        match params.get("visibility_filter").map(String::as_str) {
            Some("hidden") => {
                query.push(" AND c.hidden_from_output");
            }
            Some("all") => {}
            _ => {
                query.push(" AND NOT c.hidden_from_output");
            }
        }
    }
    if let Some(profile) = params
        .get("channel_profile_id")
        .and_then(|value| value.parse::<i64>().ok())
    {
        if user.user_level < 10 {
            query
                .push(
                    " AND (NOT EXISTS(SELECT 1 FROM accounts_user_channel_profiles WHERE user_id=",
                )
                .push_bind(user.id)
                .push(") OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles WHERE user_id=")
                .push_bind(user.id)
                .push(" AND channelprofile_id=")
                .push_bind(profile)
                .push("))");
        }
        if !params.contains_key("show_disabled") {
            query.push(" AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofilemembership m WHERE m.channel_id=c.id AND m.channel_profile_id=")
                .push_bind(profile).push(" AND m.enabled)");
        }
    }
    for (name, condition) in [
        ("only_streamless", " AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs WHERE cs.channel_id=c.id)"),
        ("only_stale", " AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE cs.channel_id=c.id AND s.is_stale)"),
        ("only_has_overrides", " AND o.id IS NOT NULL"),
        ("only_catchup", " AND c.is_catchup"),
        ("only_radio", " AND COALESCE(o.is_radio,c.is_radio)"),
    ] {
        if params.get(name).is_some_and(|value| !value.is_empty()) {
            query.push(condition);
        }
    }
    if let Some(name) = params.get("name").filter(|value| !value.is_empty()) {
        query.push(" AND ");
        contains(query, "COALESCE(o.name,c.name)", name);
    }
    if let Some(groups) = params
        .get("channel_group")
        .filter(|value| !value.is_empty())
    {
        query.push(" AND COALESCE(og.name,g.name) IN (");
        let mut list = query.separated(",");
        for group in groups.split(',') {
            list.push_bind(group.to_owned());
        }
        query.push(")");
    }
    if let Some(epgs) = params.get("epg").filter(|value| !value.is_empty()) {
        query.push(" AND (");
        for (index, epg) in epgs.split(',').map(str::trim).enumerate() {
            if index > 0 {
                query.push(" OR ");
            }
            if epg == "null" {
                query.push("o.epg_data_id IS NULL AND c.epg_data_id IS NULL");
            } else {
                contains(
                    query,
                    "CASE WHEN o.epg_data_id IS NOT NULL THEN oes.name ELSE es.name END",
                    epg,
                );
            }
        }
        query.push(")");
    }
    if let Some(search) = params.get("search").filter(|value| !value.is_empty()) {
        if search.contains('\0') {
            return Err(StoreError::InvalidInput(json!([
                "Null characters are not allowed."
            ])));
        }
        for term in channels::search_terms(search) {
            query.push(" AND (");
            contains(query, "COALESCE(o.name,c.name)", &term);
            query.push(" OR ");
            contains(query, "COALESCE(og.name,g.name)", &term);
            query.push(")");
        }
    }
    Ok(())
}

fn json_column<T: serde::de::DeserializeOwned>(
    row: &sqlx::sqlite::SqliteRow,
    field: &str,
) -> Result<T, StoreError> {
    let value: Option<String> = row.try_get(field)?;
    serde_json::from_str(value.as_deref().unwrap_or("null"))
        .map_err(|_| StoreError::Unsupported("Malformed copied SQLite channel projection"))
}

pub(crate) fn normalize_stream_timestamps(raw: &mut Value) {
    for field in STREAM_TIMESTAMPS {
        if let Some(parsed) = raw[field]
            .as_str()
            .and_then(|value| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").ok())
        {
            raw[field] = Value::String(parsed.and_utc().to_rfc3339());
        }
    }
}

fn project(
    row: sqlx::sqlite::SqliteRow,
    include: bool,
    admin: bool,
    base: &Url,
) -> Result<Value, StoreError> {
    let mut raw: Value = json_column(&row, "raw")?;
    let identity = channel_uuid(raw["uuid"].as_str())?;
    raw["uuid"] = Value::String(identity);
    let overrides: Option<Value> = json_column(&row, "overrides")?;
    let mut streams: Vec<Value> = json_column(&row, "streams")?;
    for stream in &mut streams {
        normalize_stream_timestamps(&mut stream["raw"]);
    }
    Ok(channels::serialize_channel(
        &raw,
        overrides,
        &streams,
        row.try_get("account_name")?,
        include,
        admin,
        base,
    ))
}

fn channel_uuid(value: Option<&str>) -> Result<String, StoreError> {
    value
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .map(|identity| identity.to_string())
        .ok_or(StoreError::Unsupported(
            "Malformed copied SQLite channel UUID",
        ))
}

#[derive(Clone, Copy)]
pub(crate) enum ChannelProjection {
    Ids,
    Summary,
}

pub(crate) async fn read_projection(
    connection: &mut SqliteConnection,
    user: &User,
    params: &HashMap<String, String>,
    projection: ChannelProjection,
) -> Result<Value, StoreError> {
    let fields = match projection {
        ChannelProjection::Ids => "c.id",
        ChannelProjection::Summary => {
            "c.id, c.uuid, COALESCE(o.name,c.name) AS name,
            COALESCE(o.logo_id,c.logo_id) AS logo_id,
            COALESCE(o.channel_number,c.channel_number) AS channel_number,
            COALESCE(o.epg_data_id,c.epg_data_id) AS epg_data_id,
            COALESCE(o.channel_group_id,c.channel_group_id) AS channel_group_id"
        }
    };
    let mut query = QueryBuilder::new(format!("SELECT {fields}{CHANNELS}"));
    filters(&mut query, params, user, false)?;
    query
        .push(" ORDER BY ")
        .push(channels::channel_ordering(params));
    let mut rows = query.build().fetch(connection);
    let mut values = Vec::new();
    while let Some(row) = rows.try_next().await? {
        let id: i64 = row.try_get("id")?;
        let value = match projection {
            ChannelProjection::Ids => Value::from(id),
            ChannelProjection::Summary => json!({
                "id": id,
                "uuid": channel_uuid(Some(row.try_get::<&str, _>("uuid")?))?,
                "name": row.try_get::<String, _>("name")?,
                "logo_id": row.try_get::<Option<i64>, _>("logo_id")?,
                "channel_number": row.try_get::<f64, _>("channel_number")?,
                "epg_data_id": row.try_get::<Option<i64>, _>("epg_data_id")?,
                "channel_group_id": row.try_get::<Option<i64>, _>("channel_group_id")?,
            }),
        };
        values.push(value);
    }
    Ok(Value::Array(values))
}

pub(crate) async fn read(
    connection: &mut SqliteConnection,
    user: &User,
    params: &HashMap<String, String>,
    base: &Url,
    detail: Option<i64>,
) -> Result<Value, StoreError> {
    let paginated = detail.is_none()
        && ["page", "page_size"]
            .iter()
            .any(|key| params.get(*key).is_some_and(|value| !value.is_empty()));
    let size = channels::channel_page_size(params);
    let count = if paginated {
        let mut count_query = QueryBuilder::new(format!("SELECT count(*){CHANNELS}"));
        filters(&mut count_query, params, user, false)?;
        count_query
            .build_query_scalar::<i64>()
            .fetch_one(&mut *connection)
            .await? as usize
    } else {
        0
    };
    let (page, pages) = channels::channel_page(params, paginated, count, size)?;
    let mut query = QueryBuilder::new(projected_select());
    filters(&mut query, params, user, detail.is_some())?;
    let include = params
        .get("include_streams")
        .is_some_and(|value| value == "true");
    let admin = user.user_level >= 10;
    if let Some(id) = detail {
        query.push(" AND c.id=").push_bind(id);
        let row = query
            .build()
            .fetch_optional(connection)
            .await?
            .ok_or_else(channels::missing)?;
        return project(row, include, admin, base);
    }
    query
        .push(" ORDER BY ")
        .push(channels::channel_ordering(params));
    if paginated {
        query
            .push(" LIMIT ")
            .push_bind(size as i64)
            .push(" OFFSET ")
            .push_bind(((page - 1) * size) as i64);
    }
    let rows = query.build().fetch_all(&mut *connection).await?;
    let payload = rows
        .into_iter()
        .map(|row| project(row, include, admin, base))
        .collect::<Result<Vec<_>, _>>()?;
    if !paginated {
        return Ok(Value::Array(payload));
    }
    let unassigned: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c
        LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
        WHERE c.epg_data_id IS NULL AND o.epg_data_id IS NULL)",
    )
    .fetch_one(connection)
    .await?;
    Ok(channels::channel_page_response(
        base, page, pages, count, payload, unassigned,
    ))
}
