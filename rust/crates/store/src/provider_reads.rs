//! Existing M3U/EPG management read contracts from original Dispatcharr views,
//! serializers and models. Original authors retain full credit; ATTRIBUTION.md.
use crate::{
    catalogue_options,
    channels::{float, timestamp},
    core_reads::{negotiate_literal_json_alias, omitted_json_method, JsonReadId},
    AuthService, StoreError,
};
use axum::{
    extract::{ConnectInfo, Path, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Row};
use std::net::SocketAddr;

mod m3u_json;

#[derive(Clone, Copy)]
enum Resource {
    Accounts,
    Profiles,
    Filters,
    ServerGroups,
    EpgSources,
    EpgData,
}
impl Resource {
    fn missing(self) -> StoreError {
        StoreError::StatusBody(
            404,
            json!({"detail":format!("No {} matches the given query.",self.model())}),
        )
    }
    fn model(self) -> &'static str {
        match self {
            Self::Accounts => "M3UAccount",
            Self::Profiles => "M3UAccountProfile",
            Self::Filters => "M3UFilter",
            Self::ServerGroups => "ServerGroup",
            Self::EpgSources => "EPGSource",
            Self::EpgData => "EPGData",
        }
    }
    fn query(self) -> &'static str {
        match self {
        Self::Accounts=>ACCOUNT_QUERY,
        Self::Profiles=>"SELECT to_jsonb(p) AS raw,to_jsonb(a) AS account FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE p.m3u_account_id=$2 AND ($1::bigint IS NULL OR p.id=$1)",
        Self::Filters=>"SELECT to_jsonb(f) AS raw FROM m3u_m3ufilter f WHERE f.m3u_account_id=$2 AND ($1::bigint IS NULL OR f.id=$1)",
        Self::ServerGroups=>"SELECT to_jsonb(g) AS raw FROM m3u_servergroup g WHERE ($1::bigint IS NULL OR g.id=$1) AND $2::bigint IS NULL",
        Self::EpgData=>"SELECT to_jsonb(e) AS raw FROM epg_epgdata e WHERE ($1::bigint IS NULL OR e.id=$1) AND $2::bigint IS NULL",
        // Keep list and detail as disjoint branches. A generic prepared plan for
        // (id = $1 OR $1 IS NULL) estimates a selective list filter and changes
        // the original select_related join order. A one-time NULL guard keeps
        // the list relation unfiltered; UNION ALL adds no invented ID ordering.
        Self::EpgSources=>"SELECT to_jsonb(e) AS raw,to_jsonb(t) AS task_data,to_jsonb(i) AS interval_data,to_jsonb(ct) AS crontab_data,CASE WHEN ct.id IS NULL THEN '' ELSE concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,ct.month_of_year,ct.day_of_week) END AS cron,(SELECT count(*) FROM epg_epgdata d WHERE d.epg_source_id=e.id) AS data_count,EXISTS(SELECT 1 FROM dispatcharr_channels_channel c JOIN epg_epgdata d ON d.id=c.epg_data_id WHERE d.epg_source_id=e.id) AS has_channels FROM epg_epgsource e LEFT JOIN django_celery_beat_periodictask t ON t.id=e.refresh_task_id LEFT JOIN django_celery_beat_intervalschedule i ON i.id=t.interval_id LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=t.crontab_id WHERE $1::bigint IS NULL AND $2::bigint IS NULL UNION ALL SELECT to_jsonb(e) AS raw,to_jsonb(t) AS task_data,to_jsonb(i) AS interval_data,to_jsonb(ct) AS crontab_data,CASE WHEN ct.id IS NULL THEN '' ELSE concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,ct.month_of_year,ct.day_of_week) END AS cron,(SELECT count(*) FROM epg_epgdata d WHERE d.epg_source_id=e.id) AS data_count,EXISTS(SELECT 1 FROM dispatcharr_channels_channel c JOIN epg_epgdata d ON d.id=c.epg_data_id WHERE d.epg_source_id=e.id) AS has_channels FROM epg_epgsource e LEFT JOIN django_celery_beat_periodictask t ON t.id=e.refresh_task_id LEFT JOIN django_celery_beat_intervalschedule i ON i.id=t.interval_id LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=t.crontab_id WHERE e.id=$1::bigint AND $2::bigint IS NULL",
    }
    }
}
// Pre-aggregate once per relation, including group stream counts, rather than
// issuing one query for every account/profile/filter/group in a returned list.
// Source lists have no explicit ordering, but select_related/prefetch row order
// is observable. Keep the source interval+crontab join projection, retain its
// base row ordinal before aggregate joins, and scope through-row prefetch to
// the selected parent IDs. Do not replace that order with an invented ID sort.
// Auxiliary task/interval/crontab columns intentionally preserve those joins;
// they are never included in the public serializer.
const ACCOUNT_QUERY: &str = "WITH base AS MATERIALIZED(SELECT a.id,to_jsonb(a) AS raw,row_number() OVER() AS position,to_jsonb(t) AS task_data,to_jsonb(i) AS interval_data,to_jsonb(ct) AS crontab_data,CASE WHEN ct.id IS NULL THEN '' ELSE concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,ct.month_of_year,ct.day_of_week) END AS cron FROM m3u_m3uaccount a LEFT JOIN django_celery_beat_periodictask t ON t.id=a.refresh_task_id LEFT JOIN django_celery_beat_intervalschedule i ON i.id=t.interval_id LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=t.crontab_id WHERE ($1::bigint IS NULL OR a.id=$1) AND $2::bigint IS NULL),counts AS(SELECT m3u_account_id,channel_group_id,count(*) AS n FROM dispatcharr_channels_stream WHERE ($1::bigint IS NULL OR m3u_account_id=$1) GROUP BY m3u_account_id,channel_group_id),profiles AS(SELECT m3u_account_id,jsonb_agg(to_jsonb(p)) AS rows FROM m3u_m3uaccountprofile p WHERE ($1::bigint IS NULL OR p.m3u_account_id=$1) GROUP BY m3u_account_id),filters AS(SELECT m3u_account_id,jsonb_agg(to_jsonb(f)) AS rows FROM m3u_m3ufilter f WHERE ($1::bigint IS NULL OR f.m3u_account_id=$1) GROUP BY m3u_account_id),group_base AS MATERIALIZED(SELECT r.m3u_account_id,r.channel_group_id,to_jsonb(r) AS raw,row_number() OVER() AS position FROM dispatcharr_channels_channelgroupm3uaccount r WHERE r.m3u_account_id=ANY(ARRAY(SELECT id FROM base)) AND ($1::bigint IS NULL OR r.m3u_account_id=$1)),groups AS(SELECT r.m3u_account_id,jsonb_agg(r.raw||jsonb_build_object('stream_count',COALESCE(c.n,0)) ORDER BY r.position) AS rows FROM group_base r LEFT JOIN counts c ON c.m3u_account_id=r.m3u_account_id AND c.channel_group_id=r.channel_group_id GROUP BY r.m3u_account_id) SELECT b.raw,COALESCE(p.rows,'[]'::jsonb) AS profiles,COALESCE(f.rows,'[]'::jsonb) AS filters,COALESCE(g.rows,'[]'::jsonb) AS groups,b.cron,b.task_data,b.interval_data,b.crontab_data FROM base b LEFT JOIN profiles p ON p.m3u_account_id=b.id LEFT JOIN filters f ON f.m3u_account_id=b.id LEFT JOIN groups g ON g.m3u_account_id=b.id ORDER BY b.position";

fn fields(raw: &Value, fields: &[&str]) -> Value {
    let mut result = Map::new();
    for field in fields {
        result.insert((*field).into(), raw[*field].clone());
    }
    Value::Object(result)
}
fn date(value: &Value) -> Option<DateTime<Utc>> {
    value
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}
fn seconds(value: &Value) -> Value {
    date(value)
        .map(|value| Value::String(value.to_rfc3339_opts(SecondsFormat::Secs, true)))
        .unwrap_or(Value::Null)
}
pub(crate) fn profile(raw: &Value, account: &Value) -> Value {
    let mut value = fields(
        raw,
        &[
            "id",
            "name",
            "max_streams",
            "is_active",
            "is_default",
            "current_viewers",
            "search_pattern",
            "replace_pattern",
            "custom_properties",
            "exp_date",
        ],
    );
    value["exp_date"] = timestamp(&raw["exp_date"]);
    value["account"] = json!({"id":account["id"],"name":account["name"],"account_type":account["account_type"],"is_xtream_codes":account["account_type"]=="XC"});
    value
}
pub(crate) fn filter(raw: &Value) -> Value {
    fields(
        raw,
        &[
            "id",
            "filter_type",
            "regex_pattern",
            "exclude",
            "order",
            "custom_properties",
        ],
    )
}
pub(crate) fn profile_properties(raw: &Value) -> Value {
    let parsed = if let Some(raw) = raw.as_str() {
        serde_json::from_str(raw).unwrap_or(Value::Null)
    } else {
        raw.clone()
    };
    let mut value = parsed.as_object().cloned().unwrap_or_default();
    if let Some(raw) = value.get("channel_profile_ids").cloned() {
        let items = if let Some(items) = raw.as_array() {
            items.clone()
        } else if raw.is_null() || raw == "" {
            Vec::new()
        } else {
            vec![raw]
        };
        let ids = items
            .into_iter()
            .filter_map(|value| match value {
                Value::Bool(value) => Some(json!(if value { 1 } else { 0 })),
                Value::Number(number) => {
                    if number.is_f64() {
                        number.as_f64().filter(|n| n.is_finite()).and_then(|n| {
                            serde_json::from_str::<Value>(&format!("{:.0}", n.trunc())).ok()
                        })
                    } else {
                        Some(Value::Number(number))
                    }
                }
                Value::String(value) => {
                    let raw = value.trim();
                    let unsigned = raw.strip_prefix(['+', '-']).unwrap_or(raw);
                    if unsigned.starts_with('_')
                        || unsigned.ends_with('_')
                        || unsigned.contains("__")
                    {
                        return None;
                    }
                    num_bigint::BigInt::parse_bytes(raw.replace('_', "").as_bytes(), 10)
                        .and_then(|value| serde_json::from_str(&value.to_string()).ok())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        value.insert("channel_profile_ids".into(), Value::Array(ids));
    }
    Value::Object(value)
}
fn group(raw: &Value) -> Value {
    let mut value = fields(
        raw,
        &[
            "enabled",
            "auto_channel_sync",
            "custom_properties",
            "is_stale",
            "stream_count",
        ],
    );
    value["channel_group"] = raw["channel_group_id"].clone();
    for key in ["auto_sync_channel_start", "auto_sync_channel_end"] {
        value[key] = float(raw[key].clone());
    }
    value["last_seen"] = timestamp(&raw["last_seen"]);
    value["custom_properties"] = profile_properties(&raw["custom_properties"]);
    value
}
fn account(row: &PgRow, admin: bool) -> Result<Value, StoreError> {
    let raw: Value = row.try_get("raw")?;
    let profiles: Value = row.try_get("profiles")?;
    let filters: Vec<Value> =
        serde_json::from_value(row.try_get("filters")?).expect("SQL aggregate array");
    let groups: Value = row.try_get("groups")?;
    Ok(account_value(
        &raw,
        profiles.as_array().expect("SQL aggregate array"),
        filters,
        groups.as_array().expect("SQL aggregate array"),
        row.try_get("cron")?,
        admin,
    ))
}

pub(crate) fn account_value(
    raw: &Value,
    profiles: &[Value],
    mut filters: Vec<Value>,
    groups: &[Value],
    cron: String,
    admin: bool,
) -> Value {
    let mut value = fields(
        raw,
        &[
            "id",
            "name",
            "server_url",
            "file_path",
            "max_streams",
            "is_active",
            "created_at",
            "updated_at",
            "locked",
            "refresh_interval",
            "custom_properties",
            "account_type",
            "username",
            "stale_stream_days",
            "priority",
            "status",
            "last_message",
        ],
    );
    value["server_group"] = raw["server_group_id"].clone();
    value["user_agent"] = raw["user_agent_id"].clone();
    for key in ["created_at", "updated_at"] {
        value[key] = timestamp(&raw[key]);
    }
    if admin {
        value["password"] = raw["password"].as_str().unwrap_or("").into();
    }
    for (key, default) in [
        ("enable_vod", false),
        ("auto_enable_new_groups_live", true),
        ("auto_enable_new_groups_vod", true),
        ("auto_enable_new_groups_series", true),
    ] {
        value[key] = raw["custom_properties"]
            .get(key)
            .cloned()
            .unwrap_or(json!(default));
    }
    value["hash_key"] = raw["custom_properties"]
        .get("hash_key")
        .cloned()
        .unwrap_or(Value::Null);
    value["cron_expression"] = cron.into();
    value["profiles"] = Value::Array(profiles.iter().map(|p| profile(p, raw)).collect());
    value["exp_date"] = profiles
        .iter()
        .find(|p| p["is_default"] == true)
        .map(|p| seconds(&p["exp_date"]))
        .unwrap_or(Value::Null);
    let mut expiring = profiles
        .iter()
        .filter_map(|p| date(&p["exp_date"]).map(|date| (date, p)))
        .collect::<Vec<_>>();
    expiring.sort_by_key(|(date, _)| *date);
    value["earliest_expiration"] = expiring
        .iter()
        .find(|(_, p)| p["is_active"] == true)
        .map(|(_, p)| seconds(&p["exp_date"]))
        .unwrap_or(Value::Null);
    value["all_expirations"]=Value::Array(expiring.iter().map(|(_,p)|json!({"profile_id":p["id"],"profile_name":p["name"],"exp_date":seconds(&p["exp_date"]),"is_active":p["is_active"]})).collect());
    filters.sort_by_key(|value| value["order"].as_u64().unwrap_or(0));
    value["filters"] = Value::Array(filters.iter().map(filter).collect());
    value["channel_groups"] = Value::Array(groups.iter().map(group).collect());
    value
}
fn serialize(resource: Resource, row: PgRow, admin: bool) -> Result<Value, StoreError> {
    if matches!(resource, Resource::Accounts) {
        return account(&row, admin);
    }
    let raw: Value = row.try_get("raw")?;
    Ok(match resource {
        Resource::Accounts => unreachable!(),
        Resource::Profiles => profile(&raw, &row.try_get::<Value, _>("account")?),
        Resource::Filters => filter(&raw),
        Resource::ServerGroups => fields(&raw, &["id", "name"]),
        Resource::EpgData => {
            let mut value = fields(&raw, &["id", "tvg_id", "name", "icon_url"]);
            value["epg_source"] = raw["epg_source_id"].clone();
            value
        }
        Resource::EpgSources => epg_source_projection(
            &raw,
            row.try_get::<String, _>("cron")?,
            row.try_get::<i64, _>("data_count")?,
            row.try_get::<bool, _>("has_channels")?,
        ),
    })
}
fn epg_source_projection(raw: &Value, cron: String, data_count: i64, has_channels: bool) -> Value {
    let mut value = fields(
        raw,
        &[
            "id",
            "name",
            "source_type",
            "url",
            "username",
            "is_active",
            "file_path",
            "refresh_interval",
            "cron_expression",
            "priority",
            "status",
            "last_message",
            "created_at",
            "updated_at",
            "custom_properties",
        ],
    );
    // DRF replaces this existing serializer field with the linked schedule.
    value["cron_expression"] = cron.into();
    for key in ["created_at", "updated_at"] {
        value[key] = timestamp(&raw[key]);
    }
    value["epg_data_count"] = data_count.into();
    value["has_channels"] = has_channels.into();
    value
}
// Shared row projection for the incremental EPG component; the established
// API query and serializer remain authoritative for source/data fields.
pub(crate) fn epg_read_query(source: bool) -> &'static str {
    (if source {
        Resource::EpgSources
    } else {
        Resource::EpgData
    })
    .query()
}
pub(crate) fn epg_read_projection(row: PgRow, source: bool) -> Result<Value, StoreError> {
    serialize(
        if source {
            Resource::EpgSources
        } else {
            Resource::EpgData
        },
        row,
        false,
    )
}
async fn read(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    resource: Resource,
    id: Option<String>,
    parent: Option<String>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let detail = id.is_some();
    let parse = |raw: String| {
        crate::channel_memberships::primary_key(&Value::String(raw))
            .map_err(|_| StoreError::NotFound)
    };
    let id = if let Some(raw) = id {
        Some(parse(raw)?.ok_or_else(|| resource.missing())?)
    } else {
        None
    };
    let parent = if let Some(raw) = parent {
        if !raw.bytes().all(|value| value.is_ascii_digit()) {
            return Err(StoreError::NotFound);
        }
        match parse(raw)? {
            Some(parent) => Some(parent),
            None => {
                return if detail {
                    Err(resource.missing())
                } else {
                    Ok(Json(json!([])))
                }
            }
        }
    } else {
        None
    };
    let rows = sqlx::query(resource.query())
        .bind(id)
        .bind(parent)
        .fetch_all(auth.store.pool())
        .await?;
    let mut values = rows
        .into_iter()
        .map(|row| serialize(resource, row, user.user_level >= 10))
        .collect::<Result<Vec<_>, _>>()?;
    if id.is_some() {
        return Ok(Json(values.pop().ok_or_else(|| resource.missing())?));
    }
    Ok(Json(Value::Array(values)))
}
macro_rules! endpoints {
    ($list:ident,$detail:ident,$resource:ident) => {
        async fn $list(
            State(auth): State<AuthService>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            read(auth, peer, headers, Resource::$resource, None, None).await
        }
        async fn $detail(
            State(auth): State<AuthService>,
            Path(id): Path<String>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            read(auth, peer, headers, Resource::$resource, Some(id), None).await
        }
    };
}
endpoints!(accounts, account_detail, Accounts);
endpoints!(server_groups, server_group, ServerGroups);
endpoints!(epg_sources, epg_source, EpgSources);
endpoints!(epg_data, epg_entry, EpgData);
macro_rules! provider_json_aliases {
    ($list_alias:ident, $list:ident, $detail_alias:ident, $detail:ident) => {
        async fn $list_alias(
            auth: State<AuthService>,
            peer: ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            negotiate_literal_json_alias(&headers)?;
            $list(auth, peer, headers).await
        }
        async fn $detail_alias(
            auth: State<AuthService>,
            JsonReadId(id): JsonReadId,
            peer: ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            negotiate_literal_json_alias(&headers)?;
            $detail(auth, Path(id), peer, headers).await
        }
    };
}
provider_json_aliases!(
    server_groups_json,
    server_groups,
    server_group_json,
    server_group
);
provider_json_aliases!(epg_data_json, epg_data, epg_entry_json, epg_entry);
provider_json_aliases!(accounts_json, accounts, account_detail_json, account_detail);
provider_json_aliases!(epg_sources_json, epg_sources, epg_source_json, epg_source);

struct ProviderJsonFormatter;
impl serde_json::ser::Formatter for ProviderJsonFormatter {
    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let mut rest = fragment;
        while let Some((index, separator)) = rest
            .char_indices()
            .find(|(_, c)| matches!(*c, '\u{2028}' | '\u{2029}'))
        {
            writer.write_all(&rest.as_bytes()[..index])?;
            writer.write_all(if separator == '\u{2028}' {
                b"\\u2028"
            } else {
                b"\\u2029"
            })?;
            rest = &rest[index + separator.len_utf8()..];
        }
        writer.write_all(rest.as_bytes())
    }
    fn write_f64<W>(&mut self, writer: &mut W, value: f64) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let raw = crate::source_finite_float_string(value)
            .ok_or_else(|| std::io::Error::other("finite provider number required"))?;
        writer.write_all(raw.as_bytes())
    }
    fn write_number_str<W>(&mut self, writer: &mut W, value: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        // serde_json's enabled arbitrary_precision representation delivers
        // Number::from_f64 through this callback. Counts/IDs remain integers;
        // nested JSON properties retain Source finite-float spelling.
        if value.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
            let value = value
                .parse::<f64>()
                .map_err(|_| std::io::Error::other("provider number shape"))?;
            self.write_f64(writer, value)
        } else {
            writer.write_all(value.as_bytes())
        }
    }
}
pub(super) fn provider_reply<T: serde::Serialize + ?Sized>(
    value: &T,
) -> Result<axum::response::Response, StoreError> {
    use axum::response::IntoResponse;
    const OUTPUT: usize = 64 * 1024;
    let capacity = || StoreError::Unsupported("Provider rendered response capacity");
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|n| *n <= OUTPUT)
                .ok_or_else(|| std::io::Error::other("bounded provider JSON"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde::Serialize::serialize(
        value,
        &mut serde_json::Serializer::with_formatter(&mut count, ProviderJsonFormatter),
    )
    .map_err(|_| capacity())?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(count.0).map_err(|_| capacity())?;
    let allocated = bytes.capacity();
    if allocated > OUTPUT {
        return Err(capacity());
    }
    serde::Serialize::serialize(
        value,
        &mut serde_json::Serializer::with_formatter(&mut bytes, ProviderJsonFormatter),
    )
    .map_err(|_| capacity())?;
    if bytes.len() != count.0 || bytes.capacity() != allocated {
        return Err(capacity());
    }
    Ok(([("content-type", "application/json")], bytes).into_response())
}

/// Source EPGSourceSerializer field order, including transient cron before
/// priority. The read projection's SQL join order is not serializer field order.
pub(super) fn epg_create_reply(value: &Value) -> Result<axum::response::Response, StoreError> {
    let mut ordered = Map::new();
    for name in [
        "id",
        "name",
        "source_type",
        "url",
        "username",
        "is_active",
        "file_path",
        "refresh_interval",
        "cron_expression",
        "priority",
        "status",
        "last_message",
        "created_at",
        "updated_at",
        "custom_properties",
        "epg_data_count",
        "has_channels",
    ] {
        ordered.insert(name.into(), value[name].clone());
    }
    let mut response = provider_reply(&Value::Object(ordered))?;
    // The Source CreateModelMixin uses this serializer field for Location.
    let location = value["url"].as_str().unwrap_or("None");
    response.headers_mut().insert(
        axum::http::header::LOCATION,
        axum::http::HeaderValue::from_str(location).map_err(|_| StoreError::UnhandledRestFault)?,
    );
    Ok(response)
}
/// Keep validation-error strings on the same pinned DRF separator renderer.
/// Other error variants retain the established auth/throttle/header adapters.
pub(super) fn provider_validation_error(error: StoreError) -> axum::response::Response {
    use axum::response::IntoResponse;
    let (status, value) = match error {
        StoreError::InvalidInput(value) => (400, value),
        StoreError::StatusBody(status, value) => (status, value),
        error => return error.into_response(),
    };
    match provider_reply(&value) {
        Ok(response) => {
            let mut response = (
                axum::http::StatusCode::from_u16(status)
                    .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
                response,
            )
                .into_response();
            if status == 401 {
                response.headers_mut().insert(
                    "www-authenticate",
                    axum::http::HeaderValue::from_static("Bearer realm=\"api\""),
                );
            }
            response
        }
        Err(error) => error.into_response(),
    }
}
async fn preview_response_headers(
    mut response: axum::response::Response,
) -> axum::response::Response {
    if response.status() == axum::http::StatusCode::INTERNAL_SERVER_ERROR {
        return response;
    }
    response.headers_mut().insert(
        axum::http::header::ALLOW,
        axum::http::HeaderValue::from_static("GET, HEAD, OPTIONS"),
    );
    response.headers_mut().insert(
        axum::http::header::VARY,
        axum::http::HeaderValue::from_static("Accept"),
    );
    response
}
// Delete-confirmation preview follows Original M3UAccountViewSet's detail
// action; original Dispatcharr authors retain application and algorithm credit.
async fn auto_created_channels_count(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<axum::response::Response, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = crate::channel_memberships::primary_key(&Value::String(id))
        .map_err(|_| StoreError::NotFound)?
        .ok_or_else(|| Resource::Accounts.missing())?;
    // get_object precedes channel reads, including for an empty preview.
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE id=$1)")
            .bind(id)
            .fetch_one(auth.store.pool())
            .await?;
    if !exists {
        return Err(Resource::Accounts.missing());
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM dispatcharr_channels_channel WHERE auto_created AND auto_created_by_id=$1",
    )
    .bind(id)
    .fetch_one(auth.store.pool())
    .await?;
    // Source has no model ordering or action order_by: preserve the limited
    // database result order and never replace it with an invented ID sort.
    let sample_names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM dispatcharr_channels_channel WHERE auto_created AND auto_created_by_id=$1 LIMIT 5",
    )
    .bind(id)
    .fetch_all(auth.store.pool())
    .await?;
    provider_reply(&json!({"count":count,"sample_names":sample_names}))
}
async fn auto_created_channels_count_options(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Result<axum::response::Response, StoreError> {
    use crate::channels::{
        channel_options_indentation, options_json_response, serialize_options_json,
    };

    if id.contains(['.', '/']) {
        return Err(StoreError::NotFound);
    }
    let Ok(indentation) = channel_options_indentation(&uri, &headers) else {
        return Ok(StatusCode::METHOD_NOT_ALLOWED.into_response());
    };
    auth.account_user(peer, headers, true).await?;
    // Metadata is independent of whether the account exists.
    let metadata = json!({
        "name": "Auto created channels count",
        "description": "Preview how many auto-created channels would be removed if the account\nwere deleted with cleanup_channels=true. The frontend calls this when\nthe user clicks Delete, to render a truthful confirmation dialog\n(\"Also delete N channels auto-created by this provider?\").",
        "renders": ["application/json", "text/html"],
        "parses": ["application/json", "application/x-www-form-urlencoded", "multipart/form-data"]
    });
    options_json_response(
        serialize_options_json(metadata.as_object().expect("preview metadata"), indentation)
            .ok_or(StoreError::UnhandledRestFault)?,
    )
}

macro_rules! nested {
    ($list:ident,$detail:ident,$resource:ident) => {
        async fn $list(
            State(auth): State<AuthService>,
            Path(parent): Path<String>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            read(auth, peer, headers, Resource::$resource, None, Some(parent)).await
        }
        async fn $detail(
            State(auth): State<AuthService>,
            Path((parent, id)): Path<(String, String)>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            read(
                auth,
                peer,
                headers,
                Resource::$resource,
                Some(id),
                Some(parent),
            )
            .await
        }
    };
}
nested!(profiles, profile_detail, Profiles);
nested!(filters, filter_detail, Filters);
// Actual XC11 raw create response order, before inline Groups changes status.
pub(super) fn xc_account_reply(
    value: &Value,
    create: bool,
) -> Result<axum::response::Response, StoreError> {
    let mut ordered = Map::new();
    for name in [
        "id",
        "name",
        "server_url",
        "file_path",
        "server_group",
        "max_streams",
        "is_active",
        "created_at",
        "updated_at",
        "filters",
        "user_agent",
        "profiles",
        "locked",
        "channel_groups",
        "refresh_interval",
        "cron_expression",
        "custom_properties",
        "account_type",
        "username",
        "stale_stream_days",
        "priority",
        "status",
        "last_message",
        "earliest_expiration",
        "all_expirations",
        "password",
        "enable_vod",
        "auto_enable_new_groups_live",
        "auto_enable_new_groups_vod",
        "auto_enable_new_groups_series",
        "hash_key",
        "exp_date",
    ] {
        if !create && name == "cron_expression" {
            continue;
        }
        if !create && name == "exp_date" {
            ordered.insert("cron_expression".into(), value["cron_expression"].clone());
        }
        ordered.insert(name.into(), value[name].clone());
    }
    if let Some(groups) = value["channel_groups"].as_array() {
        ordered.insert(
            "channel_groups".into(),
            Value::Array(
                groups
                    .iter()
                    .map(|group| {
                        let mut fields = Map::new();
                        for name in [
                            "channel_group",
                            "enabled",
                            "auto_channel_sync",
                            "auto_sync_channel_start",
                            "auto_sync_channel_end",
                            "custom_properties",
                            "is_stale",
                            "last_seen",
                            "stream_count",
                        ] {
                            fields.insert(name.into(), group[name].clone());
                        }
                        Value::Object(fields)
                    })
                    .collect(),
            ),
        );
    }
    provider_reply(&Value::Object(ordered))
}
async fn account_detail_headers(
    mut response: axum::response::Response,
) -> axum::response::Response {
    if response.status() != axum::http::StatusCode::INTERNAL_SERVER_ERROR {
        response.headers_mut().insert(
            axum::http::header::ALLOW,
            axum::http::HeaderValue::from_static("GET, PUT, PATCH, DELETE, HEAD, OPTIONS"),
        );
        response.headers_mut().insert(
            axum::http::header::VARY,
            axum::http::HeaderValue::from_static("Accept"),
        );
    }
    response
}
async fn account_collection_headers(
    mut response: axum::response::Response,
) -> axum::response::Response {
    if response.status() != axum::http::StatusCode::INTERNAL_SERVER_ERROR {
        response.headers_mut().insert(
            axum::http::header::ALLOW,
            axum::http::HeaderValue::from_static("GET, POST, HEAD, OPTIONS"),
        );
        response.headers_mut().insert(
            axum::http::header::VARY,
            axum::http::HeaderValue::from_static("Accept"),
        );
    }
    response
}
const CRUD_LIST_ALLOW: &str = "GET, POST, HEAD, OPTIONS";
const CRUD_DETAIL_ALLOW: &str = "GET, PUT, PATCH, DELETE, HEAD, OPTIONS";

async fn epg_library_json_headers(
    State(allow): State<&'static str>,
    request: Request,
    next: Next,
) -> Response {
    let is_get = request.method() == Method::GET;
    let mut response = next.run(request).await;
    if is_get
        && response.status() == StatusCode::OK
        && response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value == "application/json")
    {
        let headers = response.headers_mut();
        headers.insert(header::ALLOW, HeaderValue::from_static(allow));
        // The app response middleware adds the origin token to this field.
        headers.insert(header::VARY, HeaderValue::from_static("Accept"));
    }
    response
}

pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/m3u/accounts/{parent}/group-settings/",
            axum::routing::patch(crate::vod_category_settings::patch),
        )
        .route(
            "/api/m3u/accounts.json",
            get(accounts_json)
                .layer(axum::middleware::map_response(account_collection_headers))
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/m3u/accounts/{json_id}",
            get(account_detail_json)
                .layer(axum::middleware::map_response(account_detail_headers))
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/m3u/accounts/",
            get(accounts)
                .post(crate::provider_account_writes::create)
                .layer(axum::middleware::map_response(account_collection_headers)),
        )
        .route(
            "/api/m3u/accounts/{parent}/",
            get(account_detail)
                .patch(crate::provider_account_writes::patch)
                .put(crate::provider_account_writes::put)
                .delete(crate::provider_account_writes::delete)
                .layer(axum::middleware::map_response(account_detail_headers)),
        )
        .route(
            "/api/m3u/accounts/{parent}/auto-created-channels-count/",
            get(auto_created_channels_count)
                .options(auto_created_channels_count_options)
                .layer(axum::middleware::map_response(preview_response_headers)),
        )
        .route(
            "/api/m3u/accounts/{parent}/profiles/",
            get(profiles).post(crate::provider_profile_writes::create),
        )
        .route(
            "/api/m3u/accounts/{parent}/profiles/{id}/",
            get(profile_detail)
                .patch(crate::provider_profile_writes::patch)
                .put(crate::provider_profile_writes::put)
                .delete(crate::provider_profile_writes::delete),
        )
        .route(
            "/api/m3u/accounts/{parent}/filters/",
            get(filters).post(crate::provider_mutations::create_filter),
        )
        .route(
            "/api/m3u/accounts/{parent}/filters/{id}/",
            get(filter_detail)
                .patch(crate::provider_mutations::patch_filter)
                .put(crate::provider_mutations::put_filter)
                .delete(crate::provider_mutations::delete_filter),
        )
        .route(
            "/api/m3u/accounts/{parent}/profiles.json",
            get(m3u_json::read_profiles)
                .layer(from_fn_with_state(
                    CRUD_LIST_ALLOW,
                    epg_library_json_headers,
                ))
                .post(m3u_json::create_profile)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/m3u/accounts/{parent}/profiles/{json_id}",
            get(m3u_json::read_profile)
                .layer(from_fn_with_state(
                    CRUD_DETAIL_ALLOW,
                    epg_library_json_headers,
                ))
                .patch(m3u_json::patch_profile)
                .put(m3u_json::put_profile)
                .delete(m3u_json::delete_profile)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/m3u/accounts/{parent}/filters.json",
            get(m3u_json::read_filters)
                .layer(from_fn_with_state(
                    CRUD_LIST_ALLOW,
                    epg_library_json_headers,
                ))
                .post(m3u_json::create_filter)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/m3u/accounts/{parent}/filters/{json_id}",
            get(m3u_json::read_filter)
                .layer(from_fn_with_state(
                    CRUD_DETAIL_ALLOW,
                    epg_library_json_headers,
                ))
                .patch(m3u_json::patch_filter)
                .put(m3u_json::put_filter)
                .delete(m3u_json::delete_filter)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/m3u/server-groups.json",
            get(server_groups_json)
                .layer(from_fn_with_state(
                    catalogue_options::COLLECTION_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .post(m3u_json::create_group)
                .options(catalogue_options::server_group_json_alias)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/m3u/server-groups/{json_id}",
            get(server_group_json)
                .layer(from_fn_with_state(
                    catalogue_options::DETAIL_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .patch(m3u_json::patch_group)
                .put(m3u_json::put_group)
                .delete(m3u_json::delete_group)
                .options(catalogue_options::server_group_json_alias)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/m3u/server-groups/",
            get(server_groups)
                .layer(from_fn_with_state(
                    catalogue_options::COLLECTION_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .post(crate::provider_mutations::create_server_group)
                .options(catalogue_options::server_group),
        )
        .route(
            "/api/m3u/server-groups/{id}/",
            get(server_group)
                .layer(from_fn_with_state(
                    catalogue_options::DETAIL_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .options(catalogue_options::server_group)
                .patch(crate::provider_mutations::patch_server_group)
                .put(crate::provider_mutations::put_server_group)
                .delete(crate::provider_mutations::delete_server_group),
        )
        .route(
            "/api/epg/sources.json",
            get(epg_sources_json)
                .layer(from_fn_with_state(
                    CRUD_LIST_ALLOW,
                    epg_library_json_headers,
                ))
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/epg/sources/{json_id}",
            get(epg_source_json)
                .layer(from_fn_with_state(
                    CRUD_DETAIL_ALLOW,
                    epg_library_json_headers,
                ))
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/epg/sources/",
            get(epg_sources)
                .layer(from_fn_with_state(
                    CRUD_LIST_ALLOW,
                    epg_library_json_headers,
                ))
                .post(crate::sd_source_http::create),
        )
        .route(
            "/api/epg/sources/{id}/sd-lineups/",
            axum::routing::any(crate::sd_source_http::lineup),
        )
        .route(
            "/api/epg/sources/{id}/sd-lineups/search/",
            axum::routing::any(crate::sd_source_http::search),
        )
        .route(
            "/api/epg/sources/{id}/",
            get(epg_source)
                .layer(from_fn_with_state(
                    CRUD_DETAIL_ALLOW,
                    epg_library_json_headers,
                ))
                .patch(crate::epg_source_writes::patch)
                .put(crate::epg_source_writes::put)
                .delete(crate::epg_source_writes::delete),
        )
        .route(
            "/api/epg/epgdata.json",
            get(epg_data_json)
                .layer(from_fn_with_state(
                    catalogue_options::READ_ONLY_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .options(catalogue_options::epg_data_json_alias)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/epg/epgdata/{json_id}",
            get(epg_entry_json)
                .layer(from_fn_with_state(
                    catalogue_options::READ_ONLY_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .options(catalogue_options::epg_data_json_alias)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/epg/epgdata/",
            get(epg_data)
                .layer(from_fn_with_state(
                    catalogue_options::READ_ONLY_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .options(catalogue_options::epg_data),
        )
        .route(
            "/api/epg/epgdata/{id}/",
            get(epg_entry)
                .layer(from_fn_with_state(
                    catalogue_options::READ_ONLY_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .options(catalogue_options::epg_data),
        )
        .with_state(auth)
}
pub(crate) async fn physical_account_detail(
    connection: &mut sqlx::PgConnection,
    id: i64,
) -> Result<Value, StoreError> {
    let row = sqlx::query(ACCOUNT_QUERY)
        .bind(Some(id))
        .bind(None::<i64>)
        .fetch_one(connection)
        .await?;
    account(&row, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn provider_router_registers_nested_routes_without_capture_conflicts() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://unused:unused@localhost/unused")
            .unwrap();
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("fixture-only-key".into()).unwrap(),
        );
        let _router = router(auth);
    }
    #[tokio::test]
    async fn provider_account_and_epg_source_json_aliases_mount_read_only() {
        use axum::body::Body;
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("fixture-only-key".into()).unwrap(),
        );
        let app = router(auth);
        for path in [
            "/api/m3u/accounts.json",
            "/api/m3u/accounts/2.json",
            "/api/epg/sources.json",
            "/api/epg/sources/27.json",
        ] {
            for (method, status) in [
                (Method::GET, StatusCode::NOT_ACCEPTABLE),
                (Method::HEAD, StatusCode::NOT_FOUND),
                (Method::POST, StatusCode::NOT_FOUND),
            ] {
                let mut request = Request::builder()
                    .method(method)
                    .uri(format!("{path}?format=api"))
                    .header(header::ACCEPT, "text/html")
                    .body(Body::empty())
                    .unwrap();
                request
                    .extensions_mut()
                    .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 1234))));
                let response = app.clone().oneshot(request).await.unwrap();
                assert_eq!(response.status(), status, "{path}");
            }
        }
    }

    #[test]
    fn epg_source_keeps_original_compact_json_field_order() {
        let raw = json!({
            "id": 17,
            "name": "Local guide",
            "source_type": "dummy",
            "is_active": false,
            "refresh_interval": 0,
            "priority": 0,
            "status": "idle",
            "created_at": "2030-01-02T03:04:05.123456+00:00",
            "custom_properties": {},
        });
        let projection = epg_source_projection(&raw, "1 2 * * *".into(), 1, false);
        assert_eq!(
            serde_json::to_string(&projection).unwrap(),
            concat!(
                r#"{"id":17,"name":"Local guide","source_type":"dummy","url":null,"username":null,"#,
                r#""is_active":false,"file_path":null,"refresh_interval":0,"cron_expression":"1 2 * * *","#,
                r#""priority":0,"status":"idle","last_message":null,"created_at":"2030-01-02T03:04:05.123456Z","#,
                r#""updated_at":null,"custom_properties":{},"epg_data_count":1,"has_channels":false}"#,
            ),
        );
    }

    #[tokio::test]
    async fn epg_library_headers_apply_to_successful_json_gets_only() {
        use axum::body::Body;
        use tower::ServiceExt;

        const EPG_DATA_ALLOW: &str = "GET, HEAD, OPTIONS";

        let headers = |allow| from_fn_with_state(allow, epg_library_json_headers);
        let app = Router::new()
            .route(
                "/source",
                get(|| async { Json(json!([])) }).layer(headers(CRUD_LIST_ALLOW)),
            )
            .route(
                "/detail",
                get(|| async { Json(json!({})) }).layer(headers(CRUD_DETAIL_ALLOW)),
            )
            .route(
                "/data",
                get(|| async { Json(json!([])) }).layer(headers(EPG_DATA_ALLOW)),
            )
            .route(
                "/error",
                get(|| async { (StatusCode::NOT_FOUND, Json(json!({}))) })
                    .layer(headers(EPG_DATA_ALLOW)),
            )
            .route("/outside", get(|| async { Json(json!([])) }));
        for (path, method, status, expected_allow) in [
            (
                "/source",
                Method::GET,
                StatusCode::OK,
                Some(CRUD_LIST_ALLOW),
            ),
            (
                "/detail",
                Method::GET,
                StatusCode::OK,
                Some(CRUD_DETAIL_ALLOW),
            ),
            ("/data", Method::GET, StatusCode::OK, Some(EPG_DATA_ALLOW)),
            ("/data", Method::HEAD, StatusCode::OK, None),
            ("/error", Method::GET, StatusCode::NOT_FOUND, None),
            ("/outside", Method::GET, StatusCode::OK, None),
        ] {
            let request = axum::http::Request::builder()
                .uri(path)
                .method(method)
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), status);
            for (name, expected) in [
                (header::ALLOW, expected_allow),
                (header::VARY, expected_allow.map(|_| "Accept")),
            ] {
                let values = response.headers().get_all(name);
                let text = values
                    .iter()
                    .map(|value| value.to_str().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(text, expected.into_iter().collect::<Vec<_>>());
            }
        }
    }

    #[test]
    fn legacy_group_profile_ids_keep_python_integer_coercion() {
        assert_eq!(
            profile_properties(&json!(
                "{\"channel_profile_ids\":[\"12\",true,2.9,\"bad\",null]}"
            )),
            json!({"channel_profile_ids":[12,1,2]})
        );
        assert_eq!(profile_properties(&Value::Null), json!({}));
        assert_eq!(
            profile_properties(&json!({"channel_profile_ids":null})),
            json!({"channel_profile_ids":[]})
        );
    }
    #[test]
    fn profile_expiration_retains_microseconds_but_account_tooltips_truncate() {
        let raw = json!({"exp_date":"2030-01-02T03:04:05.123456+00:00"});
        assert_eq!(
            profile(&raw, &json!({"account_type":"XC"}))["exp_date"],
            "2030-01-02T03:04:05.123456Z"
        );
        assert_eq!(seconds(&raw["exp_date"]), "2030-01-02T03:04:05Z");
    }
}

#[cfg(test)]
mod create_wire_controls {
    use super::*;
    use axum::body::to_bytes;
    #[tokio::test]
    async fn preview_wire_keeps_integer_count_string_names_and_source_separators() {
        let response =
            provider_reply(&json!({"count":7,"sample_names":["01","İ\u{2028}x\u{2029}"]})).unwrap();
        let bytes = to_bytes(response.into_body(), 32768).await.unwrap();
        assert_eq!(
            bytes.as_ref(),
            "{\"count\":7,\"sample_names\":[\"01\",\"İ\\u2028x\\u2029\"]}".as_bytes()
        );
    }
    #[tokio::test]
    async fn new_dummy_wire_follows_original_serializer_order_not_read_projection_order() {
        let response = epg_create_reply(&json!({
            "name":"İ\u{2028}","id":3,"source_type":"dummy","is_active":false,
            "refresh_interval":2,"priority":5,"status":"idle","custom_properties":{},
            "epg_data_count":1,"has_channels":false,"cron_expression":""
        }))
        .unwrap();
        assert_eq!(response.headers().get("location").unwrap(), "None");
        let bytes = to_bytes(response.into_body(), 32768).await.unwrap();
        assert_eq!(bytes.as_ref(), concat!(
            "{\"id\":3,\"name\":\"İ\\u2028\",\"source_type\":\"dummy\",",
            "\"url\":null,\"username\":null,\"is_active\":false,\"file_path\":null,",
            "\"refresh_interval\":2,\"cron_expression\":\"\",\"priority\":5,",
            "\"status\":\"idle\",\"last_message\":null,\"created_at\":null,",
            "\"updated_at\":null,\"custom_properties\":{},\"epg_data_count\":1,\"has_channels\":false}"
        ).as_bytes());
    }
    #[test]
    fn remote_xmltv_create_location_uses_the_supplied_url() {
        let url = "http://guide-host:8080/guide.xml?region=us";
        let response = epg_create_reply(&json!({"url":url})).unwrap();
        assert_eq!(response.headers()[axum::http::header::LOCATION], url);
    }
}
