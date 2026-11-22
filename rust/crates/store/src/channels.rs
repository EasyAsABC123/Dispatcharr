//! Channel API read contracts from original Dispatcharr serializers and access rules.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{AuthService, Store, StoreError, User};
use axum::{
    body::Bytes,
    extract::{ConnectInfo, OriginalUri, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use sqlx::{Postgres, QueryBuilder, Row};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::OnceLock,
};
use url::Url;
pub(crate) const COLLECTION_ALLOW: HeaderValue =
    HeaderValue::from_static("GET, POST, HEAD, OPTIONS");
pub(crate) const DETAIL_ALLOW: HeaderValue =
    HeaderValue::from_static("GET, PUT, PATCH, DELETE, HEAD, OPTIONS");
const DETAIL_OPTIONS: &[u8; 5290] =
    include_bytes!("../datasets/source-channel-detail-options.json");

#[cfg(feature = "sqlite-copied-data-runtime")]
#[path = "sqlite_channel_list.rs"]
pub(crate) mod sqlite_list;
pub(crate) const FIELDS: &[&str] = &[
    "id",
    "channel_number",
    "name",
    "channel_group_id",
    "tvg_id",
    "tvc_guide_stationid",
    "epg_data_id",
    "streams",
    "stream_profile_id",
    "uuid",
    "logo_id",
    "user_level",
    "is_adult",
    "is_catchup",
    "catchup_days",
    "is_radio",
    "hidden_from_output",
    "auto_created",
    "auto_created_by_id",
];
pub(crate) const OVERRIDE_FIELDS: &[&str] = &[
    "name",
    "channel_number",
    "channel_group_id",
    "logo_id",
    "tvg_id",
    "tvc_guide_stationid",
    "epg_data_id",
    "stream_profile_id",
    "is_radio",
];
pub(crate) const STREAM_FIELDS: &[&str] = &[
    "id",
    "name",
    "url",
    "m3u_account_id",
    "logo_url",
    "tvg_id",
    "local_file",
    "current_viewers",
    "updated_at",
    "last_seen",
    "is_stale",
    "is_adult",
    "stream_profile_id",
    "is_custom",
    "channel_group_id",
    "stream_hash",
    "stream_stats",
    "stream_stats_updated_at",
    "stream_id",
    "stream_chno",
    "is_catchup",
    "catchup_days",
    "is_radio",
];
pub(crate) const SELECT:&str="SELECT to_jsonb(c) AS raw,CASE WHEN o.id IS NULL THEN NULL ELSE to_jsonb(o) END AS ov,a.name AS account_name,COALESCE((SELECT jsonb_agg(jsonb_build_object('raw',to_jsonb(s),'account_name',ma.name) ORDER BY cs.\"order\") FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id LEFT JOIN m3u_m3uaccount ma ON ma.id=s.m3u_account_id WHERE cs.channel_id=c.id),'[]'::jsonb) AS streams FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id LEFT JOIN m3u_m3uaccount a ON a.id=c.auto_created_by_id LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=c.channel_group_id LEFT JOIN dispatcharr_channels_channelgroup og ON og.id=o.channel_group_id LEFT JOIN epg_epgdata e ON e.id=c.epg_data_id LEFT JOIN epg_epgdata oe ON oe.id=o.epg_data_id LEFT JOIN epg_epgsource es ON es.id=e.epg_source_id LEFT JOIN epg_epgsource oes ON oes.id=oe.epg_source_id WHERE true";
pub(crate) fn missing() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No Channel matches the given query."}))
}
pub(crate) fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_f64() != Some(0.0),
        Some(Value::String(value)) => !value.is_empty(),
        Some(Value::Array(value)) => !value.is_empty(),
        Some(Value::Object(value)) => !value.is_empty(),
    }
}
pub(crate) fn float(value: Value) -> Value {
    value
        .as_f64()
        .and_then(serde_json::Number::from_f64)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}
pub(crate) fn timestamp(value: &Value) -> Value {
    value
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| {
            let value = value.with_timezone(&Utc);
            Value::String(value.to_rfc3339_opts(
                if value.timestamp_subsec_micros() == 0 {
                    SecondsFormat::Secs
                } else {
                    SecondsFormat::Micros
                },
                true,
            ))
        })
        .unwrap_or(Value::Null)
}
pub(crate) fn stream(value: &Value, admin: bool, base: &Url) -> Value {
    let raw = &value["raw"];
    let mut result = Map::new();
    for field in STREAM_FIELDS {
        let mut item = raw[*field].clone();
        let output = match *field {
            "m3u_account_id" => "m3u_account",
            "channel_group_id" => "channel_group",
            other => other,
        };
        if *field == "url" && !admin {
            continue;
        }
        if ["updated_at", "last_seen", "stream_stats_updated_at"].contains(field) {
            item = timestamp(&item);
        }
        if *field == "stream_chno" {
            item = float(item);
        }
        if *field == "local_file" {
            item = match item.as_str().filter(|value| !value.is_empty()) {
                Some(value) => base
                    .join(&format!("/media/{value}"))
                    .ok()
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
                None => Value::Null,
            };
        }
        result.insert(output.into(), item);
    }
    Value::Object(result)
}
pub(crate) fn serialize(
    row: sqlx::postgres::PgRow,
    include_streams: bool,
    admin: bool,
    base: &Url,
) -> Result<Value, StoreError> {
    let raw: Value = row.try_get("raw")?;
    let ov: Option<Value> = row.try_get("ov")?;
    let streams: Value = row.try_get("streams")?;
    let streams = streams
        .as_array()
        .ok_or_else(|| StoreError::Unsupported("Malformed stream relation"))?;
    let account_name = row.try_get("account_name")?;
    Ok(serialize_channel(
        &raw,
        ov,
        streams,
        account_name,
        include_streams,
        admin,
        base,
    ))
}

pub(crate) fn serialize_channel(
    raw: &Value,
    ov: Option<Value>,
    streams: &[Value],
    account_name: Option<String>,
    include_streams: bool,
    admin: bool,
    base: &Url,
) -> Value {
    let mut value = Map::new();
    for field in FIELDS {
        // Stock replaces the streams serializer in place, keeping its position
        // for both a relation ID list and an expanded stream list.
        let (name, item) = match *field {
            "channel_number" => (*field, float(raw[*field].clone())),
            "streams" => (
                *field,
                Value::Array(
                    streams
                        .iter()
                        .map(|value| {
                            if include_streams {
                                stream(value, admin, base)
                            } else {
                                value["raw"]["id"].clone()
                            }
                        })
                        .collect(),
                ),
            ),
            "auto_created_by_id" => ("auto_created_by", raw[*field].clone()),
            _ => (*field, raw[*field].clone()),
        };
        value.insert(name.into(), item);
    }
    let override_value = ov
        .map(|ov| {
            let mut result = Map::new();
            for field in OVERRIDE_FIELDS {
                result.insert(
                    field.to_string(),
                    if *field == "channel_number" {
                        float(ov[*field].clone())
                    } else {
                        ov[*field].clone()
                    },
                );
            }
            Value::Object(result)
        })
        .unwrap_or(Value::Null);
    value.insert(
        "auto_created_by_name".into(),
        account_name.map_or(Value::Null, Value::from),
    );
    value.insert("override".into(), override_value);
    value.insert(
        "source_stream".into(),
        if raw["auto_created"] == true {
            streams
                .first()
                .map(|value| {
                    json!({"id": value["raw"]["id"], "name": value["raw"]["name"],
                           "account_id": value["raw"]["m3u_account_id"], "account_name": value["account_name"]})
                })
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        },
    );
    for field in OVERRIDE_FIELDS {
        let overridden = &value["override"][*field];
        let item = if overridden.is_null() {
            value[*field].clone()
        } else {
            overridden.clone()
        };
        value.insert(format!("effective_{field}"), item);
    }
    Value::Object(value)
}
pub(crate) fn scope<'a>(query: &mut QueryBuilder<'a, Postgres>, user: &User) {
    if user.user_level >= 10 {
        return;
    }
    query.push(" AND c.user_level<=").push_bind(user.user_level);
    if truthy(user.custom_properties.get("hide_adult_content")) {
        query.push(" AND NOT c.is_adult");
    }
    query.push(" AND (NOT EXISTS(SELECT 1 FROM accounts_user_channel_profiles up WHERE up.user_id=").push_bind(user.id).push(") OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles up JOIN dispatcharr_channels_channelprofilemembership m ON m.channel_profile_id=up.channelprofile_id WHERE up.user_id=").push_bind(user.id).push(" AND m.channel_id=c.id AND m.enabled))");
}
// Django smart_split keeps closed quoted segments together; DRF then strips
// outside commas, unescapes a whole quoted term, or splits unquoted commas.
pub(crate) fn search_terms(input: &str) -> Vec<String> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut i = 0;
    let mut terms = Vec::new();
    let space = |value: char| value.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&value);
    while i < chars.len() {
        while i < chars.len() && space(chars[i]) {
            i += 1;
        }
        let start = i;
        while i < chars.len() && !space(chars[i]) {
            if ['\'', '"'].contains(&chars[i]) {
                let quote = chars[i];
                let mut end = i + 1;
                let mut closed = None;
                while end < chars.len() {
                    if chars[end] == '\\' {
                        end += 2;
                    } else if chars[end] == quote {
                        closed = Some(end);
                        break;
                    } else {
                        end += 1;
                    }
                }
                if let Some(end) = closed {
                    i = end + 1;
                    continue;
                }
            }
            i += 1;
        }
        if start == i {
            continue;
        }
        let term = chars[start..i].iter().collect::<String>();
        let term = term.trim_matches(',');
        if term.len() >= 2
            && term.starts_with(['\'', '"'])
            && term.chars().next() == term.chars().last()
        {
            let quote = term.chars().next().expect("nonempty token");
            let inner = &term[1..term.len() - 1];
            terms.push(
                inner
                    .replace(&format!("\\{quote}"), &quote.to_string())
                    .replace("\\\\", "\\"),
            );
        } else {
            terms.extend(
                term.split(',')
                    .filter(|value| !value.is_empty())
                    .map(|value| value.trim_matches(space).to_owned()),
            );
        }
    }
    terms
}
pub(crate) fn like(value: &str) -> String {
    format!(
        "%{}%",
        value
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}
pub(crate) async fn filters<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    params: &HashMap<String, String>,
    user: &User,
    auth: &AuthService,
    detail: bool,
) -> Result<(), StoreError> {
    scope(query, user);
    if !detail {
        match params
            .get("visibility_filter")
            .map(String::as_str)
            .unwrap_or("active")
        {
            "hidden" => {
                query.push(" AND c.hidden_from_output");
            }
            "all" => {}
            _ => {
                query.push(" AND NOT c.hidden_from_output");
            }
        }
    }
    if let Some(profile) = params
        .get("channel_profile_id")
        .and_then(|value| value.parse::<i64>().ok())
    {
        let allowed: bool = if user.user_level >= 10 {
            true
        } else {
            sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM accounts_user_channel_profiles WHERE user_id=$1) OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles WHERE user_id=$1 AND channelprofile_id=$2)").bind(user.id).bind(profile).fetch_one(auth.store.pool()).await?
        };
        if !allowed {
            query.push(" AND false");
        } else if !params.contains_key("show_disabled") {
            query.push(" AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofilemembership m WHERE m.channel_id=c.id AND m.channel_profile_id=").push_bind(profile).push(" AND m.enabled)");
        }
    }
    for(name,condition)in[("only_streamless"," AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs WHERE cs.channel_id=c.id)"),("only_stale"," AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE cs.channel_id=c.id AND s.is_stale)"),("only_has_overrides"," AND o.id IS NOT NULL"),("only_catchup"," AND c.is_catchup"),("only_radio"," AND COALESCE(o.is_radio,c.is_radio)")]{if params.get(name).is_some_and(|value|!value.is_empty()){query.push(condition);}}
    if let Some(name) = params.get("name").filter(|value| !value.is_empty()) {
        query
            .push(" AND UPPER(COALESCE(o.name,c.name)) LIKE UPPER(")
            .push_bind(like(name))
            .push(")");
    }
    if let Some(groups) = params
        .get("channel_group")
        .filter(|value| !value.is_empty())
    {
        query.push(" AND (");
        for (index, group) in groups.split(',').enumerate() {
            if index > 0 {
                query.push(" OR ");
            }
            query
                .push("COALESCE(og.name,g.name)=")
                .push_bind(group.to_owned());
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
                query.push("UPPER(CASE WHEN o.epg_data_id IS NOT NULL THEN oes.name ELSE es.name END) LIKE UPPER(").push_bind(like(epg)).push(")");
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
        for term in search_terms(search) {
            query
                .push(" AND (UPPER(COALESCE(o.name,c.name)) LIKE UPPER(")
                .push_bind(like(&term))
                .push(") OR UPPER(COALESCE(og.name,g.name)) LIKE UPPER(")
                .push_bind(like(&term))
                .push("))");
        }
    }
    Ok(())
}
pub(crate) fn channel_ordering(params: &HashMap<String, String>) -> String {
    let clauses = params
        .get("ordering")
        .map(String::as_str)
        .unwrap_or("")
        .split(',')
        .filter_map(|value| {
            let value = value.trim();
            let desc = value.starts_with('-');
            let field = match value.trim_start_matches('-') {
                "channel_number" => "COALESCE(o.channel_number,c.channel_number)",
                "name" => "COALESCE(o.name,c.name)",
                "channel_group__name" => "COALESCE(og.name,g.name)",
                "epg_data__name" => "COALESCE(oe.name,e.name)",
                _ => return None,
            };
            Some(format!("{field} {}", if desc { "DESC" } else { "ASC" }))
        })
        .collect::<Vec<_>>();
    if clauses.is_empty() {
        "COALESCE(o.channel_number,c.channel_number) DESC".into()
    } else {
        clauses.join(",")
    }
}

fn order(query: &mut QueryBuilder<'_, Postgres>, params: &HashMap<String, String>) {
    query.push(" ORDER BY ").push(channel_ordering(params));
}
pub(crate) fn base_url(headers: &HeaderMap, uri: &Uri) -> Result<Url, StoreError> {
    Url::parse(&format!(
        "http://{}{}",
        headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("localhost"),
        uri
    ))
    .map_err(|_| StoreError::InvalidInput(json!({"detail":"Invalid HTTP host."})))
}
pub(crate) fn page_url(base: &Url, page: usize) -> String {
    let mut value = base.clone();
    let mut pairs = value
        .query_pairs()
        .filter(|(key, _)| key != "page")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    pairs.push(("page".into(), page.to_string()));
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    value.query_pairs_mut().clear().extend_pairs(pairs);
    value.to_string()
}

pub(crate) fn channel_page_size(params: &HashMap<String, String>) -> usize {
    params
        .get("page_size")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(50)
        .min(10000)
}

pub(crate) fn channel_page(
    params: &HashMap<String, String>,
    paginated: bool,
    count: usize,
    size: usize,
) -> Result<(usize, usize), StoreError> {
    let invalid = || StoreError::StatusBody(404, json!({"detail":"Invalid page."}));
    let pages = count.div_ceil(size).max(1);
    let page = if !paginated {
        1
    } else if params.get("page").is_some_and(|value| value == "last") {
        pages
    } else {
        params
            .get("page")
            .filter(|value| !value.is_empty())
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| invalid())?
            .unwrap_or(1)
    };
    if paginated && (page == 0 || page > pages) {
        return Err(invalid());
    }
    Ok((page, pages))
}

pub(crate) fn channel_page_response(
    base: &Url,
    page: usize,
    pages: usize,
    count: usize,
    results: Vec<Value>,
    unassigned: bool,
) -> Value {
    let next = (page < pages).then(|| page_url(base, page + 1));
    let previous = (page > 1).then(|| {
        let value = page_url(base, page - 1);
        if page != 2 {
            return value;
        }
        let mut parsed = Url::parse(&value).expect("generated URL");
        let pairs = parsed
            .query_pairs()
            .filter(|(key, _)| key != "page")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        parsed.query_pairs_mut().clear().extend_pairs(pairs);
        if parsed.query() == Some("") {
            parsed.set_query(None);
        }
        parsed.to_string()
    });
    json!({"count":count,"next":next,"previous":previous,"results":results,"has_unassigned_epg_channels":unassigned})
}

pub(crate) async fn read(
    auth: AuthService,
    user: User,
    params: HashMap<String, String>,
    base: Url,
    detail: Option<i64>,
) -> Result<Json<Value>, StoreError> {
    let paginated = detail.is_none()
        && ["page", "page_size"]
            .iter()
            .any(|key| params.get(*key).is_some_and(|value| !value.is_empty()));
    let size = channel_page_size(&params);
    let count = if paginated {
        let (_, from) = SELECT
            .split_once(" FROM dispatcharr_channels_channel c LEFT JOIN")
            .expect("fixed channel query");
        let mut count = QueryBuilder::new(format!(
            "SELECT count(*) FROM dispatcharr_channels_channel c LEFT JOIN{from}"
        ));
        filters(&mut count, &params, &user, &auth, false).await?;
        count
            .build_query_scalar::<i64>()
            .fetch_one(auth.store.pool())
            .await? as usize
    } else {
        0
    };
    let (page, pages) = channel_page(&params, paginated, count, size)?;
    let mut query = QueryBuilder::new(SELECT);
    filters(&mut query, &params, &user, &auth, detail.is_some()).await?;
    if let Some(id) = detail {
        query.push(" AND c.id=").push_bind(id);
    }
    order(&mut query, &params);
    if paginated {
        query
            .push(" LIMIT ")
            .push_bind(size as i64)
            .push(" OFFSET ")
            .push_bind(((page - 1) * size) as i64);
    }
    let rows = query.build().fetch_all(auth.store.pool()).await?;
    let include = params
        .get("include_streams")
        .is_some_and(|value| value == "true");
    let values = rows
        .into_iter()
        .map(|row| serialize(row, include, user.user_level >= 10, &base))
        .collect::<Result<Vec<_>, _>>()?;
    if detail.is_some() {
        return Ok(Json(values.into_iter().next().ok_or_else(missing)?));
    }
    if !paginated {
        return Ok(Json(Value::Array(values)));
    }
    let unassigned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.epg_data_id IS NULL AND o.epg_data_id IS NULL)").fetch_one(auth.store.pool()).await?;
    Ok(Json(channel_page_response(
        &base, page, pages, count, values, unassigned,
    )))
}
async fn list(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    read(auth, user, params, base_url(&headers, &uri)?, None).await
}

async fn lightweight(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    params: HashMap<String, String>,
    summary: bool,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    // Reuse the channel filters and effective ordering, without fetching
    // channel/stream graphs or applying pagination to these flat actions.
    let (_, from) = SELECT
        .split_once(" FROM dispatcharr_channels_channel c LEFT JOIN")
        .expect("fixed channel query");
    let fields = if summary {
        "jsonb_build_object('id',c.id,'uuid',c.uuid,'name',COALESCE(o.name,c.name),'logo_id',COALESCE(o.logo_id,c.logo_id),'channel_number',COALESCE(o.channel_number,c.channel_number),'epg_data_id',COALESCE(o.epg_data_id,c.epg_data_id),'channel_group_id',COALESCE(o.channel_group_id,c.channel_group_id))"
    } else {
        "to_jsonb(c.id)"
    };
    let mut query = QueryBuilder::new(format!(
        "SELECT {fields} FROM dispatcharr_channels_channel c LEFT JOIN{from}"
    ));
    filters(&mut query, &params, &user, &auth, false).await?;
    order(&mut query, &params);
    let mut values = query
        .build_query_scalar::<Value>()
        .fetch_all(auth.store.pool())
        .await?;
    if summary {
        for value in &mut values {
            value["channel_number"] = float(value["channel_number"].clone());
        }
    }
    Ok(Json(Value::Array(values)))
}
// Source ChannelViewSet.numbers_in_range: the group editor uses this bounded
// advisory scan. Reuse only the shared ACL, never management-list filters.
fn range_boundary(raw: &str, field: &str) -> Result<f64, StoreError> {
    if raw.len() > 128 {
        return Err(StoreError::Unsupported(
            "Channel range numeric input capacity",
        ));
    }
    // Source uses float() on the query string. The shared parser supports its
    // decimal digit, underscore and whitespace grammar.
    let parsed = dispatcharr_ingest_core::source_float_literal(raw).ok_or_else(|| {
        StoreError::InvalidInput(json!({"detail":format!("Invalid {field} value")}))
    })?;
    if !parsed.is_finite() {
        return Err(StoreError::Unsupported(
            "Channel range nonfinite boundary unqualified",
        ));
    }
    Ok(parsed)
}
// Same pinned DRF JSONRenderer separator replacements as the bounded SD
// search response. Float spelling reuses the existing Source-differential
// finite formatter; no arbitrary JSON number grammar is introduced here.
struct RangeJsonFormatter;
impl serde_json::ser::Formatter for RangeJsonFormatter {
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
            .ok_or_else(|| std::io::Error::other("finite channel range number required"))?;
        writer.write_all(raw.as_bytes())
    }
    fn write_number_str<W>(&mut self, writer: &mut W, value: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        // serde_json's enabled arbitrary_precision representation delivers
        // Number::from_f64 through this callback. IDs remain literal integers;
        // the sole fractional/exponent field is the finite effective number.
        if value.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
            let value = value
                .parse::<f64>()
                .map_err(|_| std::io::Error::other("channel range number shape"))?;
            self.write_f64(writer, value)
        } else {
            writer.write_all(value.as_bytes())
        }
    }
}
fn range_reply(value: &Value) -> Result<Response, StoreError> {
    const OUTPUT: usize = 16 * 1024 * 1024;
    let capacity = || StoreError::Unsupported("Channel range rendered response capacity");
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|n| *n <= OUTPUT)
                .ok_or_else(|| std::io::Error::other("bounded channel range JSON"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde::Serialize::serialize(
        value,
        &mut serde_json::Serializer::with_formatter(&mut count, RangeJsonFormatter),
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
        &mut serde_json::Serializer::with_formatter(&mut bytes, RangeJsonFormatter),
    )
    .map_err(|_| capacity())?;
    if bytes.len() != count.0 || bytes.capacity() != allocated {
        return Err(capacity());
    }
    Ok(([("content-type", "application/json")], bytes).into_response())
}

// Source10 observes these action headers on successful and rejected GETs.
// Apply after IntoResponse so authentication and input errors are included;
// the existing outer CORS middleware merges origin into this single Vary field.
async fn range_response_headers(mut response: Response) -> Response {
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

async fn numbers_in_range(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let raw = uri.query().unwrap_or_default();
    if raw.len() > 16384 {
        return Err(StoreError::Unsupported("Channel range query capacity"));
    }
    // QueryDict.get selects the final repeated value. Unknown list-filter and
    // pagination parameters are ignored by the Source action.
    let pairs = url::form_urlencoded::parse(raw.as_bytes()).collect::<Vec<_>>();
    let last = |key: &str| {
        pairs
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_ref())
    };
    let Some(raw_start) = last("start").filter(|s| !s.is_empty()) else {
        return range_reply(&json!({"occupants":[]}));
    };
    let mut start = range_boundary(raw_start, "start")?;
    let mut end = match last("end").filter(|s| !s.is_empty()) {
        Some(value) => range_boundary(value, "end")?,
        None => start,
    };
    if end < start {
        std::mem::swap(&mut start, &mut end);
    }
    // ChannelGroup.name is a TextField. Bound it in SQL before transfer, without
    // dropping an oversized occupant or changing the Source's top-50 selection.
    // Channel and override names are schema-bounded to 512 characters.
    let mut query = QueryBuilder::new("SELECT c.id::bigint AS id,COALESCE(o.name,c.name) AS name,COALESCE(o.channel_number,c.channel_number) AS channel_number,COALESCE(o.channel_group_id,c.channel_group_id)::bigint AS channel_group_id,CASE WHEN octet_length(g.name)<=32768 THEN g.name ELSE NULL END AS channel_group,g.name IS NULL OR octet_length(g.name)<=32768 AS group_capacity,c.auto_created,NULLIF(c.auto_created_by_id,0)::bigint AS auto_created_by_account_id,(o.id IS NOT NULL AND o.channel_number IS NOT NULL) AS has_channel_number_override FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=COALESCE(o.channel_group_id,c.channel_group_id) WHERE true");
    scope(&mut query, &user);
    query
        .push(" AND COALESCE(o.channel_number,c.channel_number)>=")
        .push_bind(start);
    query
        .push(" AND COALESCE(o.channel_number,c.channel_number)<=")
        .push_bind(end);
    // No invented ID tiebreaker: Source orders solely by effective number.
    query.push(" ORDER BY COALESCE(o.channel_number,c.channel_number) LIMIT 50");
    let rows = query.build().fetch_all(auth.store.pool()).await?;
    let mut occupants = Vec::with_capacity(rows.len());
    for row in rows {
        if !row.try_get::<bool, _>("group_capacity")? {
            return Err(StoreError::Unsupported("Channel range group name capacity"));
        }
        let number: f64 = row.try_get("channel_number")?;
        // Finite bounds exclude infinities; PostgreSQL NaN cannot satisfy both.
        let number = serde_json::Number::from_f64(number)
            .ok_or(StoreError::Unsupported("Channel range output nonfinite"))?;
        occupants.push(json!({
            "id":row.try_get::<i64,_>("id")?,
            "name":row.try_get::<String,_>("name")?,
            "channel_number":number,
            "channel_group":row.try_get::<Option<String>,_>("channel_group")?,
            "channel_group_id":row.try_get::<Option<i64>,_>("channel_group_id")?,
            "auto_created":row.try_get::<bool,_>("auto_created")?,
            "auto_created_by_account_id":row.try_get::<Option<i64>,_>("auto_created_by_account_id")?,
            "has_channel_number_override":row.try_get::<bool,_>("has_channel_number_override")?
        }));
    }
    range_reply(&json!({"occupants":occupants}))
}

async fn ids(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    lightweight(auth, peer, headers, params, false).await
}
async fn summary(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    lightweight(auth, peer, headers, params, true).await
}
async fn retrieve(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    method: Method,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let id = id
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
    let body = read(auth, user, params, base_url(&headers, &uri)?, Some(id)).await?;
    let mut response = body.into_response();
    if method == Method::GET {
        response.headers_mut().insert(header::ALLOW, DETAIL_ALLOW);
    }
    Ok(response)
}
pub(crate) fn channel_options_indentation(
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<usize, StoreError> {
    let unsupported =
        || StoreError::Unsupported("Channel OPTIONS requires the ordinary JSON route");
    if !matches!(uri.query(), None | Some("format=json")) {
        return Err(unsupported());
    }
    let mut accepts = headers.get_all(header::ACCEPT).iter();
    let Some(accept) = accepts.next() else {
        return Ok(0);
    };
    if accepts.next().is_some() {
        return Err(unsupported());
    }
    let accept = accept.to_str().map_err(|_| unsupported())?;
    if accept.eq_ignore_ascii_case("application/json") || accept == "*/*" {
        return Ok(0);
    }
    let (media_type, parameter) = accept.split_once(';').ok_or_else(unsupported)?;
    let (name, value) = parameter.trim().split_once('=').ok_or_else(unsupported)?;
    if !media_type.trim().eq_ignore_ascii_case("application/json")
        || !name.trim().eq_ignore_ascii_case("indent")
        || value.contains(';')
    {
        return Err(unsupported());
    }
    Ok(value.trim().parse::<i32>().unwrap_or(0).clamp(0, 8) as usize)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ChannelOptions {
    ExistingDetail,
    MissingDetail,
    Collection,
}

fn channel_options_entity(
    scope: ChannelOptions,
    indentation: usize,
) -> Result<Response, StoreError> {
    static RENDERED: [[OnceLock<Option<Bytes>>; 9]; 3] =
        [const { [const { OnceLock::new() }; 9] }; 3];
    let body = if scope == ChannelOptions::ExistingDetail && indentation == 0 {
        Bytes::from_static(DETAIL_OPTIONS)
    } else {
        // All three shapes share the pinned static serializer and its field order.
        RENDERED[scope as usize][indentation]
            .get_or_init(|| {
                let mut metadata: Map<String, Value> =
                    serde_json::from_slice(DETAIL_OPTIONS).ok()?;
                match scope {
                    ChannelOptions::ExistingDetail => {}
                    ChannelOptions::MissingDetail => {
                        metadata.shift_remove("actions");
                    }
                    ChannelOptions::Collection => {
                        *metadata.get_mut("name")? = Value::String("Channel List".into());
                        let actions = metadata.get_mut("actions")?.as_object_mut()?;
                        let fields = actions.shift_remove("PUT")?;
                        actions.insert("POST".into(), fields);
                    }
                }
                serialize_options_json(&metadata, indentation)
            })
            .clone()
            .ok_or(StoreError::UnhandledRestFault)?
    };
    options_json_response(body)
}

pub(crate) fn serialize_options_json(
    metadata: &Map<String, Value>,
    indentation: usize,
) -> Option<Bytes> {
    if indentation == 0 {
        return serde_json::to_vec(metadata).ok().map(Bytes::from);
    }
    let formatter = serde_json::ser::PrettyFormatter::with_indent(&b"        "[..indentation]);
    let mut output = Vec::new();
    let mut serializer = serde_json::Serializer::with_formatter(&mut output, formatter);
    serde::Serialize::serialize(metadata, &mut serializer).ok()?;
    Some(Bytes::from(output))
}

pub(crate) fn options_json_response(body: Bytes) -> Result<Response, StoreError> {
    let length = HeaderValue::from_str(&body.len().to_string())
        .map_err(|_| StoreError::UnhandledRestFault)?;
    Ok((
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (header::CONTENT_LENGTH, length),
        ],
        body,
    )
        .into_response())
}

pub(crate) fn channel_options_headers(mut response: Response, allow: HeaderValue) -> Response {
    if response.status() != StatusCode::INTERNAL_SERVER_ERROR {
        response.headers_mut().insert(header::ALLOW, allow);
        // Main's normal CORS middleware adds the origin variation.
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept"));
    }
    response
}

async fn detail_options(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let result = async {
        // DRF's metadata action falls through to the UI administrator gate.
        auth.account_user(peer, headers.clone(), true).await?;
        let indentation = channel_options_indentation(&uri, &headers)?;
        let id: i64 = raw
            .parse()
            .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel WHERE id = $1)",
        )
        .bind(id)
        .fetch_one(auth.store.pool())
        .await?;
        let scope = if exists {
            ChannelOptions::ExistingDetail
        } else {
            ChannelOptions::MissingDetail
        };
        channel_options_entity(scope, indentation)
    }
    .await;
    channel_options_headers(
        result.unwrap_or_else(IntoResponse::into_response),
        DETAIL_ALLOW,
    )
}

async fn collection_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let result = async {
        auth.account_user(peer, headers.clone(), true).await?;
        let indentation = channel_options_indentation(&uri, &headers)?;
        channel_options_entity(ChannelOptions::Collection, indentation)
    }
    .await;
    channel_options_headers(
        result.unwrap_or_else(IntoResponse::into_response),
        COLLECTION_ALLOW,
    )
}

// Unlike the management list, this action starts from Channel.objects.filter
// and applies only the shared user ACL. List filtering, ordering and pagination
// must not hide a requested channel or change the action's response shape.
async fn by_uuids(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let value = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&body).map_err(|error| {
            StoreError::InvalidInput(json!({"detail":format!("JSON parse error - {}", crate::source_json_error(&body, error))}))
        })?
    };
    let Some(object) = value.as_object() else {
        return Ok(source_action_error());
    };
    let empty = Value::Array(Vec::new());
    let Some(values) = object.get("uuids").unwrap_or(&empty).as_array() else {
        return Ok((
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({"error":"uuids must be a list of strings"})),
        )
            .into_response());
    };
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for value in values {
        if value.is_null() {
            continue;
        }
        let parsed = match value {
            Value::String(value) => {
                let value = value.replace("urn:", "").replace("uuid:", "");
                uuid::Uuid::parse_str(&value.trim_matches(['{', '}']).replace('-', "")).ok()
            }
            Value::Bool(value) => Some(uuid::Uuid::from_u128(u128::from(*value))),
            Value::Number(value) => value
                .to_string()
                .parse::<u128>()
                .ok()
                .map(uuid::Uuid::from_u128),
            _ => None,
        };
        let Some(id) = parsed else {
            return Ok(source_action_error());
        };
        if seen.insert(id) {
            ids.push(id);
        }
    }
    // Freeze the un-ordered Source queryset before serializer-related joins.
    // Those joins may reorder rows even though the action never applies the
    // management list's ordering filter. Carry the selected ordinal through
    // projection without sorting requested UUIDs or normalizing response order.
    let mut query = QueryBuilder::new("WITH selected AS MATERIALIZED (SELECT c.*, row_number() OVER () AS uuid_action_position FROM dispatcharr_channels_channel c WHERE true");
    scope(&mut query, &user);
    query.push(" AND c.uuid=ANY(").push_bind(ids).push(") ) ");
    query.push(SELECT.replace("FROM dispatcharr_channels_channel c", "FROM selected c"));
    query.push(" ORDER BY c.uuid_action_position");
    let rows = query.build().fetch_all(auth.store.pool()).await?;
    let base = base_url(&headers, &uri)?;
    let include = params
        .get("include_streams")
        .is_some_and(|value| value == "true");
    let values = rows
        .into_iter()
        .map(|row| serialize(row, include, user.user_level >= 10, &base))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(Value::Array(values)).into_response())
}
fn source_action_error() -> Response {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        [("content-type", "text/html; charset=utf-8")],
        "\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Server Error (500)</title>\n</head>\n<body>\n  <h1>Server Error (500)</h1><p></p>\n</body>\n</html>\n",
    ).into_response()
}

#[path = "channel_batch_epg.rs"]
mod batch_epg;
pub(crate) use batch_epg::{batch_body as channel_mutation_json, mutation_value};

pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/channels/channels/by-uuids/", post(by_uuids))
        .route(
            "/api/channels/channels/numbers-in-range/",
            // Axum derives HEAD from GET and drops the entity after computing
            // its length, as DRF's GET action does.
            get(numbers_in_range).layer(axum::middleware::map_response(range_response_headers)),
        )
        .route("/api/channels/channels/ids/", get(ids))
        .route(
            "/api/channels/channels/from-stream/",
            post(crate::channel_writes::create_from_stream),
        )
        .route(
            "/api/channels/channels/edit/bulk/",
            axum::routing::patch(crate::channel_writes::edit_bulk),
        )
        .route(
            "/api/channels/channels/assign/",
            post(crate::channel_writes::assign_numbers),
        )
        .route("/api/channels/channels/summary/", get(summary))
        .route(
            "/api/channels/channels/batch-set-epg/",
            post(batch_epg::set),
        )
        .route(
            "/api/channels/channels/{id}/set-epg/",
            post(crate::channel_writes::set_epg),
        )
        .route(
            "/api/channels/channels/{id}/reorder/",
            post(crate::channel_writes::reorder),
        )
        .route(
            "/api/channels/channels/{id}/streams/",
            get(crate::channel_relations::streams),
        )
        .route(
            "/api/channels/channels/{id}/streams/stats/",
            get(crate::channel_relations::stats),
        )
        .route(
            "/api/channels/channels/",
            get(list)
                .post(crate::channel_writes::create)
                .options(collection_options),
        )
        .route(
            "/api/channels/channels/{id}/",
            get(retrieve)
                .patch(crate::channel_writes::patch)
                .put(crate::channel_writes::put)
                .delete(crate::channel_writes::delete)
                .options(detail_options),
        )
        .with_state(auth)
}

/// Channel identity after the original level/adult/profile visibility predicate.
#[derive(Clone, Debug)]
pub struct ChannelIdentity {
    pub id: i64,
    pub uuid: uuid::Uuid,
    pub name: String,
    pub effective_name: String,
}
impl Store {
    /// Equivalent to get_channel_for_user(user, uuid=...), without management
    /// list visibility filters. Do not reuse for an already-bound time-shift
    /// session when the original session endpoint intentionally skips this ACL.
    pub async fn channel_for_user(
        &self,
        user: &User,
        uuid: uuid::Uuid,
    ) -> Result<Option<ChannelIdentity>, StoreError> {
        if !user.is_active {
            return Ok(None);
        }
        let mut query=QueryBuilder::new("SELECT c.id::bigint AS id,c.uuid,c.name,COALESCE(o.name,c.name) AS effective_name FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.uuid=");
        query.push_bind(uuid);
        scope(&mut query, user);
        query
            .build()
            .fetch_optional(self.pool())
            .await?
            .map(|row| {
                Ok(ChannelIdentity {
                    id: row.try_get("id")?,
                    uuid: row.try_get("uuid")?,
                    name: row.try_get("name")?,
                    effective_name: row.try_get("effective_name")?,
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_json_uses_the_stock_serializer_order_for_normal_rows() {
        let stock: Value = serde_json::from_slice(DETAIL_OPTIONS).unwrap();
        let stock_fields: Vec<_> = stock["actions"]["PUT"]
            .as_object()
            .unwrap()
            .keys()
            .collect();
        let base = Url::parse("http://localhost:9192").unwrap();
        let manual = serialize_channel(
            &json!({"id": 60, "name": "Owned manual", "channel_number": 214,
                    "channel_group_id": 10, "auto_created": false}),
            None,
            &[],
            None,
            false,
            true,
            &base,
        );
        assert_eq!(manual["streams"], json!([]));
        assert_eq!(manual["auto_created_by_name"], Value::Null);
        assert_eq!(manual["override"], Value::Null);
        assert_eq!(manual["source_stream"], Value::Null);
        assert_eq!(manual["channel_group_id"], json!(10));
        assert_eq!(manual["effective_channel_group_id"], json!(10));
        assert_eq!(manual["effective_channel_number"], json!(214.0));

        let raw = json!({"id": 61, "name": "Owned managed", "channel_number": 215,
                         "channel_group_id": 11, "auto_created": true, "auto_created_by_id": 5});
        let streams = [
            json!({"raw": {"id": 41, "name": "Owned stream", "m3u_account_id": 5,
                                      "url": "http://origin/stream.ts"},
                              "account_name": "Owned provider"}),
        ];
        let managed = serialize_channel(
            &raw,
            Some(json!({"channel_number": 19.5, "is_radio": false})),
            &streams,
            Some("Owned provider".into()),
            false,
            true,
            &base,
        );
        assert_eq!(managed["streams"], json!([41]));
        assert_eq!(managed["auto_created_by"], json!(5));
        assert_eq!(managed["auto_created_by_name"], json!("Owned provider"));
        assert_eq!(
            managed["source_stream"],
            json!({"id": 41, "name": "Owned stream",
            "account_id": 5, "account_name": "Owned provider"})
        );
        assert_eq!(managed["channel_number"], json!(215.0));
        assert_eq!(managed["effective_channel_number"], json!(19.5));
        assert_eq!(managed["effective_channel_group_id"], json!(11));
        assert_eq!(managed["effective_is_radio"], json!(false));

        let expanded = serialize_channel(&raw, None, &streams, None, true, false, &base);
        assert_eq!(expanded["streams"][0]["id"], json!(41));
        assert!(expanded["streams"][0].get("url").is_none());
        for channel in [manual, managed, expanded] {
            let wire = serde_json::to_vec(&channel).unwrap();
            let body: Value = serde_json::from_slice(&wire).unwrap();
            let fields: Vec<_> = body.as_object().unwrap().keys().collect();
            assert_eq!(fields, stock_fields);
        }
    }

    #[tokio::test]
    async fn channel_detail_options_serves_the_exact_static_source_metadata() {
        use axum::body::to_bytes;
        use sha2::{Digest, Sha256};

        let response = channel_options_headers(
            channel_options_entity(ChannelOptions::ExistingDetail, 0).unwrap(),
            DETAIL_ALLOW,
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::ALLOW], DETAIL_ALLOW);
        assert_eq!(response.headers()[header::VARY], "Accept");
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "5290");
        let body = to_bytes(response.into_body(), DETAIL_OPTIONS.len())
            .await
            .unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(&body)),
            "3f87647156b6ae36e31e99d959c66a6b82a4c81036a165aae10b38a4a4180627"
        );
        let value: Value = serde_json::from_slice(&body).unwrap();
        let put = value["actions"]["PUT"].as_object().unwrap();
        assert_eq!(put.len(), 31);
        assert_eq!(put["override"]["children"].as_object().unwrap().len(), 9);
        assert_eq!(put["name"]["max_length"], 512);
        assert_eq!(
            put["override"]["children"]["channel_number"]["min_value"],
            0.0001
        );
    }

    #[tokio::test]
    async fn channel_detail_options_matches_source_json_negotiation_and_absent_row() {
        use axum::body::to_bytes;
        use sha2::{Digest, Sha256};

        for (uri, accept, exists, expected_length, expected_sha) in [
            (
                "/api/channels/channels/1/?format=json",
                "application/json",
                true,
                5290,
                "3f87647156b6ae36e31e99d959c66a6b82a4c81036a165aae10b38a4a4180627",
            ),
            (
                "/api/channels/channels/1/",
                "application/json; indent=2",
                true,
                8011,
                "5fde952c1efaa96a09a82ae94b1278f43906c463b55d2f03bbece37e8abb471c",
            ),
            (
                "/api/channels/channels/2/",
                "application/json",
                false,
                175,
                "7dca1b678d265447c1abe5246397cf1aa77e0d7daca67b8d225ca76aed900a8b",
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT, HeaderValue::from_static(accept));
            let indentation = channel_options_indentation(&uri.parse().unwrap(), &headers).unwrap();
            let scope = if exists {
                ChannelOptions::ExistingDetail
            } else {
                ChannelOptions::MissingDetail
            };
            let response = channel_options_headers(
                channel_options_entity(scope, indentation).unwrap(),
                DETAIL_ALLOW,
            );
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::ALLOW], DETAIL_ALLOW);
            assert_eq!(response.headers()[header::VARY], "Accept");
            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                expected_length.to_string()
            );
            let body = to_bytes(response.into_body(), expected_length)
                .await
                .unwrap();
            assert_eq!(body.len(), expected_length);
            assert_eq!(format!("{:x}", Sha256::digest(&body)), expected_sha);
            let metadata: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(metadata.get("actions").is_some(), exists);
        }
    }

    #[tokio::test]
    async fn channel_collection_options_matches_source_post_metadata() {
        use axum::body::to_bytes;
        use sha2::{Digest, Sha256};

        for (uri, accept, length, digest) in [
            (
                "/api/channels/channels/",
                "application/json",
                5287,
                "6a71b76fdbf2bae83d42f87c272e070845335d28e6da3719dfff03a38ed9d217",
            ),
            (
                "/api/channels/channels/?format=json",
                "application/json",
                5287,
                "6a71b76fdbf2bae83d42f87c272e070845335d28e6da3719dfff03a38ed9d217",
            ),
            (
                "/api/channels/channels/",
                "application/json; indent=2",
                8008,
                "be64590f42d8ea8776b3c1725e1e46c5a341c2592e85374c4b4d354c2760410c",
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT, HeaderValue::from_static(accept));
            let indentation = channel_options_indentation(&uri.parse().unwrap(), &headers).unwrap();
            let response = channel_options_headers(
                channel_options_entity(ChannelOptions::Collection, indentation).unwrap(),
                COLLECTION_ALLOW,
            );
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::ALLOW], COLLECTION_ALLOW);
            assert_eq!(response.headers()[header::VARY], "Accept");
            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                length.to_string()
            );
            let body = to_bytes(response.into_body(), length).await.unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(&body)), digest);
            let metadata: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(metadata["name"], "Channel List");
            assert_eq!(metadata["actions"]["POST"].as_object().unwrap().len(), 31);
        }
    }

    #[tokio::test]
    async fn channel_options_keeps_the_existing_bearer_gate() {
        use axum::{body::Body, http::Request};
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://unused:unused@localhost/unused")
            .unwrap();
        let auth = AuthService::new(
            Store::from_pool(pool),
            crate::AuthConfig::new("test-signing-secret-only".into()).unwrap(),
        );
        for (path, allow) in [
            ("/api/channels/channels/1/", DETAIL_ALLOW),
            ("/api/channels/channels/", COLLECTION_ALLOW),
        ] {
            let request = Request::builder()
                .method(Method::OPTIONS)
                .uri(path)
                .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9192))))
                .body(Body::empty())
                .unwrap();
            let response = router(auth.clone()).oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(response.headers()[header::ALLOW], allow);
            assert_eq!(response.headers()[header::VARY], "Accept");
            assert_eq!(
                response.headers()[header::WWW_AUTHENTICATE],
                "Bearer realm=\"api\""
            );
        }
    }

    #[test]
    fn channel_range_accepts_source_finite_float_spellings() {
        for (raw, expected) in [
            ("١_٢.٥", 12.5),
            ("\u{2003}１２.５\u{2003}", 12.5),
            ("+1.2_5e+1", 12.5),
            ("1e-999", 0.0),
        ] {
            assert_eq!(range_boundary(raw, "start").unwrap(), expected);
        }
        for raw in ["1__2", "１_．５", "1_.5"] {
            assert!(matches!(
                range_boundary(raw, "end"),
                Err(StoreError::InvalidInput(value)) if value == json!({"detail":"Invalid end value"})
            ));
        }
        for raw in ["+∞", "Infinity", "1e999"] {
            let actual = range_boundary(raw, "end");
            if raw == "+∞" {
                assert!(matches!(actual, Err(StoreError::InvalidInput(_))));
            } else {
                assert!(matches!(actual, Err(StoreError::Unsupported(_))));
            }
        }
    }

    #[test]
    fn search_quoting_and_commas_match_pinned_drf_vectors() {
        for (input, expected) in [
            ("a \"b c\" d", vec!["a", "b c", "d"]),
            ("a\"b c\"d", vec!["a\"b c\"d"]),
            ("\"unterminated phrase", vec!["\"unterminated", "phrase"]),
            ("\"one,two\",three", vec!["\"one", "two\"", "three"]),
            ("a,,b", vec!["a", "b"]),
            ("''", vec![""]),
            ("\"a\\\"b\"", vec!["a\"b"]),
        ] {
            assert_eq!(search_terms(input), expected);
        }
    }
}
