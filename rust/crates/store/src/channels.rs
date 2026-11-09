//! Channel API read contracts from original Dispatcharr serializers and access rules.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{AuthService, Store, StoreError, User};
use axum::{
    extract::{ConnectInfo, OriginalUri, Path, Query, State},
    http::{HeaderMap, Uri},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use sqlx::{Postgres, QueryBuilder, Row};
use std::{collections::HashMap, net::SocketAddr};
use url::Url;
const FIELDS: &[&str] = &[
    "id",
    "channel_number",
    "name",
    "channel_group_id",
    "tvg_id",
    "tvc_guide_stationid",
    "epg_data_id",
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
const OVERRIDE_FIELDS: &[&str] = &[
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
const STREAM_FIELDS: &[&str] = &[
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
fn missing() -> StoreError {
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
    let mut value = Map::new();
    for field in FIELDS {
        let item = if *field == "channel_number" {
            float(raw[*field].clone())
        } else {
            raw[*field].clone()
        };
        value.insert(
            if *field == "auto_created_by_id" {
                "auto_created_by"
            } else {
                field
            }
            .to_string(),
            item,
        );
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
    for field in OVERRIDE_FIELDS {
        let overridden = &override_value[*field];
        let raw = if *field == "channel_number" {
            float(raw[*field].clone())
        } else {
            raw[*field].clone()
        };
        value.insert(
            format!("effective_{field}"),
            if overridden.is_null() {
                raw
            } else {
                overridden.clone()
            },
        );
    }
    value.insert("override".into(), override_value);
    value.insert(
        "auto_created_by_name".into(),
        row.try_get::<Option<String>, _>("account_name")?
            .map_or(Value::Null, Value::from),
    );
    value.insert(
        "streams".into(),
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
    );
    value.insert("source_stream".into(),if raw["auto_created"]==true{streams.first().map(|value|json!({"id":value["raw"]["id"],"name":value["raw"]["name"],"account_id":value["raw"]["m3u_account_id"],"account_name":value["account_name"]})).unwrap_or(Value::Null)}else{Value::Null});
    Ok(Value::Object(value))
}
fn scope<'a>(query: &mut QueryBuilder<'a, Postgres>, user: &User) {
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
fn order(query: &mut QueryBuilder<'_, Postgres>, params: &HashMap<String, String>) {
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
    query.push(" ORDER BY ").push(if clauses.is_empty() {
        "COALESCE(o.channel_number,c.channel_number) DESC".into()
    } else {
        clauses.join(",")
    });
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
    let size = params
        .get("page_size")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(50)
        .min(10000);
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
            .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Invalid page."})))?
            .unwrap_or(1)
    };
    if paginated && (page == 0 || page > pages) {
        return Err(StoreError::StatusBody(
            404,
            json!({"detail":"Invalid page."}),
        ));
    }
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
    Ok(Json(
        json!({"count":count,"next":if page<pages{Some(page_url(&base,page+1))}else{None},"previous":if page>1{let mut url=page_url(&base,page-1);if page==2{let mut parsed=Url::parse(&url).expect("generated URL");let pairs=parsed.query_pairs().filter(|(key,_)|key!="page").map(|(key,value)|(key.into_owned(),value.into_owned())).collect::<Vec<_>>();parsed.query_pairs_mut().clear().extend_pairs(pairs);if parsed.query()==Some(""){parsed.set_query(None);}url=parsed.to_string();}Some(url)}else{None},"results":values,"has_unassigned_epg_channels":unassigned}),
    ))
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
async fn retrieve(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let id = id
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
    read(auth, user, params, base_url(&headers, &uri)?, Some(id)).await
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/channels/channels/",
            get(list).post(crate::channel_writes::create),
        )
        .route(
            "/api/channels/channels/{id}/",
            get(retrieve)
                .patch(crate::channel_writes::patch)
                .put(crate::channel_writes::put)
                .delete(crate::channel_writes::delete),
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
