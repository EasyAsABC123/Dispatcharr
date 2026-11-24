//! Series rule preview derived from Original Dispatcharr's channels API and
//! EPG query helpers. Original Dispatcharr authors and contributors retain credit.
use super::{
    json_negotiation, projected_fields, unavailable, BoundedJson, EpgProgramReads, OwnedJson,
};
use crate::{channels::truthy, recording_http, StoreError};
use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{Acquire, PgConnection, Postgres, QueryBuilder, Row};
use std::{
    net::SocketAddr,
    sync::{Arc, LazyLock},
};
use tokio::sync::oneshot;

#[path = "series_evaluate.rs"]
pub mod evaluate;

pub(super) const PATH: &str = "/api/channels/series-rules/preview/";

pub(super) async fn handle(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    preview_response(respond(state, peer, request).await)
}

fn preview_response(result: Result<Response, StoreError>) -> Response {
    let mut response = result.unwrap_or_else(IntoResponse::into_response);
    if response.status() == StatusCode::INTERNAL_SERVER_ERROR {
        return response;
    }
    let headers = response.headers_mut();
    headers.insert(header::ALLOW, HeaderValue::from_static("POST, OPTIONS"));
    headers.insert(header::VARY, HeaderValue::from_static("Accept"));
    response
}

async fn respond(
    state: EpgProgramReads,
    peer: SocketAddr,
    request: Request,
) -> Result<Response, StoreError> {
    json_negotiation(request.headers(), request.uri())?;
    // The route permits DVR managers below admin level. Apply the UI
    // network gate before evaluating the shared DVR manage permission.
    let user = state
        .auth
        .account_user(peer, request.headers().clone(), false)
        .await?;
    recording_http::access(&user, true)?;
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();
    let raw = to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .map_err(|_| {
            StoreError::StatusBody(413, json!({"detail":"Request body exceeded capacity."}))
        })?;
    let body = if raw.is_empty() {
        Value::Null
    } else if content_type.eq_ignore_ascii_case("application/json") {
        serde_json::from_slice(&raw).map_err(|error| StoreError::InvalidInput(
            json!({"detail": format!("JSON parse error - {}", crate::source_json_error(&raw, error))}),
        ))?
    } else {
        return Err(StoreError::StatusBody(
            415,
            json!({"detail": format!("Unsupported media type \"{content_type}\" in request.")}),
        ));
    };
    let preview = Preview::parse(&body)?;
    let now = state.current_now();
    let deadline = state.limits.deadline;
    let lease = Arc::new(
        state
            .workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?,
    );
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let result = state.series_preview(preview, now).await.map(|data| {
            Bytes::from_owner(OwnedJson {
                data,
                _lease: lease,
            })
        });
        let _ = sender.send(result);
    });
    let data = tokio::time::timeout(deadline, receiver)
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())??;
    Ok(Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(data))
        .expect("static preview headers"))
}

struct Preview {
    tvg_id: String,
    source_id: Option<i64>,
    title: Option<Expression>,
    description: Option<Expression>,
    limit: i64,
    new_only: bool,
    untagged: bool,
}

impl Preview {
    fn parse(body: &Value) -> Result<Self, StoreError> {
        if truthy(Some(body)) && !body.is_object() {
            return Err(StoreError::UnhandledRestFault);
        }
        let text = |key| -> Result<&str, StoreError> {
            match body.get(key).filter(|value| truthy(Some(value))) {
                None => Ok(""),
                Some(value) => value.as_str().ok_or(StoreError::UnhandledRestFault),
            }
        };
        let title = text("title")?.trim();
        let description = text("description")?.trim();
        if title.is_empty() && description.is_empty() {
            return Err(StoreError::InvalidInput(
                json!({"error":"A title or description is required"}),
            ));
        }
        let new_only = text("mode")?.eq_ignore_ascii_case("new");
        let tvg_id = match body.get("tvg_id").filter(|value| truthy(Some(value))) {
            Some(Value::String(value)) => value.trim().to_owned(),
            Some(Value::Number(value)) => value.to_string(),
            Some(Value::Bool(true)) => "True".to_owned(),
            _ => String::new(),
        };
        let limit = body.get("limit").filter(|value| truthy(Some(value)));
        let limit = recording_http::python_int(limit)
            .ok()
            .flatten()
            .unwrap_or(25)
            .clamp(1, 100);
        let source_id = recording_http::python_int(body.get("epg_source_id"))
            .ok()
            .flatten()
            .filter(|id| *id > 0);
        let title_mode = text("title_mode")?;
        Ok(Self {
            tvg_id,
            source_id,
            title: if title.is_empty() {
                None
            } else if title_mode.is_empty() || title_mode.eq_ignore_ascii_case("exact") {
                Some(Expression::Term(Match::Exact, title.to_owned()))
            } else {
                parse_text(title, title_mode)?
            },
            description: parse_text(description, text("description_mode")?)?,
            limit,
            new_only,
            untagged: new_only && truthy(body.get("untagged_is_new")),
        })
    }
}

impl EpgProgramReads {
    async fn series_preview(
        &self,
        preview: Preview,
        now: DateTime<Utc>,
    ) -> Result<Vec<u8>, StoreError> {
        let mut connection = self.auth.store.pool().acquire().await?;
        let mut tx = connection.begin().await?;
        let result = async {
            sqlx::query("SET TRANSACTION READ ONLY")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SELECT set_config('statement_timeout',$1,true)")
                .bind(format!("{}ms", self.limits.deadline.as_millis()))
                .execute(&mut *tx)
                .await?;
            let value = preview.read(&mut tx, now, self.limits.row_bytes).await?;
            let mut out = BoundedJson::new(self.limits.response_bytes);
            serde_json::to_writer(&mut out, &value).map_err(|_| unavailable())?;
            Ok(out.into_data())
        }
        .await;
        tx.rollback().await?;
        result
    }
}

impl Preview {
    async fn read(
        &self,
        connection: &mut PgConnection,
        now: DateTime<Utc>,
        row_bytes: usize,
    ) -> Result<Value, StoreError> {
        let epg_ids = if self.tvg_id.is_empty() {
            None
        } else {
            let (found, mapped) = mapped_epg_ids(connection, &self.tvg_id, self.source_id).await?;
            if mapped.is_empty() {
                let status = if found {
                    "no_channel_for_epg"
                } else {
                    "no_epg_match"
                };
                return Ok(
                    json!({"matches": [], "total": 0, "epg_found": found, "status": status}),
                );
            }
            Some(mapped)
        };

        let mut query = QueryBuilder::<Postgres>::new(format!(
            "SELECT {} FROM epg_programdata p CROSS JOIN (SELECT ",
            projected_fields("preview_bounds.row_bytes")
        ));
        query
            .push_bind(row_bytes as i64)
            .push("::bigint AS row_bytes) preview_bounds WHERE p.end_time > ")
            .push_bind(now)
            .push(" AND p.start_time <= ")
            .push_bind(now + Duration::days(7));
        if let Some(ids) = epg_ids {
            query.push(" AND p.epg_id = ANY(").push_bind(ids).push(")");
        }
        for (field, expression) in [
            ("p.title", &self.title),
            ("p.description", &self.description),
        ] {
            if let Some(expression) = expression {
                query.push(" AND ");
                expression.write_sql(&mut query, field);
            }
        }
        // Each ProgramData row is unique. The EXISTS mapping avoids joins that
        // would require DISTINCT before applying the Original candidate cap.
        query
            .push(" ORDER BY p.start_time LIMIT ")
            .push_bind(self.limit * 4);
        let mut rows = query.build().fetch(&mut *connection);
        let mut matches = Vec::new();
        let mut total = 0;
        while let Some(row) = rows.try_next().await.map_err(programme_query_error)? {
            if row.try_get::<bool, _>("oversized")? {
                return Err(unavailable());
            }
            let props: Option<Value> = row.try_get("custom_properties")?;
            let props = props.as_ref().unwrap_or(&Value::Null);
            let is_new = is_new(props, self.untagged);
            if self.new_only && !is_new {
                continue;
            }
            total += 1;
            if matches.len() < self.limit as usize {
                matches.push(json!({
                    "id": row.try_get::<i64, _>("id")?,
                    "tvg_id": row.try_get::<Option<String>, _>("tvg_id")?,
                    "title": row.try_get::<String, _>("title")?,
                    "sub_title": row.try_get::<Option<String>, _>("sub_title")?,
                    "description": row.try_get::<Option<String>, _>("description")?,
                    "start_time": recording_http::python_iso(row.try_get("start_time")?),
                    "end_time": recording_http::python_iso(row.try_get("end_time")?),
                    "season": props.get("season"),
                    "episode": props.get("episode"),
                    "is_new": is_new,
                }));
            }
        }
        Ok(
            json!({"matches":matches,"total":total,"limit":self.limit,"epg_found":true,"warn":total>50}),
        )
    }
}

async fn mapped_epg_ids(
    connection: &mut PgConnection,
    tvg_id: &str,
    source_id: Option<i64>,
) -> Result<(bool, Vec<i64>), sqlx::Error> {
    sqlx::query_as(
        "SELECT count(*) > 0, coalesce(array_agg(e.id::bigint) FILTER (WHERE
           EXISTS (SELECT 1 FROM dispatcharr_channels_channel c WHERE c.epg_data_id=e.id)
           OR EXISTS (SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.epg_data_id=e.id)
         ), ARRAY[]::bigint[]) FROM epg_epgdata e
         WHERE e.tvg_id=$1 AND ($2::bigint IS NULL OR e.epg_source_id=$2)",
    )
    .bind(tvg_id)
    .bind(source_id)
    .fetch_one(connection)
    .await
}

fn programme_query_error(error: sqlx::Error) -> StoreError {
    match error
        .as_database_error()
        .and_then(|error| error.code())
        .as_deref()
    {
        Some("2201B") => StoreError::UnhandledRestFault,
        Some("57014") => unavailable(),
        _ => error.into(),
    }
}

pub fn is_new(props: &Value, untagged: bool) -> bool {
    truthy(props.get("new")) || (untagged && !truthy(props.get("previously_shown")))
}

#[derive(Clone, Copy)]
enum Match {
    Exact,
    Contains,
    Word,
    Regex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operator {
    And,
    Or,
}

enum Expression {
    Term(Match, String),
    Binary(Box<Self>, Operator, Box<Self>),
}

fn combine(
    left: Option<Expression>,
    op: Operator,
    right: Option<Expression>,
) -> Option<Expression> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Expression::Binary(Box::new(left), op, Box::new(right))),
        (left, right) => left.or(right),
    }
}

impl Expression {
    fn write_sql(&self, sql: &mut QueryBuilder<'_, Postgres>, field: &'static str) {
        match self {
            Self::Binary(left, op, right) => {
                sql.push("(");
                left.write_sql(sql, field);
                sql.push(match op {
                    Operator::And => " AND ",
                    Operator::Or => " OR ",
                });
                right.write_sql(sql, field);
                sql.push(")");
            }
            Self::Term(Match::Exact, term) => {
                sql.push("upper(")
                    .push(field)
                    .push(") = upper(")
                    .push_bind(term.clone())
                    .push(")");
            }
            Self::Term(Match::Contains, term) => {
                sql.push("strpos(upper(")
                    .push(field)
                    .push("), upper(")
                    .push_bind(term.clone())
                    .push(")) > 0");
            }
            Self::Term(kind, term) => {
                let pattern = match kind {
                    Match::Word => format!(r"\y{}\y", regex::escape(term)),
                    _ => term.clone(),
                };
                sql.push(field).push(" ~* ").push_bind(pattern);
            }
        }
    }
}

fn parse_text(value: &str, mode: &str) -> Result<Option<Expression>, StoreError> {
    if value.len() > 4096 || value.contains('\0') {
        return Err(StoreError::InvalidInput(
            json!({"error":"Search expression exceeds supported bounds"}),
        ));
    }
    let kind = match mode.to_ascii_lowercase().as_str() {
        "search" => Match::Word,
        "regex" => Match::Regex,
        _ => Match::Contains,
    };
    // The Original replaces quoted phrases with opaque markers before
    // processing parentheses or operators, including in regex mode.
    let mut phrases = Vec::new();
    static QUOTE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r#""([^"]*)""#).expect("static quoted-phrase expression")
    });
    let processed = QUOTE.replace_all(value, |captures: &regex::Captures<'_>| {
        let marker = format!("\0P{}\0", phrases.len());
        phrases.push(captures[1].to_owned());
        marker
    });
    let mut budget = 128;
    parse_expression(&processed, kind, &phrases, &mut budget)
}

fn parse_expression(
    input: &str,
    kind: Match,
    phrases: &[String],
    budget: &mut usize,
) -> Result<Option<Expression>, StoreError> {
    let consume = |budget: &mut usize| {
        *budget = budget.checked_sub(1).ok_or_else(|| {
            StoreError::InvalidInput(json!({"error":"Search expression exceeds supported bounds"}))
        })?;
        Ok::<_, StoreError>(())
    };
    consume(budget)?;
    let input = input.trim();
    if let Some(start) = input.rfind('(') {
        let rest = &input[start + 1..];
        let Some(end) = rest.find(')') else {
            return Ok(None);
        };
        let mut result = parse_expression(&rest[..end], kind, phrases, budget)?;
        let before = input[..start].trim_end();
        let after = rest[end + 1..].trim_start();
        let (before, before_op) = if before.to_ascii_uppercase().ends_with(" AND") {
            (&before[..before.len() - 4], Operator::And)
        } else if before.to_ascii_uppercase().ends_with(" OR") {
            (&before[..before.len() - 3], Operator::Or)
        } else {
            (before, Operator::And)
        };
        let (after, after_op) = if after.to_ascii_uppercase().starts_with("AND ") {
            (&after[4..], Operator::And)
        } else if after.to_ascii_uppercase().starts_with("OR ") {
            (&after[3..], Operator::Or)
        } else {
            (after, Operator::And)
        };
        if !before.trim().is_empty() {
            result = combine(
                parse_expression(before, kind, phrases, budget)?,
                before_op,
                result,
            );
        }
        if !after.trim().is_empty() {
            result = combine(
                result,
                after_op,
                parse_expression(after, kind, phrases, budget)?,
            );
        }
        return Ok(result);
    }

    let term = |token: &str| {
        let token = token.trim();
        let token = token
            .strip_prefix("\0P")
            .and_then(|s| s.strip_suffix('\0'))
            .and_then(|index| index.parse::<usize>().ok())
            .and_then(|index| phrases.get(index))
            .map_or(token, String::as_str)
            .trim();
        (!token.is_empty()).then(|| Expression::Term(kind, token.to_owned()))
    };
    let mut rest = input;
    let mut result = None;
    let mut previous = Operator::And;
    loop {
        let upper = rest.to_ascii_uppercase();
        let next = match (upper.find(" AND "), upper.find(" OR ")) {
            (Some(and), Some(or)) if and < or => Some((and, 5, Operator::And)),
            (_, Some(or)) => Some((or, 4, Operator::Or)),
            (Some(and), None) => Some((and, 5, Operator::And)),
            (None, None) => None,
        };
        let Some((index, length, op)) = next else {
            return Ok(combine(result, previous, term(rest)));
        };
        if !rest[..index].trim().is_empty() {
            consume(budget)?;
            result = combine(result, previous, term(&rest[..index]));
            previous = op;
        }
        rest = &rest[index + length..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_headers_cover_success_validation_and_permission_responses() {
        for (result, status) in [
            (Ok(Response::new(Body::empty())), 200),
            (
                Err(StoreError::InvalidInput(
                    json!({"error":"A title or description is required"}),
                )),
                400,
            ),
            (Err(StoreError::PermissionDenied), 403),
        ] {
            let response = preview_response(result);
            assert_eq!(response.status().as_u16(), status);
            assert_eq!(response.headers()[header::ALLOW], "POST, OPTIONS");
            assert_eq!(response.headers()[header::VARY], "Accept");
        }
    }

    #[test]
    fn preview_preserves_unhandled_rest_error_headers() {
        let response = preview_response(Err(StoreError::UnhandledRestFault));
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert_eq!(response.headers()[header::VARY], "origin");
        assert!(!response.headers().contains_key(header::ALLOW));
    }

    #[test]
    fn parses_react_request_and_original_limit_fallbacks() {
        for (input, expected) in [
            (json!(null), 25),
            (json!(0), 25),
            (json!("0"), 1),
            (json!(-2), 1),
            (json!("bad"), 25),
            (json!(250), 100),
        ] {
            let request = Preview::parse(&json!({
                "title":"  Match  ", "tvg_id":"  guide  ", "epg_source_id":"12",
                "mode":"NEW", "untagged_is_new":true, "limit":input
            }))
            .unwrap();
            assert!(
                matches!(request.title, Some(Expression::Term(Match::Exact, term)) if term == "Match")
            );
            assert_eq!(request.tvg_id, "guide");
            assert_eq!(request.source_id, Some(12));
            assert_eq!(request.limit, expected);
            assert!(request.new_only && request.untagged);
        }
        assert!(matches!(
            Preview::parse(&json!({"tvg_id":"only"})),
            Err(StoreError::InvalidInput(_))
        ));
    }

    #[test]
    fn explicit_new_overrides_repeat_and_truthiness_matches_original() {
        assert!(is_new(&json!({"new":true, "previously_shown":true}), false));
        assert!(!is_new(&json!({"previously_shown":{"start":"2026"}}), true));
        assert!(!is_new(&json!({"new":false}), false));
        assert!(is_new(&json!({"new":false, "previously_shown":{}}), true));
        assert!(is_new(&json!({"new":"false"}), false));
    }

    #[test]
    fn compiles_quoted_groups_and_left_associated_operators_to_bound_predicates() {
        for (input, expected) in [
            (
                "one OR two AND three",
                "((p.title ~* $1 OR p.title ~* $2) AND p.title ~* $3)",
            ),
            (
                r#"one AND ("two OR three" OR four)"#,
                "(p.title ~* $1 AND (p.title ~* $2 OR p.title ~* $3))",
            ),
            (
                "one AND (two OR (three AND four))",
                "((p.title ~* $1 AND p.title ~* $2) AND p.title ~* $3)",
            ),
        ] {
            let parsed = parse_text(input, "search").unwrap().unwrap();
            let mut query = QueryBuilder::<Postgres>::new("");
            parsed.write_sql(&mut query, "p.title");
            assert_eq!(query.sql(), expected);
        }
        assert!(matches!(parse_text(r#""^Show (HD)$""#, "regex").unwrap(),
            Some(Expression::Term(Match::Regex, value)) if value == "^Show (HD)$"));
        assert!(parse_text("one AND (two", "contains").unwrap().is_none());
        assert!(matches!(parse_text(r#""a OR b""#, "contains").unwrap(),
            Some(Expression::Term(Match::Contains, value)) if value == "a OR b"));
    }
}
