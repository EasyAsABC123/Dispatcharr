//! Original Dispatcharr's admin-only Connect delivery log list.
//! Dispatcharr authors and contributors retain all application credit.
use super::{capacity, RESPONSE_LIMIT};
use crate::{
    vod_api_draft::vod_api_pagination::{last_query, ordinary_with_limits, page_url},
    Store, StoreError,
};
use axum::http::{header, uri::Authority, HeaderMap, Uri};
use serde_json::json;

const ROUTE: &str = "/api/connect/logs/";
const FILTERED: &str = r#"
    FROM dispatcharr_connect_deliverylog l
    WHERE NOT $3::boolean AND (($1::bigint IS NULL AND $2::text IS NULL) OR EXISTS (
        SELECT 1 FROM dispatcharr_connect_eventsubscription s
        JOIN dispatcharr_connect_integration i ON i.id=s.integration_id
        WHERE s.id=l.subscription_id AND ($1 IS NULL OR i.id=$1) AND ($2 IS NULL OR i.type=$2)
    ))
"#;
const PAGE_PROJECTION: &str = r#"
    , page_rows AS MATERIALIZED (
        SELECT p.id,p.status,p.created_at,p.request_payload,p.response_payload,p.error_message,
               s.id AS subscription_id,s.event,s.enabled,s.payload_template,s.integration_id
        FROM page p JOIN dispatcharr_connect_eventsubscription s ON s.id=p.subscription_id
    ), size_bound AS (
        SELECT COALESCE(SUM(octet_length(request_payload::text)::bigint
                          + octet_length(response_payload::text)
                          + COALESCE(octet_length(to_json(error_message)::text),4)
                          + COALESCE(octet_length(to_json(payload_template)::text),4)
                          + octet_length(to_json(event)::text)
                          + octet_length(to_json(status)::text) + 384),0)
             + COALESCE(octet_length(to_json($7::text)::text),4)
             + COALESCE(octet_length(to_json($8::text)::text),4) + 80 AS bytes
        FROM page_rows
    )
    SELECT CASE WHEN bytes <= $9::bigint THEN json_build_object(
        'count',$6::bigint,'next',$7::text,'previous',$8::text,'results',COALESCE((
            SELECT json_agg(json_build_object(
                'id',id,'subscription',json_build_object(
                    'id',subscription_id,'event',event,'enabled',enabled,
                    'payload_template',payload_template,'integration',integration_id),
                'status',status,'request_payload',request_payload,'response_payload',response_payload,
                'error_message',error_message,
                'created_at',to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
                    || CASE WHEN MOD(EXTRACT(MICROSECONDS FROM created_at)::bigint,1000000)=0
                            THEN '' ELSE to_char(created_at AT TIME ZONE 'UTC','.US') END || 'Z'
            ) ORDER BY created_at DESC) FROM page_rows
        ),'[]'::json)
    )::text END FROM size_bound
"#;

fn request_origin(headers: &HeaderMap) -> Result<String, StoreError> {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    host.parse::<Authority>()
        .map_err(|_| StoreError::InvalidInput(json!({"detail":"Invalid HTTP host."})))?;
    // DRF pagination uses Django build_absolute_uri. Django honors the first
    // X-Forwarded-Proto value for HTTPS and retains the original Host header.
    let secure = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').next().map(str::trim) == Some("https"));
    let scheme = if secure { "https" } else { "http" };
    Ok(format!("{scheme}://{host}"))
}

pub(super) async fn list(
    store: &Store,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<String, StoreError> {
    let query = url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    let integration = last_query(&query, "integration")
        .filter(|value| !value.is_empty())
        .map(|value| {
            dispatcharr_ingest_core::source_integer_literal(value)
                .ok_or(StoreError::UnhandledRestFault)
        })
        .transpose()?;
    let integration_id = integration.as_deref().and_then(|id| id.parse::<i64>().ok());
    let outside_integer_range = integration.is_some() && integration_id.is_none();
    let kind = last_query(&query, "type").filter(|value| !value.is_empty());

    // Count and returned rows must describe the same snapshot even during
    // concurrent delivery writes or integration deletion.
    let mut transaction = store.pool().begin().await?;
    let result = async {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let count_sql = format!("SELECT COUNT(*) {FILTERED}");
        let count: i64 = sqlx::query_scalar(&count_sql)
            .bind(integration_id)
            .bind(kind)
            .bind(outside_integer_range)
            .fetch_one(&mut *transaction)
            .await?;
        let page = ordinary_with_limits(&query, count as u64, 50, 250)
            .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Invalid page."})))?;
        let origin = request_origin(headers)?;
        let next = page
            .has_next()
            .then(|| page_url(&origin, ROUTE, &query, Some(page.number + 1)));
        let previous = page.has_previous().then(|| {
            page_url(
                &origin,
                ROUTE,
                &query,
                (page.number > 2).then_some(page.number - 1),
            )
        });
        let page_sql = format!(
            "WITH page AS MATERIALIZED (SELECT l.* {FILTERED} ORDER BY l.created_at DESC \n\
             LIMIT $5::bigint OFFSET $4::bigint) {PAGE_PROJECTION}"
        );
        sqlx::query_scalar::<_, Option<String>>(&page_sql)
            .bind(integration_id)
            .bind(kind)
            .bind(outside_integer_range)
            .bind(page.offset as i64)
            .bind(page.size as i64)
            .bind(count)
            .bind(next.as_deref())
            .bind(previous.as_deref())
            .bind(RESPONSE_LIMIT)
            .fetch_one(&mut *transaction)
            .await
            .map_err(StoreError::from)
    }
    .await;
    match result {
        Ok(result) => {
            transaction.commit().await?;
            result.ok_or_else(capacity)
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}
