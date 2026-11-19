//! Stock JSON Connect integration create ported from Original IntegrationSerializer.
//! Original Dispatcharr authors and contributors retain full application credit.
use super::{capacity, RESPONSE_LIMIT};
use crate::{admin_writes, channel_memberships::datatype, startup_bootstrap, Store, StoreError};
use axum::{body::to_bytes, extract::Request, http::header};
use serde_json::{json, Map, Value};

const INSERT: &str = r#"
WITH input AS MATERIALIZED (
    SELECT $1::text AS name,$2::text AS type,$3::jsonb AS config,$4::bool AS enabled
), inserted AS (
    INSERT INTO dispatcharr_connect_integration(name,type,config,enabled,created_at)
    SELECT name,type,config,enabled,clock_timestamp() FROM input
    WHERE octet_length(to_json(name)::text)::bigint + octet_length(config::text) + 512 <= $5::bigint
    RETURNING id,name,type,config,enabled,created_at
)
SELECT json_build_object(
    'id',id,'name',name,'type',type,'config',config,'enabled',enabled,
    'created_at',to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
        || CASE WHEN MOD(EXTRACT(MICROSECONDS FROM created_at)::bigint,1000000)=0
                THEN '' ELSE to_char(created_at AT TIME ZONE 'UTC','.US') END || 'Z',
    'subscriptions','[]'::json
)::text FROM inserted
"#;

const UPDATE: &str = r#"
WITH input AS MATERIALIZED (
    SELECT $2::text AS name,$3::text AS type,$4::jsonb AS config,$5::bool AS enabled
), updated AS (
    UPDATE dispatcharr_connect_integration i
    SET name=input.name,type=input.type,config=input.config,enabled=input.enabled
    FROM input
    WHERE i.id=$1 AND octet_length(to_json(input.name)::text)::bigint
        + octet_length(input.config::text) + 512
        + (SELECT COALESCE(SUM(octet_length(to_json(s.event)::text)::bigint
                     + COALESCE(octet_length(to_json(s.payload_template)::text),4) + 192),0)
           FROM dispatcharr_connect_eventsubscription s WHERE s.integration_id=i.id) <= $6::bigint
    RETURNING i.id,i.name,i.type,i.config,i.enabled,i.created_at
)
SELECT json_build_object(
    'id',id,'name',name,'type',type,'config',config,'enabled',enabled,
    'created_at',to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
        || CASE WHEN MOD(EXTRACT(MICROSECONDS FROM created_at)::bigint,1000000)=0
                THEN '' ELSE to_char(created_at AT TIME ZONE 'UTC','.US') END || 'Z',
    'subscriptions',COALESCE((SELECT json_agg(json_build_object(
        'id',s.id,'event',s.event,'enabled',s.enabled,
        'payload_template',s.payload_template,'integration',s.integration_id))
        FROM dispatcharr_connect_eventsubscription s WHERE s.integration_id=updated.id),'[]'::json)
)::text FROM updated
"#;

fn invalid(field: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({field:[message]}))
}

fn required<'a>(fields: &'a Map<String, Value>, field: &str) -> Result<&'a Value, StoreError> {
    fields
        .get(field)
        .ok_or_else(|| invalid(field, "This field is required."))
}

fn integration_type(value: &Value) -> Result<&str, StoreError> {
    match value.as_str() {
        Some(kind @ ("webhook" | "script" | "api")) => Ok(kind),
        _ if value.is_null() => Err(invalid("type", "This field may not be null.")),
        _ => {
            let spelling = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            Err(invalid(
                "type",
                &format!("\"{spelling}\" is not a valid choice."),
            ))
        }
    }
}

fn collect<T>(
    errors: &mut Map<String, Value>,
    field: Result<T, StoreError>,
) -> Result<Option<T>, StoreError> {
    match field {
        Ok(value) => Ok(Some(value)),
        Err(StoreError::InvalidInput(Value::Object(fields))) => {
            errors.extend(fields);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn json_payload(request: Request) -> Result<Option<Value>, StoreError> {
    let mime = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned();
    let raw = to_bytes(request.into_body(), RESPONSE_LIMIT as usize)
        .await
        .map_err(|_| capacity())?;
    if raw.is_empty() {
        return Ok(None);
    }
    if mime != "application/json" {
        return Err(StoreError::StatusBody(
            415,
            json!({"detail":format!("Unsupported media type \"{mime}\" in request.")}),
        ));
    }
    serde_json::from_slice::<Value>(&raw).map(Some).map_err(|error| {
        StoreError::InvalidInput(json!({"detail":format!("JSON parse error - {}", startup_bootstrap::json_error(&raw,error))}))
    })
}

async fn fields(request: Request) -> Result<Map<String, Value>, StoreError> {
    match json_payload(request).await? {
        None => Ok(Map::new()),
        Some(Value::Object(fields)) => Ok(fields),
        Some(Value::Null) => Err(invalid("non_field_errors", "No data provided")),
        Some(value) => Err(invalid(
            "non_field_errors",
            &format!(
                "Invalid data. Expected a dictionary, but got {}.",
                datatype(&value)
            ),
        )),
    }
}

struct IntegrationInput {
    name: String,
    kind: String,
    config: Value,
    enabled: bool,
}

async fn validate(
    mut fields: Map<String, Value>,
    stored_config: Option<Value>,
    stored_enabled: bool,
) -> Result<IntegrationInput, StoreError> {
    let mut errors = Map::new();
    let name = collect(
        &mut errors,
        required(&fields, "name")
            .and_then(|value| admin_writes::character("name", value, Some(255), false)),
    )?;
    let kind = collect(
        &mut errors,
        required(&fields, "type")
            .and_then(integration_type)
            .map(str::to_owned),
    )?;
    let config = fields.remove("config").or(stored_config);
    if config.as_ref().is_some_and(Value::is_null) {
        errors.insert("config".into(), json!(["This field may not be null."]));
    }
    let enabled = collect(
        &mut errors,
        fields.get("enabled").map_or(Ok(stored_enabled), |value| {
            admin_writes::boolean("enabled", value)
        }),
    )?;
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    let name = name.ok_or(StoreError::UnhandledRestFault)?;
    let kind = kind.ok_or(StoreError::UnhandledRestFault)?;
    let enabled = enabled.ok_or(StoreError::UnhandledRestFault)?;
    let (field, message) = match kind.as_str() {
        "webhook" => ("url", "Webhook config must include a 'url' string"),
        "script" => ("path", "Script config must include a 'path' string"),
        _ => return Err(invalid("type", "Unsupported integration type")),
    };
    let config = config.ok_or_else(capacity)?;
    if !config.is_object() && crate::channels::truthy(Some(&config)) {
        return Err(StoreError::UnhandledRestFault);
    }
    let target = config
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("config", message))?;
    if kind == "script" && tokio::fs::metadata(target).await.is_err() {
        return Err(invalid(
            "config",
            &format!("Script path does not exist: {target}"),
        ));
    }

    Ok(IntegrationInput {
        name,
        kind,
        config,
        enabled,
    })
}

pub(super) async fn create(store: &Store, request: Request) -> Result<String, StoreError> {
    let IntegrationInput {
        name,
        kind,
        config,
        enabled,
    } = validate(fields(request).await?, Some(json!({})), true).await?;
    // A matched INSERT and its response execute as one statement. The size
    // check precedes insertion, and this path has no delivery or write hooks.
    sqlx::query_scalar::<_, String>(INSERT)
        .bind(name)
        .bind(kind)
        .bind(sqlx::types::Json(config))
        .bind(enabled)
        .bind(RESPONSE_LIMIT)
        .fetch_optional(store.pool())
        .await?
        .ok_or_else(capacity)
}

fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No Integration matches the given query."}),
    )
}

pub(super) async fn update(
    store: &Store,
    raw: String,
    request: Request,
) -> Result<String, StoreError> {
    let id = dispatcharr_ingest_core::source_integer_literal(&raw)
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(missing)?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_integration WHERE id=$1)",
    )
    .bind(id)
    .fetch_one(store.pool())
    .await?;
    if !exists {
        return Err(missing());
    }
    let fields = fields(request).await?;
    let needs_config = !fields.contains_key("config");
    let mut transaction = store.pool().begin().await?;
    let result = async {
        // Do not hold a database lock while the request body is arriving. The
        // second lookup locks the row while applying defaults from current SQL.
        let (stored_config, stored_enabled): (Option<String>, bool) = sqlx::query_as(
            "SELECT CASE WHEN $2 AND octet_length(config::text)<=$3 THEN config::text END,enabled FROM dispatcharr_connect_integration WHERE id=$1 FOR UPDATE",
        ).bind(id)
        .bind(needs_config)
        .bind(RESPONSE_LIMIT)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(missing)?;
        let stored_config = stored_config.as_deref()
            .map(serde_json::from_str::<Value>)
            .transpose()
            .map_err(|_| StoreError::UnhandledRestFault)?;
        let IntegrationInput {name,kind,config,enabled} = validate(fields, stored_config, stored_enabled).await?;
        sqlx::query_scalar::<_, String>(UPDATE)
            .bind(id)
            .bind(name)
            .bind(kind)
            .bind(sqlx::types::Json(config))
            .bind(enabled)
            .bind(RESPONSE_LIMIT)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(capacity)
    }.await;
    match result {
        Ok(payload) => {
            transaction.commit().await?;
            Ok(payload)
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}
