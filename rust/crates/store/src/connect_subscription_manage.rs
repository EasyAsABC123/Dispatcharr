//! Original Connect subscription REST reads and durable management.
//! Original Dispatcharr authors retain all application credit.
use super::{capacity, create_integration, lookup, subscriptions, RESPONSE_LIMIT};
use crate::{admin_writes, channel_memberships::datatype, json_compat, Store, StoreError};
use axum::extract::Request;
use serde_json::{json, Map, Value};
use sqlx::{Postgres, Transaction};

const TABLE: &str = "dispatcharr_connect_eventsubscription";
const PROJECTION: &str = "json_build_object('id',id,'event',event,'enabled',enabled,                 'payload_template',payload_template,'integration',integration_id)";
const BYTES: &str = "octet_length(to_json(event)::text)::bigint                     + COALESCE(octet_length(to_json(payload_template)::text),4) + 192";

fn absent() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No EventSubscription matches the given query."}),
    )
}

pub(super) async fn list(store: &Store, integration: Option<&str>) -> Result<String, StoreError> {
    let integration = match integration {
        Some(raw) => {
            let parsed = dispatcharr_ingest_core::source_integer_literal(raw)
                .ok_or(StoreError::UnhandledRestFault)?;
            let Ok(id) = parsed.parse::<i64>() else {
                return Ok("[]".into());
            };
            Some(id)
        }
        None => None,
    };
    let filtered = format!("FROM {TABLE} WHERE ($1::bigint IS NULL OR integration_id=$1)");
    let sql = format!(
        r#"
        WITH size_bound AS (SELECT COALESCE(SUM({BYTES}),0)+2 AS bytes {filtered})
        SELECT CASE WHEN bytes<=$2::bigint THEN (
            SELECT COALESCE(json_agg({PROJECTION}),'[]'::json)::text {filtered}
        ) END FROM size_bound
        "#
    );
    sqlx::query_scalar::<_, Option<String>>(&sql)
        .bind(integration)
        .bind(RESPONSE_LIMIT)
        .fetch_one(store.pool())
        .await?
        .ok_or_else(capacity)
}

pub(super) async fn retrieve(store: &Store, raw: &str) -> Result<String, StoreError> {
    let id = lookup(raw)?.ok_or_else(absent)?;
    let sql = format!(
        "SELECT CASE WHEN {BYTES}<=$2::bigint THEN {PROJECTION}::text END FROM {TABLE} WHERE id=$1"
    );
    sqlx::query_scalar::<_, Option<String>>(&sql)
        .bind(id)
        .bind(RESPONSE_LIMIT)
        .fetch_optional(store.pool())
        .await?
        .ok_or_else(absent)?
        .ok_or_else(capacity)
}

fn event(value: &Value) -> Result<String, StoreError> {
    if let Some(text) = value
        .as_str()
        .filter(|text| subscriptions::EVENTS.contains(text))
    {
        return Ok(text.to_owned());
    }
    if value.is_null() {
        return Err(create_integration::invalid(
            "event",
            "This field may not be null.",
        ));
    }
    Err(create_integration::invalid(
        "event",
        &format!("\"{}\" is not a valid choice.", display(value)),
    ))
}

fn display(value: &Value) -> String {
    match value {
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::String(text) => text.clone(),
        Value::Number(number) if number.is_f64() => number
            .as_f64()
            .map_or_else(|| number.to_string(), json_compat::float_string),
        _ => value.to_string(),
    }
}

fn template(value: &Value) -> Result<Option<String>, StoreError> {
    if value.is_null() {
        return Ok(None);
    }
    // Python str.strip additionally admits the information separators.
    if let Value::String(text) = value {
        let text =
            text.trim_matches(|ch: char| ch.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&ch));
        return admin_writes::character(
            "payload_template",
            &Value::String(text.into()),
            None,
            true,
        )
        .map(Some);
    }
    admin_writes::character("payload_template", value, None, true).map(Some)
}

fn integration_id(value: &Value) -> Result<Option<i64>, StoreError> {
    if value.is_null() || value.as_str() == Some("") {
        return Err(create_integration::invalid(
            "integration",
            "This field may not be null.",
        ));
    }
    let incorrect = || {
        create_integration::invalid(
            "integration",
            &format!(
                "Incorrect type. Expected pk value, received {}.",
                datatype(value)
            ),
        )
    };
    match value {
        Value::String(text) => Ok(dispatcharr_ingest_core::source_integer_literal(text)
            .ok_or_else(incorrect)?
            .parse()
            .ok()),
        Value::Number(number) if number.is_f64() => Ok(number.as_f64().and_then(|value| {
            let truncated = value.trunc();
            (truncated >= i64::MIN as f64 && truncated < -(i64::MIN as f64))
                .then_some(truncated as i64)
        })),
        Value::Number(number) => Ok(number.as_i64()),
        _ => Err(incorrect()),
    }
}

async fn integration(
    transaction: &mut Transaction<'_, Postgres>,
    value: &Value,
) -> Result<i64, StoreError> {
    let found = match integration_id(value)? {
        Some(id) => {
            sqlx::query_scalar::<_, i64>(
                "SELECT id FROM dispatcharr_connect_integration WHERE id=$1 FOR KEY SHARE",
            )
            .bind(id)
            .fetch_optional(&mut **transaction)
            .await?
        }
        None => None,
    };
    found.ok_or_else(|| {
        create_integration::invalid(
            "integration",
            &format!("Invalid pk \"{}\" - object does not exist.", display(value)),
        )
    })
}

struct Changes {
    event: Option<String>,
    enabled: Option<bool>,
    template: Option<Option<String>>,
    integration: Option<i64>,
}

async fn validate(
    transaction: &mut Transaction<'_, Postgres>,
    fields: &Map<String, Value>,
    partial: bool,
) -> Result<Changes, StoreError> {
    let mut errors = Map::new();
    let mandatory = |field: &str| {
        if partial {
            Ok(fields.get(field))
        } else {
            create_integration::required(fields, field).map(Some)
        }
    };
    let event = create_integration::collect(
        &mut errors,
        mandatory("event").and_then(|value| value.map(event).transpose()),
    )?
    .flatten();
    let enabled = create_integration::collect(
        &mut errors,
        fields
            .get("enabled")
            .map(|v| admin_writes::boolean("enabled", v))
            .transpose(),
    )?
    .flatten();
    let template = create_integration::collect(
        &mut errors,
        fields.get("payload_template").map(template).transpose(),
    )?
    .flatten();
    let integration = match mandatory("integration") {
        Ok(Some(value)) => integration(transaction, value).await.map(Some),
        other => other.map(|_| None),
    };
    let integration = create_integration::collect(&mut errors, integration)?.flatten();
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    Ok(Changes {
        event,
        enabled,
        template,
        integration,
    })
}

pub(super) async fn create(store: &Store, request: Request) -> Result<String, StoreError> {
    let fields = create_integration::fields(request).await?;
    let mut transaction = store.pool().begin().await?;
    let result = async {
        let changes = validate(&mut transaction, &fields, false).await?;
        let sql = format!(
            r#"
            WITH inserted AS (
                INSERT INTO {TABLE}(event,enabled,payload_template,integration_id)
                SELECT $1,$2,$3::text,$4
                WHERE COALESCE(octet_length(to_json($3::text)::text),4)+256<=$5::bigint
                RETURNING *
            ) SELECT {PROJECTION}::text FROM inserted
            "#
        );
        sqlx::query_scalar::<_, String>(&sql)
            .bind(changes.event.ok_or(StoreError::UnhandledRestFault)?)
            .bind(changes.enabled.unwrap_or(true))
            .bind(changes.template.flatten())
            .bind(changes.integration.ok_or(StoreError::UnhandledRestFault)?)
            .bind(RESPONSE_LIMIT)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(capacity)
    }
    .await;
    subscriptions::settle(transaction, result).await
}

pub(super) async fn update(
    store: &Store,
    raw: &str,
    request: Request,
    partial: bool,
) -> Result<String, StoreError> {
    let id = lookup(raw)?.ok_or_else(absent)?;
    let present: bool =
        sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {TABLE} WHERE id=$1)"))
            .bind(id)
            .fetch_one(store.pool())
            .await?;
    if !present {
        return Err(absent());
    }
    let fields = create_integration::fields(request).await?;
    let mut transaction = store.pool().begin().await?;
    let result = async {
        // Validation locks any new parent before touching the subscription,
        // matching the lock order of integration cascade deletion.
        let changes = validate(&mut transaction, &fields, partial).await?;
        let sql = format!(
            r#"
            WITH selected AS MATERIALIZED (SELECT id FROM {TABLE} WHERE id=$1 FOR UPDATE),
            updated AS (
                UPDATE {TABLE}
                SET event=COALESCE($2,event),enabled=COALESCE($3,enabled),
                    payload_template=CASE WHEN $4 THEN $5::text ELSE payload_template END,
                    integration_id=COALESCE($6,integration_id)
                WHERE id IN(SELECT id FROM selected) AND
                    COALESCE(octet_length(to_json(
                        CASE WHEN $4 THEN $5::text ELSE payload_template END)::text),4)::bigint + 256<=$7::bigint
                RETURNING *
            )
            SELECT EXISTS(SELECT 1 FROM selected),(SELECT {PROJECTION}::text FROM updated)
            "#
        );
        let (present, value): (bool, Option<String>) = sqlx::query_as(&sql)
            .bind(id)
            .bind(changes.event)
            .bind(changes.enabled)
            .bind(changes.template.is_some())
            .bind(changes.template.flatten())
            .bind(changes.integration)
            .bind(RESPONSE_LIMIT)
            .fetch_one(&mut *transaction)
            .await?;
        if !present {
            return Err(absent());
        }
        value.ok_or_else(capacity)
    }.await;
    subscriptions::settle(transaction, result).await
}

pub(super) async fn delete(store: &Store, raw: &str) -> Result<(), StoreError> {
    let id = lookup(raw)?.ok_or_else(absent)?;
    let mut transaction = store.pool().begin().await?;
    let result = async {
        sqlx::query_scalar::<_, i64>(&format!("SELECT id FROM {TABLE} WHERE id=$1 FOR UPDATE"))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(absent)?;
        sqlx::query("DELETE FROM dispatcharr_connect_deliverylog WHERE subscription_id=$1")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(&format!("DELETE FROM {TABLE} WHERE id=$1"))
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        Ok(())
    }
    .await;
    subscriptions::settle(transaction, result).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_subscription_primary_key_and_nullable_template() {
        assert_eq!(integration_id(&json!(1.6)).unwrap(), Some(1));
        assert_eq!(integration_id(&json!("١")).unwrap(), Some(1));
        assert_eq!(integration_id(&json!("9223372036854775808")).unwrap(), None);
        for (value, detail) in [
            (
                json!("1.0"),
                "Incorrect type. Expected pk value, received str.",
            ),
            (
                json!(true),
                "Incorrect type. Expected pk value, received bool.",
            ),
            (json!(""), "This field may not be null."),
        ] {
            let Err(StoreError::InvalidInput(response)) = integration_id(&value) else {
                panic!("invalid integration field was accepted");
            };
            assert_eq!(response, json!({"integration":[detail]}));
        }
        assert_eq!(template(&Value::Null).unwrap(), None);
        assert_eq!(
            template(&json!(" \u{001c}x\u{0085} ")).unwrap().as_deref(),
            Some("x")
        );
        assert_eq!(template(&json!(1.0)).unwrap().as_deref(), Some("1.0"));
        assert!(event(&json!(" channel_start ")).is_err());
    }
}
