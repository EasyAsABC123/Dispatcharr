//! Next M6 slice: original ServerGroup CRUD, with no job signals.
//! Original Dispatcharr authors retain full credit; see ATTRIBUTION.md.
//! Source HTTP goldens are required before marking this slice verified.
//! Account/profile/source refresh writes remain gated.
use crate::{
    admin_writes::{body_object, character},
    AuthService, StoreError,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use std::net::SocketAddr;
type Body = Result<Json<Value>, JsonRejection>;

fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No ServerGroup matches the given query."}),
    )
}
fn id(raw: String) -> Result<i64, StoreError> {
    crate::channel_memberships::primary_key(&Value::String(raw))
        .map_err(|_| StoreError::NotFound)?
        .ok_or_else(missing)
}
fn name(body: Body, partial: bool) -> Result<Option<String>, StoreError> {
    let body = body_object(body)?;
    match body.get("name") {
        Some(value) => character("name", value, Some(100), false).map(Some),
        None if partial => Ok(None),
        None => Err(StoreError::InvalidInput(
            json!({"name":["This field is required."]}),
        )),
    }
}
async fn unique(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    id: Option<i64>,
) -> Result<(), StoreError> {
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM m3u_servergroup WHERE name=$1 AND ($2::bigint IS NULL OR id<>$2))")
        .bind(name).bind(id).fetch_one(&mut **tx).await?;
    if exists {
        return Err(StoreError::InvalidInput(
            json!({"name":["server group with this name already exists."]}),
        ));
    }
    Ok(())
}
pub(crate) async fn create_server_group(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<Value>), StoreError> {
    auth.account_user(peer, headers, true).await?;
    let name = name(body, false)?.expect("required name");
    let mut tx = auth.store.pool().begin().await?;
    unique(&mut tx, &name, None).await?;
    let id: i64 =
        sqlx::query_scalar("INSERT INTO m3u_servergroup(name) VALUES($1) RETURNING id::bigint")
            .bind(&name)
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(json!({"id":id,"name":name}))))
}
async fn change(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    raw: String,
    body: Body,
    partial: bool,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    let old: String = sqlx::query_scalar("SELECT name FROM m3u_servergroup WHERE id=$1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(missing)?;
    // Generic ModelViewSet resolves the object before validating input.
    let name = name(body, partial)?.unwrap_or(old);
    unique(&mut tx, &name, Some(id)).await?;
    sqlx::query("UPDATE m3u_servergroup SET name=$2 WHERE id=$1")
        .bind(id)
        .bind(&name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Json(json!({"id":id,"name":name})))
}
pub(crate) async fn patch_server_group(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change(auth, peer, headers, raw, body, true).await
}
pub(crate) async fn put_server_group(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change(auth, peer, headers, raw, body, false).await
}
pub(crate) async fn delete_server_group(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT id::bigint FROM m3u_servergroup WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    if exists.is_none() {
        return Err(missing());
    }
    // Django's collector implements SET_NULL; its PostgreSQL FK does not.
    // Do not run Account.save or touch updated_at/refresh/profile signals.
    sqlx::query("UPDATE m3u_m3uaccount SET server_group_id=NULL WHERE server_group_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM m3u_servergroup WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

fn filter_missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No M3UFilter matches the given query."}),
    )
}
fn filter_key(raw: String) -> Result<i64, StoreError> {
    crate::channel_memberships::primary_key(&Value::String(raw))
        .map_err(|_| StoreError::NotFound)?
        .ok_or_else(filter_missing)
}
fn choice_text(value: &Value, nested: bool) -> String {
    match value {
        Value::Null => "None".into(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::String(value) if !nested => value.clone(),
        Value::String(value) => {
            let quote = if value.contains('\'') && !value.contains('"') {
                '"'
            } else {
                '\''
            };
            let mut out = String::new();
            out.push(quote);
            for c in value.chars() {
                match c {
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if c == quote => {
                        out.push('\\');
                        out.push(c);
                    }
                    c if c.is_control() => out.push_str(&format!("\\x{:02x}", c as u32)),
                    c => out.push(c),
                }
            }
            out.push(quote);
            out
        }
        Value::Number(number) if number.is_f64() => number
            .as_f64()
            .map(crate::json_compat::float_string)
            .unwrap_or_else(|| number.to_string()),
        Value::Number(number) => number.to_string(),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| choice_text(value, true))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!(
                    "{}: {}",
                    choice_text(&Value::String(key.clone()), true),
                    choice_text(value, true)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}
fn filter_values(body: Body, old: Option<&Value>, partial: bool) -> Result<Value, StoreError> {
    let input = body_object(body)?;
    let mut value=old.cloned().unwrap_or_else(||json!({"filter_type":"group","regex_pattern":"","exclude":true,"order":0,"custom_properties":{}}));
    let mut errors = serde_json::Map::new();
    if !partial && !input.contains_key("regex_pattern") {
        errors.insert("regex_pattern".into(), json!(["This field is required."]));
    }
    for (field, input) in input {
        let parsed = match field.as_str() {
            "regex_pattern" => character(&field, &input, Some(200), false).map(Value::String),
            "filter_type" => {
                if input.is_null() {
                    Err(StoreError::InvalidInput(
                        json!({"filter_type":["This field may not be null."]}),
                    ))
                } else if input
                    .as_str()
                    .is_some_and(|value| ["group", "name", "url"].contains(&value))
                {
                    Ok(input)
                } else {
                    Err(StoreError::InvalidInput(
                        json!({"filter_type":[format!("\"{}\" is not a valid choice.",choice_text(&input,false))]}),
                    ))
                }
            }
            "exclude" => crate::admin_writes::boolean(&field, &input).map(Value::Bool),
            "order" => crate::admin_writes::integer(&field, &input).and_then(|value| {
                if value < 0 {
                    Err(StoreError::InvalidInput(
                        json!({"order":["Ensure this value is greater than or equal to 0."]}),
                    ))
                } else {
                    Ok(json!(value))
                }
            }),
            "custom_properties" => Ok(input),
            _ => continue,
        };
        match parsed {
            Ok(parsed) => value[&field] = parsed,
            Err(StoreError::InvalidInput(Value::Object(fields))) => errors.extend(fields),
            Err(error) => return Err(error),
        }
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    // ModelSerializer does not call M3UFilter.clean/full_clean. Validate the
    // source API behavior, not the stricter Django-admin regex form behavior.
    Ok(value)
}
fn json_property(value: &Value) -> Option<sqlx::types::Json<Value>> {
    (!value.is_null()).then(|| sqlx::types::Json(value.clone()))
}
pub(crate) async fn create_filter(
    State(auth): State<AuthService>,
    Path(parent): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<Value>), StoreError> {
    auth.account_user(peer, headers, true).await?;
    let values = filter_values(body, None, false)?;
    let parent = filter_key(parent)?;
    let mut raw:Value=sqlx::query_scalar("INSERT INTO m3u_m3ufilter(m3u_account_id,filter_type,regex_pattern,exclude,\"order\",custom_properties) VALUES($1,$2,$3,$4,$5,$6) RETURNING to_jsonb(m3u_m3ufilter)")
        .bind(parent).bind(values["filter_type"].as_str().unwrap()).bind(values["regex_pattern"].as_str().unwrap())
        .bind(values["exclude"].as_bool().unwrap()).bind(values["order"].as_i64().unwrap() as i32)
        .bind(json_property(&values["custom_properties"])).fetch_one(auth.store.pool()).await?;
    // Django's write response retains request JSON number types; a later GET
    // observes PostgreSQL JSONB's canonical number representation instead.
    raw["custom_properties"] = values["custom_properties"].clone();
    Ok((
        StatusCode::CREATED,
        Json(crate::provider_reads::filter(&raw)),
    ))
}
async fn change_filter(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    keys: (String, String),
    body: Body,
    partial: bool,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let parent = filter_key(keys.0)?;
    let id = filter_key(keys.1)?;
    let mut tx = auth.store.pool().begin().await?;
    let old: Value = sqlx::query_scalar(
        "SELECT to_jsonb(f) FROM m3u_m3ufilter f WHERE f.m3u_account_id=$1 AND f.id=$2 FOR UPDATE",
    )
    .bind(parent)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(filter_missing)?;
    let values = filter_values(body, Some(&old), partial)?;
    let mut raw:Value=sqlx::query_scalar("UPDATE m3u_m3ufilter f SET filter_type=$3,regex_pattern=$4,exclude=$5,\"order\"=$6,custom_properties=$7 WHERE f.m3u_account_id=$1 AND f.id=$2 RETURNING to_jsonb(f)")
        .bind(parent).bind(id).bind(values["filter_type"].as_str().unwrap()).bind(values["regex_pattern"].as_str().unwrap())
        .bind(values["exclude"].as_bool().unwrap()).bind(values["order"].as_i64().unwrap() as i32)
        .bind(json_property(&values["custom_properties"])).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    raw["custom_properties"] = values["custom_properties"].clone();
    Ok(Json(crate::provider_reads::filter(&raw)))
}
pub(crate) async fn patch_filter(
    State(auth): State<AuthService>,
    Path(keys): Path<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change_filter(auth, peer, headers, keys, body, true).await
}
pub(crate) async fn put_filter(
    State(auth): State<AuthService>,
    Path(keys): Path<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change_filter(auth, peer, headers, keys, body, false).await
}
pub(crate) async fn delete_filter(
    State(auth): State<AuthService>,
    Path(keys): Path<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let parent = filter_key(keys.0)?;
    let id = filter_key(keys.1)?;
    let result = sqlx::query("DELETE FROM m3u_m3ufilter WHERE m3u_account_id=$1 AND id=$2")
        .bind(parent)
        .bind(id)
        .execute(auth.store.pool())
        .await?;
    if result.rows_affected() == 0 {
        return Err(filter_missing());
    }
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filter_validation_uses_serializer_defaults_not_model_clean_and_preserves_json_types() {
        let value = filter_values(
            Ok(Json(
                json!({"regex_pattern":" [ ","custom_properties":{"n":1e23}}),
            )),
            None,
            false,
        )
        .unwrap();
        assert_eq!(value["regex_pattern"], "[");
        assert_eq!(value["filter_type"], "group");
        assert_eq!(value["exclude"], true);
        assert_eq!(value["order"], 0);
        assert!(value["custom_properties"]["n"]
            .as_number()
            .unwrap()
            .is_f64());
        assert!(filter_values(Ok(Json(json!({}))), Some(&value), false).is_err());
        assert_eq!(
            filter_values(Ok(Json(json!({}))), Some(&value), true).unwrap(),
            value
        );
        for body in [
            json!({"regex_pattern":""}),
            json!({"regex_pattern":"x".repeat(201)}),
            json!({"regex_pattern":"ok","filter_type":" group "}),
            json!({"regex_pattern":"ok","exclude":null}),
            json!({"regex_pattern":"ok","order":true}),
            json!({"regex_pattern":"ok","order":-1}),
        ] {
            assert!(filter_values(Ok(Json(body)), None, false).is_err());
        }
    }
    #[test]
    fn server_group_source_validation_required_partial_length_types_and_readonly_fields() {
        assert!(name(Ok(Json(json!({}))), false).is_err());
        assert_eq!(name(Ok(Json(json!({}))), true).unwrap(), None);
        assert_eq!(
            name(
                Ok(Json(json!({"name":" abc ","id":123,"unknown":true}))),
                false
            )
            .unwrap(),
            Some("abc".into())
        );
        assert_eq!(
            name(Ok(Json(json!({"name":12}))), false).unwrap(),
            Some("12".into())
        );
        assert!(name(Ok(Json(json!({"name":"x".repeat(100)}))), false).is_ok());
        for value in [
            Value::Null,
            json!(true),
            json!(""),
            json!(" "),
            json!("x".repeat(101)),
            json!("bad\0name"),
        ] {
            assert!(name(Ok(Json(json!({"name":value}))), false).is_err());
        }
        assert!(name(Ok(Json(json!([]))), true).is_err());
    }
}
