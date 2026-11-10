use crate::{AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::RngCore;
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::net::SocketAddr;

const ADMIN_PROPERTIES: &[&str] = &[
    "xc_password",
    "allowed_networks",
    "catchup_enabled",
    "vod_movies_enabled",
    "vod_series_enabled",
    "dvr_access",
    "allowed_m3u_profile_ids",
];
const NAV_ITEMS: &[&str] = &[
    "channels",
    "vods",
    "sources",
    "guide",
    "dvr",
    "stats",
    "plugins",
    "integrations",
    "system",
    "settings",
];

fn error(field: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({field:[message]}))
}

/// Match Python's default json.dumps character length (ensure_ascii=True).
fn python_json_length(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(value) => {
            if *value {
                4
            } else {
                5
            }
        }
        Value::Number(value) => crate::json_compat::python_number_length(value),
        Value::String(value) => {
            2 + value
                .chars()
                .map(|character| match character {
                    '"' | '\\' | '\n' | '\r' | '\t' | '\u{0008}' | '\u{000c}' => 2,
                    character if !character.is_ascii() || character.is_control() => {
                        6 * character.len_utf16()
                    }
                    _ => 1,
                })
                .sum::<usize>()
        }
        Value::Array(values) => {
            2 + values.iter().map(python_json_length).sum::<usize>()
                + values.len().saturating_sub(1) * 2
        }
        Value::Object(values) => {
            2 + values
                .iter()
                .map(|(key, value)| {
                    python_json_length(&Value::String(key.clone())) + 2 + python_json_length(value)
                })
                .sum::<usize>()
                + values.len().saturating_sub(1) * 2
        }
    }
}

pub(crate) fn validate_custom(value: &Value) -> Result<Map<String, Value>, StoreError> {
    if value.is_null() {
        return Ok(Map::new());
    }
    let values = value.as_object().ok_or_else(|| {
        error(
            "custom_properties",
            "custom_properties must be a dictionary",
        )
    })?;
    if python_json_length(value) > 102_400 {
        return Err(error(
            "custom_properties",
            "custom_properties exceeds maximum size of 102400 bytes",
        ));
    }
    for field in ["navOrder", "hiddenNav"] {
        if let Some(value) = values.get(field) {
            let array = value
                .as_array()
                .ok_or_else(|| error("custom_properties", &format!("{field} must be an array")))?;
            if array.len() > 50 {
                return Err(error(
                    "custom_properties",
                    &format!("{field} exceeds maximum length of 50 items"),
                ));
            }
            for item in array {
                let name = item.as_str().ok_or_else(|| {
                    error(
                        "custom_properties",
                        &format!("{field} items must be strings"),
                    )
                })?;
                if !NAV_ITEMS.contains(&name) {
                    return Err(error(
                        "custom_properties",
                        &format!("'{name}' is not a valid navigation item ID"),
                    ));
                }
            }
        }
    }
    if let Some(value) = values
        .get("xc_password")
        .filter(|value| !value.is_null() && **value != json!("") && **value != json!(false))
    {
        if value.as_str().is_none_or(|value| {
            value.is_empty()
                || !value.chars().all(|character| {
                    character.is_ascii_alphanumeric() || "._@-".contains(character)
                })
        }) {
            return Err(error("custom_properties","XC password may only contain letters, numbers, periods (.), underscores (_), at signs (@), and hyphens (-)"));
        }
    }
    if let Some(value) = values
        .get("allowed_m3u_profile_ids")
        .filter(|value| !value.is_null())
    {
        if value.as_array().is_none_or(|values| {
            values
                .iter()
                .any(|value| value.as_i64().is_none_or(|id| id <= 0))
        }) {
            return Err(error(
                "custom_properties",
                "allowed_m3u_profile_ids must be a list of positive integer IDs",
            ));
        }
    }
    Ok(values.clone())
}

fn owner_patch(body: Value) -> Result<Map<String, Value>, StoreError> {
    let mut fields = body.as_object().cloned().ok_or_else(|| {
        StoreError::InvalidInput(
            json!({"non_field_errors":["Invalid data. Expected a dictionary, but got list."]}),
        )
    })?;
    fields.retain(|name, _| {
        matches!(
            name.as_str(),
            "custom_properties" | "first_name" | "last_name" | "email" | "password"
        )
    });
    if let Some(value) = fields.get_mut("custom_properties") {
        if let Some(values) = value.as_object_mut() {
            for name in ADMIN_PROPERTIES {
                values.remove(*name);
            }
        }
        *value = Value::Object(validate_custom(value)?);
    }
    for name in ["first_name", "last_name", "email", "password"] {
        if let Some(value) = fields.get_mut(name) {
            if value.is_null() {
                return Err(error(name, "This field may not be null."));
            }
            let string = match value {
                Value::String(value) => value.trim().to_owned(),
                Value::Number(value) => value.to_string(),
                _ => return Err(error(name, "Not a valid string.")),
            };
            let limit = match name {
                "first_name" | "last_name" => 150,
                "email" => 254,
                _ => 4096,
            };
            if string.chars().count() > limit {
                return Err(error(
                    name,
                    &format!("Ensure this field has no more than {limit} characters."),
                ));
            }
            if name == "password" && string.is_empty() {
                return Err(error(name, "This field may not be blank."));
            }
            if name == "email" && !string.is_empty() && !plausible_email(&string) {
                return Err(error(name, "Enter a valid email address."));
            }
            *value = Value::String(string);
        }
    }
    Ok(fields)
}

pub(crate) fn plausible_email(value: &str) -> bool {
    let Some((local, domain)) = value.rsplit_once('@') else {
        return false;
    };
    !local.is_empty()
        && !local.contains('@')
        && !value.chars().any(char::is_whitespace)
        && !domain.is_empty()
        && (domain == "localhost"
            || (domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.')))
}

pub(crate) fn merged_properties(existing: Option<Value>, patch: Option<&Value>) -> Option<Value> {
    let Some(Value::Object(changes)) = patch else {
        return existing;
    };
    let mut properties = existing
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    for (key, value) in changes {
        if value.is_null() {
            properties.remove(key);
        } else {
            properties.insert(key.clone(), value.clone());
        }
    }
    for name in ["navOrder", "hiddenNav"] {
        if let Some(Value::Array(values)) = properties.get_mut(name) {
            values.retain(|value| value.as_str().is_some_and(|name| NAV_ITEMS.contains(&name)));
        }
    }
    Some(Value::Object(properties))
}

pub(crate) async fn patch_me(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    let Json(body) =
        body.map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?;
    // Original me() calls request.data.keys() before its serializer. Preserve
    // the captured default Django HTML500 for authenticated nonobject bodies;
    // permissions still run first and no model mutation is attempted.
    if !body.is_object() {
        return Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/html; charset=utf-8")],
            r#"
<!doctype html>
<html lang="en">
<head>
  <title>Server Error (500)</title>
</head>
<body>
  <h1>Server Error (500)</h1><p></p>
</body>
</html>
"#,
        )
            .into_response());
    }
    let fields = owner_patch(body)?;
    let password = if let Some(value) = fields.get("password") {
        Some(
            auth.hash_password(value.as_str().unwrap().to_owned())
                .await?,
        )
    } else {
        None
    };
    let mut transaction = auth.store.pool().begin().await?;
    let row=sqlx::query("SELECT first_name,last_name,email,custom_properties FROM accounts_user WHERE id=$1 FOR UPDATE").bind(user.id).fetch_optional(&mut *transaction).await?.ok_or(StoreError::NotFound)?;
    let properties = merged_properties(
        row.try_get::<Option<Value>, _>("custom_properties")?,
        fields.get("custom_properties"),
    );
    let first_name = fields
        .get("first_name")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or(row.try_get("first_name")?);
    let last_name = fields
        .get("last_name")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or(row.try_get("last_name")?);
    let email = fields
        .get("email")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or(row.try_get("email")?);
    sqlx::query("UPDATE accounts_user SET first_name=$2,last_name=$3,email=$4,custom_properties=$5,password=COALESCE($6,password) WHERE id=$1")
        .bind(user.id).bind(first_name).bind(last_name).bind(email).bind(properties).bind(password).execute(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(Json(auth.store.serialized_user(user.id).await?).into_response())
}

async fn change_key(
    auth: AuthService,
    actor: crate::User,
    body: Value,
    generate: bool,
) -> Result<Response, StoreError> {
    let requested = body.get("user_id").filter(|value| {
        !value.is_null() && **value != json!(0) && **value != json!("") && **value != json!(false)
    });
    let id = if let Some(value) = requested {
        if actor.user_level < 10 {
            return Err(StoreError::StatusBody(
                403,
                json!({"detail":if generate {"Not allowed to create keys for other users."}else{"Not allowed to revoke keys for other users."}}),
            ));
        }
        let id = value
            .as_i64()
            .or_else(|| value.as_str()?.parse().ok())
            .ok_or_else(|| StoreError::StatusBody(404, json!({"detail":"User not found."})))?;
        if auth.store.user_by_id(id).await?.is_none() {
            return Err(StoreError::StatusBody(
                404,
                json!({"detail":"User not found."}),
            ));
        }
        id
    } else {
        actor.id
    };
    if generate {
        let mut bytes = [0u8; 40];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let key = URL_SAFE_NO_PAD.encode(bytes);
        sqlx::query("UPDATE accounts_user SET api_key=$2 WHERE id=$1")
            .bind(id)
            .bind(&key)
            .execute(auth.store.pool())
            .await?;
        Ok((
            StatusCode::CREATED,
            Json(json!({"key":key,"user":auth.store.serialized_user(id).await?})),
        )
            .into_response())
    } else {
        sqlx::query("UPDATE accounts_user SET api_key=NULL WHERE id=$1")
            .bind(id)
            .execute(auth.store.pool())
            .await?;
        Ok(Json(json!({"success":true})).into_response())
    }
}

pub(crate) async fn generate_key(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    let Json(body) =
        body.map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?;
    change_key(auth, actor, body, true).await
}
pub(crate) async fn revoke_key(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    let Json(body) =
        body.map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?;
    change_key(auth, actor, body, false).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn omitted_properties_preserve_legacy_sql_and_json_null() {
        assert_eq!(merged_properties(None, None), None);
        assert_eq!(
            merged_properties(Some(Value::Null), None),
            Some(Value::Null)
        );
        assert_eq!(merged_properties(None, Some(&json!({}))), Some(json!({})));
        assert_eq!(
            merged_properties(
                Some(json!({"remove":1,"retain":2})),
                Some(&json!({"remove":null,"new":3}))
            ),
            Some(json!({"retain":2,"new":3}))
        );
    }
    #[test]
    fn owner_cannot_change_admin_fields_or_admin_managed_properties() {
        let fields=owner_patch(json!({"user_level":10,"is_superuser":true,"username":"root","custom_properties":{"xc_password":"evil","allowed_networks":{},"dvr_access":"manage","theme":"dark","remove_me":null}})).unwrap();
        assert_eq!(
            fields,
            json!({"custom_properties":{"theme":"dark","remove_me":null}})
                .as_object()
                .unwrap()
                .clone()
        );
    }
    #[test]
    fn python_json_size_counts_escaping_and_unicode_like_source() {
        assert_eq!(python_json_length(&json!({"a":"é🔑","b":[true,null]})), 46);
        assert!(owner_patch(json!({"custom_properties":{"x":"é".repeat(20_000)}})).is_err());
    }
    #[test]
    fn invalid_navigation_email_and_names_fail_validation() {
        assert!(owner_patch(json!({"custom_properties":{"hiddenNav":["unknown"]}})).is_err());
        assert!(owner_patch(json!({"custom_properties":[]})).is_err());
        assert!(owner_patch(json!({"email":"invalid"})).is_err());
        assert!(owner_patch(json!({"first_name":"x".repeat(151)})).is_err());
        assert_eq!(
            owner_patch(json!({"custom_properties":null,"email":" test@example.com "})).unwrap()
                ["email"],
            json!("test@example.com")
        );
    }
}
