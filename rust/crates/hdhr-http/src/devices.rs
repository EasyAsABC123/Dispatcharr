//! Source HDHRDeviceSerializer and viewset. Original Dispatcharr author credit retained.
use crate::gateway::{unavailable, Inner};
use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode},
};
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::StoreError;
use futures_util::TryStreamExt;
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Row};
fn projection(row: &PgRow) -> Result<Value, sqlx::Error> {
    Ok(
        json!({"id":row.try_get::<i64,_>("id")?,"friendly_name":row.try_get::<String,_>("friendly_name")?,"device_id":row.try_get::<String,_>("device_id")?,"tuner_count":row.try_get::<i32,_>("tuner_count")?}),
    )
}
const COLUMNS: &str = "id::bigint AS id,friendly_name,device_id,tuner_count";
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No HDHRDevice matches the given query."}),
    )
}
fn invalid(value: Value) -> StoreError {
    StoreError::InvalidInput(value)
}
pub(crate) async fn handle(
    inner: &Inner,
    identity: &PlaybackRequest,
    lookup: Option<String>,
    method: &Method,
    headers: &HeaderMap,
    body: Body,
) -> Result<(StatusCode, Option<Vec<u8>>), StoreError> {
    let read = matches!(*method, Method::GET | Method::HEAD);
    let user = inner.auth.authorize_ui(identity, !read).await?;
    if read && user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let detail = lookup.is_some();
    if *method == Method::OPTIONS {
        return Ok((StatusCode::OK, Some(encode(options(detail))?)));
    }
    let supported = if detail {
        matches!(
            *method,
            Method::GET | Method::HEAD | Method::PUT | Method::PATCH | Method::DELETE
        )
    } else {
        matches!(*method, Method::GET | Method::HEAD | Method::POST)
    };
    if !supported {
        return Err(StoreError::StatusBody(
            405,
            json!({"detail":format!("Method \"{method}\" not allowed.")}),
        ));
    }
    let current = if let Some(id) = &lookup {
        let value = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM hdhr_hdhrdevice WHERE id::text=$1"
        ))
        .bind(id)
        .fetch_optional(inner.store.pool())
        .await?
        .ok_or_else(missing)?;
        Some(projection(&value)?)
    } else {
        None
    };
    if read {
        if let Some(value) = current {
            return Ok((StatusCode::OK, Some(encode(value)?)));
        }
        let query = format!("SELECT {COLUMNS} FROM hdhr_hdhrdevice LIMIT $1");
        let mut rows = sqlx::query(&query)
            .bind((inner.config.max_devices + 1) as i64)
            .fetch(inner.store.pool());
        let mut data = vec![b'['];
        let mut count = 0;
        let mut bytes = 0;
        while let Some(row) = rows.try_next().await? {
            let value = encode(projection(&row)?)?;
            count += 1;
            bytes += value.len();
            if count > inner.config.max_devices
                || bytes > inner.config.max_metadata_bytes
                || data.len() + value.len() + usize::from(count > 1) + 1
                    > inner.config.max_response_bytes
            {
                return Err(unavailable());
            }
            if count > 1 {
                data.push(b',')
            }
            data.extend_from_slice(&value);
        }
        data.push(b']');
        return Ok((StatusCode::OK, Some(data)));
    }
    if *method == Method::DELETE {
        sqlx::query("DELETE FROM hdhr_hdhrdevice WHERE id::text=$1")
            .bind(lookup)
            .execute(inner.store.pool())
            .await?;
        return Ok((StatusCode::NO_CONTENT, None));
    }
    let bytes = axum::body::to_bytes(body, inner.config.max_body_bytes)
        .await
        .map_err(|_| unavailable())?;
    let input = if bytes.is_empty() {
        json!({})
    } else if headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/x-www-form-urlencoded"))
    {
        Value::Object(
            url::form_urlencoded::parse(&bytes)
                .map(|(key, value)| (key.into_owned(), Value::String(value.into_owned())))
                .collect(),
        )
    } else {
        let mime = headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .unwrap_or("")
            .trim();
        if mime != "application/json" {
            return Err(StoreError::StatusBody(
                415,
                json!({"detail":format!("Unsupported media type \"{mime}\" in request.")}),
            ));
        }
        serde_json::from_slice::<Value>(&bytes).map_err(|error| {
            StoreError::StatusBody(400, json!({"detail":format!("JSON parse error - {}",dispatcharr_store::source_json_error(&bytes,error))}))
        })?
    };
    if input.is_null() {
        return Err(invalid(json!({"non_field_errors":["No data provided"]})));
    }
    let object=input.as_object().ok_or_else(||invalid(json!({"non_field_errors":[format!("Invalid data. Expected a dictionary, but got {}.",match input{Value::Array(_)=>"list",Value::Null=>"NoneType",Value::Bool(_)=>"bool",Value::Number(_)=>"int",_=>"str"})]})))?;
    let mut errors = Map::new();
    let mut changes = Map::new();
    for (key, max, required) in [("friendly_name", 100, false), ("device_id", 32, true)] {
        match object.get(key) {
            None => {
                if required && *method != Method::PATCH {
                    errors.insert(key.into(), json!(["This field is required."]));
                }
            }
            Some(value) => match string(value, max) {
                Ok(value) => {
                    changes.insert(key.into(), Value::String(value));
                }
                Err(error) => {
                    errors.insert(key.into(), json!([error]));
                }
            },
        }
    }
    if let Some(value) = object.get("tuner_count") {
        match integer(value) {
            Ok(value) => {
                changes.insert("tuner_count".into(), json!(value));
            }
            Err(error) => {
                errors.insert("tuner_count".into(), json!([error]));
            }
        }
    }
    if let Some(Value::String(value)) = changes.get("device_id") {
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM hdhr_hdhrdevice WHERE device_id=$1 AND ($2::text IS NULL OR id::text<>$2))").bind(value).bind(&lookup).fetch_one(inner.store.pool()).await?;
        if exists {
            errors.insert(
                "device_id".into(),
                json!(["hdhr device with this device id already exists."]),
            );
        }
    }
    if !errors.is_empty() {
        return Err(invalid(Value::Object(errors)));
    }
    let mut merged = current
        .clone()
        .unwrap_or_else(|| json!({"friendly_name":"Dispatcharr HDHomeRun","tuner_count":3}));
    for (key, value) in changes {
        merged.as_object_mut().unwrap().insert(key, value);
    }
    let result = if detail {
        sqlx::query(&format!("UPDATE hdhr_hdhrdevice SET friendly_name=$1,device_id=$2,tuner_count=$3 WHERE id::text=$4 RETURNING {COLUMNS}")).bind(merged["friendly_name"].as_str().unwrap()).bind(merged["device_id"].as_str().unwrap()).bind(merged["tuner_count"].as_i64().unwrap() as i32).bind(&lookup).fetch_optional(inner.store.pool()).await
    } else {
        sqlx::query(&format!("INSERT INTO hdhr_hdhrdevice (friendly_name,device_id,tuner_count) VALUES ($1,$2,$3) RETURNING {COLUMNS}")).bind(merged["friendly_name"].as_str().unwrap()).bind(merged["device_id"].as_str().unwrap()).bind(merged["tuner_count"].as_i64().unwrap() as i32).fetch_optional(inner.store.pool()).await
    };
    let row = match result {
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("23505") => {
            return Err(invalid(
                json!({"device_id":["hdhr device with this device id already exists."]}),
            ))
        }
        value => value?,
    };
    Ok((
        if detail {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Some(encode(projection(&row.ok_or_else(missing)?)?)?),
    ))
}
fn encode(value: Value) -> Result<Vec<u8>, StoreError> {
    serde_json::to_vec(&value).map_err(|_| unavailable())
}
fn string(value: &Value, max: usize) -> Result<String, String> {
    let value = match value {
        Value::Null => return Err("This field may not be null.".into()),
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => return Err("Not a valid string.".into()),
    };
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err("This field may not be blank.".into());
    }
    if value.chars().count() > max {
        return Err(format!(
            "Ensure this field has no more than {max} characters."
        ));
    }
    if value.contains('\0') {
        return Err("Null characters are not allowed.".into());
    }
    Ok(value)
}
fn integer(value: &Value) -> Result<i32, &'static str> {
    if value.is_null() {
        return Err("This field may not be null.");
    }
    if value.is_boolean() {
        return Err("A valid integer is required.");
    }
    let text = match value {
        Value::String(value) => value.trim().to_string(),
        Value::Number(value) => value.to_string(),
        _ => return Err("A valid integer is required."),
    };
    let text = text
        .split_once('.')
        .map_or(Some(text.as_str()), |(whole, fraction)| {
            fraction.bytes().all(|v| v == b'0').then_some(whole)
        })
        .ok_or("A valid integer is required.")?;
    let value = text
        .parse::<i64>()
        .map_err(|_| "A valid integer is required.")?;
    if value < 0 {
        return Err("Ensure this value is greater than or equal to 0.");
    }
    if value > i32::MAX as i64 {
        return Err("Ensure this value is less than or equal to 2147483647.");
    }
    Ok(value as i32)
}
fn options(detail: bool) -> Value {
    let fields = json!({"id":{"type":"integer","required":false,"read_only":true,"label":"ID"},"friendly_name":{"type":"string","required":false,"read_only":false,"label":"Friendly name","max_length":100},"device_id":{"type":"string","required":true,"read_only":false,"label":"Device id","max_length":32},"tuner_count":{"type":"integer","required":false,"read_only":false,"label":"Tuner count","min_value":0,"max_value":2147483647}});
    let mut value = json!({"name":if detail{"Hdhr Device Instance"}else{"Hdhr Device List"},"description":"Handles CRUD operations for HDHomeRun devices","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"],"actions":{}});
    value["actions"][if detail { "PUT" } else { "POST" }] = fields;
    value
}
