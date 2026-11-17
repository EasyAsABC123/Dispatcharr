//! Nested profile PUT/PATCH from Dispatcharr's original view, serializer and
//! model.save(). Original authors retain full application/algorithm credit.
//! POST/DELETE remain separate migration domains; these writes allocate no IDs.
use crate::{
    admin_writes::{body_object, boolean, character, integer, parse_datetime},
    xc_account_tasks::SavedProfile,
    AuthService, StoreError,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::{net::SocketAddr, sync::Arc};
type Body = Result<Json<Value>, JsonRejection>;

fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No M3UAccountProfile matches the given query."}),
    )
}
fn key(raw: String) -> Result<i64, StoreError> {
    raw.parse().map_err(|_| StoreError::NotFound)
}

fn positive_integer(field: &str, value: &Value) -> Result<Value, StoreError> {
    let minimum = || {
        StoreError::InvalidInput(
            json!({field:["Ensure this value is greater than or equal to 0."]}),
        )
    };
    let number = integer(field, value).map_err(|error| match error {
        StoreError::InvalidInput(ref fields)
            if fields[field]
                == json!(["Ensure this value is greater than or equal to -2147483648."]) =>
        {
            minimum()
        }
        error => error,
    })?;
    if number < 0 {
        Err(minimum())
    } else {
        Ok(json!(number))
    }
}

fn validate(input: Map<String, Value>, old: &Value, partial: bool) -> Result<Value, StoreError> {
    let mut supplied = Map::new();
    let mut errors = Map::new();
    // Serializer field order matters for validation; read-only id/account and
    // unknown properties are ignored, as in DRF ModelSerializer.
    for field in [
        "name",
        "max_streams",
        "is_active",
        "is_default",
        "current_viewers",
        "search_pattern",
        "replace_pattern",
        "custom_properties",
        "exp_date",
    ] {
        let Some(value) = input.get(field) else {
            if field == "name" && !partial {
                errors.insert(field.into(), json!(["This field is required."]));
            }
            continue;
        };
        let result = match field {
            "name" | "search_pattern" | "replace_pattern" => {
                character(field, value, Some(255), field != "name").map(Value::String)
            }
            "is_active" | "is_default" => boolean(field, value).map(Value::Bool),
            "max_streams" | "current_viewers" => positive_integer(field, value),
            "exp_date" => parse_datetime(field, value, true).map(|date| json!(date)),
            _ => Ok(value.clone()),
        };
        match result {
            Ok(value) => {
                supplied.insert(field.into(), value);
            }
            Err(StoreError::InvalidInput(Value::Object(fields))) => errors.extend(fields),
            Err(error) => return Err(error),
        }
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    if old["is_default"] == true {
        let forbidden = supplied
            .keys()
            .filter(|field| {
                !matches!(
                    field.as_str(),
                    "name"
                        | "custom_properties"
                        | "exp_date"
                        | "search_pattern"
                        | "replace_pattern"
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        if !forbidden.is_empty() {
            return Err(StoreError::InvalidInput(
                json!({"non_field_errors":[format!(
                "Default profiles can only modify name, notes, expiration, and URL patterns. Cannot modify: {}",
                forbidden.join(", "))]}),
            ));
        }
    } else {
        // The original validate() checks submitted data even for PATCH; it
        // deliberately does not fill missing patterns from the old instance.
        for field in ["search_pattern", "replace_pattern"] {
            if supplied
                .get(field)
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                return Err(StoreError::InvalidInput(
                    json!({field:["This field is required for non-default profiles."]}),
                ));
            }
        }
    }
    let mut saved = old.clone();
    for (field, value) in supplied {
        saved[&field] = value;
    }
    // Original model.save normalizes historical JSON strings and non-object
    // values, while preserving None as SQL NULL.
    if !saved["custom_properties"].is_null() && !saved["custom_properties"].is_object() {
        saved["custom_properties"] = saved["custom_properties"]
            .as_str()
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
    }
    Ok(saved)
}

fn model_expiration(
    saved: &Value,
    account_type: &Value,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, StoreError> {
    let current = parse_datetime("exp_date", &saved["exp_date"], true)?;
    if account_type != "XC" {
        return Ok(current);
    }
    // Original XC model calls user_info.get() before saving. Explicit
    // non-object user_info raises there; absent user_info is an empty dict.
    if saved["custom_properties"]
        .get("user_info")
        .is_some_and(|value| !value.is_object())
    {
        return Err(StoreError::UnhandledRestFault);
    }
    Ok(
        crate::xc_account_provider::expiration(
            &saved["custom_properties"]["user_info"]["exp_date"],
        )
        .or(current),
    )
}

async fn change(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    raw: (String, String),
    body: Body,
    partial: bool,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let (parent, id) = (key(raw.0)?, key(raw.1)?);
    // Use the existing production callback and one borrowed Store writer.
    // No helper implementation or second notification pool is introduced.
    let signal = auth
        .profile_save_signal
        .clone()
        .ok_or(StoreError::Unsupported(
            "Profile post-save notification transport is unavailable.",
        ))?;
    let owner: Arc<dyn Send + Sync> = Arc::new(auth.clone());
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    // The same four admitted Store writers bound these complete operations.
    // A disconnected HTTP waiter must not leave an already saved profile
    // without its original post_save effects or physical session close.
    let operation = tokio::spawn(async move {
        let outcome: Result<Json<Value>, StoreError> = async {
        let row = session.run(move |connection| Box::pin(async move {
            sqlx::query("SELECT to_jsonb(p) AS raw,to_jsonb(a) AS account FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE p.id=$1 AND p.m3u_account_id=$2")
                .bind(id).bind(parent).fetch_optional(connection).await
        })).await??.ok_or_else(missing)?;
        let old: Value = row.try_get("raw")?;
        let account: Value = row.try_get("account")?;
        // Resolve the scoped object before parsing/validating the request body.
        let mut saved = validate(body_object(body)?, &old, partial)?;
        let expiration = model_expiration(&saved, &account["account_type"])?;
        saved["exp_date"] = json!(expiration);
        signal.guard(session.clone(), id, owner.clone()).await?;
        let properties = if saved["custom_properties"].is_null() { None }
            else { Some(sqlx::types::Json(saved["custom_properties"].clone())) };
        let name = saved["name"].as_str().expect("validated model name").to_owned();
        let profile_name = name.clone();
        let max_streams = saved["max_streams"].as_i64().expect("model positive integer") as i32;
        let current_viewers = saved["current_viewers"].as_i64().expect("model positive integer") as i32;
        let active = saved["is_active"].as_bool().expect("model boolean");
        let default = saved["is_default"].as_bool().expect("model boolean");
        let search = saved["search_pattern"].as_str().expect("model pattern").to_owned();
        let replace = saved["replace_pattern"].as_str().expect("model pattern").to_owned();
        let after: Value = session.run(move |connection| Box::pin(async move {
            sqlx::query_scalar("UPDATE m3u_m3uaccountprofile SET name=$3,max_streams=$4,is_active=$5,is_default=$6,current_viewers=$7,search_pattern=$8,replace_pattern=$9,custom_properties=$10,exp_date=$11 WHERE id=$1 AND m3u_account_id=$2 RETURNING to_jsonb(m3u_m3uaccountprofile)")
                .bind(id).bind(parent).bind(name).bind(max_streams).bind(active).bind(default)
                .bind(current_viewers).bind(search).bind(replace).bind(properties).bind(expiration)
                .fetch_optional(connection).await
        })).await??.ok_or_else(missing)?;
        signal.after_save(session.clone(), SavedProfile {
            id, account_id: parent, name: profile_name,
            account_name: account["name"].as_str().unwrap_or_default().to_owned(), expiration,
        }, owner).await?;
        Ok(Json(crate::provider_reads::profile(&after, &account)))
    }.await;
        let closed = session.close().await;
        let value = outcome?;
        closed?;
        Ok(value)
    });
    operation.await.map_err(|_| {
        StoreError::StatusBody(503, json!({"detail":"Profile update processing failed."}))
    })?
}

pub(crate) async fn put(
    State(auth): State<AuthService>,
    Path(raw): Path<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change(auth, peer, headers, raw, body, false).await
}
pub(crate) async fn patch(
    State(auth): State<AuthService>,
    Path(raw): Path<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change(auth, peer, headers, raw, body, true).await
}

#[cfg(test)]
mod tests {
    use super::*;
    fn old() -> Value {
        json!({"id":6,"m3u_account_id":5,"name":"Default","is_default":true,"max_streams":1,
            "current_viewers":0,"is_active":true,"search_pattern":"","replace_pattern":"",
            "custom_properties":{},"exp_date":null})
    }
    #[test]
    fn captured_default_put_changes_patterns_and_notes_without_replacing_capacity_or_identity() {
        let input = json!({"name":"Owned profile A","custom_properties":{"notes":"Owned credential lifecycle"},
            "search_pattern":"/live/([^/]+)/([^/]+)/","replace_pattern":"/live/alias-$1/alias-$2/",
            "id":999,"account":{"id":999},"unrecognized":"ignored"});
        let after = validate(input.as_object().unwrap().clone(), &old(), false).unwrap();
        for field in [
            "id",
            "m3u_account_id",
            "is_default",
            "max_streams",
            "current_viewers",
            "is_active",
            "exp_date",
        ] {
            assert_eq!(after[field], old()[field], "{field}");
        }
        assert_eq!(after["replace_pattern"], "/live/alias-$1/alias-$2/");
        assert_eq!(
            after["custom_properties"],
            json!({"notes":"Owned credential lifecycle"})
        );
    }
    #[test]
    fn default_capacity_presence_is_rejected_even_when_unchanged_and_put_requires_name() {
        let error = validate(
            json!({"max_streams":1}).as_object().unwrap().clone(),
            &old(),
            true,
        )
        .unwrap_err();
        assert!(
            matches!(error, StoreError::InvalidInput(value) if value==json!({"non_field_errors":["Default profiles can only modify name, notes, expiration, and URL patterns. Cannot modify: max_streams"]}))
        );
        assert!(validate(Map::new(), &old(), false).is_err());
        assert!(validate(Map::new(), &old(), true).is_ok());
    }
    #[test]
    fn nondefault_patch_still_requires_submitted_patterns_and_normalizes_legacy_properties() {
        let mut old = old();
        old["is_default"] = json!(false);
        assert!(validate(Map::new(), &old, true).is_err());
        let after = validate(json!({"search_pattern":"[", "replace_pattern":"$1", "custom_properties":"{\"notes\":\"legacy\"}"}).as_object().unwrap().clone(), &old, true).unwrap();
        assert_eq!(after["search_pattern"], "["); // Source performs no regex validation.
        assert_eq!(after["custom_properties"], json!({"notes":"legacy"}));
    }
    #[test]
    fn xc_bad_user_info_fails_before_save_and_standard_preserves_manual_expiration() {
        for user_info in [Value::Null, json!([]), json!("invalid"), json!(false)] {
            let mut saved = old();
            saved["custom_properties"] = json!({"user_info":user_info});
            assert!(matches!(
                model_expiration(&saved, &json!("XC")),
                Err(StoreError::UnhandledRestFault)
            ));
            assert!(model_expiration(&saved, &json!("STD")).unwrap().is_none());
        }
        let mut saved = old();
        saved["exp_date"] = json!("2035-01-01T00:00:00Z");
        saved["custom_properties"] = json!({"user_info":{"exp_date":"0"}});
        assert_eq!(
            model_expiration(&saved, &json!("XC"))
                .unwrap()
                .unwrap()
                .timestamp(),
            0
        );
        assert_eq!(
            model_expiration(&saved, &json!("STD"))
                .unwrap()
                .unwrap()
                .timestamp(),
            2051222400
        );
        saved["custom_properties"] = json!({"user_info":{"exp_date":"invalid"}});
        assert_eq!(
            model_expiration(&saved, &json!("XC"))
                .unwrap()
                .unwrap()
                .timestamp(),
            2051222400
        );
    }
    #[test]
    fn positive_fields_keep_zero_lower_bound_even_outside_signed_i32() {
        for value in [json!(-1), json!(-2147483649i64), json!("-2147483649")] {
            let error = positive_integer("max_streams", &value).unwrap_err();
            assert!(
                matches!(error, StoreError::InvalidInput(value) if value==json!({"max_streams":["Ensure this value is greater than or equal to 0."]}))
            );
        }
        assert_eq!(
            positive_integer("max_streams", &json!(0)).unwrap(),
            json!(0)
        );
        assert!(positive_integer("max_streams", &json!(2147483648i64)).is_err());
    }
}
