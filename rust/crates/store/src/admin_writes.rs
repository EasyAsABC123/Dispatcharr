//! Administrative User/Group CRUD against the original Django tables.
//! Original application authorship belongs to Dispatcharr's contributors.
use crate::{
    account_writes::{merged_properties, plausible_email, validate_custom},
    accounts::{serialized_user, SERIALIZED_USER},
    AuthService, StoreError,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike, Utc};
use serde_json::{json, Map, Value};
use sqlx::{Postgres, Row, Transaction};
use std::net::SocketAddr;

type Body = Result<Json<Value>, JsonRejection>;
const USERNAME_MESSAGE:&str="Username may only contain letters, numbers, periods (.), underscores (_), at signs (@), and hyphens (-)";
const DATETIME_MESSAGE:&str="Datetime has wrong format. Use one of these formats instead: YYYY-MM-DDThh:mm[:ss[.uuuuuu]][+HH:MM|-HH:MM|Z].";

fn field_error(field: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({field:[message]}))
}
pub(crate) fn body_object(body: Body) -> Result<Map<String, Value>, StoreError> {
    let Json(value) =
        body.map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?;
    value.as_object().cloned().ok_or_else(||StoreError::InvalidInput(json!({"non_field_errors":[format!("Invalid data. Expected a dictionary, but got {}.",datatype(&value))]})))
}
fn datatype(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
        Value::Number(value) => {
            if value.is_f64() {
                "float"
            } else {
                "int"
            }
        }
    }
}
pub(crate) fn character(
    field: &str,
    value: &Value,
    max: Option<usize>,
    blank: bool,
) -> Result<String, StoreError> {
    if value.is_null() {
        return Err(field_error(field, "This field may not be null."));
    }
    let value = match value {
        Value::String(value) => value.trim().to_owned(),
        Value::Number(value) => {
            if value.is_f64() {
                crate::json_compat::float_string(
                    value
                        .as_f64()
                        .ok_or_else(|| field_error(field, "Not a valid string."))?,
                )
            } else {
                value.to_string()
            }
        }
        _ => return Err(field_error(field, "Not a valid string.")),
    };
    if value.is_empty() && !blank {
        return Err(field_error(field, "This field may not be blank."));
    }
    if let Some(max) = max {
        if value.chars().count() > max {
            return Err(field_error(
                field,
                &format!("Ensure this field has no more than {max} characters."),
            ));
        }
    }
    if value.contains('\0') {
        return Err(field_error(field, "Null characters are not allowed."));
    }
    Ok(value)
}
pub(crate) fn integer(field: &str, value: &Value) -> Result<i64, StoreError> {
    let value = crate::channel_memberships::integer(value).map_err(|error| match error {
        StoreError::InvalidInput(Value::Object(mut fields)) => {
            if let Some(message) = fields.remove("channel_id") {
                fields.insert(field.into(), message);
            }
            StoreError::InvalidInput(Value::Object(fields))
        }
        other => other,
    })?;
    let number = value.as_i64().ok_or_else(|| {
        field_error(
            field,
            if value.to_string().starts_with('-') {
                "Ensure this value is greater than or equal to -2147483648."
            } else {
                "Ensure this value is less than or equal to 2147483647."
            },
        )
    })?;
    if number > i32::MAX as i64 {
        return Err(field_error(
            field,
            "Ensure this value is less than or equal to 2147483647.",
        ));
    }
    if number < i32::MIN as i64 {
        return Err(field_error(
            field,
            "Ensure this value is greater than or equal to -2147483648.",
        ));
    }
    Ok(number)
}
pub(crate) fn boolean(field: &str, value: &Value) -> Result<bool, StoreError> {
    if let Some(value) = value.as_bool() {
        return Ok(value);
    }
    if crate::json_compat::python_equal(value, &json!(1)) {
        return Ok(true);
    }
    if crate::json_compat::python_equal(value, &json!(0)) {
        return Ok(false);
    }
    if let Some(value) = value.as_str() {
        match value.to_lowercase().as_str() {
            "t" | "y" | "yes" | "true" | "on" | "1" => return Ok(true),
            "f" | "n" | "no" | "false" | "off" | "0" => return Ok(false),
            _ => {}
        }
    }
    if value.is_null() {
        return Err(field_error(field, "This field may not be null."));
    }
    Err(field_error(field, "Must be a valid boolean."))
}
pub(crate) fn parse_datetime(
    field: &str,
    value: &Value,
    nullable: bool,
) -> Result<Option<DateTime<Utc>>, StoreError> {
    if value.is_null() {
        return if nullable {
            Ok(None)
        } else {
            Err(field_error(field, "This field may not be null."))
        };
    }
    let value = value
        .as_str()
        .ok_or_else(|| field_error(field, DATETIME_MESSAGE))?
        .trim();
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return checked_datetime(field, value.with_timezone(&Utc)).map(Some);
    }
    for pattern in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(value) = NaiveDateTime::parse_from_str(value, pattern) {
            return checked_datetime(field, value.and_utc()).map(Some);
        }
    }
    if let Ok(value) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return checked_datetime(field, value.and_hms_opt(0, 0, 0).unwrap().and_utc()).map(Some);
    }
    Err(field_error(field, DATETIME_MESSAGE))
}
fn checked_datetime(field: &str, value: DateTime<Utc>) -> Result<DateTime<Utc>, StoreError> {
    if !(1..=9999).contains(&value.year()) || value.nanosecond() >= 1_000_000_000 {
        return Err(field_error(field, DATETIME_MESSAGE));
    }
    Ok(value
        .with_nanosecond(value.nanosecond() / 1000 * 1000)
        .unwrap())
}
fn relation_ids(field: &str, value: &Value) -> Result<Vec<i64>, StoreError> {
    if value.is_null() {
        return Err(field_error(field, "This field may not be null."));
    }
    let values = value.as_array().ok_or_else(|| {
        field_error(
            field,
            &format!(
                "Expected a list of items but got type \"{}\".",
                datatype(value)
            ),
        )
    })?;
    let mut result = Vec::new();
    for value in values {
        if value.is_boolean() || value.is_array() || value.is_object() || value.is_null() {
            return Err(field_error(
                field,
                &format!(
                    "Incorrect type. Expected pk value, received {}.",
                    datatype(value)
                ),
            ));
        }
        let id = value
            .as_i64()
            .or_else(|| value.as_str()?.parse().ok())
            .ok_or_else(|| {
                field_error(
                    field,
                    &format!(
                        "Incorrect type. Expected pk value, received {}.",
                        datatype(value)
                    ),
                )
            })?;
        result.push(id);
    }
    Ok(result)
}

fn user_input(
    mut fields: Map<String, Value>,
    partial: bool,
    create: bool,
) -> Result<Map<String, Value>, StoreError> {
    fields.retain(|name, _| {
        matches!(
            name.as_str(),
            "username"
                | "password"
                | "email"
                | "first_name"
                | "last_name"
                | "user_level"
                | "stream_limit"
                | "custom_properties"
                | "avatar_config"
                | "is_staff"
                | "is_superuser"
                | "last_login"
                | "date_joined"
                | "channel_profiles"
        )
    });
    let mut errors = Map::new();
    if !partial && !fields.contains_key("username") {
        errors.insert("username".into(), json!(["This field is required."]));
    }
    for field in ["username", "password", "email", "first_name", "last_name"] {
        if let Some(value) = fields.get(field).cloned() {
            let max = match field {
                "username" | "first_name" | "last_name" => Some(150),
                "email" => Some(254),
                _ => None,
            };
            let validated=character(field,&value,max,!matches!(field,"username"|"password")).and_then(|value|{
                if field=="username" {
                    if !value.chars().all(|character|character.is_alphanumeric() || "@.+-_".contains(character)){return Err(field_error(field,"Enter a valid username. This value may contain only letters, numbers, and @/./+/-/_ characters."));}
                    if !value.chars().all(|character|character.is_ascii_alphanumeric() || "._@-".contains(character)){return Err(field_error(field,USERNAME_MESSAGE));}
                }
                if field=="email" && !value.is_empty() && !plausible_email(&value){return Err(field_error(field,"Enter a valid email address."));}
                Ok(Value::String(value))
            });
            store_validation(&mut fields, &mut errors, field, validated);
        }
    }
    for field in ["user_level", "stream_limit"] {
        if let Some(value) = fields.get(field).cloned() {
            store_validation(
                &mut fields,
                &mut errors,
                field,
                integer(field, &value).map(|value| json!(value)),
            );
        }
    }
    for field in ["is_staff", "is_superuser"] {
        if let Some(value) = fields.get(field).cloned() {
            store_validation(
                &mut fields,
                &mut errors,
                field,
                boolean(field, &value).map(Value::Bool),
            );
        }
    }
    for field in ["last_login", "date_joined"] {
        if let Some(value) = fields.get(field).cloned() {
            store_validation(
                &mut fields,
                &mut errors,
                field,
                parse_datetime(field, &value, field == "last_login")
                    .map(|value| value.map_or(Value::Null, |value| json!(value.to_rfc3339()))),
            );
        }
    }
    if let Some(value) = fields.get("custom_properties").cloned() {
        store_validation(
            &mut fields,
            &mut errors,
            "custom_properties",
            validate_custom(&value).map(Value::Object),
        );
    }
    if let Some(value) = fields.get("channel_profiles").cloned() {
        store_validation(
            &mut fields,
            &mut errors,
            "channel_profiles",
            relation_ids("channel_profiles", &value).map(|value| json!(value)),
        );
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    if create && !fields.contains_key("password") {
        // The baseline serializer marks this optional but its create() indexes
        // validated_data['password'], producing a server error. Fail without panic.
        return Err(StoreError::StatusBody(
            500,
            json!({"detail":"User creation requires a password."}),
        ));
    }
    Ok(fields)
}
fn store_validation(
    fields: &mut Map<String, Value>,
    errors: &mut Map<String, Value>,
    field: &str,
    result: Result<Value, StoreError>,
) {
    match result {
        Ok(value) => {
            fields.insert(field.into(), value);
        }
        Err(StoreError::InvalidInput(Value::Object(values))) => errors.extend(values),
        Err(_) => {
            errors.insert(field.into(), json!(["Invalid value."]));
        }
    }
}
fn field_string(fields: &Map<String, Value>, name: &str, fallback: String) -> String {
    fields
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or(fallback)
}
fn field_i32(fields: &Map<String, Value>, name: &str, fallback: i32) -> i32 {
    fields
        .get(name)
        .and_then(Value::as_i64)
        .map_or(fallback, |value| value as i32)
}
fn field_bool(fields: &Map<String, Value>, name: &str, fallback: bool) -> bool {
    fields
        .get(name)
        .and_then(Value::as_bool)
        .unwrap_or(fallback)
}

async fn check_user_unique(
    auth: &AuthService,
    fields: &Map<String, Value>,
    id: Option<i64>,
) -> Result<(), StoreError> {
    if let Some(username) = fields.get("username").and_then(Value::as_str) {
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts_user WHERE username=$1 AND ($2::bigint IS NULL OR id<>$2))").bind(username).bind(id).fetch_one(auth.store.pool()).await?;
        if exists {
            return Err(field_error(
                "username",
                "A user with that username already exists.",
            ));
        }
    }
    if let Some(value) = fields.get("channel_profiles") {
        check_related(
            auth,
            "channel_profiles",
            &relation_ids("channel_profiles", value)?,
            "dispatcharr_channels_channelprofile",
        )
        .await?;
    }
    Ok(())
}
async fn check_related(
    auth: &AuthService,
    field: &str,
    ids: &[i64],
    table: &str,
) -> Result<(), StoreError> {
    // table names are exclusively internal constants, never user input.
    let found: Vec<i64> =
        sqlx::query_scalar(&format!("SELECT id::bigint FROM {table} WHERE id=ANY($1)"))
            .bind(ids)
            .fetch_all(auth.store.pool())
            .await?;
    if let Some(missing) = ids.iter().find(|id| !found.contains(id)) {
        return Err(field_error(
            field,
            &format!("Invalid pk \"{missing}\" - object does not exist."),
        ));
    }
    Ok(())
}
fn map_unique(error: sqlx::Error, field: &str, message: &str) -> StoreError {
    if matches!(&error,sqlx::Error::Database(error) if error.code().as_deref()==Some("23505")) {
        field_error(field, message)
    } else {
        StoreError::Database(error)
    }
}
async fn set_user_profiles(
    transaction: &mut Transaction<'_, Postgres>,
    id: i64,
    values: &Value,
) -> Result<(), StoreError> {
    let mut ids = relation_ids("channel_profiles", values)?;
    ids.sort_unstable();
    ids.dedup();
    sqlx::query("DELETE FROM accounts_user_channel_profiles WHERE user_id=$1 AND NOT(channelprofile_id=ANY($2))").bind(id).bind(&ids).execute(&mut **transaction).await?;
    for profile in ids {
        sqlx::query("INSERT INTO accounts_user_channel_profiles(user_id,channelprofile_id) VALUES($1,$2) ON CONFLICT DO NOTHING").bind(id).bind(profile).execute(&mut **transaction).await?;
    }
    Ok(())
}
async fn transaction_user(
    transaction: &mut Transaction<'_, Postgres>,
    id: i64,
) -> Result<Value, StoreError> {
    let row = sqlx::query(&format!("{SERIALIZED_USER} WHERE u.id=$1"))
        .bind(id)
        .fetch_one(&mut **transaction)
        .await?;
    serialized_user(row)
}

pub(crate) async fn create_user(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let fields = user_input(body_object(body)?, false, true)?;
    check_user_unique(&auth, &fields, None).await?;
    let password = auth
        .hash_password(fields["password"].as_str().unwrap().to_owned())
        .await?;
    let joined = fields
        .get("date_joined")
        .map(|value| parse_datetime("date_joined", value, false))
        .transpose()?
        .flatten()
        .unwrap_or_else(Utc::now);
    let login = fields
        .get("last_login")
        .map(|value| parse_datetime("last_login", value, true))
        .transpose()?
        .flatten();
    let mut transaction = auth.store.pool().begin().await?;
    let avatar = fields.get("avatar_config").map_or_else(
        || Some(json!({})),
        |value| {
            if value.is_null() {
                None
            } else {
                Some(value.clone())
            }
        },
    );
    let id:i64=sqlx::query_scalar("INSERT INTO accounts_user(password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,avatar_config,stream_limit,api_key) VALUES($1,$2,$3,$4,$5,$6,$7,$8,TRUE,$9,$10,$11,$12,$13,NULL) RETURNING id::bigint")
        .bind(password).bind(login).bind(field_bool(&fields,"is_superuser",false)).bind(fields["username"].as_str().unwrap())
        .bind(field_string(&fields,"first_name",String::new())).bind(field_string(&fields,"last_name",String::new())).bind(field_string(&fields,"email",String::new())).bind(field_bool(&fields,"is_staff",false))
        .bind(joined).bind(field_i32(&fields,"user_level",0)).bind(fields.get("custom_properties").cloned().unwrap_or_else(||json!({})))
        .bind(avatar).bind(field_i32(&fields,"stream_limit",0))
        .fetch_one(&mut *transaction).await.map_err(|error|map_unique(error,"username","A user with that username already exists."))?;
    if let Some(values) = fields.get("channel_profiles") {
        set_user_profiles(&mut transaction, id, values).await?;
    }
    let value = transaction_user(&mut transaction, id).await?;
    transaction.commit().await?;
    Ok((StatusCode::CREATED, Json(value)).into_response())
}

async fn change_user(
    auth: AuthService,
    id: i64,
    fields: Map<String, Value>,
    partial: bool,
) -> Result<Json<Value>, StoreError> {
    // Match DRF's object lookup before validation so missing objects return 404.
    if auth.store.user_by_id(id).await?.is_none() {
        return Err(user_missing());
    }
    let fields = user_input(fields, partial, false)?;
    check_user_unique(&auth, &fields, Some(id)).await?;
    let password = if let Some(value) = fields.get("password") {
        Some(
            auth.hash_password(value.as_str().unwrap().to_owned())
                .await?,
        )
    } else {
        None
    };
    let mut transaction = auth.store.pool().begin().await?;
    let row=sqlx::query("SELECT username,first_name,last_name,email,user_level,stream_limit,is_staff,is_superuser,last_login,date_joined,custom_properties,avatar_config FROM accounts_user WHERE id=$1 FOR UPDATE").bind(id).fetch_optional(&mut *transaction).await?.ok_or_else(user_missing)?;
    let login = if let Some(value) = fields.get("last_login") {
        parse_datetime("last_login", value, true)?
    } else {
        row.try_get("last_login")?
    };
    let joined = if let Some(value) = fields.get("date_joined") {
        parse_datetime("date_joined", value, false)?.unwrap()
    } else {
        row.try_get::<DateTime<Utc>, _>("date_joined")?
    };
    let properties = merged_properties(
        row.try_get::<Option<Value>, _>("custom_properties")?,
        fields.get("custom_properties"),
    );
    let avatar = match fields.get("avatar_config") {
        Some(value) if value.is_null() => None,
        Some(value) => Some(value.clone()),
        None => row.try_get::<Option<Value>, _>("avatar_config")?,
    };
    sqlx::query("UPDATE accounts_user SET username=$2,first_name=$3,last_name=$4,email=$5,user_level=$6,stream_limit=$7,is_staff=$8,is_superuser=$9,last_login=$10,date_joined=$11,custom_properties=$12,avatar_config=$13,password=COALESCE($14,password) WHERE id=$1")
        .bind(id).bind(field_string(&fields,"username",row.try_get("username")?)).bind(field_string(&fields,"first_name",row.try_get("first_name")?)).bind(field_string(&fields,"last_name",row.try_get("last_name")?))
        .bind(field_string(&fields,"email",row.try_get("email")?)).bind(field_i32(&fields,"user_level",row.try_get("user_level")?)).bind(field_i32(&fields,"stream_limit",row.try_get("stream_limit")?))
        .bind(field_bool(&fields,"is_staff",row.try_get("is_staff")?)).bind(field_bool(&fields,"is_superuser",row.try_get("is_superuser")?)).bind(login).bind(joined).bind(properties).bind(avatar).bind(password)
        .execute(&mut *transaction).await.map_err(|error|map_unique(error,"username","A user with that username already exists."))?;
    if let Some(values) = fields.get("channel_profiles") {
        set_user_profiles(&mut transaction, id, values).await?;
    }
    let value = transaction_user(&mut transaction, id).await?;
    transaction.commit().await?;
    Ok(Json(value))
}
fn user_missing() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No User matches the given query."}))
}
fn group_missing() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No Group matches the given query."}))
}
pub(crate) async fn patch_user(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    change_user(auth, id, body_object(body)?, true).await
}
pub(crate) async fn put_user(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    change_user(auth, id, body_object(body)?, false).await
}
pub(crate) async fn delete_user(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let mut transaction = auth.store.pool().begin().await?;
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT id::bigint FROM accounts_user WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?;
    if exists.is_none() {
        return Err(user_missing());
    }
    // Django implements CASCADE in its collector, not as PostgreSQL ON DELETE.
    for table in [
        "accounts_user_channel_profiles",
        "accounts_user_groups",
        "accounts_user_user_permissions",
        "accounts_userappdata",
        "core_notificationdismissal",
        "django_admin_log",
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE user_id=$1"))
            .bind(id)
            .execute(&mut *transaction)
            .await?;
    }
    sqlx::query("DELETE FROM accounts_user WHERE id=$1")
        .bind(id)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

fn group_input(
    mut fields: Map<String, Value>,
    partial: bool,
) -> Result<Map<String, Value>, StoreError> {
    fields.retain(|name, _| matches!(name.as_str(), "name" | "permissions"));
    let mut errors = Map::new();
    for name in ["name", "permissions"] {
        if !partial && !fields.contains_key(name) {
            errors.insert(name.into(), json!(["This field is required."]));
        }
    }
    if let Some(value) = fields.get("name").cloned() {
        store_validation(
            &mut fields,
            &mut errors,
            "name",
            character("name", &value, Some(150), false).map(Value::String),
        );
    }
    if let Some(value) = fields.get("permissions").cloned() {
        store_validation(
            &mut fields,
            &mut errors,
            "permissions",
            relation_ids("permissions", &value).map(|values| json!(values)),
        );
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    Ok(fields)
}
async fn check_group(
    auth: &AuthService,
    fields: &Map<String, Value>,
    id: Option<i64>,
) -> Result<(), StoreError> {
    if let Some(name) = fields.get("name").and_then(Value::as_str) {
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM auth_group WHERE name=$1 AND ($2::bigint IS NULL OR id<>$2))").bind(name).bind(id).fetch_one(auth.store.pool()).await?;
        if exists {
            return Err(field_error("name", "group with this name already exists."));
        }
    }
    if let Some(value) = fields.get("permissions") {
        check_related(
            auth,
            "permissions",
            &relation_ids("permissions", value)?,
            "auth_permission",
        )
        .await?;
    }
    Ok(())
}
async fn set_group_permissions(
    transaction: &mut Transaction<'_, Postgres>,
    id: i64,
    values: &Value,
) -> Result<(), StoreError> {
    let mut ids = relation_ids("permissions", values)?;
    ids.sort_unstable();
    ids.dedup();
    sqlx::query(
        "DELETE FROM auth_group_permissions WHERE group_id=$1 AND NOT(permission_id=ANY($2))",
    )
    .bind(id)
    .bind(&ids)
    .execute(&mut **transaction)
    .await?;
    for permission in ids {
        sqlx::query("INSERT INTO auth_group_permissions(group_id,permission_id) VALUES($1,$2) ON CONFLICT DO NOTHING").bind(id).bind(permission).execute(&mut **transaction).await?;
    }
    Ok(())
}
pub(crate) async fn create_group(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let fields = group_input(body_object(body)?, false)?;
    check_group(&auth, &fields, None).await?;
    let mut transaction = auth.store.pool().begin().await?;
    let id: i64 =
        sqlx::query_scalar("INSERT INTO auth_group(name) VALUES($1) RETURNING id::bigint")
            .bind(fields["name"].as_str().unwrap())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| map_unique(error, "name", "group with this name already exists."))?;
    set_group_permissions(&mut transaction, id, &fields["permissions"]).await?;
    transaction.commit().await?;
    Ok((
        StatusCode::CREATED,
        Json(auth.store.serialized_groups(Some(id)).await?),
    )
        .into_response())
}
async fn change_group(
    auth: AuthService,
    id: i64,
    fields: Map<String, Value>,
    partial: bool,
) -> Result<Json<Value>, StoreError> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM auth_group WHERE id=$1)")
        .bind(id)
        .fetch_one(auth.store.pool())
        .await?;
    if !exists {
        return Err(group_missing());
    }
    let fields = group_input(fields, partial)?;
    check_group(&auth, &fields, Some(id)).await?;
    let mut transaction = auth.store.pool().begin().await?;
    let row = sqlx::query("SELECT name FROM auth_group WHERE id=$1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(group_missing)?;
    sqlx::query("UPDATE auth_group SET name=$2 WHERE id=$1")
        .bind(id)
        .bind(field_string(&fields, "name", row.try_get("name")?))
        .execute(&mut *transaction)
        .await
        .map_err(|error| map_unique(error, "name", "group with this name already exists."))?;
    if let Some(values) = fields.get("permissions") {
        set_group_permissions(&mut transaction, id, values).await?;
    }
    transaction.commit().await?;
    Ok(Json(auth.store.serialized_groups(Some(id)).await?))
}
pub(crate) async fn patch_group(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    change_group(auth, id, body_object(body)?, true).await
}
pub(crate) async fn put_group(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    change_group(auth, id, body_object(body)?, false).await
}
pub(crate) async fn delete_group(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let mut transaction = auth.store.pool().begin().await?;
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT id::bigint FROM auth_group WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?;
    if exists.is_none() {
        return Err(group_missing());
    }
    sqlx::query("DELETE FROM accounts_user_groups WHERE group_id=$1")
        .bind(id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM auth_group_permissions WHERE group_id=$1")
        .bind(id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM auth_group WHERE id=$1")
        .bind(id)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admin_fields_are_normalized_and_unknown_or_readonly_fields_ignored() {
        let fields=user_input(json!({"username":" rust-probe-new ","password":" pass ","is_staff":"yes","user_level":"1.0","api_key":"ignored","is_active":false,"custom_properties":null,"channel_profiles":["1",2]}).as_object().unwrap().clone(),false,true).unwrap();
        assert_eq!(fields["username"], json!("rust-probe-new"));
        assert_eq!(fields["password"], json!("pass"));
        assert_eq!(fields["is_staff"], json!(true));
        assert_eq!(fields["user_level"], json!(1));
        assert_eq!(fields["custom_properties"], json!({}));
        assert_eq!(fields["channel_profiles"], json!([1, 2]));
        assert!(!fields.contains_key("api_key"));
        assert!(!fields.contains_key("is_active"));
    }
    #[test]
    fn admin_validation_collects_required_and_invalid_fields() {
        let result = user_input(
            json!({"email":"bad","stream_limit":"not-a-number","is_superuser":null})
                .as_object()
                .unwrap()
                .clone(),
            false,
            false,
        );
        let Err(StoreError::InvalidInput(Value::Object(errors))) = result else {
            panic!("expected field errors")
        };
        for field in ["username", "email", "stream_limit", "is_superuser"] {
            assert!(errors.contains_key(field));
        }
        assert!(matches!(
            user_input(
                json!({"username":"rust-probe-new"})
                    .as_object()
                    .unwrap()
                    .clone(),
                false,
                true
            ),
            Err(StoreError::StatusBody(500, _))
        ));
    }
    #[test]
    fn admin_custom_properties_accept_valid_credentials_and_reject_bad_profile_allowlists() {
        assert!(validate_custom(
            &json!({"xc_password":"safe-user.password","allowed_m3u_profile_ids":[1,2]})
        )
        .is_ok());
        assert!(validate_custom(&json!({"xc_password":"bad space"})).is_err());
        for value in [json!([true]), json!([0]), json!("1")] {
            assert!(validate_custom(&json!({"allowed_m3u_profile_ids":value})).is_err());
        }
    }
    #[test]
    fn group_full_and_partial_requests_preserve_required_relation_contract() {
        assert!(group_input(json!({"name":"group"}).as_object().unwrap().clone(), false).is_err());
        assert!(group_input(
            json!({"permissions":[]}).as_object().unwrap().clone(),
            false
        )
        .is_err());
        assert!(group_input(
            json!({"name":" group ","permissions":[]})
                .as_object()
                .unwrap()
                .clone(),
            false
        )
        .is_ok());
        assert!(group_input(Map::new(), true).is_ok());
        assert!(relation_ids("permissions", &json!([true])).is_err());
    }
    #[test]
    fn timestamps_accept_source_common_iso_forms_and_nullable_last_login() {
        assert!(parse_datetime("last_login", &Value::Null, true)
            .unwrap()
            .is_none());
        assert!(parse_datetime("date_joined", &Value::Null, false).is_err());
        assert_eq!(
            parse_datetime("date_joined", &json!("2026-11-09T01:00:00+01:00"), false)
                .unwrap()
                .unwrap(),
            "2026-11-09T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert!(parse_datetime("date_joined", &json!("2026-11-09"), false).is_ok());
        assert!(parse_datetime("date_joined", &json!("0000-01-01T00:00:00Z"), false).is_err());
        assert!(parse_datetime("date_joined", &json!("2016-12-31T23:59:60Z"), false).is_err());
        assert_eq!(
            parse_datetime(
                "date_joined",
                &json!("1999-01-01T00:00:00.123456789Z"),
                false
            )
            .unwrap()
            .unwrap()
            .nanosecond(),
            123456000
        );
    }
}
