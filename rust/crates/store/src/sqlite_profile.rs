//! Private copied SQLite self-profile route. Original Dispatcharr authors retain credit.
use super::*;
use axum::{
    body::{Body, HttpBody},
    extract::{FromRequest, Request},
    http::{header, HeaderValue, Method},
};
use sqlx::{Row, SqliteConnection};

const USER: &str = "SELECT id,username,api_key,email,user_level,custom_properties,avatar_config,
    stream_limit,is_staff,is_superuser,last_login,date_joined,first_name,last_name
    FROM accounts_user WHERE id=?";
const PROFILES: &str = "SELECT channelprofile_id FROM accounts_user_channel_profiles
    WHERE user_id=? ORDER BY channelprofile_id LIMIT 65537";
const PATCH: &str = "UPDATE accounts_user SET
    first_name=COALESCE(?1,first_name),last_name=COALESCE(?2,last_name),email=COALESCE(?3,email),
    custom_properties=CASE WHEN ?4 THEN ?5 ELSE custom_properties END,
    password=COALESCE(?6,password) WHERE id=?7";
const PROPS: &str = "SELECT custom_properties FROM accounts_user WHERE id=?";
pub(crate) const STATEMENTS: [&str; 4] = [USER, PROFILES, PATCH, PROPS];

async fn load(db: &mut SqliteConnection, id: i64) -> Result<Value, StoreError> {
    let row = sqlx::query(USER)
        .bind(id)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(StoreError::NotFound)?;
    let profiles: Vec<i64> = sqlx::query_scalar(PROFILES).bind(id).fetch_all(db).await?;
    if profiles.len() > 65_536 {
        return Err(StoreError::Unsupported(
            "Copied SQLite profile exceeds the supported size",
        ));
    }
    let json_field = |name| -> Result<Value, StoreError> {
        Ok(row
            .try_get::<Option<sqlx::types::Json<Value>>, _>(name)?
            .map_or(Value::Null, |value| value.0))
    };
    Ok(json!({
        "id": row.try_get::<i64,_>("id")?, "username": row.try_get::<String,_>("username")?,
        "api_key": row.try_get::<Option<String>,_>("api_key")?, "email": row.try_get::<String,_>("email")?,
        "user_level": row.try_get::<i32,_>("user_level")?, "channel_profiles": profiles,
        "custom_properties": json_field("custom_properties")?, "avatar_config": json_field("avatar_config")?,
        "stream_limit": row.try_get::<i32,_>("stream_limit")?, "is_staff": row.try_get::<bool,_>("is_staff")?,
        "is_superuser": row.try_get::<bool,_>("is_superuser")?,
        "last_login": crate::accounts::datetime(row.try_get("last_login")?),
        "date_joined": crate::accounts::datetime(row.try_get("date_joined")?),
        "first_name": row.try_get::<String,_>("first_name")?, "last_name": row.try_get::<String,_>("last_name")?
    }))
}

async fn update(runtime: &CopiedSqliteRuntime, id: i64, body: Value) -> Result<Value, StoreError> {
    let fields = crate::account_writes::owner_patch(body)?;
    let password = match fields.get("password").and_then(Value::as_str) {
        Some(value) => Some(hash_for_workers(&runtime.password_workers, value.to_owned()).await?),
        None => None,
    };
    let mut tx = runtime
        .identity
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await?;
    let changes_properties = fields.contains_key("custom_properties");
    let properties = if changes_properties {
        let existing: Option<sqlx::types::Json<Value>> = sqlx::query_scalar(PROPS)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StoreError::NotFound)?;
        crate::account_writes::merged_properties(
            existing.map(|row| row.0),
            fields.get("custom_properties"),
        )
        .map(sqlx::types::Json)
    } else {
        None
    };
    let updated = sqlx::query(PATCH)
        .bind(fields.get("first_name").and_then(Value::as_str))
        .bind(fields.get("last_name").and_then(Value::as_str))
        .bind(fields.get("email").and_then(Value::as_str))
        .bind(changes_properties)
        .bind(properties)
        .bind(password)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    if updated.rows_affected() != 1 {
        return Err(StoreError::NotFound);
    }
    let result = load(&mut tx, id).await?;
    tx.commit().await?;
    Ok(result)
}

fn wire(mut response: Response, head: bool) -> Response {
    let length = response.body().size_hint().exact();
    response.headers_mut().insert(
        header::ALLOW,
        HeaderValue::from_static("GET, PATCH, HEAD, OPTIONS"),
    );
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept, origin"));
    if let Some(length) = length {
        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&length.to_string()).expect("bounded decimal response length"),
        );
    }
    if head {
        *response.body_mut() = Body::empty();
    }
    response
}

pub(super) async fn me(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    incoming: Request,
) -> Response {
    let method = incoming.method().clone();
    let result = async {
        let held = runtime.allocate()?;
        // DRF uses a separate metadata action, restricted to administrators.
        let admin = !matches!(method, Method::GET | Method::HEAD | Method::PATCH);
        let subject = held.bearer_account_user(peer, incoming.headers().clone(), admin).await?;
        let id = subject.user().id;
        match method {
            Method::GET | Method::HEAD => {
                let mut transaction = runtime.identity.pool().begin().await?;
                let result = load(&mut transaction, id).await?;
                transaction.commit().await?;
                Ok(Json(result).into_response())
            }
            Method::PATCH => {
                let body = Json::<Value>::from_request(incoming, &()).await;
                let Json(body) = body.map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?;
                if !body.is_object() {
                    return Ok(crate::account_writes::nonobject_profile_patch());
                }
                Ok(Json(update(&runtime, id, body).await?).into_response())
            }
            Method::OPTIONS => Ok(Json(json!({
                "name": "Me", "description": "Handles CRUD operations for Users",
                "renders": ["application/json","text/html"],
                "parses": ["application/json","application/x-www-form-urlencoded","multipart/form-data"]
            })).into_response()),
            _ => Err(StoreError::StatusBody(405, json!({"detail":format!("Method \"{method}\" not allowed.")}))),
        }
    }.await;
    wire(
        result.unwrap_or_else(IntoResponse::into_response),
        method == Method::HEAD,
    )
}
