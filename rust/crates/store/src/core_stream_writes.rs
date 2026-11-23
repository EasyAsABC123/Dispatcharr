//! No-job StreamProfile writes derived from original Dispatcharr serializers.
//! All original authors retain application credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, boolean, character},
    channel_writes::foreign,
    AuthService, StoreError,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Map, Value};
use sqlx::{Postgres, Transaction};
use std::net::SocketAddr;
type Body = Result<Json<Value>, JsonRejection>;
const SELECT:&str="SELECT json_build_object('id',id,'name',name,'command',command,'parameters',parameters,'is_active',is_active,'user_agent',user_agent_id,'locked',locked) FROM core_streamprofile WHERE id=$1";
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No StreamProfile matches the given query."}),
    )
}
fn key(raw: String) -> Result<i64, StoreError> {
    crate::channel_memberships::primary_key(&Value::String(raw))
        .map_err(|_| StoreError::NotFound)?
        .ok_or_else(missing)
}
async fn validated(
    auth: &AuthService,
    body: Map<String, Value>,
    partial: bool,
) -> Result<Map<String, Value>, StoreError> {
    let mut out = Map::new();
    let mut errors = Map::new();
    for field in [
        "name",
        "command",
        "parameters",
        "is_active",
        "user_agent",
        "locked",
    ] {
        let result = if let Some(value) = body.get(field) {
            Some(match field {
                "name" => character(field, value, Some(255), false).map(Value::from),
                "command" => character(field, value, Some(255), true).map(Value::from),
                "parameters" => character(field, value, None, true).map(Value::from),
                "is_active" | "locked" => boolean(field, value).map(Value::from),
                "user_agent" => foreign(auth, field, value, "core_useragent", true).await,
                _ => unreachable!(),
            })
        } else if field == "name" && !partial {
            Some(Err(StoreError::InvalidInput(
                json!({"name":["This field is required."]}),
            )))
        } else {
            None
        };
        if let Some(result) = result {
            match result {
                Ok(value) => {
                    out.insert(field.into(), value);
                }
                Err(StoreError::InvalidInput(Value::Object(fields))) => errors.extend(fields),
                Err(other) => return Err(other),
            }
        }
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    Ok(out)
}
fn merge(old: Option<Value>, changes: Map<String, Value>) -> Result<Value, StoreError> {
    let mut value=old.clone().unwrap_or_else(||json!({"name":"","command":"","parameters":"","is_active":true,"user_agent":null,"locked":false}));
    for (field, change) in changes {
        value[field] = change;
    }
    if let Some(old) = old.filter(|old| old["locked"] == true) {
        // Model._meta.fields ordering determines the first locked-field error.
        for field in ["name", "command", "parameters", "locked", "is_active"] {
            if old[field] != value[field] {
                return Err(StoreError::InvalidInput(json!([format!(
                    "Cannot modify {field} on a protected profile."
                )])));
            }
        }
    }
    Ok(value)
}
async fn save(
    tx: &mut Transaction<'_, Postgres>,
    id: Option<i64>,
    value: &Value,
) -> Result<Value, StoreError> {
    let id: i64 = if let Some(id) = id {
        sqlx::query_scalar("UPDATE core_streamprofile SET name=$2,command=$3,parameters=$4,is_active=$5,user_agent_id=$6,locked=$7 WHERE id=$1 RETURNING id::bigint").bind(id).bind(value["name"].as_str().expect("validated name")).bind(value["command"].as_str().expect("validated command")).bind(value["parameters"].as_str().expect("validated parameters")).bind(value["is_active"].as_bool().expect("validated boolean")).bind(value["user_agent"].as_i64()).bind(value["locked"].as_bool().expect("validated boolean")).fetch_one(&mut **tx).await?
    } else {
        sqlx::query_scalar("INSERT INTO core_streamprofile(name,command,parameters,is_active,user_agent_id,locked) VALUES($1,$2,$3,$4,$5,$6) RETURNING id::bigint").bind(value["name"].as_str().expect("validated name")).bind(value["command"].as_str().expect("validated command")).bind(value["parameters"].as_str().expect("validated parameters")).bind(value["is_active"].as_bool().expect("validated boolean")).bind(value["user_agent"].as_i64()).bind(value["locked"].as_bool().expect("validated boolean")).fetch_one(&mut **tx).await?
    };
    Ok(sqlx::query_scalar(SELECT)
        .bind(id)
        .fetch_one(&mut **tx)
        .await?)
}
pub(crate) async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<Value>), StoreError> {
    auth.account_user(peer, headers, true).await?;
    let value = merge(None, validated(&auth, body_object(body)?, false).await?)?;
    let mut tx = auth.store.pool().begin().await?;
    let value = save(&mut tx, None, &value).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(value)))
}
enum ProfileUpdate {
    Json(Body),
    Multipart(Request),
}

async fn multipart_fields(request: Request) -> Result<Map<String, Value>, StoreError> {
    let (mut fields, _) = crate::logo_writes::mutation_fields(request).await?;
    for field in ["is_active", "locked"] {
        if fields.get(field).and_then(Value::as_str) == Some("") {
            fields.shift_remove(field);
        } else {
            fields.entry(field).or_insert(Value::Bool(false));
        }
    }
    Ok(fields)
}

async fn change(
    auth: &AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    raw: String,
    body: ProfileUpdate,
    partial: bool,
    html_path: Option<&str>,
) -> Result<(Value, Option<Response>), StoreError> {
    let browse = html_path.map(|path| (path, headers.clone()));
    let user = auth.account_user(peer, headers, true).await?;
    let id = key(raw)?;
    // Resolve before validation, as DRF UpdateModelMixin does. Validate FK
    // fields before taking a transaction connection, so bounded pools cannot
    // deadlock when all concurrent updates need a second connection.
    let _: Value = sqlx::query_scalar(SELECT)
        .bind(id)
        .fetch_optional(auth.store.pool())
        .await?
        .ok_or_else(missing)?;
    let fields = match body {
        ProfileUpdate::Json(body) => body_object(body)?,
        ProfileUpdate::Multipart(request) => multipart_fields(request).await?,
    };
    let changes = validated(auth, fields, partial).await?;
    let mut tx = auth.store.pool().begin().await?;
    let old: Value = sqlx::query_scalar(&format!("{SELECT} FOR UPDATE"))
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(missing)?;
    let value = merge(Some(old), changes)?;
    let mut value = save(&mut tx, Some(id), &value).await?;
    // A rejected choice lookup or bounded render must roll the profile back.
    let html = if let Some((path, headers)) = browse {
        Some(
            crate::core_reads::profile_updated_html(
                &mut *tx,
                path,
                &headers,
                &user.username,
                &mut value,
            )
            .await?,
        )
    } else {
        None
    };
    tx.commit().await?;
    Ok((value, html))
}

pub(crate) async fn put_multipart(
    auth: &AuthService,
    peer: SocketAddr,
    id: String,
    request: Request,
    html_path: Option<&str>,
) -> Result<Response, StoreError> {
    let (value, html) = change(
        auth,
        peer,
        request.headers().clone(),
        id,
        ProfileUpdate::Multipart(request),
        false,
        html_path,
    )
    .await?;
    Ok(html.unwrap_or_else(|| Json(value).into_response()))
}

pub(crate) async fn patch(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change(
        &auth,
        peer,
        headers,
        raw,
        ProfileUpdate::Json(body),
        true,
        None,
    )
    .await
    .map(|(value, _)| Json(value))
}
pub(crate) async fn put(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change(
        &auth,
        peer,
        headers,
        raw,
        ProfileUpdate::Json(body),
        false,
        None,
    )
    .await
    .map(|(value, _)| Json(value))
}
pub(crate) async fn delete(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = key(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    let locked: bool =
        sqlx::query_scalar("SELECT locked FROM core_streamprofile WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(missing)?;
    if locked {
        return Err(StoreError::InvalidInput(json!([
            "This profile is locked and cannot be deleted."
        ])));
    }
    // Django's collector SET_NULL never calls related models' save/signals.
    for table in [
        "dispatcharr_channels_stream",
        "dispatcharr_channels_channel",
        "dispatcharr_channels_channeloverride",
        "m3u_m3uaccount",
    ] {
        sqlx::query(&format!(
            "UPDATE {table} SET stream_profile_id=NULL WHERE stream_profile_id=$1"
        ))
        .bind(id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("DELETE FROM core_streamprofile WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn profile_put_multipart_uses_shared_parser_and_full_form_checkbox_rules() {
        let body = concat!(
            "--fixture\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\nProfile\r\n",
            "--fixture\r\nContent-Disposition: form-data; name=\"command\"\r\n\r\n\r\n",
            "--fixture\r\nContent-Disposition: form-data; name=\"user_agent\"\r\n\r\n5\r\n",
            "--fixture--\r\n"
        );
        let request = |content_type| {
            Request::builder()
                .header(axum::http::header::CONTENT_TYPE, content_type)
                .body(axum::body::Body::from(body))
                .unwrap()
        };
        let fields = multipart_fields(request("multipart/form-data; boundary=fixture"))
            .await
            .unwrap();
        assert_eq!(fields["name"], "Profile");
        assert_eq!(fields["command"], "");
        assert_eq!(fields["user_agent"], "5");
        assert_eq!(fields["is_active"], false);
        assert_eq!(fields["locked"], false);
        assert!(matches!(
            multipart_fields(request("multipart/form-data")).await,
            Err(StoreError::InvalidInput(_)) | Err(StoreError::StatusBody(400, _))
        ));
    }

    #[test]
    fn locked_profile_only_accepts_user_agent() {
        let old = json!({"id":12,"name":"locked","command":"cmd","parameters":"p","is_active":true,"locked":true,"user_agent":null});
        let mut changes = Map::new();
        changes.insert("user_agent".into(), json!(3));
        assert_eq!(merge(Some(old.clone()), changes).unwrap()["user_agent"], 3);
        let mut changes = Map::new();
        changes.insert("locked".into(), json!(false));
        changes.insert("is_active".into(), json!(false));
        let Err(StoreError::InvalidInput(value)) = merge(Some(old), changes) else {
            panic!("must reject locked change")
        };
        assert_eq!(
            value,
            json!(["Cannot modify locked on a protected profile."])
        );
    }
}
