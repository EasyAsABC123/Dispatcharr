//! Original Dispatcharr's administrative ProgramData detail mutations.
//! Original Dispatcharr, Django and DRF authors retain full behavior credit.
use super::{
    json_negotiation, program_id, projected_fields, unavailable, BoundedJson, EpgProgramReads,
    OwnedJson,
};
use crate::{
    admin_writes::{body_object, character, parse_datetime},
    EpgReadResource, StoreError,
};
use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, Path, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde_json::{json, Map, Value};
use sqlx::{PgConnection, Row};
use std::{convert::Infallible, net::SocketAddr, sync::Arc};

const UPDATE: &str = "
WITH changed AS (
    UPDATE epg_programdata
    SET start_time = COALESCE(($2::jsonb->>'start_time')::timestamptz,start_time),
        end_time = COALESCE(($2::jsonb->>'end_time')::timestamptz,end_time),
        title = COALESCE($2::jsonb->>'title',title),
        sub_title = CASE WHEN $2::jsonb ? 'sub_title' THEN $2::jsonb->>'sub_title' ELSE sub_title END,
        description = CASE WHEN $2::jsonb ? 'description' THEN $2::jsonb->>'description' ELSE description END,
        tvg_id = CASE WHEN $2::jsonb ? 'tvg_id' THEN $2::jsonb->>'tvg_id' ELSE tvg_id END
    WHERE id=$1
    RETURNING id,start_time,end_time,title,sub_title,description,tvg_id,custom_properties
)";

fn validate(input: &Map<String, Value>, partial: bool) -> Result<Value, StoreError> {
    let mut values = Map::new();
    let mut errors = Map::new();
    // ModelSerializer exposes only these writable fields. EPG identity and
    // custom metadata are omitted from the write, even if supplied by a client.
    for (name, maximum, required) in [
        ("start_time", None, true),
        ("end_time", None, true),
        ("title", Some(255), true),
        ("sub_title", None, false),
        ("description", None, false),
        ("tvg_id", Some(255), false),
    ] {
        let Some(value) = input.get(name) else {
            if required && !partial {
                errors.insert(name.into(), json!(["This field is required."]));
            }
            continue;
        };
        let parsed = match name {
            "start_time" | "end_time" => parse_datetime(name, value, false).map(|date| json!(date)),
            _ if !required && value.is_null() => Ok(Value::Null),
            _ => character(name, value, maximum, !required).map(Value::String),
        };
        match parsed {
            Ok(value) => {
                values.insert(name.into(), value);
            }
            Err(StoreError::InvalidInput(Value::Object(fields))) => errors.extend(fields),
            Err(error) => return Err(error),
        }
    }
    if errors.is_empty() {
        Ok(Value::Object(values))
    } else {
        Err(StoreError::InvalidInput(Value::Object(errors)))
    }
}

async fn read_body(request: Request, limit: usize) -> Result<Map<String, Value>, StoreError> {
    let media = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let raw = to_bytes(request.into_body(), limit)
        .await
        .map_err(|_| StoreError::StatusBody(413, json!({"detail":"Request body too large."})))?;
    if raw.is_empty() {
        return Ok(Map::new());
    }
    let kind = media.split(';').next().unwrap_or_default().trim();
    match kind {
        "application/json" => {
            let value = serde_json::from_slice::<Value>(&raw).map_err(|error| {
                StoreError::InvalidInput(json!({"detail":format!(
                    "JSON parse error - {}", crate::source_json_error(&raw, error)
                )}))
            })?;
            body_object(Ok(Json(value)))
        }
        "application/x-www-form-urlencoded" => Ok(url::form_urlencoded::parse(&raw)
            .map(|(name, value)| (name.into_owned(), Value::String(value.into_owned())))
            .collect()),
        "multipart/form-data" => multipart(&media, raw).await,
        _ => Err(StoreError::StatusBody(
            415,
            json!({"detail":format!("Unsupported media type \"{media}\" in request.")}),
        )),
    }
}

async fn multipart(media: &str, raw: Bytes) -> Result<Map<String, Value>, StoreError> {
    let invalid = || StoreError::InvalidInput(json!({"detail":"Multipart form parse error."}));
    let boundary = multer::parse_boundary(media).map_err(|_| invalid())?;
    let stream = futures_util::stream::once(async move { Ok::<_, Infallible>(raw) });
    let mut form = multer::Multipart::new(stream, boundary);
    let mut fields = Map::new();
    while let Some(field) = form.next_field().await.map_err(|_| invalid())? {
        let Some(name) = field.name().map(str::to_owned) else {
            continue;
        };
        let value = if field.file_name().is_some() {
            // Uploaded files cannot be interpreted as writable DRF text fields.
            Value::Object(Map::new())
        } else {
            Value::String(field.text().await.map_err(|_| invalid())?)
        };
        fields.insert(name, value);
    }
    Ok(fields)
}

async fn apply(
    connection: &mut PgConnection,
    state: &EpgProgramReads,
    id: i64,
    mutation: Option<Result<Value, StoreError>>,
) -> Result<Option<Vec<u8>>, StoreError> {
    let Some(mutation) = mutation else {
        let result = sqlx::query("DELETE FROM epg_programdata WHERE id=$1")
            .bind(id)
            .execute(&mut *connection)
            .await?;
        return if result.rows_affected() == 1 {
            Ok(None)
        } else {
            Err(EpgReadResource::Programs.missing())
        };
    };
    let values = match mutation {
        Ok(values) => values,
        Err(error) => {
            // Source's get_object runs before the serializer reads the body.
            let found: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM epg_programdata WHERE id=$1)")
                    .bind(id)
                    .fetch_one(&mut *connection)
                    .await?;
            return Err(if found {
                error
            } else {
                EpgReadResource::Programs.missing()
            });
        }
    };
    let fields = projected_fields("$3");
    let sql = format!("{UPDATE} SELECT {fields} FROM changed p");
    let row = sqlx::query(&sql)
        .bind(id)
        .bind(values)
        .bind(state.limits.row_bytes as i64)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| EpgReadResource::Programs.missing())?;
    if row.try_get::<bool, _>("oversized")? {
        return Err(unavailable());
    }
    let value = EpgReadResource::Programs.project(row, false, "")?;
    let mut out = BoundedJson::new(state.limits.response_bytes);
    serde_json::to_writer(&mut out, &value).map_err(|_| unavailable())?;
    Ok(Some(out.into_data()))
}

async fn change(
    state: EpgProgramReads,
    peer: SocketAddr,
    raw_id: String,
    request: Request,
    partial: Option<bool>,
) -> Result<Response, StoreError> {
    json_negotiation(request.headers(), request.uri())?;
    state
        .auth
        .account_user(peer, request.headers().clone(), true)
        .await?;
    let id = program_id(&raw_id)?;
    let lease = Arc::new(
        state
            .workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?,
    );
    let mutation = if let Some(partial) = partial {
        let body_limit = state
            .limits
            .row_bytes
            .saturating_add(65536)
            .min(2 * 1024 * 1024);
        let input = tokio::time::timeout(state.limits.deadline, read_body(request, body_limit))
            .await
            .map_err(|_| unavailable())?;
        Some(input.and_then(|input| validate(&input, partial)))
    } else {
        None
    };
    let writer = state.auth.task_sql_writer()?;
    // After dispatch, the physical transaction and permits settle even when
    // the HTTP caller closes. A response timeout cannot report an uncommitted
    // result that later writes successfully in the background.
    tokio::spawn(async move {
        let _writer = writer;
        let mut tx = state.auth.store.pool().begin().await?;
        sqlx::query("SELECT set_config('statement_timeout',$1,true)")
            .bind(format!("{}ms", state.limits.deadline.as_millis()))
            .execute(&mut *tx)
            .await?;
        let body = match apply(&mut tx, &state, id, mutation).await {
            Ok(body) => {
                tx.commit().await?;
                body
            }
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        Ok(match body {
            Some(data) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(Bytes::from_owner(OwnedJson {
                    data,
                    _lease: lease,
                })))
                .expect("static programme headers"),
            None => StatusCode::NO_CONTENT.into_response(),
        })
    })
    .await
    .map_err(|_| unavailable())?
}

pub(super) async fn patch(
    State(state): State<EpgProgramReads>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    change(state, peer, id, request, Some(true)).await
}

pub(super) async fn replace(
    State(state): State<EpgProgramReads>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    change(state, peer, id, request, Some(false)).await
}

pub(super) async fn delete(
    State(state): State<EpgProgramReads>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    change(state, peer, id, request, None).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_only_writes_supplied_serializer_fields() {
        let posted = json!({"title":"  News  ", "description":null,"tvg_id":17,
            "epg":4,"program_id":"ignored","custom_properties":{"season":1},"season":5});
        let parsed = validate(posted.as_object().unwrap(), true).unwrap();
        assert_eq!(
            parsed,
            json!({"title":"News","description":null,"tvg_id":"17"})
        );
    }

    #[test]
    fn put_requires_mandatory_fields_and_reports_each_error() {
        let posted = json!({"title":" ","start_time":null,"tvg_id":true});
        let StoreError::InvalidInput(errors) =
            validate(posted.as_object().unwrap(), false).unwrap_err()
        else {
            panic!("expected DRF validation response")
        };
        assert_eq!(
            errors,
            json!({
                "title":["This field may not be blank."],
                "start_time":["This field may not be null."],
                "end_time":["This field is required."],
                "tvg_id":["Not a valid string."]
            })
        );
    }

    #[tokio::test]
    async fn form_and_multipart_use_the_last_value_like_source_querydict() {
        let form = axum::http::Request::builder()
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("title=Old&title=New+Name&description="))
            .unwrap();
        assert_eq!(
            read_body(form, 512).await.unwrap(),
            json!({"title":"New Name","description":""})
                .as_object()
                .unwrap()
                .clone()
        );
        let form = axum::http::Request::builder()
            .header(header::CONTENT_TYPE, "multipart/form-data; boundary=owned")
            .body(Body::from(concat!(
                "--owned\r\nContent-Disposition: form-data; name=\"title\"\r\n\r\nOld\r\n",
                "--owned\r\nContent-Disposition: form-data; name=\"title\"\r\n\r\nNew\r\n",
                "--owned--\r\n"
            )))
            .unwrap();
        let parsed = read_body(form, 512).await.unwrap();
        assert_eq!(parsed["title"], json!("New"));
    }
}
