//! Original Dispatcharr's administrative ProgramData mutations.
//! Original Dispatcharr, Django and DRF authors retain full behavior credit.
use super::{
    json_negotiation, negotiate_literal_json_alias, program_id, programme_json_id,
    projected_fields, unavailable, BoundedJson, EpgProgramReads, OwnedJson,
};
use crate::{
    admin_writes::{body_object, character, parse_datetime},
    EpgReadResource, StoreError,
};
use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, Path, Request, State},
    http::{header, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgDatabaseError, PgConnection, Row};
use std::{convert::Infallible, net::SocketAddr, sync::Arc};

// The Source serializer has no writable EPG relation, but the model requires
// one. Preserve the failed INSERT and its sequence effect without ever
// supplying an inferred EPG or committing an orphan on a changed schema.
const INSERT_WITHOUT_EPG: &str = "
INSERT INTO epg_programdata(
    epg_id,start_time,end_time,title,sub_title,description,tvg_id,custom_properties
) VALUES (
    NULL,($1::jsonb->>'start_time')::timestamptz,($1::jsonb->>'end_time')::timestamptz,
    $1::jsonb->>'title',$1::jsonb->>'sub_title',$1::jsonb->>'description',$1::jsonb->>'tvg_id',
    '{}'::jsonb
)";

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
    json_negotiation(request.headers(), request.uri())?;
    change(state, peer, id, request, Some(true)).await
}

pub(super) async fn replace(
    State(state): State<EpgProgramReads>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    json_negotiation(request.headers(), request.uri())?;
    change(state, peer, id, request, Some(false)).await
}

pub(super) async fn delete(
    State(state): State<EpgProgramReads>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    json_negotiation(request.headers(), request.uri())?;
    change(state, peer, id, request, None).await
}

pub(super) async fn change_json(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let id = programme_json_id(request.uri().path())
        .ok_or(StoreError::NotFound)?
        .to_owned();
    negotiate_literal_json_alias(request.headers())?;
    let partial = match *request.method() {
        Method::PUT => Some(false),
        Method::PATCH => Some(true),
        Method::DELETE => None,
        _ => return Err(StoreError::NotFound),
    };
    change(state, peer, id, request, partial).await
}

pub(super) async fn create(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    json_negotiation(request.headers(), request.uri())?;
    create_with_json(state, peer, request).await
}

pub(super) async fn create_json(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    negotiate_literal_json_alias(request.headers())?;
    create_with_json(state, peer, request).await
}

async fn create_with_json(
    state: EpgProgramReads,
    peer: SocketAddr,
    request: Request,
) -> Result<Response, StoreError> {
    state
        .auth
        .account_user(peer, request.headers().clone(), true)
        .await?;
    let lease = state
        .workers
        .clone()
        .try_acquire_owned()
        .map_err(|_| unavailable())?;
    let input = tokio::time::timeout(
        state.limits.deadline,
        read_body(
            request,
            state
                .limits
                .row_bytes
                .saturating_add(65536)
                .min(2 * 1024 * 1024),
        ),
    )
    .await
    .map_err(|_| unavailable())??;
    let values = validate(&input, false)?;
    let writer = state.auth.task_sql_writer()?;
    tokio::spawn(async move {
        let (_writer, _lease) = (writer, lease);
        let mut tx = state.auth.store.pool().begin().await?;
        sqlx::query("SELECT set_config('statement_timeout',$1,true)")
            .bind(format!("{}ms", state.limits.deadline.as_millis()))
            .execute(&mut *tx)
            .await?;
        let attempted = sqlx::query(INSERT_WITHOUT_EPG)
            .bind(values)
            .execute(&mut *tx)
            .await;
        tx.rollback().await?;
        match attempted {
            Err(sqlx::Error::Database(error))
                if error
                    .try_downcast_ref::<PgDatabaseError>()
                    .is_some_and(|postgres| {
                        postgres.code() == "23502"
                            && postgres.table() == Some("epg_programdata")
                            && postgres.column() == Some("epg_id")
                    }) =>
            {
                Err(StoreError::UnhandledRestFault)
            }
            Err(error) => Err(error.into()),
            Ok(_) => Err(StoreError::UnhandledRestFault),
        }
    })
    .await
    .map_err(|_| unavailable())?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn programme_creation_preserves_required_fields_and_stock_fault_envelope() {
        use sha2::{Digest, Sha256};

        let err = validate(&Map::new(), false).unwrap_err().into_response();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        let entity = to_bytes(err.into_body(), 512).await.unwrap();
        assert_eq!(entity.len(), 117);
        assert_eq!(
            format!("{:x}", Sha256::digest(&entity)),
            "76773c94fb0f31bfa7343907c7e33493aac729b476f036d5cc7a59e7e0858fd8"
        );
        let request = json!({"epg":17,"epg_id":17,"title":"Owned Programme",
                             "start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"});
        let values = validate(request.as_object().unwrap(), false).unwrap();
        assert!(values.get("epg").is_none());
        assert!(values.get("epg_id").is_none());
        let fault = StoreError::UnhandledRestFault.into_response();
        assert_eq!(fault.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            fault.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let entity = to_bytes(fault.into_body(), 512).await.unwrap();
        assert_eq!(entity.len(), 145);
        assert_eq!(
            format!("{:x}", Sha256::digest(&entity)),
            "fd62a53afdd49595ccd111b6ac06466a1690e7c8da4aeb0e884b5e0f4e937624"
        );
    }

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
