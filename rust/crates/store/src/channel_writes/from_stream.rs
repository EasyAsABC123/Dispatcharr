//! Synchronous single Create Channel, matching the Original Streams form.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use super::{
    assign_numbers::lock_numbers, body_object, channel_defaults, create_rows, decimal, next_number,
    response, validation, Body,
};
use crate::{channels, json_compat, AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, OriginalUri, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, Postgres, Row, Transaction};
use std::{collections::HashMap, net::SocketAddr};

#[derive(Debug, PartialEq)]
enum Number {
    Auto,
    Highest,
    Specific(f64),
}

fn number(value: Option<&Value>, provider: Option<f64>) -> Result<Number, StoreError> {
    match value.filter(|value| !value.is_null()) {
        None => Ok(provider.map_or(Number::Auto, Number::Specific)),
        Some(value) if json_compat::python_equal(value, &json!(0)) => Ok(Number::Auto),
        Some(value) if json_compat::python_equal(value, &json!(-1)) => Ok(Number::Highest),
        Some(value) => decimal("channel_number", value, None)
            .map_err(|_| bad("channel_number must be an integer."))?
            .as_f64()
            .map(Number::Specific)
            .ok_or_else(|| bad("channel_number must be an integer.")),
    }
}

fn bad(message: &'static str) -> StoreError {
    StoreError::StatusBody(400, json!({"error": message}))
}

fn no_stream() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No Stream matches the given query."}))
}

async fn free_number(
    tx: &mut Transaction<'_, Postgres>,
    mode: Number,
) -> Result<Value, StoreError> {
    if mode == Number::Auto {
        return next_number(tx).await;
    }
    let start = match mode {
        Number::Specific(value) => value,
        Number::Highest => {
            let highest: Option<f64> = sqlx::query_scalar(
                "SELECT GREATEST((SELECT max(channel_number) FROM dispatcharr_channels_channel),
                   (SELECT max(channel_number) FROM dispatcharr_channels_channeloverride))",
            )
            .fetch_one(&mut **tx)
            .await?;
            highest.map_or(1.0, |value| value.trunc() + 1.0)
        }
        Number::Auto => unreachable!("automatic numbering returns above"),
    };
    if !start.is_finite() {
        return Err(bad("channel_number must be an integer."));
    }
    let reserved: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel WHERE channel_number=$1
                       UNION ALL
                       SELECT 1 FROM dispatcharr_channels_channeloverride WHERE channel_number=$1)",
    )
    .bind(start)
    .fetch_one(&mut **tx)
    .await?;
    if !reserved {
        return Ok(json!(start));
    }
    let number: f64 = sqlx::query_scalar(
        "WITH reserved AS MATERIALIZED (
             SELECT channel_number AS number FROM dispatcharr_channels_channel WHERE channel_number IS NOT NULL
             UNION
             SELECT channel_number FROM dispatcharr_channels_channeloverride WHERE channel_number IS NOT NULL
         )
         SELECT $1::double precision + steps.n
         FROM generate_series(0,(SELECT count(*) FROM reserved)) AS steps(n)
         LEFT JOIN reserved r ON r.number = $1 + steps.n
         WHERE r.number IS NULL
         ORDER BY steps.n LIMIT 1",
    )
    .bind(start)
    .fetch_one(&mut **tx)
    .await?;
    Ok(json!(number))
}

async fn logo(
    tx: &mut PgConnection,
    url: Option<String>,
    name: &str,
) -> Result<Option<i64>, StoreError> {
    let Some(url) = url.filter(|value| !value.is_empty() && value.len() <= 2000) else {
        return Ok(None);
    };
    if let Some(id) =
        sqlx::query_scalar("SELECT id::bigint FROM dispatcharr_channels_logo WHERE url=$1")
            .bind(&url)
            .fetch_optional(&mut *tx)
            .await?
    {
        return Ok(Some(id));
    }
    let name = name.chars().take(255).collect::<String>();
    let inserted: Option<i64> = sqlx::query_scalar(
        "INSERT INTO dispatcharr_channels_logo(name,url) VALUES($1,$2)
         ON CONFLICT(url) DO NOTHING RETURNING id::bigint",
    )
    .bind(name)
    .bind(&url)
    .fetch_optional(&mut *tx)
    .await?;
    match inserted {
        Some(id) => Ok(Some(id)),
        None => Ok(sqlx::query_scalar(
            "SELECT id::bigint FROM dispatcharr_channels_logo WHERE url=$1",
        )
        .bind(url)
        .fetch_optional(tx)
        .await?),
    }
}

pub(crate) async fn handle(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers.clone(), true).await?;
    let writer = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(writer.clone())?;
    let owner = auth.core_publication_owner(writer);
    let base = channels::base_url(&headers, &uri)?;
    let body = body_object(body)?;
    let raw = body
        .get("stream_id")
        .filter(|value| channels::truthy(Some(value)))
        .ok_or_else(|| bad("Missing stream_id"))?;
    let stream_id = json_compat::python_json_int(raw)
        .ok()
        .and_then(|value| value.as_i64())
        .ok_or_else(no_stream)?;
    tokio::spawn(async move {
        let mut connection = auth.store.pool().acquire().await?;
        let work = async {
            let mut tx = connection.begin().await?;
            lock_numbers(&mut tx).await?;
            let stream = sqlx::query(
                "SELECT name, stream_chno, channel_group_id::bigint, tvg_id, logo_url,
                    is_adult, is_radio, custom_properties->'tvc-guide-stationid' AS station
                 FROM dispatcharr_channels_stream WHERE id=$1 FOR KEY SHARE",
            )
            .bind(stream_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(no_stream)?;
            let title: String = stream.try_get("name")?;
            let tvg: Option<String> = stream.try_get("tvg_id")?;
            let channel_number = free_number(&mut tx, number(body.get("channel_number"), stream.try_get("stream_chno")?)?).await?;
            let supplied_name = body.get("name").filter(|value| !value.is_null());
            let name = supplied_name.cloned().unwrap_or_else(|| json!(title));
            let form = json!({
                "name": name, "channel_number": channel_number, "tvg_id": tvg,
                "tvc_guide_stationid": stream.try_get::<Option<Value>,_>("station")?,
                "is_radio": stream.try_get::<bool,_>("is_radio")?,
                "is_adult": stream.try_get::<bool,_>("is_adult")?,
            }).as_object().expect("typed single channel form").clone();
            let group = stream.try_get::<Option<i64>,_>("channel_group_id")?;
            let logo_name = if title.is_empty() { tvg.as_deref().unwrap_or_default() } else { &title };
            let logo_id = logo(&mut tx, stream.try_get("logo_url")?, logo_name).await?;
            // Django's first() orders a model without explicit ordering by its primary key.
            let epg: Option<i64> = sqlx::query_scalar(
                "SELECT id::bigint FROM epg_epgdata WHERE tvg_id IS NOT DISTINCT FROM $1 ORDER BY id LIMIT 1",
            )
            .bind(tvg.as_deref())
            .fetch_optional(&mut *tx)
            .await?;
            // Validate caller-facing scalar fields with the existing serializer logic.
            // Relationships were read or created on this connection, so the
            // write does not require any additional pool connection.
            let mut validated = validation(&auth, &form, false).await?;
            validated.streams = Some(vec![stream_id]);
            let mut data = channel_defaults();
            data.extend(std::mem::take(&mut validated.scalars));
            data.insert("channel_group_id".into(), json!(group));
            data.insert("logo_id".into(), json!(logo_id));
            data.insert("epg_data_id".into(), json!(epg));
            let (id, profile_error) = create_rows(
                &auth, &mut tx, &mut data, validated, body.get("channel_profile_ids"),
                epg.is_some(), owner.clone(),
            ).await?;
            tx.commit().await?;
            Ok::<_, StoreError>((id, profile_error, data))
        }.await;
        let ready = connection.ping().await;
        let closed = connection.close().await;
        let (id, profile_error, data) = work?;
        ready?;
        closed?;
        if let Some(error) = profile_error {
            return Ok((StatusCode::BAD_REQUEST, Json(error)).into_response());
        }
        if let Some(events) = &auth.event_sink {
            let payload = json!({"type":"update","data":{
                "type":"channels_created","count":1,"channel_id":id,
                "channel_name":data["name"],"channel_number":data["channel_number"],
            }});
            if events.send_retained("updates", payload, owner).await.is_err() {
                tracing::warn!("Single channel creation event publication failed");
            }
        }
        Ok((
            StatusCode::CREATED,
            [(header::ALLOW, "POST, OPTIONS")],
            Json(response(auth, user, params, base, id, &data, false).await?),
        ).into_response())
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_create_number_selection_follows_the_form_and_provider() {
        assert_eq!(number(None, None).unwrap(), Number::Auto);
        assert_eq!(
            number(Some(&Value::Null), Some(30.5)).unwrap(),
            Number::Specific(30.5)
        );
        assert_eq!(number(Some(&json!(0)), Some(30.5)).unwrap(), Number::Auto);
        assert_eq!(
            number(Some(&json!(-1)), Some(30.5)).unwrap(),
            Number::Highest
        );
        assert_eq!(
            number(Some(&json!(120.5)), Some(30.5)).unwrap(),
            Number::Specific(120.5)
        );
    }
}
