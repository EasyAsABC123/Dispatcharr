//! Stock bounded remote XMLTV form and its pinned Source post-save ordering.
use super::{
    character, field, no_custom_triggers, nullable_character, project, schedule_refresh, validate,
    AuthService, Owner, StoreError,
};
use crate::{
    provider_account_writes::ordinary_provider_http_url, sd_source_http::InitialXmltvRefresh,
    sd_source_writes::close_result,
};
use chrono::Utc;
use serde_json::{json, Value};

struct Input {
    name: String,
    url: String,
    username: Option<String>,
    password: Option<String>,
    active: bool,
    interval_hours: i32,
}

fn unsupported() -> StoreError {
    StoreError::Unsupported("This remote XMLTV creation variant has not been migrated")
}

fn form(value: &Value) -> Result<Input, StoreError> {
    let mut input = value.as_object().ok_or_else(unsupported)?.clone();
    let url = character(
        "url",
        input.get("url").ok_or_else(unsupported)?,
        Some(1000),
        false,
    )?;
    // Preserve the actual Source text. This guarded subset cannot silently
    // accept a different scheme, credentials, fragments or parser rewriting.
    if !url.is_ascii()
        || !(url.starts_with("http://") || url.starts_with("https://"))
        || url.contains('\\')
        || url
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        || !ordinary_provider_http_url(&url)
    {
        return Err(unsupported());
    }
    input.insert("url".into(), json!(&url));

    let username = input
        .get("username")
        .map(|value| nullable_character("username", value, Some(255)))
        .transpose()?
        .unwrap_or(Value::Null);
    let password = input
        .get("password")
        .map(|value| nullable_character("password", value, Some(255)))
        .transpose()?
        .unwrap_or(Value::Null);
    for credential in [&username, &password] {
        if !credential.is_null() && credential.as_str() != Some("") {
            return Err(unsupported());
        }
    }
    if input
        .get("custom_properties")
        .is_some_and(|value| value != &json!({}))
    {
        return Err(unsupported());
    }
    // The update validator compares against an existing row. A new Source
    // row starts idle even when its initial is_active value is false.
    let initial_active = input
        .get("is_active")
        .and_then(|value| super::boolean("is_active", value).ok())
        .unwrap_or(true);
    let defaults = json!({"name":"","source_type":"xmltv","url":url,
        "username":username,"password":password,"is_active":initial_active,"file_path":null,
        "refresh_interval":0,"priority":0,"status":"idle","last_message":null,
        "custom_properties":{},"updated_at":null});
    let checked = validate(input, &defaults, "", false)?;
    let saved = checked.saved;
    let interval_hours = saved["refresh_interval"]
        .as_i64()
        .and_then(|hours| i32::try_from(hours).ok())
        .filter(|hours| *hours >= 0)
        .ok_or_else(unsupported)?;
    if !checked.cron.is_empty()
        || !saved["file_path"].is_null()
        || saved["priority"] != 0
        || saved["status"] != "idle"
        || !saved["last_message"].is_null()
        || !saved["updated_at"].is_null()
    {
        return Err(unsupported());
    }
    Ok(Input {
        name: saved["name"].as_str().ok_or_else(unsupported)?.into(),
        url,
        username: username.as_str().map(str::to_owned),
        password: password.as_str().map(str::to_owned),
        active: saved["is_active"].as_bool().ok_or_else(unsupported)?,
        interval_hours,
    })
}

pub(crate) async fn create_xmltv(
    auth: AuthService,
    value: &Value,
    owner: Owner,
    publisher: &dyn InitialXmltvRefresh,
) -> Result<Value, StoreError> {
    let input = form(value)?;
    let active = input.active;
    let interval_hours = input.interval_hours;
    let result = async {
        let session = auth.borrow_task_sql_session(owner.clone()).await?;
        let created = async {
            let id = session.run(move |connection| Box::pin(async move {
                no_custom_triggers(connection).await?;
                let duplicate: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE name=$1)",
                ).bind(&input.name).fetch_one(&mut *connection).await?;
                if duplicate {
                    return Err(field("name", "epg source with this name already exists."));
                }
                let id: i64 = sqlx::query_scalar(
                    "INSERT INTO epg_epgsource(name,source_type,url,username,password,is_active,file_path,extracted_file_path,refresh_interval,refresh_task_id,created_at,updated_at,status,last_message,custom_properties,priority)
                    VALUES($1,'xmltv',$2,$3,$4,$5,NULL,NULL,$6,NULL,$7,NULL,'idle',NULL,'{}'::jsonb,0) RETURNING id::bigint",
                ).bind(input.name).bind(input.url).bind(input.username).bind(input.password)
                    .bind(active).bind(interval_hours).bind(Utc::now())
                    .fetch_one(connection).await?;
                Ok::<_, StoreError>(id)
            })).await??;
            // Source serializes the created object, even when the newly
            // published worker updates status before the response is sent.
            let reply = project(&session, id, Some(String::new())).await?.0;
            Ok::<_, StoreError>((id, reply))
        }.await;
        let (id, mut reply) = close_result(&session, created).await?;

        // The row was committed before post_save. The first receiver submits
        // the active root; only the next receiver writes and links the schedule.
        // A zero interval disables recurrence but still runs this first import.
        // An unconfirmed root is never submitted again or hidden by rollback.
        if active {
            publisher.publish_initial(id, owner.clone()).await?;
        }
        let session = auth.borrow_task_sql_session(owner).await?;
        let scheduled = async {
            schedule_refresh(
                &session, id, active, interval_hours, String::new(), "UTC".into()
            ).await?;
            // SerializerMethodField counts related rows at response time; the
            // newly published worker may already have discovered the guide.
            let count: i64 = session.run(move |connection| Box::pin(async move {
                sqlx::query_scalar("SELECT count(*) FROM epg_epgdata WHERE epg_source_id=$1")
                    .bind(id).fetch_one(connection).await
            })).await??;
            Ok::<_, StoreError>(count)
        }.await;
        reply["epg_data_count"] = json!(close_result(&session, scheduled).await?);
        Ok::<_, StoreError>(reply)
    }.await;
    result.map_err(|error| match error {
        StoreError::Database(_) => StoreError::UnhandledRestFault,
        error => error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stock(active: bool) -> Value {
        json!({"name":"Guide","source_type":"xmltv","url":"http://guide-host:8080/guide.xml",
            "username":"","password":"","is_active":active,
            "refresh_interval":24,"cron_expression":"","priority":0})
    }

    #[test]
    fn stock_remote_url_and_empty_credentials_are_retained() {
        for active in [true, false] {
            let input = form(&stock(active)).unwrap();
            assert_eq!(input.active, active);
            assert_eq!(input.interval_hours, 24);
            assert_eq!(input.url, "http://guide-host:8080/guide.xml");
            assert_eq!(input.username.as_deref(), Some(""));
            assert_eq!(input.password.as_deref(), Some(""));
        }
    }

    #[test]
    fn stock_remote_xmltv_custom_or_disabled_interval_is_preserved() {
        for active in [true, false] {
            for hours in [0, 6, 12] {
                let mut request = stock(active);
                request["refresh_interval"] = json!(hours);
                let parsed = form(&request).unwrap();
                assert_eq!(parsed.interval_hours, hours);
                assert_eq!(parsed.active, active);
            }
            let mut request = stock(active);
            request.as_object_mut().unwrap().remove("refresh_interval");
            assert_eq!(form(&request).unwrap().interval_hours, 0);
        }
    }

    #[test]
    fn broader_remote_source_variants_remain_gated() {
        for (key, value) in [
            ("url", json!("ftp://guide-host/guide.xml")),
            ("url", json!("http://user@guide-host/guide.xml")),
            ("url", json!("http://guide-host/guide.xml#fragment")),
            ("url", json!("http://guide-\thost/guide.xml")),
            ("file_path", json!("/data/guide.xml")),
            ("cron_expression", json!("0 1 * * *")),
            ("refresh_interval", json!(-1)),
            ("password", json!("fixture-not-supported")),
            ("custom_properties", json!({"auto_apply_epg_logos": true})),
        ] {
            let mut request = stock(true);
            request[key] = value;
            assert!(
                matches!(form(&request), Err(StoreError::Unsupported(_))),
                "{key}"
            );
        }
    }
}
