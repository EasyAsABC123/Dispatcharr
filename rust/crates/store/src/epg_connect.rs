//! Native Source Connect EPG subscription/DeliveryLog orchestration.
//! Original Dispatcharr/Django/Requests authors retain all algorithm credit.
//! One provided task SQL session; callbacks run only between actual Ready fences.
use crate::{StoreError, TaskSqlSession};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;
#[path = "epg_connect_http.rs"]
mod http;
#[path = "epg_connect_templates.rs"]
mod templates;
pub use templates::{dispatch_scoped_event, EventReport};

/// Use the event delivery transport for a single saved webhook. This action
/// has no event subscription, template evaluation, or DeliveryLog write.
pub(crate) async fn send_saved_webhook(config: &Value, value: Value) -> Result<Value, Error> {
    http::send(config, &Payload::Original(value)).await
}

type Owner = Arc<dyn Send + Sync>;
#[derive(Debug)]
pub enum Error {
    Store(Box<StoreError>),
    Http(Box<reqwest::Error>),
    Body(Box<std::io::Error>),
    Unsupported(&'static str),
    Supervisor,
    Close {
        first: Option<Box<Error>>,
        close: Box<StoreError>,
    },
}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        Self::Store(Box::new(value))
    }
}
#[derive(Clone)]
pub enum Payload {
    Original(Value),
    Rendered {
        body: String,
        source_json_loads_succeeded: bool,
    },
}
/// Actual Source Template result AND the projection of the same retained Python
/// event payload (which built-in as-variable nodes may mutate).
pub enum TemplateResult {
    Rendered {
        body: String,
        source_json_loads_succeeded: bool,
        payload: Value,
    },
    Caught {
        actual_class: String,
        message: String,
        payload: Value,
    },
}
/// Implement using Ops' actual scoped PluginHookSession. No no-op implementation
/// is provided. Its Python event object must survive through the plugin phase.
pub trait Templates: Send + Sync {
    fn begin_event(&self, event: String, payload: Value) -> BoxFuture<'_, Result<(), StoreError>>;
    fn render(&self, template: String) -> BoxFuture<'_, Result<TemplateResult, StoreError>>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Complete,
    CaughtAbort,
}
pub struct Report {
    pub flow: Flow,
    pub selected_count: u64,
    pub visited: u64,
    pub disabled: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub templates_caught: u64,
    /// Updated projection; actual Python object remains in the scoped session.
    pub payload: Value,
}
struct Subscription {
    id: i64,
    kind: String,
    config: Value,
    template: Option<String>,
}
fn known_database(error: &StoreError) -> bool {
    matches!(error, StoreError::Database(sqlx::Error::Database(_)))
}
fn report(payload: Value) -> Report {
    Report {
        flow: Flow::Complete,
        selected_count: 0,
        visited: 0,
        disabled: 0,
        succeeded: 0,
        failed: 0,
        templates_caught: 0,
        payload,
    }
}
/// Accepted event work is supervised even when an HTTP/task observer disappears.
/// The SAME caller/template actor/session remain owned until HTTP EOF and all
/// SQL replies/cursor closure; no new pool, writer or task semaphore is created.
pub async fn dispatch(
    event: String,
    payload: Value,
    session: TaskSqlSession,
    templates: Arc<dyn Templates>,
    owner: Owner,
) -> Result<Report, Error> {
    if !matches!(
        event.as_str(),
        "epg_refresh" | "epg_error" | "recording_start" | "recording_end"
    ) || !payload.is_object()
    {
        return Err(Error::Unsupported("Connect EPG event/payload unsupported"));
    }
    if serde_json::to_vec(&payload)
        .map_err(|_| Error::Unsupported("Connect event payload encoding"))?
        .len()
        > 256 * 1024
    {
        return Err(Error::Unsupported("Connect event payload capacity"));
    }
    tokio::spawn(async move{
        let _owner=owner;
        templates.begin_event(event.clone(),payload.clone()).await?;
        let mut result=report(payload);
        let count_event=event.clone();
        let count=session.run(move|c|Box::pin(async move{sqlx::query_scalar::<_,i64>("SELECT count(*) FROM dispatcharr_connect_eventsubscription WHERE event=$1 AND enabled").bind(count_event).fetch_one(c).await.map_err(StoreError::from)})).await?;
        result.selected_count=match count {Ok(count)=>u64::try_from(count).map_err(|_|Error::Unsupported("Connect subscription count invalid"))?,Err(error)if known_database(&error)=>{result.flow=Flow::CaughtAbort;return Ok(result)},Err(error)=>return Err(error.into())};
        let cursor=format!("connect_epg_{}",uuid::Uuid::new_v4().simple());
        let create=cursor.clone();
        let opened=session.run(move|c|Box::pin(async move{
            if !c.protocol_transaction_is_idle().await? {return Err(StoreError::Unsupported("Connect held cursor requires actual idle SQL boundary"))}
            sqlx::query("BEGIN").execute(&mut*c).await?;
            let sql=format!("DECLARE {create} NO SCROLL CURSOR WITH HOLD FOR SELECT s.id::bigint,i.enabled,i.type,CASE WHEN octet_length(i.config::text)<=65536 THEN i.config::text END AS config,CASE WHEN octet_length(s.payload_template)<=65536 THEN s.payload_template END AS payload_template,(octet_length(i.config::text)>65536 OR coalesce(octet_length(s.payload_template)>65536,false)) AS oversized FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.event=$1 AND s.enabled");
            let outcome=async{sqlx::query(&sql).bind(event).execute(&mut*c).await?;sqlx::query("COMMIT").execute(&mut*c).await?;Ok::<_,sqlx::Error>(())}.await;
            if outcome.is_err(){sqlx::query("ROLLBACK").execute(&mut*c).await?;}
            outcome.map_err(StoreError::from)
        })).await?;
        if let Err(error)=opened {if known_database(&error){result.flow=Flow::CaughtAbort;return Ok(result)}return Err(error.into())}
        let outcome=run_cursor(&session,&cursor,templates.as_ref(),&mut result).await;
        let close=cursor.clone();
        let closed=session.run(move|c|Box::pin(async move{sqlx::query(&format!("CLOSE {close}")).execute(c).await.map(|_|()).map_err(StoreError::from)})).await;
        match closed {Ok(Ok(()))=>{},Ok(Err(close))|Err(close)=>return Err(Error::Close{first:outcome.err().map(Box::new),close:Box::new(close)})}
        outcome?;Ok(result)
    }).await.map_err(|_|Error::Supervisor)?
}
async fn run_cursor(
    session: &TaskSqlSession,
    cursor: &str,
    templates: &dyn Templates,
    report: &mut Report,
) -> Result<(), Error> {
    loop {
        let cursor = cursor.to_owned();
        // Source queryset iteration has no Meta.ordering. Preserve that actual
        // SELECT snapshot/order; FETCH1 never holds a SQLx borrow across HTTP,
        // Django template custom tags or a later ORM callback on THIS session.
        let row = session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("FETCH FORWARD 1 FROM {cursor}"))
                        .fetch_optional(c)
                        .await
                        .map_err(StoreError::from)
                })
            })
            .await?;
        let row = match row {
            Ok(Some(row)) => row,
            Ok(None) => return Ok(()),
            Err(error) if known_database(&error) => {
                report.flow = Flow::CaughtAbort;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        report.visited += 1;
        if !row
            .try_get::<bool, _>("enabled")
            .map_err(StoreError::from)?
        {
            report.disabled += 1;
            continue;
        }
        if row
            .try_get::<bool, _>("oversized")
            .map_err(StoreError::from)?
        {
            return Err(Error::Unsupported("Connect subscription/config capacity"));
        }
        let raw: String = row.try_get("config").map_err(StoreError::from)?;
        let config = crate::xc_bounded_user::checked_properties(
            &raw,
            0,
            crate::XcUserReadLimits {
                max_raw_bytes: 65536,
                max_nodes: 4096,
                max_depth: 32,
                work_bytes: 4 * 1024 * 1024,
            },
        )?
        .0;
        let sub = Subscription {
            id: row.try_get("id").map_err(StoreError::from)?,
            kind: row.try_get("type").map_err(StoreError::from)?,
            config,
            template: row.try_get("payload_template").map_err(StoreError::from)?,
        };
        let mut payload = Payload::Original(report.payload.clone());
        if sub.kind == "webhook" {
            if let Some(template) = sub.template.filter(|v| !v.is_empty()) {
                match templates.render(template).await? {
                    TemplateResult::Rendered {
                        body,
                        source_json_loads_succeeded,
                        payload: projection,
                    } => {
                        if body.len() > 256 * 1024 {
                            return Err(Error::Unsupported("Connect rendered body capacity"));
                        }
                        report.payload = projection;
                        payload = Payload::Rendered {
                            body,
                            source_json_loads_succeeded,
                        };
                    }
                    TemplateResult::Caught {
                        actual_class: _,
                        message: _,
                        payload: projection,
                    } => {
                        report.templates_caught += 1;
                        report.payload = projection;
                        payload = Payload::Original(report.payload.clone());
                    }
                }
            }
        }
        if sub.kind == "script" {
            return Err(Error::Unsupported(
                "Connect script execution/physical ownership port not bound",
            ));
        }
        if sub.kind != "webhook" {
            let message = format!("No handler for integration type '{}'", sub.kind);
            if let Err(error) = write_log(
                session,
                sub.id,
                "failed",
                payload_value(&payload),
                json!({}),
                Some(message),
            )
            .await?
            {
                if known_database(&error) {
                    report.flow = Flow::CaughtAbort;
                    return Ok(());
                }
                return Err(error.into());
            }
            report.failed += 1;
            continue;
        }
        let response = http::send(&sub.config, &payload).await?;
        let status = if response.get("success") == Some(&Value::Bool(true)) {
            "success"
        } else {
            "failed"
        };
        match write_log(
            session,
            sub.id,
            status,
            payload_value(&payload),
            response,
            None,
        )
        .await?
        {
            Ok(()) => {
                if status == "success" {
                    report.succeeded += 1
                } else {
                    report.failed += 1
                }
            }
            Err(error) => {
                // Source's try includes the success DeliveryLog INSERT itself.
                // A known server failure is caught and attempts a FAILED log;
                // a second known failure aborts remaining Connect/plugins.
                let Some(message) = crate::plugin_actions::setting_error(&error) else {
                    return Err(error.into());
                };
                match write_log(
                    session,
                    sub.id,
                    "failed",
                    payload_value(&payload),
                    json!({}),
                    Some(message),
                )
                .await?
                {
                    Ok(()) => report.failed += 1,
                    Err(error) if known_database(&error) => {
                        report.flow = Flow::CaughtAbort;
                        return Ok(());
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
}
fn payload_value(payload: &Payload) -> Value {
    match payload {
        Payload::Original(value) => value.clone(),
        Payload::Rendered { body, .. } => Value::String(body.clone()),
    }
}
async fn write_log(
    session: &TaskSqlSession,
    id: i64,
    status: &str,
    request: Value,
    response: Value,
    error: Option<String>,
) -> Result<Result<(), StoreError>, StoreError> {
    let status = status.to_owned();
    session.run(move|c|Box::pin(async move{
        let now=chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).ok_or(StoreError::Unsupported("Connect timestamp unsupported"))?;
        let rows=sqlx::query("INSERT INTO dispatcharr_connect_deliverylog(subscription_id,status,request_payload,response_payload,error_message,created_at)VALUES($1,$2,$3,$4,$5,$6)").bind(id).bind(status).bind(request).bind(response).bind(error).bind(now).execute(c).await?.rows_affected();
        if rows!=1{return Err(StoreError::Unsupported("Connect DeliveryLog INSERT unmatched"))}Ok(())
    })).await
}
