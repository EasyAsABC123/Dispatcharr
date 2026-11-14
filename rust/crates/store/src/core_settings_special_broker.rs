//! Source51 normal Redis/Celery publications and token-owned rehash handoff.
//! Uses the already shared NotificationCache transport, no Store→job-transport edge.
use crate::core_settings_special_budget::Scratch;
use crate::{AuthService, StoreError};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{Connection, Row};
use std::collections::BTreeSet;

fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"CoreSettings resource capacity is unavailable."}),
    )
}
async fn finish(mut c: sqlx::pool::PoolConnection<sqlx::Postgres>) -> Result<(), StoreError> {
    if let Err(error) = Connection::ping(&mut *c).await {
        let _ = c.close().await;
        return Err(error.into());
    }
    Connection::shrink_buffers(&mut *c);
    c.return_to_pool().await;
    drop(c);
    Ok(())
}
/// Owned IDs/token never come from a response or guessed public counter.
pub(crate) struct RehashLocks {
    pub(crate) ids: Vec<i64>,
    pub(crate) token: String,
    _charge: std::sync::Arc<crate::core_settings_special_budget::Charge>,
}
impl RehashLocks {
    pub(crate) async fn acquire(auth: &AuthService, scratch: &Scratch) -> Result<Self, StoreError> {
        // Existing scratch grant covers retained row text/row descriptors,
        // bounded IDs and command encoding before SQL transfer/allocation.
        let charge = scratch.charge(65536 + 1024 * 512)?;
        let mut c = auth.store.pool().acquire().await?;
        // Source includes inactive/custom accounts and excludes valid per-account
        // hash overrides. Bound before transferring any unbounded JSON property text.
        let result=sqlx::query("WITH bounded AS (SELECT id::bigint AS id,CASE WHEN jsonb_typeof(custom_properties)='object' THEN jsonb_typeof(custom_properties->'hash_key') END AS kind,COALESCE(CASE WHEN jsonb_typeof(custom_properties)='object' THEN custom_properties->'hash_key' #>> '{}' END,'') AS props FROM m3u_m3uaccount ORDER BY id LIMIT 1025), measured AS (SELECT id,kind,props,octet_length(props) AS bytes,sum(octet_length(props)) OVER () AS total,count(*) OVER () AS rows FROM bounded) SELECT id,kind,bytes,total,rows,CASE WHEN total<=65536 AND rows<=1024 THEN props END AS props FROM measured ORDER BY id").fetch_all(&mut*c).await;
        finish(c).await?;
        let rows = result?;
        if rows.len() > 1024 {
            return Err(unavailable());
        }
        let mut ids = Vec::new();
        let mut total = 0usize;
        for row in rows {
            let aggregate: i64 = row.try_get("total")?;
            let count: i64 = row.try_get("rows")?;
            if aggregate > 65536 || count > 1024 {
                return Err(unavailable());
            }
            let bytes: i32 = row.try_get("bytes")?;
            if !(0..=65536).contains(&bytes) {
                return Err(unavailable());
            }
            total = total.checked_add(bytes as usize).ok_or_else(unavailable)?;
            if total > 65536 {
                return Err(unavailable());
            }
            let raw: Option<String> = row.try_get("props")?;
            let kind: Option<String> = row.try_get("kind")?;
            let override_fields = match kind.as_deref() {
                None | Some("null") => Vec::new(),
                Some("string") => super::core_settings_special::hash_fields_text(
                    raw.as_deref().ok_or_else(unavailable)?,
                ),
                _ => {
                    return Err(StoreError::Unsupported(
                        "Uncaptured per-account hash override type",
                    ))
                }
            };
            if override_fields.is_empty() {
                ids.push(row.try_get("id")?)
            }
        }
        let cache = auth.notification_cache.as_ref().ok_or_else(unavailable)?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        let mut acquired = Vec::new();
        let mut blocked = Vec::new();
        for id in ids {
            let mut cmd = redis::cmd("SET");
            cmd.arg(format!("task_lock_refresh_single_m3u_account_{id}"))
                .arg(&token)
                .arg("EX")
                .arg(7200)
                .arg("NX");
            match cache.command_settled::<Option<String>>(cmd).await {
                Ok(Some(reply)) if reply == "OK" => acquired.push(id),
                Ok(None) => blocked.push(id),
                Ok(_) => {
                    let owned = Self {
                        ids: acquired,
                        token,
                        _charge: charge.clone(),
                    };
                    owned.release(auth).await?;
                    return Err(StoreError::Unsupported("Unexpected rehash lock reply"));
                }
                Err(error) => {
                    let owned = Self {
                        ids: acquired,
                        token,
                        _charge: charge.clone(),
                    };
                    let _ = owned.release(auth).await;
                    return Err(error);
                }
            }
        }
        let owned = Self {
            ids: acquired,
            token,
            _charge: charge,
        };
        if !blocked.is_empty() {
            owned.release(auth).await?;
            return Err(StoreError::StatusBody(
                409,
                json!({"detail":"Cannot change the M3U Hash Key while one or more accounts that use the global default are refreshing. Wait for those refreshes to finish, then try again.","m3u_hash_key":["One or more M3U accounts are currently refreshing. Try again later."],"blocked_accounts":blocked}),
            ));
        }
        Ok(owned)
    }
    pub(crate) async fn release(&self, auth: &AuthService) -> Result<(), StoreError> {
        let cache = auth.notification_cache.as_ref().ok_or_else(unavailable)?;
        for id in &self.ids {
            let mut cmd = redis::cmd("EVAL");
            cmd.arg("if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0").arg(1).arg(format!("task_lock_refresh_single_m3u_account_{id}")).arg(&self.token);
            let _: i64 = cache.command_settled(cmd).await?;
        }
        Ok(())
    }
}
#[derive(Serialize)]
struct Message<'a> {
    body: String,
    #[serde(rename = "content-encoding")]
    encoding: &'static str,
    #[serde(rename = "content-type")]
    content_type: &'static str,
    headers: Headers<'a>,
    properties: Properties<'a>,
}
#[derive(Serialize)]
struct Headers<'a> {
    lang: &'static str,
    task: &'a str,
    id: &'a str,
    shadow: Option<()>,
    eta: Option<()>,
    expires: Option<()>,
    group: Option<()>,
    group_index: Option<()>,
    retries: u8,
    timelimit: [Option<u8>; 2],
    root_id: &'a str,
    parent_id: Option<()>,
    ignore_result: bool,
}
#[derive(Serialize)]
struct Properties<'a> {
    correlation_id: &'a str,
    reply_to: &'a str,
    delivery_mode: u8,
    delivery_info: Delivery,
    priority: u8,
    body_encoding: &'static str,
    delivery_tag: &'a str,
}
#[derive(Serialize)]
struct Delivery {
    exchange: &'static str,
    routing_key: &'static str,
}
/// No retries or mutation replay. A lost Redis reply remains an error/uncertainty.
pub(crate) async fn publish(
    auth: &AuthService,
    scratch: &Scratch,
    task: &str,
    args: Value,
    kwargs: Value,
) -> Result<String, StoreError> {
    use base64::Engine;
    if ![
        "core.tasks.rehash_streams",
        "apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change",
    ]
    .contains(&task)
    {
        return Err(StoreError::Unsupported("Unqualified special settings task"));
    }
    let cache = auth.notification_cache.as_ref().ok_or_else(unavailable)?;
    // Count without cloning argument trees or allocating transport buffers.
    let embed = json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null});
    let envelope = (&args, &kwargs, &embed);
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, &envelope).map_err(|_| unavailable())?;
    if count.0 > 16384 {
        return Err(unavailable());
    }
    let charge = scratch.charge(
        count
            .0
            .checked_mul(8)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(unavailable)?,
    )?;
    let mut body = Vec::with_capacity(count.0);
    serde_json::to_writer(&mut body, &envelope).map_err(|_| unavailable())?;
    let id = uuid::Uuid::new_v4().to_string();
    let reply = uuid::Uuid::new_v4().to_string();
    let delivery = uuid::Uuid::new_v4().to_string();
    let message = Message {
        body: base64::engine::general_purpose::STANDARD.encode(&body),
        encoding: "utf-8",
        content_type: "application/json",
        headers: Headers {
            lang: "py",
            task,
            id: &id,
            shadow: None,
            eta: None,
            expires: None,
            group: None,
            group_index: None,
            retries: 0,
            timelimit: [None, None],
            root_id: &id,
            parent_id: None,
            ignore_result: false,
        },
        properties: Properties {
            correlation_id: &id,
            reply_to: &reply,
            delivery_mode: 2,
            delivery_info: Delivery {
                exchange: "",
                routing_key: "celery",
            },
            priority: 0,
            body_encoding: "base64",
            delivery_tag: &delivery,
        },
    };
    let raw = serde_json::to_vec(&message).map_err(|_| unavailable())?;
    if raw.len() > 65536 {
        return Err(unavailable());
    }
    // The existing cache's physical task carries this charge and original request
    // token after logical error/observer loss; no second writer slot is acquired.
    let cache = cache.clone().with_retained_scope(charge);
    let mut binding = redis::cmd("SADD");
    binding
        .arg("_kombu.binding.celery")
        .arg("celery\u{6}\u{16}\u{6}\u{16}celery");
    let _: i64 = cache.command_settled(binding).await?;
    let mut push = redis::cmd("LPUSH");
    push.arg("celery").arg(raw);
    let _: i64 = cache.command_settled(push).await?;
    Ok(id)
}
pub(crate) fn changed(old: &[String], new: &[String]) -> bool {
    old.iter().collect::<BTreeSet<_>>() != new.iter().collect::<BTreeSet<_>>()
}

struct Counter(usize);
impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= 16384)
            .ok_or_else(|| std::io::Error::other("task body capacity"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
