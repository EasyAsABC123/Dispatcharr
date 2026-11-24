//! Source51 normal Redis/Celery publications and token-owned rehash handoff.
//! Uses the already shared NotificationCache transport, no Store→job-transport edge.
use crate::core_settings_special_budget::{Charge, Scratch};
use crate::{AuthService, CoreProfilePublication, CoreTaskPublication, StoreError};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::{collections::BTreeSet, sync::Arc};

fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"CoreSettings resource capacity is unavailable."}),
    )
}
/// Owned IDs/token never come from a response or guessed public counter.
pub(crate) struct RehashLocks {
    pub(crate) ids: Vec<i64>,
    pub(crate) token: String,
    _charge: Vec<std::sync::Arc<crate::core_settings_special_budget::Charge>>,
}
impl RehashLocks {
    pub(crate) async fn acquire(auth: &AuthService, scratch: &Scratch) -> Result<Self, StoreError> {
        // Bounded SQL pages plus actual retained envelope IDs use the existing
        // actor quote. Catalog cardinality is not a fixed admission condition.
        let page = scratch.charge(65536 + 256 * 512)?;
        let cache = auth.notification_cache.as_ref().ok_or_else(unavailable)?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        let mut c = auth.store.pool().acquire().await?;
        let mut charges = vec![page];
        let mut acquired = Vec::new();
        let mut blocked = Vec::new();
        let observed=async{
            let count=crate::core_task_rehash::prepare_global_targets(&mut c).await?;
            charges.push(scratch.charge(count.checked_mul(128).ok_or_else(unavailable)?)?);
            sqlx::query("DECLARE rust_core_rehash_targets NO SCROLL CURSOR WITH HOLD FOR SELECT id FROM rust_rehash_targets ORDER BY seq").execute(&mut*c).await?;
            loop{
                let rows=sqlx::query("FETCH FORWARD 256 FROM rust_core_rehash_targets").fetch_all(&mut*c).await?;
                if rows.is_empty(){break;}
                for row in rows{
                    let id:i64=row.try_get("id")?;
                    let mut cmd=redis::cmd("SET");cmd.arg(format!("task_lock_refresh_single_m3u_account_{id}")).arg(&token).arg("EX").arg(7200).arg("NX");
                    match cache.command_settled::<Option<String>>(cmd).await?{
                        Some(reply)if reply=="OK"=>acquired.push(id),None=>blocked.push(id),_=>return Err(StoreError::Unsupported("Unexpected rehash lock reply"))
                    }
                }
            }
            sqlx::query("CLOSE rust_core_rehash_targets").execute(&mut*c).await?;
            Ok::<_,StoreError>(())
        }.await;
        // TEMP state stays on this one slot; close it instead of returning a
        // session containing task-private tables/cursors to another actor.
        let closed = c.close().await;
        let owned = Self {
            ids: acquired,
            token,
            _charge: charges,
        };
        if let Err(error) = observed {
            let release = owned.release(auth).await;
            eprintln!(
                "Core rehash preparation failed {error:?}; release {release:?}; close {closed:?}"
            );
            return Err(error);
        }
        if let Err(error) = closed {
            let release = owned.release(auth).await;
            eprintln!("Core rehash SQL close failed {error:?}; release {release:?}");
            return Err(error.into());
        }
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
    timelimit: [Option<u64>; 2],
    root_id: &'a str,
    parent_id: Option<()>,
    argsrepr: String,
    kwargsrepr: String,
    origin: String,
    ignore_result: bool,
    replaced_task_nesting: u8,
    stamped_headers: Option<()>,
    stamps: serde_json::Map<String, Value>,
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
    let (raw, id, charge) = build_frame(auth, scratch, task, args, kwargs)?;
    if let Some(broker) = auth.core_task_broker.get() {
        let publication = CoreTaskPublication::new(raw, &id)?;
        broker
            .publish(publication, auth.core_publication_owner(charge))
            .await?;
        return Ok(id);
    }
    if auth.normal_core_task_broker {
        return Err(StoreError::Unsupported(
            "Normal Core task broker has not been composed",
        ));
    }
    // Legacy qualified constructors retain their original explicit cache-DB
    // publication path; normal Main above can never fall through to this path.
    let cache = auth.notification_cache.as_ref().ok_or_else(unavailable)?;
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

pub(crate) async fn publish_provider_rehash(
    auth: &AuthService,
    scratch: &Scratch,
    keys: Vec<String>,
    account: i64,
    token: &str,
    admission: Option<&Arc<dyn crate::ProviderRehashAdmission>>,
) -> Result<crate::ProviderRehashResult, StoreError> {
    let task = "core.tasks.rehash_streams";
    let args = json!([keys]);
    let kwargs = json!({"account_id":account,"locks_preacquired":true,
        "preacquired_lock_ids":[account],"preacquired_lock_token":token});
    match admission {
        Some(admission) => {
            let (raw, id, charge) = build_frame(auth, scratch, task, args, kwargs)?;
            let publication = crate::ProviderRehashPublication(CoreTaskPublication::new(raw, &id)?);
            let owner = Arc::new((auth.core_publication_owner(charge), admission.clone()));
            admission.publish(publication, owner).await
        }
        None => {
            publish(auth, scratch, task, args, kwargs).await?;
            Ok(crate::ProviderRehashResult::Acknowledged)
        }
    }
}

pub(crate) async fn publish_tvg_programmes(
    auth: &AuthService,
    epg_data_id: i64,
) -> Result<(), StoreError> {
    let scratch = Scratch::new();
    let task = "apps.epg.tasks.parse_programs_for_tvg_id";
    if auth.core_task_broker.get().is_some() {
        // Selected Main keeps its existing shared broker and deferred worker.
        return publish(auth, &scratch, task, json!([epg_data_id]), json!({}))
            .await
            .map(|_| ());
    }
    let publisher = auth
        .tvg_programme_publisher
        .get()
        .ok_or(StoreError::Unsupported(
            "TVG programme publisher unavailable",
        ))?;
    let (raw, id, charge) = build_frame(auth, &scratch, task, json!([epg_data_id]), json!({}))?;
    let frame = crate::TvgProgrammePublication(CoreTaskPublication::new(raw, &id)?);
    publisher
        .publish(frame, auth.core_publication_owner(charge))
        .await
}

pub(crate) async fn publish_sd_guide_root(
    auth: &AuthService,
    mapped: bool,
    target: i64,
) -> Result<(), StoreError> {
    let publisher = auth
        .sd_guide_root_publisher
        .get()
        .ok_or(StoreError::Unsupported(
            "Schedules Direct guide publisher unavailable",
        ))?;
    let task = if mapped {
        "apps.epg.tasks.fetch_sd_mapped_guide_batch"
    } else {
        "apps.epg.tasks.parse_programs_for_tvg_id"
    };
    let (raw, id, charge) = build_frame(auth, &Scratch::new(), task, json!([target]), json!({}))?;
    let frame = crate::SdGuideRootPublication(CoreTaskPublication::new(raw, &id)?);
    publisher
        .publish(frame, auth.core_publication_owner(charge))
        .await
}

pub(crate) async fn publish_profiles_root(
    auth: &AuthService,
    scratch: &Scratch,
    account: i64,
) -> Result<CoreProfilePublication, StoreError> {
    let broker = auth.core_task_broker.get().ok_or(StoreError::Unsupported(
        "Normal Core task broker has not been composed",
    ))?;
    let (raw, id, charge) = build_frame(
        auth,
        scratch,
        "apps.m3u.tasks.refresh_account_profiles",
        json!([account]),
        json!({}),
    )?;
    let publication = CoreTaskPublication::new(raw, &id)?;
    broker
        .publish_profiles_root(publication, auth.core_publication_owner(charge))
        .await
}

fn build_frame(
    auth: &AuthService,
    scratch: &Scratch,
    task: &str,
    args: Value,
    kwargs: Value,
) -> Result<(Vec<u8>, String, Arc<Charge>), StoreError> {
    use base64::Engine;
    if ![
        "core.tasks.rehash_streams",
        "apps.epg.tasks.parse_programs_for_tvg_id",
        "apps.epg.tasks.fetch_sd_mapped_guide_batch",
        "apps.m3u.tasks.refresh_m3u_groups",
        "apps.m3u.tasks.refresh_account_profiles",
        "apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change",
    ]
    .contains(&task)
    {
        return Err(StoreError::Unsupported("Unqualified special settings task"));
    }

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
            .checked_mul(12)
            .and_then(|n| n.checked_add(16384))
            .ok_or_else(unavailable)?,
    )?;
    let mut body = Vec::with_capacity(count.0);
    serde_json::to_writer(&mut body, &envelope).map_err(|_| unavailable())?;
    let id = uuid::Uuid::new_v4().to_string();
    // One actual shared application budget is the stable publisher-instance
    // identity across Auth/cache scope clones, never a request or fixture ID.
    let identity =
        crate::celery_metadata::identity(Arc::as_ptr(&auth.user_agent_workers) as usize)?;
    let argsrepr = crate::celery_metadata::arguments(&args)?;
    let kwargsrepr = crate::celery_metadata::keywords(&kwargs)?;
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
            timelimit: if matches!(
                task,
                "apps.epg.tasks.parse_programs_for_tvg_id"
                    | "apps.epg.tasks.fetch_sd_mapped_guide_batch"
            ) {
                [Some(3600), Some(3500)]
            } else {
                [None, None]
            },
            root_id: &id,
            parent_id: None,
            argsrepr,
            kwargsrepr,
            origin: identity.origin,
            ignore_result: false,
            replaced_task_nesting: 0,
            stamped_headers: None,
            stamps: serde_json::Map::new(),
        },
        properties: Properties {
            correlation_id: &id,
            reply_to: &identity.reply_to,
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
    Ok((raw, id, charge))
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
