//! Immutable per-refresh child lineage; no transport/application authority.
//! Original Dispatcharr authors retain full application/algorithm credit.
use crate::protocol::{self, ProtocolError, Task};
use crate::refresh_protocol::{self, RefreshCall, RefreshMessage};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use dispatcharr_ingest_epg::{
    file_job::{ChildTask, GuideTaskDispatcher},
    ImportError, SwapPolicyContext,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
pub type PublishFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ImportError>> + Send + 'a>>;
/// Implementations must keep context and bytes through actual physical reply,
/// including cancellation/timeout. No broker implementation is implied here.
pub trait RefreshPublisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        id: Uuid,
        bytes: Vec<u8>,
        context: SwapPolicyContext,
    ) -> PublishFuture<'a>;
}
#[derive(Clone, Debug)]
pub enum ChildPublication {
    Refresh {
        source_id: i64,
        force: bool,
        retry: u32,
        eta: DateTime<Utc>,
    },
    ProgrammeIndex {
        source_id: i64,
    },
    SeriesRules,
}
pub fn encode_child(
    child: &ChildPublication,
    id: Uuid,
    root: Uuid,
    parent: Uuid,
    reply: Uuid,
    delivery: Uuid,
    origin: &str,
) -> Result<Vec<u8>, ProtocolError> {
    if let ChildPublication::Refresh {
        source_id,
        force,
        retry,
        eta,
    } = child
    {
        if !(1..=480).contains(retry) {
            return Err(ProtocolError::InvalidArgument);
        }
        return refresh_protocol::encode(
            &RefreshMessage {
                task_id: id,
                source_id: *source_id,
                force: *force,
                retry: *retry,
                root,
                parent: Some(parent),
                eta: Some(*eta),
                call: RefreshCall::deferred(*source_id, *force, *retry),
            },
            reply,
            delivery,
            origin,
        );
    }
    let mut e: Value = serde_json::from_slice(&protocol::encode(
        &Task::CreateBackup,
        id,
        reply,
        delivery,
        origin,
    )?)
    .map_err(|_| ProtocolError::InvalidArgument)?;
    let (task, args, repr) = match child {
        ChildPublication::ProgrammeIndex { source_id } if *source_id > 0 => (
            "apps.epg.tasks.build_programme_index_task",
            json!([source_id]),
            format!("({source_id},)"),
        ),
        ChildPublication::SeriesRules => (
            "apps.channels.tasks.evaluate_series_rules",
            json!([]),
            "()".into(),
        ),
        _ => return Err(ProtocolError::InvalidArgument),
    };
    e["headers"]["task"] = json!(task);
    e["headers"]["root_id"] = json!(root.to_string());
    e["headers"]["parent_id"] = json!(parent.to_string());
    e["headers"]["argsrepr"] = json!(repr);
    e["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([args,{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ProtocolError::InvalidArgument)?
    ));
    let bytes = serde_json::to_vec(&e).map_err(|_| ProtocolError::InvalidArgument)?;
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    Ok(bytes)
}
struct ChildBudget {
    _permit: tokio::sync::OwnedSemaphorePermit,
}
struct PublicationContext {
    _caller: Arc<dyn Send + Sync>,
    _budget: Arc<ChildBudget>,
}
pub struct RefreshDispatcher {
    root: Uuid,
    parent: Uuid,
    publisher: Arc<dyn RefreshPublisher>,
    store: dispatcharr_store::Store,
    clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    children: Mutex<Vec<(Uuid, Arc<ChildBudget>)>>,
    child_slots: Arc<tokio::sync::Semaphore>,
}
impl RefreshDispatcher {
    pub fn new(
        root: Uuid,
        parent: Uuid,
        publisher: Arc<dyn RefreshPublisher>,
        store: dispatcharr_store::Store,
        clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    ) -> Self {
        Self {
            root,
            parent,
            publisher,
            store,
            clock,
            children: Mutex::new(Vec::new()),
            child_slots: Arc::new(tokio::sync::Semaphore::new(64)),
        }
    }
    pub fn children(&self) -> Vec<Uuid> {
        self.children
            .lock()
            .unwrap()
            .iter()
            .map(|(id, _)| *id)
            .collect()
    }
}
impl GuideTaskDispatcher for RefreshDispatcher {
    fn dispatch<'a>(&'a self, task: ChildTask, context: SwapPolicyContext) -> PublishFuture<'a> {
        Box::pin(async move {
            context.ensure_active()?;
            let child = match task {
                ChildTask::RefreshDeferred {
                    source_id,
                    force,
                    retry,
                    delay_seconds,
                } => {
                    if delay_seconds != 15 || !(1..=480).contains(&retry) {
                        return Err(ImportError::Capacity("refresh defer contract"));
                    }
                    ChildPublication::Refresh {
                        source_id,
                        force,
                        retry,
                        eta: (self.clock)()
                            .checked_add_signed(chrono::Duration::seconds(i64::from(delay_seconds)))
                            .ok_or(ImportError::Capacity("refresh ETA overflow"))?,
                    }
                }
                ChildTask::ProgrammeIndex { source_id } => {
                    ChildPublication::ProgrammeIndex { source_id }
                }
                ChildTask::SeriesRules => ChildPublication::SeriesRules,
                ChildTask::LateProgrammes {
                    source_id,
                    initial_epg_ids,
                } => {
                    if source_id <= 0 || initial_epg_ids.len() > 100000 {
                        return Err(ImportError::Capacity("refresh late mapping scope"));
                    }
                    // Observed ordinary imports have no concurrent newly mapped rows. A real
                    // late child requires its own task-family contract; never silently drop it.
                    let missed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM epg_epgdata e WHERE e.epg_source_id=$1 AND e.id<>ALL($2) AND (EXISTS(SELECT 1 FROM dispatcharr_channels_channel c WHERE c.epg_data_id=e.id) OR EXISTS(SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.epg_data_id=e.id)))").bind(source_id).bind(initial_epg_ids).fetch_one(self.store.pool()).await?;
                    if missed {
                        return Err(ImportError::Capacity("unverified late mapping child"));
                    }
                    return Ok(());
                }
            };
            let budget = Arc::new(ChildBudget {
                _permit: self
                    .child_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| ImportError::Capacity("refresh child count"))?,
            });
            let context = SwapPolicyContext {
                cancelled: context.cancelled,
                retained: Arc::new(PublicationContext {
                    _caller: context.retained,
                    _budget: budget.clone(),
                }),
            };
            let id = Uuid::new_v4();
            let bytes = encode_child(
                &child,
                id,
                self.root,
                self.parent,
                Uuid::new_v4(),
                Uuid::new_v4(),
                "rust-probe@refresh-component",
            )
            .map_err(|_| ImportError::Worker)?;
            self.publisher.publish(id, bytes, context).await?;
            self.children.lock().unwrap().push((id, budget));
            Ok(())
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_actual_imported_children_and_deferred_wire_fields_match_except_declared_child_route() {
        let corpus: Value =
            serde_json::from_str(include_str!("../datasets/source-epg-refresh-import-2.json"))
                .unwrap();
        let mut count = 0;
        for row in corpus["messages"].as_array().unwrap() {
            if row["kind"] != "child" {
                continue;
            }
            let e = &row["envelope"];
            let h = &e["headers"];
            let body: Value =
                serde_json::from_slice(&STANDARD.decode(e["body"].as_str().unwrap()).unwrap())
                    .unwrap();
            let child = match h["task"].as_str().unwrap() {
                "apps.epg.tasks.refresh_epg_data" => ChildPublication::Refresh {
                    source_id: body[0][0].as_i64().unwrap(),
                    force: body[1]["force"].as_bool().unwrap(),
                    retry: body[1]["_file_defer_retry"].as_u64().unwrap() as u32,
                    eta: DateTime::parse_from_rfc3339(h["eta"].as_str().unwrap())
                        .unwrap()
                        .with_timezone(&Utc),
                },
                "apps.epg.tasks.build_programme_index_task" => ChildPublication::ProgrammeIndex {
                    source_id: body[0][0].as_i64().unwrap(),
                },
                "apps.channels.tasks.evaluate_series_rules" => ChildPublication::SeriesRules,
                _ => panic!("unexpected Source family"),
            };
            let uuid = |v: &Value| Uuid::parse_str(v.as_str().unwrap()).unwrap();
            let native = encode_child(
                &child,
                uuid(&h["id"]),
                uuid(&h["root_id"]),
                uuid(&h["parent_id"]),
                uuid(&e["properties"]["reply_to"]),
                uuid(&e["properties"]["delivery_tag"]),
                h["origin"].as_str().unwrap(),
            )
            .unwrap();
            let mut native: Value = serde_json::from_slice(&native).unwrap();
            let mut original = e.clone();
            for frame in [&mut native, &mut original] {
                frame["body"] = serde_json::from_slice::<Value>(
                    &STANDARD.decode(frame["body"].as_str().unwrap()).unwrap(),
                )
                .unwrap();
            }
            if h["task"] != "apps.epg.tasks.refresh_epg_data" {
                assert_eq!(
                    original["properties"]["delivery_info"]["routing_key"],
                    "owned-refresh-children"
                );
                original["properties"]["delivery_info"]["routing_key"] = json!("celery");
            }
            assert_eq!(native, original);
            count += 1;
        }
        assert_eq!(count, 6)
    }
}
