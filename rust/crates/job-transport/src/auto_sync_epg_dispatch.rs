//! Source auto-sync EPG descendants through the existing shared Jobs producer.
//! Original Dispatcharr/Celery/Kombu authors retain algorithm/protocol credit.
//! Draft component; not registered or claimed as normal Main acceptance.
use crate::transport::{Error, Jobs};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_store::{
    auto_sync_epg::{Plan, Task},
    ProducerIdentitySource,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
/// Only a completed actual producer constructs this child-metadata capability.
/// Its retained SQL plan is read in bounded pages by final metadata assembly.
pub(crate) struct Dispatched {
    plan: Plan,
    parent: Uuid,
    root: Uuid,
}
impl Dispatched {
    pub(crate) fn parent_task_id(&self) -> Uuid {
        self.parent
    }
    pub(crate) fn root_task_id(&self) -> Uuid {
        self.root
    }
    pub(crate) async fn children(&self, after: i64) -> Result<Vec<(i64, Uuid)>, Error> {
        self.plan.children(after).await.map_err(|_| Error::Backend)
    }
    pub(crate) async fn close(self) -> Result<(), Error> {
        self.plan.close().await.map_err(|_| Error::Backend)
    }
}
pub(crate) struct Dispatcher {
    jobs: Jobs,
    identity: ProducerIdentitySource,
}
impl Dispatcher {
    pub(crate) fn new(jobs: Jobs, identity: ProducerIdentitySource) -> Self {
        Self { jobs, identity }
    }
    pub(crate) async fn dispatch(
        &self,
        plan: Plan,
        parent: Uuid,
        root: Uuid,
        owner: Owner,
        stopped: impl Fn() -> bool,
    ) -> Result<Dispatched, Error> {
        if parent.is_nil() || root.is_nil() {
            return Err(Error::Protocol);
        }
        // There is not yet a normal SD guide network/consumer implementation.
        // Refuse before publishing ANY descendant; never queue a known blocked
        // SD head and acknowledge the parent as successful. SD with existing
        // ProgramData and dummy are actual Source skips, absent from this plan.
        if plan.contains_sd().await.map_err(|_| Error::Backend)? {
            plan.close().await.map_err(|_| Error::Backend)?;
            return Err(Error::UnsupportedTask);
        }
        let mut after = 0;
        loop {
            let page = plan.next(after).await.map_err(|_| Error::Backend)?;
            if page.is_empty() {
                break;
            }
            let mut receipts = Vec::with_capacity(page.len());
            for item in page {
                let Task::ParseProgrammes(epg) = item.task else {
                    return Err(Error::UnsupportedTask);
                };
                let identity = self.identity.current().map_err(|_| Error::Protocol)?;
                let id = Uuid::new_v4();
                let raw = encode_child(epg, id, parent, root, identity.reply_to, &identity.origin)?;
                loop {
                    if stopped() {
                        return Err(Error::DrainTimeout);
                    }
                    match self.jobs.publish_auto_sync_epg(&raw, owner.clone()).await {
                        Ok(receipt) if receipt.task_id == id => break,
                        Ok(_) => return Err(Error::Corrupt),
                        Err(Error::Capacity) => {
                            tokio::time::sleep(Duration::from_millis(100)).await
                        }
                        Err(error) => return Err(error),
                    }
                }
                // Only the settled actual broker receipt enters child metadata.
                receipts.push((item.ordinal, id));
                after = item.ordinal;
            }
            plan.record_publications(receipts)
                .await
                .map_err(|_| Error::Backend)?;
        }
        Ok(Dispatched { plan, parent, root })
    }
}

impl crate::full_metadata::ActualPublishedChildren for Dispatched {
    fn parent_task_id(&self) -> Uuid {
        Dispatched::parent_task_id(self)
    }
    fn root_task_id(&self) -> Uuid {
        Dispatched::root_task_id(self)
    }
    fn children(
        &self,
        after: i64,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Vec<(i64, Uuid)>, Error>> {
        Box::pin(Dispatched::children(self, after))
    }
}

fn encode_child(
    epg: i64,
    id: Uuid,
    parent: Uuid,
    root: Uuid,
    reply_to: Uuid,
    origin: &str,
) -> Result<Vec<u8>, Error> {
    let raw = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        id,
        reply_to,
        Uuid::new_v4(),
        origin,
    )
    .map_err(|_| Error::Protocol)?;
    let mut frame: Value = serde_json::from_slice(&raw).map_err(|_| Error::Protocol)?;
    frame["headers"]["task"] = json!("apps.epg.tasks.parse_programs_for_tvg_id");
    frame["headers"]["timelimit"] = json!([3600, 3500]);
    frame["headers"]["argsrepr"] = json!(format!("({epg},)"));
    frame["headers"]["root_id"] = json!(root);
    frame["headers"]["parent_id"] = json!(parent);
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[epg],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| Error::Protocol)?
    ));
    serde_json::to_vec(&frame).map_err(|_| Error::Protocol)
}

#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn actual_auto_child_encoder_roundtrips_normal_consumer_with_exact_lineage() {
        let id = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let root = Uuid::new_v4();
        let raw = encode_child(41, id, parent, root, Uuid::new_v4(), "gen321@owned").unwrap();
        let (message, parsed_parent) = crate::tvg_programme_protocol::parse_normal(&raw).unwrap();
        assert_eq!(message.task_id, id);
        assert_eq!(parsed_parent, Some(parent));
        let value: Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(value["headers"]["root_id"], root.to_string());
        assert!(crate::tvg_programme_protocol::parse_initial(&raw).is_err());
        let mut bad = value;
        bad["headers"]["argsrepr"] = json!("(42,)");
        assert!(
            crate::tvg_programme_protocol::parse_normal(&serde_json::to_vec(&bad).unwrap())
                .is_err()
        );
    }
}
