//! Optional taskless restore reservation. Ordinary restore admission is
//! deliberately separate. Original Dispatcharr authors retain full credit.
use super::{files::PinnedMutationLock, guard, PluginActions};
use crate::{
    plugin_execution_host::TasklessHostHold, plugin_runtime::LoadedTasklessModule,
    BackupRestoreAdmission, BackupRestoreError, PluginRuntime, StoreError,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, OnceLock},
};
use tokio::time::Instant;

type Owner = Arc<dyn Send + Sync>;

struct Holds {
    files: PinnedMutationLock,
    host: TasklessHostHold,
    runtime: PluginRuntime,
}
struct Physical {
    held: OnceLock<Holds>,
    _caller: Owner,
}
impl Physical {
    fn holds(&self) -> Result<&Holds, StoreError> {
        self.held.get().ok_or_else(guard)
    }
}

/// Identity observed from the actual retained Main SQL connection.
#[derive(Eq, PartialEq)]
pub struct TasklessRestoreTarget {
    database: String,
    database_oid: i64,
    server_started_micros: i64,
    fence_pid: i32,
    fence_started_micros: i64,
}

impl TasklessRestoreTarget {
    async fn observe(admission: &BackupRestoreAdmission) -> Result<Self, StoreError> {
        let (database, database_oid, server_started_micros, fence_pid, fence_started_micros) =
            admission
                .session()
                .run(|connection| {
                    Box::pin(async move {
                        sqlx::query_as::<_, (String, i64, i64, i32, i64)>(
                            "SELECT pg_catalog.current_database(), datid::bigint, \
                     (extract(epoch FROM pg_catalog.pg_postmaster_start_time())*1000000)::bigint, \
                     pid,(extract(epoch FROM backend_start)*1000000)::bigint \
                     FROM pg_catalog.pg_stat_activity WHERE pid=pg_catalog.pg_backend_pid()",
                        )
                        .fetch_one(connection)
                        .await
                    })
                })
                .await??;
        if database != admission.current_database()
            || server_started_micros != admission.server_started_micros()
            || database_oid <= 0
            || fence_pid <= 0
            || fence_started_micros < server_started_micros
        {
            return Err(guard());
        }
        Ok(Self {
            database,
            database_oid,
            server_started_micros,
            fence_pid,
            fence_started_micros,
        })
    }

    /// The physical helper binds these through libpq parameters on its effect connection.
    pub fn database(&self) -> &str {
        &self.database
    }
    pub fn database_oid(&self) -> i64 {
        self.database_oid
    }
    pub fn server_started_micros(&self) -> i64 {
        self.server_started_micros
    }
    pub fn fence_pid(&self) -> i32 {
        self.fence_pid
    }
    pub fn fence_started_micros(&self) -> i64 {
        self.fence_started_micros
    }
}

/// Actual restore fence and cooperating files/host lifetimes, before claim.
/// No archive contents or caller-supplied task identifiers can mint it.
pub struct TasklessRestoreReservation {
    admission: BackupRestoreAdmission,
    physical: Arc<Physical>,
    loaded: LoadedTasklessModule,
    target: TasklessRestoreTarget,
}
/// Private reconciliation observation of the registry and relevant schedule cells.
#[derive(serde::Deserialize)]
pub struct TasklessRegistryObservation {
    pub plugin: serde_json::Value,
    pub periodic: Vec<serde_json::Value>,
}

impl TasklessRestoreReservation {
    /// Read the exact pre/post archive comparison rows from this same fence.
    pub async fn observe_registry(&self) -> Result<TasklessRegistryObservation, StoreError> {
        self.revalidate().await?;
        let raw = self.admission.session().run(|connection| Box::pin(async move {
            sqlx::query_scalar::<_, Option<String>>(
                "WITH plugin AS (SELECT * FROM plugins_pluginconfig ORDER BY id LIMIT 2), \
                 periodic AS (SELECT id,name,task,enabled,queue,exchange,routing_key,args,kwargs, \
                   interval_id,crontab_id,solar_id,clocked_id FROM django_celery_beat_periodictask ORDER BY id LIMIT 257), \
                 projection AS (SELECT CASE WHEN (SELECT count(*) FROM plugin)=1 AND \
                    (SELECT count(*) FROM periodic)<=256 THEN jsonb_build_object( \
                    'plugin',(SELECT to_jsonb(p) FROM plugin p LIMIT 1), \
                    'periodic',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.id),'[]'::jsonb) FROM periodic p))::text END AS raw) \
                 SELECT CASE WHEN octet_length(raw)<=1048576 THEN raw END FROM projection"
            ).fetch_one(connection).await
        })).await??.ok_or_else(guard)?;
        let observation: TasklessRegistryObservation =
            serde_json::from_str(&raw).map_err(|_| guard())?;
        if !self.loaded.matches_registry_key(&observation.plugin) {
            return Err(guard());
        }
        Ok(observation)
    }
    /// Bind the observer to the runtime on this exact Main restore owner.
    pub fn belongs_to_runtime(&self, runtime: &PluginRuntime) -> bool {
        self.physical
            .holds()
            .is_ok_and(|held| held.runtime.same_instance(runtime))
    }

    /// Actual retained Main auth-writer identity, not database-name metadata.
    pub fn belongs_to_auth(&self, auth: &crate::AuthService) -> bool {
        self.admission.shares_auth_writer(auth)
    }

    pub fn target(&self) -> &TasklessRestoreTarget {
        &self.target
    }

    pub fn current_database(&self) -> &str {
        self.admission.current_database()
    }
    pub fn server_started_micros(&self) -> i64 {
        self.admission.server_started_micros()
    }
    pub fn expected_migrations(&self) -> &BTreeSet<(String, String)> {
        self.admission.expected_migrations()
    }
    /// A final synchronous lifetime check for a future admitted blocking
    /// SQL owner. This does not replace the actual database identity query.
    pub fn check_lifetime(&self) -> Result<(), StoreError> {
        let held = self.physical.holds()?;
        if !self.admission.is_open() || !held.host.is_live() {
            return Err(guard());
        }
        held.files.validate()
    }
    /// Revalidate with the same retained session and exact owned child.
    pub async fn revalidate(&self) -> Result<(), StoreError> {
        let held = self.physical.holds()?;
        let now = held
            .runtime
            .witness_with_hold(
                self.admission.session(),
                self.physical.clone(),
                Some(&held.host),
            )
            .await?;
        let physical = self.physical.clone();
        tokio::task::spawn_blocking(move || physical.holds()?.files.validate())
            .await
            .map_err(|_| guard())??;
        if !self.admission.is_open()
            || !self.loaded.same_loaded_module(&now)
            || !held.host.is_live()
            || TasklessRestoreTarget::observe(&self.admission).await? != self.target
        {
            return Err(guard());
        }
        Ok(())
    }
    /// Reuse Main's current-schema policy while the same fence is still held.
    pub async fn reconcile_schema(&self) -> Result<(), BackupRestoreError> {
        self.check_lifetime()?;
        self.admission.reconcile_current_schema().await
    }

    /// Call only after the real admitted physical work has settled. The
    /// reservation still owns both additional lifetimes on a failed close.
    pub async fn close(&self) -> Result<(), StoreError> {
        self.admission.close().await
    }
}

impl PluginActions {
    /// Reserve the fenced database first and then the exact cooperating file
    /// lock and existing action host. Callers must settle or retain this owner.
    pub async fn prepare_taskless_restore_reservation(
        &self,
        expected_database: String,
        owner: Owner,
        deadline: Instant,
    ) -> Result<TasklessRestoreReservation, BackupRestoreError> {
        let runtime = self
            .inner
            .runtime
            .get()
            .ok_or(BackupRestoreError::UnsupportedPlugins)?
            .clone();
        let physical = Arc::new(Physical {
            held: OnceLock::new(),
            _caller: owner,
        });
        // Every physical fence session owns this same Arc. The physical
        // object owns only file/host resources, so it cannot form a cycle.
        let admission = self
            .inner
            .auth
            .prepare_backup_restore(expected_database, physical.clone(), deadline)
            .await?;
        let result = async {
            let target = TasklessRestoreTarget::observe(&admission).await?;
            let root = self.inner.root.clone();
            let keep = physical.clone();
            let files = tokio::task::spawn_blocking(move || {
                let _owned = keep;
                PinnedMutationLock::try_acquire(&root)
            })
            .await
            .map_err(|_| guard())??
            .ok_or(BackupRestoreError::Capacity)?;
            let host = runtime
                .try_hold_taskless_host()?
                .ok_or(BackupRestoreError::Capacity)?;
            physical
                .held
                .set(Holds {
                    files,
                    host,
                    runtime,
                })
                .map_err(|_| guard())?;
            let held = physical.holds()?;
            let witness = held
                .runtime
                .witness_with_hold(admission.session(), physical.clone(), Some(&held.host))
                .await?;
            let current = physical.clone();
            tokio::task::spawn_blocking(move || current.holds()?.files.validate())
                .await
                .map_err(|_| guard())??;
            if !admission.is_open() || !held.host.is_live() {
                return Err(BackupRestoreError::UnsupportedPlugins);
            }
            if TasklessRestoreTarget::observe(&admission).await? != target {
                return Err(BackupRestoreError::UnsupportedPlugins);
            }
            Ok((witness, target))
        }
        .await;
        match result {
            Ok((loaded, target)) => Ok(TasklessRestoreReservation {
                admission,
                physical,
                loaded,
                target,
            }),
            Err(error) => {
                admission.close().await?;
                match error {
                    BackupRestoreError::Store(error)
                        if matches!(*error, StoreError::Unsupported(_)) =>
                    {
                        Err(BackupRestoreError::UnsupportedPlugins)
                    }
                    other => Err(other),
                }
            }
        }
    }
}

#[cfg(test)]
mod generation_controls {
    use super::*;

    fn observed() -> TasklessRestoreTarget {
        TasklessRestoreTarget {
            database: "fixture".into(),
            database_oid: 42,
            server_started_micros: 500,
            fence_pid: 800,
            fence_started_micros: 600,
        }
    }

    #[test]
    fn target_retains_the_fenced_backend_and_physical_generation() {
        let exact = observed();
        assert!(exact == observed());
        assert!(
            exact
                != TasklessRestoreTarget {
                    database: "control".into(),
                    ..observed()
                }
        );
        assert!(
            exact
                != TasklessRestoreTarget {
                    database_oid: 43,
                    ..observed()
                }
        );
        assert!(
            exact
                != TasklessRestoreTarget {
                    server_started_micros: 501,
                    ..observed()
                }
        );
        assert!(
            exact
                != TasklessRestoreTarget {
                    fence_pid: 801,
                    ..observed()
                }
        );
        assert!(
            exact
                != TasklessRestoreTarget {
                    fence_started_micros: 601,
                    ..observed()
                }
        );
    }
}
