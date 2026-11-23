//! Actual current-schema restore orchestration on the existing backup worker.
//! Original Dispatcharr/Django/PostgreSQL authors retain all application credit.
use super::*;
use dispatcharr_backups::{Cancellation, CurrentSchemaRestoreAuthority};
use dispatcharr_store::{BackupRestoreAdmission, BackupRestoreError, StoreError};
struct Authority(Arc<BackupRestoreAdmission>);
impl CurrentSchemaRestoreAuthority for Authority {
    fn current_database(&self) -> &str {
        self.0.current_database()
    }
    fn server_started_micros(&self) -> i64 {
        self.0.server_started_micros()
    }
    fn expected_migrations(&self) -> &std::collections::BTreeSet<(String, String)> {
        self.0.expected_migrations()
    }
}
fn failed(message: &'static str) -> Value {
    json!({"status":"failed","error":message})
}
fn archive_failure(error: ArchiveError) -> Result<Value, Error> {
    match error {
        ArchiveError::Cancelled|ArchiveError::Timeout|ArchiveError::WorkerPanic => Err(Error::Supervisor),
        ArchiveError::Zip => Ok(failed("Native restore archive is not a readable ZIP backup")),
        ArchiveError::MissingMetadata => Ok(failed("Invalid backup: missing metadata.json")),
        ArchiveError::Metadata => Ok(failed("Native restore requires valid current-schema metadata and disabled plugins without task history")),
        ArchiveError::WrongDatabaseType => Ok(failed("Database type mismatch: current database is postgresql")),
        ArchiveError::SqliteDatabaseTypeMismatch => Ok(failed(
            "Database type mismatch: backup is sqlite, but current database is postgresql",
        )),
        ArchiveError::Capacity => Ok(failed("Native restore archive capacity exceeded")),
        _ => Ok(failed("Native PostgreSQL restore failed")),
    }
}
impl Executor {
    pub(crate) fn bind_restore_plugins(&self, plugins: PluginActions) -> Result<(), Error> {
        self.restore_plugins
            .set(plugins)
            .map_err(|_| Error::Configuration)
    }
    pub(crate) fn restore_ready(&self) -> bool {
        self.restore_plugins.get().is_some()
    }
    pub(crate) fn restore_unsettled(&self) -> bool {
        self.uncertain_restore
            .lock()
            .map_or(true, |v| !v.is_empty())
    }
    pub(crate) fn retain_restore(
        &self,
        admission: Arc<BackupRestoreAdmission>,
        owner: Arc<dyn Send + Sync>,
    ) {
        if let Ok(mut pending) = self.uncertain_restore.lock() {
            if !pending
                .iter()
                .any(|(existing, _)| Arc::ptr_eq(existing, &admission))
            {
                pending.push((admission, owner));
            }
        }
    }
    pub(crate) async fn prepare_restore(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Arc<BackupRestoreAdmission>, BackupRestoreError> {
        let plugins = self.restore_plugins.get().ok_or_else(|| {
            BackupRestoreError::Store(Box::new(StoreError::Unsupported(
                "Restore plugin reconciliation port is not bound",
            )))
        })?;
        plugins
            .require_empty_restore_plugins(owner.clone())
            .await
            .map_err(|_| BackupRestoreError::UnsupportedPlugins)?;
        self.auth
            .prepare_backup_restore(
                self.config.database.clone(),
                owner,
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            )
            .await
            .map(Arc::new)
    }
    pub(crate) async fn release_restore(
        &self,
        admission: Arc<BackupRestoreAdmission>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if admission.close().await.is_err() {
            self.retain_restore(admission, owner);
            return Err(Error::Supervisor);
        }
        Ok(())
    }
    pub(crate) async fn restore(
        &self,
        filename: String,
        admission: Arc<BackupRestoreAdmission>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Value, Error> {
        let plugins = self
            .restore_plugins
            .get()
            .ok_or(Error::Configuration)?
            .clone();
        let scoped = admission.clone();
        let physical = owner.clone();
        let owned = self.clone();
        let archive_name = filename.clone();
        let archive = tokio::task::spawn_blocking(move || {
            let _owner = physical;
            dispatcharr_backups::restore_current_schema(
                &owned.config,
                &owned.runtime,
                &owned.root.join(archive_name),
                &owned.scratch,
                &Limits::default(),
                &Cancellation::default(),
                &Authority(scoped),
            )
        })
        .await;
        let mut value = match archive {
            Ok(Ok(())) => None,
            Ok(Err(error)) => match archive_failure(error) {
                Ok(value) => Some(value),
                Err(error) => {
                    self.retain_restore(admission, owner);
                    return Err(error);
                }
            },
            Err(_) => {
                self.retain_restore(admission, owner);
                return Err(Error::Supervisor);
            }
        };
        if value.is_none() {
            match admission.reconcile_current_schema().await {
                Ok(()) => {}
                Err(BackupRestoreError::UnsupportedSchema) => {
                    value = Some(failed("Native restored schema is incompatible"))
                }
                Err(BackupRestoreError::Store(error))
                    if matches!(&*error, StoreError::Database(sqlx::Error::Database(_))) =>
                {
                    value = Some(failed("Native restored database reconciliation failed"))
                }
                Err(_) => {
                    self.retain_restore(admission, owner);
                    return Err(Error::Supervisor);
                }
            }
        }
        if value.is_none() {
            match plugins
                .reconcile_backup_restore(admission.clone(), owner.clone())
                .await
            {
                Ok(()) => {}
                // Source plugin post_migrate receiver catches real SQL exceptions.
                // Native capability guards/unknown physical outcomes remain errors.
                Err(StoreError::Database(sqlx::Error::Database(_))) => {}
                Err(_) => {
                    self.retain_restore(admission, owner);
                    return Err(Error::Supervisor);
                }
            }
        }
        if admission.close().await.is_err() {
            self.retain_restore(admission, owner);
            return Err(Error::Supervisor);
        }
        Ok(value.unwrap_or_else(|| json!({"status":"completed","filename":filename})))
    }
}

#[cfg(test)]
mod archive_failure_tests {
    use super::*;

    #[test]
    fn normal_restore_reports_the_two_observed_source_refusals() {
        for (error, message) in [
            (ArchiveError::MissingMetadata, "Invalid backup: missing metadata.json"),
            (
                ArchiveError::SqliteDatabaseTypeMismatch,
                "Database type mismatch: backup is sqlite, but current database is postgresql",
            ),
        ] {
            assert_eq!(
                archive_failure(error).unwrap(),
                json!({"status":"failed","error":message}),
            );
        }
    }
}
