//! Pinned current-schema restore proof and native post-migrate rows.
//! Original Dispatcharr/Django authors retain schema/algorithm credit.
use crate::task_sql_session::{BackupPoolFence, BackupPoolFenceError};
use crate::{AuthService, StoreError, TaskSqlSession};
use serde::Deserialize;
use std::{collections::BTreeSet, sync::Arc};
use tokio::time::Instant;

#[derive(Clone, Deserialize)]
struct ContentType {
    app_label: String,
    model: String,
}
#[derive(Clone, Deserialize)]
struct Permission {
    app_label: String,
    model: String,
    name: String,
    codename: String,
}
#[derive(Deserialize)]
struct Policy {
    migrations: Vec<(String, String)>,
    content_types: Vec<ContentType>,
    permissions: Vec<Permission>,
}
#[derive(Debug)]
pub enum BackupRestoreError {
    Capacity,
    UnsupportedSchema,
    UnsupportedPlugins,
    Store(Box<StoreError>),
}
impl From<BackupPoolFenceError> for BackupRestoreError {
    fn from(value: BackupPoolFenceError) -> Self {
        match value {
            BackupPoolFenceError::Capacity => Self::Capacity,
            BackupPoolFenceError::Store(error) => Self::Store(error),
        }
    }
}
impl From<StoreError> for BackupRestoreError {
    fn from(value: StoreError) -> Self {
        Self::Store(Box::new(value))
    }
}
impl From<sqlx::Error> for BackupRestoreError {
    fn from(value: sqlx::Error) -> Self {
        StoreError::Database(value).into()
    }
}
fn policy() -> Result<Arc<Policy>, BackupRestoreError> {
    serde_json::from_str(include_str!(
        "../datasets/backup-restore-current-schema.json"
    ))
    .map(Arc::new)
    .map_err(|_| unsupported())
}
fn unsupported() -> BackupRestoreError {
    BackupRestoreError::UnsupportedSchema
}
/// Actual retained Main pool/SQL admission, not a caller-authored database ID.
/// No Clone: external supervisors may retain Arc of this SAME admission only.
pub struct BackupRestoreAdmission {
    database: String,
    server_started_micros: i64,
    expected: BTreeSet<(String, String)>,
    policy: Arc<Policy>,
    fence: BackupPoolFence,
}
impl AuthService {
    pub async fn prepare_backup_restore(
        &self,
        expected_database: String,
        owner: Arc<dyn Send + Sync>,
        deadline: Instant,
    ) -> Result<BackupRestoreAdmission, BackupRestoreError> {
        let policy = policy()?;
        let expected = policy.migrations.iter().cloned().collect::<BTreeSet<_>>();
        let fence = self.borrow_backup_pool_fence(owner, deadline).await?;
        let admitted = async {
            let (database,server_started_micros,rows) = fence.primary().run(|c| Box::pin(async move {
                let (database,server_started_micros):(String,i64) = sqlx::query_as("SELECT current_database(),(extract(epoch FROM pg_postmaster_start_time())*1000000)::bigint")
                    .fetch_one(&mut *c).await?;
                let rows: Vec<(String,String)> = sqlx::query_as("SELECT app,name FROM public.django_migrations ORDER BY app,name LIMIT 185")
                    .fetch_all(&mut *c).await?;
                Ok::<_,sqlx::Error>((database,server_started_micros,rows))
            })).await??;
            if database!=expected_database || rows.len()!=expected.len()
                || rows.into_iter().collect::<BTreeSet<_>>()!=expected { return Err(unsupported()); }
            Ok((database,server_started_micros))
        }.await;
        match admitted {
            Ok((database, server_started_micros)) => Ok(BackupRestoreAdmission {
                database,
                server_started_micros,
                expected,
                policy,
                fence,
            }),
            Err(error) => {
                fence.close().await?;
                Err(error)
            }
        }
    }
}
impl BackupRestoreAdmission {
    pub fn current_database(&self) -> &str {
        &self.database
    }
    pub fn server_started_micros(&self) -> i64 {
        self.server_started_micros
    }
    pub fn expected_migrations(&self) -> &BTreeSet<(String, String)> {
        &self.expected
    }
    pub(crate) fn is_open(&self) -> bool {
        self.fence.is_open()
    }
    pub fn session(&self) -> TaskSqlSession {
        self.fence.primary().clone()
    }
    pub(crate) fn shares_auth_writer(&self, auth: &AuthService) -> bool {
        self.fence.primary().shares_auth_writer(auth)
    }
    /// Same pinned Django no-plan post_migrate semantics: create missing natural
    /// keys only, preserving IDs and existing permission names. No Source body.
    pub async fn reconcile_current_schema(&self) -> Result<(), BackupRestoreError> {
        self.fence.clear_prepared_statements().await?;
        let expected = self.expected.clone();
        self.session()
            .run(move |c| {
                Box::pin(async move {
                    let rows: Vec<(String, String)> = sqlx::query_as(
                        "SELECT app,name FROM public.django_migrations ORDER BY app,name LIMIT 185",
                    )
                    .fetch_all(&mut *c)
                    .await?;
                    if rows.len() != expected.len()
                        || rows.into_iter().collect::<BTreeSet<_>>() != expected
                    {
                        return Err(unsupported());
                    }
                    Ok::<_, BackupRestoreError>(())
                })
            })
            .await??;
        let mut seen = BTreeSet::new();
        for content_type in &self.policy.content_types {
            if !seen.insert(content_type.app_label.clone()) {
                continue;
            }
            let label = &content_type.app_label;
            let mut types = self
                .policy
                .content_types
                .iter()
                .filter(|x| &x.app_label == label)
                .cloned()
                .collect::<Vec<_>>();
            types.sort_by(|a, b| a.model.cmp(&b.model));
            let permissions = self
                .policy
                .permissions
                .iter()
                .filter(|x| &x.app_label == label)
                .cloned()
                .collect::<Vec<_>>();
            self.session().run(move |c| Box::pin(async move {
                use sqlx::Connection;
                let mut tx=c.begin().await?;
                for value in types {
                    sqlx::query("INSERT INTO django_content_type(app_label,model) SELECT $1,$2 WHERE NOT EXISTS(SELECT 1 FROM django_content_type WHERE app_label=$1 AND model=$2)")
                        .bind(value.app_label).bind(value.model).execute(&mut *tx).await?;
                }
                tx.commit().await?;
                Ok::<_,StoreError>(())
            })).await??;
            self.session().run(move |c| Box::pin(async move {
                use sqlx::Connection;
                let mut tx=c.begin().await?;
                for value in permissions {
                    sqlx::query("INSERT INTO auth_permission(name,content_type_id,codename) SELECT $1,id,$2 FROM django_content_type t WHERE app_label=$3 AND model=$4 AND NOT EXISTS(SELECT 1 FROM auth_permission p WHERE p.content_type_id=t.id AND p.codename=$2)")
                        .bind(value.name).bind(value.codename).bind(value.app_label).bind(value.model)
                        .execute(&mut *tx).await?;
                }
                tx.commit().await?;
                Ok::<_,StoreError>(())
            })).await??;
        }
        Ok(())
    }
    pub async fn close(&self) -> Result<(), StoreError> {
        self.fence.close().await
    }
}
