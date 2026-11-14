//! PRIVATE UNCOMPILED wrapper over actual Source3 cleanup order. Package FILE
//! witness verifies models36bd19.../signalsda6509..., not a new database run.
//! Original Dispatcharr/Django/django-celery-beat authors retain credit.
use dispatcharr_store::{recording_beat_cleanup as sql, recording_capture::RecordingIdentity};
use std::sync::Arc;
pub(crate) struct Component {
    store: Arc<sql::Store>,
}
impl Component {
    pub(crate) fn source3_normal(pool: sqlx::PgPool) -> Self {
        Self {
            store: Arc::new(sql::Store::source3_normal(pool)),
        }
    }
    pub(crate) fn source61_local_video_audio(pool: sqlx::PgPool) -> Self {
        Self {
            store: Arc::new(sql::Store::source61_local_video_audio(pool)),
        }
    }
    pub(crate) async fn prepare(
        &self,
        identity: RecordingIdentity,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<sql::Prepared, sql::Error> {
        self.store.clone().prepare(identity, owner).await
    }
    pub(crate) async fn execute(
        &self,
        prepared: sql::Prepared,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<sql::Prefix, sql::Error> {
        self.store.clone().execute(prepared, owner).await
    }
}
