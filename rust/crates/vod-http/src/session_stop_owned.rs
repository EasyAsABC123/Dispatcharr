//! Bounded retained admin stop writes. Original Dispatcharr authors retain
//! application/algorithm credit. No write retry; public stop order/TTL preserved.
use super::{key, redis_error, unavailable, Inner};
use dispatcharr_store::StoreError;
use redis::AsyncCommands;
use std::sync::Arc;

pub(super) async fn stop(inner: Arc<Inner>, id: &str) -> Result<bool, StoreError> {
    if *inner.stopped.borrow() {
        return Err(unavailable());
    }
    // Admin controls must remain possible when all stream body credits are held.
    let permit = inner
        .admin_controls
        .clone()
        .try_acquire_owned()
        .map_err(|_| unavailable())?;
    let id = id.to_owned();
    let task = tokio::spawn(async move {
        let _permit = permit;
        let _service = inner;
        if *_service.stopped.borrow() {
            return Err(unavailable());
        }
        let mut connection = _service.connection.clone();
        if !connection
            .exists::<_, bool>(key(&id))
            .await
            .map_err(|_| redis_error())?
        {
            return Ok(false);
        }
        let _: () = connection
            .set_ex(format!("vod_proxy:client:{id}:stop"), "true", 60)
            .await
            .map_err(|_| redis_error())?;
        if let Some(session) = _service.sessions.lock().await.get(&id) {
            if session.scope.get().is_none() {
                session.stop.send_replace(true);
            }
        }
        Ok(true)
    });
    // Dropping this observer detaches the owned work, not its physical credit.
    task.await.map_err(|_| redis_error())?
}
