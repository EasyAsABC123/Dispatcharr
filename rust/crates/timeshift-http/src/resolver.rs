use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_store::{AuthService, ChannelIdentity, Store, StoreError, User};
use serde_json::Value;
use sqlx::Row;

#[derive(Clone)]
pub struct SessionChannel {
    pub identity: ChannelIdentity,
    pub is_catchup: bool,
    pub has_catchup_streams: bool,
}
pub trait SessionResolver: Send + Sync {
    fn authenticate<'a>(
        &'a self,
        request: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<User, StoreError>>;
    fn enabled<'a>(&'a self, user: &'a User) -> BoxFuture<'a, Result<bool, StoreError>>;
    fn channel<'a>(
        &'a self,
        user: &'a User,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'a, Result<Option<SessionChannel>, StoreError>>;
    fn bound_channel(
        &self,
        id: i64,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'_, Result<Option<ChannelIdentity>, StoreError>>;
    fn user(&self, id: i64) -> BoxFuture<'_, Result<Option<User>, StoreError>>;
}
#[derive(Clone)]
pub struct SqlSessionResolver {
    store: Store,
    auth: AuthService,
}
impl SqlSessionResolver {
    pub fn new(store: Store, auth: AuthService) -> Self {
        Self { store, auth }
    }
}
impl SessionResolver for SqlSessionResolver {
    fn authenticate<'a>(
        &'a self,
        request: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<User, StoreError>> {
        Box::pin(async move {
            let user = self
                .auth
                .authenticate(request)
                .await?
                .ok_or(StoreError::Unauthorized(
                    "Authentication credentials were not provided.",
                ))?;
            if user.user_level < 1
                || !self
                    .store
                    .network_allowed(self.auth.client_ip(request), "UI", Some(&user))
                    .await?
            {
                return Err(StoreError::PermissionDenied);
            }
            if !self
                .store
                .network_allowed(self.auth.client_ip(request), "STREAMS", Some(&user))
                .await?
            {
                return Err(StoreError::Forbidden);
            }
            Ok(user)
        })
    }
    fn enabled<'a>(&'a self, user: &'a User) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            if user.custom_properties.get("catchup_enabled") == Some(&Value::Bool(false)) {
                return Ok(false);
            }
            Ok(self
                .store
                .setting("system_settings")
                .await?
                .get("catchup_enabled")
                != Some(&Value::Bool(false)))
        })
    }
    fn channel<'a>(
        &'a self,
        user: &'a User,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'a, Result<Option<SessionChannel>, StoreError>> {
        Box::pin(async move {
            let Some(identity) = self.store.channel_for_user(user, uuid).await? else {
                return Ok(None);
            };
            let row=sqlx::query("SELECT c.is_catchup,EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active) AS has_stream FROM dispatcharr_channels_channel c WHERE c.id=$1").bind(identity.id).fetch_optional(self.store.pool()).await?;
            row.map(|row| {
                Ok(SessionChannel {
                    identity,
                    is_catchup: row.try_get("is_catchup")?,
                    has_catchup_streams: row.try_get("has_stream")?,
                })
            })
            .transpose()
        })
    }
    fn bound_channel(
        &self,
        id: i64,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'_, Result<Option<ChannelIdentity>, StoreError>> {
        Box::pin(async move {
            let row=sqlx::query("SELECT id::bigint AS id,uuid,name FROM dispatcharr_channels_channel WHERE id=$1 AND uuid=$2").bind(id).bind(uuid).fetch_optional(self.store.pool()).await?;
            row.map(|row| {
                let name: String = row.try_get("name")?;
                Ok(ChannelIdentity {
                    id: row.try_get("id")?,
                    uuid: row.try_get("uuid")?,
                    effective_name: name.clone(),
                    name,
                })
            })
            .transpose()
        })
    }
    fn user(&self, id: i64) -> BoxFuture<'_, Result<Option<User>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .store
                .user_by_id(id)
                .await?
                .filter(|user| user.is_active))
        })
    }
}
