use crate::{SessionResolver, SqlSessionResolver};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{AuthService, ChannelIdentity, Store, StoreError, User};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const HANDSHAKE_TTL_SECONDS: u64 = 60;
pub const SESSION_IDLE_TTL_SECONDS: u64 = 600;
#[derive(Clone)]
pub struct SessionConfig {
    pub max_operations: usize,
    pub max_queued_operations: usize,
    pub command_timeout: Duration,
    pub position_scan_limit: usize,
}
impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_operations: 64,
            max_queued_operations: 1024,
            command_timeout: Duration::from_secs(3),
            position_scan_limit: 50000,
        }
    }
}
#[derive(Clone)]
pub struct CatchupSessions {
    pub(crate) resolver: Arc<dyn SessionResolver>,
    transport: RedisTransport,
    config: SessionConfig,
}
pub struct CatchupSession {
    pub session_id: String,
    pub channel_uuid: uuid::Uuid,
    pub expires_at: u64,
    pub start: String,
    pub duration: Option<u32>,
}
impl CatchupSession {
    pub fn json(&self) -> Value {
        json!({"session_id":self.session_id,"playback_url":format!("/proxy/catchup/{}?session_id={}",self.channel_uuid,self.session_id),"expires_at":self.expires_at,"channel_uuid":self.channel_uuid.to_string(),"start":self.start,"duration":self.duration})
    }
}
pub struct SessionBinding {
    pub user: User,
    pub channel: ChannelIdentity,
    pub start: String,
    pub duration: Option<String>,
}
fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"error":"Session service unavailable"}))
}
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 256
        && !id
            .bytes()
            .any(|b| b == b':' || b == b'*' || b == b'?' || b == b'[' || b == b']')
}
fn key(id: &str) -> String {
    format!("timeshift:session:{id}")
}
impl CatchupSessions {
    pub async fn new(
        store: Store,
        auth: AuthService,
        redis_url: &str,
        config: SessionConfig,
    ) -> Result<Self, StoreError> {
        Self::with_resolver(
            Arc::new(SqlSessionResolver::new(store, auth)),
            redis_url,
            config,
        )
        .await
    }
    pub async fn with_resolver(
        resolver: Arc<dyn SessionResolver>,
        redis_url: &str,
        config: SessionConfig,
    ) -> Result<Self, StoreError> {
        if config.position_scan_limit == 0 {
            return Err(unavailable());
        }
        let transport = RedisTransport::connect(
            redis_url,
            TransportConfig {
                max_operations: config.max_operations,
                max_queued_operations: config.max_queued_operations,
                command_timeout: config.command_timeout,
            },
        )
        .await
        .map_err(|_| unavailable())?;
        Ok(Self {
            resolver,
            transport,
            config,
        })
    }
    pub async fn create(
        &self,
        user: &User,
        channel: uuid::Uuid,
        start: String,
        duration: Option<u32>,
    ) -> Result<CatchupSession, StoreError> {
        if !self.resolver.enabled(user).await? {
            return Err(StoreError::StatusBody(
                403,
                json!({"error":"Catch-up is disabled"}),
            ));
        }
        if dispatcharr_timeshift_core::parse_timestamp(&start).is_none() {
            return Err(StoreError::StatusBody(
                400,
                json!({"error":"Invalid start timestamp"}),
            ));
        }
        let channel = self
            .resolver
            .channel(user, channel)
            .await?
            .ok_or(StoreError::StatusBody(
                404,
                json!({"detail":"Channel not found"}),
            ))?;
        if !channel.is_catchup || !channel.has_catchup_streams {
            return Err(StoreError::StatusBody(
                400,
                json!({"error":"Catch-up not supported for this channel"}),
            ));
        }
        let id = URL_SAFE_NO_PAD.encode(uuid::Uuid::new_v4().as_bytes());
        let created = now() as u64;
        let fields=json!({"user_id":user.id.to_string(),"channel_id":channel.identity.id.to_string(),"channel_uuid":channel.identity.uuid.to_string(),"start":start,"created_at":created.to_string()}).to_string();
        let duration_string = duration.map(|v| v.to_string()).unwrap_or_default();
        let (result, _): (i64, ()) = self
            .transport
            .command(
                CREATE,
                &[key(&id)],
                &[fields.as_bytes(), duration_string.as_bytes()],
                (),
            )
            .await
            .map_err(|_| unavailable())?;
        if result != 1 {
            return Err(unavailable());
        }
        Ok(CatchupSession {
            session_id: id,
            channel_uuid: channel.identity.uuid,
            expires_at: created + HANDSHAKE_TTL_SECONDS,
            start,
            duration,
        })
    }
    pub async fn owns(&self, id: &str, user: i64) -> bool {
        if !valid_id(id) {
            return false;
        }
        self.record(id)
            .await
            .and_then(|record| record.get("user_id").and_then(|v| v.parse::<i64>().ok()))
            .is_some_and(|owner| owner == user)
    }
    pub async fn delete(&self, id: &str, user: i64) -> bool {
        if !valid_id(id) {
            return false;
        }
        let user = user.to_string();
        self.transport.command::<i64,_>("if redis.call('HGET',KEYS[1],'user_id')~=ARGV[1] then return 0 end return redis.call('DEL',KEYS[1])",&[key(id)],&[user.as_bytes()],()).await.is_ok_and(|(deleted,_)|deleted==1)
    }
    async fn record(&self, id: &str) -> Option<HashMap<String, String>> {
        if !valid_id(id) {
            return None;
        }
        let (record, _) = self
            .transport
            .command::<HashMap<String, String>, _>(
                "return redis.call('HGETALL',KEYS[1])",
                &[key(id)],
                &[],
                (),
            )
            .await
            .ok()?;
        if record.is_empty() {
            None
        } else {
            Some(record)
        }
    }
    pub async fn resolve(
        &self,
        id: &str,
        uuid: uuid::Uuid,
    ) -> Result<Option<SessionBinding>, StoreError> {
        let Some(mut record) = self.record(id).await else {
            return Ok(None);
        };
        if record.get("channel_uuid").map(String::as_str) != Some(uuid.to_string().as_str()) {
            return Ok(None);
        }
        let (Some(owner), Some(channel_id), Some(start)) = (
            record.get("user_id").and_then(|v| v.parse::<i64>().ok()),
            record.get("channel_id").and_then(|v| v.parse::<i64>().ok()),
            record.get("start").filter(|v| !v.is_empty()).cloned(),
        ) else {
            return Ok(None);
        };
        let pool_user = self
            .transport
            .command::<Option<String>, _>(
                "return redis.call('HGET',KEYS[1],'user_id')",
                &[format!("timeshift:pool:{id}")],
                &[],
                (),
            )
            .await
            .ok()
            .and_then(|(value, _)| value)
            .and_then(|v| v.parse::<i64>().ok());
        let Some(user) = self.resolver.user(pool_user.unwrap_or(owner)).await? else {
            return Ok(None);
        };
        let Some(channel) = self.resolver.bound_channel(channel_id, uuid).await? else {
            return Ok(None);
        };
        if user.id != owner && self.resolver.channel(&user, uuid).await?.is_none() {
            return Ok(None);
        }
        let _ = self
            .transport
            .command::<i64, _>(
                "return redis.call('EXPIRE',KEYS[1],600)",
                &[key(id)],
                &[],
                (),
            )
            .await;
        Ok(Some(SessionBinding {
            user,
            channel,
            start,
            duration: record.remove("duration"),
        }))
    }
    pub async fn position(&self, id: &str, user: i64, position: f64, paused: Option<bool>) -> bool {
        if !valid_id(id) || !position.is_finite() || !(0.0..=28800.0).contains(&position) {
            return false;
        }
        let user = user.to_string();
        let position = format!("{position:?}");
        let timestamp = now().to_string();
        let paused = paused.map(|v| if v { "1" } else { "0" }).unwrap_or("");
        let limit = self.config.position_scan_limit.to_string();
        self.transport
            .command::<i64, _>(
                POSITION,
                &[key(id)],
                &[
                    id.as_bytes(),
                    user.as_bytes(),
                    position.as_bytes(),
                    timestamp.as_bytes(),
                    paused.as_bytes(),
                    limit.as_bytes(),
                ],
                (),
            )
            .await
            .is_ok_and(|(result, _)| result == 1)
    }
}
const CREATE: &str = r#"
if redis.call('EXISTS',KEYS[1])==1 then return 0 end
local data=cjson.decode(ARGV[1])
for field,value in pairs(data) do redis.call('HSET',KEYS[1],field,value) end
if ARGV[2]~='' then redis.call('HSET',KEYS[1],'duration',ARGV[2]) end
redis.call('EXPIRE',KEYS[1],60)
return 1
"#;
const POSITION: &str = r#"
if redis.call('HGET',KEYS[1],'user_id')~=ARGV[2] then return 0 end
local cursor='0'
local examined=0
repeat
  local batch=redis.call('SCAN',cursor,'MATCH','timeshift:channel:*:clients:'..ARGV[1],'COUNT',100)
  cursor=batch[1]
  for _,client in ipairs(batch[2]) do
    examined=examined+1
    if examined>tonumber(ARGV[6]) then return 0 end
    local kind=redis.call('TYPE',client)
    if type(kind)=='table' then kind=kind.ok end
    if kind=='hash' then
      local user=redis.call('HGET',client,'user_id')
      if user and user~=ARGV[2] then return 0 end
      local channel=string.match(client,'^timeshift:channel:(.+):clients:')
      if not channel then return 0 end
      redis.call('HSET',client,'playback_base_secs',ARGV[3],'position_anchor_at',ARGV[4],'last_active',ARGV[4])
      if ARGV[5]=='1' then redis.call('HSET',client,'paused','1') elseif ARGV[5]=='0' then redis.call('HDEL',client,'paused') end
      redis.call('EXPIRE',client,60)
      redis.call('EXPIRE','timeshift:channel:'..channel..':clients',60)
      redis.call('EXPIRE','timeshift:channel:'..channel..':metadata',60)
      redis.call('EXPIRE',KEYS[1],600)
      return 1
    end
  end
until cursor=='0'
return 0
"#;
