//! Original HLS capability Lua is retained verbatim from Dispatcharr.
//! Full original author credit: ATTRIBUTION.md; AGPL-3.0-only.
use super::*;

#[derive(Clone, Debug)]
pub struct HlsSessionInfo {
    pub channel_id: String,
    pub client_id: String,
    pub user_id: Option<i64>,
    pub metadata: BTreeMap<String, String>,
}

fn valid_token(token: &str) -> bool {
    token.len() == 32
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

async fn invoke<T: redis::FromRedisValue>(
    store: &Inner,
    source: &str,
    keys: &[String],
    args: &[String],
) -> Result<T, Error> {
    let script = Script::new(source);
    let mut invocation = script.prepare_invoke();
    for key in keys {
        invocation.key(key);
    }
    for arg in args {
        invocation.arg(arg);
    }
    let mut connection = store.connection.clone();
    time::timeout(
        store.config.command_timeout,
        invocation.invoke_async(&mut connection),
    )
    .await
    .map_err(|_| Error::RedisUnavailable)?
    .map_err(|_| Error::RedisUnavailable)
}

impl ClientLease {
    pub async fn bind_hls_token(&self, token: &str, user_id: Option<i64>) -> Result<(), Error> {
        if !valid_token(token) || self.inner.output_format != "hls" || user_id != self.inner.user_id
        {
            return Err(Error::InvalidClient);
        }
        let store = self.inner.store.upgrade().ok_or(Error::Stopped)?;
        let result: i64 = invoke(
            &store,
            MINT,
            &[
                keys(&self.inner.channel, &self.inner.id).0,
                format!("live:hls:session:{token}"),
            ],
            &[
                self.inner.channel.clone(),
                self.inner.id.clone(),
                token.to_owned(),
                store.config.ttl.as_secs().to_string(),
                user_id.map_or(String::new(), |id| id.to_string()),
            ],
        )
        .await?;
        if result == 1 {
            Ok(())
        } else {
            Err(Error::Stopped)
        }
    }
}

impl ClientStore {
    pub async fn claim_output_owner(
        &self,
        channel: &str,
        format: &str,
        token: &str,
        ttl: Duration,
    ) -> Result<bool, Error> {
        output_args(channel, format, token, ttl)?;
        let key = format!("live:channel:{channel}:output:{format}:owner");
        let result:i64=invoke(&self.inner,
            "local old=redis.call('GET',KEYS[1]); if old and old~=ARGV[1] then return 0 end redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2]); return 1",
            &[key],&[token.to_owned(),ttl.as_millis().to_string()]).await?;
        Ok(result == 1)
    }

    pub async fn renew_output_owner(
        &self,
        channel: &str,
        format: &str,
        token: &str,
        ttl: Duration,
    ) -> Result<bool, Error> {
        output_args(channel, format, token, ttl)?;
        let result:i64=invoke(&self.inner,
            "if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end redis.call('PEXPIRE',KEYS[1],ARGV[2]); return 1",
            &[format!("live:channel:{channel}:output:{format}:owner")],&[token.to_owned(),ttl.as_millis().to_string()]).await?;
        Ok(result == 1)
    }

    pub async fn publish_output_playlist(
        &self,
        channel: &str,
        format: &str,
        token: &str,
        descriptor: &serde_json::Value,
        ttl: Duration,
    ) -> Result<bool, Error> {
        output_args(channel, format, token, ttl)?;
        let encoded = descriptor.to_string();
        if encoded.len() > 65_536 {
            return Err(Error::InvalidClient);
        }
        let result:i64=invoke(&self.inner,
            "if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end redis.call('SET',KEYS[2],ARGV[2],'PX',ARGV[3]); return 1",
            &[format!("live:channel:{channel}:output:{format}:owner"),format!("live:channel:{channel}:output:{format}:playlist")],
            &[token.to_owned(),encoded,ttl.as_millis().to_string()]).await?;
        Ok(result == 1)
    }

    pub async fn release_output_owner(
        &self,
        channel: &str,
        format: &str,
        token: &str,
    ) -> Result<bool, Error> {
        output_args(channel, format, token, Duration::from_secs(1))?;
        let result:i64=invoke(&self.inner,
            "if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end redis.call('DEL',KEYS[1],KEYS[2]); return 1",
            &[format!("live:channel:{channel}:output:{format}:owner"),format!("live:channel:{channel}:output:{format}:playlist")],&[token.to_owned()]).await?;
        Ok(result == 1)
    }
    pub async fn touch_hls_session(&self, token: &str) -> Result<Option<HlsSessionInfo>, Error> {
        if !valid_token(token) {
            return Ok(None);
        }
        let values: Vec<String> = invoke(
            &self.inner,
            TOUCH,
            &[format!("live:hls:session:{token}")],
            &[
                now().to_string(),
                self.inner.config.ttl.as_secs().to_string(),
                token.to_owned(),
            ],
        )
        .await?;
        match values.first().map(String::as_str) {
            Some("1") => Err(Error::Stopped),
            Some("3") if values.len() >= 3 => {
                let metadata = values[3..]
                    .chunks_exact(2)
                    .map(|pair| (pair[0].clone(), pair[1].clone()))
                    .collect::<BTreeMap<_, _>>();
                let user_id = metadata
                    .get("user_id")
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|id| *id > 0);
                Ok(Some(HlsSessionInfo {
                    channel_id: values[1].clone(),
                    client_id: values[2].clone(),
                    user_id,
                    metadata,
                }))
            }
            _ => Ok(None),
        }
    }

    /// Read the current capability binding and client activity without refreshing
    /// TTL or last_active. Used by replica-local HLS cache/demand cleanup.
    pub async fn inspect_hls_session(&self, token: &str) -> Result<Option<HlsSessionInfo>, Error> {
        if !valid_token(token) {
            return Ok(None);
        }
        let values: Vec<String> = invoke(&self.inner, r#"
local channel=redis.call('HGET',KEYS[1],'channel_id'); local client=redis.call('HGET',KEYS[1],'client_id')
if not channel or not client then return {} end
local row='live:channel:'..channel..':clients:'..client
if redis.call('EXISTS',row)==0 or redis.call('HGET',row,'hls_token')~=ARGV[1] then return {} end
if redis.call('EXISTS','live:channel:'..channel..':stopping')==1 or redis.call('EXISTS','live:channel:'..channel..':client:'..client..':stop')==1 or redis.call('HGET',row,'rust_preempted')=='1' then return {} end
local result={channel,client};local metadata=redis.call('HGETALL',row)
for _,value in ipairs(metadata) do table.insert(result,value) end
return result
"#, &[format!("live:hls:session:{token}")], &[token.to_owned()]).await?;
        if values.len() < 2 {
            return Ok(None);
        }
        let metadata = values[2..]
            .chunks_exact(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect::<BTreeMap<_, _>>();
        let user_id = metadata
            .get("user_id")
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|id| *id > 0);
        Ok(Some(HlsSessionInfo {
            channel_id: values[0].clone(),
            client_id: values[1].clone(),
            user_id,
            metadata,
        }))
    }

    pub async fn get_hls_session(&self, token: &str) -> Result<Option<HlsSessionInfo>, Error> {
        if !valid_token(token) {
            return Ok(None);
        }
        let mut connection = self.inner.connection.clone();
        let metadata: BTreeMap<String, String> = time::timeout(
            self.inner.config.command_timeout,
            redis::cmd("HGETALL")
                .arg(format!("live:hls:session:{token}"))
                .query_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        let (Some(channel), Some(client)) = (metadata.get("channel_id"), metadata.get("client_id"))
        else {
            return Ok(None);
        };
        Ok(Some(HlsSessionInfo {
            channel_id: channel.clone(),
            client_id: client.clone(),
            user_id: metadata
                .get("user_id")
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|id| *id > 0),
            metadata,
        }))
    }

    pub async fn drop_hls_token(&self, token: &str) -> Result<(), Error> {
        if !valid_token(token) {
            return Ok(());
        }
        let mut connection = self.inner.connection.clone();
        let _: i64 = time::timeout(
            self.inner.config.command_timeout,
            redis::cmd("DEL")
                .arg(format!("live:hls:session:{token}"))
                .query_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        Ok(())
    }

    pub async fn enter_hls_cold_start(&self, token: &str) -> Result<Option<i64>, Error> {
        if !valid_token(token) {
            return Ok(None);
        }
        let count: i64 = invoke(
            &self.inner,
            ENTER,
            &[format!("live:hls:session:{token}")],
            &[],
        )
        .await?;
        Ok((count >= 0).then_some(count))
    }
    pub async fn exit_hls_cold_start(&self, token: &str) -> Result<Option<i64>, Error> {
        if !valid_token(token) {
            return Ok(None);
        }
        let count: i64 = invoke(
            &self.inner,
            EXIT,
            &[format!("live:hls:session:{token}")],
            &[],
        )
        .await?;
        Ok((count >= 0).then_some(count))
    }
    pub async fn claim_abandoned_hls(
        &self,
        token: &str,
        playlist_key: &str,
        since: f64,
    ) -> Result<Option<(String, String)>, Error> {
        if !valid_token(token) || !since.is_finite() {
            return Ok(None);
        }
        let values: Vec<String> = invoke(
            &self.inner,
            CLAIM,
            &[format!("live:hls:session:{token}"), playlist_key.to_owned()],
            &[since.to_string()],
        )
        .await?;
        if values.len() == 3 && values[0] == "1" {
            Ok(Some((values[1].clone(), values[2].clone())))
        } else {
            Ok(None)
        }
    }
}

fn output_args(channel: &str, format: &str, token: &str, ttl: Duration) -> Result<(), Error> {
    let format_ok = matches!(format, "hls" | "fmp4")
        || format.split_once(":p").is_some_and(|(kind, id)| {
            matches!(kind, "hls" | "fmp4") && id.parse::<u64>().is_ok_and(|value| value > 0)
        });
    if channel.is_empty()
        || channel.len() > 256
        || channel.contains(':')
        || !format_ok
        || format.len() > 64
        || token.is_empty()
        || token.len() > 128
        || ttl.as_millis() < 1
        || ttl > Duration::from_secs(3600)
    {
        return Err(Error::InvalidClient);
    }
    Ok(())
}

const MINT: &str = r###"
local client_key = KEYS[1]
local session_key = KEYS[2]
if redis.call('EXISTS', client_key) == 0 then
  return 0
end
local old = redis.call('HGET', client_key, 'hls_token')
if old and old ~= false and old ~= ARGV[3] then
  redis.call('DEL', 'live:hls:session:' .. old)
end
redis.call(
  'HSET', session_key,
  'channel_id', ARGV[1],
  'client_id', ARGV[2],
  'user_id', ARGV[5]
)
redis.call('EXPIRE', session_key, tonumber(ARGV[4]))
redis.call('HSET', client_key, 'hls_token', ARGV[3])
redis.call('EXPIRE', client_key, tonumber(ARGV[4]))
return 1
"###;

const TOUCH: &str = r###"
local session_key = KEYS[1]
local channel_id = redis.call('HGET', session_key, 'channel_id')
local client_id = redis.call('HGET', session_key, 'client_id')
if (not channel_id) or channel_id == false or (not client_id) or client_id == false then
  redis.call('DEL', session_key)
  return {0}
end
local client_key = 'live:channel:' .. channel_id .. ':clients:' .. client_id
local clients_key = 'live:channel:' .. channel_id .. ':clients'
local stopping_key = 'live:channel:' .. channel_id .. ':stopping'
local client_stop_key = 'live:channel:' .. channel_id .. ':client:' .. client_id .. ':stop'
if redis.call('EXISTS', stopping_key) == 1 or redis.call('EXISTS', client_stop_key) == 1 then
  redis.call('DEL', session_key)
  return {1}
end
local hash = redis.call('HGETALL', client_key)
if #hash == 0 then
  redis.call('SREM', clients_key, client_id)
  redis.call('DEL', session_key)
  return {2}
end
local bound = redis.call('HGET', client_key, 'hls_token')
if bound ~= ARGV[3] then
  redis.call('DEL', session_key)
  return {0}
end
local ttl = tonumber(ARGV[2])
redis.call('HSET', client_key, 'last_active', ARGV[1])
redis.call('EXPIRE', client_key, ttl)
redis.call('SADD', clients_key, client_id)
redis.call('EXPIRE', clients_key, ttl)
redis.call('EXPIRE', session_key, ttl)
local reply = {3, channel_id, client_id}
for i = 1, #hash do
  reply[#reply + 1] = hash[i]
end
return reply
"###;

const ENTER: &str = r###"
local session_key = KEYS[1]
if redis.call('EXISTS', session_key) == 0 then
  return -1
end
return redis.call('HINCRBY', session_key, 'cold_streams', 1)
"###;

const EXIT: &str = r###"
local session_key = KEYS[1]
if redis.call('EXISTS', session_key) == 0 then
  return -1
end
local held = redis.call('HINCRBY', session_key, 'cold_streams', -1)
if held < 0 then
  redis.call('HSET', session_key, 'cold_streams', 0)
  return 0
end
return held
"###;

const CLAIM: &str = r###"
local session_key = KEYS[1]
local playlist_key = KEYS[2]
local channel_id = redis.call('HGET', session_key, 'channel_id')
local client_id = redis.call('HGET', session_key, 'client_id')
if (not channel_id) or channel_id == false or (not client_id) or client_id == false then
  return {0}
end
local held = tonumber(redis.call('HGET', session_key, 'cold_streams'))
if held and held > 0 then
  return {0}
end
if redis.call('EXISTS', playlist_key) == 1 then
  return {0}
end
local client_key = 'live:channel:' .. channel_id .. ':clients:' .. client_id
local last_active = tonumber(redis.call('HGET', client_key, 'last_active'))
if last_active and last_active > tonumber(ARGV[1]) then
  return {0}
end
redis.call('DEL', session_key)
return {1, channel_id, client_id}
"###;
