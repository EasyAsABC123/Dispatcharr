//! Shared live-channel control derived from original ChannelService stop behavior.
use super::*;
impl ClientStore {
    pub async fn channel_client_count(&self, channel: &str) -> Result<usize, Error> {
        validate(channel)?;
        let mut connection = self.inner.connection.clone();
        let count: usize = time::timeout(
            self.inner.config.command_timeout,
            redis::cmd("SCARD")
                .arg(format!("live:channel:{channel}:clients"))
                .query_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        if count > self.inner.config.max_clients {
            return Err(Error::ScanLimit);
        }
        Ok(count)
    }
    /// Stop all existing live-format clients atomically, including HLS
    /// capabilities. Media/frame admission remains held until its actual owner
    /// drops, independently of the removed user-accounting metadata.
    pub async fn request_channel_stop(&self, channel: &str) -> Result<bool, Error> {
        validate(channel)?;
        let mut connection = self.inner.connection.clone();
        let result: i64 = time::timeout(
            self.inner.config.command_timeout,
            Script::new(STOP)
                .key(format!("live:channel:{channel}:owner"))
                .key(format!("live:channel:{channel}:metadata"))
                .key(format!("live:channel:{channel}:clients"))
                .key(format!("live:channel:{channel}:stopping"))
                .arg(channel)
                .arg(self.inner.config.max_clients)
                .arg(uuid::Uuid::new_v4().to_string())
                .arg(now())
                .arg(&self.inner.config.worker_id)
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        if result == 1 {
            let clients = self
                .inner
                .clients
                .lock()
                .unwrap()
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            for client in clients.iter().filter(|client| client.channel == channel) {
                client.stop.send_replace(true);
                if client.registered.load(Ordering::Acquire) {
                    client.released.store(true, Ordering::Release);
                    client.cleanup_done.send_replace(true);
                }
            }
            statistics::stopped_channel(&self.inner, channel, &clients);
        }
        Ok(result == 1)
    }
    pub async fn channel_is_stopping(&self, channel: &str) -> Result<bool, Error> {
        validate(channel)?;
        let mut connection = self.inner.connection.clone();
        time::timeout(
            self.inner.config.command_timeout,
            redis::cmd("EXISTS")
                .arg(format!("live:channel:{channel}:stopping"))
                .query_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)
    }
    /// Clear only this producer's stop marker after input/provider cleanup.
    pub async fn clear_channel_stop(&self, channel: &str, owner: &str) -> Result<(), Error> {
        validate(channel)?;
        let mut connection = self.inner.connection.clone();
        let _:i64=time::timeout(self.inner.config.command_timeout,Script::new("if redis.call('GET',KEYS[1])==ARGV[1] and redis.call('GET',KEYS[2])~=ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0")
            .key(format!("live:channel:{channel}:stopping")).key(format!("live:channel:{channel}:owner")).arg(owner).invoke_async(&mut connection)).await.map_err(|_|Error::RedisUnavailable)?.map_err(|_|Error::RedisUnavailable)?;
        Ok(())
    }
}
fn validate(channel: &str) -> Result<(), Error> {
    if channel.is_empty() || channel.len() > 256 || channel.contains(':') {
        Err(Error::InvalidClient)
    } else {
        Ok(())
    }
}
const STOP: &str = r#"
local owner=redis.call('GET',KEYS[1]);local meta_type=redis.call('TYPE',KEYS[2]).ok
if meta_type~='none' and meta_type~='hash' then return redis.error_reply('CORRUPT channel metadata') end
if not owner and meta_type=='none' then return 0 end
local ids=redis.call('SMEMBERS',KEYS[3]);if #ids>tonumber(ARGV[2]) then return redis.error_reply('client limit') end
local prefix='live:channel:'..ARGV[1]
for _,id in ipairs(ids) do
 local kind=redis.call('TYPE',prefix..':clients:'..id).ok;if kind~='none' and kind~='hash' then return redis.error_reply('CORRUPT client') end
 local stop=redis.call('TYPE',prefix..':client:'..id..':stop').ok;if stop~='none' and stop~='string' then return redis.error_reply('CORRUPT stop') end
 local token=redis.call('HGET',prefix..':clients:'..id,'hls_token');if token then local kind=redis.call('TYPE','live:hls:session:'..token).ok;if kind~='none' and kind~='hash' then return redis.error_reply('CORRUPT capability') end end
end
local marker=owner or redis.call('HGET',KEYS[2],'rust_token') or ARGV[3]
redis.call('SET',KEYS[4],marker,'EX',60)
if meta_type=='hash' then redis.call('HSET',KEYS[2],'state','stopping','state_changed_at',ARGV[4]) end
for _,id in ipairs(ids) do
 local key=prefix..':clients:'..id;local token=redis.call('HGET',key,'hls_token')
 if token then
  local cap='live:hls:session:'..token
  if redis.call('HGET',cap,'channel_id')==ARGV[1] and redis.call('HGET',cap,'client_id')==id then redis.call('DEL',cap) end
 end
 redis.call('SET',prefix..':client:'..id..':stop','true','EX',30)
 redis.call('DEL',key);redis.call('SREM',KEYS[3],id)
end
redis.call('PUBLISH','live:events:'..ARGV[1],cjson.encode({event='channel_stop',channel_id=ARGV[1],requester_worker_id=ARGV[5],timestamp=tonumber(ARGV[4])}))
return 1
"#;
