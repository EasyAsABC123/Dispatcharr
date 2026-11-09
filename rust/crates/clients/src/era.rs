//! Input-era client fencing for safe dead-owner cleanup.
//! Original Dispatcharr author credit and policy behavior remain unchanged.
use super::*;

fn validate(channel: &str, owner: &str) -> Result<(), Error> {
    if channel.is_empty()
        || channel.len() > 256
        || channel.contains(':')
        || owner.is_empty()
        || owner.len() > 256
        || owner.bytes().any(|b| b == 0)
    {
        Err(Error::InvalidClient)
    } else {
        Ok(())
    }
}
fn local(store: &Inner, channel: &str) -> Vec<Arc<ClientInner>> {
    let all = {
        let map = store.clients.lock().unwrap();
        map.values().filter_map(Weak::upgrade).collect::<Vec<_>>()
    };
    all.into_iter()
        .filter(|client| client.channel == channel)
        .collect()
}
pub(super) async fn supervised<T, F, Fut>(store: Arc<Inner>, operation: F) -> Result<T, Error>
where
    T: Send + 'static,
    F: FnOnce(MultiplexedConnection) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, Error>> + Send + 'static,
{
    let permit = match store.control_slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let _queue = store
                .control_queue
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Capacity)?;
            time::timeout(
                store.config.command_timeout,
                store.control_slots.clone().acquire_owned(),
            )
            .await
            .map_err(|_| Error::RedisUnavailable)?
            .map_err(|_| Error::RedisUnavailable)?
        }
    };
    let connection = store.connection.clone();
    let job = store.runtime.spawn(async move {
        let _permit = permit;
        operation(connection).await
    });
    time::timeout(store.config.command_timeout, job)
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?
}
impl ClientStore {
    /// Tag only this process's token-matched empty/same-era registrations.
    /// REGISTER itself reads the owner, covering jobs still queued during bind.
    pub async fn bind_local_owner(&self, channel: &str, owner: &str) -> Result<usize, Error> {
        validate(channel, owner)?;
        let snapshot_store = self.inner.clone();
        let channel = channel.to_owned();
        let owner = owner.to_owned();
        let maximum = self.inner.config.max_clients;
        supervised(self.inner.clone(), move |mut connection| async move {
            let clients = local(&snapshot_store, &channel);
            let entries = clients
                .iter()
                .filter(|client| !client.released.load(Ordering::Acquire) && !*client.stop.borrow())
                .map(|client| (&client.id, &client.token))
                .collect::<Vec<_>>();
            let encoded = serde_json::to_string(&entries).map_err(|_| Error::InvalidClient)?;
            let _clients = clients;
            Script::new(BIND)
                .key(format!("live:channel:{channel}:owner"))
                .arg(&channel)
                .arg(owner)
                .arg(encoded)
                .arg(maximum)
                .invoke_async(&mut connection)
                .await
                .map_err(|_| Error::RedisUnavailable)
        })
        .await
    }
    /// An absent coordinator owner is required. Remove only hashes tagged with
    /// the exact old input owner, preserving empty-era/new-owner/legacy rows.
    pub async fn stop_abandoned_channel(
        &self,
        channel: &str,
        old_owner: &str,
    ) -> Result<usize, Error> {
        validate(channel, old_owner)?;
        let snapshot_store = self.inner.clone();
        let channel = channel.to_owned();
        let owner = old_owner.to_owned();
        let maximum = self.inner.config.max_clients;
        supervised(self.inner.clone(), move |mut connection| async move {
            let clients = local(&snapshot_store, &channel);
            let rows: Vec<(String, String)> = Script::new(STOP_ABANDONED)
                .key(format!("live:channel:{channel}:owner"))
                .key(format!("live:channel:{channel}:clients"))
                .key(format!("live:channel:{channel}:metadata"))
                .arg(&channel)
                .arg(owner)
                .arg(maximum)
                .invoke_async(&mut connection)
                .await
                .map_err(|_| Error::RedisUnavailable)?;
            let targets: std::collections::HashSet<_> = rows
                .iter()
                .map(|(id, token)| (id.as_str(), token.as_str()))
                .collect();
            for client in clients {
                if targets.contains(&(client.id.as_str(), client.token.as_str())) {
                    client.stop.send_replace(true);
                    if client.registered.load(Ordering::Acquire) {
                        client.released.store(true, Ordering::Release);
                        client.cleanup_done.send_replace(true);
                    }
                    let announced=client.stats_announced.swap(false,Ordering::AcqRel);
                    statistics::released(&snapshot_store,&channel,&client.id,announced);
                }
            }
            Ok(rows.len())
        })
        .await
    }
}
const BIND: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[2] then return 0 end
local entries=cjson.decode(ARGV[3]);if #entries>tonumber(ARGV[4]) then return redis.error_reply('client limit') end
local selected={};local prefix='live:channel:'..ARGV[1]..':clients:'
for _,entry in ipairs(entries) do
 if #entry~=2 or string.len(entry[1])>256 or string.len(entry[2])>256 then return redis.error_reply('CORRUPT binding') end
 local key=prefix..entry[1];local kind=redis.call('TYPE',key).ok
 if kind~='none' and kind~='hash' then return redis.error_reply('CORRUPT client') end
 if redis.call('HGET',key,'rust_token')==entry[2] then
  local era=redis.call('HGET',key,'rust_input_owner') or ''
  if era=='' or era==ARGV[2] then table.insert(selected,key) end
 end
end
for _,key in ipairs(selected) do redis.call('HSET',key,'rust_input_owner',ARGV[2]) end
return #selected
"#;
const STOP_ABANDONED: &str = r#"
if redis.call('EXISTS',KEYS[1])==1 then return {} end
local metakind=redis.call('TYPE',KEYS[3]).ok
if metakind~='none' and metakind~='hash' then return redis.error_reply('CORRUPT channel metadata') end
local remove_meta=redis.call('HGET',KEYS[3],'rust_token')==ARGV[2]
if redis.call('SCARD',KEYS[2])>tonumber(ARGV[3]) then return redis.error_reply('client limit') end
local ids=redis.call('SMEMBERS',KEYS[2]);local prefix='live:channel:'..ARGV[1];local selected={}
for _,id in ipairs(ids) do
 if string.len(id)>256 then return redis.error_reply('CORRUPT client id') end
 local key=prefix..':clients:'..id;local kind=redis.call('TYPE',key).ok
 if kind~='none' and kind~='hash' then return redis.error_reply('CORRUPT client') end
 if redis.call('HGET',key,'rust_input_owner')==ARGV[2] then
  local token=redis.call('HGET',key,'rust_token')
  if not token or string.len(token)>256 then return redis.error_reply('CORRUPT client fence') end
  local stop=prefix..':client:'..id..':stop';local skind=redis.call('TYPE',stop).ok
  if skind~='none' and skind~='string' then return redis.error_reply('CORRUPT stop') end
  local hls=redis.call('HGET',key,'hls_token')
  if hls then
   if string.len(hls)>128 then return redis.error_reply('CORRUPT capability') end
   local capkind=redis.call('TYPE','live:hls:session:'..hls).ok
   if capkind~='none' and capkind~='hash' then return redis.error_reply('CORRUPT capability') end
  end
  table.insert(selected,{id,token,hls or ''})
 end
end
local result={}
for _,entry in ipairs(selected) do
 local id=entry[1];local cap='live:hls:session:'..entry[3]
 if entry[3]~='' and redis.call('HGET',cap,'channel_id')==ARGV[1] and redis.call('HGET',cap,'client_id')==id then redis.call('DEL',cap) end
 redis.call('SET',prefix..':client:'..id..':stop','true','EX',30)
 redis.call('DEL',prefix..':clients:'..id);redis.call('SREM',KEYS[2],id)
 table.insert(result,{id,entry[2]})
end
if remove_meta then redis.call('DEL',KEYS[3]) end
return result
"#;
