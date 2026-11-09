//! Conservative durable cleanup for known Rust input eras only.
//! Original Dispatcharr authors retain full credit; see ATTRIBUTION.md.
use super::{keys, Error, Inner, InputStore};
use std::{sync::Arc, time::Duration};
const CATALOG: &str = "dispatcharr:rust:input:gc:catalog";
const DUE: &str = "dispatcharr:rust:input:gc:due";
pub(super) fn publication_keys(channel: &str) -> Result<Vec<String>, Error> {
    let mut result = keys(channel)?.to_vec();
    result.extend([
        CATALOG.to_owned(),
        DUE.to_owned(),
        format!("live:channel:{channel}:input:buffer:rust_gc_fence"),
    ]);
    Ok(result)
}
pub(super) fn start(inner: &Arc<Inner>) {
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let Some(inner) = weak.upgrade() else { break };
            // One batch at a time. Transport supervision retains admission if
            // this future is cancelled while Redis is processing its command.
            let _ = InputStore { inner }.reap_expired(32).await;
        }
    });
}
impl InputStore {
    /// Inspect at most32 due catalog rows. Never scans arbitrary Redis keys.
    /// Returns the number of known Rust indexes actually reclaimed.
    pub async fn reap_expired(&self, batch: usize) -> Result<usize, Error> {
        if batch == 0 || batch > 32 {
            return Err(Error::Configuration);
        }
        let maximum = self.inner.config.max_catalog_entries.to_string();
        let batch = batch.to_string();
        let (count, ()): (usize, ()) = self
            .inner
            .transport
            .command(
                REAP,
                &[CATALOG.to_owned(), DUE.to_owned()],
                &[batch.as_bytes(), maximum.as_bytes()],
                (),
            )
            .await?;
        Ok(count)
    }
}
const REAP: &str = r#"
local ct=redis.call('TYPE',KEYS[1]).ok;local dt=redis.call('TYPE',KEYS[2]).ok
if (ct~='none' and ct~='hash') or (dt~='none' and dt~='zset') then return redis.error_reply('CORRUPT GC catalog') end
if redis.call('HLEN',KEYS[1])>65536 or redis.call('ZCARD',KEYS[2])>65536 then return redis.error_reply('CORRUPT GC capacity') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local due=redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',now,'LIMIT',0,ARGV[1]);local actions={}
-- Preflight the whole bounded batch before any deletion.
for _,channel in ipairs(due) do
 if string.len(channel)==0 or string.len(channel)>256 or string.find(channel,':',1,true) then return redis.error_reply('CORRUPT GC channel') end
 local entry=redis.call('HGET',KEYS[1],channel);local p='live:channel:'..channel;local b=p..':input:buffer:'
 local ot=redis.call('TYPE',p..':owner').ok;local ft=redis.call('TYPE',b..'rust_gc_fence').ok
 if (ot~='none' and ot~='string') or (ft~='none' and ft~='string') then return redis.error_reply('CORRUPT GC fence') end
 if not entry then table.insert(actions,{channel=channel,action='forget'})
 elseif string.len(entry)>1024 then return redis.error_reply('CORRUPT GC entry')
 elseif redis.call('GET',b..'rust_gc_fence')~=entry then table.insert(actions,{channel=channel,action='forget'})
 else
  local ok,row=pcall(cjson.decode,entry);if not ok or type(row)~='table' or type(row.owner)~='string' or type(row.index)~='string' then return redis.error_reply('CORRUPT GC entry') end
  local it=redis.call('TYPE',b..'index').ok;if it~='none' and it~='string' then return redis.error_reply('CORRUPT GC index') end
  local index=redis.call('GET',b..'index')
  if not index or index~=row.index then table.insert(actions,{channel=channel,action='forget',clear_marker=true})
  elseif redis.call('EXISTS',p..':owner',b..'rust_owner',b..'chunk_timestamps',b..'discontinuities')>0 then table.insert(actions,{channel=channel,action='retry'})
  elseif redis.call('EXISTS',b..'chunk:'..row.index)>0 then table.insert(actions,{channel=channel,action='retry'})
  else table.insert(actions,{channel=channel,action='delete'}) end
 end
end
local removed=0
for _,action in ipairs(actions) do
 local b='live:channel:'..action.channel..':input:buffer:'
 if action.action=='retry' then redis.call('ZADD',KEYS[2],now+5000,action.channel)
 else
  if action.action=='delete' then redis.call('DEL',b..'index',b..'rust_gc_fence');removed=removed+1 end
  if action.clear_marker then redis.call('DEL',b..'rust_gc_fence') end
  redis.call('HDEL',KEYS[1],action.channel);redis.call('ZREM',KEYS[2],action.channel)
 end
end
return removed
"#;
