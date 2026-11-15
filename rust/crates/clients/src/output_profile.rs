//! Source shared profile demand uses actual per-client profile metadata.
//! Full Original Dispatcharr credit retained.
use super::*;
impl ClientStore {
    pub async fn output_profile_client_count<G: Send + 'static>(
        &self,
        channel: &str,
        profile: i64,
        guard: G,
    ) -> Result<usize, Error> {
        if channel.is_empty() || channel.len() > 256 || channel.contains(':') || profile <= 0 {
            return Err(Error::InvalidClient);
        }
        let channel = channel.to_owned();
        let maximum = self.inner.config.max_clients;
        era::supervised(self.inner.clone(), move |mut connection| async move {
            let _guard = guard;
            let count: usize = Script::new(COUNT)
                .key(format!("live:channel:{channel}:clients"))
                .arg(format!("live:channel:{channel}:clients:"))
                .arg(profile)
                .arg(maximum)
                .invoke_async(&mut connection)
                .await
                .map_err(|_| Error::RedisUnavailable)?;
            Ok(count)
        })
        .await
    }
}
const COUNT: &str = r#"
if redis.call('SCARD',KEYS[1])>tonumber(ARGV[3]) then return redis.error_reply('profile client limit') end
local ids=redis.call('SMEMBERS',KEYS[1]);local count=0
for _,id in ipairs(ids) do
 if string.len(id)>256 then return redis.error_reply('CORRUPT profile client id') end
 local key=ARGV[1]..id;local kind=redis.call('TYPE',key).ok
 if kind~='none' and kind~='hash' then return redis.error_reply('CORRUPT profile client') end
 local pid=redis.call('HGET',key,'output_profile_id')
 if pid and string.len(pid)>32 then return redis.error_reply('CORRUPT profile id') end
 if pid==ARGV[2] then count=count+1 end
end
return count
"#;
