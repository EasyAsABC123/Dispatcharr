use super::*;
/// Read-only Source redirect allowlist capacity check. No owner/client/profile
/// assignment is created, and max_streams0 bypasses counters as in Source.
impl Coordinator {
    pub async fn redirect_profile_has_capacity(
        &self,
        provider: ProviderReservation,
        retained: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if provider.max_streams == 0 {
            return Ok(true);
        }
        let profile = provider
            .profile_id
            .filter(|id| *id > 0)
            .ok_or(Error::InvalidRequest)?;
        let group = provider
            .credential_pool
            .as_ref()
            .map(|p| p.counter_key.clone())
            .unwrap_or_default();
        if group.len() > 256
            || (!group.is_empty() && !group.starts_with("server_group_connections:"))
        {
            return Err(Error::InvalidRequest);
        }
        let group_limit = provider
            .credential_pool
            .as_ref()
            .map_or(0, |p| p.max_streams);
        let permit = self.permit().await?;
        let mut connection = self.inner.connection.clone();
        let timeout = self.inner.config.command_timeout;
        let task = tokio::spawn(async move {
            let _scope = (permit, retained);
            redis::Script::new(CAPACITY)
                .key(format!("profile_connections:{profile}"))
                .key(group)
                .arg(provider.max_streams)
                .arg(group_limit)
                .invoke_async::<i64>(&mut connection)
                .await
                .map(|v| v == 1)
                .map_err(|_| Error::RedisUnavailable)
        });
        time::timeout(timeout, task)
            .await
            .map_err(|_| Error::RedisUnavailable)?
            .map_err(|_| Error::RedisUnavailable)?
    }
}

const CAPACITY: &str = r#"
local function count(key)
 local kind=redis.call('TYPE',key).ok
 if kind=='none' then return 0 end
 if kind~='string' or redis.call('STRLEN',key)>32 then error('CORRUPT redirect capacity') end
 local n=tonumber(redis.call('GET',key))
 if not n or n~=n or math.abs(n)==math.huge or n~=math.floor(n) then error('CORRUPT redirect counter') end
 return n
end
if count(KEYS[1])>=tonumber(ARGV[1]) then return 0 end
if ARGV[2]~='0' and count(KEYS[2])>=tonumber(ARGV[2]) then return 0 end
return 1
"#;
