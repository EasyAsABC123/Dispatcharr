//! Exact bounded, payload-free retention diagnostics for known input keys.
//! Original Dispatcharr authors/contributors retain full application credit.
use super::{keys, Error, InputStore};
use serde_json::{json, Value};
impl InputStore {
    /// Logical payload bytes and Redis MEMORY USAGE of tracked input keys.
    /// No payload transfer, TTL refresh, owner mutation or arbitrary key scan.
    pub async fn retained_diagnostics(&self, channel: &str) -> Result<Value, Error> {
        let keys = keys(channel)?;
        let config = &self.inner.config;
        let count = config.max_chunks.to_string();
        let chunk = config.max_chunk_bytes.to_string();
        let bytes = config.max_channel_bytes.to_string();
        type Row = (String, String, u64, u64, u64, u64, u64);
        let ((owner, published, index, tracked, retained, logical, allocated), ()): (Row, ()) =
            self.inner
                .transport
                .command(
                    RETAINED,
                    &keys,
                    &[count.as_bytes(), chunk.as_bytes(), bytes.as_bytes()],
                    (),
                )
                .await?;
        Ok(
            json!({"current_owner":if owner.is_empty(){None}else{Some(&owner)},"published_owner":if published.is_empty(){None}else{Some(&published)},
            "fence_matches_owner":!owner.is_empty()&&owner==published,"index":index,"tracked_chunks":tracked,"retained_chunks":retained,
            "logical_payload_bytes":logical,"redis_known_key_allocation_bytes":allocated,
            "allocation_scope":"MEMORY USAGE of tracked payload/index/timestamp/discontinuity/fence keys; excludes server allocator/database overhead"}),
        )
    }
}
const RETAINED: &str = r#"
for i=1,5 do
 local t=redis.call('TYPE',KEYS[i]).ok;local expected='string';if i==4 or i==5 then expected='zset' end
 if t~='none' and t~=expected then return redis.error_reply('CORRUPT retained key type') end
end
if redis.call('STRLEN',KEYS[1])>256 or redis.call('STRLEN',KEYS[2])>256 or redis.call('STRLEN',KEYS[3])>32 then return redis.error_reply('CORRUPT retained metadata') end
local owner=redis.call('GET',KEYS[1]) or '';local published=redis.call('GET',KEYS[2]) or ''
local index=tonumber(redis.call('GET',KEYS[3]) or '0');if not index or index<0 or index>9007199254740000 or index~=math.floor(index) then return redis.error_reply('CORRUPT retained index') end
local tracked=redis.call('ZCARD',KEYS[4]);if tracked>tonumber(ARGV[1]) or redis.call('ZCARD',KEYS[5])>tonumber(ARGV[1])+1 then return redis.error_reply('CORRUPT retained cardinality') end
local members=redis.call('ZRANGE',KEYS[4],0,-1);local logical=0;local retained=0;local allocated=0
for _,member in ipairs(members) do
 local seq=tonumber(member);if not seq or seq<=0 or seq>index or seq~=math.floor(seq) then return redis.error_reply('CORRUPT retained sequence') end
 local key=KEYS[6]..member;local t=redis.call('TYPE',key).ok;if t~='none' and t~='string' then return redis.error_reply('CORRUPT retained payload type') end
 local size=redis.call('STRLEN',key);if size>tonumber(ARGV[2]) then return redis.error_reply('CORRUPT retained chunk size') end
 logical=logical+size;if logical>tonumber(ARGV[3]) then return redis.error_reply('CORRUPT retained byte bound') end
 if size>0 then retained=retained+1 end
 allocated=allocated+(redis.call('MEMORY','USAGE',key) or 0)
end
for i=2,5 do allocated=allocated+(redis.call('MEMORY','USAGE',KEYS[i]) or 0) end
return {owner,published,index,tracked,retained,logical,allocated}
"#;
