//! Bounded metadata-only projection of the original ChannelStatus buffer probe.
use super::{keys, Error, InputStore};
use serde_json::{json, Value};

impl InputStore {
    /// Samples the same five recent canonical keys as the original status API.
    /// No media payload is transferred: only lengths, first bytes and TTLs.
    pub async fn buffer_diagnostics(&self, channel: &str, index: u64) -> Result<Value, Error> {
        let keys = keys(channel)?;
        let index_arg = index.to_string();
        type Sample = (u64, i64, i64);
        let ((ttl, samples), ()): ((i64, Vec<Sample>), ()) = self
            .inner
            .transport
            .command(SAMPLE, &[keys[5].clone()], &[index_arg.as_bytes()], ())
            .await?;
        let mut stats = json!({"chunks":index,"diagnostics":{},"latest_chunk_ttl":ttl});
        let mut sizes = Vec::new();
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for (sequence, size, first) in samples {
            if size < 0 {
                missing.push(sequence);
            } else if size > 0 {
                if found.is_empty() {
                    stats["diagnostics"]["first_chunk"] = json!({"index":sequence,"size":size,"ts_packets":size/188,"aligned":size%188==0,"first_byte":first});
                }
                found.push(sequence);
                sizes.push(size);
            }
        }
        if !sizes.is_empty() {
            let total: i64 = sizes.iter().sum();
            stats["avg_chunk_size"] = json!(total as f64 / sizes.len() as f64);
            stats["total_sample_bytes"] = json!(total);
            stats["estimated_ts_packets"] = json!(total / 188);
            stats["is_ts_aligned"] = json!(sizes.iter().all(|size| size % 188 == 0));
            stats["recent_chunk_sizes"] = json!(sizes);
            stats["keys_found"] = json!(found);
            stats["keys_missing"] = json!(missing);
        } else if index > 0 {
            // A bounded diagnostic scan is deliberately separate from the hot
            // publisher/read path. Fail explicitly if the keyspace scan exceeds
            // its budget rather than silently report that real keys are absent.
            let ((complete, count, names), ()): ((bool, usize, Vec<String>), ()) = self
                .inner
                .transport
                .command(SCAN, &[keys[5].clone()], &[], ())
                .await?;
            stats["diagnostics"]["all_buffer_keys"] = json!(names);
            stats["diagnostics"]["total_buffer_keys"] = json!(count);
            if !complete {
                stats["error"] = json!("Buffer diagnostic scan limit reached");
            }
        }
        Ok(stats)
    }
}
const SAMPLE: &str = r#"
local index=tonumber(ARGV[1]);local samples={}
for i=math.max(1,index-4),index do
 local key=KEYS[1]..i
 if redis.call('EXISTS',key)==0 then table.insert(samples,{i,-1,-1})
 else
  local size=redis.call('STRLEN',key);local byte=redis.call('GETRANGE',key,0,0)
  table.insert(samples,{i,size,string.byte(byte) or -1})
 end
end
return {redis.call('TTL',KEYS[1]..index),samples}
"#;
const SCAN: &str = r#"
local cursor='0';local names={};local count=0
for round=1,128 do
 local result=redis.call('SCAN',cursor,'MATCH',KEYS[1]..'*','COUNT',100)
 cursor=result[1]
 for _,key in ipairs(result[2]) do count=count+1;if #names<20 then table.insert(names,key) end end
 if cursor=='0' or count>=20 then return {1,count,names} end
end
return {0,count,names}
"#;
