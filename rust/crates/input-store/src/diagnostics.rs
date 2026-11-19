//! Bounded admin detail projection of Original ChannelStatus's decoded Redis reads.
//! Original Dispatcharr authors and contributors retain all application credit.
use super::{keys, Error, InputStore};
use serde_json::{json, Value};

const MAX_SAMPLE_BYTES: usize = 1024 * 1024;
const EXACT_INDEX_LIMIT: u64 = 9_007_199_254_740_000;

impl InputStore {
    /// Used by the authenticated channel detail route. Playback and aggregate
    /// status never transfer or decode diagnostic media samples.
    pub async fn buffer_diagnostics(&self, channel: &str, index: u64) -> Result<Value, Error> {
        if index > EXACT_INDEX_LIMIT {
            return Err(Error::InvalidKey);
        }
        let prefix = keys(channel)?[5].clone();
        let mut permit = self
            .inner
            .diagnostic_slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let maximum = MAX_SAMPLE_BYTES.to_string();
        let mut stats = json!({"chunks":index,"diagnostics":{}});
        let mut sizes = Vec::new();
        let mut found = Vec::new();
        let mut missing = Vec::new();
        let mut decode_failed = false;

        if index != 0 {
            for sequence in index.saturating_sub(4).max(1)..=index {
                let key = format!("{prefix}{sequence}");
                // Check the actual Redis value size and read it atomically. Keep
                // the permit with this physical reply through timeout/cancellation.
                let ((size, bytes), returned): ((i64, Vec<u8>), _) = self
                    .inner
                    .transport
                    .command(READ_SAMPLE, &[key], &[maximum.as_bytes()], permit)
                    .await?;
                permit = returned;
                match size {
                    -2 => return Err(Error::Capacity),
                    -1 => missing.push(sequence),
                    0 if bytes.is_empty() => (),
                    size if usize::try_from(size).ok() == Some(bytes.len())
                        && bytes.len() <= MAX_SAMPLE_BYTES =>
                    {
                        match std::str::from_utf8(&bytes) {
                            Ok(text) => {
                                let mut characters = text.chars();
                                let first = characters.next().ok_or(Error::Corrupt)?;
                                let length = 1 + characters.count();
                                if found.is_empty() {
                                    stats["diagnostics"]["first_chunk"] = json!({
                                        "index":sequence,"size":length,"ts_packets":length/188,
                                        "aligned":length%188==0,"first_byte":first.to_string()
                                    });
                                }
                                found.push(sequence);
                                sizes.push(length);
                            }
                            Err(error) => {
                                let message = source_decode_error(&bytes, error);
                                stats["diagnostics"]["exception"] = json!(message);
                                stats["error"] = json!(message);
                                decode_failed = true;
                                break;
                            }
                        }
                    }
                    _ => return Err(Error::Corrupt),
                }
            }
        }

        if !decode_failed {
            if !sizes.is_empty() {
                let total: usize = sizes.iter().sum();
                stats["avg_chunk_size"] = json!(total as f64 / sizes.len() as f64);
                stats["total_sample_bytes"] = json!(total);
                stats["estimated_ts_packets"] = json!(total / 188);
                stats["is_ts_aligned"] = json!(sizes.iter().all(|size| size % 188 == 0));
                stats["recent_chunk_sizes"] = json!(sizes);
                stats["keys_found"] = json!(found);
                stats["keys_missing"] = json!(missing);
            } else if index != 0 {
                // Keep the existing bound on the cold missing-key scan.
                let ((complete, count, names), returned): ((bool, usize, Vec<String>), _) = self
                    .inner
                    .transport
                    .command(SCAN, std::slice::from_ref(&prefix), &[], permit)
                    .await?;
                permit = returned;
                stats["diagnostics"]["all_buffer_keys"] = json!(names);
                stats["diagnostics"]["total_buffer_keys"] = json!(count);
                if !complete {
                    stats["error"] = json!("Buffer diagnostic scan limit reached");
                }
            }
        }
        let key = format!("{prefix}{index}");
        let (ttl, _permit): (i64, _) = self
            .inner
            .transport
            .command(TTL, &[key], &[], permit)
            .await?;
        stats["latest_chunk_ttl"] = json!(ttl);
        Ok(stats)
    }
}

/// Source catches CPython's strict UTF-8 error. Only its offset, byte and
/// classification are rendered. No payload text or Redis failure is exposed.
fn source_decode_error(bytes: &[u8], error: std::str::Utf8Error) -> String {
    let start = error.valid_up_to();
    let first = bytes[start];
    let (width, reason) = match error.error_len() {
        None => (bytes.len() - start, "unexpected end of data"),
        Some(width) if (0xc2..=0xf4).contains(&first) => (width, "invalid continuation byte"),
        Some(width) => (width, "invalid start byte"),
    };
    let location = if width == 1 {
        format!("byte 0x{first:02x} in position {start}")
    } else {
        format!("bytes in position {start}-{}", start + width - 1)
    };
    format!("'utf-8' codec can't decode {location}: {reason}")
}

const READ_SAMPLE: &str = r#"
if redis.call('EXISTS',KEYS[1])==0 then return {-1,''} end
local size=redis.call('STRLEN',KEYS[1])
if size>tonumber(ARGV[1]) then return {-2,''} end
return {size,redis.call('GET',KEYS[1])}
"#;
const TTL: &str = "return redis.call('TTL',KEYS[1])";
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_error_prints_offsets_without_payload() {
        for (input, expected) in [
            (
                &b"G\x1f\xff\x10"[..],
                "byte 0xff in position 2: invalid start byte",
            ),
            (
                &b"G\xe2\x82-"[..],
                "bytes in position 1-2: invalid continuation byte",
            ),
            (
                &b"G\xe2\x82"[..],
                "bytes in position 1-2: unexpected end of data",
            ),
        ] {
            let message = source_decode_error(input, std::str::from_utf8(input).unwrap_err());
            assert_eq!(message, format!("'utf-8' codec can't decode {expected}"));
        }
    }
}
