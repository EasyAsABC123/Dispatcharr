//! Source raw-ingress health through the existing retained Redis transport.
//! Original Dispatcharr authors retain algorithm/application credit.
use super::{keys, valid_owner, Error, InputStore};
fn health_keys(channel: &str) -> Result<Vec<String>, Error> {
    let canonical = keys(channel)?;
    Ok(vec![
        canonical[0].clone(),
        format!("live:channel:{channel}:last_data"),
    ])
}
fn parse(raw: Option<Vec<u8>>) -> Result<Option<f64>, Error> {
    match raw {
        None => Ok(None),
        Some(bytes) if bytes.is_empty() => Ok(None),
        Some(bytes) => std::str::from_utf8(&bytes)
            .map_err(|_| Error::Corrupt)?
            .trim()
            .parse::<f64>()
            .map(Some)
            .map_err(|_| Error::Corrupt),
    }
}
impl InputStore {
    /// Raw Source GET only: no ownership requirement, TTL refresh, cache or
    /// health-policy defaults. Caller implements Source's per-reader2s cache,
    /// dynamic connection_timeout and error=>false/missing=>true policy.
    pub async fn read_source_last_data_retained<G: Send + 'static>(
        &self,
        channel: &str,
        guard: G,
    ) -> Result<(Option<f64>, G), Error> {
        let keys = health_keys(channel)?;
        let (raw, guard): (Option<Vec<u8>>, G) = self
            .inner
            .transport
            .command(READ, &keys[1..], &[], guard)
            .await?;
        Ok((parse(raw)?, guard))
    }
    /// Call after successful Source-style buffer.add_chunk acceptance, including
    /// nonempty partial/subtarget buffered ingress BEFORE first Redis chunk.
    /// It must NOT require a published index. Exact coordinator owner comparison
    /// prevents an old local reader from refreshing a new owner's raw health.
    /// TTL is Source's fixed60s, independent of configured chunk_ttl.
    pub async fn put_source_last_data_retained<G: Send + 'static>(
        &self,
        channel: &str,
        owner: &str,
        accepted_at: f64,
        guard: G,
    ) -> Result<((), G), Error> {
        if !valid_owner(owner) {
            return Err(Error::InvalidKey);
        }
        if !accepted_at.is_finite() {
            return Err(Error::InvalidChunk);
        }
        let keys = health_keys(channel)?;
        let timestamp = accepted_at.to_string();
        let (stored, guard): (i64, G) = self
            .inner
            .transport
            .command(
                WRITE,
                &keys,
                &[owner.as_bytes(), timestamp.as_bytes()],
                guard,
            )
            .await?;
        if stored == 1 {
            Ok(((), guard))
        } else {
            Err(Error::LostOwnership)
        }
    }
    /// After actual upstream stop/drain, before releasing current ownership.
    /// Source owner cleanup removes this raw key with its channel namespace.
    /// A stale/non-owner cannot delete a successor's health. If ownership is
    /// already absent/lost, do not guess provenance; Source EX60 remains bound.
    pub async fn clear_source_last_data_retained<G: Send + 'static>(
        &self,
        channel: &str,
        owner: &str,
        guard: G,
    ) -> Result<(bool, G), Error> {
        if !valid_owner(owner) {
            return Err(Error::InvalidKey);
        }
        let keys = health_keys(channel)?;
        let (deleted, guard): (i64, G) = self
            .inner
            .transport
            .command(CLEAR, &keys, &[owner.as_bytes()], guard)
            .await?;
        Ok((deleted == 1, guard))
    }
}
// Source produces canonical finite epoch strings <128bytes. Bound before GET
// allocates response bytes. Wrong-type/parse/transport errors remain errors;
// callers must not convert them into a missing timestamp/unhealthy decision.
const READ: &str = r#"
local kind=redis.call('TYPE',KEYS[1]).ok
if kind=='none' then return false end
if kind~='string' or redis.call('STRLEN',KEYS[1])>128 then return redis.error_reply('CORRUPT Source last_data') end
return redis.call('GET',KEYS[1])
"#;
const WRITE: &str = r#"
if redis.call('TYPE',KEYS[1]).ok~='string' or redis.call('STRLEN',KEYS[1])>256 then return 0 end
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
redis.call('SET',KEYS[2],ARGV[2],'EX',60);return 1
"#;
const CLEAR: &str = r#"
if redis.call('TYPE',KEYS[1]).ok~='string' or redis.call('STRLEN',KEYS[1])>256 then return 0 end
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
return redis.call('DEL',KEYS[2])
"#;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn raw_missing_empty_and_exceptional_values_do_not_collapse_into_one_health_state() {
        assert_eq!(parse(None), Ok(None));
        assert_eq!(parse(Some(Vec::new())), Ok(None));
        assert_eq!(
            parse(Some(b"1770000000.125".to_vec())),
            Ok(Some(1770000000.125))
        );
        assert_eq!(
            parse(Some(b"not-a-timestamp".to_vec())),
            Err(Error::Corrupt)
        );
        assert_eq!(parse(Some(vec![255])), Err(Error::Corrupt));
        // float(raw) in Source accepts these too. Do not turn NaN into missing:
        // now-NaN >= dynamic_timeout is false; -inf compares differently.
        assert!(parse(Some(b"nan".to_vec())).unwrap().unwrap().is_nan());
        assert_eq!(parse(Some(b"-inf".to_vec())), Ok(Some(f64::NEG_INFINITY)));
    }
}
