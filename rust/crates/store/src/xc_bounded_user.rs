//! Opt-in XC user decoding under the caller's existing memory grant.
//! Ordinary Store user lookups remain unchanged. Original Dispatcharr credit.
use crate::{Store, StoreError, User};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::json;
use sqlx::{Connection, Row};
use std::{fmt, mem::size_of};

/// These are capacity limits, not Source credential predicates. No permit or
/// memory pool is created here; the gateway must already own work_bytes.
#[derive(Clone, Copy)]
pub struct XcUserReadLimits {
    pub max_raw_bytes: usize,
    pub max_nodes: usize,
    pub max_depth: usize,
    pub work_bytes: usize,
}
pub struct BoundedXcUser {
    pub user: User,
    /// Keep this conservative portion of the SAME grant unavailable to readers
    /// until User and auth/projection temporaries are dropped.
    pub retained_bytes: usize,
}
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"XC read resource capacity is unavailable."}),
    )
}
fn malformed() -> StoreError {
    StoreError::StatusBody(500, json!({"error":"XC authentication unavailable"}))
}
impl XcUserReadLimits {
    pub fn validate(self) -> Result<Self, StoreError> {
        if self.max_raw_bytes == 0
            || self.max_raw_bytes > 1024 * 1024
            || self.max_nodes == 0
            || self.max_nodes > 65536
            || self.max_depth == 0
            || self.max_depth > 96
            || self.work_bytes < 128 * 1024
            || self.work_bytes > 64 * 1024 * 1024
        {
            return Err(capacity());
        }
        Ok(self)
    }
}

// Reserve before Value decoding, not after allocation. The quote covers raw
// PgRow/String overlap (8x wire text), recursion/decoder scratch (128KiB), and
// every scalar/container/key (4x Value+String+four pointer words, plus128).
// This deliberately bounds both dense tiny-node objects and large strings.
// Preserve-order maps have a dense entry vector and hash-index/control storage;
// 4x includes both old/new vector/index capacities during growth. Arrays and
// owned arbitrary-precision number/string tokens fit the same per-token quote.
fn quote(raw: usize, nodes: usize) -> Option<usize> {
    let token = (size_of::<serde_json::Value>() + size_of::<String>() + 4 * size_of::<usize>())
        .checked_mul(4)?
        .checked_add(128)?;
    raw.checked_mul(8)?
        .checked_add(nodes.checked_mul(token)?)?
        .checked_add(128 * 1024)
}
struct Scan {
    nodes: usize,
    limit: XcUserReadLimits,
    capacity: bool,
}
struct Seed<'a> {
    scan: &'a mut Scan,
    depth: usize,
}
// A literal key can activate serde_json's private embedded-JSON decoder.
// Refuse that uncaptured grammar before Value allocation; otherwise nested
// decoded nodes would not be covered by the outer borrowed-token count.
struct KeySeed<'a>(Seed<'a>);
impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, de: D) -> Result<(), D::Error> {
        self.0.scan.nodes += 1;
        if self.0.depth > self.0.scan.limit.max_depth
            || self.0.scan.nodes > self.0.scan.limit.max_nodes
        {
            self.0.scan.capacity = true;
            return Err(serde::de::Error::custom("capacity"));
        }
        de.deserialize_str(self)
    }
}
impl<'de> Visitor<'de> for KeySeed<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded JSON key")
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
        if value == "$serde_json::private::RawValue" {
            self.0.scan.capacity = true;
            return Err(E::custom("unsupported private decoder grammar"));
        }
        Ok(())
    }
    fn visit_borrowed_str<E: serde::de::Error>(self, value: &'de str) -> Result<(), E> {
        self.visit_str(value)
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<(), E> {
        self.visit_str(&value)
    }
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, de: D) -> Result<(), D::Error> {
        self.scan.nodes = self
            .scan
            .nodes
            .checked_add(1)
            .ok_or_else(|| serde::de::Error::custom("capacity"))?;
        if self.depth > self.scan.limit.max_depth || self.scan.nodes > self.scan.limit.max_nodes {
            self.scan.capacity = true;
            return Err(serde::de::Error::custom("capacity"));
        }
        de.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded JSON value")
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_borrowed_str<E: serde::de::Error>(self, _: &'de str) -> Result<(), E> {
        Ok(())
    }
    fn visit_string<E: serde::de::Error>(self, _: String) -> Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq
            .next_element_seed(Seed {
                scan: self.scan,
                depth: self.depth + 1,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while map
            .next_key_seed(KeySeed(Seed {
                scan: self.scan,
                depth: self.depth + 1,
            }))?
            .is_some()
        {
            map.next_value_seed(Seed {
                scan: self.scan,
                depth: self.depth + 1,
            })?;
        }
        Ok(())
    }
}
pub(crate) fn checked_properties(
    raw: &str,
    scalar_bytes: usize,
    limit: XcUserReadLimits,
) -> Result<(serde_json::Value, usize), StoreError> {
    let limit = limit.validate()?;
    if raw.len() > limit.max_raw_bytes {
        return Err(capacity());
    }
    // The non-building scanner itself is bounded before touching the decoder.
    if quote(raw.len().checked_add(scalar_bytes).ok_or_else(capacity)?, 0)
        .is_none_or(|n| n > limit.work_bytes)
    {
        return Err(capacity());
    }
    let mut scan = Scan {
        nodes: 0,
        limit,
        capacity: false,
    };
    let mut de = serde_json::Deserializer::from_str(raw);
    if (Seed {
        scan: &mut scan,
        depth: 0,
    })
    .deserialize(&mut de)
    .and_then(|()| de.end())
    .is_err()
    {
        return Err(if scan.capacity {
            capacity()
        } else {
            malformed()
        });
    }
    let retained = quote(
        raw.len().checked_add(scalar_bytes).ok_or_else(capacity)?,
        scan.nodes,
    )
    .ok_or_else(capacity)?;
    if retained > limit.work_bytes {
        return Err(capacity());
    }
    let value = serde_json::from_str(raw).map_err(|_| malformed())?;
    Ok((value, retained))
}

fn checked_lookup_input(username: &str, limits: XcUserReadLimits) -> Result<(), StoreError> {
    // This occurs at the lookup phase, after global policy/throttle checks.
    // A huge input that matches no user must not allocate unquoted arguments.
    if username.len() > limits.max_raw_bytes
        || quote(username.len(), 0).is_none_or(|n| n > limits.work_bytes)
    {
        Err(capacity())
    } else {
        Ok(())
    }
}

impl Store {
    pub(crate) async fn xc_user_by_username_bounded(
        &self,
        username: &str,
        limits: XcUserReadLimits,
    ) -> Result<Option<BoundedXcUser>, StoreError> {
        let limits = limits.validate()?;
        checked_lookup_input(username, limits)?;
        let cap = i64::try_from(limits.max_raw_bytes).map_err(|_| capacity())?;
        // Do not filter oversized rows out: that would classify capacity as a
        // missing user and mutate failed-login throttle/event state.
        let mut connection = self.pool.acquire().await?;
        let result = sqlx::query("SELECT id::bigint AS id,is_active,user_level,stream_limit,CASE WHEN octet_length(username)<=$2 THEN username END AS username,CASE WHEN octet_length(password)<=$2 THEN password END AS password,CASE WHEN octet_length(COALESCE(custom_properties,'{}'::jsonb)::text)<=$2 THEN COALESCE(custom_properties,'{}'::jsonb)::text END AS properties FROM accounts_user WHERE username=$1")
            .bind(username).bind(cap).fetch_optional(&mut *connection).await;
        if let Err(error) = Connection::flush(&mut *connection).await {
            let _closed = connection.close().await;
            return Err(error.into());
        }
        Connection::shrink_buffers(&mut *connection);
        drop(connection);
        let Some(row) = result? else {
            return Ok(None);
        };
        let username: String = row
            .try_get::<Option<String>, _>("username")?
            .ok_or_else(capacity)?;
        let password: String = row
            .try_get::<Option<String>, _>("password")?
            .ok_or_else(capacity)?;
        let raw: String = row
            .try_get::<Option<String>, _>("properties")?
            .ok_or_else(capacity)?;
        let scalars = username
            .capacity()
            .checked_add(password.capacity())
            .ok_or_else(capacity)?;
        let (custom_properties, retained_bytes) = checked_properties(&raw, scalars, limits)?;
        Ok(Some(BoundedXcUser {
            user: User {
                id: row.try_get("id")?,
                username,
                password,
                is_active: row.try_get("is_active")?,
                user_level: row.try_get("user_level")?,
                stream_limit: row.try_get("stream_limit")?,
                custom_properties,
            },
            retained_bytes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> XcUserReadLimits {
        XcUserReadLimits {
            max_raw_bytes: 65536,
            max_nodes: 4096,
            max_depth: 32,
            work_bytes: 4 * 1024 * 1024,
        }
    }
    fn status(result: Result<(serde_json::Value, usize), StoreError>) -> u16 {
        match result {
            Err(StoreError::StatusBody(status, _)) => status,
            _ => panic!("expected typed refusal"),
        }
    }
    #[test]
    fn preserves_unknown_fields_shapes_and_arbitrary_precision() {
        for raw in [
            "{}",
            "[]",
            "false",
            "null",
            "42",
            r#"{"xc_password":"x","unknown":[{},1,true,"\\u0000"],"number":123456789012345678901234567890}"#,
        ] {
            let (value, charge) = checked_properties(raw, 128, limits()).unwrap();
            assert_eq!(
                value,
                serde_json::from_str::<serde_json::Value>(raw).unwrap()
            );
            assert!(charge <= limits().work_bytes);
        }
    }
    #[test]
    fn custom_raw_and_node_limits_are_capacity_not_auth_failure() {
        let mut limit = limits();
        limit.max_raw_bytes = 8;
        assert_eq!(
            status(checked_properties(r#"{"big":"0123456789"}"#, 0, limit)),
            503
        );
        let mut limit = limits();
        limit.max_nodes = 4;
        assert_eq!(status(checked_properties("[0,0,0,0]", 0, limit)), 503);
    }
    #[test]
    fn tiny_nodes_and_overlap_can_exhaust_work_before_value_allocation() {
        let raw = format!("[{}]", vec!["0"; 1024].join(","));
        let mut limit = limits();
        limit.work_bytes = 256 * 1024;
        assert_eq!(status(checked_properties(&raw, 0, limit)), 503);
        assert_eq!(status(checked_properties("{}", 1024 * 1024, limits())), 503);
    }
    #[test]
    fn malformed_and_depth_are_not_invalid_credentials() {
        assert_eq!(status(checked_properties("{", 0, limits())), 500);
        let mut limit = limits();
        limit.max_depth = 2;
        assert_eq!(status(checked_properties("[[[0]]]", 0, limit)), 503);
        let mut limit = limits();
        limit.max_nodes = 0;
        assert_eq!(status(checked_properties("{}", 0, limit)), 503);
    }
    #[test]
    fn baseline_raw_decoder_shape_is_unchanged_and_bounded_xc_refuses_amplification() {
        let raw = r#"{"$serde_json::private::RawValue":"[0,0,0,0,0,0]"}"#;
        // Existing axum/sqlx already enable this decoder feature in806. This
        // preserves baseline ordinary Value behavior; it is NOT Source
        // authority. Only the bounded XC loader refuses the unqualified
        // embedded-tree shape before allocation, without a global rejection.
        assert!(serde_json::from_str::<serde_json::Value>(raw)
            .unwrap()
            .is_array());
        assert_eq!(status(checked_properties(raw, 0, limits())), 503);
        let nested = r#"{"unknown":{"\u0024serde_json::private::RawValue":"[0,0]"}}"#;
        assert_eq!(status(checked_properties(nested, 0, limits())), 503);
        let ordinary = r#"{"unknown":"$serde_json::private::RawValue"}"#;
        assert!(checked_properties(ordinary, 0, limits()).is_ok());
    }
    #[test]
    fn lookup_input_scratch_is_refused_before_sql_not_as_bad_login() {
        assert!(checked_lookup_input("short", limits()).is_ok());
        let mut limit = limits();
        limit.max_raw_bytes = 4;
        assert!(matches!(
            checked_lookup_input("long-name", limit),
            Err(StoreError::StatusBody(503, _))
        ));
        let mut limit = limits();
        limit.work_bytes = 256 * 1024;
        assert!(matches!(
            checked_lookup_input(&"x".repeat(32768), limit),
            Err(StoreError::StatusBody(503, _))
        ));
    }
}
