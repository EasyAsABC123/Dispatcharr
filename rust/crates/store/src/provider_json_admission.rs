//! Non-building JSON admission shared by normal provider consumers.
//! Derived from the existing recording provider's admission, not a new pool or
//! permit. Original Dispatcharr and serde authors retain algorithm credit.
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
pub(crate) struct Limits {
    pub work_bytes: usize,
    pub depth: usize,
    pub string_bytes: usize,
    pub string_copies: usize,
}
#[derive(Debug)]
pub(crate) enum Error {
    Capacity,
    Syntax(serde_json::Error),
}
struct State {
    remaining: usize,
    exhausted: bool,
    depth: usize,
    string_bytes: usize,
    string_copies: usize,
}
struct Admission<'a> {
    state: &'a mut State,
    depth: usize,
}
impl Admission<'_> {
    fn charge<E: serde::de::Error>(&mut self, n: usize) -> Result<(), E> {
        match self.state.remaining.checked_sub(n) {
            Some(v) => {
                self.state.remaining = v;
                Ok(())
            }
            None => {
                self.state.exhausted = true;
                Err(E::custom("provider JSON capacity"))
            }
        }
    }
    fn child(&mut self) -> Admission<'_> {
        Admission {
            state: self.state,
            depth: self.depth + 1,
        }
    }
    fn string<E: serde::de::Error>(&mut self, value: &str, capacity: usize) -> Result<(), E> {
        if value.len() > self.state.string_bytes {
            self.state.exhausted = true;
            return Err(E::custom("provider JSON string capacity"));
        }
        self.charge(capacity.saturating_mul(self.state.string_copies))
    }
}
impl<'de> DeserializeSeed<'de> for Admission<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(mut self, de: D) -> Result<(), D::Error> {
        if self.depth > self.state.depth {
            self.state.exhausted = true;
            return Err(serde::de::Error::custom("provider JSON depth capacity"));
        }
        self.charge::<D::Error>(256)?;
        de.deserialize_any(self)
    }
}
struct Key<'a>(Admission<'a>);
impl<'de> DeserializeSeed<'de> for Key<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(mut self, de: D) -> Result<(), D::Error> {
        if self.0.depth > self.0.state.depth {
            self.0.state.exhausted = true;
            return Err(serde::de::Error::custom("provider JSON depth capacity"));
        }
        self.0.charge::<D::Error>(256)?;
        de.deserialize_str(self)
    }
}
impl<'de> Visitor<'de> for Key<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("bounded provider JSON key")
    }
    fn visit_str<E: serde::de::Error>(mut self, value: &str) -> Result<(), E> {
        if value == "$serde_json::private::RawValue" {
            self.0.state.exhausted = true;
            return Err(E::custom("provider private decoder grammar"));
        }
        self.0.string(value, value.len())
    }
    fn visit_string<E: serde::de::Error>(mut self, value: String) -> Result<(), E> {
        if value == "$serde_json::private::RawValue" {
            self.0.state.exhausted = true;
            return Err(E::custom("provider private decoder grammar"));
        }
        self.0.string(&value, value.capacity())
    }
}
impl<'de> Visitor<'de> for Admission<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("bounded provider JSON")
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
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
    fn visit_str<E: serde::de::Error>(mut self, value: &str) -> Result<(), E> {
        self.string(value, value.len())
    }
    fn visit_string<E: serde::de::Error>(mut self, value: String) -> Result<(), E> {
        self.string(&value, value.capacity())
    }
    fn visit_seq<A: SeqAccess<'de>>(mut self, mut access: A) -> Result<(), A::Error> {
        while access.next_element_seed(self.child())?.is_some() {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(mut self, mut access: A) -> Result<(), A::Error> {
        while access.next_key_seed(Key(self.child()))?.is_some() {
            access.next_value_seed(self.child())?;
        }
        Ok(())
    }
}
/// Every visitor returns unit. No Value tree, collection, owned map key, output
/// projection or clone is built until this full pass (including end) succeeds.
/// The existing 256-byte token quote covers preserve-order map/array growth and
/// arbitrary-precision tokens; strings additionally quote their capacity times the caller's retained copy factor.
/// Wire residency and serde's bounded escaped-token scratch remain separately
/// bounded by each caller's existing body limit.
pub(crate) fn admit(bytes: &[u8], limits: Limits) -> Result<(), Error> {
    let mut state = State {
        remaining: limits.work_bytes,
        exhausted: false,
        depth: limits.depth,
        string_bytes: limits.string_bytes,
        string_copies: limits.string_copies,
    };
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    match (Admission {
        state: &mut state,
        depth: 0,
    })
    .deserialize(&mut decoder)
    .and_then(|()| decoder.end())
    {
        Ok(()) => Ok(()),
        Err(error) => Err(if state.exhausted {
            Error::Capacity
        } else {
            Error::Syntax(error)
        }),
    }
}
#[cfg(test)]
mod controls {
    use super::*;
    fn limits() -> Limits {
        Limits {
            work_bytes: 32 * 1024,
            depth: 8,
            string_bytes: 1024,
            string_copies: 8,
        }
    }
    // A builder spy proves that the production admission boundary rejects dense
    // input before a caller can begin allocating its Value tree/projections.
    fn after_admission(bytes: &[u8], called: &mut bool) -> Result<(), Error> {
        admit(bytes, limits())?;
        *called = true;
        let _: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        Ok(())
    }
    #[test]
    fn dense_arrays_reject_before_value_builder() {
        let raw = format!("[{}]", vec!["0"; 4096].join(","));
        let mut called = false;
        assert!(matches!(
            after_admission(raw.as_bytes(), &mut called),
            Err(Error::Capacity)
        ));
        assert!(!called);
    }
    #[test]
    fn nesting_and_escaped_strings_reject_before_value_builder() {
        for raw in [
            format!("{}0{}", "[".repeat(10), "]".repeat(10)),
            format!("\"{}\"", "\\u0061".repeat(1025)),
        ] {
            let mut called = false;
            assert!(matches!(
                after_admission(raw.as_bytes(), &mut called),
                Err(Error::Capacity)
            ));
            assert!(!called);
        }
    }
    #[test]
    fn syntax_stays_distinct_and_ordinary_documents_can_build() {
        let mut called = false;
        assert!(matches!(
            after_admission(b"[0,", &mut called),
            Err(Error::Syntax(_))
        ));
        assert!(!called);
        assert!(after_admission(
            br#"[{"stream_id":7,"stream_icon":null,"num":1.5}]"#,
            &mut called
        )
        .is_ok());
        assert!(called);
    }
}
