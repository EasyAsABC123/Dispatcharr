//! Original XC generated-ID Python string-hash semantics. Dispatcharr credit.
//! Hash secrets are independent per process, as measured in actual Source.
use std::{
    collections::hash_map::{DefaultHasher, RandomState},
    hash::{BuildHasher, Hasher},
    sync::OnceLock,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity,
    UnsupportedSeed,
}
fn zero_seed(seed: Option<&str>) -> Result<bool, Error> {
    match seed {
        None | Some("random") => Ok(false),
        Some("0") => Ok(true),
        _ => Err(Error::UnsupportedSeed),
    }
}

fn hashed(value: &str, mut hasher: impl Hasher) -> String {
    if value.is_empty() {
        return "0".into();
    }
    if value.is_ascii() {
        hasher.write(value.as_bytes())
    } else {
        // CPython hashes its compact 1/2/4-byte Unicode storage, not UTF8.
        let maximum = value.chars().map(u32::from).max().expect("nonempty value");
        let width = if maximum <= 255 {
            1
        } else if maximum <= 65535 {
            2
        } else {
            4
        };
        let mut data = Vec::with_capacity(value.chars().count() * width);
        for ch in value.chars() {
            match width {
                1 => data.push(ch as u8),
                2 => data.extend_from_slice(&(ch as u16).to_le_bytes()),
                _ => data.extend_from_slice(&(ch as u32).to_le_bytes()),
            }
        }
        hasher.write(&data);
    }
    let number = hasher.finish() as i64;
    (if number == -1 { -2 } else { number })
        .unsigned_abs()
        .to_string()
}
/// Fixed toolchain uses the SipHash13 primitive demonstrated by Source's
/// seven zero-seed vectors. A fresh independently seeded native process changes
/// generated IDs, while stored programme IDs are always returned unchanged.
/// Python hash-seed configuration beyond the zero case is a separate gate.
pub fn synthetic_id(channel: &str, timestamp: i64) -> Result<String, Error> {
    if channel.len() > 65536 {
        return Err(Error::Capacity);
    }
    let value = format!("{channel}_{timestamp}");
    static STATE: OnceLock<RandomState> = OnceLock::new();
    if zero_seed(std::env::var("PYTHONHASHSEED").ok().as_deref())? {
        Ok(hashed(&value, DefaultHasher::new()))
    } else {
        Ok(hashed(
            &value,
            STATE.get_or_init(RandomState::new).build_hasher(),
        ))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_source_zero_seed_vectors_and_process_stability() {
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/source-epg-hash.json")).unwrap();
        let inputs = corpus["input_strings"].as_array().unwrap();
        let outputs = corpus["zero_seed_outputs"].as_array().unwrap();
        assert_eq!(inputs.len(), 7);
        for (input, expected) in inputs.iter().zip(outputs) {
            assert_eq!(
                hashed(input.as_str().unwrap(), DefaultHasher::new()),
                expected.as_str().unwrap()
            );
        }
        assert_eq!(
            synthetic_id("٢", 123).unwrap(),
            synthetic_id("٢", 123).unwrap()
        );
        assert!(synthetic_id(&"x".repeat(65537), 0).is_err());
        assert_eq!(zero_seed(None), Ok(false));
        assert_eq!(zero_seed(Some("random")), Ok(false));
        assert_eq!(zero_seed(Some("0")), Ok(true));
        assert_eq!(zero_seed(Some("1")), Err(Error::UnsupportedSeed));
        assert_eq!(zero_seed(Some("invalid")), Err(Error::UnsupportedSeed));
    }
}
