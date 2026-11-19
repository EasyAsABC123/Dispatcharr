//! Original XC generated-ID Python string-hash semantics. Dispatcharr credit.
//! Default synthetic IDs use an independently randomized key per process.
use siphasher::sip::SipHasher13;
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hasher},
    sync::OnceLock,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity,
    UnsupportedSeed,
}
fn configured_seed(value: Option<&str>) -> Result<Option<u32>, Error> {
    let value = match value {
        None | Some("" | "random") => return Ok(None),
        Some(value) => value.trim_start_matches(|ch| matches!(ch, '\t'..='\r' | ' ')),
    };
    // CPython's Linux initialization parses an unsigned long in base 10,
    // rejects unconsumed bytes and bounds the result to 32 bits.
    let (digits, negative) = match value.strip_prefix('-') {
        Some(digits) => (digits, true),
        None => (value.strip_prefix('+').unwrap_or(value), false),
    };
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::UnsupportedSeed);
    }
    let mut number: u64 = digits.parse().map_err(|_| Error::UnsupportedSeed)?;
    if negative {
        number = number.wrapping_neg();
    }
    u32::try_from(number)
        .map(Some)
        .map_err(|_| Error::UnsupportedSeed)
}

fn fixed_hasher(mut seed: u32) -> SipHasher13 {
    // CPython Python/bootstrap_hash.c initializes the two SipHash13 keys
    // with the first 16 bytes of its seeded hash buffer. Zero disables it.
    let mut key = [0; 16];
    if seed != 0 {
        for byte in &mut key {
            seed = seed.wrapping_mul(214013).wrapping_add(2531011);
            *byte = (seed >> 16) as u8;
        }
    }
    let (first, second) = key.split_at(8);
    SipHasher13::new_with_keys(
        u64::from_le_bytes(first.try_into().expect("eight key bytes")),
        u64::from_le_bytes(second.try_into().expect("eight key bytes")),
    )
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
/// Generate a programme ID matching the Source's compact Unicode hashing.
/// A configured seed is fixed for the process, as in CPython initialization.
pub fn synthetic_id(channel: &str, timestamp: i64) -> Result<String, Error> {
    if channel.len() > 65536 {
        return Err(Error::Capacity);
    }
    static FIXED: OnceLock<Result<Option<SipHasher13>, Error>> = OnceLock::new();
    static RANDOM: OnceLock<RandomState> = OnceLock::new();
    let fixed = (*FIXED.get_or_init(|| {
        let seed = match std::env::var("PYTHONHASHSEED") {
            Ok(value) => configured_seed(Some(&value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(Error::UnsupportedSeed),
        }?;
        Ok(seed.map(fixed_hasher))
    }))?;
    let value = format!("{channel}_{timestamp}");
    Ok(match fixed {
        Some(hasher) => hashed(&value, hasher),
        None => hashed(&value, RANDOM.get_or_init(RandomState::new).build_hasher()),
    })
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
                hashed(input.as_str().unwrap(), fixed_hasher(0)),
                expected.as_str().unwrap()
            );
        }
        assert_eq!(
            synthetic_id("٢", 123).unwrap(),
            synthetic_id("٢", 123).unwrap()
        );
        assert!(synthetic_id(&"x".repeat(65537), 0).is_err());
    }

    #[test]
    fn cpython_nonzero_seeds_and_configuration() {
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/source-epg-seeded-hash.json")).unwrap();
        let inputs = corpus["input_strings"].as_array().unwrap();
        for (seed, outputs) in corpus["outputs"].as_object().unwrap() {
            let hasher = fixed_hasher(configured_seed(Some(seed)).unwrap().unwrap());
            let outputs = outputs.as_array().unwrap();
            assert_eq!(inputs.len(), outputs.len());
            for (value, expected) in inputs.iter().zip(outputs) {
                assert_eq!(
                    hashed(value.as_str().unwrap(), hasher),
                    expected.as_str().unwrap()
                );
            }
        }
        assert_eq!(configured_seed(None), Ok(None));
        assert_eq!(configured_seed(Some("")), Ok(None));
        assert_eq!(configured_seed(Some("random")), Ok(None));
        assert_eq!(configured_seed(Some("0")), Ok(Some(0)));
        assert_eq!(configured_seed(Some("-0")), Ok(Some(0)));
        assert_eq!(configured_seed(Some("\t+00042")), Ok(Some(42)));
        assert_eq!(configured_seed(Some("-18446744073709551615")), Ok(Some(1)));
        for seed in [
            "-1",
            "4294967296",
            "invalid",
            "1 ",
            " random",
            "0x1",
            "-+1",
            "١",
            " ",
        ] {
            assert_eq!(configured_seed(Some(seed)), Err(Error::UnsupportedSeed));
        }
    }
}
