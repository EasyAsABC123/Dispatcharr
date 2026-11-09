//! Bounded Django Redis cache primitives, without executable pickle opcodes.
//! Original Dispatcharr authors retain application credit; see ATTRIBUTION.md.
use serde_json::Value;

/// Encode JSON primitives for existing Django Redis readers. Top-level Python
/// integers use Redis decimal bytes; bools and every other primitive use the
/// restricted interoperable pickle encoder. Oversized values are rejected.
pub fn encode_source_cache(value: &Value) -> Option<Vec<u8>> {
    if let Value::Number(number) = value {
        if !number.is_f64() {
            let text = number.to_string();
            return (text.trim_start_matches('-').len() <= 4300).then(|| text.into_bytes());
        }
    }
    crate::django_pickle::encode(value)
}

/// Decode existing Django integer/pickle cache values. Object construction,
/// imports and reducers are rejected; byte, node, work and depth bounds apply.
pub fn decode_source_cache(raw: &[u8]) -> Option<Value> {
    crate::notifications::decode_cache(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn django_counter_bytes_are_distinct_from_boolean_and_float_pickles() {
        assert_eq!(encode_source_cache(&json!(42)), Some(b"42".to_vec()));
        assert_eq!(
            encode_source_cache(&json!(true)),
            Some(vec![0x80, 4, 0x88, b'.'])
        );
        assert_eq!(decode_source_cache(b"42"), Some(json!(42)));
        assert_eq!(
            decode_source_cache(&[0x80, 4, 0x88, b'.']),
            Some(json!(true))
        );
        let floating = encode_source_cache(&json!(42.0)).unwrap();
        assert_eq!(floating[2], b'G');
        assert!(decode_source_cache(&floating)
            .unwrap()
            .as_number()
            .unwrap()
            .is_f64());
    }
}
