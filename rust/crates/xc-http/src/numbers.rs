//! Collision-free XC numbers from original apps/output/views.py setup.
//! Original Dispatcharr authors retain credit. Awaiting actual-source vectors.
use std::collections::HashMap;

#[derive(Debug)]
pub enum NumberError {
    Invalid,
    Capacity,
}

/// Conservative draft reservation for at most 50k finite-f64 input numbers.
/// Finite IEEE754 integer spellings need at most 310 bytes including a sign;
/// this includes hash buckets, owned successor/result strings and path scratch.
/// Allocator/adversarial proof remains a required post-quiet gate.
pub fn reservation(rows: usize) -> Result<usize, NumberError> {
    if rows > 50000 {
        return Err(NumberError::Capacity);
    }
    rows.checked_mul(2048)
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or(NumberError::Capacity)
}

fn integer(value: f64) -> Result<String, NumberError> {
    if !value.is_finite() {
        return Err(NumberError::Invalid);
    }
    let value = value.trunc();
    if value == 0.0 {
        return Ok("0".into());
    }
    Ok(format!("{:.0}", value.trunc()))
}

fn successor(value: &str) -> String {
    let negative = value.starts_with('-');
    let mut digits = value.trim_start_matches('-').as_bytes().to_vec();
    if negative {
        for byte in digits.iter_mut().rev() {
            if *byte > b'0' {
                *byte -= 1;
                break;
            } else {
                *byte = b'9'
            }
        }
        let first = digits.iter().position(|byte| *byte != b'0');
        return first.map_or_else(
            || "0".into(),
            |at| {
                format!(
                    "-{}",
                    std::str::from_utf8(&digits[at..]).expect("decimal digits")
                )
            },
        );
    }
    let mut carry = true;
    for byte in digits.iter_mut().rev() {
        if *byte < b'9' {
            *byte += 1;
            carry = false;
            break;
        } else {
            *byte = b'0'
        }
    }
    if carry {
        digits.insert(0, b'1')
    }
    String::from_utf8(digits).expect("decimal digits")
}

/// Equivalent assignment order with a compressed successor map. Repeated null
/// or fractional values avoid the source's quadratic scan over occupied ints.
/// Decimal strings retain Python's arbitrary integer range for finite f64s.
/// The explicit allocation guard is independent of the number of HTTP viewers.
pub fn assign(
    rows: &[(i64, Option<f64>)],
    max_bytes: usize,
) -> Result<HashMap<i64, String>, NumberError> {
    if reservation(rows.len())? > max_bytes {
        return Err(NumberError::Capacity);
    }
    let mut occupied = HashMap::<String, String>::new();
    let mut result = HashMap::<i64, String>::new();
    let mut bytes = 0usize;
    let charge = |bytes: &mut usize, size: usize| {
        *bytes = bytes.checked_add(size).ok_or(NumberError::Capacity)?;
        if *bytes > max_bytes {
            Err(NumberError::Capacity)
        } else {
            Ok(())
        }
    };
    for (id, value) in rows {
        if let Some(value) = value {
            if !value.is_finite() {
                return Err(NumberError::Invalid);
            }
            if value.fract() == 0.0 {
                let number = integer(*value)?;
                charge(
                    &mut bytes,
                    number.len() + std::mem::size_of::<(i64, String)>(),
                )?;
                result.insert(*id, number.clone());
                if let std::collections::hash_map::Entry::Vacant(entry) = occupied.entry(number) {
                    let next = successor(entry.key());
                    charge(
                        &mut bytes,
                        entry.key().len() + next.len() + std::mem::size_of::<(String, String)>(),
                    )?;
                    entry.insert(next);
                }
            }
        }
    }
    for (id, value) in rows {
        if result.contains_key(id) {
            continue;
        }
        let mut candidate = value.map_or_else(|| Ok("1".into()), integer)?;
        let mut path = Vec::new();
        while let Some(next) = occupied.get(&candidate) {
            if path.len() > rows.len() {
                return Err(NumberError::Invalid);
            }
            path.push(candidate.clone());
            candidate = next.clone();
        }
        // Pointers only advance to greater integers, so compression has no
        // cycles and does not alter the first free number chosen by the source.
        for old in path {
            occupied.insert(old, candidate.clone());
        }
        let next = successor(&candidate);
        charge(
            &mut bytes,
            candidate.len() * 2
                + next.len()
                + std::mem::size_of::<(String, String)>()
                + std::mem::size_of::<(i64, String)>(),
        )?;
        occupied.insert(candidate.clone(), next);
        result.insert(*id, candidate);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_assignment_block_vectors() {
        let source: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/source-numbers.json")).unwrap();
        let cases = source["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 392);
        for case in cases {
            let rows: Vec<_> = case["numbers"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(at, value)| {
                    let number = if value.is_null() {
                        None
                    } else if let Some(value) = value.as_f64() {
                        Some(value)
                    } else {
                        Some(match value["nonfinite"].as_str().unwrap() {
                            "inf" => f64::INFINITY,
                            "-inf" => f64::NEG_INFINITY,
                            "nan" => f64::NAN,
                            _ => panic!("unknown Source number"),
                        })
                    };
                    ((at + 1) as i64, number)
                })
                .collect();
            let actual = assign(&rows, reservation(rows.len()).unwrap());
            if case.get("error").is_some() {
                assert!(matches!(actual, Err(NumberError::Invalid)));
            } else {
                let actual = actual.unwrap();
                let expected = case["assigned"].as_object().unwrap();
                assert_eq!(actual.len(), expected.len());
                for (id, value) in expected {
                    assert_eq!(
                        actual.get(&id.parse::<i64>().unwrap()).unwrap(),
                        value.as_str().unwrap(),
                        "Source case {}",
                        case["case"]
                    );
                }
            }
        }
    }
}
