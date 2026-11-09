//! Original supported Django Scrypt credentials, using RustCrypto primitives.
//! Original application credit remains with Dispatcharr contributors.
use base64::{engine::general_purpose::STANDARD, Engine};
use subtle::ConstantTimeEq;
fn parameters(n: u64, r: u32, p: u32) -> Option<scrypt::Params> {
    if !n.is_power_of_two() || n < 2 || r == 0 || p == 0 {
        return None;
    }
    // The original hashlib/OpenSSL default maxmem is32MiB. Bound native
    // allocations and CPU before entering the existing4-worker password pool.
    let memory = n
        .checked_mul(u64::from(r))?
        .checked_mul(128)?
        .checked_add(u64::from(p).checked_mul(u64::from(r))?.checked_mul(128)?)?
        .checked_add(u64::from(r).checked_mul(256)?)?;
    let work = n.checked_mul(u64::from(r))?.checked_mul(u64::from(p))?;
    if memory > 32 * 1024 * 1024 || work > 8_388_608 {
        return None;
    }
    scrypt::Params::new(n.trailing_zeros() as u8, r, p, 64).ok()
}
pub(crate) fn verify(password: &str, encoded: &str) -> bool {
    let fields: Vec<_> = encoded.split('$').collect();
    if fields.len() != 6 || fields[0] != "scrypt" || fields[2].is_empty() || fields[2].len() > 1024
    {
        return false;
    }
    let Ok(n) = fields[1].parse() else {
        return false;
    };
    let Ok(r) = fields[3].parse() else {
        return false;
    };
    let Ok(p) = fields[4].parse() else {
        return false;
    };
    let Some(params) = parameters(n, r, p) else {
        return false;
    };
    let Ok(expected) = STANDARD.decode(fields[5]) else {
        return false;
    };
    if expected.len() != 64 {
        return false;
    }
    let mut actual = [0u8; 64];
    if scrypt::scrypt(
        password.as_bytes(),
        fields[2].as_bytes(),
        &params,
        &mut actual,
    )
    .is_err()
    {
        return false;
    }
    let canonical = format!(
        "scrypt${n}${}${r}${p}${}",
        fields[2],
        STANDARD.encode(actual)
    );
    bool::from(actual.ct_eq(expected.as_slice()))
        && bool::from(canonical.as_bytes().ct_eq(encoded.as_bytes()))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_captured_low_cost_scrypt_utf8_and_empty_password_vectors() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/django-scrypt-vectors.json")).unwrap();
        for case in vectors.as_array().unwrap() {
            let password = case["password"].as_str().unwrap();
            let encoded = case["encoded"].as_str().unwrap();
            assert!(verify(password, encoded));
            assert!(!verify(&format!("{password}wrong"), encoded));
        }
    }
    #[test]
    fn unsupported_work_bounds_and_integer_overflow_reject_before_allocation() {
        for (n, r, p) in [
            (0, 8, 5),
            (3, 8, 5),
            (16384, 0, 5),
            (16384, 8, 0),
            (1u64 << 63, u32::MAX, u32::MAX),
            (32768, 8, 5),
            (16384, 8, 1000),
        ] {
            assert!(parameters(n, r, p).is_none());
        }
        assert!(parameters(16384, 8, 5).is_some());
    }
}
