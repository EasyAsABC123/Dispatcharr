use base64::{engine::general_purpose::STANDARD, Engine};
use pbkdf2::pbkdf2_hmac;
use rand::{distributions::Alphanumeric, Rng};
use sha1::Sha1;
use sha2::Sha256;
use subtle::ConstantTimeEq;

pub(crate) fn encode_password(password: &str) -> Option<String> {
    if password.len() > 4096 {
        return None;
    }
    let salt: String = rand::rngs::OsRng
        .sample_iter(&Alphanumeric)
        .take(22)
        .map(char::from)
        .collect();
    let mut hash = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt.as_bytes(), 1_200_000, &mut hash);
    Some(format!(
        "pbkdf2_sha256$1200000${salt}${}",
        STANDARD.encode(hash)
    ))
}

/// Verify existing Django default and legacy PBKDF2 hashes without plaintext storage.
/// Unsupported/unusable hashes fail closed; they are never treated as passwords.
pub fn verify_password(password: &str, encoded: &str) -> bool {
    if password.len() > 4096 || encoded.len() > 4096 {
        return false;
    }
    let fields: Vec<_> = encoded.split('$').collect();
    if fields.len() != 4 || fields[2].len() > 1024 {
        return false;
    }
    let Ok(iterations) = fields[1].parse::<u32>() else {
        return false;
    };
    if iterations == 0 || iterations > 10_000_000 {
        return false;
    }
    let Ok(expected) = STANDARD.decode(fields[3]) else {
        return false;
    };
    let actual = match fields[0] {
        "pbkdf2_sha256" if expected.len() == 32 => {
            let mut output = [0; 32];
            pbkdf2_hmac::<Sha256>(
                password.as_bytes(),
                fields[2].as_bytes(),
                iterations,
                &mut output,
            );
            output.to_vec()
        }
        "pbkdf2_sha1" if expected.len() == 20 => {
            let mut output = [0; 20];
            pbkdf2_hmac::<Sha1>(
                password.as_bytes(),
                fields[2].as_bytes(),
                iterations,
                &mut output,
            );
            output.to_vec()
        }
        _ => return false,
    };
    bool::from(actual.ct_eq(&expected))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn django_pbkdf2_vectors_and_unusable_hashes() {
        // Python hashlib.pbkdf2_hmac('sha256', b'password', b'salt', 1).
        assert!(verify_password(
            "password",
            "pbkdf2_sha256$1$salt$Eg+2z/z4syxD5yJSVsT4N6hlSMkszDVICAWYfLcL4Xs="
        ));
        assert!(!verify_password(
            "wrong",
            "pbkdf2_sha256$1$salt$Eg+2z/z4syxD5yJSVsT4N6hlSMkszDVICAWYfL4Xrts="
        ));
        for value in [
            "!unusable",
            "plaintext",
            "pbkdf2_sha256$0$salt$bad",
            "pbkdf2_sha256$9999999999$salt$bad",
        ] {
            assert!(!verify_password("password", value));
        }
    }

    #[test]
    fn unicode_and_legacy_sha1_match_python_vectors() {
        assert!(verify_password(
            " pãss🔑 ",
            "pbkdf2_sha256$100$sälty$p/dw8JlTtwWd2JE/RLk65keQ1ozpwGb/xpgK1jAy0Pk="
        ));
        assert!(verify_password(
            "password",
            "pbkdf2_sha1$1$salt$DGDID5YfDnHzqbUkr2ASBi/gN6Y="
        ));
        assert!(!verify_password(
            "pãss🔑",
            "pbkdf2_sha256$100$sälty$p/dw8JlTtwWd2JE/RLk65keQ1ozpwGb/xpgK1jAy0Pk="
        ));
    }

    #[test]
    fn oversized_inputs_and_wrong_digest_lengths_fail_before_expensive_work() {
        assert!(!verify_password(
            &"a".repeat(4097),
            "pbkdf2_sha256$1$salt$bad"
        ));
        assert!(!verify_password("password", &"$".repeat(4097)));
        assert!(!verify_password(
            "password",
            "pbkdf2_sha256$10000000$salt$AAAA"
        ));
        assert!(!verify_password(
            "password",
            "pbkdf2_sha256$10000001$salt$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        ));
    }
}
