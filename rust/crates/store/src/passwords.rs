use base64::{engine::general_purpose::STANDARD, Engine};
use pbkdf2::pbkdf2_hmac;
use rand::{distributions::Alphanumeric, Rng};
use sha1::Sha1;
use sha2::Sha256;
use subtle::ConstantTimeEq;

const PREFERRED_ITERATIONS: u32 = 1_200_000;

/// Django's default PBKDF2 policy counts salt characters, not UTF-8 bytes.
/// Its entropy calculation assumes the 62-character random-string alphabet.
pub(crate) fn needs_upgrade(encoded: &str) -> bool {
    let mut fields = encoded.split('$');
    let algorithm = fields.next();
    let iterations = fields.next().and_then(|value| value.parse::<u32>().ok());
    let salt = fields.next().unwrap_or_default();
    algorithm != Some("pbkdf2_sha256")
        || iterations != Some(PREFERRED_ITERATIONS)
        || salt.chars().count() < 22
}

/// Run only in the bounded password-worker pool. As in Django's ModelBackend,
/// a matching legacy hash is upgraded even when the account is inactive.
pub(crate) fn verify_and_upgrade(password: &str, encoded: &str) -> (bool, Option<String>) {
    if password.len() > 4096 || encoded.len() > 4096 {
        return (false, None);
    }
    let matched = verify_password(password, encoded);
    let update = needs_upgrade(encoded);
    if matched {
        return (true, update.then(|| encode_password(password)).flatten());
    }
    let fields: Vec<_> = encoded.split('$').collect();
    if fields.len() == 4 && fields[0] == "pbkdf2_sha256" && update {
        if let Ok(iterations) = fields[1].parse::<u32>() {
            if iterations > 0 && iterations < PREFERRED_ITERATIONS && fields[2].len() <= 1024 {
                let mut output = [0u8; 32];
                pbkdf2_hmac::<Sha256>(
                    password.as_bytes(),
                    fields[2].as_bytes(),
                    PREFERRED_ITERATIONS - iterations,
                    &mut output,
                );
            }
        }
    } else if encoded.starts_with('!')
        || !["pbkdf2_sha256", "pbkdf2_sha1", "scrypt"].contains(&fields[0])
    {
        // Django spends a default hash for unusable/unknown hashes. Unsupported
        // installed algorithms still fail closed until their native port exists.
        let _ = encode_password(password);
    }
    (false, None)
}

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
    pbkdf2_hmac::<Sha256>(
        password.as_bytes(),
        salt.as_bytes(),
        PREFERRED_ITERATIONS,
        &mut hash,
    );
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
    if encoded.starts_with("scrypt$") {
        return crate::password_scrypt::verify(password, encoded);
    }
    let fields: Vec<_> = encoded.split('$').collect();
    if fields.len() != 4 || fields[2].is_empty() || fields[2].len() > 1024 {
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
    // Django compares the complete regenerated encoding, not just digest bytes.
    let canonical = format!(
        "{}${iterations}${}${}",
        fields[0],
        fields[2],
        STANDARD.encode(&actual)
    );
    bool::from(actual.ct_eq(&expected))
        && bool::from(canonical.as_bytes().ct_eq(encoded.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_full_encoding_comparison_rejects_noncanonical_parameter_spellings() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/django-hash-canonical.json")).unwrap();
        for case in cases.as_array().unwrap() {
            assert_eq!(
                verify_password(
                    case["password"].as_str().unwrap(),
                    case["encoded"].as_str().unwrap()
                ),
                case["expected"].as_bool().unwrap()
            );
        }
    }
    #[test]
    fn pinned_original_upgrade_policy_counts_unicode_salt_characters() {
        let source: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/password-upgrade-policy.json")).unwrap();
        for case in source["cases"].as_array().unwrap() {
            assert_eq!(
                needs_upgrade(case["encoded"].as_str().unwrap()),
                case["needs_upgrade"].as_bool().unwrap()
            );
        }
    }
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
