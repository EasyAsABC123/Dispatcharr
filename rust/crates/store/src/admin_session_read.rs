//! Read-only compatibility with Django 6.0.9 database sessions.
//! Dispatcharr and Django authors retain application and protocol credit.
use axum::http::{header, HeaderMap};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use flate2::bufread::ZlibDecoder;
use pbkdf2::hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::io::Read;
use subtle::ConstantTimeEq;

const SESSION_SALT: &[u8] = b"django.contrib.sessions.SessionStoresigner";
const PASSWORD_SALT: &[u8] = b"django.contrib.auth.models.AbstractBaseUser.get_session_auth_hash";
const MODEL_BACKEND: &str = "django.contrib.auth.backends.ModelBackend";
const SIGNING_DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const MAX_SIGNED_BYTES: usize = 16 * 1024;
const MAX_JSON_BYTES: usize = 8 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SessionError {
    Invalid,
    Expired,
    Limit,
}

#[derive(Deserialize)]
struct AuthData {
    #[serde(rename = "_auth_user_id")]
    user_id: String,
    #[serde(rename = "_auth_user_backend")]
    backend: String,
    #[serde(rename = "_auth_user_hash")]
    password_proof: String,
}

/// The signing key is supplied by the existing runtime owner. Authentication
/// of a decoded row still needs the current SQL actor and stored password hash.
pub(crate) struct DatabaseSessions<'key> {
    secret: &'key [u8],
}

pub(crate) struct SessionClaims {
    pub(crate) user_id: i64,
    password_proof: String,
}

impl<'key> DatabaseSessions<'key> {
    pub(crate) fn new(secret: &'key [u8]) -> Result<Self, SessionError> {
        if secret.is_empty() {
            return Err(SessionError::Invalid);
        }
        Ok(Self { secret })
    }

    pub(crate) fn open(
        &self,
        row: &str,
        database_expiry: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<SessionClaims, SessionError> {
        if database_expiry <= now {
            return Err(SessionError::Expired);
        }
        if row.len() > MAX_SIGNED_BYTES {
            return Err(SessionError::Limit);
        }
        let (frame, signature) = row.rsplit_once(':').ok_or(SessionError::Invalid)?;
        let expected = URL_SAFE_NO_PAD.encode(self.mac(SESSION_SALT, frame.as_bytes()));
        if !bool::from(expected.as_bytes().ct_eq(signature.as_bytes())) {
            return Err(SessionError::Invalid);
        }
        let (body, stamp) = frame.rsplit_once(':').ok_or(SessionError::Invalid)?;
        // Django's DB backend checks expire_date, not the signature timestamp.
        // Validate the bounded timestamp field without introducing an additional TTL.
        let _signing_timestamp = read_base62(stamp).ok_or(SessionError::Invalid)?;
        let json = decode_body(body)?;
        let data = serde_json::from_slice::<AuthData>(&json).map_err(|_| SessionError::Invalid)?;
        let user_id = data
            .user_id
            .parse::<i64>()
            .ok()
            .filter(|id| *id > 0 && id.to_string() == data.user_id)
            .ok_or(SessionError::Invalid)?;
        if data.backend != MODEL_BACKEND
            || data.password_proof.len() != 64
            || !data
                .password_proof
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(SessionError::Invalid);
        }
        Ok(SessionClaims {
            user_id,
            password_proof: data.password_proof,
        })
    }

    pub(crate) fn password_matches(&self, claims: &SessionClaims, stored_hash: &str) -> bool {
        !stored_hash.is_empty()
            && bool::from(
                self.password_proof(stored_hash)
                    .as_bytes()
                    .ct_eq(claims.password_proof.as_bytes()),
            )
    }

    fn password_proof(&self, stored_hash: &str) -> String {
        use std::fmt::Write;
        let mut hex = String::with_capacity(64);
        for byte in self.mac(PASSWORD_SALT, stored_hash.as_bytes()) {
            write!(&mut hex, "{byte:02x}").expect("format to string");
        }
        hex
    }

    fn mac(&self, salt: &[u8], message: &[u8]) -> [u8; 32] {
        let mut derive = Sha256::new();
        derive.update(salt);
        derive.update(self.secret);
        let mut hmac = Hmac::<Sha256>::new_from_slice(&derive.finalize())
            .expect("HMAC accepts any key length");
        hmac.update(message);
        hmac.finalize().into_bytes().into()
    }
}

fn read_base62(value: &str) -> Option<u64> {
    if value.is_empty() || value.len() > 11 {
        return None;
    }
    value.bytes().try_fold(0_u64, |out, byte| {
        let digit = SIGNING_DIGITS.iter().position(|digit| *digit == byte)?;
        out.checked_mul(62)?.checked_add(digit as u64)
    })
}

fn decode_body(body: &str) -> Result<Vec<u8>, SessionError> {
    let payload = body.strip_prefix('.');
    let bytes = URL_SAFE_NO_PAD
        .decode(payload.unwrap_or(body))
        .map_err(|_| SessionError::Invalid)?;
    if payload.is_none() {
        return (bytes.len() <= MAX_JSON_BYTES)
            .then_some(bytes)
            .ok_or(SessionError::Limit);
    }
    let mut reader = ZlibDecoder::new(bytes.as_slice());
    let mut json = Vec::new();
    reader
        .by_ref()
        .take((MAX_JSON_BYTES + 1) as u64)
        .read_to_end(&mut json)
        .map_err(|_| SessionError::Invalid)?;
    if json.len() > MAX_JSON_BYTES {
        return Err(SessionError::Limit);
    }
    if reader.total_in() != bytes.len() as u64 {
        return Err(SessionError::Invalid);
    }
    Ok(json)
}

pub(crate) fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    let value = cookie(headers, "sessionid")?;
    (value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()))
    .then_some(value)
}

pub(crate) fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut found = None;
    for header in headers.get_all(header::COOKIE) {
        for entry in header.to_str().ok()?.split(';') {
            if let Some((key, value)) = entry.trim().split_once('=') {
                if key == name && found.replace(value).is_some() {
                    return None;
                }
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deterministic vectors for invented fixture data, independently assembled
    // with Python's SHA-256, HMAC, zlib and URL-safe base64 primitives.
    const PLAIN: &str = concat!(
        "eyJfYXV0aF91c2VyX2lkIjoiNzMiLCJfYXV0aF91c2VyX2JhY2tlbmQiOiJkamFuZ28uY29u",
        "dHJpYi5hdXRoLmJhY2tlbmRzLk1vZGVsQmFja2VuZCIsIl9hdXRoX3VzZXJfaGFzaCI6IjE4",
        "NDY1MzNjOGZlNjcxOGJmMjc5N2ExNTEzMTY1NTAzOTNlZjRmMDQzYWMxZGM4OTMzMTQzY2Q4",
        "NzUzMGRmMjUifQ:S:ddw8L2JHXQlvKnyRRZFq0XTtrfP3_aVXr8dvZ5573pQ",
    );
    const COMPRESSED: &str = concat!(
        ".eJxVjDkOwjAQAP-yNbK8WZ8p6XmD5RMHkC3FSYX4O4qUAtqZ0bzB-X2rbh95dUuCGTTB5Rc",
        "GH5-5HSY9fLt3Fnvb1iWwI2GnHezWU35dz_ZvUP2oMAMaoSRRNCUrjSaUSVvtUSKhkpKTpVx",
        "E4YJ8xBSNJUJBMRktiacySfh8AdYFNsI:S:FpTCtPk75YVOIVxxwHScop-0yQFb_oBTXnTYm",
        "O_la_w",
    );

    #[test]
    fn legacy_database_session_requires_valid_signature_expiry_and_current_password() {
        let signing = [81_u8; 32];
        let codec = DatabaseSessions::new(&signing).unwrap();
        let wrong_signing = [82_u8; 32];
        let wrong_codec = DatabaseSessions::new(&wrong_signing).unwrap();
        let now = DateTime::from_timestamp(100, 0).unwrap();
        let future = DateTime::from_timestamp(101, 0).unwrap();
        for data in [PLAIN, COMPRESSED] {
            let actor = codec.open(data, future, now).unwrap();
            assert_eq!(actor.user_id, 73);
            assert!(codec.password_matches(&actor, "owned-fixture-password-field"));
            assert!(!codec.password_matches(&actor, "different-password-field"));
            assert!(matches!(
                codec.open(data, now, now),
                Err(SessionError::Expired)
            ));
            assert!(matches!(
                wrong_codec.open(data, future, now),
                Err(SessionError::Invalid)
            ));
        }
    }

    #[test]
    fn only_one_canonical_session_cookie_selects_an_owned_row() {
        let mut headers = HeaderMap::new();
        assert!(session_cookie(&headers).is_none());
        headers.insert(
            header::COOKIE,
            "sessionid=0123456789abcdef0123456789abcdef"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            session_cookie(&headers),
            Some("0123456789abcdef0123456789abcdef")
        );
        headers.append(
            header::COOKIE,
            "sessionid=0123456789abcdef0123456789abcdef"
                .parse()
                .unwrap(),
        );
        assert!(session_cookie(&headers).is_none());
    }
}
