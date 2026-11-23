//! Existing Django database session admission for the ChannelGroup admin read.
//! Original Dispatcharr and Django authors retain full credit.
use std::{fmt::Write as _, io::Write as _, sync::Arc};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use flate2::{write::ZlibEncoder, Compression, Decompress, FlushDecompress, Status};
use hmac::{Hmac, Mac};
use rand::{rngs::OsRng, Rng};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Row};
use subtle::ConstantTimeEq;
use unicode_normalization::UnicodeNormalization;

use crate::{AuthService, Store, StoreError};

const MAX_SIGNED_ROW: usize = 32 * 1024;
const MAX_SESSION_JSON: usize = 64 * 1024;
const MODEL_BACKEND: &str = "django.contrib.auth.backends.ModelBackend";
const STORE_SALT: &[u8] = b"django.contrib.sessions.SessionStoresigner";
const USER_HASH_SALT: &[u8] = b"django.contrib.auth.models.AbstractBaseUser.get_session_auth_hash";

const SESSION_ROW: &str = "
    SELECT session_data FROM django_session
    WHERE session_key = $1 AND expire_date > statement_timestamp()
          AND octet_length(session_data) <= $2";

const CHANNEL_GROUP_ACTOR: &str = "
    SELECT actor.password, actor.is_active, actor.is_staff, actor.is_superuser,
           EXISTS (
               SELECT 1
               FROM auth_permission AS perm
               JOIN django_content_type AS subject ON subject.id = perm.content_type_id
               WHERE subject.app_label = 'dispatcharr_channels'
                     AND subject.model = 'channelgroup'
                     AND perm.codename IN ('view_channelgroup', 'change_channelgroup')
                     AND (
                         EXISTS (
                             SELECT 1
                             FROM accounts_user_user_permissions AS direct
                             WHERE direct.user_id = actor.id AND direct.permission_id = perm.id
                         )
                         OR EXISTS (
                             SELECT 1
                             FROM accounts_user_groups AS membership
                             JOIN auth_group_permissions AS granted ON granted.group_id = membership.group_id
                             WHERE membership.user_id = actor.id AND granted.permission_id = perm.id
                         )
                     )
           ) AS has_channel_group_permission
    FROM accounts_user AS actor
    JOIN django_session AS session ON session.session_key = $1
         AND session.session_data COLLATE \"C\" = $2 AND session.expire_date > statement_timestamp()
    WHERE actor.id = $3";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DjangoChannelGroupRead {
    LoginRequired,
    /// Django flushes this session. No write occurs in this read-only component.
    SessionNeedsInvalidation,
    Forbidden,
    Allowed {
        user_id: i64,
    },
}

#[derive(Clone, Copy)]
pub enum DjangoChannelGroupOrder {
    IdAscending,
    IdDescending,
    NameAscending,
    NameDescending,
}

impl DjangoChannelGroupOrder {
    fn sql(self) -> &'static str {
        match self {
            Self::IdAscending => "id ASC",
            Self::IdDescending => "id DESC",
            Self::NameAscending => "name ASC",
            Self::NameDescending => "name DESC",
        }
    }
}

pub struct DjangoChannelGroupQuery<'a> {
    pub search: &'a str,
    pub page: i64,
    pub show_all: bool,
    pub order: DjangoChannelGroupOrder,
}

pub struct DjangoChannelGroupPage {
    pub username: String,
    pub rows: Vec<(i64, String)>,
    pub total: i64,
    pub matches: i64,
    pub page: i64,
    pub pages: i64,
    pub show_all: bool,
}

pub enum DjangoChannelGroupPageResult {
    View(DjangoChannelGroupPage),
    Denied(DjangoChannelGroupRead),
    PageOutOfRange,
}

const CHANNEL_GROUP_TABLE: &str = "dispatcharr_channels_channelgroup";
const SEARCH_FILTER: &str = "(strpos(upper(name), upper($1)) > 0 OR strpos(id::text, $1) > 0)";
const PAGE_SIZE: i64 = 100;
const MAX_SHOW_ALL: i64 = 200;

/// Verifies ordinary Django database sessions with a current signing key.
/// Configured fallback remains outside the default-key owner.
#[derive(Clone)]
pub struct DjangoAdminSessions {
    store: Store,
    signing_key: Arc<[u8]>,
}

impl DjangoAdminSessions {
    /// The caller supplies the configured Django signing key. The pinned stock
    /// configuration has an empty fallback list.
    pub fn with_current_key(store: Store, signing_key: &str) -> Result<Self, StoreError> {
        if signing_key.is_empty() {
            return Err(StoreError::Unsupported(
                "Django session signing key is required",
            ));
        }
        Ok(Self {
            store,
            signing_key: Arc::from(signing_key.as_bytes()),
        })
    }

    #[cfg(test)]
    async fn channel_group_read(
        &self,
        session_key: &str,
    ) -> Result<DjangoChannelGroupRead, StoreError> {
        if !issued_session_key(session_key) {
            return Ok(DjangoChannelGroupRead::LoginRequired);
        }
        let mut connection = self.store.pool().acquire().await?;
        self.admission(session_key, &mut connection).await
    }

    /// Shares a read-only snapshot for live session admission and the bounded page.
    pub async fn channel_group_page(
        &self,
        session_key: &str,
        selection: DjangoChannelGroupQuery<'_>,
    ) -> Result<DjangoChannelGroupPageResult, StoreError> {
        use DjangoChannelGroupPageResult::{Denied, PageOutOfRange, View};

        if !issued_session_key(session_key) {
            return Ok(Denied(DjangoChannelGroupRead::LoginRequired));
        }
        let mut tx = self.store.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let decision = self.admission(session_key, &mut tx).await?;
        let DjangoChannelGroupRead::Allowed { user_id } = decision else {
            tx.commit().await?;
            return Ok(Denied(decision));
        };

        let username: String =
            sqlx::query_scalar("SELECT username FROM accounts_user WHERE id = $1")
                .bind(user_id)
                .fetch_one(&mut *tx)
                .await?;
        let search = selection.search.trim();
        let counts = format!(
            "SELECT count(*), count(*) FILTER (WHERE {SEARCH_FILTER}) FROM {CHANNEL_GROUP_TABLE}"
        );
        let (total, matches): (i64, i64) = sqlx::query_as(&counts)
            .bind(search)
            .fetch_one(&mut *tx)
            .await?;
        let pages = 1 + matches.saturating_sub(1).max(0) / PAGE_SIZE;
        let show_all = selection.show_all && matches <= MAX_SHOW_ALL;
        let page = if show_all || pages == 1 {
            1
        } else {
            selection.page
        };
        if !(1..=pages).contains(&page) {
            tx.commit().await?;
            return Ok(PageOutOfRange);
        }
        let query = format!(
            "SELECT id, name FROM {CHANNEL_GROUP_TABLE} WHERE {SEARCH_FILTER}
             ORDER BY {} LIMIT $2 OFFSET $3",
            selection.order.sql(),
        );
        let rows = sqlx::query_as(&query)
            .bind(search)
            .bind(if show_all { MAX_SHOW_ALL } else { PAGE_SIZE })
            .bind((page - 1) * PAGE_SIZE)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(View(DjangoChannelGroupPage {
            username,
            rows,
            total,
            matches,
            page,
            pages,
            show_all,
        }))
    }

    /// Validate a current staff session for the dedicated login/logout surface.
    pub async fn signed_staff(&self, session_key: &str) -> Result<bool, StoreError> {
        if !issued_session_key(session_key) {
            return Ok(false);
        }
        let mut tx = self.store.pool().begin().await?;
        let signed = self.locked_staff(&mut tx, session_key).await?;
        tx.commit().await?;
        Ok(signed)
    }

    async fn locked_staff(
        &self,
        connection: &mut PgConnection,
        session_key: &str,
    ) -> Result<bool, StoreError> {
        let Some((encoded, active)) = lock_session(connection, session_key).await? else {
            return Ok(false);
        };
        let Some(session) = active
            .then(|| decode_session(&encoded, &self.signing_key))
            .flatten()
        else {
            return Ok(false);
        };
        let account = sqlx::query_as::<_, (String, bool, bool)>(
            "SELECT password, is_active, is_staff FROM accounts_user WHERE id = $1 FOR SHARE",
        )
        .bind(session.user_id)
        .fetch_optional(&mut *connection)
        .await?;
        Ok(account.is_some_and(|(password, active, staff)| {
            active
                && staff
                && session.auth_hash.as_ref().is_some_and(|hash| {
                    bool::from(
                        current_auth_hash(&self.signing_key, &password)
                            .as_bytes()
                            .ct_eq(hash.as_bytes()),
                    )
                })
        }))
    }

    /// Issue an ordinary current-key admin session after the dedicated origin verifies CSRF.
    pub async fn login(
        &self,
        auth: &AuthService,
        username: &str,
        password: String,
        previous_key: Option<&str>,
    ) -> Result<Option<String>, StoreError> {
        if !auth.uses_store_pool(&self.store) {
            return Err(StoreError::Unsupported(
                "Django admin must share the configured account store",
            ));
        }
        let username: String = username.trim().nfkc().collect();
        if username.is_empty()
            || username.chars().count() > 150
            || password.is_empty()
            || password.len() > crate::passwords::MAX_CREDENTIAL_BYTES
        {
            return Ok(None);
        }
        let observed = sqlx::query_as::<_, (i64, String)>(
            "SELECT id, password FROM accounts_user WHERE username = $1",
        )
        .bind(&username)
        .fetch_optional(self.store.pool())
        .await?;
        let (matched, legacy_upgrade) = auth
            .verify_local_password(password, observed.as_ref().map(|(_, hash)| hash.clone()))
            .await?;
        let Some((actor, password_field)) =
            observed.filter(|_| matched && legacy_upgrade.is_none())
        else {
            return Ok(None);
        };
        let _writer = auth.task_sql_writer()?;
        let mut tx = self.store.pool().begin().await?;
        let previous = match previous_key.filter(|key| issued_session_key(key)) {
            Some(key) => lock_session(&mut tx, key)
                .await?
                .map(|record| (key, record)),
            None => None,
        };
        let current = sqlx::query_as::<_, (String, bool, bool)>(
            "SELECT password, is_active, is_staff FROM accounts_user
             WHERE id = $1 AND username = $2 FOR UPDATE",
        )
        .bind(actor)
        .bind(username)
        .fetch_optional(&mut *tx)
        .await?;
        if !current
            .is_some_and(|(current, active, staff)| current == password_field && active && staff)
        {
            tx.rollback().await?;
            return Ok(None);
        }
        let hash = current_auth_hash(&self.signing_key, &password_field);
        let mut fields = Map::new();
        let mut retire = None;
        let mut retain = None;
        if let Some((old_key, (encoded, true))) = previous {
            if let Some(payload) = verified_session_json(&encoded, &self.signing_key) {
                let prior_fields: Map<String, Value> = serde_json::from_slice(&payload)
                    .map_err(|_| StoreError::Unsupported("Unsupported Django session object"))?;
                if prior_fields.contains_key("_session_expiry") {
                    return Err(StoreError::Unsupported(
                        "Custom Django session expiry is outside the current admin scope",
                    ));
                }
                if prior_fields.contains_key("_auth_user_id") {
                    let previous_owner = decode_session(&encoded, &self.signing_key);
                    if previous_owner.is_some_and(|previous| {
                        previous.user_id == actor
                            && previous.auth_hash.as_ref().is_some_and(|value| {
                                bool::from(value.as_bytes().ct_eq(hash.as_bytes()))
                            })
                    }) {
                        fields = prior_fields;
                        retain = Some(old_key);
                    }
                } else {
                    fields = prior_fields;
                }
                if retain.is_none() {
                    retire = Some(old_key);
                }
            }
        }
        fields.insert("_auth_user_id".into(), Value::String(actor.to_string()));
        fields.insert(
            "_auth_user_backend".into(),
            Value::String(MODEL_BACKEND.into()),
        );
        fields.insert("_auth_user_hash".into(), Value::String(hash));
        let timestamp = chrono::Utc::now();
        let encoded = encode_session(&fields, &self.signing_key, timestamp.timestamp() as u64)?;
        let expires = timestamp + chrono::Duration::days(14);
        let key = if let Some(key) = retain {
            sqlx::query("UPDATE django_session SET session_data = $2, expire_date = $3 WHERE session_key = $1")
                .bind(key).bind(&encoded).bind(expires).execute(&mut *tx).await?;
            key.to_owned()
        } else {
            let mut issued = None;
            for _ in 0..8 {
                let key = session_key();
                let inserted = sqlx::query(
                    "INSERT INTO django_session (session_key, session_data, expire_date)
                    VALUES ($1, $2, $3) ON CONFLICT (session_key) DO NOTHING",
                )
                .bind(&key)
                .bind(&encoded)
                .bind(expires)
                .execute(&mut *tx)
                .await?;
                if inserted.rows_affected() == 1 {
                    issued = Some(key);
                    break;
                }
            }
            issued.ok_or(StoreError::Unsupported("Django session allocation failed"))?
        };
        if let Some(old_key) = retire {
            sqlx::query("DELETE FROM django_session WHERE session_key = $1")
                .bind(old_key)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE accounts_user SET last_login = $2 WHERE id = $1")
            .bind(actor)
            .bind(timestamp)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(key))
    }

    /// Retire exactly the active staff session on the already CSRF-verified dedicated POST.
    pub async fn logout(&self, auth: &AuthService, session_key: &str) -> Result<bool, StoreError> {
        if !issued_session_key(session_key) {
            return Ok(false);
        }
        if !auth.uses_store_pool(&self.store) {
            return Err(StoreError::Unsupported(
                "Django admin must share the configured account store",
            ));
        }
        let _writer = auth.task_sql_writer()?;
        let mut tx = self.store.pool().begin().await?;
        if !self.locked_staff(&mut tx, session_key).await? {
            tx.rollback().await?;
            return Ok(false);
        }
        sqlx::query("DELETE FROM django_session WHERE session_key = $1")
            .bind(session_key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn admission(
        &self,
        session_key: &str,
        connection: &mut PgConnection,
    ) -> Result<DjangoChannelGroupRead, StoreError> {
        let Some(encoded) = sqlx::query_scalar::<_, String>(SESSION_ROW)
            .bind(session_key)
            .bind(MAX_SIGNED_ROW as i32)
            .fetch_optional(&mut *connection)
            .await?
        else {
            return Ok(DjangoChannelGroupRead::LoginRequired);
        };
        let Some(session) = decode_session(&encoded, &self.signing_key) else {
            return Ok(DjangoChannelGroupRead::LoginRequired);
        };

        // Confirm the exact session remains live in the statement that
        // reads current account state and permission links.
        let Some(row) = sqlx::query(CHANNEL_GROUP_ACTOR)
            .bind(session_key)
            .bind(encoded)
            .bind(session.user_id)
            .fetch_optional(&mut *connection)
            .await?
        else {
            return Ok(DjangoChannelGroupRead::LoginRequired);
        };
        let actor = Actor {
            password: row.try_get("password")?,
            active: row.try_get("is_active")?,
            staff: row.try_get("is_staff")?,
            superuser: row.try_get("is_superuser")?,
            model_permission: row.try_get("has_channel_group_permission")?,
        };
        Ok(authorize(&session, &actor, &self.signing_key))
    }
}

async fn lock_session(
    connection: &mut PgConnection,
    session_key: &str,
) -> Result<Option<(String, bool)>, StoreError> {
    Ok(sqlx::query_as(
        "SELECT session_data, expire_date > statement_timestamp() FROM django_session
         WHERE session_key = $1 AND octet_length(session_data) <= $2 FOR UPDATE",
    )
    .bind(session_key)
    .bind(MAX_SIGNED_ROW as i32)
    .fetch_optional(connection)
    .await?)
}

fn current_auth_hash(secret: &[u8], password: &str) -> String {
    let key = salted_hmac(secret, USER_HASH_SALT, password.as_bytes()).finalize();
    format!("{:x}", key.into_bytes())
}

fn session_key() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = OsRng;
    (0..32)
        .map(|_| char::from(ALPHABET[rng.gen_range(0..ALPHABET.len())]))
        .collect()
}

fn encode_session(
    fields: &Map<String, Value>,
    secret: &[u8],
    timestamp: u64,
) -> Result<String, StoreError> {
    let json = serde_json::to_string(fields)
        .map_err(|_| StoreError::Unsupported("Django session JSON encoding failed"))?;
    let mut ascii = String::with_capacity(json.len());
    for ch in json.chars() {
        if ch.is_ascii() {
            ascii.push(ch);
        } else {
            let mut buffer = [0; 2];
            for unit in ch.encode_utf16(&mut buffer) {
                write!(ascii, "\\u{unit:04x}").expect("writing to session string");
            }
        }
    }
    if ascii.len() > MAX_SESSION_JSON {
        return Err(StoreError::Unsupported(
            "Django session exceeds the supported size",
        ));
    }
    let mut compressor = ZlibEncoder::new(Vec::new(), Compression::default());
    compressor
        .write_all(ascii.as_bytes())
        .map_err(|_| StoreError::Unsupported("Django session compression failed"))?;
    let compressed = compressor
        .finish()
        .map_err(|_| StoreError::Unsupported("Django session compression failed"))?;
    let packed = if compressed.len() + 1 < ascii.len() {
        format!(".{}", URL_SAFE_NO_PAD.encode(compressed))
    } else {
        URL_SAFE_NO_PAD.encode(ascii)
    };
    let signed = format!("{packed}:{}", base62(timestamp));
    let signature = URL_SAFE_NO_PAD.encode(
        salted_hmac(secret, STORE_SALT, signed.as_bytes())
            .finalize()
            .into_bytes(),
    );
    let encoded = format!("{signed}:{signature}");
    if encoded.len() > MAX_SIGNED_ROW {
        return Err(StoreError::Unsupported(
            "Django signed session exceeds the supported size",
        ));
    }
    Ok(encoded)
}

fn base62(mut value: u64) -> String {
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut digits = Vec::new();
    loop {
        digits.push(char::from(ALPHABET[(value % 62) as usize]));
        value /= 62;
        if value == 0 {
            return digits.into_iter().rev().collect();
        }
    }
}

fn issued_session_key(key: &str) -> bool {
    key.len() == 32
        && key
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

#[derive(Deserialize)]
struct AuthFields {
    #[serde(rename = "_auth_user_id")]
    user_id: String,
    #[serde(rename = "_auth_user_backend")]
    backend: String,
    #[serde(rename = "_auth_user_hash")]
    auth_hash: Option<String>,
}

struct DecodedSession {
    user_id: i64,
    auth_hash: Option<String>,
}

struct Actor {
    password: String,
    active: bool,
    staff: bool,
    superuser: bool,
    model_permission: bool,
}

fn salted_hmac(secret: &[u8], salt: &[u8], message: &[u8]) -> Hmac<Sha256> {
    let mut derived = Sha256::new();
    derived.update(salt);
    derived.update(secret);
    let mut mac = Hmac::<Sha256>::new_from_slice(&derived.finalize())
        .expect("HMAC-SHA256 accepts a 32-byte derived key");
    mac.update(message);
    mac
}

fn verified_session_json(encoded: &str, current_secret: &[u8]) -> Option<Vec<u8>> {
    if encoded.len() > MAX_SIGNED_ROW {
        return None;
    }
    let (signed, signature) = encoded.rsplit_once(':')?;
    if signature.len() != 43 {
        return None;
    }
    let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
    salted_hmac(current_secret, STORE_SALT, signed.as_bytes())
        .verify_slice(&signature)
        .ok()?;

    let (packed, timestamp) = signed.rsplit_once(':')?;
    let timestamp = timestamp.strip_prefix('-').unwrap_or(timestamp);
    if timestamp.is_empty() || !timestamp.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return None;
    }
    let content = if let Some(compressed) = packed.strip_prefix('.') {
        let compressed = URL_SAFE_NO_PAD.decode(compressed).ok()?;
        decompress_session(&compressed)?
    } else {
        URL_SAFE_NO_PAD.decode(packed).ok()?
    };
    if content.len() > MAX_SESSION_JSON {
        return None;
    }
    Some(content)
}

fn decode_session(encoded: &str, current_secret: &[u8]) -> Option<DecodedSession> {
    let fields: AuthFields =
        serde_json::from_slice(&verified_session_json(encoded, current_secret)?).ok()?;
    if fields.backend != MODEL_BACKEND {
        return None;
    }
    let user_id = fields.user_id.parse::<i64>().ok()?;
    Some(DecodedSession {
        user_id,
        auth_hash: fields.auth_hash,
    })
}

fn decompress_session(compressed: &[u8]) -> Option<Vec<u8>> {
    let mut decompressor = Decompress::new(true);
    let mut output = vec![0; MAX_SESSION_JSON + 1];
    let status = decompressor
        .decompress(compressed, &mut output, FlushDecompress::Finish)
        .ok()?;
    let bytes = usize::try_from(decompressor.total_out()).ok()?;
    if status != Status::StreamEnd
        || bytes > MAX_SESSION_JSON
        || decompressor.total_in() != compressed.len() as u64
    {
        return None;
    }
    output.truncate(bytes);
    Some(output)
}

fn authorize(session: &DecodedSession, actor: &Actor, secret: &[u8]) -> DjangoChannelGroupRead {
    use DjangoChannelGroupRead::{Allowed, Forbidden, LoginRequired, SessionNeedsInvalidation};

    if !actor.active {
        return LoginRequired;
    }
    let expected = current_auth_hash(secret, &actor.password);
    if !session
        .auth_hash
        .as_ref()
        .is_some_and(|hash| bool::from(expected.as_bytes().ct_eq(hash.as_bytes())))
    {
        return SessionNeedsInvalidation;
    }
    if !actor.staff {
        return LoginRequired;
    }
    if actor.superuser || actor.model_permission {
        Allowed {
            user_id: session.user_id,
        }
    } else {
        Forbidden
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[derive(Deserialize)]
    struct DjangoRows {
        actor_id: i64,
        current_password_field: String,
        changed_password_field: String,
        uncompressed: String,
        compressed: String,
        missing_hash: String,
        old_key: String,
        unexpected_backend: String,
        oversize_decoded: String,
    }

    fn fixtures() -> (DjangoRows, String) {
        let rows = serde_json::from_str(include_str!(
            "../datasets/django-admin-session-stock-6-0-9.json"
        ))
        .unwrap();
        let secret =
            Sha256::digest(b"Dispatcharr synthetic Django SessionStore contract / current");
        (rows, format!("{secret:x}"))
    }

    fn default_actor(fixture: &DjangoRows) -> Actor {
        Actor {
            password: fixture.current_password_field.clone(),
            active: true,
            staff: true,
            superuser: false,
            model_permission: true,
        }
    }

    #[test]
    fn pinned_django_roundtrip_plain_and_compressed() {
        let (fixture, key) = fixtures();
        assert!(!fixture.uncompressed.starts_with('.'));
        assert!(fixture.compressed.starts_with('.'));
        for row in [&fixture.uncompressed, &fixture.compressed] {
            let session = decode_session(row, key.as_bytes()).expect("genuine Django session");
            assert_eq!(session.user_id, fixture.actor_id);
            assert_eq!(
                authorize(&session, &default_actor(&fixture), key.as_bytes()),
                DjangoChannelGroupRead::Allowed {
                    user_id: fixture.actor_id
                }
            );
        }
    }

    #[test]
    fn native_issuer_roundtrips_django_auth_and_ascii_escaped_session_values() {
        let (fixture, key) = fixtures();
        let original = verified_session_json(&fixture.compressed, key.as_bytes()).unwrap();
        let mut fields: Map<String, Value> = serde_json::from_slice(&original).unwrap();
        fields.insert("owned_preference".into(), Value::String("café ☀".into()));
        let signed = encode_session(&fields, key.as_bytes(), 1770000000).unwrap();
        let bytes = verified_session_json(&signed, key.as_bytes()).unwrap();
        assert!(bytes.is_ascii());
        assert_eq!(
            serde_json::from_slice::<Map<String, Value>>(&bytes).unwrap(),
            fields
        );
        let session = decode_session(&signed, key.as_bytes()).unwrap();
        assert_eq!(session.user_id, fixture.actor_id);
        assert_eq!(
            authorize(&session, &default_actor(&fixture), key.as_bytes()),
            DjangoChannelGroupRead::Allowed {
                user_id: fixture.actor_id
            },
        );
    }

    #[test]
    fn invalid_shapes_oversize_and_previous_signing_key_do_not_grant() {
        let (fixture, key) = fixtures();
        let malformed = fixture.compressed.rsplit_once(':').unwrap().0;
        let oversized = "a".repeat(MAX_SIGNED_ROW + 1);
        for row in [
            malformed,
            &fixture.old_key,
            &fixture.unexpected_backend,
            &fixture.oversize_decoded,
            &oversized,
        ] {
            assert!(decode_session(row, key.as_bytes()).is_none());
        }
        assert!(!issued_session_key(""));
        assert!(!issued_session_key(&"A".repeat(32)));
        assert!(!issued_session_key(&"b".repeat(33)));
        assert!(issued_session_key(&"a7".repeat(16)));
    }

    #[test]
    fn auth_hash_change_and_missing_hash_require_session_invalidation() {
        let (fixture, key) = fixtures();
        let mut actor = default_actor(&fixture);
        let missing = decode_session(&fixture.missing_hash, key.as_bytes()).unwrap();
        assert_eq!(
            authorize(&missing, &actor, key.as_bytes()),
            DjangoChannelGroupRead::SessionNeedsInvalidation
        );
        let session = decode_session(&fixture.compressed, key.as_bytes()).unwrap();
        actor.password = fixture.changed_password_field;
        assert_eq!(
            authorize(&session, &actor, key.as_bytes()),
            DjangoChannelGroupRead::SessionNeedsInvalidation
        );
    }

    #[test]
    fn requires_current_active_staff_and_view_or_change_authority() {
        let (fixture, key) = fixtures();
        let session = decode_session(&fixture.compressed, key.as_bytes()).unwrap();
        let mut actor = default_actor(&fixture);
        actor.model_permission = false;
        assert_eq!(
            authorize(&session, &actor, key.as_bytes()),
            DjangoChannelGroupRead::Forbidden
        );
        actor.superuser = true;
        assert_eq!(
            authorize(&session, &actor, key.as_bytes()),
            DjangoChannelGroupRead::Allowed {
                user_id: fixture.actor_id
            }
        );
        actor.staff = false;
        assert_eq!(
            authorize(&session, &actor, key.as_bytes()),
            DjangoChannelGroupRead::LoginRequired
        );
        actor.staff = true;
        actor.active = false;
        assert_eq!(
            authorize(&session, &actor, key.as_bytes()),
            DjangoChannelGroupRead::LoginRequired
        );
    }

    #[tokio::test]
    #[ignore = "Requires a coordinator-owned disposable PostgreSQL database"]
    async fn current_postgres_session_expiry_role_and_permission_rows_are_rechecked() {
        let url = std::env::var("RUST_TEST_DJANGO_ADMIN_DATABASE_URL").unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET search_path TO pg_temp")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TEMP TABLE django_session (session_key varchar(40) PRIMARY KEY,
                  session_data text NOT NULL, expire_date timestamptz NOT NULL);
             CREATE TEMP TABLE accounts_user (id bigint PRIMARY KEY, password text NOT NULL,
                  is_active bool, is_staff bool, is_superuser bool, username text NOT NULL);
             CREATE TEMP TABLE dispatcharr_channels_channelgroup (id bigint PRIMARY KEY, name text UNIQUE NOT NULL);
             CREATE TEMP TABLE django_content_type (id int PRIMARY KEY, app_label text, model text);
             CREATE TEMP TABLE auth_permission (id int PRIMARY KEY, content_type_id int, codename text);
             CREATE TEMP TABLE accounts_user_user_permissions (user_id bigint, permission_id int);
             CREATE TEMP TABLE accounts_user_groups (user_id bigint, group_id int);
             CREATE TEMP TABLE auth_group_permissions (group_id int, permission_id int);
             INSERT INTO django_content_type VALUES (1, 'dispatcharr_channels', 'channelgroup'),
                 (2, 'dispatcharr_channels', 'channel');
             INSERT INTO auth_permission VALUES (1, 1, 'view_channelgroup'),
                 (2, 1, 'change_channelgroup'), (3, 2, 'view_channelgroup');",
        )
        .execute(&pool)
        .await
        .unwrap();

        let (fixture, key) = fixtures();
        let cookie = "a7".repeat(16);
        sqlx::query("INSERT INTO accounts_user VALUES ($1, $2, true, true, false, 'owned-viewer')")
            .bind(fixture.actor_id)
            .bind(&fixture.current_password_field)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO django_session VALUES ($1, $2, now() + interval '1 hour')")
            .bind(&cookie)
            .bind(&fixture.compressed)
            .execute(&pool)
            .await
            .unwrap();
        let read =
            DjangoAdminSessions::with_current_key(Store::from_pool(pool.clone()), &key).unwrap();
        let allowed = DjangoChannelGroupRead::Allowed {
            user_id: fixture.actor_id,
        };
        assert_eq!(
            read.channel_group_read(&cookie).await.unwrap(),
            DjangoChannelGroupRead::Forbidden
        );
        sqlx::query("INSERT INTO accounts_user_user_permissions VALUES ($1, 3)")
            .bind(fixture.actor_id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            read.channel_group_read(&cookie).await.unwrap(),
            DjangoChannelGroupRead::Forbidden
        );
        sqlx::query("INSERT INTO accounts_user_user_permissions VALUES ($1, 1)")
            .bind(fixture.actor_id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(read.channel_group_read(&cookie).await.unwrap(), allowed);
        sqlx::query("DELETE FROM accounts_user_user_permissions WHERE permission_id = 1")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts_user_groups VALUES ($1, 5)")
            .bind(fixture.actor_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO auth_group_permissions VALUES (5, 2)")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(read.channel_group_read(&cookie).await.unwrap(), allowed);

        sqlx::raw_sql(
            "INSERT INTO dispatcharr_channels_channelgroup VALUES (7, 'News'), (12, 'Entertainment');
             INSERT INTO dispatcharr_channels_channelgroup
                 SELECT id, 'synthetic-' || id FROM generate_series(100, 199) AS id;",
        )
        .execute(&pool)
        .await
        .unwrap();
        let query = DjangoChannelGroupQuery {
            search: "",
            page: 2,
            show_all: false,
            order: DjangoChannelGroupOrder::IdDescending,
        };
        let DjangoChannelGroupPageResult::View(page) =
            read.channel_group_page(&cookie, query).await.unwrap()
        else {
            panic!("authorized read-only page")
        };
        assert_eq!(page.username, "owned-viewer");
        assert_eq!((page.total, page.matches, page.pages), (102, 102, 2));
        assert_eq!(
            page.rows.iter().map(|item| item.0).collect::<Vec<_>>(),
            vec![12, 7]
        );
        let query = DjangoChannelGroupQuery {
            search: "ENTERTAINMENT",
            page: 1,
            show_all: true,
            order: DjangoChannelGroupOrder::NameAscending,
        };
        let DjangoChannelGroupPageResult::View(page) =
            read.channel_group_page(&cookie, query).await.unwrap()
        else {
            panic!("authorized searched page")
        };
        assert_eq!((page.total, page.matches), (102, 1));
        assert_eq!(page.rows, vec![(12, "Entertainment".to_owned())]);

        sqlx::query("UPDATE accounts_user SET password = $1 WHERE id = $2")
            .bind(&fixture.changed_password_field)
            .bind(fixture.actor_id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            read.channel_group_read(&cookie).await.unwrap(),
            DjangoChannelGroupRead::SessionNeedsInvalidation
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM django_session")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        sqlx::query("UPDATE django_session SET expire_date = now() WHERE session_key = $1")
            .bind(&cookie)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            read.channel_group_read(&cookie).await.unwrap(),
            DjangoChannelGroupRead::LoginRequired
        );
        sqlx::query("ALTER TABLE accounts_user ADD COLUMN last_login timestamptz")
            .execute(&pool)
            .await
            .unwrap();
        let synthetic_password = "ordinary temporary form input";
        let encoded_password = crate::passwords::encode_password(synthetic_password).unwrap();
        sqlx::query("UPDATE accounts_user SET password = $1 WHERE id = $2")
            .bind(encoded_password)
            .bind(fixture.actor_id)
            .execute(&pool)
            .await
            .unwrap();
        let auth = AuthService::new(
            Store::from_pool(pool.clone()),
            crate::AuthConfig::new(key).unwrap(),
        );
        let fresh = read
            .login(&auth, "owned-viewer", synthetic_password.into(), None)
            .await
            .unwrap()
            .expect("owned active staff login");
        assert!(read.signed_staff(&fresh).await.unwrap());
        assert_eq!(read.channel_group_read(&fresh).await.unwrap(), allowed);
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT last_login IS NOT NULL FROM accounts_user WHERE id = $1"
        )
        .bind(fixture.actor_id)
        .fetch_one(&pool)
        .await
        .unwrap());
        let repeated = read
            .login(
                &auth,
                "owned-viewer",
                synthetic_password.into(),
                Some(&fresh),
            )
            .await
            .unwrap()
            .expect("same actor repeat");
        assert_eq!(fresh, repeated);
        assert!(read.logout(&auth, &fresh).await.unwrap());
        assert!(!read.signed_staff(&fresh).await.unwrap());

        pool.close().await;
    }
}
