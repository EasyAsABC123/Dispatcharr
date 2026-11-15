//! Bounded SQL account snapshot and exact-ID Redis ownership journal.
//! Original Dispatcharr authors retain algorithm credit; journal is internal.
use super::*;
use crate::notifications::NotificationCache;
use sqlx::PgConnection;
pub(super) const PAGE: usize = 256;
/// At most five known keys, even when an account CSV value is arbitrarily long.
#[derive(Default)]
struct Fields {
    word: String,
    invalid: bool,
    trailing: bool,
    keys: Vec<String>,
}
impl Fields {
    fn finish(&mut self) {
        if !self.invalid
            && ["name", "url", "tvg_id", "m3u_id", "group"].contains(&self.word.as_str())
            && !self.keys.contains(&self.word)
        {
            self.keys.push(self.word.clone());
        }
        self.word.clear();
        self.invalid = false;
        self.trailing = false;
    }
    fn feed(&mut self, text: &str) {
        for ch in text.chars() {
            if ch == ',' {
                self.finish();
                continue;
            }
            if self.invalid {
                continue;
            }
            if ch.is_whitespace() || matches!(ch, '\u{1c}'..='\u{1f}') {
                if !self.word.is_empty() {
                    self.trailing = true;
                }
                continue;
            }
            if self.trailing || self.word.len() >= 6 || !ch.is_ascii() {
                self.invalid = true;
                continue;
            }
            self.word.push(ch);
        }
    }
}
/// All task-private tables disappear with this one physical SQL connection.
/// Only bounded pages and <=5 parsed fields are transferred to Rust.
pub(super) async fn prepare(
    connection: &mut PgConnection,
    request: &Request,
) -> Result<usize, StoreError> {
    sqlx::query("CREATE TEMP TABLE rust_rehash_raw(id bigint,property_type text,kind text,raw jsonb) ON COMMIT PRESERVE ROWS").execute(&mut*connection).await?;
    sqlx::query("INSERT INTO rust_rehash_raw SELECT id::bigint,jsonb_typeof(custom_properties),jsonb_typeof(custom_properties->'hash_key'),custom_properties->'hash_key' FROM m3u_m3uaccount WHERE custom_properties ? 'hash_key'").execute(&mut*connection).await?;
    sqlx::query("CREATE TEMP TABLE rust_rehash_overrides(id bigint PRIMARY KEY,fields text[] NOT NULL) ON COMMIT PRESERVE ROWS").execute(&mut*connection).await?;
    sqlx::query("DECLARE rust_rehash_accounts NO SCROLL CURSOR WITH HOLD FOR SELECT id,property_type,kind FROM rust_rehash_raw").execute(&mut*connection).await?;
    loop {
        let rows = sqlx::query("FETCH FORWARD 256 FROM rust_rehash_accounts")
            .fetch_all(&mut *connection)
            .await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let id: i64 = row.try_get("id")?;
            let property_type: Option<String> = row.try_get("property_type")?;
            let kind: Option<String> = row.try_get("kind")?;
            if property_type.as_deref() != Some("object") {
                return Err(capacity());
            }
            let mut fields = Fields::default();
            match kind.as_deref() {
                Some("string") => {
                    let mut offset = 1i32;
                    loop {
                        let chunk:Option<String>=sqlx::query_scalar("SELECT substring(raw #>> '{}' FROM $2 FOR 4096) FROM rust_rehash_raw WHERE id=$1").bind(id).bind(offset).fetch_one(&mut*connection).await?;
                        let Some(chunk) = chunk else {
                            return Err(capacity());
                        };
                        let chars = chunk.chars().count();
                        if chars == 0 {
                            break;
                        }
                        fields.feed(&chunk);
                        offset = offset
                            .checked_add(i32::try_from(chars).map_err(|_| capacity())?)
                            .ok_or_else(capacity)?;
                        if chars < 4096 {
                            break;
                        }
                    }
                    fields.finish();
                }
                None | Some("null" | "boolean" | "number") => {}
                _ => return Err(capacity()),
            }
            if !fields.keys.is_empty() {
                sqlx::query("INSERT INTO rust_rehash_overrides(id,fields) VALUES($1,$2)")
                    .bind(id)
                    .bind(fields.keys)
                    .execute(&mut *connection)
                    .await?;
            }
        }
    }
    sqlx::query("CLOSE rust_rehash_accounts")
        .execute(&mut *connection)
        .await?;
    sqlx::query("DROP TABLE rust_rehash_raw")
        .execute(&mut *connection)
        .await?;
    sqlx::query("CREATE TEMP TABLE rust_rehash_targets(seq bigserial PRIMARY KEY,id bigint NOT NULL) ON COMMIT PRESERVE ROWS").execute(&mut*connection).await?;
    sqlx::query("INSERT INTO rust_rehash_targets(id) SELECT a.id::bigint FROM m3u_m3uaccount a LEFT JOIN rust_rehash_overrides o ON o.id=a.id WHERE ($1::bigint IS NULL OR a.id=$1) AND ($1::bigint IS NOT NULL OR o.id IS NULL)").bind(request.account).execute(&mut*connection).await?;
    sqlx::query("CREATE TEMP TABLE rust_rehash_seen(hash text PRIMARY KEY,id bigint NOT NULL) ON COMMIT PRESERVE ROWS").execute(&mut*connection).await?;
    sqlx::query(
        "CREATE TEMP TABLE rust_rehash_deleted(id bigint PRIMARY KEY) ON COMMIT PRESERVE ROWS",
    )
    .execute(&mut *connection)
    .await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*)::bigint FROM rust_rehash_targets")
        .fetch_one(&mut *connection)
        .await?;
    usize::try_from(count).map_err(|_| capacity())
}
#[derive(Clone)]
pub(super) struct Owned {
    cache: NotificationCache,
    key: String,
    token: String,
}
impl Owned {
    pub(super) fn new(cache: NotificationCache, token: String) -> Self {
        Self {
            cache,
            key: format!("rust:rehash:owned:{}", uuid::Uuid::new_v4().simple()),
            token,
        }
    }
    async fn record(&self, id: i64) -> Result<(), StoreError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("redis.call('SADD',KEYS[1],ARGV[1]);redis.call('EXPIRE',KEYS[1],7200);return 1")
            .arg(1)
            .arg(&self.key)
            .arg(id);
        let _: i64 = self.cache.command_settled(cmd).await?;
        Ok(())
    }
    pub(super) async fn acquire(&self, id: i64, preacquired: bool) -> Result<bool, StoreError> {
        if preacquired && self.token.is_empty() {
            return Ok(false);
        }
        let key = format!("{KEY}{id}");
        let result = async {
            if preacquired {
                let mut cmd = redis::cmd("GET");
                cmd.arg(&key);
                let old: Option<String> = self.cache.command_settled(cmd).await?;
                if old.as_deref() == Some(self.token.as_str()) {
                    let mut cmd = redis::cmd("EXPIRE");
                    cmd.arg(&key).arg(300);
                    let _: i64 = self.cache.command_settled(cmd).await?;
                    return Ok::<_, StoreError>(true);
                }
                if old.is_some() {
                    return Ok(false);
                }
            }
            let mut cmd = redis::cmd("SET");
            cmd.arg(&key)
                .arg(&self.token)
                .arg("EX")
                .arg(if preacquired { 300 } else { 7200 })
                .arg("NX");
            let reply: Option<String> = self.cache.command_settled(cmd).await?;
            Ok(reply.as_deref() == Some("OK"))
        }
        .await;
        match result {
            Ok(true) => {
                if let Err(error) = self.record(id).await {
                    // SET/confirmation is known-owned. Attempt its conditional
                    // release even if journal publication failed, never invent ACK.
                    let release = self.release_one(id).await;
                    eprintln!(
                        "Rehash journal {} id {id} failed: {error:?}; release {release:?}",
                        self.key
                    );
                    return Err(error);
                }
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(error) => {
                // Account SET itself may be ambiguous. A conditional release is
                // safe for both accepted and unaccepted NX; failures stay unknown.
                let witness = self.record(id).await;
                let release = self.release_one(id).await;
                eprintln!(
                    "Rehash lock {} id {id} unknown: {error:?}; attempt witness {witness:?}; release {release:?}",
                    self.key
                );
                Err(error)
            }
        }
    }
    async fn page(&self, cursor: u64) -> Result<(u64, Vec<i64>), StoreError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("local r=redis.call('SSCAN',KEYS[1],ARGV[1],'COUNT',256);if redis.call('EXISTS',KEYS[1])==1 then redis.call('EXPIRE',KEYS[1],7200) end;return r").arg(1).arg(&self.key).arg(cursor);
        self.cache.command_settled(cmd).await
    }
    async fn release_one(&self, id: i64) -> Result<(), StoreError> {
        // SREM is sent ONLY AFTER the real compare-token release reply. Unknown
        // release replies therefore leave the journal witness and its TTL intact.
        script(&self.cache, RELEASE, id, &self.token).await?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("redis.call('SREM',KEYS[1],ARGV[1]);if redis.call('SCARD',KEYS[1])==0 then redis.call('DEL',KEYS[1]) else redis.call('EXPIRE',KEYS[1],7200) end;return 1").arg(1).arg(&self.key).arg(id);
        let _: i64 = self.cache.command_settled(cmd).await?;
        Ok(())
    }
    pub(super) async fn release(&self) -> Result<(), StoreError> {
        let mut cursor = 0u64;
        let mut failed = None;
        loop {
            let (next, ids) = self.page(cursor).await?;
            for id in ids {
                if let Err(error) = self.release_one(id).await {
                    if failed.is_none() {
                        failed = Some(error);
                    }
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        match failed {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    pub(super) async fn renew(&self) -> Result<(), StoreError> {
        let mut cursor = 0u64;
        loop {
            let (next, ids) = self.page(cursor).await?;
            for id in ids {
                script(&self.cache, RENEW, id, &self.token).await?;
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn csv_chunk_boundaries_ignore_large_unknown_tokens_without_growing_state() {
        let mut stream = Fields::default();
        for part in [
            "\u{1c}u",
            "rl, name, tv",
            "g_id, url,",
            "m3u_id, group,na me,",
        ] {
            stream.feed(part);
        }
        for _ in 0..10000 {
            stream.feed("unknown");
            assert!(stream.word.len() <= 6);
        }
        stream.feed(",url");
        stream.finish();
        assert_eq!(stream.keys, ["url", "name", "tvg_id", "m3u_id", "group"]);
    }
}
