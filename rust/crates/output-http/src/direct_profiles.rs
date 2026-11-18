//! Restricted direct M3U: Source active-profile selection and raw URL dependency.
//! Original Dispatcharr/CPython/regex authors retain all behavior credit.
use crate::{epg_custom_scope::Scope, render::Channel, render::Direct};
use dispatcharr_store::{Store, StoreError, User};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;

pub(crate) struct Profile {
    account: i64,
    account_type: String,
    server: Option<String>,
    username: Option<String>,
    password: Option<String>,
    search: String,
    replacement: String,
}
pub(crate) struct Allowed {
    profiles: Vec<Profile>,
    bytes: usize,
}
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"M3U export profile capacity exceeded."}),
    )
}
fn unsupported() -> StoreError {
    StoreError::Unsupported(
        "M3U direct legacy provider allowlist coercion is outside the qualified domain",
    )
}
fn charge(used: &mut usize, count: usize, limit: usize) -> Result<(), StoreError> {
    *used = used
        .checked_add(count)
        .filter(|v| *v <= limit)
        .ok_or_else(capacity)?;
    Ok(())
}
impl Allowed {
    /// None means unrestricted; empty/malformed lists remain explicitly restricted.
    /// Normal persisted integer IDs are supported; legacy coercions stay visible.
    pub(crate) async fn read(
        store: &Store,
        user: Option<&User>,
        limit: usize,
    ) -> Result<Option<Self>, StoreError> {
        let Some(value) = user
            .and_then(|u| u.custom_properties.get("allowed_m3u_profile_ids"))
            .filter(|v| !v.is_null())
        else {
            return Ok(None);
        };
        let Some(values) = value.as_array() else {
            return Ok(Some(Self {
                profiles: Vec::new(),
                bytes: 0,
            }));
        };
        if values.len() > 65536 || values.len().saturating_mul(std::mem::size_of::<i64>()) > limit {
            return Err(capacity());
        }
        // Exact preallocation avoids geometric Result<Vec<_>> growth. Check actual
        // capacity before admitting the query, not merely the number of IDs.
        let mut ids = Vec::<i64>::with_capacity(values.len());
        if ids.capacity().saturating_mul(std::mem::size_of::<i64>()) > limit {
            return Err(capacity());
        }
        for value in values {
            ids.push(value.as_i64().ok_or_else(unsupported)?);
        }
        if ids.is_empty() {
            return Ok(Some(Self {
                profiles: Vec::new(),
                bytes: 0,
            }));
        }
        let mut rows = sqlx::query(r#"SELECT DISTINCT ON(p.m3u_account_id) p.m3u_account_id::bigint AS account,
          a.account_type,CASE WHEN octet_length(a.server_url)<=65536 THEN a.server_url END AS server,
          CASE WHEN octet_length(a.username)<=65536 THEN a.username END AS username,
          CASE WHEN octet_length(a.password)<=65536 THEN a.password END AS password,
          CASE WHEN octet_length(p.search_pattern)<=65536 THEN p.search_pattern END AS search,
          CASE WHEN octet_length(p.replace_pattern)<=65536 THEN p.replace_pattern END AS replacement,
          (COALESCE(octet_length(a.server_url),0)>65536 OR COALESCE(octet_length(a.username),0)>65536 OR COALESCE(octet_length(a.password),0)>65536 OR COALESCE(octet_length(p.search_pattern),0)>65536 OR COALESCE(octet_length(p.replace_pattern),0)>65536) AS oversized
          FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id
          WHERE p.id=ANY($1::bigint[]) AND p.is_active AND a.is_active
          ORDER BY p.m3u_account_id,p.is_default DESC,p.id"#).bind(&ids).fetch(store.pool());
        let mut profiles = Vec::new();
        let mut strings = 0usize;
        while let Some(row) = rows.try_next().await? {
            if row.try_get::<bool, _>("oversized")? {
                return Err(capacity());
            }
            let p = Profile {
                account: row.try_get("account")?,
                account_type: row.try_get("account_type")?,
                server: row.try_get("server")?,
                username: row.try_get("username")?,
                password: row.try_get("password")?,
                search: row
                    .try_get::<Option<String>, _>("search")?
                    .unwrap_or_default(),
                replacement: row
                    .try_get::<Option<String>, _>("replacement")?
                    .unwrap_or_default(),
            };
            let own = p.account_type.capacity()
                + p.server.as_ref().map_or(0, String::capacity)
                + p.username.as_ref().map_or(0, String::capacity)
                + p.password.as_ref().map_or(0, String::capacity)
                + p.search.capacity()
                + p.replacement.capacity();
            charge(&mut strings, own, limit)?;
            if profiles.len() == profiles.capacity() {
                let next = profiles.capacity().max(4).saturating_mul(2);
                if next
                    .saturating_mul(std::mem::size_of::<Profile>())
                    .saturating_add(strings)
                    .saturating_add(ids.capacity() * 8)
                    > limit
                {
                    return Err(capacity());
                }
                profiles.reserve_exact(next - profiles.len());
            }
            if profiles
                .capacity()
                .saturating_mul(std::mem::size_of::<Profile>())
                .saturating_add(strings)
                .saturating_add(ids.capacity() * 8)
                > limit
            {
                return Err(capacity());
            }
            profiles.push(p);
        }
        drop(rows);
        drop(ids);
        let bytes = profiles
            .capacity()
            .saturating_mul(std::mem::size_of::<Profile>())
            .saturating_add(strings);
        Ok(Some(Self { profiles, bytes }))
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
    pub(crate) async fn apply(
        &self,
        store: &Store,
        channels: &mut [Channel],
        scope: &Arc<Scope>,
        used: &mut usize,
        limit: usize,
    ) -> Result<(), StoreError> {
        // Metadata cursor is already fully consumed/dropped. This one streamed
        // SQL query per page invokes only the pure dependency while held, never
        // another SQL checkout; even a one-connection pool cannot self-deadlock.
        for page in channels.chunks_mut(100) {
            let ids = page.iter().map(|c| c.id).collect::<Vec<_>>();
            let mut rows=sqlx::query(r#"SELECT selected.ordinal,cs.channel_id::bigint AS channel,s.m3u_account_id::bigint AS account,
              CASE WHEN octet_length(s.url)<=65536 THEN s.url END AS url,COALESCE(octet_length(s.url),0)>65536 AS oversized,
              s.stream_id::bigint,s.is_catchup,s.catchup_days
              FROM unnest($1::bigint[]) WITH ORDINALITY selected(id,ordinal)
              JOIN dispatcharr_channels_channelstream cs ON cs.channel_id=selected.id
              JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
              ORDER BY selected.ordinal,cs."order""#).bind(&ids).fetch(store.pool());
            while let Some(row) = rows.try_next().await? {
                let ordinal: i64 = row.try_get("ordinal")?;
                let index = usize::try_from(ordinal - 1).map_err(|_| capacity())?;
                let c = page.get_mut(index).ok_or_else(capacity)?;
                if c.id != row.try_get::<i64, _>("channel")? {
                    return Err(capacity());
                }
                if c.direct.is_some() {
                    continue;
                }
                let account: Option<i64> = row.try_get("account")?;
                let Some(profile) = account
                    .and_then(|id| self.profiles.binary_search_by_key(&id, |p| p.account).ok())
                    .map(|i| &self.profiles[i])
                else {
                    continue;
                };
                if row.try_get::<bool, _>("oversized")? {
                    return Err(capacity());
                }
                let stored: Option<String> = row.try_get("url")?;
                let stream: Option<i64> = row.try_get("stream_id")?;
                // Bound construction before cloning source fields into the RPC.
                let input = profile.server.as_ref().map_or(0, String::len)
                    + profile.username.as_ref().map_or(0, String::len)
                    + profile.password.as_ref().map_or(0, String::len)
                    + profile.search.len()
                    + profile.replacement.len()
                    + stored.as_ref().map_or(0, String::len)
                    + 1024;
                if input.saturating_mul(6) > 1024 * 1024
                    || used
                        .checked_add(input.saturating_mul(6))
                        .is_none_or(|v| v > limit)
                {
                    return Err(capacity());
                }
                let value=scope.profile_urls(vec![json!({"account_type":profile.account_type,"server_url":profile.server,"username":profile.username,"password":profile.password,"search":profile.search,"replacement":profile.replacement,"stored_url":stored,"stream_id":stream})]).await?;
                if value.get("capacity") == Some(&Value::Bool(true)) {
                    return Err(capacity());
                }
                if value.get("capacity") != Some(&Value::Bool(false)) {
                    return Err(unsupported());
                }
                let result = value
                    .get("rows")
                    .and_then(Value::as_array)
                    .filter(|r| r.len() == 1)
                    .and_then(|r| r.first())
                    .ok_or_else(unsupported)?;
                let fields = result
                    .as_object()
                    .filter(|v| {
                        v.len() == 3
                            && v.contains_key("url")
                            && v.contains_key("source_exception")
                            && v.contains_key("caught")
                    })
                    .ok_or_else(unsupported)?;
                let exception = |value: &Value| {
                    value.is_object()
                        && value.as_object().is_some_and(|v| v.len() == 1)
                        && value
                            .get("actual_class")
                            .and_then(Value::as_str)
                            .is_some_and(|v| !v.is_empty() && v.len() <= 128)
                };
                match fields.get("caught") {
                    Some(Value::Null) => {}
                    Some(v) if exception(v) => {}
                    _ => return Err(unsupported()),
                }
                match fields.get("source_exception") {
                    Some(Value::Null) => {}
                    Some(v) if exception(v) => return Err(StoreError::UnhandledRestFault),
                    _ => return Err(unsupported()),
                }
                let url = match result.get("url") {
                    Some(Value::Null) => continue,
                    Some(Value::String(v)) if v.is_empty() => continue,
                    Some(Value::String(v)) if v.len() <= 65536 => v.to_owned(),
                    _ => return Err(unsupported()),
                };
                charge(used, url.capacity(), limit)?;
                c.direct = Some(Direct {
                    url,
                    catchup_days: if profile.account_type == "XC"
                        && row.try_get::<bool, _>("is_catchup")?
                    {
                        row.try_get::<Option<i32>, _>("catchup_days")?.unwrap_or(0)
                    } else {
                        0
                    },
                });
            }
        }
        Ok(())
    }
}
