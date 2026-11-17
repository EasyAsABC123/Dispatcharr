//! Provider account-info/profile refresh from pinned Original Dispatcharr tasks.
//! Original Dispatcharr/Django authors retain all application/algorithm credit.
use crate::{
    task_sql_session::TaskSqlSession, xc_account_provider as provider, AuthService, StoreError,
};
use chrono::{DateTime, Datelike, Utc};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::{sync::Arc, time::Duration};
type Owner = Arc<dyn Send + Sync>;
/// Actual profile.save() instance fields; never includes credentials or CP.
#[derive(Clone)]
pub struct SavedProfile {
    pub id: i64,
    pub account_id: i64,
    pub name: String,
    pub account_name: String,
    pub expiration: Option<DateTime<Utc>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalOutcome {
    Applied,
    CaughtSettledFailure,
}
/// Required normal production callback. No no-op/default constructor exists.
pub trait ProfileSaveSignal: Send + Sync {
    /// DRAFT extension: explicit unsupported fallback, never a no-op success.
    /// The normal production implementation owns the Collector transaction.
    fn delete_profile(
        &self,
        _session: TaskSqlSession,
        _account_id: i64,
        _profile_id: i64,
        _owner: Owner,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async {
            Err(StoreError::Unsupported(
                "Profile deletion lifecycle is unavailable.",
            ))
        })
    }

    /// All profile rows have already been deleted by the owned account Collector;
    /// this is only the shared Source post_delete receiver sequence.
    fn after_profile_delete_in_collector<'a>(
        &'a self,
        _connection: &'a mut sqlx::PgConnection,
        _account_id: i64,
        _profile_id: i64,
        _owner: Owner,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async {
            Err(StoreError::Unsupported(
                "Profile post-delete Collector unavailable.",
            ))
        })
    }
    /// Original account pre_save cleanup; no profile deletion/allow-list scrub.
    fn deactivate_account(
        &self,
        _session: TaskSqlSession,
        _account_id: i64,
        _owner: Owner,
    ) -> BoxFuture<'_, Result<SignalOutcome, StoreError>> {
        Box::pin(async {
            Err(StoreError::Unsupported(
                "Account deactivation signal unavailable.",
            ))
        })
    }

    fn guard(
        &self,
        session: TaskSqlSession,
        profile_id: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<(), StoreError>>;
    fn after_save(
        &self,
        session: TaskSqlSession,
        profile: SavedProfile,
        owner: Owner,
    ) -> BoxFuture<'_, Result<SignalOutcome, StoreError>>;
}
pub struct Prepared {
    session: TaskSqlSession,
    default_agent: String,
    signal: Arc<dyn ProfileSaveSignal>,
    owner: Owner,
}
struct Profile {
    id: i64,
    name: String,
    account: i64,
    account_name: String,
    expiration: Option<DateTime<Utc>>,
    kind: String,
    base: String,
    username: String,
    password: String,
    agent: Option<String>,
    search: String,
    replace: String,
    properties: Map<String, Value>,
}
// Credential-bearing rows intentionally never implement Debug/Display.
fn bad() -> StoreError {
    StoreError::Unsupported("XC account row/response capacity or type")
}
pub async fn prepare(
    auth: AuthService,
    owner: Owner,
    signal: Arc<dyn ProfileSaveSignal>,
) -> Result<Prepared, StoreError> {
    // Resolve existing cache-authoritative default before holding any SQL cursor.
    // No callback can check out another connection behind a held profile cursor.
    let writer = auth.task_sql_writer()?;
    let auth = auth
        .with_request_writer_permit(writer)?
        .with_retained_scope(owner.clone());
    let default_agent = crate::core_settings_special_effects::default_user_agent(&auth).await?;
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    Ok(Prepared {
        session,
        default_agent,
        signal,
        owner,
    })
}
impl Prepared {
    pub async fn close(&self) -> Result<(), StoreError> {
        self.session.close().await
    }
    /// Read-only domain admission before exact broker CAS. Unsupported regex,
    /// URL and row budgets retain the actual raw FIFO head without claiming it.
    pub async fn preflight(&self, id: i64, all: bool) -> Result<(), StoreError> {
        self.session.run(move|c|Box::pin(async move{sqlx::query("DECLARE rust_xc_admit NO SCROLL CURSOR WITH HOLD FOR SELECT p.id::bigint,p.name,p.exp_date,a.name AS account_name,p.m3u_account_id::bigint AS account,a.account_type,CASE WHEN COALESCE(octet_length(a.server_url),0)<=65536 THEN a.server_url END AS server_url,CASE WHEN COALESCE(octet_length(a.username),0)<=65536 THEN a.username END AS username,CASE WHEN COALESCE(octet_length(a.password),0)<=65536 THEN a.password END AS password,CASE WHEN COALESCE(octet_length(u.user_agent),0)<=65536 THEN u.user_agent END AS user_agent,p.search_pattern,p.replace_pattern,CASE WHEN COALESCE(octet_length(p.custom_properties::text),0)<=1048576 THEN p.custom_properties END AS custom_properties,(COALESCE(octet_length(p.custom_properties::text),0)<=1048576 AND COALESCE(octet_length(a.server_url),0)<=65536 AND COALESCE(octet_length(a.username),0)<=65536 AND COALESCE(octet_length(a.password),0)<=65536 AND COALESCE(octet_length(u.user_agent),0)<=65536) AS fits FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id LEFT JOIN core_useragent u ON u.id=a.user_agent_id WHERE (CASE WHEN $2 THEN (a.id=$1::bigint AND a.is_active AND p.is_active) ELSE p.id=$1::bigint END) AND a.account_type='XC'").bind(id).bind(all).execute(&mut*c).await.map(|_|())})).await??;
        let observed = async {
            loop {
                let rows = self
                    .session
                    .run(|c| {
                        Box::pin(async move {
                            let rows = sqlx::query("FETCH FORWARD 32 FROM rust_xc_admit")
                                .fetch_all(&mut *c)
                                .await?;
                            rows.into_iter()
                                .map(profile)
                                .collect::<Result<Vec<_>, StoreError>>()
                        })
                    })
                    .await??;
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    let _ = provider::credentials(
                        &row.base,
                        &row.username,
                        &row.password,
                        &row.search,
                        &row.replace,
                    )?;
                    self.signal
                        .guard(self.session.clone(), row.id, self.owner.clone())
                        .await?;
                }
            }
            Ok::<_, StoreError>(())
        }
        .await;
        let closed = self
            .session
            .run(|c| {
                Box::pin(async move {
                    sqlx::query("CLOSE rust_xc_admit")
                        .execute(&mut *c)
                        .await
                        .map(|_| ())
                })
            })
            .await?;
        closed?;
        observed
    }
    pub async fn info(
        &self,
        id: i64,
        events: Arc<dyn UpdateEventSink>,
        owner: Owner,
    ) -> Result<Value, StoreError> {
        let profile=self.session.run(move|c|Box::pin(async move{
            let row=sqlx::query("SELECT p.id::bigint,p.name,p.exp_date,a.name AS account_name,p.m3u_account_id::bigint AS account,a.account_type,CASE WHEN COALESCE(octet_length(a.server_url),0)<=65536 THEN a.server_url END AS server_url,CASE WHEN COALESCE(octet_length(a.username),0)<=65536 THEN a.username END AS username,CASE WHEN COALESCE(octet_length(a.password),0)<=65536 THEN a.password END AS password,CASE WHEN COALESCE(octet_length(u.user_agent),0)<=65536 THEN u.user_agent END AS user_agent,p.search_pattern,p.replace_pattern,CASE WHEN COALESCE(octet_length(p.custom_properties::text),0)<=1048576 THEN p.custom_properties END AS custom_properties,(COALESCE(octet_length(p.custom_properties::text),0)<=1048576 AND COALESCE(octet_length(a.server_url),0)<=65536 AND COALESCE(octet_length(a.username),0)<=65536 AND COALESCE(octet_length(a.password),0)<=65536 AND COALESCE(octet_length(u.user_agent),0)<=65536) AS fits FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id LEFT JOIN core_useragent u ON u.id=a.user_agent_id WHERE p.id=$1::bigint").bind(id).fetch_optional(&mut*c).await?;
            row.map(profile).transpose()
        })).await??;
        let Some(profile) = profile else {
            let error = format!("Profile {id} not found");
            emit(events,json!({"type":"account_refresh_error","profile_id":id,"error":"Profile not found","message":error}),owner).await?;
            return Ok(json!(error));
        };
        if profile.kind != "XC" {
            return Ok(json!(format!(
                "Profile {id} belongs to account {} which is not an XtreamCodes account.",
                profile.account
            )));
        }
        let Some(credentials) = provider::credentials(
            &profile.base,
            &profile.username,
            &profile.password,
            &profile.search,
            &profile.replace,
        )?
        else {
            return Ok(json!(format!(
                "Credential transform failed for profile {} ({id})",
                profile.name
            )));
        };
        let agent = profile
            .agent
            .as_deref()
            .filter(|v| !v.is_empty())
            .unwrap_or(&self.default_agent);
        match provider::fetch(credentials, agent).await? {
            Ok(info) => {
                self.save(&profile, info).await?;
                emit(events,json!({"type":"account_info_refresh_success","profile_id":id,"profile_name":profile.name,"message":format!("Account information successfully refreshed for profile '{}'",profile.name)}),owner).await?;
                Ok(json!(format!(
                    "Account info refresh completed for profile {id} ({}).",
                    profile.name
                )))
            }
            Err(error) => {
                emit(events,json!({"type":"account_refresh_error","profile_id":id,"error":error,"message":format!("Failed to refresh account info: {error}")}),owner).await?;
                Ok(json!(format!(
                    "Error refreshing account info for profile {id}: {error}"
                )))
            }
        }
    }
    pub async fn profiles(&self, id: i64, delay: Duration) -> Result<Value, StoreError> {
        let account=self.session.run(move|c|Box::pin(async move{sqlx::query_as::<_,(String,String)>("SELECT account_type,name FROM m3u_m3uaccount WHERE id=$1::bigint AND is_active").bind(id).fetch_optional(&mut*c).await})).await??;
        let Some((kind, name)) = account else {
            return Ok(json!(format!("Account {id} not found")));
        };
        if kind != "XC" {
            return Ok(json!(format!("Account {id} is not an XtreamCodes account")));
        }
        // HOLD freezes the same initial queryset/CP/credentials while bounded
        // FETCH pages avoid materializing all profiles. Saves use THIS session.
        self.session.run(move|c|Box::pin(async move{sqlx::query("DECLARE rust_xc_profiles NO SCROLL CURSOR WITH HOLD FOR SELECT p.id::bigint,p.name,p.exp_date,a.name AS account_name,p.m3u_account_id::bigint AS account,a.account_type,CASE WHEN COALESCE(octet_length(a.server_url),0)<=65536 THEN a.server_url END AS server_url,CASE WHEN COALESCE(octet_length(a.username),0)<=65536 THEN a.username END AS username,CASE WHEN COALESCE(octet_length(a.password),0)<=65536 THEN a.password END AS password,CASE WHEN COALESCE(octet_length(u.user_agent),0)<=65536 THEN u.user_agent END AS user_agent,p.search_pattern,p.replace_pattern,CASE WHEN COALESCE(octet_length(p.custom_properties::text),0)<=1048576 THEN p.custom_properties END AS custom_properties,(COALESCE(octet_length(p.custom_properties::text),0)<=1048576 AND COALESCE(octet_length(a.server_url),0)<=65536 AND COALESCE(octet_length(a.username),0)<=65536 AND COALESCE(octet_length(a.password),0)<=65536 AND COALESCE(octet_length(u.user_agent),0)<=65536) AS fits FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id LEFT JOIN core_useragent u ON u.id=a.user_agent_id WHERE p.m3u_account_id=$1::bigint AND p.is_active").bind(id).execute(&mut*c).await.map(|_|())})).await??;
        let (mut updated, mut failed, mut total) = (0usize, 0usize, 0usize);
        loop {
            let rows = self
                .session
                .run(|c| {
                    Box::pin(async move {
                        let rows = sqlx::query("FETCH FORWARD 32 FROM rust_xc_profiles")
                            .fetch_all(&mut *c)
                            .await?;
                        rows.into_iter()
                            .map(profile)
                            .collect::<Result<Vec<_>, StoreError>>()
                    })
                })
                .await??;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                if total > 0 {
                    tokio::time::sleep(delay).await
                }
                total += 1;
                let Some(credentials) = provider::credentials(
                    &row.base,
                    &row.username,
                    &row.password,
                    &row.search,
                    &row.replace,
                )?
                else {
                    failed += 1;
                    continue;
                };
                let agent = row
                    .agent
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .unwrap_or(&self.default_agent);
                match provider::fetch(credentials, agent).await? {
                    Ok(info) => {
                        self.save(&row, info).await?;
                        updated += 1
                    }
                    Err(_) => failed += 1,
                }
            }
        }
        self.session
            .run(|c| {
                Box::pin(async move {
                    sqlx::query("CLOSE rust_xc_profiles")
                        .execute(&mut *c)
                        .await
                        .map(|_| ())
                })
            })
            .await??;
        Ok(json!(if total == 0 {
            format!("No active profiles for account {id}")
        } else {
            format!(
                "Profile refresh complete for account {name}: {updated} updated, {failed} failed"
            )
        }))
    }
    async fn save(&self, row: &Profile, info: Value) -> Result<(), StoreError> {
        let mut properties = row.properties.clone();
        properties.extend(info.as_object().ok_or_else(bad)?.clone());
        let expiration =
            provider::expiration(&properties["user_info"]["exp_date"]).or(row.expiration);
        self.signal
            .guard(self.session.clone(), row.id, self.owner.clone())
            .await?;
        let id = row.id;
        let value = Value::Object(properties);
        if serde_json::to_vec(&value).map_err(|_| bad())?.len() > 1048576 {
            return Err(bad());
        }
        let rows=self.session.run(move|c|Box::pin(async move{sqlx::query("UPDATE m3u_m3uaccountprofile SET custom_properties=$2,exp_date=$3 WHERE id=$1::bigint").bind(id).bind(value).bind(expiration).execute(&mut*c).await.map(|v|v.rows_affected())})).await??;
        if rows != 1 {
            return Err(StoreError::Unsupported(
                "XC profile changed before settled save",
            ));
        }
        let saved = SavedProfile {
            id: row.id,
            account_id: row.account,
            name: row.name.clone(),
            account_name: row.account_name.clone(),
            expiration,
        };
        self.signal
            .after_save(self.session.clone(), saved, self.owner.clone())
            .await?;
        Ok(())
    }
}
fn profile(row: sqlx::postgres::PgRow) -> Result<Profile, StoreError> {
    if !row.try_get::<bool, _>("fits")? {
        return Err(bad());
    }
    let properties: Value = row
        .try_get::<Option<Value>, _>("custom_properties")?
        .unwrap_or(Value::Null);
    let properties = if let Some(v) = properties.as_object() {
        v.clone()
    } else if let Some(s) = properties.as_str() {
        serde_json::from_str::<Value>(s)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default()
    } else {
        Map::new()
    };
    let expiration: Option<DateTime<Utc>> = row.try_get("exp_date")?;
    if expiration.is_some_and(|value| !(1..=9999).contains(&value.year())) {
        return Err(bad());
    }
    Ok(Profile {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        account: row.try_get("account")?,
        account_name: row.try_get("account_name")?,
        expiration,
        kind: row.try_get("account_type")?,
        base: row
            .try_get::<Option<String>, _>("server_url")?
            .unwrap_or_default(),
        username: row
            .try_get::<Option<String>, _>("username")?
            .unwrap_or_default(),
        password: row
            .try_get::<Option<String>, _>("password")?
            .unwrap_or_default(),
        agent: row.try_get("user_agent")?,
        search: row.try_get("search_pattern")?,
        replace: row.try_get("replace_pattern")?,
        properties,
    })
}
async fn emit(
    events: Arc<dyn UpdateEventSink>,
    data: Value,
    owner: Owner,
) -> Result<(), StoreError> {
    crate::core_task_offsets::retained_event(events, json!({"type":"update","data":data}), owner)
        .await
}

/// Source Python equality, including bool/int and integral float matches.
/// Used by the deletion transaction; no API token/cache invalidation is added.
pub fn scrub_deleted_profile_value(properties: &Value, id: i64) -> Option<Value> {
    let mut updated = properties.as_object()?.clone();
    let old = updated.get("allowed_m3u_profile_ids")?.as_array()?;
    let kept = old
        .iter()
        .filter(|item| !crate::json_compat::python_equal(item, &json!(id)))
        .cloned()
        .collect::<Vec<_>>();
    if kept.len() == old.len() {
        return None;
    }
    updated.insert("allowed_m3u_profile_ids".into(), Value::Array(kept));
    Some(Value::Object(updated)) // preserve [] and every unrelated field/type.
}

#[cfg(test)]
mod deleted_profile_allowlist_tests {
    use super::*;
    #[test]
    fn source_python_numeric_equality_preserves_unrelated_restrictions() {
        let before =
            json!({"allowed_m3u_profile_ids":[6,6.0,"6",true,7,{},[6]],"untouched":{"n":1}});
        assert_eq!(
            scrub_deleted_profile_value(&before, 6),
            Some(json!({
            "allowed_m3u_profile_ids":["6",true,7,{},[6]],"untouched":{"n":1}}))
        );
        assert_eq!(
            scrub_deleted_profile_value(
                &json!({"allowed_m3u_profile_ids":[1,1.0,true,"1",false]}),
                1
            ),
            Some(json!({"allowed_m3u_profile_ids":["1",false]}))
        );
    }
    #[test]
    fn source_observed_last_restriction_keeps_empty_key_and_nonlists_unchanged() {
        assert_eq!(
            scrub_deleted_profile_value(&json!({"allowed_m3u_profile_ids":[7]}), 7),
            Some(json!({"allowed_m3u_profile_ids":[]}))
        );
        for value in [
            json!(null),
            json!([]),
            json!({}),
            json!({"allowed_m3u_profile_ids":null}),
            json!({"allowed_m3u_profile_ids":"7"}),
            json!({"allowed_m3u_profile_ids":["7"]}),
        ] {
            assert_eq!(scrub_deleted_profile_value(&value, 7), None);
        }
    }
}
