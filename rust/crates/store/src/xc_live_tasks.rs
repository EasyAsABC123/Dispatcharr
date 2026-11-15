//! Normal XC live refresh; algorithms belong to Original Dispatcharr authors.
//! Uses one actual Main pool/writer lease, never a second client or SQL pool.
use crate::{
    import_batches::{ImportedStream, ImportedXcStream},
    import_groups::ImportedGroup,
    task_sql_session::TaskSqlSession,
    xc_live_provider::{scalar, truthy, Client},
    AuthService, GroupCleanupAccount, Store, StoreError,
};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_ingest_core::{project_entry, Entry, ProjectionContext};
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::{collections::BTreeMap, sync::Arc};
type Owner = Arc<dyn Send + Sync>;
fn unsupported() -> StoreError {
    StoreError::Unsupported("XC live refresh stage/value domain is not composed")
}
// No Debug implementation: this snapshot includes actual provider credentials.
struct Account {
    id: i64,
    name: String,
    active: bool,
    kind: String,
    status: String,
    message: Option<String>,
    base: String,
    user: String,
    password: String,
    agent: String,
    properties: Map<String, Value>,
    locked: bool,
    days: i64,
}
pub struct Prepared {
    store: Store,
    session: TaskSqlSession,
    account: Account,
    events: Arc<dyn UpdateEventSink>,
    owner: Owner,
    hash_keys: Vec<String>,
    max_events: i64,
    filters: crate::xc_live_filters::Filters,
}
pub struct Completion {
    pub result: Value,
    pub invalidate: bool,
}
/// Returns None only for an observed non-XC row. A caller can then delegate to
/// the existing STD service, without claiming/changing the broker head.
pub async fn prepare(
    store: Store,
    auth: AuthService,
    id: i64,
    owner: Owner,
    events: Arc<dyn UpdateEventSink>,
) -> Result<Option<Prepared>, StoreError> {
    prepare_for(store, auth, id, owner, events, "XC").await
}
/// Actual remote STD accounts use the same owned preparation/writer/session;
/// missing/nonremote types delegate without claiming the broker head.
pub async fn prepare_std_remote(
    store: Store,
    auth: AuthService,
    id: i64,
    owner: Owner,
    events: Arc<dyn UpdateEventSink>,
) -> Result<Option<Prepared>, StoreError> {
    prepare_for(store, auth, id, owner, events, "STD").await
}
async fn prepare_for(
    store: Store,
    auth: AuthService,
    id: i64,
    owner: Owner,
    events: Arc<dyn UpdateEventSink>,
    expected_type: &'static str,
) -> Result<Option<Prepared>, StoreError> {
    if !auth.uses_store_pool(&store) {
        return Err(unsupported());
    }
    let writer = auth.task_sql_writer()?;
    let owner: Owner = Arc::new((owner, writer.clone()));
    let auth = auth
        .with_request_writer_permit(writer)?
        .with_retained_scope(owner.clone());
    let default_agent = crate::core_settings_special_effects::default_user_agent(&auth).await?;
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    let read=async {
        let row=session.run(move|c|Box::pin(async move{sqlx::query("SELECT a.id::bigint,a.name,a.account_type,a.status,CASE WHEN COALESCE(octet_length(a.last_message),0)<=65536 THEN a.last_message END AS last_message,a.is_active,a.locked,a.stale_stream_days::bigint,CASE WHEN COALESCE(octet_length(a.server_url),0)<=65536 THEN a.server_url END AS server_url,CASE WHEN COALESCE(octet_length(a.username),0)<=65536 THEN a.username END AS username,CASE WHEN COALESCE(octet_length(a.password),0)<=65536 THEN a.password END AS password,CASE WHEN COALESCE(octet_length(u.user_agent),0)<=65536 THEN u.user_agent END AS user_agent,CASE WHEN COALESCE(octet_length(a.custom_properties::text),0)<=65536 THEN a.custom_properties END AS custom_properties,(COALESCE(octet_length(a.last_message),0)<=65536 AND COALESCE(octet_length(a.server_url),0)<=65536 AND COALESCE(octet_length(a.username),0)<=65536 AND COALESCE(octet_length(a.password),0)<=65536 AND COALESCE(octet_length(a.custom_properties::text),0)<=65536 AND COALESCE(octet_length(u.user_agent),0)<=65536) AS fits FROM m3u_m3uaccount a LEFT JOIN core_useragent u ON u.id=a.user_agent_id WHERE a.id=$1").bind(id).fetch_optional(&mut*c).await})).await??;
        let Some(row)=row else{return Err(StoreError::NotFound)};
        if row.try_get::<String,_>("account_type")?!=expected_type{return Ok(None)}
        if expected_type=="STD"&&row.try_get::<Option<String>,_>("server_url")?.is_none_or(|v|v.is_empty()){return Ok(None)}
        if !row.try_get::<bool,_>("is_active")?{return Err(unsupported())}
        if !row.try_get::<bool,_>("fits")? {return Err(unsupported())}
        let properties=properties(row.try_get::<Option<Value>,_>("custom_properties")?.unwrap_or(Value::Null))?;
        if expected_type=="XC"&&properties.get("enable_vod").is_some_and(truthy){return Err(unsupported())}
        let forbidden=session.run(move|c|Box::pin(async move{sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=$1 AND auto_channel_sync) OR EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event IN('m3u_refresh','m3u_error')) OR EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled) OR EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN('m3u_m3uaccount'::regclass,'dispatcharr_channels_channelgroup'::regclass,'dispatcharr_channels_channelgroupm3uaccount'::regclass,'core_systemevent'::regclass))").bind(id).fetch_one(&mut*c).await})).await??;
        if forbidden{return Err(unsupported())}
        let account=Account{id,kind:row.try_get("account_type")?,name:row.try_get("name")?,status:row.try_get("status")?,message:row.try_get("last_message")?,active:row.try_get("is_active")?,base:row.try_get::<Option<String>,_>("server_url")?.unwrap_or_default(),user:row.try_get::<Option<String>,_>("username")?.unwrap_or_default(),password:row.try_get::<Option<String>,_>("password")?.unwrap_or_default(),agent:row.try_get::<Option<String>,_>("user_agent")?.filter(|v|!v.is_empty()).unwrap_or(default_agent),properties,locked:row.try_get("locked")?,days:row.try_get("stale_stream_days")?};
        let settings=session.system_settings(auth.clone()).await?;
        let max_events=match settings.get("max_system_events"){None|Some(Value::Null)=>100,Some(v)if v==&json!(0)=>100,Some(v)=>v.as_i64().filter(|v|(1..=10000).contains(v)).ok_or_else(unsupported)?};
        let stream=stream_settings(&auth,&session).await?;
        let hash_keys=hash_keys(account.properties.get("hash_key").unwrap_or(&Value::Null),stream.get("m3u_hash_key").unwrap_or(&Value::Null))?;
        let filters=crate::xc_live_filters::Filters::read(&session,id).await?;
        Ok(Some((account,max_events,hash_keys,filters)))
    }.await;
    match read {
        Ok(Some((account, max_events, hash_keys, filters))) => Ok(Some(Prepared {
            store,
            session,
            account,
            events,
            owner,
            hash_keys,
            max_events,
            filters,
        })),
        other => {
            session.close().await?;
            other.map(|_| None)
        }
    }
}
fn properties(raw: Value) -> Result<Map<String, Value>, StoreError> {
    match raw {
        Value::Object(v) => Ok(v),
        Value::Null => Ok(Map::new()),
        Value::String(v) => match serde_json::from_str::<Value>(&v) {
            Ok(Value::Object(v)) => Ok(v),
            _ => Ok(Map::new()),
        },
        _ => Ok(Map::new()),
    }
}
async fn stream_settings(
    auth: &AuthService,
    session: &TaskSqlSession,
) -> Result<Value, StoreError> {
    let session = session.clone();
    let load = move || async move {
        session.run(|c|Box::pin(async move{let row=sqlx::query("SELECT octet_length(value::text)<=65536 AS fits,CASE WHEN octet_length(value::text)<=65536 THEN value END AS value FROM core_coresettings WHERE key='stream_settings'").fetch_optional(&mut*c).await?;match row{None=>Ok(json!({"m3u_hash_key":""})),Some(row)=>{if !row.try_get::<bool,_>("fits")?{return Err(unsupported())}let v:Value=row.try_get("value")?;if v.is_object(){Ok(v)}else{Ok(json!({"m3u_hash_key":""}))}}}})).await?
    };
    match &auth.notification_cache {
        Some(cache) => cache.settings_group("stream_settings", load).await,
        None => load().await,
    }
}
fn hash_keys(account: &Value, global: &Value) -> Result<Vec<String>, StoreError> {
    let parse = |v: &Value| -> Result<Vec<String>, StoreError> {
        let text = match v {
            Value::Null => "",
            Value::String(v) => v,
            _ => return Err(unsupported()),
        };
        let mut result = Vec::new();
        for key in text.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            if !["name", "url", "tvg_id", "m3u_id", "group"].contains(&key) {
                continue;
            }
            if !result.iter().any(|v| v == key) {
                result.push(key.to_owned())
            }
        }
        Ok(result)
    };
    let local = parse(account)?;
    if local.is_empty() {
        parse(global)
    } else {
        Ok(local)
    }
}
impl Prepared {
    pub async fn close(&self) -> Result<(), StoreError> {
        self.session.close().await
    }
    pub fn publishes_profiles(&self) -> bool {
        self.account.active
            && !self.account.base.is_empty()
            && !self.account.user.is_empty()
            && !self.account.password.is_empty()
    }
    pub fn active(&self) -> bool {
        self.account.active
    }
    pub async fn groups(&self, full: bool, start: DateTime<Utc>) -> Result<Completion, StoreError> {
        if !self.account.active {
            return Ok(Completion {
                invalidate: false,
                result: json!([
                    format!(
                        "M3UAccount with ID={} not found or inactive.",
                        self.account.id
                    ),
                    null
                ]),
            });
        }
        if self.account.base.is_empty() {
            return self
                .group_error("Missing server URL for Xtream Codes account")
                .await;
        }
        if self.account.user.is_empty() || self.account.password.is_empty() {
            return self
                .group_error("Missing username or password for Xtream Codes account")
                .await;
        }
        let Some(mut client) = Client::new(
            &self.account.base,
            &self.account.user,
            &self.account.password,
            self.account.agent.clone(),
        )?
        else {
            return Err(unsupported());
        };
        let value = match client.action("get_live_categories").await? {
            Ok(v) => v,
            Err(error) => {
                let auth = [
                    "auth",
                    "credential",
                    "login",
                    "unauthorized",
                    "forbidden",
                    "401",
                    "403",
                    "512",
                    "513",
                ]
                .iter()
                .any(|keyword| error.to_lowercase().contains(keyword));
                let message = if auth {
                    format!("Failed to authenticate with XC server: {error}")
                } else {
                    format!("Failed to get categories from XC server: {error}")
                };
                return self.group_error(&message).await;
            }
        };
        let groups = categories(&value)?;
        self.event(
            "processing_groups",
            0,
            if full {
                "fetching"
            } else {
                &self.account.status
            },
            if full {
                Some("Refresh in progress...")
            } else {
                self.account.message.as_deref()
            },
            json!({}),
        )
        .await?;
        let store = self.store.clone();
        let id = self.account.id;
        let props = Value::Object(self.account.properties.clone());
        let incoming = groups
            .iter()
            .map(|(name, cp)| ImportedGroup {
                name: name.clone(),
                custom_properties: cp.clone(),
            })
            .collect();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    store
                        .import_m3u_groups_report_on_connection(c, id, &props, incoming, start)
                        .await
                })
            })
            .await?
            .map_err(|e| e.source)?;
        if !full {
            let text = "M3U groups loaded. Please select groups or refresh M3U to complete setup.";
            self.status("pending_setup", Some(text), false).await?;
            self.event(
                "processing_groups",
                100,
                "pending_setup",
                Some(text),
                json!({}),
            )
            .await?;
        }
        let result: Map<String, Value> = groups
            .into_iter()
            .map(|(name, props)| (name, Value::Object(props)))
            .collect();
        Ok(Completion {
            invalidate: false,
            result: json!([[], result]),
        })
    }
    pub async fn begin_single(&self) -> Result<(), StoreError> {
        self.status("fetching", Some("Refresh in progress..."), false)
            .await
    }
    pub async fn single(
        &self,
        start: DateTime<Utc>,
        elapsed: std::time::Instant,
    ) -> Result<Completion, StoreError> {
        if !self.account.active {
            return Err(unsupported());
        }

        self.status("parsing", Some("Refresh in progress..."), false)
            .await?;
        let id = self.account.id;
        let enabled=self.session.run(move|c|Box::pin(async move{let rows=sqlx::query("SELECT g.id::bigint,g.name,CASE WHEN COALESCE(octet_length(r.custom_properties::text),0)<=65536 THEN r.custom_properties END AS custom_properties,COALESCE(octet_length(r.custom_properties::text),0)<=65536 AS fits FROM dispatcharr_channels_channelgroup g JOIN dispatcharr_channels_channelgroupm3uaccount r ON r.channel_group_id=g.id WHERE r.m3u_account_id=$1 AND r.enabled LIMIT 32769").bind(id).fetch_all(&mut*c).await?;if rows.len()>32768{return Err(unsupported())}let mut out=BTreeMap::new();for row in rows{if !row.try_get::<bool,_>("fits")?{return Err(unsupported())}let cp=properties(row.try_get::<Value,_>("custom_properties")?)?;if let Some(xc)=cp.get("xc_id"){if out.insert(scalar(xc)?,(row.try_get::<String,_>("name")?,row.try_get::<i64,_>("id")?)).is_some(){return Err(unsupported())}}}Ok::<_,StoreError>(out)})).await??;
        // Source creates a NEW XCClient when collecting streams; do not reuse
        // the categories authentication/session or request per category.
        let Some(mut client) = Client::new(
            &self.account.base,
            &self.account.user,
            &self.account.password,
            self.account.agent.clone(),
        )?
        else {
            return Err(unsupported());
        };
        let value = match client.action("get_live_streams").await? {
            Ok(v) => v,
            Err(_) => json!([]),
        };
        let streams = value.as_array().ok_or_else(unsupported)?;
        let groups: BTreeMap<String, i64> = enabled
            .values()
            .map(|(name, id)| (name.clone(), *id))
            .collect();
        let keys = self
            .hash_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let context = ProjectionContext {
            account_id: id,
            account_type: "XC",
            groups: &groups,
            hash_keys: &keys,
            max_name_characters: 512,
        };
        let prefix = client.live_prefix();
        let mut batch = Vec::with_capacity(1000);
        let mut total = crate::ImportCounts::default();
        let mut eligible = 0usize;
        let mut failed = 0usize;
        for value in streams {
            let category = scalar(
                value
                    .as_object()
                    .ok_or_else(unsupported)?
                    .get("category_id")
                    .unwrap_or(&json!("")),
            )?;
            if !enabled.contains_key(&category) {
                continue;
            }
            eligible += 1;
            if let Some(row) = project(value, &self.account.name, &prefix, &enabled, &context)? {
                let group = row.stream.custom_properties["group-title"]
                    .as_str()
                    .ok_or_else(unsupported)?;
                if self
                    .filters
                    .allows(&row.stream.name, &row.stream.url, group)?
                {
                    batch.push(row)
                }
            }
            if eligible.is_multiple_of(1000)
                && !self.batch(std::mem::take(&mut batch), &mut total).await?
            {
                failed += 1
            }
        }
        if !eligible.is_multiple_of(1000) && !self.batch(batch, &mut total).await? {
            failed += 1
        }
        if failed > 0 {
            crate::xc_live_rollup::account(&self.session, id).await?;
            self.error_status(
                &format!("Refresh incomplete: {failed} batch(es) failed to process."),
                "parsing",
            )
            .await?;
            return Ok(Completion {
                invalidate: true,
                result: json!(
                    "Failed to update m3u account, one or more batches failed to process"
                ),
            });
        }
        if eligible == 0 {
            self.error_status("No streams returned from Xtream Codes provider", "parsing")
                .await?;
            return Ok(Completion {
                invalidate: false,
                result: json!("Failed to update m3u account, no streams returned from provider"),
            });
        }
        self.finish_import(total, start, elapsed).await
    }
    pub async fn finish_import(
        &self,
        total: crate::ImportCounts,
        start: DateTime<Utc>,
        elapsed: std::time::Instant,
    ) -> Result<Completion, StoreError> {
        let id = self.account.id;
        let selected = total.created + total.updated + total.unchanged;
        let progress = elapsed.elapsed().as_secs_f64();
        if self.account.kind == "STD" {
            self.event_current("parsing",100.0,json!({"elapsed_time":format!("{progress:.2}").parse::<f64>().map_err(|_|unsupported())?,"time_remaining":0,"streams_processed":selected})).await?;
        } else {
            self.event("parsing",100,"parsing",Some("Refresh in progress..."),json!({"elapsed_time":format!("{progress:.2}").parse::<f64>().map_err(|_|unsupported())?,"time_remaining":0,"streams_processed":selected})).await?;
        }
        let stale=self.session.run(move|c|Box::pin(async move{let n=sqlx::query("UPDATE dispatcharr_channels_stream SET is_stale=true WHERE m3u_account_id=$1 AND last_seen<$2").bind(id).bind(start).execute(&mut*c).await?.rows_affected();sqlx::query("UPDATE dispatcharr_channels_channelgroupm3uaccount SET is_stale=true WHERE m3u_account_id=$1 AND last_seen<$2").bind(id).bind(start).execute(&mut*c).await?;Ok::<_,StoreError>(n)})).await??;
        let store = self.store.clone();
        let deleted = self
            .session
            .run(move |c| {
                Box::pin(async move { store.cleanup_m3u_streams_on_connection(c, id, start).await })
            })
            .await??
            .reported_total;
        let store = self.store.clone();
        let account = GroupCleanupAccount {
            id,
            locked: self.account.locked,
            name: self.account.name.clone(),
            stale_stream_days: self.account.days,
        };
        self.session
            .run(move |c| {
                Box::pin(async move {
                    store
                        .cleanup_m3u_group_relationships_on_connection(c, &account, start)
                        .await
                })
            })
            .await??;
        // Existing mapped channels use Source's account-scoped aggregation on
        // this same owned session. Source catches only actual settled failures.
        crate::xc_live_rollup::account(&self.session, id).await?;
        let duration = elapsed.elapsed().as_secs_f64();
        let text=format!("Processing completed in {duration:.1} seconds. Streams: {} created, {} updated, {stale} marked stale, {deleted} removed. Total processed: {selected}.",total.created,total.updated);
        self.status("success", Some(&text), true).await?;
        self.audit("m3u_refresh",json!({"account_name":self.account.name,"elapsed_time":format!("{duration:.2}").parse::<f64>().map_err(|_|unsupported())?,"streams_created":total.created,"streams_updated":total.updated,"streams_stale":stale,"streams_deleted":deleted,"total_processed":selected})).await?;
        self.event("parsing",100,"success",Some(&text),json!({"elapsed_time":duration,"time_remaining":0,"streams_processed":selected,"streams_created":total.created,"streams_updated":total.updated,"streams_stale":stale,"streams_deleted":deleted,"channels_created":0,"channels_updated":0,"channels_deleted":0,"channels_failed":0,"failed_stream_details":[]})).await?;
        Ok(Completion {
            invalidate: true,
            result: json!("Dispatched jobs complete."),
        })
    }
    pub async fn finish_std_failure(&self, failed: usize) -> Result<Completion, StoreError> {
        self.error_status(
            &format!("Refresh incomplete: {failed} batch(es) failed to process."),
            "parsing",
        )
        .await?;
        crate::xc_live_rollup::account(&self.session, self.account.id).await?;
        Ok(Completion {
            invalidate: true,
            result: json!("Failed to update m3u account, one or more batches failed to process"),
        })
    }
    pub async fn empty_std(&self) -> Result<Completion, StoreError> {
        self.error_status("No streams found in M3U source", "parsing")
            .await?;
        Ok(Completion {
            invalidate: false,
            result: json!("Failed to update m3u account, no streams found"),
        })
    }
    pub async fn import_std_groups(
        &self,
        names: Vec<String>,
        start: DateTime<Utc>,
        full: bool,
    ) -> Result<(), StoreError> {
        self.event_current("processing_groups", 0.0, json!({}))
            .await?;
        let incoming = names
            .into_iter()
            .map(|name| ImportedGroup {
                name,
                custom_properties: Map::new(),
            })
            .collect();
        let store = self.store.clone();
        let id = self.account.id;
        let props = Value::Object(self.account.properties.clone());
        self.session
            .run(move |c| {
                Box::pin(async move {
                    store
                        .import_m3u_groups_report_on_connection(c, id, &props, incoming, start)
                        .await
                })
            })
            .await?
            .map_err(|e| e.source)?;
        if !full {
            let message =
                "M3U groups loaded. Please select groups or refresh M3U to complete setup.";
            self.status("pending_setup", Some(message), false).await?;
            self.event_current("processing_groups", 100.0, json!({}))
                .await?;
        }
        Ok(())
    }
    pub async fn std_groups(&self) -> Result<BTreeMap<String, i64>, StoreError> {
        let id = self.account.id;
        self.session.run(move|c|Box::pin(async move{let rows=sqlx::query("SELECT g.id::bigint,g.name FROM dispatcharr_channels_channelgroup g JOIN dispatcharr_channels_channelgroupm3uaccount r ON r.channel_group_id=g.id WHERE r.m3u_account_id=$1 AND r.enabled LIMIT 32769").bind(id).fetch_all(&mut*c).await?;if rows.len()>32768{return Err(unsupported())}rows.into_iter().map(|row|Ok((row.try_get("name")?,row.try_get("id")?))).collect::<Result<BTreeMap<String,i64>,StoreError>>()})).await?
    }
    pub async fn std_batch(
        &self,
        entries: Vec<Entry>,
        groups: &BTreeMap<String, i64>,
        total: &mut crate::ImportCounts,
    ) -> Result<bool, StoreError> {
        let keys = self
            .hash_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let context = ProjectionContext {
            account_id: self.account.id,
            account_type: "STD",
            groups,
            hash_keys: &keys,
            max_name_characters: 512,
        };
        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(projected) = project_entry(&entry, &context).map_err(|_| unsupported())? {
                let group = projected.custom_properties["group-title"]
                    .as_str()
                    .unwrap_or("Default Group");
                if !self
                    .filters
                    .allows(&projected.name, &projected.url, group)?
                {
                    continue;
                }
                let stream = ImportedStream::from(projected);
                rows.push(ImportedXcStream {
                    logo_url: Some(stream.logo_url.clone()),
                    tvg_id: Some(stream.tvg_id.clone()),
                    stream,
                });
            }
        }
        self.batch(rows, total).await
    }
    pub async fn parsing_only(&self) -> Result<(), StoreError> {
        let id = self.account.id;
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query("UPDATE m3u_m3uaccount SET status='parsing' WHERE id=$1")
                        .bind(id)
                        .execute(&mut *c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
    pub async fn caught_remote_error(&self, message: &str, action: &str) -> Result<(), StoreError> {
        self.error_status(message, action).await
    }
    pub fn remote_input(
        &self,
        directory: std::path::PathBuf,
        use_cache: bool,
    ) -> crate::remote_m3u_io::Input {
        crate::remote_m3u_io::Input {
            account: self.account.id,
            url: self.account.base.clone(),
            agent: self.account.agent.clone(),
            directory,
            use_cache,
        }
    }
    async fn event_current(
        &self,
        action: &str,
        progress: f64,
        extra: Value,
    ) -> Result<(), StoreError> {
        let id = self.account.id;
        let row=self.session.run(move|c|Box::pin(async move{sqlx::query("SELECT status,CASE WHEN COALESCE(octet_length(last_message),0)<=65536 THEN last_message END AS last_message,COALESCE(octet_length(last_message),0)<=65536 AS fits FROM m3u_m3uaccount WHERE id=$1").bind(id).fetch_one(&mut*c).await})).await??;
        if !row.try_get::<bool, _>("fits")? {
            return Err(unsupported());
        }
        let mut data = json!({"progress":progress,"type":"m3u_refresh","account":id,"action":action,"status":row.try_get::<String,_>("status")?});
        if let Some(message) = row
            .try_get::<Option<String>, _>("last_message")?
            .filter(|m| !m.is_empty())
        {
            data["message"] = json!(message)
        }
        for (k, v) in extra.as_object().ok_or_else(unsupported)? {
            data[k] = v.clone();
        }
        self.events
            .send_retained(
                "updates",
                json!({"type":"update","data":data}),
                self.owner.clone(),
            )
            .await
            .map_err(|_| unsupported())
    }
    async fn error_status(&self, message: &str, action: &str) -> Result<(), StoreError> {
        self.status("error", Some(message), false).await?;
        self.event(
            action,
            100,
            "error",
            Some(message),
            json!({"error":message}),
        )
        .await?;
        self.audit(
            "m3u_error",
            json!({"account_name":self.account.name,"message":message}),
        )
        .await
    }
    async fn group_error(&self, message: &str) -> Result<Completion, StoreError> {
        self.error_status(message, "processing_groups").await?;
        Ok(Completion {
            invalidate: false,
            result: json!([message, null]),
        })
    }
    async fn batch(
        &self,
        rows: Vec<ImportedXcStream>,
        counts: &mut crate::ImportCounts,
    ) -> Result<bool, StoreError> {
        for attempt in 0..2 {
            let store = self.store.clone();
            let id = self.account.id;
            let rows = rows.clone();
            let result = self
                .session
                .run(move |c| {
                    Box::pin(async move { store.import_xc_batch_on_connection(c, id, rows).await })
                })
                .await?;
            match result {
                Ok(n) => {
                    counts.created += n.created;
                    counts.updated += n.updated;
                    counts.unchanged += n.unchanged;
                    return Ok(true);
                }
                Err(crate::BoundedM3uBatchError::MutationRolledBack {
                    source: sqlx::Error::Database(_),
                    ..
                }) => {
                    if attempt == 1 {
                        return Ok(false);
                    }
                }
                Err(_) => return Err(unsupported()),
            }
        }
        Err(unsupported())
    }
    async fn status(
        &self,
        status: &str,
        message: Option<&str>,
        timestamp: bool,
    ) -> Result<(), StoreError> {
        let id = self.account.id;
        let status = status.to_owned();
        let message = message.map(str::to_owned);
        self.session.run(move|c|Box::pin(async move{if timestamp{sqlx::query("UPDATE m3u_m3uaccount SET status=$2,last_message=$3,updated_at=$4 WHERE id=$1").bind(id).bind(status).bind(message).bind(Utc::now()).execute(&mut*c).await?;}else{sqlx::query("UPDATE m3u_m3uaccount SET status=$2,last_message=$3 WHERE id=$1").bind(id).bind(status).bind(message).execute(&mut*c).await?;}Ok::<_,StoreError>(())})).await?
    }
    async fn event(
        &self,
        action: &str,
        progress: i64,
        status: &str,
        message: Option<&str>,
        extra: Value,
    ) -> Result<(), StoreError> {
        let mut data = json!({"progress":progress,"type":"m3u_refresh","account":self.account.id,"action":action,"status":status});
        if let Some(message) = message.filter(|v| !v.is_empty()) {
            data["message"] = json!(message)
        }
        for (k, v) in extra.as_object().ok_or_else(unsupported)? {
            data[k] = v.clone()
        }
        self.events
            .send_retained(
                "updates",
                json!({"type":"update","data":data}),
                self.owner.clone(),
            )
            .await
            .map_err(|_| unsupported())
    }
    async fn audit(&self, kind: &str, details: Value) -> Result<(), StoreError> {
        let kind = kind.to_owned();
        let max = self.max_events;
        self.session.run(move|c|Box::pin(async move{sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,$3,NULL,NULL,$2)").bind(kind).bind(details).bind(Utc::now()).execute(&mut*c).await?;let rows:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(max).fetch_all(&mut*c).await?;if let Some(cutoff)=rows.first(){sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(&mut*c).await?;}Ok::<_,StoreError>(())})).await?
    }
}
/// Preserve category insertion order, duplicate-name replacement and Python
/// scalar IDs. No fabricated category/name if the provider supplies null.
type Categories = Vec<(String, Map<String, Value>)>;
fn categories(value: &Value) -> Result<Categories, StoreError> {
    let rows = value.as_array().ok_or_else(unsupported)?;
    if rows.len() > 32767 {
        return Err(unsupported());
    }
    let mut result = vec![("Default Group".to_owned(), Map::new())];
    for row in rows {
        let row = row.as_object().ok_or_else(unsupported)?;
        let name = match row.get("category_name") {
            None => "Unknown Category",
            Some(Value::String(v)) => v,
            _ => return Err(unsupported()),
        }
        .to_owned();
        let id = row.get("category_id").cloned().unwrap_or(json!("0"));
        let props = Map::from_iter([("xc_id".to_owned(), id)]);
        if let Some(old) = result.iter_mut().find(|(key, _)| key == &name) {
            old.1 = props;
        } else {
            result.push((name, props));
        }
    }
    Ok(result)
}
fn project(
    value: &Value,
    account: &str,
    prefix: &str,
    enabled: &BTreeMap<String, (String, i64)>,
    context: &ProjectionContext<'_>,
) -> Result<Option<ImportedXcStream>, StoreError> {
    let row = value.as_object().ok_or_else(unsupported)?;
    let category = scalar(row.get("category_id").unwrap_or(&json!("")))?;
    let Some((group, _)) = enabled.get(&category) else {
        return Ok(None);
    };
    let name = match row.get("name").filter(|v| truthy(v)) {
        Some(Value::String(v)) => v.clone(),
        None => format!(
            "{account} - {}",
            scalar(row.get("stream_id").unwrap_or(&json!("Unknown")))?
        ),
        _ => return Err(unsupported()),
    };
    let mut attrs = Map::new();
    attrs.insert(
        "tvg-id".into(),
        row.get("epg_channel_id").cloned().unwrap_or(json!("")),
    );
    attrs.insert(
        "tvg-logo".into(),
        row.get("stream_icon").cloned().unwrap_or(json!("")),
    );
    attrs.insert("group-title".into(), json!(group));
    attrs.insert(
        "stream_id".into(),
        json!(scalar(row.get("stream_id").unwrap_or(&json!("")))?),
    );
    attrs.insert("num".into(), row.get("num").cloned().unwrap_or(Value::Null));
    attrs.insert("category_id".into(), json!(category));
    for key in ["stream_type", "added", "custom_sid"] {
        attrs.insert(key.into(), row.get(key).cloned().unwrap_or(json!("")));
    }
    attrs.insert(
        "is_adult".into(),
        json!(scalar(row.get("is_adult").unwrap_or(&json!("0")))?),
    );
    for (key, value) in row {
        if ![
            "name",
            "stream_id",
            "stream_icon",
            "epg_channel_id",
            "category_id",
            "num",
            "stream_type",
            "added",
            "custom_sid",
            "is_adult",
        ]
        .contains(&key.as_str())
            && !value.is_null()
        {
            attrs.insert(key.clone(), json!(scalar(value)?));
        }
    }
    let logo = match &attrs["tvg-logo"] {
        Value::Null => None,
        Value::String(v) => Some(v.clone()),
        _ => return Err(unsupported()),
    };
    let tvg = match &attrs["tvg-id"] {
        Value::Null => None,
        Value::String(v) => Some(v.clone()),
        _ => return Err(unsupported()),
    };
    let mut text = BTreeMap::new();
    for (key, value) in &attrs {
        let v = if value.is_null() {
            String::new()
        } else {
            scalar(value)?
        };
        text.insert(key.clone(), v);
    }
    if let Some(Value::Bool(v)) = attrs.get("num") {
        text.insert("num".into(), if *v { "1" } else { "0" }.into());
    }
    let entry = Entry {
        display_name: name.clone(),
        vlc_opts: None,
        name,
        attributes: text,
        url: Some(format!(
            "{prefix}{}.ts",
            scalar(row.get("stream_id").unwrap_or(&Value::Null))?
        )),
    };
    let Some(projected) = project_entry(&entry, context).map_err(|_| unsupported())? else {
        return Ok(None);
    };
    let mut stream = ImportedStream::from(projected);
    stream.stream_hash = dispatcharr_ingest_core::stream_hash_nullable(
        &dispatcharr_ingest_core::NullableStreamIdentity {
            name: Some(&stream.name),
            url: Some(&stream.url),
            tvg_id: tvg.as_deref(),
            m3u_id: Some(context.account_id),
            group: Some(group),
            account_type: "XC",
            stream_id: stream.stream_id,
        },
        context.hash_keys,
    );
    stream.custom_properties = Value::Object(attrs);
    Ok(Some(ImportedXcStream {
        stream,
        logo_url: logo,
        tvg_id: tvg,
    }))
}

impl crate::remote_m3u_io::Observer for Prepared {
    fn starting(
        &self,
        _owner: Owner,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), crate::remote_m3u_io::Error>> {
        Box::pin(async move {
            self.status("fetching", Some("Starting download..."), false)
                .await
                .map_err(|_| crate::remote_m3u_io::Error::Unsettled)
        })
    }
    fn began(
        &self,
        _owner: Owner,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), crate::remote_m3u_io::Error>> {
        Box::pin(async move {
            self.event_current("downloading", 0.0, json!({}))
                .await
                .map_err(|_| crate::remote_m3u_io::Error::Unsettled)
        })
    }
    fn progress(
        &self,
        value: crate::remote_m3u_io::Progress,
        _owner: Owner,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), crate::remote_m3u_io::Error>> {
        Box::pin(async move {
            let id = self.account.id;
            let message = value.message.clone();
            self.session
                .run(move |c| {
                    Box::pin(async move {
                        sqlx::query("UPDATE m3u_m3uaccount SET last_message=$2 WHERE id=$1")
                            .bind(id)
                            .bind(message)
                            .execute(&mut *c)
                            .await?;
                        Ok::<_, StoreError>(())
                    })
                })
                .await
                .map_err(|_| crate::remote_m3u_io::Error::Unsettled)?
                .map_err(|_| crate::remote_m3u_io::Error::Unsettled)?;
            self.event_current("downloading",value.progress,json!({"speed":value.speed,"elapsed_time":value.elapsed,"time_remaining":value.remaining,"message":value.message})).await.map_err(|_|crate::remote_m3u_io::Error::Unsettled)
        })
    }
}

#[cfg(test)]
mod source_controls {
    use super::*;
    #[test]
    fn xc_categories_keep_source_default_replacement_position_and_scalar_ids() {
        let result=categories(&json!([{"category_name":"Sports","category_id":7},{"category_name":"News","category_id":"8"},{"category_name":"Sports","category_id":"9"},{"category_id":true}])).unwrap();
        assert_eq!(
            result.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
            ["Default Group", "Sports", "News", "Unknown Category"]
        );
        assert_eq!(result[1].1["xc_id"], "9");
        assert_eq!(result[3].1["xc_id"], true);
        assert!(categories(&json!({"error":"not a category list"})).is_err());
    }
    #[test]
    fn xc_projection_keeps_nullable_fields_raw_num_and_source_identity() {
        let enabled = BTreeMap::from([("7".into(), ("Sports".into(), 12))]);
        let groups = BTreeMap::from([("Sports".into(), 12)]);
        let context = ProjectionContext {
            account_id: 41,
            account_type: "XC",
            groups: &groups,
            hash_keys: &["url", "tvg_id"],
            max_name_characters: 512,
        };
        let row=project(&json!({"stream_id":93,"category_id":7,"name":"","stream_icon":null,"epg_channel_id":null,"num":4.5,"is_adult":1,"tv_archive":1,"tv_archive_duration":3,"stream_type":"radio_streams"}),"Provider","https://owned.invalid/base/live/private-user/private-pass/",&enabled,&context).unwrap().unwrap();
        assert_eq!(row.stream.name, "Provider - 93");
        assert_eq!(
            row.stream.url,
            "https://owned.invalid/base/live/private-user/private-pass/93.ts"
        );
        assert!(row.logo_url.is_none() && row.tvg_id.is_none());
        assert_eq!(row.stream.custom_properties["num"], 4.5);
        assert_eq!(row.stream.custom_properties["tvg-id"], Value::Null);
        assert_eq!(row.stream.stream_id, Some(93));
        assert_eq!(row.stream.stream_chno, Some(4.5));
        assert!(row.stream.is_adult && row.stream.is_catchup && row.stream.is_radio);
        assert_eq!(row.stream.catchup_days, 3);
        // Source Stream.generate_hash_key: actual provider ID replaces URL;
        // account identity is forced, and the null EPG ID stays JSON null.
        assert_eq!(
            row.stream.stream_hash,
            dispatcharr_ingest_core::stream_hash_nullable(
                &dispatcharr_ingest_core::NullableStreamIdentity {
                    name: Some("Provider - 93"),
                    url: Some("unrelated credentials"),
                    tvg_id: None,
                    m3u_id: Some(41),
                    group: Some("Sports"),
                    account_type: "XC",
                    stream_id: Some(93)
                },
                &["url", "tvg_id"]
            )
        );
        assert!(project(
            &json!({"category_id":"disabled","stream_id":94}),
            "Provider",
            "https://owned.invalid/live/u/p/",
            &enabled,
            &context
        )
        .unwrap()
        .is_none());
    }
    #[test]
    fn xc_stream_name_uses_actual_source_stream_model_512_codepoints() {
        let enabled = BTreeMap::from([("7".into(), ("Sports".into(), 12))]);
        let groups = BTreeMap::from([("Sports".into(), 12)]);
        let context = ProjectionContext {
            account_id: 41,
            account_type: "XC",
            groups: &groups,
            hash_keys: &["name", "url"],
            max_name_characters: 512,
        };
        let title = "界".repeat(513);
        let row = project(
            &json!({"stream_id":93,"category_id":7,"name":title}),
            "Provider",
            "https://owned.invalid/live/u/p/",
            &enabled,
            &context,
        )
        .unwrap()
        .unwrap();
        assert_eq!(row.stream.name, "界".repeat(512));
        assert_eq!(row.stream.name.chars().count(), 512);
    }
    #[test]
    fn xc_hash_override_drops_unknown_tokens_deduplicates_and_falls_back() {
        assert_eq!(
            hash_keys(&json!(" name,unknown,url,name,tvg_id "), &json!("group")).unwrap(),
            ["name", "url", "tvg_id"]
        );
        assert_eq!(
            hash_keys(&json!("unknown"), &json!(" url,group,url")).unwrap(),
            ["url", "group"]
        );
    }
}
