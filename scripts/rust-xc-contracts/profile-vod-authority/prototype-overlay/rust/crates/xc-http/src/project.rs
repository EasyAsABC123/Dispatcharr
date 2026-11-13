//! Draft account/live DTOs from original apps/output/views.py.
//! Original authors retain credit. Source/native parity is not yet tested.
use crate::{
    counts::ConnectionCounts,
    numbers,
    reader::{self, Channel, ReadLimit},
};
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::{AuthService, Store, StoreError, User};
use serde_json::{json, Value};
use sqlx::Row;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

pub(crate) async fn info(
    store: &Store,
    counts: &ConnectionCounts,
    request: &PlaybackRequest,
    user: &User,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<Value, StoreError> {
    let limited = user.stream_limit > 0;
    let active = counts
        .count(if limited { Some(user.id) } else { None }, permit)
        .await?;
    let maximum = if limited {
        i64::from(user.stream_limit)
    } else {
        tuners(store).await.unwrap_or(1)
    };
    // Django request.scheme uses SECURE_PROXY_SSL_HEADER independently of the
    // trusted-forwarding absolute-logo URL helper. Do not merge these rules.
    let scheme = if request
        .header("x-forwarded-proto")
        .and_then(|value| value.split(',').next())
        .is_some_and(|value| value.trim() == "https")
    {
        "https"
    } else {
        "http"
    };
    let host = request.header("host").unwrap_or("localhost");
    let (hostname, port) = host
        .split_once(':')
        .unwrap_or((host, if scheme == "https" { "443" } else { "80" }));
    let at = chrono::Utc::now();
    Ok(json!({
        "user_info":{
            "username":request.parameter("username"),"password":request.parameter("password"),
            "message":"Dispatcharr XC API","auth":1,"status":"Active",
            "exp_date":(at.timestamp()+90*86400).to_string(),"active_cons":active.to_string(),
            "max_connections":maximum.to_string(),"allowed_output_formats":["ts","mp4","m3u8"]
        },
        "server_info":{"url":hostname,"server_protocol":scheme,"port":port,"timezone":"UTC",
            "timestamp_now":at.timestamp(),"time_now":at.format("%Y-%m-%d %H:%M:%S").to_string(),"process":true}
    }))
}
async fn tuners(store: &Store) -> Result<i64, sqlx::Error> {
    let row=sqlx::query("SELECT COALESCE(bool_or(p.max_streams=0),false) AS unlimited,COALESCE(sum(p.max_streams) FILTER(WHERE p.max_streams>0),0)::bigint AS limited FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE p.id<>1 AND p.is_active AND a.is_active").fetch_one(store.pool()).await?;
    let custom: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dispatcharr_channels_stream WHERE is_custom")
            .fetch_one(store.pool())
            .await?;
    Ok((if row.try_get::<bool, _>("unlimited")? {
        50
    } else {
        row.try_get::<i64, _>("limited")?
    } + custom)
        .max(1))
}

pub(crate) struct LiveRows {
    rows: Vec<Channel>,
    numbers: HashMap<i64, String>,
    default: Option<i64>,
    catchup: bool,
    origin: String,
}
impl LiveRows {
    pub(crate) async fn read(
        store: &Store,
        request: &PlaybackRequest,
        user: &User,
        limits: &ReadLimit,
    ) -> Result<Vec<Channel>, StoreError> {
        let category = request
            .parameter("category_id")
            .map(|value| {
                dispatcharr_ingest_core::source_integer_literal(value)
                    .and_then(|value| value.parse::<i64>().ok())
                    .ok_or_else(|| {
                        StoreError::StatusBody(500, json!({"error":"XC category lookup failed."}))
                    })
            })
            .transpose()?;
        reader::channels(store, user, category, limits).await
    }
    pub(crate) async fn project(
        auth: &AuthService,
        request: &PlaybackRequest,
        user: &User,
        rows: Vec<Channel>,
        number_budget: usize,
    ) -> Result<Self, StoreError> {
        let pairs: Vec<_> = rows.iter().map(|row| (row.id, row.number)).collect();
        let numbers = numbers::assign(&pairs, number_budget).map_err(|_| reader::capacity())?;
        let catchup = user.custom_properties.get("catchup_enabled") != Some(&Value::Bool(false))
            && auth.system_settings().await?.get("catchup_enabled") != Some(&Value::Bool(false));
        let origin = dispatcharr_output_http::source_output_origin(auth, request);
        Ok(Self {
            rows,
            numbers,
            default: None,
            catchup,
            origin,
        })
    }
    pub(crate) async fn write(
        &mut self,
        store: &Store,
        out: &mut JsonBytes,
        as_object: bool,
    ) -> Result<(), StoreError> {
        out.raw(if as_object { b"{" } else { b"[" })?;
        for (index, row) in self.rows.iter().enumerate() {
            if index > 0 {
                out.raw(if as_object { b", " } else { b"," })?
            }
            if as_object {
                out.string(&row.id.to_string())?;
                out.raw(b": ")?;
            }
            let number = self.numbers.get(&row.id).ok_or_else(reader::capacity)?;
            let group = if let Some(group) = row.effective_group {
                group
            } else if let Some(group) = self.default {
                group
            } else {
                let group = reader::default_group(store).await?;
                self.default = Some(group);
                group
            };
            let value = json!({
                "num":serde_json::from_str::<Value>(number).map_err(|_|reader::capacity())?,
                "name":row.name,"stream_type":if row.radio{"radio_streams"}else{"live"},"stream_id":row.id,
                "stream_icon":row.logo.map(|id|format!("{}/api/channels/logos/{id}/cache/",self.origin)),
                "epg_channel_id":number,"added":(row.created_micros/1_000_000).to_string(),"is_adult":i32::from(row.adult),
                "category_id":group.to_string(),"category_ids":[group],"custom_sid":"",
                "tv_archive":i32::from(self.catchup&&row.catchup),"direct_source":"",
                "tv_archive_duration":if self.catchup&&row.catchup{row.catchup_days}else{0}
            });
            out.value(&value)?;
        }
        out.raw(if as_object { b"}" } else { b"]" })
    }
}

/// Bounded ASCII JSON writer for these integer/string/bool-only Source DTOs.
/// It avoids a second whole-response Value or escaped temporary allocation.
/// Unsupported float values fail closed until separate formatting proof exists.
pub(crate) struct JsonBytes {
    data: Vec<u8>,
    limit: usize,
}
impl JsonBytes {
    pub(crate) fn new(limit: usize) -> Result<Self, StoreError> {
        Ok(Self {
            data: Vec::new(),
            limit,
        })
    }
    pub(crate) fn finish(self) -> Vec<u8> {
        self.data
    }
    pub(crate) fn raw(&mut self, value: &[u8]) -> Result<(), StoreError> {
        let needed = self
            .data
            .len()
            .checked_add(value.len())
            .ok_or_else(reader::capacity)?;
        if needed > self.limit {
            return Err(reader::capacity());
        }
        if needed > self.data.capacity() {
            let target = needed
                .checked_next_power_of_two()
                .unwrap_or(self.limit)
                .min(self.limit);
            self.data
                .try_reserve_exact(target - self.data.len())
                .map_err(|_| reader::capacity())?;
            if self.data.capacity() > self.limit {
                return Err(reader::capacity());
            }
        }
        self.data.extend_from_slice(value);
        Ok(())
    }
    pub(crate) fn string(&mut self, value: &str) -> Result<(), StoreError> {
        self.raw(b"\"")?;
        for c in value.chars() {
            match c {
                '"' => self.raw(b"\\\"")?,
                '\\' => self.raw(b"\\\\")?,
                '\n' => self.raw(b"\\n")?,
                '\r' => self.raw(b"\\r")?,
                '\t' => self.raw(b"\\t")?,
                '\u{8}' => self.raw(b"\\b")?,
                '\u{c}' => self.raw(b"\\f")?,
                '\u{20}'..='\u{7e}' => self.raw(&[c as u8])?,
                c => {
                    let mut units = [0u16; 2];
                    for unit in c.encode_utf16(&mut units) {
                        let hex = format!("\\u{:04x}", *unit);
                        self.raw(hex.as_bytes())?;
                    }
                }
            }
        }
        self.raw(b"\"")
    }
    pub(crate) fn value(&mut self, value: &Value) -> Result<(), StoreError> {
        match value {
            Value::Null => self.raw(b"null"),
            Value::Bool(true) => self.raw(b"true"),
            Value::Bool(false) => self.raw(b"false"),
            Value::String(value) => self.string(value),
            Value::Number(value) => {
                let text = value.to_string();
                if text.bytes().any(|c| matches!(c, b'.' | b'e' | b'E')) {
                    return Err(reader::capacity());
                }
                self.raw(text.as_bytes())
            }
            Value::Array(values) => {
                self.raw(b"[")?;
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        self.raw(b", ")?
                    }
                    self.value(value)?;
                }
                self.raw(b"]")
            }
            Value::Object(values) => {
                self.raw(b"{")?;
                for (index, (key, value)) in values.iter().enumerate() {
                    if index > 0 {
                        self.raw(b", ")?
                    }
                    self.string(key)?;
                    self.raw(b": ")?;
                    self.value(value)?;
                }
                self.raw(b"}")
            }
        }
    }
}
