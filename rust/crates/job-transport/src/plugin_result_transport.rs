//! Actual pinned RedisBackend commands and PubSub over the SAME Jobs results
//! transport. No independent client/pool, fabricated result, or dropped read.
//! Original Celery/Redis/Dispatcharr authors retain protocol/algorithm credit.
use super::plugin_registered_transport::PluginError;
use super::{Authority, Jobs};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_output_store::transport::{LogicalSubscription, SubscriptionError};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
type Owner = Arc<dyn Send + Sync>;
#[derive(Clone)]
pub struct PluginResults {
    jobs: Jobs,
    lifetime: Owner,
    closed: Arc<AtomicBool>,
    clients: Arc<tokio::sync::Mutex<BTreeMap<String, LogicalSubscription>>>,
}
fn unavailable() -> PluginError {
    PluginError::Unsupported
}
fn subscription(error: SubscriptionError) -> PluginError {
    match error {
        SubscriptionError::Capacity => PluginError::Capacity,
        SubscriptionError::Closed => PluginError::Closed,
        _ => PluginError::Backend,
    }
}
const BOUNDED: &str = r#"
local r=redis.pcall(ARGV[1],unpack(ARGV,3))
local limit=tonumber(ARGV[2]);local q={r};local bytes=0;local nodes=0
while #q>0 do local v=table.remove(q);nodes=nodes+1
 if nodes>65536 then return redis.error_reply('NATIVE_CAPACITY result nodes') end
 if type(v)=='string' then bytes=bytes+#v;if bytes>limit then return redis.error_reply('NATIVE_CAPACITY result bytes') end
 elseif type(v)=='table' then for _,x in pairs(v)do q[#q+1]=x end end
end
return r
"#;
fn allowed(command: &str) -> bool {
    matches!(
        command,
        "GET"
            | "SET"
            | "SETNX"
            | "SETEX"
            | "PSETEX"
            | "MGET"
            | "MSET"
            | "MSETNX"
            | "DEL"
            | "UNLINK"
            | "EXISTS"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "PERSIST"
            | "TTL"
            | "PTTL"
            | "INCR"
            | "INCRBY"
            | "INCRBYFLOAT"
            | "DECR"
            | "DECRBY"
            | "HGET"
            | "HGETALL"
            | "HSET"
            | "HMGET"
            | "HMSET"
            | "HDEL"
            | "HEXISTS"
            | "HINCRBY"
            | "HLEN"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "LLEN"
            | "LRANGE"
            | "LTRIM"
            | "LREM"
            | "SADD"
            | "SREM"
            | "SMEMBERS"
            | "SCARD"
            | "SISMEMBER"
            | "ZADD"
            | "ZREM"
            | "ZCARD"
            | "ZRANGE"
            | "ZRANGEBYSCORE"
            | "ZREMRANGEBYSCORE"
            | "ZSCORE"
            | "ZCOUNT"
            | "PUBLISH"
            | "PING"
            | "TYPE"
    )
}
fn wire(value: redis::Value) -> Result<Value, PluginError> {
    use redis::Value as R;
    Ok(match value {
        R::Nil => json!({"kind":"nil"}),
        R::Int(value) => json!({"kind":"int","value":value.to_string()}),
        R::BulkString(value) => json!({"kind":"bytes","value":STANDARD.encode(value)}),
        R::SimpleString(value) => json!({"kind":"bytes","value":STANDARD.encode(value.as_bytes())}),
        R::Okay => json!({"kind":"bytes","value":STANDARD.encode(b"OK")}),
        R::Array(values) => {
            json!({"kind":"array","value":values.into_iter().map(wire).collect::<Result<Vec<_>,_>>()?})
        }
        R::ServerError(error) => {
            json!({"kind":"server_error","value":format!("{}{}",error.code(),error.details().map(|v|format!(" {v}")).unwrap_or_default())})
        }
        _ => return Err(unavailable()),
    })
}
fn wire_error(error: redis::RedisError) -> Value {
    if let Some(code) = error.code() {
        json!({"kind":"server_error","value":format!("{}{}",code,error.detail().map(|v|format!(" {v}")).unwrap_or_default())})
    } else if error.kind() == redis::ErrorKind::AuthenticationFailed {
        json!({"kind":"authentication_error","value":error.to_string()})
    } else if error.is_timeout() {
        json!({"kind":"timeout","value":error.to_string()})
    } else {
        json!({"kind":"connection_error","value":error.to_string()})
    }
}

impl PluginResults {
    pub fn prepare(jobs: Jobs, lifetime: Owner) -> Result<Self, PluginError> {
        if !matches!(jobs.inner.authority, Authority::DefaultServices) {
            return Err(unavailable());
        }
        Ok(Self {
            jobs,
            lifetime,
            closed: Arc::new(AtomicBool::new(false)),
            clients: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        })
    }
    async fn pipeline(
        &self,
        pipeline: redis::Pipeline,
        offset: usize,
        count: usize,
        owner: Owner,
    ) -> Result<Result<Vec<redis::Value>, redis::RedisError>, PluginError> {
        let lease = self.jobs.lease(owner).map_err(|_| PluginError::Closed)?;
        self.jobs
            .inner
            .result
            .dependency_pipeline(pipeline, offset, count, lease)
            .await
            .map(|(value, _)| value)
            .map_err(|_| PluginError::Backend)
    }
    async fn logical(&self, params: &Value) -> Result<LogicalSubscription, PluginError> {
        let id = params["id"]
            .as_str()
            .filter(|v| v.len() <= 128)
            .ok_or_else(unavailable)?;
        self.clients
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or(PluginError::Closed)
    }
    pub async fn callback(
        &self,
        method: &str,
        params: Value,
        owner: Owner,
    ) -> Result<Value, PluginError> {
        if self.closed.load(Ordering::SeqCst) && method != "pubsub_close" {
            return Err(PluginError::Closed);
        }
        if method == "validate_target" {
            let target = self.jobs.inner.result.configured_endpoint();
            let same = match &target.addr {
                redis::ConnectionAddr::Tcp(host, port) => {
                    params["transport"] == "tcp"
                        && params["host"] == *host
                        && params["port"] == u64::from(*port)
                }
                redis::ConnectionAddr::Unix(path) => {
                    params["transport"] == "unix"
                        && params["path"] == path.to_string_lossy().as_ref()
                }
                _ => false,
            };
            if !same
                || params["db"] != target.redis.db
                || params["username"] != json!(target.redis.username)
                || params["password"] != json!(target.redis.password)
                || params["protocol"] != 2
            {
                return Err(unavailable());
            }
            return Ok(Value::Null);
        }
        if method == "pubsub_open" {
            let port = self.clone();
            return tokio::spawn(async move {
                // One global result subscriber, not one reserved operation per host/client.
                let hub = port
                    .jobs
                    .inner
                    .result
                    .dependency_subscriptions(port.lifetime.clone(), owner.clone())
                    .await
                    .map_err(subscription)?;
                let mut clients = port.clients.lock().await;
                if port.closed.load(Ordering::SeqCst) {
                    return Err(PluginError::Closed);
                }
                if clients.len() >= 64 {
                    return Err(PluginError::Capacity);
                }
                let client = hub
                    .open(
                        port.lifetime.clone(),
                        params["ignore_subscribe_messages"] == true,
                    )
                    .await
                    .map_err(subscription)?;
                let id = uuid::Uuid::new_v4().to_string();
                clients.insert(id.clone(), client);
                Ok(json!({"id":id}))
            })
            .await
            .map_err(|_| PluginError::Backend)?;
        }
        if method == "pubsub_close" {
            let id = params["id"].as_str().ok_or_else(unavailable)?;
            let client = self.clients.lock().await.remove(id);
            if let Some(client) = client {
                client.close().await.map_err(subscription)?;
            }
            return Ok(Value::Null);
        }
        if method == "pubsub_read" {
            let client = self.logical(&params).await?;
            let timeout = if params["timeout"].is_null() {
                None
            } else {
                let seconds = params["timeout"]
                    .as_f64()
                    .filter(|n| n.is_finite() && *n >= 0.0)
                    .ok_or_else(unavailable)?;
                Some(Duration::try_from_secs_f64(seconds).map_err(|_| unavailable())?)
            };
            return match client.read(timeout, owner).await {
                Ok(None) => Ok(Value::Null),
                Ok(Some(message)) => wire((*message.value).clone()),
                Err(SubscriptionError::Capacity) => {
                    Ok(json!({"kind":"server_error","value":"NATIVE_CAPACITY result subscription"}))
                }
                Err(SubscriptionError::Unsupported) => Err(PluginError::Unsupported),
                Err(_) => Ok(
                    json!({"kind":"connection_error","value":"Actual native result subscription closed"}),
                ),
            };
        }
        if method == "pubsub_command" {
            let client = self.logical(&params).await?;
            let args = decode(&params["args"])?;
            let command = args[0].as_slice();
            match command {
                b"SUBSCRIBE" => client.subscribe(args[1..].to_vec(), owner).await,
                b"UNSUBSCRIBE" => client.unsubscribe(args[1..].to_vec(), owner).await,
                _ => return Err(unavailable()),
            }
            .map_err(subscription)?;
            return Ok(Value::Null);
        }
        if method != "commands" {
            return Err(unavailable());
        }
        let rows = params["commands"]
            .as_array()
            .filter(|v| !v.is_empty() && v.len() <= 128)
            .ok_or_else(unavailable)?;
        let mut decoded = Vec::new();
        let mut bytes = 0_usize;
        for row in rows {
            let args = decode(row)?;
            bytes = bytes
                .checked_add(args.iter().map(Vec::len).sum::<usize>())
                .ok_or(PluginError::Capacity)?;
            if bytes > 1048576 {
                return Err(PluginError::Capacity);
            }
            let command = std::str::from_utf8(&args[0])
                .map_err(|_| unavailable())?
                .to_ascii_uppercase();
            if !allowed(&command) {
                return Err(unavailable());
            }
            decoded.push((command, args));
        }
        let transaction = params["transaction"] == true;
        if transaction {
            let mut check = redis::pipe();
            check.cmd("MULTI");
            for (command, args) in &decoded {
                check.cmd(command);
                for arg in &args[1..] {
                    check.arg(arg);
                }
            }
            check.cmd("DISCARD");
            match self
                .pipeline(check, 0, decoded.len() + 2, owner.clone())
                .await?
            {
                Ok(values) => {
                    let errors = values
                        .into_iter()
                        .skip(1)
                        .take(decoded.len())
                        .enumerate()
                        .filter_map(|(index, v)| {
                            if matches!(&v, redis::Value::ServerError(_)) {
                                Some(wire(v).map(|v| json!({"index":index,"error":v})))
                            } else {
                                None
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if !errors.is_empty() {
                        return Ok(json!({"queue_errors":errors}));
                    }
                }
                Err(error) => return Ok(json!({"error":wire_error(error)})),
            }
        }
        let mut pipeline = redis::pipe();
        if transaction {
            pipeline.atomic();
        }
        // Sum of all Redis payloads is bounded before req_packed_commands allocates
        // client replies; base64 expansion still fits the actual8MiB host frame.
        let per_reply = 4194304 / decoded.len();
        for (command, args) in &decoded {
            pipeline
                .cmd("EVAL")
                .arg(BOUNDED)
                .arg(0)
                .arg(command)
                .arg(per_reply);
            for arg in &args[1..] {
                pipeline.arg(arg);
            }
        }
        let (offset, count) = if transaction {
            (decoded.len() + 1, 1)
        } else {
            (0, decoded.len())
        };
        match self.pipeline(pipeline, offset, count, owner).await? {
            Ok(mut values) => {
                if transaction {
                    match values.pop() {
                        Some(redis::Value::Array(v)) => values = v,
                        _ => return Err(unavailable()),
                    }
                }
                Ok(json!({"values":values.into_iter().map(wire).collect::<Result<Vec<_>,_>>()?}))
            }
            Err(error) => Ok(json!({"error":wire_error(error)})),
        }
    }
    /// Host-specific close runs before joining callback futures; this wakes a real
    /// get_message(None). It does not close other hosts' shared upstream socket.
    pub async fn close_host(&self) -> Result<(), PluginError> {
        self.closed.store(true, Ordering::SeqCst);
        let clients = std::mem::take(&mut *self.clients.lock().await);
        let mut first = None;
        for client in clients.into_values() {
            if let Err(error) = client.close().await {
                if first.is_none() {
                    first = Some(subscription(error));
                }
            }
        }
        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
fn decode(value: &Value) -> Result<Vec<Vec<u8>>, PluginError> {
    let values = value
        .as_array()
        .filter(|v| !v.is_empty() && v.len() <= 4096)
        .ok_or_else(unavailable)?;
    let mut total = 0usize;
    values
        .iter()
        .map(|v| {
            let text = v
                .as_str()
                .filter(|v| v.len() <= 2097152)
                .ok_or_else(unavailable)?;
            let raw = STANDARD.decode(text).map_err(|_| unavailable())?;
            total = total.checked_add(raw.len()).ok_or(PluginError::Capacity)?;
            if total > 1048576 {
                return Err(PluginError::Capacity);
            }
            Ok(raw)
        })
        .collect()
}

impl Jobs {
    /// Close the one physical subscriber before the final common Jobs drain;
    /// host-specific callback drains must first close each logical client.
    pub async fn shutdown_plugin_subscriptions(
        &self,
        timeout: Duration,
    ) -> Result<(), PluginError> {
        self.inner
            .result
            .close_dependency_subscriptions(timeout)
            .await
            .map_err(subscription)?;
        Ok(())
    }
}
