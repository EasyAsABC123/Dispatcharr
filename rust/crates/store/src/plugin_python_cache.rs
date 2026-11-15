//! Actual redis-py/django_redis callbacks through Main's EXISTING cache client,
//! permits and retained physical replies. No new Redis socket/pool/client.
#[path = "plugin_python_cache_scripts.rs"]
mod scripts;
use super::plugin_execution_host::Invocation;
use crate::{AuthService, StoreError};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
#[derive(Clone)]
pub struct PythonCachePort {
    auth: AuthService,
}
fn unavailable() -> StoreError {
    StoreError::Unsupported("Lazy Python cache command is outside bounded shared transport")
}
// A single Redis command remains atomic. Bound RESP data BEFORE the shared
// client allocates it; unknown scripts/stateful/blocking protocols are guards.
const BOUNDED: &str = r#"
local r=redis.pcall(ARGV[1],unpack(ARGV,3))
local q={r};local bytes=0;local nodes=0
while #q>0 do
 local v=table.remove(q);nodes=nodes+1
 if nodes>65536 then return redis.error_reply('NATIVE_CAPACITY cache nodes') end
 if type(v)=='string' then bytes=bytes+#v;if bytes>tonumber(ARGV[2]) then return redis.error_reply('NATIVE_CAPACITY cache bytes') end
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
            | "SCAN"
            | "HGET"
            | "HGETALL"
            | "HSET"
            | "HMGET"
            | "HMSET"
            | "HDEL"
            | "HEXISTS"
            | "HINCRBY"
            | "HLEN"
            | "HSCAN"
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
            | "SSCAN"
            | "ZADD"
            | "ZREM"
            | "ZCARD"
            | "ZRANGE"
            | "ZRANGEBYSCORE"
            | "ZREMRANGEBYSCORE"
            | "ZSCORE"
            | "ZCOUNT"
            | "ZSCAN"
            | "PUBLISH"
            | "PING"
            | "TYPE"
    )
}
fn wire(value: redis::Value) -> Result<Value, StoreError> {
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
impl PythonCachePort {
    pub fn new(auth: AuthService) -> Self {
        Self { auth }
    }
    pub async fn callback(
        &self,
        method: &str,
        params: Value,
        scope: Invocation,
    ) -> Result<Value, StoreError> {
        let auth = self.auth.clone().with_retained_scope(scope.owner);
        let cache = auth.notification_cache.as_ref().ok_or_else(unavailable)?;
        if method == "validate_target" {
            let target = &cache.plugin_target;
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
        if method != "commands" {
            return Err(unavailable());
        }
        let rows = params["commands"].as_array().ok_or_else(unavailable)?;
        if rows.is_empty() || rows.len() > 256 {
            return Err(unavailable());
        }
        let transaction = params["transaction"] == true;
        let mut decoded = Vec::new();
        let mut total = 0usize;
        for row in rows {
            let args = row.as_array().ok_or_else(unavailable)?;
            if args.is_empty() || args.len() > 4096 {
                return Err(unavailable());
            }
            let mut args = args
                .iter()
                .map(|value| {
                    let raw = value.as_str().ok_or_else(unavailable)?;
                    if raw.len() > 2 * 1024 * 1024 {
                        return Err(unavailable());
                    }
                    STANDARD.decode(raw).map_err(|_| unavailable())
                })
                .collect::<Result<Vec<_>, _>>()?;
            for arg in &args {
                total = total.checked_add(arg.len()).ok_or_else(unavailable)?;
                if total > 1024 * 1024 {
                    return Err(unavailable());
                }
            }
            let words = std::str::from_utf8(&args[0])
                .map_err(|_| unavailable())?
                .split_ascii_whitespace()
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<_>>();
            if words.is_empty() {
                return Err(unavailable());
            }
            args.splice(0..1, words);
            let command = std::str::from_utf8(&args[0])
                .map_err(|_| unavailable())?
                .to_ascii_uppercase();
            let config =
                command == "CONFIG" && args.len() == 4 && args[1].eq_ignore_ascii_case(b"SET");
            if !allowed(&command) && !config && !scripts::direct(&command, &args) {
                return Err(unavailable());
            }
            decoded.push((command, args));
        }
        if transaction {
            // Source validates arity while queueing MULTI, before any effect.
            // Dry queue+DISCARD on this same packed physical operation checks
            // original commands, so wrapping their bounded replies cannot turn
            // Source EXECABORT into partially-applied writes.
            let mut check = redis::pipe();
            check.cmd("MULTI");
            for (command, args) in &decoded {
                check.cmd(command);
                for arg in &args[1..] {
                    check.arg(arg);
                }
            }
            check.cmd("DISCARD");
            match cache.plugin_pipeline(check, 0, decoded.len() + 2).await? {
                Ok(values) => {
                    let mut errors = Vec::new();
                    for (index, value) in values.into_iter().skip(1).take(decoded.len()).enumerate()
                    {
                        if let redis::Value::ServerError(_) = &value {
                            errors.push(json!({"index":index,"error":wire(value)?}));
                        }
                    }
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
        for (command, args) in &decoded {
            if command == "CONFIG" || scripts::direct(command, args) {
                pipeline.cmd(command);
            } else {
                pipeline
                    .cmd("EVAL")
                    .arg(BOUNDED)
                    .arg(0)
                    .arg(command)
                    .arg(4194304 / decoded.len());
            }
            for arg in &args[1..] {
                pipeline.arg(arg);
            }
        }
        let (offset, count) = if transaction {
            (decoded.len() + 1, 1)
        } else {
            (0, decoded.len())
        };
        match cache.plugin_pipeline(pipeline, offset, count).await? {
            Ok(mut values) => {
                if transaction {
                    match values.pop() {
                        Some(redis::Value::Array(items)) => values = items,
                        _ => return Err(unavailable()),
                    }
                }
                Ok(json!({"values":values.into_iter().map(wire).collect::<Result<Vec<_>,_>>()?}))
            }
            Err(error) => Ok(json!({"error":wire_error(error)})),
        }
    }
}
