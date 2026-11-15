//! Actual lazy-host DBAPI connection handles. Reuses an invocation's exact
//! admitted TaskSqlSession; background SQL admits through the SAME Auth4/pool8.
use super::plugin_execution_host::Invocation;
use super::plugin_python_sql::PythonSqlSession;
use crate::{AuthService, StoreError, TaskSqlSession};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::Mutex;
struct Entry {
    session: TaskSqlSession,
    owned: bool,
}
#[derive(Clone)]
pub struct PythonSqlRegistry {
    auth: AuthService,
    closed: Arc<AtomicBool>,
    entries: Arc<Mutex<HashMap<String, Entry>>>,
}
fn unavailable() -> StoreError {
    StoreError::Unsupported("Lazy Python owned SQL handle unavailable")
}
impl PythonSqlRegistry {
    pub fn new(auth: AuthService) -> Self {
        Self {
            auth,
            closed: Arc::new(AtomicBool::new(false)),
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub async fn callback(
        &self,
        method: &str,
        params: Value,
        scope: Invocation,
    ) -> Result<Value, StoreError> {
        if self.closed.load(Ordering::Acquire) && method != "close" {
            return Err(unavailable());
        }
        if method == "settings" {
            let options = self.auth.store.pool().connect_options();
            let host = options
                .get_socket()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| options.get_host().to_owned());
            return Ok(
                serde_json::json!({"NAME":options.get_database().unwrap_or(options.get_username()),"USER":options.get_username(),"HOST":host,"PORT":options.get_port().to_string(),"ENGINE":"dispatcharr_native.db"}),
            );
        }
        let key = params["connection"]
            .as_str()
            .filter(|v| v.len() == 32 && v.bytes().all(|b| b.is_ascii_hexdigit()))
            .ok_or_else(unavailable)?
            .to_owned();
        match method {
            "open" => {
                {
                    let entries = self.entries.lock().await;
                    if entries.contains_key(&key) || entries.len() >= 64 {
                        return Err(unavailable());
                    }
                }
                let (owned, session) = match scope.sql {
                    Some(session) => (false, session),
                    None => (true, self.auth.borrow_task_sql_session(scope.owner).await?),
                };
                let info = PythonSqlSession::new(session.clone()).info().await;
                if info.is_err() {
                    if owned {
                        let _ = session.close().await;
                    }
                    return info;
                }
                let mut entries = self.entries.lock().await;
                if entries.contains_key(&key) || entries.len() >= 64 {
                    drop(entries);
                    if owned {
                        session.close().await?;
                    }
                    return Err(unavailable());
                }
                entries.insert(key, Entry { session, owned });
                info
            }
            "query" => {
                let session = self
                    .entries
                    .lock()
                    .await
                    .get(&key)
                    .ok_or_else(unavailable)?
                    .session
                    .clone();
                let encoded = params["query_base64"].as_str().ok_or_else(unavailable)?;
                if encoded.len() > 2 * 1024 * 1024 {
                    return Err(unavailable());
                }
                let raw = STANDARD.decode(encoded).map_err(|_| unavailable())?;
                if raw.len() > 1024 * 1024 {
                    return Err(unavailable());
                }
                let sql = String::from_utf8(raw).map_err(|_| unavailable())?;
                PythonSqlSession::new(session).query(sql).await
            }
            "close" => {
                let entry = self
                    .entries
                    .lock()
                    .await
                    .remove(&key)
                    .ok_or_else(unavailable)?;
                if entry.owned {
                    entry.session.close().await?;
                }
                // Existing invocation lease belongs to the actual Rust caller,
                // not to a Django wrapper close. No second checkout or detach.
                Ok(Value::Null)
            }
            _ => Err(unavailable()),
        }
    }
    /// After actual child EOF, join/close every owned background SQL session;
    /// borrowed invocation leases remain with their original physical owner.
    pub fn begin_shutdown(&self) {
        self.closed.store(true, Ordering::Release);
    }
    pub async fn shutdown(&self) -> Result<(), StoreError> {
        self.begin_shutdown();
        let entries = std::mem::take(&mut *self.entries.lock().await);
        let mut failed = None;
        for (_, entry) in entries {
            if entry.owned {
                if let Err(error) = entry.session.close().await {
                    failed = Some(error);
                }
            }
        }
        failed.map_or(Ok(()), Err)
    }
}
