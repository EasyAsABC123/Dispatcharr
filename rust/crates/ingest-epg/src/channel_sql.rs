//! Private owned channel-pass session. Ported from original Dispatcharr tasks.
//! Original authors retain all application/algorithm credit.
use crate::{channel_core::ChannelRow, ImportError};
use dispatcharr_store::Store;
use futures_util::TryStreamExt;
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
const BATCH: usize = 500;
struct Memory {
    used: AtomicUsize,
    max: usize,
}
struct Bytes {
    memory: Arc<Memory>,
    bytes: usize,
}
impl Memory {
    fn add(&self, bytes: usize) -> Result<(), ImportError> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                old.checked_add(bytes).filter(|value| *value <= self.max)
            })
            .map(|_| ())
            .map_err(|_| ImportError::Capacity("channel state bytes"))
    }
    fn hold(self: &Arc<Self>, bytes: usize) -> Result<Bytes, ImportError> {
        self.add(bytes)?;
        Ok(Bytes {
            memory: self.clone(),
            bytes,
        })
    }
}
impl Bytes {
    fn resize(&mut self, bytes: usize) -> Result<(), ImportError> {
        if bytes > self.bytes {
            self.memory.add(bytes - self.bytes)?;
        } else {
            self.memory
                .used
                .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
        }
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for Bytes {
    fn drop(&mut self) {
        self.memory.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct Pending {
    row: ChannelRow,
    _bytes: Bytes,
}
struct Cached {
    id: i64,
    name: String,
    icon: Option<String>,
    bytes: Bytes,
    key_bytes: usize,
}
impl Cached {
    fn replace(&mut self, name: String, icon: Option<String>) -> Result<(), ImportError> {
        self.bytes.resize(
            256 + self.key_bytes + name.capacity() + icon.as_ref().map_or(0, String::capacity),
        )?;
        self.name = name;
        self.icon = icon;
        Ok(())
    }
}
pub(crate) struct ChannelSession {
    connection: PgConnection,
    _retained: Arc<dyn Send + Sync>,
    source_id: i64,
    existing: HashSet<String>,
    scanned: HashSet<String>,
    set_bytes: Vec<Bytes>,
    cached: HashMap<String, Arc<Mutex<Cached>>>,
    creates: Vec<Pending>,
    updates: Vec<Arc<Mutex<Cached>>>,
    memory: Arc<Memory>,
    _base_bytes: Bytes,
    max_keys: usize,
}
impl ChannelSession {
    /// Called only inside the channel worker's supervised task; its retained
    /// guard must already exist before acquiring/sending any database command.
    pub(crate) async fn open(
        store: &Store,
        source_id: i64,
        retained: Arc<dyn Send + Sync>,
        max_keys: usize,
        max_bytes: usize,
    ) -> Result<Self, ImportError> {
        let connection = store.pool().acquire().await?.detach();
        let memory = Arc::new(Memory {
            used: AtomicUsize::new(0),
            max: max_bytes,
        });
        let base = memory.hold(128 * 1024)?;
        let mut this = Self {
            connection,
            _retained: retained,
            source_id,
            existing: HashSet::new(),
            scanned: HashSet::new(),
            set_bytes: Vec::new(),
            cached: HashMap::new(),
            creates: Vec::with_capacity(BATCH),
            updates: Vec::with_capacity(BATCH),
            memory,
            _base_bytes: base,
            max_keys,
        };
        let mut rows =
            sqlx::query("SELECT tvg_id FROM epg_epgdata WHERE epg_source_id=$1 LIMIT $2")
                .bind(source_id)
                .bind(max_keys as i64 + 1)
                .fetch(&mut this.connection);
        let mut count = 0;
        while let Some(row) = rows.try_next().await? {
            count += 1;
            if count > max_keys {
                return Err(ImportError::Capacity("existing channel identifiers"));
            }
            let tvg: Option<String> = row.try_get("tvg_id")?;
            if let Some(tvg) = tvg {
                if !this.existing.contains(&tvg) {
                    let guard = this.memory.hold(tvg.capacity() + 128)?;
                    this.existing.insert(tvg);
                    this.set_bytes.push(guard);
                }
            }
        }
        drop(rows);
        Ok(this)
    }
    pub(crate) async fn count(&mut self) -> Result<u64, ImportError> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM epg_epgdata WHERE epg_source_id=$1")
                .bind(self.source_id)
                .fetch_one(&mut self.connection)
                .await? as u64,
        )
    }
    /// True preserves Source's DoesNotExist early-continue branch: it bypasses
    /// that channel's flush/progress/cache-clear checks after queueing a create.
    pub(crate) async fn channel(&mut self, row: ChannelRow) -> Result<bool, ImportError> {
        if !self.scanned.contains(&row.tvg_id) {
            if self.scanned.len() >= self.max_keys {
                return Err(ImportError::Capacity("scanned channel identifiers"));
            }
            let key = row.tvg_id.clone();
            let bytes = self.memory.hold(key.capacity() + 128)?;
            self.scanned.insert(key);
            self.set_bytes.push(bytes);
        }
        if self.existing.contains(&row.tvg_id) {
            if !self.cached.contains_key(&row.tvg_id) {
                let existing=sqlx::query("SELECT id::bigint AS id,name,icon_url FROM epg_epgdata WHERE epg_source_id=$1 AND tvg_id=$2").bind(self.source_id).bind(&row.tvg_id).fetch_optional(&mut self.connection).await?;
                let Some(existing) = existing else {
                    self.existing.remove(&row.tvg_id);
                    self.create(row)?;
                    return Ok(true);
                };
                let name: String = existing.try_get("name")?;
                let icon: Option<String> = existing.try_get("icon_url")?;
                let key = row.tvg_id.clone();
                let key_bytes = key.capacity();
                let bytes = self.memory.hold(
                    256 + key_bytes + name.capacity() + icon.as_ref().map_or(0, String::capacity),
                )?;
                self.cached.insert(
                    key,
                    Arc::new(Mutex::new(Cached {
                        id: existing.try_get("id")?,
                        name,
                        icon,
                        bytes,
                        key_bytes,
                    })),
                );
            }
            let existing = self.cached.get(&row.tvg_id).unwrap().clone();
            let changed = {
                let value = existing.lock().unwrap();
                value.name != row.name || value.icon != row.icon_url
            };
            if changed {
                existing.lock().unwrap().replace(row.name, row.icon_url)?;
                self.updates.push(existing);
            }
        } else {
            self.create(row)?;
        }
        Ok(false)
    }
    fn create(&mut self, row: ChannelRow) -> Result<(), ImportError> {
        let bytes = self.memory.hold(
            256 + row.tvg_id.capacity()
                + row.name.capacity()
                + row.icon_url.as_ref().map_or(0, String::capacity),
        )?;
        self.creates.push(Pending { row, _bytes: bytes });
        Ok(())
    }
    pub(crate) async fn flush_full(&mut self) -> Result<(), ImportError> {
        if self.creates.len() >= BATCH {
            self.flush_creates().await?;
        }
        if self.updates.len() >= BATCH {
            self.flush_updates().await?;
        }
        Ok(())
    }
    pub(crate) fn clear_cache(&mut self) {
        self.cached.clear();
    }
    async fn flush_creates(&mut self) -> Result<(), ImportError> {
        if self.creates.is_empty() {
            return Ok(());
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO epg_epgdata(tvg_id,name,icon_url,epg_source_id) ",
        );
        query.push_values(self.creates.iter(), |mut bind, value| {
            bind.push_bind(&value.row.tvg_id)
                .push_bind(&value.row.name)
                .push_bind(&value.row.icon_url)
                .push_bind(self.source_id);
        });
        query.push(" ON CONFLICT DO NOTHING");
        query.build().execute(&mut self.connection).await?;
        self.creates.clear();
        Ok(())
    }
    async fn flush_updates(&mut self) -> Result<(), ImportError> {
        if self.updates.is_empty() {
            return Ok(());
        }
        // Keep each mutable object until final encoding. Duplicate references
        // see its last fields; after cache-clear distinct objects retain order.
        let rows = self
            .updates
            .iter()
            .map(|row| {
                let row = row.lock().unwrap();
                (row.id, row.name.clone(), row.icon.clone())
            })
            .collect::<Vec<_>>();
        let mut query = QueryBuilder::<Postgres>::new("UPDATE epg_epgdata SET name=CASE id ");
        for (id, name, _) in &rows {
            query
                .push("WHEN ")
                .push_bind(id)
                .push(" THEN ")
                .push_bind(name)
                .push(" ");
        }
        query.push("END,icon_url=CASE id ");
        for (id, _, icon) in &rows {
            query
                .push("WHEN ")
                .push_bind(id)
                .push(" THEN ")
                .push_bind(icon)
                .push(" ");
        }
        query.push("END WHERE id IN (");
        let mut ids = query.separated(",");
        for (id, _, _) in &rows {
            ids.push_bind(id);
        }
        query.push(")");
        query.build().execute(&mut self.connection).await?;
        self.updates.clear();
        Ok(())
    }
    pub(crate) async fn finish(mut self) -> Result<(), ImportError> {
        self.flush_creates().await?;
        self.flush_updates().await?;
        // Snapshot the whole stale selection once, as Django's collector does,
        // then keep every public cascade/delete in one transaction. Private
        // temporary rows bound client memory even for a large catalogue.
        sqlx::query("CREATE TEMP TABLE dispatcharr_channel_stale_names(tvg_id text PRIMARY KEY) ON COMMIT PRESERVE ROWS").execute(&mut self.connection).await?;
        let _stale_bytes = self
            .memory
            .hold(self.existing.len() * std::mem::size_of::<&String>() + 4096)?;
        let stale = self.existing.difference(&self.scanned).collect::<Vec<_>>();
        for chunk in stale.chunks(500) {
            let mut query = QueryBuilder::<Postgres>::new(
                "INSERT INTO dispatcharr_channel_stale_names(tvg_id) ",
            );
            query.push_values(chunk.iter(), |mut row, value| {
                row.push_bind(value.as_str());
            });
            query.build().execute(&mut self.connection).await?;
        }
        sqlx::query("CREATE TEMP TABLE dispatcharr_channel_stale_ids ON COMMIT PRESERVE ROWS AS SELECT e.id FROM epg_epgdata e JOIN dispatcharr_channel_stale_names s ON s.tvg_id=e.tvg_id WHERE e.epg_source_id=$1 AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c WHERE c.epg_data_id=e.id)").bind(self.source_id).execute(&mut self.connection).await?;
        let mut tx = self.connection.begin().await?;
        sqlx::query("DELETE FROM epg_programdata WHERE epg_id IN(SELECT id FROM dispatcharr_channel_stale_ids)").execute(&mut *tx).await?;
        sqlx::query("UPDATE dispatcharr_channels_channeloverride SET epg_data_id=NULL WHERE epg_data_id IN(SELECT id FROM dispatcharr_channel_stale_ids)").execute(&mut *tx).await?;
        sqlx::query("UPDATE dispatcharr_channels_channel SET epg_data_id=NULL WHERE epg_data_id IN(SELECT id FROM dispatcharr_channel_stale_ids)").execute(&mut *tx).await?;
        sqlx::query(
            "DELETE FROM epg_epgdata WHERE id IN(SELECT id FROM dispatcharr_channel_stale_ids)",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
