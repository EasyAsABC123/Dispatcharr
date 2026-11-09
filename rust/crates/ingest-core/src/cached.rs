//! Bounded two-pass reading of original Dispatcharr's parsed M3U JSON cache.
//! Original application/algorithm credit belongs to the original authors.
use crate::{Entry, ImportedCacheGroup};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::{
    cell::Cell,
    collections::HashMap,
    fmt,
    fs::File,
    io::{self, BufReader, Read, Seek},
    path::Path,
    rc::Rc,
};

#[derive(Clone, Debug)]
pub struct CacheReadLimits {
    pub bytes: u64,
    pub entries: u64,
    pub entry_bytes: usize,
    pub groups: usize,
    pub group_bytes: usize,
}
impl Default for CacheReadLimits {
    fn default() -> Self {
        Self {
            bytes: 4 * 1024 * 1024 * 1024,
            entries: 5_000_000,
            entry_bytes: 8 * 1024 * 1024,
            groups: 32768,
            group_bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Debug)]
pub enum CacheReadError {
    Io(io::Error),
    Json(serde_json::Error),
    Capacity,
}
impl From<io::Error> for CacheReadError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<serde_json::Error> for CacheReadError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
impl fmt::Display for CacheReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Json(e) => e.fmt(f),
            Self::Capacity => f.write_str("Parsed M3U cache capacity exceeded"),
        }
    }
}
impl std::error::Error for CacheReadError {}
struct Budget {
    field: Cell<usize>,
    total: Cell<u64>,
}
struct Reader<R> {
    inner: R,
    budget: Rc<Budget>,
}
impl<R: Read> Read for Reader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.budget.field.get() == 0 || self.budget.total.get() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Parsed M3U cache field capacity exceeded",
            ));
        }
        let size = output
            .len()
            .min(self.budget.field.get())
            .min(self.budget.total.get().min(usize::MAX as u64) as usize);
        let count = self.inner.read(&mut output[..size])?;
        self.budget.field.set(self.budget.field.get() - count);
        self.budget
            .total
            .set(self.budget.total.get() - count as u64);
        Ok(count)
    }
}
struct Rows<'a, F> {
    limits: &'a CacheReadLimits,
    budget: Rc<Budget>,
    consume: &'a mut F,
    count: &'a mut u64,
}
struct OneRow<'a, F> {
    limits: &'a CacheReadLimits,
    budget: Rc<Budget>,
    consume: &'a mut F,
}
impl<'de, F: FnMut(Entry) -> Result<(), String>> DeserializeSeed<'de> for OneRow<'_, F> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        self.budget.field.set(self.limits.entry_bytes);
        let row = Entry::deserialize(deserializer)?;
        (self.consume)(row).map_err(de::Error::custom)
    }
}
impl<'de, F: FnMut(Entry) -> Result<(), String>> DeserializeSeed<'de> for Rows<'_, F> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}
impl<'de, F: FnMut(Entry) -> Result<(), String>> Visitor<'de> for Rows<'_, F> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("parsed M3U entry array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        loop {
            self.budget.field.set(self.limits.entry_bytes);
            if sequence
                .next_element_seed(OneRow {
                    limits: self.limits,
                    budget: self.budget.clone(),
                    consume: self.consume,
                })?
                .is_none()
            {
                break;
            }
            *self.count += 1;
            if *self.count > self.limits.entries {
                return Err(de::Error::custom(
                    "Parsed M3U entry-count capacity exceeded",
                ));
            }
        }
        Ok(())
    }
}
struct CacheVisitor<'a, F> {
    limits: &'a CacheReadLimits,
    budget: Rc<Budget>,
    consume: &'a mut F,
}
struct Groups<'a> {
    limits: &'a CacheReadLimits,
}
impl<'de> DeserializeSeed<'de> for Groups<'_> {
    type Value = Vec<ImportedCacheGroup>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Groups<'_> {
    type Value = Vec<ImportedCacheGroup>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("parsed M3U group object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut groups: Vec<ImportedCacheGroup> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        while let Some(name) = map.next_key::<String>()? {
            if groups.len() >= self.limits.groups && !positions.contains_key(&name) {
                return Err(de::Error::custom("Parsed group-count capacity exceeded"));
            }
            let properties = map.next_value::<Map<String, Value>>()?;
            if let Some(index) = positions.get(&name).copied() {
                groups[index].properties = properties;
            } else {
                positions.insert(name.clone(), groups.len());
                groups.push(ImportedCacheGroup { name, properties });
            }
        }
        Ok(groups)
    }
}
impl<'de, F: FnMut(Entry) -> Result<(), String>> Visitor<'de> for CacheVisitor<'_, F> {
    type Value = (u64, Vec<ImportedCacheGroup>);
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("original parsed M3U cache object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut count = 0;
        let mut saw_rows = false;
        let mut groups = None;
        loop {
            self.budget.field.set(1024);
            let Some(key) = map.next_key::<String>()? else {
                break;
            };
            match key.as_str() {
                "extinf_data" => {
                    if saw_rows {
                        return Err(de::Error::custom("Duplicate parsed entry array"));
                    }
                    saw_rows = true;
                    map.next_value_seed(Rows {
                        limits: self.limits,
                        budget: self.budget.clone(),
                        consume: self.consume,
                        count: &mut count,
                    })?;
                }
                "groups" => {
                    if groups.is_some() {
                        return Err(de::Error::custom("Duplicate parsed group map"));
                    }
                    self.budget.field.set(self.limits.group_bytes);
                    groups = Some(map.next_value_seed(Groups {
                        limits: self.limits,
                    })?);
                }
                _ => {
                    self.budget.field.set(self.limits.entry_bytes);
                    map.next_value::<de::IgnoredAny>()?;
                }
            }
        }
        if !saw_rows {
            return Err(de::Error::missing_field("extinf_data"));
        }
        Ok((
            count,
            groups.ok_or_else(|| de::Error::missing_field("groups"))?,
        ))
    }
}
fn parse<F: FnMut(Entry) -> Result<(), String>>(
    file: &mut File,
    limits: &CacheReadLimits,
    consume: &mut F,
) -> Result<(u64, Vec<ImportedCacheGroup>), CacheReadError> {
    let budget = Rc::new(Budget {
        field: Cell::new(1024),
        total: Cell::new(limits.bytes.saturating_add(1)),
    });
    let input = Reader {
        inner: BufReader::with_capacity(8192, file),
        budget: budget.clone(),
    };
    let mut decoder = serde_json::Deserializer::from_reader(input);
    let result = de::Deserializer::deserialize_map(
        &mut decoder,
        CacheVisitor {
            limits,
            budget: budget.clone(),
            consume,
        },
    )?;
    budget.field.set(1024);
    decoder.end()?;
    Ok(result)
}
/// A read-only file descriptor validated to EOF before any caller side effects.
/// Callers hold the account lock; producers replace caches atomically. Concurrent
/// legacy in-place writers are not a supported mixed-worker mode.
pub struct ValidatedCache {
    file: File,
    limits: CacheReadLimits,
    pub entries: u64,
    pub groups: Vec<ImportedCacheGroup>,
}
impl ValidatedCache {
    pub fn open(path: &Path, limits: CacheReadLimits) -> Result<Self, CacheReadError> {
        Self::open_while(path, limits, || true)
    }
    /// Validate without exposing rows; cancellation is checked before opening
    /// and after each bounded entry, including during the initial EOF pass.
    pub fn open_while(
        path: &Path,
        limits: CacheReadLimits,
        mut proceed: impl FnMut() -> bool,
    ) -> Result<Self, CacheReadError> {
        if !proceed() {
            return Err(CacheReadError::Io(io::Error::new(
                io::ErrorKind::Interrupted,
                "Parsed M3U cache validation cancelled",
            )));
        }
        if limits.bytes == 0
            || limits.entry_bytes == 0
            || limits.groups == 0
            || limits.group_bytes == 0
        {
            return Err(CacheReadError::Capacity);
        }
        let mut file = File::open(path)?;
        if file.metadata()?.len() > limits.bytes {
            return Err(CacheReadError::Capacity);
        }
        let (entries, groups) = parse(&mut file, &limits, &mut |_| {
            if proceed() {
                Ok(())
            } else {
                Err("Parsed M3U cache validation cancelled".into())
            }
        })?;
        if !proceed() {
            return Err(CacheReadError::Io(io::Error::new(
                io::ErrorKind::Interrupted,
                "Parsed M3U cache validation cancelled",
            )));
        }
        file.rewind()?;
        Ok(Self {
            file,
            limits,
            entries,
            groups,
        })
    }
    pub fn for_each(
        &mut self,
        mut consume: impl FnMut(Entry) -> Result<(), String>,
    ) -> Result<(), CacheReadError> {
        self.file.rewind()?;
        let (count, _) = parse(&mut self.file, &self.limits, &mut consume)?;
        if count != self.entries {
            return Err(CacheReadError::Capacity);
        }
        Ok(())
    }
}
