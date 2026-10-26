//! The local page cache.
//!
//! A read-write database's cache is authoritative while this process holds
//! the lease: a cached page is served without a request. A read-only
//! database's cache saves bandwidth only: each read sends the cached copy's
//! checksum, and the server answers "unchanged" or with the bytes. Every page
//! served from the cache is checked against its checksum first, so a damaged
//! cache costs a request, never a wrong answer.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

use crate::config::CacheConfig;

/// Where one database's cached pages live, keyed by file offset.
pub trait CacheStore: Send {
    fn read(&mut self, key: u64) -> Option<Vec<u8>>;
    /// Returns false when the page could not be stored.
    fn write(&mut self, key: u64, data: &[u8]) -> bool;
    fn remove(&mut self, key: u64);
    fn clear(&mut self);
}

/// Opens the store for one database.
pub trait CacheStores: Send + Sync {
    fn open(&self, database: &str, readonly: bool) -> Option<Box<dyn CacheStore>>;
}

/// Stores each page as a file in a per-database directory.
pub struct DiskStores {
    dir: PathBuf,
}

impl DiskStores {
    pub fn new(dir: PathBuf) -> DiskStores {
        DiskStores { dir }
    }
}

impl CacheStores for DiskStores {
    fn open(&self, database: &str, readonly: bool) -> Option<Box<dyn CacheStore>> {
        let mode = if readonly { "ro" } else { "rw" };
        let dir = self
            .dir
            .join(format!("{:016x}-{mode}", xxh3_64(database.as_bytes())));
        // Pages left by an earlier process may be stale.
        let _ = std::fs::remove_dir_all(&dir);
        match std::fs::create_dir_all(&dir) {
            Ok(()) => Some(Box::new(DiskStore { dir })),
            Err(e) => {
                log::warn!(
                    "page cache disabled for {database}: creating {}: {e}",
                    dir.display()
                );
                None
            }
        }
    }
}

struct DiskStore {
    dir: PathBuf,
}

impl DiskStore {
    fn path(&self, key: u64) -> PathBuf {
        self.dir.join(format!("{key:016x}"))
    }
}

impl CacheStore for DiskStore {
    fn read(&mut self, key: u64) -> Option<Vec<u8>> {
        std::fs::read(self.path(key)).ok()
    }

    fn write(&mut self, key: u64, data: &[u8]) -> bool {
        std::fs::write(self.path(key), data).is_ok()
    }

    fn remove(&mut self, key: u64) {
        let _ = std::fs::remove_file(self.path(key));
    }

    fn clear(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::create_dir_all(&self.dir);
    }
}

impl Drop for DiskStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// How the VFS builds page caches.
#[derive(Clone)]
pub struct CacheSettings {
    pub stores: Arc<dyn CacheStores>,
    /// The most bytes each open database may cache.
    pub max_bytes: u64,
}

impl CacheSettings {
    pub fn from_config(config: &CacheConfig) -> CacheSettings {
        CacheSettings {
            stores: Arc::new(DiskStores::new(config.dir.clone())),
            max_bytes: config.max_bytes,
        }
    }
}

#[derive(Clone, Copy)]
struct Entry {
    len: u64,
    checksum: u64,
    /// When the page was last used, for least-recently-used eviction.
    tick: u64,
}

pub struct PageCache {
    store: Box<dyn CacheStore>,
    entries: BTreeMap<u64, Entry>,
    /// Keys by last use, oldest first.
    recency: BTreeMap<u64, u64>,
    next_tick: u64,
    bytes: u64,
    max_bytes: u64,
    /// The database's page size, from its header; 0 until known.
    page_size: u64,
}

/// The page size recorded in a database header, if `data` starts with one.
fn header_page_size(data: &[u8]) -> Option<u64> {
    if data.len() < 18 || &data[..16] != b"SQLite format 3\0" {
        return None;
    }
    let raw = u16::from_be_bytes([data[16], data[17]]) as u64;
    let size = if raw == 1 { 65536 } else { raw };
    (size.is_power_of_two() && (512..=65536).contains(&size)).then_some(size)
}

impl PageCache {
    pub fn open(settings: &CacheSettings, database: &str, readonly: bool) -> Option<PageCache> {
        let store = settings.stores.open(database, readonly)?;
        Some(PageCache {
            store,
            entries: BTreeMap::new(),
            recency: BTreeMap::new(),
            next_tick: 0,
            bytes: 0,
            max_bytes: settings.max_bytes,
            page_size: 0,
        })
    }

    pub fn close(self) {}

    pub fn clear(&mut self) {
        self.store.clear();
        self.entries.clear();
        self.recency.clear();
        self.bytes = 0;
    }

    /// Learns the page size from a header; a change empties the cache.
    fn observe_header(&mut self, offset: u64, data: &[u8]) {
        if offset != 0 {
            return;
        }
        if let Some(size) = header_page_size(data) {
            if size != self.page_size {
                self.clear();
                self.page_size = size;
            }
        }
    }

    fn is_page(&self, offset: u64, len: u64) -> bool {
        self.page_size != 0 && len == self.page_size && offset % self.page_size == 0
    }

    /// The checksum of the cached copy of `[offset, offset + len)`, or 0.
    pub fn checksum(&self, offset: u64, len: u64) -> u64 {
        match self.entries.get(&offset) {
            Some(e) if e.len == len && self.is_page(offset, len) => e.checksum,
            _ => 0,
        }
    }

    /// Serves a whole page from the cache, if present and intact.
    pub fn read(&mut self, offset: u64, buf: &mut [u8]) -> Option<usize> {
        let checksum = self.checksum(offset, buf.len() as u64);
        if checksum == 0 {
            return None;
        }
        self.read_verified(offset, buf, checksum)
    }

    /// Copies the cached page into `buf` if its bytes hash to `checksum`.
    pub fn read_verified(&mut self, offset: u64, buf: &mut [u8], checksum: u64) -> Option<usize> {
        match self.store.read(offset) {
            Some(data) if data.len() == buf.len() && xxh3_64(&data) == checksum => {
                buf.copy_from_slice(&data);
                self.touch(offset);
                Some(buf.len())
            }
            _ => {
                self.remove(offset);
                None
            }
        }
    }

    /// Caches bytes read from the server.
    pub fn put_read(&mut self, offset: u64, requested: u64, data: Bytes) {
        self.observe_header(offset, &data);
        if self.is_page(offset, requested) && data.len() as u64 == requested {
            self.insert(offset, &data);
        }
    }

    /// Records committed bytes: whole pages are cached, and any other cached
    /// page they touch is dropped.
    pub fn put_committed(&mut self, offset: u64, data: &Bytes) {
        self.observe_header(offset, data);
        let end = offset + data.len() as u64;
        self.invalidate(offset, end);
        if self.page_size == 0 {
            return;
        }
        let ps = self.page_size;
        let mut pos = offset.next_multiple_of(ps);
        while pos + ps <= end {
            let start = (pos - offset) as usize;
            self.insert(pos, &data[start..start + ps as usize]);
            pos += ps;
        }
    }

    /// Drops pages at or past `size`.
    pub fn truncate(&mut self, size: u64) {
        let doomed: Vec<u64> = self
            .entries
            .iter()
            .filter(|(k, e)| **k + e.len > size)
            .map(|(k, _)| *k)
            .collect();
        for key in doomed {
            self.remove(key);
        }
    }

    fn invalidate(&mut self, start: u64, end: u64) {
        let from = start.saturating_sub(65536);
        let doomed: Vec<u64> = self
            .entries
            .range(from..end)
            .filter(|(k, e)| **k + e.len > start)
            .map(|(k, _)| *k)
            .collect();
        for key in doomed {
            self.remove(key);
        }
    }

    fn touch(&mut self, key: u64) {
        if let Some(e) = self.entries.get_mut(&key) {
            self.recency.remove(&e.tick);
            e.tick = self.next_tick;
            self.recency.insert(self.next_tick, key);
            self.next_tick += 1;
        }
    }

    fn insert(&mut self, key: u64, data: &[u8]) {
        self.remove(key);
        let len = data.len() as u64;
        if len > self.max_bytes || !self.store.write(key, data) {
            self.store.remove(key);
            return;
        }
        let tick = self.next_tick;
        self.next_tick += 1;
        self.entries.insert(
            key,
            Entry {
                len,
                checksum: xxh3_64(data),
                tick,
            },
        );
        self.recency.insert(tick, key);
        self.bytes += len;
        while self.bytes > self.max_bytes {
            let Some((_, oldest)) = self.recency.pop_first() else {
                break;
            };
            self.remove(oldest);
        }
    }

    fn remove(&mut self, key: u64) {
        if let Some(e) = self.entries.remove(&key) {
            self.recency.remove(&e.tick);
            self.bytes -= e.len;
            self.store.remove(key);
        }
    }

    /// How many pages the cache holds.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
