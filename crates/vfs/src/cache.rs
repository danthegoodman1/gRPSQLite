//! The local page cache.
//!
//! A read-write database's cache is authoritative while this process holds
//! the lease: a cached page is served without a request.
//!
//! A read-only database's cache tracks the version its pages are current at.
//! When a read transaction starts at a newer version, the server's
//! ChangedPages list says which pages to drop, and the transaction then reads
//! cached pages without a request. On servers without ChangedPages, each read
//! sends the cached copy's checksum instead, and the server answers
//! "unchanged" or with the bytes.
//!
//! Every page served from the cache is checked against its checksum first,
//! so a damaged cache costs a request, never a wrong answer.
//!
//! A cache outlives its process. At a clean close it writes a manifest of its
//! pages. A read-write cache records the header's change counter, which
//! SQLite increments with every commit in rollback-journal modes; when the
//! database next opens, the pages count only if the server's header still
//! carries that counter. A read-only cache records its version. Opening
//! removes the manifest, so a crash leaves none and the next open starts
//! empty.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

use crate::config::CacheConfig;

/// Where one database's cached pages live, keyed by file offset, plus the
/// manifest written at close.
pub trait CacheStore: Send {
    fn read(&mut self, key: u64) -> Option<Vec<u8>>;
    /// Returns false when the page could not be stored.
    fn write(&mut self, key: u64, data: &[u8]) -> bool;
    fn remove(&mut self, key: u64);
    /// Removes every page and the manifest.
    fn clear(&mut self);
    fn read_manifest(&mut self) -> Option<Vec<u8>>;
    /// Returns false when the manifest could not be stored.
    fn write_manifest(&mut self, data: &[u8]) -> bool;
    fn remove_manifest(&mut self);
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
        let opened = std::fs::create_dir_all(&dir)
            .and_then(|()| File::create(dir.join("lock")))
            .and_then(|lock| {
                lock.try_lock()
                    .map(|()| lock)
                    .map_err(std::io::Error::other)
            });
        match opened {
            Ok(lock) => Some(Box::new(DiskStore { dir, _lock: lock })),
            Err(e) => {
                log::warn!(
                    "page cache disabled for {database}: {} is unavailable or used by another process: {e}",
                    dir.display()
                );
                None
            }
        }
    }
}

struct DiskStore {
    dir: PathBuf,
    /// Held for the store's lifetime, so two processes never share it.
    _lock: File,
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
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if entry.file_name() != "lock" {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }

    fn read_manifest(&mut self) -> Option<Vec<u8>> {
        std::fs::read(self.dir.join("manifest")).ok()
    }

    fn write_manifest(&mut self, data: &[u8]) -> bool {
        let tmp = self.dir.join("manifest.tmp");
        std::fs::write(&tmp, data).is_ok()
            && std::fs::rename(&tmp, self.dir.join("manifest")).is_ok()
    }

    fn remove_manifest(&mut self) {
        let _ = std::fs::remove_file(self.dir.join("manifest"));
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    len: u64,
    checksum: u64,
    /// When the page was last used, for least-recently-used eviction.
    tick: u64,
}

/// What a cache knew about its pages when it closed.
#[derive(Debug, PartialEq, Eq)]
struct Manifest {
    /// The header's change counter, for a read-write cache.
    counter: Option<[u8; 4]>,
    /// The version the pages are current at, for a read-only cache.
    version: Option<Bytes>,
    page_size: u64,
    /// `(offset, len, checksum)`, least recently used first.
    pages: Vec<(u64, u64, u64)>,
}

const MANIFEST_MAGIC: &[u8; 8] = b"GSQLCM01";

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32 + self.pages.len() * 24);
        out.extend_from_slice(MANIFEST_MAGIC);
        match self.counter {
            Some(c) => {
                out.push(1);
                out.extend_from_slice(&c);
            }
            None => out.extend_from_slice(&[0; 5]),
        }
        match &self.version {
            Some(v) => {
                out.push(1);
                out.extend_from_slice(&(v.len() as u64).to_le_bytes());
                out.extend_from_slice(v);
            }
            None => out.push(0),
        }
        out.extend_from_slice(&self.page_size.to_le_bytes());
        out.extend_from_slice(&(self.pages.len() as u64).to_le_bytes());
        for (offset, len, checksum) in &self.pages {
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&checksum.to_le_bytes());
        }
        out
    }

    fn decode(data: &[u8]) -> Option<Manifest> {
        let rest = data.strip_prefix(MANIFEST_MAGIC)?;
        let (flag, rest) = rest.split_first()?;
        let (counter, rest) = rest.split_first_chunk::<4>()?;
        let counter = match flag {
            0 => None,
            1 => Some(*counter),
            _ => return None,
        };
        let (flag, rest) = rest.split_first()?;
        let (version, rest) = match flag {
            0 => (None, rest),
            1 => {
                let (len, rest) = rest.split_first_chunk::<8>()?;
                let len = usize::try_from(u64::from_le_bytes(*len)).ok()?;
                let (v, rest) = rest.split_at_checked(len)?;
                (Some(Bytes::copy_from_slice(v)), rest)
            }
            _ => return None,
        };
        let (page_size, rest) = rest.split_first_chunk::<8>()?;
        let (count, mut rest) = rest.split_first_chunk::<8>()?;
        let count = u64::from_le_bytes(*count);
        if rest.len() as u64 != count.checked_mul(24)? {
            return None;
        }
        let mut pages = Vec::with_capacity(count as usize);
        while let Some((page, tail)) = rest.split_first_chunk::<24>() {
            let field =
                |i: usize| u64::from_le_bytes(page[i * 8..i * 8 + 8].try_into().unwrap_or([0; 8]));
            pages.push((field(0), field(1), field(2)));
            rest = tail;
        }
        Some(Manifest {
            counter,
            version,
            page_size: u64::from_le_bytes(*page_size),
            pages,
        })
    }
}

pub struct PageCache {
    store: Box<dyn CacheStore>,
    readonly: bool,
    /// Whether pages may outlive the process: always for read-only caches,
    /// which verify every page with the server, and for read-write caches
    /// only when every commit updates the header's change counter.
    reusable: bool,
    entries: BTreeMap<u64, Entry>,
    /// Keys by last use, oldest first.
    recency: BTreeMap<u64, u64>,
    next_tick: u64,
    bytes: u64,
    max_bytes: u64,
    /// The database's page size, from its header; 0 until known.
    page_size: u64,
    /// A read-write manifest waiting for the server's header.
    pending: Option<Manifest>,
    /// Read-only: the version every cached page is current at, if known.
    valid_at: Option<Bytes>,
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

/// The header's change counter.
fn header_counter(data: &[u8]) -> Option<[u8; 4]> {
    data.get(24..28)?.try_into().ok()
}

impl PageCache {
    /// Opens the cache. `reusable` says whether pages from an earlier
    /// process may be used once the header confirms them.
    pub fn open(
        settings: &CacheSettings,
        database: &str,
        readonly: bool,
        reusable: bool,
    ) -> Option<PageCache> {
        let mut store = settings.stores.open(database, readonly)?;
        let manifest = store.read_manifest().and_then(|m| Manifest::decode(&m));
        store.remove_manifest();
        let mut cache = PageCache {
            store,
            readonly,
            reusable,
            entries: BTreeMap::new(),
            recency: BTreeMap::new(),
            next_tick: 0,
            bytes: 0,
            max_bytes: settings.max_bytes,
            page_size: 0,
            pending: None,
            valid_at: None,
        };
        match manifest {
            Some(m) if reusable && readonly => cache.adopt(m),
            Some(m) if reusable && m.counter.is_some() => cache.pending = Some(m),
            _ => cache.store.clear(),
        }
        Some(cache)
    }

    fn adopt(&mut self, m: Manifest) {
        self.page_size = m.page_size;
        self.valid_at = m.version;
        for (offset, len, checksum) in m.pages {
            let tick = self.next_tick;
            self.next_tick += 1;
            self.entries.insert(
                offset,
                Entry {
                    len,
                    checksum,
                    tick,
                },
            );
            self.recency.insert(tick, offset);
            self.bytes += len;
        }
        self.evict();
    }

    /// Writes the manifest. `header` is the committed header a read-write
    /// cache's pages belong to.
    pub fn close(mut self, header: Option<&[u8]>) {
        if !self.reusable || self.entries.is_empty() || self.pending.is_some() {
            self.store.clear();
            return;
        }
        let counter = if self.readonly {
            None
        } else {
            match header.and_then(header_counter) {
                Some(c) => Some(c),
                None => {
                    self.store.clear();
                    return;
                }
            }
        };
        let manifest = Manifest {
            counter,
            version: self.valid_at.clone(),
            page_size: self.page_size,
            pages: self
                .recency
                .values()
                .filter_map(|k| self.entries.get(k).map(|e| (*k, e.len, e.checksum)))
                .collect(),
        };
        if !self.store.write_manifest(&manifest.encode()) {
            self.store.clear();
        }
    }

    pub fn clear(&mut self) {
        self.store.clear();
        self.entries.clear();
        self.recency.clear();
        self.bytes = 0;
        self.pending = None;
        self.valid_at = None;
    }

    /// The version every cached page is current at, if known.
    pub fn valid_at(&self) -> Option<&Bytes> {
        self.valid_at.as_ref()
    }

    /// Makes the cache current at `version` by dropping the byte ranges in
    /// `changed`, which lists everything that differs from the version the
    /// cache was current at. `None` means the differences are unknown and
    /// empties the cache.
    pub fn advance_to(&mut self, version: Bytes, changed: Option<&[(u64, u64)]>) {
        match changed {
            Some(ranges) => {
                for &(offset, len) in ranges {
                    self.invalidate(offset, offset.saturating_add(len));
                }
            }
            None => self.clear(),
        }
        self.valid_at = Some(version);
    }

    /// Learns the page size from a header the server sent, and decides a
    /// pending manifest; a change of page size empties the cache.
    fn observe_header(&mut self, offset: u64, data: &[u8], from_server: bool) {
        if offset != 0 {
            return;
        }
        if let Some(m) = self.pending.take() {
            let confirmed = from_server
                && header_counter(data).is_some_and(|c| Some(c) == m.counter)
                && header_page_size(data) == Some(m.page_size);
            if confirmed {
                self.adopt(m);
            } else {
                self.store.clear();
            }
        }
        if let Some(size) = header_page_size(data)
            && size != self.page_size
        {
            // Pages exist only once the page size is known.
            if self.page_size != 0 {
                self.clear();
            }
            self.page_size = size;
        }
    }

    fn is_page(&self, offset: u64, len: u64) -> bool {
        self.page_size != 0 && len == self.page_size && offset.is_multiple_of(self.page_size)
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
        self.observe_header(offset, &data, true);
        if self.is_page(offset, requested) && data.len() as u64 == requested {
            self.insert(offset, &data);
        }
    }

    /// Records committed bytes: whole pages are cached, and any other cached
    /// page they touch is dropped.
    pub fn put_committed(&mut self, offset: u64, data: &Bytes) {
        self.observe_header(offset, data, false);
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
        if self.pending.take().is_some() {
            self.store.clear();
        }
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
        self.evict();
    }

    fn evict(&mut self) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    #[derive(Default)]
    struct Mem {
        pages: BTreeMap<u64, Vec<u8>>,
        manifest: Option<Vec<u8>>,
    }

    #[derive(Clone, Default)]
    struct Shared(Arc<Mutex<Mem>>);

    impl CacheStore for Shared {
        fn read(&mut self, key: u64) -> Option<Vec<u8>> {
            self.0.lock().pages.get(&key).cloned()
        }
        fn write(&mut self, key: u64, data: &[u8]) -> bool {
            self.0.lock().pages.insert(key, data.to_vec());
            true
        }
        fn remove(&mut self, key: u64) {
            self.0.lock().pages.remove(&key);
        }
        fn clear(&mut self) {
            *self.0.lock() = Mem::default();
        }
        fn read_manifest(&mut self) -> Option<Vec<u8>> {
            self.0.lock().manifest.clone()
        }
        fn write_manifest(&mut self, data: &[u8]) -> bool {
            self.0.lock().manifest = Some(data.to_vec());
            true
        }
        fn remove_manifest(&mut self) {
            self.0.lock().manifest = None;
        }
    }

    struct One(Shared);

    impl CacheStores for One {
        fn open(&self, _: &str, _: bool) -> Option<Box<dyn CacheStore>> {
            Some(Box::new(self.0.clone()))
        }
    }

    fn header(counter: u32) -> Vec<u8> {
        let mut h = vec![0u8; 512];
        h[..16].copy_from_slice(b"SQLite format 3\0");
        h[16..18].copy_from_slice(&512u16.to_be_bytes());
        h[24..28].copy_from_slice(&counter.to_be_bytes());
        h
    }

    fn settings(store: &Shared) -> CacheSettings {
        CacheSettings {
            stores: Arc::new(One(store.clone())),
            max_bytes: 1 << 20,
        }
    }

    /// A writer's cache with page 1 and page 2 cached, closed at `counter`.
    fn closed_writer(store: &Shared, counter: u32) {
        let mut c =
            PageCache::open(&settings(store), "db", false, true).unwrap_or_else(|| unreachable!());
        c.put_read(0, 512, Bytes::from(header(counter)));
        c.put_read(512, 512, Bytes::from(vec![7u8; 512]));
        c.close(Some(&header(counter)));
    }

    #[test]
    fn manifest_round_trips() {
        let writer = Manifest {
            counter: Some([1, 2, 3, 4]),
            version: None,
            page_size: 4096,
            pages: vec![(0, 4096, 9), (8192, 4096, 10)],
        };
        let reader = Manifest {
            counter: None,
            version: Some(Bytes::from_static(b"v42")),
            page_size: 512,
            pages: vec![(512, 512, 11)],
        };
        for m in [writer, reader] {
            assert_eq!(Manifest::decode(&m.encode()), Some(m));
        }
        assert_eq!(Manifest::decode(b"GSQLCM01garbage"), None);
    }

    fn reader_cache(store: &Shared) -> PageCache {
        PageCache::open(&settings(store), "db", true, true).unwrap_or_else(|| unreachable!())
    }

    #[test]
    fn learning_the_page_size_keeps_the_version() {
        let store = Shared::default();
        let mut c = reader_cache(&store);
        c.advance_to(Bytes::from_static(b"v1"), None);
        c.put_read(0, 512, Bytes::from(header(1)));
        assert_eq!(c.valid_at(), Some(&Bytes::from_static(b"v1")));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn advancing_drops_only_changed_pages() {
        let store = Shared::default();
        let mut c = reader_cache(&store);
        c.put_read(0, 512, Bytes::from(header(1)));
        for page in 1..4u8 {
            c.put_read(page as u64 * 512, 512, Bytes::from(vec![page; 512]));
        }
        c.advance_to(Bytes::from_static(b"v1"), Some(&[]));
        c.advance_to(Bytes::from_static(b"v2"), Some(&[(1024, 10)]));
        assert_eq!(c.valid_at(), Some(&Bytes::from_static(b"v2")));
        let mut buf = [0u8; 512];
        assert_eq!(c.read(512, &mut buf), Some(512));
        assert_eq!(c.read(1024, &mut buf), None, "a changed page is dropped");
        assert_eq!(c.read(1536, &mut buf), Some(512));
        c.advance_to(Bytes::from_static(b"v3"), None);
        assert!(c.is_empty(), "unknown changes empty the cache");
        assert_eq!(c.valid_at(), Some(&Bytes::from_static(b"v3")));
    }

    #[test]
    fn reader_keeps_its_version_across_restarts() {
        let store = Shared::default();
        let mut c = reader_cache(&store);
        c.put_read(0, 512, Bytes::from(header(1)));
        c.put_read(512, 512, Bytes::from(vec![7u8; 512]));
        c.advance_to(Bytes::from_static(b"v9"), Some(&[]));
        c.close(None);
        let mut c = reader_cache(&store);
        assert_eq!(c.valid_at(), Some(&Bytes::from_static(b"v9")));
        let mut buf = [0u8; 512];
        assert_eq!(c.read(512, &mut buf), Some(512));
        assert_eq!(buf, [7u8; 512]);
    }

    #[test]
    fn writer_reuses_pages_when_the_counter_matches() {
        let store = Shared::default();
        closed_writer(&store, 5);
        let mut c =
            PageCache::open(&settings(&store), "db", false, true).unwrap_or_else(|| unreachable!());
        let mut buf = [0u8; 512];
        assert_eq!(
            c.read(512, &mut buf),
            None,
            "unconfirmed until the header arrives"
        );
        c.put_read(0, 100, Bytes::from(header(5)[..100].to_vec()));
        assert_eq!(c.read(512, &mut buf), Some(512));
        assert_eq!(buf, [7u8; 512]);
    }

    #[test]
    fn writer_drops_pages_when_the_counter_moved() {
        let store = Shared::default();
        closed_writer(&store, 5);
        let mut c =
            PageCache::open(&settings(&store), "db", false, true).unwrap_or_else(|| unreachable!());
        c.put_read(0, 100, Bytes::from(header(6)[..100].to_vec()));
        let mut buf = [0u8; 512];
        assert_eq!(c.read(512, &mut buf), None);
        assert!(store.0.lock().pages.is_empty());
    }

    #[test]
    fn a_crash_leaves_no_manifest() {
        let store = Shared::default();
        closed_writer(&store, 5);
        // Opening consumes the manifest; dropping without close is a crash.
        drop(PageCache::open(&settings(&store), "db", false, true));
        let c =
            PageCache::open(&settings(&store), "db", false, true).unwrap_or_else(|| unreachable!());
        assert!(c.pending.is_none() && c.is_empty());
    }

    #[test]
    fn unreusable_caches_start_empty() {
        let store = Shared::default();
        closed_writer(&store, 5);
        let c = PageCache::open(&settings(&store), "db", false, false)
            .unwrap_or_else(|| unreachable!());
        assert!(c.pending.is_none() && c.is_empty());
        assert!(store.0.lock().pages.is_empty());
    }
}
