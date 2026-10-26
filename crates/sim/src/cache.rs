//! An in-memory page-cache store with injected damage. One
//! [`SimCacheStores`] is one machine's disk: its pages and manifests survive
//! the client's reopens and crashes.

use std::collections::BTreeMap;
use std::sync::Arc;

use grpsqlite::cache::{CacheSettings, CacheStore, CacheStores};
use parking_lot::Mutex;

use crate::prng::Prng;

/// Damage rates, per thousand operations.
#[derive(Clone, Debug, Default)]
pub struct CacheFaults {
    /// A read returns the page with one bit flipped.
    pub bit_flip: u64,
    /// A read finds the page (or the manifest) missing.
    pub lost: u64,
    /// A write fails.
    pub failed_write: u64,
}

#[derive(Default)]
struct Disk {
    pages: BTreeMap<u64, Vec<u8>>,
    manifest: Option<Vec<u8>>,
}

/// One disk per database and read-only flag.
type Disks = BTreeMap<(String, bool), Arc<Mutex<Disk>>>;

pub struct SimCacheStores {
    faults: CacheFaults,
    rng: Arc<Mutex<Prng>>,
    disks: Mutex<Disks>,
}

impl SimCacheStores {
    pub fn settings(faults: CacheFaults, rng: Prng, max_bytes: u64) -> CacheSettings {
        CacheSettings {
            stores: Arc::new(SimCacheStores {
                faults,
                rng: Arc::new(Mutex::new(rng)),
                disks: Mutex::new(BTreeMap::new()),
            }),
            max_bytes,
        }
    }
}

impl CacheStores for SimCacheStores {
    fn open(&self, database: &str, readonly: bool) -> Option<Box<dyn CacheStore>> {
        let disk = self
            .disks
            .lock()
            .entry((database.to_string(), readonly))
            .or_default()
            .clone();
        Some(Box::new(MemStore {
            disk,
            faults: self.faults.clone(),
            rng: self.rng.clone(),
        }))
    }
}

struct MemStore {
    disk: Arc<Mutex<Disk>>,
    faults: CacheFaults,
    rng: Arc<Mutex<Prng>>,
}

impl CacheStore for MemStore {
    fn read(&mut self, key: u64) -> Option<Vec<u8>> {
        let mut rng = self.rng.lock();
        if rng.per_mille(self.faults.lost) {
            return None;
        }
        let mut page = self.disk.lock().pages.get(&key)?.clone();
        if !page.is_empty() && rng.per_mille(self.faults.bit_flip) {
            let bit = rng.index(page.len() * 8);
            page[bit / 8] ^= 1 << (bit % 8);
        }
        Some(page)
    }

    fn write(&mut self, key: u64, data: &[u8]) -> bool {
        if self.rng.lock().per_mille(self.faults.failed_write) {
            self.disk.lock().pages.remove(&key);
            return false;
        }
        self.disk.lock().pages.insert(key, data.to_vec());
        true
    }

    fn remove(&mut self, key: u64) {
        self.disk.lock().pages.remove(&key);
    }

    fn clear(&mut self) {
        *self.disk.lock() = Disk::default();
    }

    fn read_manifest(&mut self) -> Option<Vec<u8>> {
        if self.rng.lock().per_mille(self.faults.lost) {
            return None;
        }
        self.disk.lock().manifest.clone()
    }

    fn write_manifest(&mut self, data: &[u8]) -> bool {
        if self.rng.lock().per_mille(self.faults.failed_write) {
            return false;
        }
        self.disk.lock().manifest = Some(data.to_vec());
        true
    }

    fn remove_manifest(&mut self) {
        self.disk.lock().manifest = None;
    }
}
