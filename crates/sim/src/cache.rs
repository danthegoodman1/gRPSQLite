//! An in-memory page-cache store with injected damage.

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
    /// A read finds the page missing.
    pub lost: u64,
    /// A write fails.
    pub failed_write: u64,
}

pub struct SimCacheStores {
    faults: CacheFaults,
    rng: Arc<Mutex<Prng>>,
}

impl SimCacheStores {
    pub fn settings(faults: CacheFaults, rng: Prng, max_bytes: u64) -> CacheSettings {
        CacheSettings {
            stores: Arc::new(SimCacheStores {
                faults,
                rng: Arc::new(Mutex::new(rng)),
            }),
            max_bytes,
        }
    }
}

impl CacheStores for SimCacheStores {
    fn open(&self, _database: &str, _readonly: bool) -> Option<Box<dyn CacheStore>> {
        Some(Box::new(MemStore {
            pages: BTreeMap::new(),
            faults: self.faults.clone(),
            rng: self.rng.clone(),
        }))
    }
}

struct MemStore {
    pages: BTreeMap<u64, Vec<u8>>,
    faults: CacheFaults,
    rng: Arc<Mutex<Prng>>,
}

impl CacheStore for MemStore {
    fn read(&mut self, key: u64) -> Option<Vec<u8>> {
        let mut rng = self.rng.lock();
        if rng.per_mille(self.faults.lost) {
            return None;
        }
        let mut page = self.pages.get(&key)?.clone();
        if !page.is_empty() && rng.per_mille(self.faults.bit_flip) {
            let bit = rng.index(page.len() * 8);
            page[bit / 8] ^= 1 << (bit % 8);
        }
        Some(page)
    }

    fn write(&mut self, key: u64, data: &[u8]) -> bool {
        if self.rng.lock().per_mille(self.faults.failed_write) {
            self.pages.remove(&key);
            return false;
        }
        self.pages.insert(key, data.to_vec());
        true
    }

    fn remove(&mut self, key: u64) {
        self.pages.remove(&key);
    }

    fn clear(&mut self) {
        self.pages.clear();
    }
}
