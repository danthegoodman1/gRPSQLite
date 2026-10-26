//! SQLite's file locks among the connections of one process that share a
//! database. The lease already excludes other processes from writing.

use std::collections::BTreeMap;

use sqlite_plugin::flags::LockLevel;

#[derive(Debug, Default)]
pub struct LockTable {
    holders: BTreeMap<u64, LockLevel>,
}

/// Another connection holds a conflicting lock.
#[derive(Debug, PartialEq, Eq)]
pub struct Busy;

impl LockTable {
    pub fn level(&self, id: u64) -> LockLevel {
        self.holders
            .get(&id)
            .copied()
            .unwrap_or(LockLevel::Unlocked)
    }

    fn others_at_least(&self, id: u64, level: LockLevel) -> bool {
        self.holders.iter().any(|(h, l)| *h != id && *l >= level)
    }

    /// Raises `id`'s lock to `want`, following the rules of SQLite's unix VFS:
    /// a pending lock blocks new shared locks, and an exclusive lock waits in
    /// pending until every other shared lock is gone.
    pub fn lock(&mut self, id: u64, want: LockLevel) -> Result<(), Busy> {
        let have = self.level(id);
        if have >= want {
            return Ok(());
        }
        let granted = match want {
            LockLevel::Unlocked => LockLevel::Unlocked,
            LockLevel::Shared => {
                if self.others_at_least(id, LockLevel::Pending) {
                    return Err(Busy);
                }
                LockLevel::Shared
            }
            LockLevel::Reserved => {
                if self.others_at_least(id, LockLevel::Reserved) {
                    return Err(Busy);
                }
                LockLevel::Reserved
            }
            LockLevel::Pending | LockLevel::Exclusive => {
                if self.others_at_least(id, LockLevel::Reserved) {
                    return Err(Busy);
                }
                if self.others_at_least(id, LockLevel::Shared) {
                    self.holders.insert(id, LockLevel::Pending);
                    return Err(Busy);
                }
                want
            }
        };
        self.holders.insert(id, granted);
        Ok(())
    }

    /// Lowers `id`'s lock to `to`.
    pub fn unlock(&mut self, id: u64, to: LockLevel) {
        if self.level(id) <= to {
            return;
        }
        if to == LockLevel::Unlocked {
            self.holders.remove(&id);
        } else {
            self.holders.insert(id, to);
        }
    }

    /// Whether any connection holds a reserved lock or stronger.
    pub fn reserved(&self) -> bool {
        self.holders.values().any(|l| *l >= LockLevel::Reserved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use LockLevel::*;

    #[test]
    fn readers_share_writer_excludes() {
        let mut t = LockTable::default();
        assert_eq!(t.lock(1, Shared), Ok(()));
        assert_eq!(t.lock(2, Shared), Ok(()));
        assert_eq!(t.lock(1, Reserved), Ok(()));
        assert_eq!(t.lock(2, Reserved), Err(Busy));
        assert!(t.reserved());
        // Exclusive waits in pending while 2 reads, and pending blocks new readers.
        assert_eq!(t.lock(1, Exclusive), Err(Busy));
        assert_eq!(t.level(1), Pending);
        assert_eq!(t.lock(3, Shared), Err(Busy));
        t.unlock(2, Unlocked);
        assert_eq!(t.lock(1, Exclusive), Ok(()));
        t.unlock(1, Shared);
        assert!(!t.reserved());
        assert_eq!(t.lock(3, Shared), Ok(()));
    }
}
