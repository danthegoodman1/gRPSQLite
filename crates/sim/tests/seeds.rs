//! Runs a fixed range of VOPR seeds, checks that a seed replays exactly, and
//! replays seeds that once failed.

use grpsqlite_sim::vopr;

#[test]
fn seed_range() {
    let first: u64 = std::env::var("VOPR_FIRST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let count: u64 = std::env::var("VOPR_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    for seed in first..first + count {
        if let Err(e) = vopr::run(seed, false) {
            panic!("{e}");
        }
    }
}

#[test]
fn a_seed_replays_exactly() {
    for seed in [7, 38, 303] {
        let a = vopr::run(seed, false).map(|r| r.trace);
        let b = vopr::run(seed, false).map(|r| r.trace);
        assert_eq!(a, b, "seed {seed}");
    }
}

/// Seeds that found bugs, with the bug each one found. Seed 38 found that a
/// writer in exclusive locking mode lets replicas read a mix of versions;
/// servers with atomic commits now reject exclusive locking
/// (`r6_exclusive_locking_is_rejected_with_atomic_commits` in `repro.rs`).
#[test]
fn regressions() {
    for (seed, bug) in [(
        303,
        "a failed close left a lease that blocked the next open",
    )] {
        if let Err(e) = vopr::run(seed, false) {
            panic!("{bug}: {e}");
        }
    }
}
