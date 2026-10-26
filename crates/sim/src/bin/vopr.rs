//! Runs VOPR seeds.
//!
//! ```text
//! vopr [SEED] [--seeds N] [--threads N] [--verbose]
//! ```
//!
//! With a seed, runs that seed and the following ones; without, starts from a
//! random seed. Exits nonzero on the first failure and prints its seed.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use grpsqlite_sim::vopr;

fn main() {
    let mut seed = None;
    let mut seeds = 1u64;
    let mut threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut verbose = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| -> u64 {
            args.next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| usage(&format!("{name} needs a number")))
        };
        match arg.as_str() {
            "--seeds" => seeds = value("--seeds"),
            "--threads" => threads = value("--threads") as usize,
            "--verbose" | "-v" => verbose = true,
            s => {
                seed = Some(
                    s.parse()
                        .unwrap_or_else(|_| usage(&format!("bad seed {s}"))),
                )
            }
        }
    }
    let first = seed.unwrap_or_else(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        nanos ^ (std::process::id() as u64) << 32
    });
    grpsqlite_sim::harness::seed_sqlite(first);
    let next = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicBool::new(false));
    let totals = Arc::new(Mutex::new(vopr::Report::default()));
    let handles: Vec<_> = (0..threads.min(seeds as usize).max(1))
        .map(|_| {
            let (next, failed, totals) = (next.clone(), failed.clone(), totals.clone());
            std::thread::spawn(move || {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= seeds || failed.load(Ordering::Relaxed) {
                        return;
                    }
                    let seed = first.wrapping_add(i);
                    let result = std::panic::catch_unwind(|| vopr::run(seed, verbose));
                    match result {
                        Ok(Ok(report)) => {
                            let mut t = totals.lock().unwrap_or_else(|e| e.into_inner());
                            t.steps += report.steps;
                            t.commits += report.commits;
                            t.ambiguous_commits += report.ambiguous_commits;
                            t.crashes += report.crashes;
                            t.takeovers += report.takeovers;
                            t.reader_checks += report.reader_checks;
                            if verbose || seeds == 1 {
                                println!("seed {seed}: ok {report:?}");
                            }
                        }
                        Ok(Err(msg)) => {
                            failed.store(true, Ordering::Relaxed);
                            eprintln!("FAILED {msg}\nreplay: cargo run --release -p grpsqlite-sim --bin vopr -- {seed} --verbose");
                        }
                        Err(_) => {
                            failed.store(true, Ordering::Relaxed);
                            eprintln!("FAILED seed {seed} panicked\nreplay: cargo run --release -p grpsqlite-sim --bin vopr -- {seed} --verbose");
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
    let t = totals.lock().unwrap_or_else(|e| e.into_inner());
    println!(
        "seeds {first}..{}: steps={} commits={} ambiguous={} crashes={} takeovers={} reader_checks={}",
        first.wrapping_add(seeds),
        t.steps,
        t.commits,
        t.ambiguous_commits,
        t.crashes,
        t.takeovers,
        t.reader_checks
    );
    if failed.load(Ordering::Relaxed) {
        std::process::exit(1);
    }
}

fn usage(msg: &str) -> ! {
    eprintln!("{msg}\nusage: vopr [SEED] [--seeds N] [--threads N] [--verbose]");
    std::process::exit(2);
}
