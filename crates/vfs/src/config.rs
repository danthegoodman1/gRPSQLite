//! Configuration, read from `GRPSQLITE_*` environment variables.

use std::path::PathBuf;
use std::time::Duration;

/// Settings the VFS core uses.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// Sent with every `Open`; from `GRPSQLITE_CLIENT_TOKEN`.
    pub client_token: String,
    /// The local page cache, when enabled.
    pub cache: Option<CacheConfig>,
}

/// A local page cache: `GRPSQLITE_CACHE_DIR` and `GRPSQLITE_CACHE_MAX_BYTES`,
/// which must be set together.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub dir: PathBuf,
    /// The most bytes each open database may cache.
    pub max_bytes: u64,
}

/// Settings for [`crate::GrpcBackend`].
#[derive(Clone, Debug)]
pub struct GrpcConfig {
    /// `GRPSQLITE_URL`, default `http://localhost:50051`.
    pub url: String,
    /// `GRPSQLITE_CONNECT_TIMEOUT_MS`, default 10000.
    pub connect_timeout: Duration,
    /// `GRPSQLITE_RPC_TIMEOUT_MS`, default 30000.
    pub rpc_timeout: Duration,
    /// `GRPSQLITE_KEEPALIVE_INTERVAL_MS`, default 10000.
    pub keepalive_interval: Duration,
    /// `GRPSQLITE_KEEPALIVE_TIMEOUT_MS`, default 20000.
    pub keepalive_timeout: Duration,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        GrpcConfig {
            url: "http://localhost:50051".to_string(),
            connect_timeout: Duration::from_secs(10),
            rpc_timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(10),
            keepalive_timeout: Duration::from_secs(20),
        }
    }
}

/// Everything the environment configures.
#[derive(Clone, Debug)]
pub struct EnvConfig {
    pub vfs: Config,
    pub grpc: GrpcConfig,
    /// `GRPSQLITE_LOG`: off, error, warn (default), info, debug or trace.
    pub log_level: log::LevelFilter,
}

impl EnvConfig {
    pub fn from_env() -> Result<EnvConfig, String> {
        EnvConfig::from_lookup(|name| std::env::var(name).ok())
    }

    /// Parses configuration from `lookup`, which maps variable names to values.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<EnvConfig, String> {
        let millis = |name: &str, default: Duration| -> Result<Duration, String> {
            match lookup(name) {
                None => Ok(default),
                Some(v) => v
                    .parse::<u64>()
                    .map(Duration::from_millis)
                    .map_err(|_| format!("{name}={v:?} is not a number of milliseconds")),
            }
        };
        let defaults = GrpcConfig::default();
        let grpc = GrpcConfig {
            url: lookup("GRPSQLITE_URL").unwrap_or(defaults.url),
            connect_timeout: millis("GRPSQLITE_CONNECT_TIMEOUT_MS", defaults.connect_timeout)?,
            rpc_timeout: millis("GRPSQLITE_RPC_TIMEOUT_MS", defaults.rpc_timeout)?,
            keepalive_interval: millis(
                "GRPSQLITE_KEEPALIVE_INTERVAL_MS",
                defaults.keepalive_interval,
            )?,
            keepalive_timeout: millis(
                "GRPSQLITE_KEEPALIVE_TIMEOUT_MS",
                defaults.keepalive_timeout,
            )?,
        };
        let cache = match (
            lookup("GRPSQLITE_CACHE_DIR"),
            lookup("GRPSQLITE_CACHE_MAX_BYTES"),
        ) {
            (None, None) => None,
            (Some(dir), Some(max)) => {
                let max_bytes = max.parse::<u64>().map_err(|_| {
                    format!("GRPSQLITE_CACHE_MAX_BYTES={max:?} is not a number of bytes")
                })?;
                if dir.is_empty() || max_bytes == 0 {
                    return Err(
                        "GRPSQLITE_CACHE_DIR must be a path and GRPSQLITE_CACHE_MAX_BYTES positive"
                            .into(),
                    );
                }
                Some(CacheConfig {
                    dir: PathBuf::from(dir),
                    max_bytes,
                })
            }
            _ => {
                return Err(
                    "set GRPSQLITE_CACHE_DIR and GRPSQLITE_CACHE_MAX_BYTES together".into(),
                );
            }
        };
        let log_level = match lookup("GRPSQLITE_LOG") {
            None => log::LevelFilter::Warn,
            Some(v) => v
                .parse()
                .map_err(|_| format!("GRPSQLITE_LOG={v:?} is not a log level"))?,
        };
        Ok(EnvConfig {
            vfs: Config {
                client_token: lookup("GRPSQLITE_CLIENT_TOKEN").unwrap_or_default(),
                cache,
            },
            grpc,
            log_level,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn parse(vars: &[(&str, &str)]) -> Result<EnvConfig, String> {
        let map: BTreeMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        EnvConfig::from_lookup(|name| map.get(name).cloned())
    }

    #[test]
    fn defaults() {
        let c = parse(&[]).unwrap_or_else(|e| unreachable!("{e}"));
        assert!(c.vfs.cache.is_none());
        assert_eq!(c.grpc.url, "http://localhost:50051");
        assert_eq!(c.log_level, log::LevelFilter::Warn);
    }

    #[test]
    fn cache_settings_go_together() {
        assert!(parse(&[("GRPSQLITE_CACHE_DIR", "/tmp/c")]).is_err());
        assert!(parse(&[("GRPSQLITE_CACHE_MAX_BYTES", "100")]).is_err());
        assert!(
            parse(&[
                ("GRPSQLITE_CACHE_DIR", "/tmp/c"),
                ("GRPSQLITE_CACHE_MAX_BYTES", "x")
            ])
            .is_err()
        );
        let c = parse(&[
            ("GRPSQLITE_CACHE_DIR", "/tmp/c"),
            ("GRPSQLITE_CACHE_MAX_BYTES", "100"),
        ]);
        assert!(c.is_ok_and(|c| c.vfs.cache.is_some_and(|c| c.max_bytes == 100)));
    }

    #[test]
    fn rejects_bad_numbers() {
        assert!(parse(&[("GRPSQLITE_RPC_TIMEOUT_MS", "soon")]).is_err());
        assert!(parse(&[("GRPSQLITE_LOG", "loud")]).is_err());
    }
}
