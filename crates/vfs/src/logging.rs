//! Sends `log` records to `sqlite3_log`.

use sqlite_plugin::logger::{SqliteLogLevel, SqliteLogger};

struct Bridge {
    logger: SqliteLogger,
    level: log::LevelFilter,
}

impl log::Log for Bridge {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let level = match record.level() {
            log::Level::Error => SqliteLogLevel::Error,
            log::Level::Warn => SqliteLogLevel::Warn,
            _ => SqliteLogLevel::Notice,
        };
        self.logger
            .log(level, &format!("grpsqlite: {}", record.args()));
    }

    fn flush(&self) {}
}

/// Installs the bridge unless the program already set a logger, which then
/// keeps receiving the VFS's records.
pub fn install(logger: SqliteLogger, level: log::LevelFilter) {
    if log::set_boxed_logger(Box::new(Bridge { logger, level })).is_ok() {
        log::set_max_level(level);
    }
}
