use alloc::ffi::CString;
use core::ffi::{c_char, c_int};

use crate::vars;

#[allow(non_snake_case)]
type Sqlite3Log = unsafe extern "C" fn(iErrCode: c_int, arg2: *const c_char, ...);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SqliteLogLevel {
    Error = 1,
    Warn,
    Notice,
}

impl SqliteLogLevel {
    fn into_err_code(self) -> c_int {
        match self {
            Self::Notice => vars::SQLITE_NOTICE,
            Self::Warn => vars::SQLITE_WARNING,
            Self::Error => vars::SQLITE_INTERNAL,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SqliteLogger {
    log: Sqlite3Log,
}

impl SqliteLogger {
    pub(crate) fn new(log: Sqlite3Log) -> Self {
        Self { log }
    }

    /// Log bytes directly to the `SQLite3` log handle.
    /// Note that `SQLite` silently truncates writes larger than
    /// roughly 230 bytes by default. It's recommended that you
    /// split your log messages by lines before calling this method.
    pub fn log(&self, level: SqliteLogLevel, msg: &str) {
        let code = level.into_err_code();
        let z_format = CString::new(msg).unwrap();
        unsafe { (self.log)(code, z_format.as_ptr()) }
    }
}
