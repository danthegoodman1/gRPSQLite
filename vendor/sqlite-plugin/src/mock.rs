#![cfg(test)]

// tests use std
extern crate std;

use core::fmt::{self, Display};
use std::boxed::Box;
use std::collections::HashMap;
use std::println;
use std::{string::String, vec::Vec};

use alloc::borrow::{Cow, ToOwned};
use alloc::format;
use alloc::sync::Arc;
use parking_lot::{Mutex, MutexGuard};

use crate::flags::{self, AccessFlags, LockLevel, OpenOpts};
use crate::logger::{SqliteLogLevel, SqliteLogger};
use crate::vars;
use crate::vfs::{
    DEFAULT_DEVICE_CHARACTERISTICS, DEFAULT_SECTOR_SIZE, Pragma, PragmaErr, Vfs, VfsHandle,
    VfsResult,
};

pub struct File {
    pub name: String,
    pub data: Vec<u8>,
    pub delete_on_close: bool,
}

#[allow(unused_variables)]
pub trait Hooks {
    fn canonical_path(&mut self, path: &str) {}
    fn open(&mut self, path: &Option<&str>, opts: &OpenOpts) {}
    fn delete(&mut self, path: &str) {}
    fn access(&mut self, path: &str, flags: AccessFlags) {}
    fn file_size(&mut self, handle: MockHandle) {}
    fn truncate(&mut self, handle: MockHandle, size: usize) {}
    fn write(&mut self, handle: MockHandle, offset: usize, buf: &[u8]) {}
    fn read(&mut self, handle: MockHandle, offset: usize, buf: &[u8]) {}
    fn sync(&mut self, handle: MockHandle) {}
    fn close(&mut self, handle: MockHandle) {}
    fn pragma(
        &mut self,
        handle: MockHandle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        Err(PragmaErr::NotFound)
    }
    fn sector_size(&mut self, handle: MockHandle) {}
    fn device_characteristics(&mut self, handle: MockHandle) {
        println!("device_characteristics");
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MockHandle {
    id: usize,
    readonly: bool,
}

impl Display for MockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockHandle({})", self.id)
    }
}

impl MockHandle {
    pub fn new(id: usize, readonly: bool) -> Self {
        Self { id, readonly }
    }
}

impl VfsHandle for MockHandle {
    fn readonly(&self) -> bool {
        self.readonly
    }

    fn in_memory(&self) -> bool {
        false
    }
}

// MockVfs implements a very simple in-memory VFS for testing purposes.
// See the memvfs example for a more complete implementation.
pub struct MockVfs {
    state: Arc<Mutex<MockState>>,
}

pub struct MockState {
    next_id: usize,
    files: HashMap<MockHandle, File>,
    hooks: Box<dyn Hooks + Send>,
    log: Option<SqliteLogger>,
}

impl MockState {
    pub fn new(hooks: Box<dyn Hooks + Send>) -> Self {
        MockState {
            next_id: 0,
            files: HashMap::new(),
            hooks,
            log: None,
        }
    }

    pub fn setup_logger(&mut self, logger: SqliteLogger) {
        self.log = Some(logger)
    }
}

impl MockVfs {
    pub fn new(state: Arc<Mutex<MockState>>) -> Self {
        Self { state }
    }

    fn state(&self) -> MutexGuard<'_, MockState> {
        self.state.lock()
    }
}

impl MockState {
    fn log(&self, f: fmt::Arguments<'_>) {
        if let Some(log) = &self.log {
            log.log(SqliteLogLevel::Notice, &format!("{f}"));
        } else {
            panic!("MockVfs is missing registered log handler")
        }
    }

    fn next_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

impl Vfs for MockVfs {
    // a simple usize that represents a file handle.
    type Handle = MockHandle;

    fn canonical_path<'a>(&self, path: Cow<'a, str>) -> VfsResult<Cow<'a, str>> {
        let mut state = self.state();
        state.log(format_args!("canonical_path: path={path:?}"));
        state.hooks.canonical_path(&path);
        Ok(path)
    }

    fn open(&self, path: Option<&str>, opts: flags::OpenOpts) -> VfsResult<Self::Handle> {
        let mut state = self.state();
        state.log(format_args!("open: path={path:?} opts={opts:?}"));
        state.hooks.open(&path, &opts);

        let id = state.next_id();
        let file_handle = MockHandle::new(id, opts.mode().is_readonly());

        if let Some(path) = path {
            // if file is already open return existing handle
            for (handle, file) in state.files.iter() {
                if file.name == path {
                    return Ok(*handle);
                }
            }
            state.files.insert(
                file_handle,
                File {
                    name: path.to_owned(),
                    data: Vec::new(),
                    delete_on_close: opts.delete_on_close(),
                },
            );
        }
        Ok(file_handle)
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        let mut state = self.state();
        state.log(format_args!("delete: path={path:?}"));
        state.hooks.delete(path);
        state.files.retain(|_, file| file.name != path);
        Ok(())
    }

    fn access(&self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        let mut state = self.state();
        state.log(format_args!("access: path={path:?} flags={flags:?}"));
        state.hooks.access(path, flags);
        Ok(state.files.values().any(|file| file.name == path))
    }

    fn file_size(&self, meta: &mut Self::Handle) -> VfsResult<usize> {
        let mut state = self.state();
        state.log(format_args!("file_size: handle={meta:?}"));
        state.hooks.file_size(*meta);
        Ok(state.files.get(meta).map_or(0, |file| file.data.len()))
    }

    fn truncate(&self, meta: &mut Self::Handle, size: usize) -> VfsResult<()> {
        let mut state = self.state();
        state.log(format_args!("truncate: handle={meta:?} size={size:?}"));
        state.hooks.truncate(*meta, size);
        if let Some(file) = state.files.get_mut(meta) {
            if size > file.data.len() {
                file.data.resize(size, 0);
            } else {
                file.data.truncate(size);
            }
        }
        Ok(())
    }

    fn write(&self, meta: &mut Self::Handle, offset: usize, buf: &[u8]) -> VfsResult<usize> {
        let mut state = self.state();
        state.log(format_args!(
            "write: handle={:?} offset={:?} buf.len={}",
            meta,
            offset,
            buf.len()
        ));
        state.hooks.write(*meta, offset, buf);
        if let Some(file) = state.files.get_mut(meta) {
            if offset + buf.len() > file.data.len() {
                file.data.resize(offset + buf.len(), 0);
            }
            file.data[offset..offset + buf.len()].copy_from_slice(buf);
            Ok(buf.len())
        } else {
            Err(vars::SQLITE_IOERR_WRITE)
        }
    }

    fn read(&self, meta: &mut Self::Handle, offset: usize, buf: &mut [u8]) -> VfsResult<usize> {
        let mut state = self.state();
        state.log(format_args!(
            "read: handle={:?} offset={:?} buf.len={}",
            meta,
            offset,
            buf.len()
        ));
        state.hooks.read(*meta, offset, buf);
        if let Some(file) = state.files.get(meta) {
            if offset > file.data.len() {
                return Ok(0);
            }
            let len = buf.len().min(file.data.len() - offset);
            buf[..len].copy_from_slice(&file.data[offset..offset + len]);
            Ok(len)
        } else {
            Err(vars::SQLITE_IOERR_READ)
        }
    }

    fn sync(&self, meta: &mut Self::Handle) -> VfsResult<()> {
        let mut state = self.state();
        state.log(format_args!("sync: handle={meta:?}"));
        state.hooks.sync(*meta);
        Ok(())
    }

    fn lock(&self, meta: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        let state = self.state();
        state.log(format_args!("lock: handle={meta:?} level={level:?}"));
        Ok(())
    }

    fn unlock(&self, meta: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        let state = self.state();
        state.log(format_args!("unlock: handle={meta:?} level={level:?}"));
        Ok(())
    }

    fn check_reserved_lock(&self, meta: &mut Self::Handle) -> VfsResult<bool> {
        let state = self.state();
        state.log(format_args!("check_reserved_lock: handle={meta:?}"));
        Ok(false)
    }

    fn close(&self, meta: Self::Handle) -> VfsResult<()> {
        let mut state = self.state();
        state.log(format_args!("close: handle={meta:?}"));
        state.hooks.close(meta);
        if let Some(file) = state.files.get(&meta) {
            if file.delete_on_close {
                state.files.remove(&meta);
            }
        }
        Ok(())
    }

    fn pragma(
        &self,
        meta: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        let mut state = self.state();
        state.log(format_args!("pragma: handle={meta:?} pragma={pragma:?}"));
        state.hooks.pragma(*meta, pragma)
    }

    fn sector_size(&self, handle: &mut Self::Handle) -> VfsResult<i32> {
        let mut state = self.state();
        state.log(format_args!("sector_size"));
        state.hooks.sector_size(*handle);
        Ok(DEFAULT_SECTOR_SIZE)
    }

    fn device_characteristics(&self, handle: &mut Self::Handle) -> VfsResult<i32> {
        let mut state = self.state();
        state.log(format_args!("device_characteristics"));
        state.hooks.device_characteristics(*handle);
        Ok(DEFAULT_DEVICE_CHARACTERISTICS)
    }
}
