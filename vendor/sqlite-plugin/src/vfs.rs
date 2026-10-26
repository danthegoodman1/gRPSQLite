use crate::flags::{AccessFlags, LockLevel, OpenOpts, ShmLockMode};
use crate::logger::SqliteLogger;
use crate::vars::SQLITE_ERROR;
use crate::{ffi, vars};
use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::ffi::CString;
use alloc::format;
use alloc::string::String;
use core::mem::{ManuallyDrop, size_of};
use core::slice;
use core::{
    ffi::{CStr, c_char, c_int, c_void},
    ptr::{NonNull, null_mut},
};

/// The minimim supported `SQLite` version.
// If you need to make this earlier, make sure the tests are testing the earlier version
pub const MIN_SQLITE_VERSION_NUMBER: i32 = 3037002;

const DEFAULT_MAX_PATH_LEN: i32 = 512;
pub const DEFAULT_SECTOR_SIZE: i32 = 4096;

pub const DEFAULT_DEVICE_CHARACTERISTICS: i32 =
    // writes of any size are atomic
    vars::SQLITE_IOCAP_ATOMIC |
    // after reboot following a crash or power loss, the only bytes in a file that were written
    // at the application level might have changed and that adjacent bytes, even bytes within
    // the same sector are guaranteed to be unchanged
    vars::SQLITE_IOCAP_POWERSAFE_OVERWRITE |
    // when data is appended to a file, the data is appended first then the size of the file is
    // extended, never the other way around
    vars::SQLITE_IOCAP_SAFE_APPEND |
    // information is written to disk in the same order as calls to xWrite()
    vars::SQLITE_IOCAP_SEQUENTIAL;

/// A `SQLite3` extended error code
pub type SqliteErr = i32;

pub type VfsResult<T> = Result<T, SqliteErr>;

// FileWrapper needs to be repr(C) and have sqlite3_file as it's first member
// because it's a "subclass" of sqlite3_file
#[repr(C)]
struct FileWrapper<Handle> {
    file: ffi::sqlite3_file,
    vfs: *mut ffi::sqlite3_vfs,
    handle: Handle,
}

struct AppData<Vfs> {
    base_vfs: *mut ffi::sqlite3_vfs,
    vfs: Vfs,
    io_methods: ffi::sqlite3_io_methods,
    sqlite_api: SqliteApi,
}

#[derive(Debug)]
pub struct Pragma<'a> {
    pub name: &'a str,
    pub arg: Option<&'a str>,
}

#[derive(Debug)]
pub enum PragmaErr {
    NotFound,
    Fail(SqliteErr, Option<String>),
}

impl PragmaErr {
    pub fn required_arg(p: &Pragma<'_>) -> Self {
        PragmaErr::Fail(
            SQLITE_ERROR,
            Some(format!(
                "argument required (e.g. `pragma {} = ...`)",
                p.name
            )),
        )
    }
}

fn fallible(mut cb: impl FnMut() -> Result<i32, SqliteErr>) -> i32 {
    cb().unwrap_or_else(|err| err)
}

unsafe fn lossy_cstr<'a>(p: *const c_char) -> VfsResult<Cow<'a, str>> {
    unsafe {
        p.as_ref()
            .map(|p| CStr::from_ptr(p).to_string_lossy())
            .ok_or(vars::SQLITE_INTERNAL)
    }
}

macro_rules! unwrap_appdata {
    ($p_vfs:expr, $t_vfs:ty) => {
        unsafe {
            let out: VfsResult<&AppData<$t_vfs>> = (*$p_vfs)
                .pAppData
                .cast::<AppData<$t_vfs>>()
                .as_ref()
                .ok_or(vars::SQLITE_INTERNAL);
            out
        }
    };
}

macro_rules! unwrap_vfs {
    ($p_vfs:expr, $t_vfs:ty) => {{
        let out: VfsResult<&$t_vfs> = unwrap_appdata!($p_vfs, $t_vfs).map(|app_data| &app_data.vfs);
        out
    }};
}

macro_rules! unwrap_base_vfs {
    ($p_vfs:expr, $t_vfs:ty) => {{
        let out: VfsResult<&mut ffi::sqlite3_vfs> =
            unwrap_appdata!($p_vfs, $t_vfs).and_then(|app_data| {
                unsafe { app_data.base_vfs.as_mut() }.ok_or(vars::SQLITE_INTERNAL)
            });
        out
    }};
}

macro_rules! unwrap_file {
    ($p_file:expr, $t_vfs:ty) => {
        unsafe {
            let out: VfsResult<&mut FileWrapper<<$t_vfs>::Handle>> = $p_file
                .cast::<FileWrapper<<$t_vfs>::Handle>>()
                .as_mut()
                .ok_or(vars::SQLITE_INTERNAL);
            out
        }
    };
}

pub trait VfsHandle: Send {
    fn readonly(&self) -> bool;
    fn in_memory(&self) -> bool;
}

#[allow(unused_variables)]
pub trait Vfs: Send + Sync {
    type Handle: VfsHandle;

    /// construct a canonical version of the given path
    fn canonical_path<'a>(&self, path: Cow<'a, str>) -> VfsResult<Cow<'a, str>> {
        Ok(path)
    }

    // file system operations
    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle>;
    fn delete(&self, path: &str) -> VfsResult<()>;
    fn access(&self, path: &str, flags: AccessFlags) -> VfsResult<bool>;

    // file operations
    fn file_size(&self, handle: &mut Self::Handle) -> VfsResult<usize>;
    fn truncate(&self, handle: &mut Self::Handle, size: usize) -> VfsResult<()>;
    fn write(&self, handle: &mut Self::Handle, offset: usize, data: &[u8]) -> VfsResult<usize>;
    fn read(&self, handle: &mut Self::Handle, offset: usize, data: &mut [u8]) -> VfsResult<usize>;

    fn lock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()>;

    fn unlock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()>;

    fn check_reserved_lock(&self, handle: &mut Self::Handle) -> VfsResult<bool>;

    fn sync(&self, handle: &mut Self::Handle) -> VfsResult<()> {
        Ok(())
    }

    fn close(&self, handle: Self::Handle) -> VfsResult<()>;

    fn pragma(
        &self,
        handle: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        Err(PragmaErr::NotFound)
    }

    /// Handle a file control opcode other than `SQLITE_FCNTL_PRAGMA`.
    /// Return `Err(SQLITE_NOTFOUND)` for opcodes the VFS does not handle.
    fn file_control(
        &self,
        handle: &mut Self::Handle,
        op: c_int,
        p_arg: *mut c_void,
    ) -> VfsResult<()> {
        Err(vars::SQLITE_NOTFOUND)
    }

    // system queries
    fn sector_size(&self, handle: &mut Self::Handle) -> VfsResult<i32> {
        Ok(DEFAULT_SECTOR_SIZE)
    }

    fn device_characteristics(&self, handle: &mut Self::Handle) -> VfsResult<i32> {
        Ok(DEFAULT_DEVICE_CHARACTERISTICS)
    }

    fn shm_map(
        &self,
        handle: &mut Self::Handle,
        region_idx: usize,
        region_size: usize,
        extend: bool,
    ) -> VfsResult<Option<NonNull<u8>>> {
        Err(vars::SQLITE_READONLY_CANTINIT)
    }

    fn shm_lock(
        &self,
        handle: &mut Self::Handle,
        offset: u32,
        count: u32,
        mode: ShmLockMode,
    ) -> VfsResult<()> {
        Err(vars::SQLITE_IOERR)
    }

    fn shm_barrier(&self, handle: &mut Self::Handle) {}

    fn shm_unmap(&self, handle: &mut Self::Handle, delete: bool) -> VfsResult<()> {
        Err(vars::SQLITE_IOERR)
    }

    /// Memory-mapped page read (xFetch). Return a pointer to `amt` bytes of
    /// the file starting at `offset`, or `Ok(None)` to decline and have `SQLite`
    /// fall back to `xRead`.
    ///
    /// The default implementation declines all mmap requests. Override this to
    /// enable memory-mapped I/O for your VFS (e.g. mmap the database file).
    ///
    /// # Safety contract
    ///
    /// The returned pointer must remain valid until `unfetch` is called with
    /// the same offset. `SQLite` may read from the pointer concurrently from
    /// multiple threads.
    fn fetch(
        &self,
        handle: &mut Self::Handle,
        offset: i64,
        amt: usize,
    ) -> VfsResult<Option<NonNull<u8>>> {
        Ok(None)
    }

    /// Release a memory-mapped page previously returned by `fetch`.
    ///
    /// If `ptr` is null, this is a hint that the VFS should reduce its
    /// memory-mapped footprint (`SQLite` calls this when shrinking mmap).
    /// The default implementation is a no-op.
    fn unfetch(&self, handle: &mut Self::Handle, offset: i64, ptr: *mut u8) -> VfsResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct SqliteApi {
    register: unsafe extern "C" fn(arg1: *mut ffi::sqlite3_vfs, arg2: c_int) -> c_int,
    find: unsafe extern "C" fn(arg1: *const c_char) -> *mut ffi::sqlite3_vfs,
    mprintf: unsafe extern "C" fn(arg1: *const c_char, ...) -> *mut c_char,
    log: unsafe extern "C" fn(arg1: c_int, arg2: *const c_char, ...),
    libversion_number: unsafe extern "C" fn() -> c_int,
}

impl SqliteApi {
    #[cfg(feature = "static")]
    pub fn new_static() -> Self {
        Self {
            register: ffi::sqlite3_vfs_register,
            find: ffi::sqlite3_vfs_find,
            mprintf: ffi::sqlite3_mprintf,
            log: ffi::sqlite3_log,
            libversion_number: ffi::sqlite3_libversion_number,
        }
    }

    /// Initializes SqliteApi from a filled `sqlite3_api_routines` object.
    /// # Safety
    /// `api` must be a valid, aligned pointer to a `sqlite3_api_routines` struct
    #[cfg(feature = "dynamic")]
    pub unsafe fn new_dynamic(api: &ffi::sqlite3_api_routines) -> VfsResult<Self> {
        Ok(Self {
            register: api.vfs_register.ok_or(vars::SQLITE_INTERNAL)?,
            find: api.vfs_find.ok_or(vars::SQLITE_INTERNAL)?,
            mprintf: api.mprintf.ok_or(vars::SQLITE_INTERNAL)?,
            log: api.log.ok_or(vars::SQLITE_INTERNAL)?,
            libversion_number: api.libversion_number.ok_or(vars::SQLITE_INTERNAL)?,
        })
    }

    /// Copies the provided string into a memory buffer allocated by `sqlite3_mprintf`.
    /// Writes the pointer to the memory buffer to `out` if `out` is not null.
    /// # Safety
    /// 1. the out pointer must not be null
    /// 2. it is the callers responsibility to eventually free the allocated buffer
    pub unsafe fn mprintf(&self, s: &str, out: *mut *const c_char) -> VfsResult<()> {
        let s = CString::new(s).map_err(|_| vars::SQLITE_INTERNAL)?;
        let p = unsafe { (self.mprintf)(s.as_ptr()) };
        if p.is_null() {
            Err(vars::SQLITE_NOMEM)
        } else {
            unsafe {
                *out = p;
            }
            Ok(())
        }
    }
}

pub struct RegisterOpts {
    /// If true, make this vfs the default vfs for `SQLite`.
    pub make_default: bool,
}

#[cfg(feature = "static")]
pub fn register_static<T: Vfs>(
    name: CString,
    vfs: T,
    opts: RegisterOpts,
) -> VfsResult<SqliteLogger> {
    register_inner(SqliteApi::new_static(), name, vfs, opts)
}

/// Register a vfs with `SQLite` using the dynamic API. This API is available when
/// `SQLite` is initializing extensions.
/// # Safety
/// `p_api` must be a valid, aligned pointer to a `sqlite3_api_routines` struct
#[cfg(feature = "dynamic")]
pub unsafe fn register_dynamic<T: Vfs>(
    p_api: *mut ffi::sqlite3_api_routines,
    name: CString,
    vfs: T,
    opts: RegisterOpts,
) -> VfsResult<SqliteLogger> {
    let api = unsafe { p_api.as_ref() }.ok_or(vars::SQLITE_INTERNAL)?;
    let sqlite_api = unsafe { SqliteApi::new_dynamic(api)? };
    register_inner(sqlite_api, name, vfs, opts)
}

fn register_inner<T: Vfs>(
    sqlite_api: SqliteApi,
    name: CString,
    vfs: T,
    opts: RegisterOpts,
) -> VfsResult<SqliteLogger> {
    let version = unsafe { (sqlite_api.libversion_number)() };
    if version < MIN_SQLITE_VERSION_NUMBER {
        panic!(
            "sqlite3 must be at least version {}, found version {}",
            MIN_SQLITE_VERSION_NUMBER, version
        );
    }

    let io_methods = ffi::sqlite3_io_methods {
        iVersion: 3,
        xClose: Some(x_close::<T>),
        xRead: Some(x_read::<T>),
        xWrite: Some(x_write::<T>),
        xTruncate: Some(x_truncate::<T>),
        xSync: Some(x_sync::<T>),
        xFileSize: Some(x_file_size::<T>),
        xLock: Some(x_lock::<T>),
        xUnlock: Some(x_unlock::<T>),
        xCheckReservedLock: Some(x_check_reserved_lock::<T>),
        xFileControl: Some(x_file_control::<T>),
        xSectorSize: Some(x_sector_size::<T>),
        xDeviceCharacteristics: Some(x_device_characteristics::<T>),
        xShmMap: Some(x_shm_map::<T>),
        xShmLock: Some(x_shm_lock::<T>),
        xShmBarrier: Some(x_shm_barrier::<T>),
        xShmUnmap: Some(x_shm_unmap::<T>),
        xFetch: Some(x_fetch::<T>),
        xUnfetch: Some(x_unfetch::<T>),
    };

    let logger = SqliteLogger::new(sqlite_api.log);

    let p_name = ManuallyDrop::new(name).as_ptr();
    let base_vfs = unsafe { (sqlite_api.find)(null_mut()) };
    let vfs_register = sqlite_api.register;
    let p_appdata = Box::into_raw(Box::new(AppData { base_vfs, vfs, io_methods, sqlite_api }));

    let filewrapper_size: c_int = size_of::<FileWrapper<T::Handle>>()
        .try_into()
        .map_err(|_| vars::SQLITE_INTERNAL)?;

    let p_vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
        iVersion: 3,
        szOsFile: filewrapper_size,
        mxPathname: DEFAULT_MAX_PATH_LEN,
        pNext: null_mut(),
        zName: p_name,
        pAppData: p_appdata.cast(),
        xOpen: Some(x_open::<T>),
        xDelete: Some(x_delete::<T>),
        xAccess: Some(x_access::<T>),
        xFullPathname: Some(x_full_pathname::<T>),
        xDlOpen: Some(x_dlopen::<T>),
        xDlError: Some(x_dlerror::<T>),
        xDlSym: Some(x_dlsym::<T>),
        xDlClose: Some(x_dlclose::<T>),
        xRandomness: Some(x_randomness::<T>),
        xSleep: Some(x_sleep::<T>),
        xCurrentTime: Some(x_current_time::<T>),
        xGetLastError: None,
        xCurrentTimeInt64: Some(x_current_time_int64::<T>),
        xSetSystemCall: None,
        xGetSystemCall: None,
        xNextSystemCall: None,
    }));

    let result = unsafe { vfs_register(p_vfs, opts.make_default.into()) };
    if result != vars::SQLITE_OK {
        // cleanup memory
        unsafe {
            drop(Box::from_raw(p_vfs));
            drop(Box::from_raw(p_appdata));
            drop(CString::from_raw(p_name as *mut c_char));
        };
        Err(result)
    } else {
        Ok(logger)
    }
}

unsafe extern "C" fn x_open<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    p_file: *mut ffi::sqlite3_file,
    flags: c_int,
    p_out_flags: *mut c_int,
) -> c_int {
    // From https://sqlite.org/c3ref/vfs.html:
    // "Note that the xOpen method must set the sqlite3_file.pMethods to either a valid
    // sqlite3_io_methods object or to NULL. xOpen must do this even if the open fails."
    if let Some(f) = unsafe { p_file.as_mut() } {
        f.pMethods = core::ptr::null();
    }

    fallible(|| {
        let opts = flags.into();
        let name = unsafe { lossy_cstr(z_name) }.ok();
        let vfs = unwrap_vfs!(p_vfs, T)?;
        let handle = vfs.open(name.as_ref().map(|s| s.as_ref()), opts)?;

        let appdata = unwrap_appdata!(p_vfs, T)?;

        if let Some(p_out_flags) = unsafe { p_out_flags.as_mut() } {
            let mut out_flags = flags;
            if handle.readonly() {
                out_flags |= vars::SQLITE_OPEN_READONLY;
            }
            if handle.in_memory() {
                out_flags |= vars::SQLITE_OPEN_MEMORY;
            }
            *p_out_flags = out_flags;
        }

        let out_file = p_file.cast::<FileWrapper<T::Handle>>();
        // Safety: SQLite owns the heap allocation backing p_file (it handles malloc/free).
        // We use ptr::write to initialize that allocation directly.
        unsafe {
            core::ptr::write(
                out_file,
                FileWrapper {
                    file: ffi::sqlite3_file { pMethods: &appdata.io_methods },
                    vfs: p_vfs,
                    handle,
                },
            );
        }

        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_delete<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    _sync_dir: c_int,
) -> c_int {
    fallible(|| {
        let name = unsafe { lossy_cstr(z_name)? };
        let vfs = unwrap_vfs!(p_vfs, T)?;
        vfs.delete(&name)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_access<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    flags: c_int,
    p_res_out: *mut c_int,
) -> c_int {
    fallible(|| {
        let name = unsafe { lossy_cstr(z_name)? };
        let vfs = unwrap_vfs!(p_vfs, T)?;
        let result = vfs.access(&name, flags.into())?;
        let out = unsafe { p_res_out.as_mut() }.ok_or(vars::SQLITE_IOERR_ACCESS)?;
        *out = result as i32;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_full_pathname<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    n_out: c_int,
    z_out: *mut c_char,
) -> c_int {
    fallible(|| {
        let name = unsafe { lossy_cstr(z_name)? };
        let vfs = unwrap_vfs!(p_vfs, T)?;
        let full_name = vfs.canonical_path(name)?;
        let n_out = n_out.try_into().map_err(|_| vars::SQLITE_INTERNAL)?;
        let out = unsafe { slice::from_raw_parts_mut(z_out as *mut u8, n_out) };
        let from = &full_name.as_bytes()[..full_name.len().min(n_out - 1)];
        // copy the name into the output buffer
        out[..from.len()].copy_from_slice(from);
        // add the trailing null byte
        out[from.len()] = 0;
        Ok(vars::SQLITE_OK)
    })
}

// file operations

unsafe extern "C" fn x_close<T: Vfs>(p_file: *mut ffi::sqlite3_file) -> c_int {
    fallible(|| {
        // Safety: SQLite owns the heap allocation backing p_file (it handles
        // malloc/free). We use ptr::read to copy our Handle out of that
        // allocation so it can be passed to vfs.close() and properly dropped.
        // SQLite will not call any other file methods after x_close without
        // first calling x_open to reinitialize the handle.
        let (vfs, handle) = unsafe {
            // verify p_file is not null and get a mutable reference
            let p_file_ref = p_file.as_mut().ok_or(vars::SQLITE_INTERNAL)?;
            // set pMethods to null, signaling to SQLite that the file is closed
            p_file_ref.pMethods = core::ptr::null();

            // extract a copy of the FileWrapper
            let file = core::ptr::read(p_file.cast::<FileWrapper<T::Handle>>());
            (file.vfs, file.handle)
        };

        let vfs = unwrap_vfs!(vfs, T)?;
        vfs.close(handle)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_read<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    i_amt: c_int,
    i_ofst: ffi::sqlite_int64,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        let buf_len: usize = i_amt.try_into().map_err(|_| vars::SQLITE_IOERR_READ)?;
        let offset: usize = i_ofst.try_into().map_err(|_| vars::SQLITE_IOERR_READ)?;
        let buf = unsafe { slice::from_raw_parts_mut(buf.cast::<u8>(), buf_len) };
        let bytes_read = vfs.read(&mut file.handle, offset, buf)?;
        if bytes_read < buf_len {
            // From https://sqlite.org/c3ref/io_methods.html:
            // "If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill in the unread portions
            // of the buffer with zeros."
            buf[bytes_read..].fill(0);
            return Err(vars::SQLITE_IOERR_SHORT_READ);
        }
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_write<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    i_amt: c_int,
    i_ofst: ffi::sqlite_int64,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        let buf_len: usize = i_amt.try_into().map_err(|_| vars::SQLITE_IOERR_WRITE)?;
        let offset: usize = i_ofst.try_into().map_err(|_| vars::SQLITE_IOERR_WRITE)?;
        let buf = unsafe { slice::from_raw_parts(buf.cast::<u8>(), buf_len) };
        let n = vfs.write(&mut file.handle, offset, buf)?;
        if n != buf_len {
            return Err(vars::SQLITE_IOERR_WRITE);
        }
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_truncate<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    size: ffi::sqlite_int64,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        let size: usize = size.try_into().map_err(|_| vars::SQLITE_IOERR_TRUNCATE)?;
        vfs.truncate(&mut file.handle, size)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_sync<T: Vfs>(p_file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.sync(&mut file.handle)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_file_size<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    p_size: *mut ffi::sqlite3_int64,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        let size = vfs.file_size(&mut file.handle)?;
        let p_size = unsafe { p_size.as_mut() }.ok_or(vars::SQLITE_INTERNAL)?;
        *p_size = size.try_into().map_err(|_| vars::SQLITE_IOERR_FSTAT)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_lock<T: Vfs>(p_file: *mut ffi::sqlite3_file, raw_lock: c_int) -> c_int {
    fallible(|| {
        let level: LockLevel = raw_lock.into();
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.lock(&mut file.handle, level)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_unlock<T: Vfs>(p_file: *mut ffi::sqlite3_file, raw_lock: c_int) -> c_int {
    fallible(|| {
        let level: LockLevel = raw_lock.into();
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.unlock(&mut file.handle, level)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_check_reserved_lock<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    p_out: *mut c_int,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        unsafe {
            *p_out = vfs.check_reserved_lock(&mut file.handle)? as c_int;
        }
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_file_control<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    op: c_int,
    p_arg: *mut c_void,
) -> c_int {
    /*
    Other interesting ops:
    SIZE_HINT: hint of how large the database will grow during the current transaction
    COMMIT_PHASETWO: after transaction commits before file unlocks (only used in WAL mode)
    VFS_NAME: should return this vfs's name + / + base vfs's name

    Atomic write support: (requires SQLITE_IOCAP_BATCH_ATOMIC device characteristic)
    Docs: https://www3.sqlite.org/cgi/src/technote/714f6cbbf78c8a1351cbd48af2b438f7f824b336
    BEGIN_ATOMIC_WRITE: start an atomic write operation
    COMMIT_ATOMIC_WRITE: commit an atomic write operation
    ROLLBACK_ATOMIC_WRITE: rollback an atomic write operation
    */

    if op == vars::SQLITE_FCNTL_PRAGMA {
        return fallible(|| {
            let file = unwrap_file!(p_file, T)?;
            let vfs = unwrap_vfs!(file.vfs, T)?;

            // p_arg is a pointer to an array of strings
            // the second value is the pragma name
            // the third value is either null or the pragma arg
            let args = p_arg.cast::<*const c_char>();
            let name = unsafe { lossy_cstr(*args.add(1)) }?;
            let arg = unsafe {
                (*args.add(2))
                    .as_ref()
                    .map(|p| CStr::from_ptr(p).to_string_lossy())
            };
            let pragma = Pragma { name: &name, arg: arg.as_deref() };

            let (result, msg) = match vfs.pragma(&mut file.handle, pragma) {
                Ok(msg) => (Ok(vars::SQLITE_OK), msg),
                Err(PragmaErr::NotFound) => (Err(vars::SQLITE_NOTFOUND), None),
                Err(PragmaErr::Fail(err, msg)) => (Err(err), msg),
            };

            if let Some(msg) = msg {
                // write the msg back to the first element of the args array.
                // SQLite is responsible for eventually freeing the result
                let appdata = unwrap_appdata!(file.vfs, T)?;
                unsafe { appdata.sqlite_api.mprintf(&msg, args)? };
            }

            result
        });
    }

    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.file_control(&mut file.handle, op, p_arg)?;
        Ok(vars::SQLITE_OK)
    })
}

// system queries

unsafe extern "C" fn x_sector_size<T: Vfs>(p_file: *mut ffi::sqlite3_file) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.sector_size(&mut file.handle)
    })
}

unsafe extern "C" fn x_device_characteristics<T: Vfs>(p_file: *mut ffi::sqlite3_file) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.device_characteristics(&mut file.handle)
    })
}

unsafe extern "C" fn x_shm_map<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    pg: c_int,
    pgsz: c_int,
    extend: c_int,
    p_page: *mut *mut c_void,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        if let Some(region) = vfs.shm_map(
            &mut file.handle,
            pg.try_into().map_err(|_| vars::SQLITE_IOERR)?,
            pgsz.try_into().map_err(|_| vars::SQLITE_IOERR)?,
            extend != 0,
        )? {
            unsafe { *p_page = region.as_ptr() as *mut c_void }
        } else {
            unsafe { *p_page = null_mut() }
        }
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_shm_lock<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.shm_lock(
            &mut file.handle,
            offset.try_into().map_err(|_| vars::SQLITE_IOERR)?,
            n.try_into().map_err(|_| vars::SQLITE_IOERR)?,
            ShmLockMode::try_from(flags)?,
        )?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_shm_barrier<T: Vfs>(p_file: *mut ffi::sqlite3_file) {
    if let Ok(file) = unwrap_file!(p_file, T) {
        if let Ok(vfs) = unwrap_vfs!(file.vfs, T) {
            vfs.shm_barrier(&mut file.handle)
        }
    }
}

unsafe extern "C" fn x_shm_unmap<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    delete_flag: c_int,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.shm_unmap(&mut file.handle, delete_flag != 0)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_fetch<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    i_ofst: ffi::sqlite3_int64,
    i_amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        let amt: usize = i_amt.try_into().map_err(|_| vars::SQLITE_IOERR)?;
        if let Some(ptr) = vfs.fetch(&mut file.handle, i_ofst, amt)? {
            unsafe { *pp = ptr.as_ptr() as *mut c_void }
        } else {
            unsafe { *pp = null_mut() }
        }
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_unfetch<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    i_ofst: ffi::sqlite3_int64,
    p: *mut c_void,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file, T)?;
        let vfs = unwrap_vfs!(file.vfs, T)?;
        vfs.unfetch(&mut file.handle, i_ofst, p as *mut u8)?;
        Ok(vars::SQLITE_OK)
    })
}

// the following functions are wrappers around the base vfs functions

unsafe extern "C" fn x_dlopen<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_path: *const c_char,
) -> *mut c_void {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs, T) {
        if let Some(x_dlopen) = vfs.xDlOpen {
            return unsafe { x_dlopen(vfs, z_path) };
        }
    }
    null_mut()
}

unsafe extern "C" fn x_dlerror<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    n_byte: c_int,
    z_err_msg: *mut c_char,
) {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs, T) {
        if let Some(x_dlerror) = vfs.xDlError {
            unsafe { x_dlerror(vfs, n_byte, z_err_msg) };
        }
    }
}

unsafe extern "C" fn x_dlsym<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    p_handle: *mut c_void,
    z_symbol: *const c_char,
) -> Option<unsafe extern "C" fn(arg1: *mut ffi::sqlite3_vfs, arg2: *mut c_void, arg3: *const c_char)>
{
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs, T) {
        if let Some(x_dlsym) = vfs.xDlSym {
            return unsafe { x_dlsym(vfs, p_handle, z_symbol) };
        }
    }
    None
}

unsafe extern "C" fn x_dlclose<T: Vfs>(p_vfs: *mut ffi::sqlite3_vfs, p_handle: *mut c_void) {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs, T) {
        if let Some(x_dlclose) = vfs.xDlClose {
            unsafe { x_dlclose(vfs, p_handle) };
        }
    }
}

unsafe extern "C" fn x_randomness<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    n_byte: c_int,
    z_out: *mut c_char,
) -> c_int {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs, T) {
        if let Some(x_randomness) = vfs.xRandomness {
            return unsafe { x_randomness(vfs, n_byte, z_out) };
        }
    }
    vars::SQLITE_INTERNAL
}

unsafe extern "C" fn x_sleep<T: Vfs>(p_vfs: *mut ffi::sqlite3_vfs, microseconds: c_int) -> c_int {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs, T) {
        if let Some(x_sleep) = vfs.xSleep {
            return unsafe { x_sleep(vfs, microseconds) };
        }
    }
    vars::SQLITE_INTERNAL
}

unsafe extern "C" fn x_current_time<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    p_time: *mut f64,
) -> c_int {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs, T) {
        if let Some(x_current_time) = vfs.xCurrentTime {
            return unsafe { x_current_time(vfs, p_time) };
        }
    }
    vars::SQLITE_INTERNAL
}

unsafe extern "C" fn x_current_time_int64<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    p_time: *mut i64,
) -> c_int {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs, T) {
        if let Some(x_current_time_int64) = vfs.xCurrentTimeInt64 {
            return unsafe { x_current_time_int64(vfs, p_time) };
        }
    }
    vars::SQLITE_INTERNAL
}

#[cfg(test)]
mod tests {
    // tests use std
    extern crate std;

    use super::*;
    use crate::{
        flags::{CreateMode, OpenKind, OpenMode},
        mock::*,
    };
    use alloc::{sync::Arc, vec::Vec};
    use parking_lot::Mutex;
    use rusqlite::{Connection, OpenFlags};
    use std::{boxed::Box, io::Write, println};

    fn log_handler(_: i32, arg2: &str) {
        println!("{arg2}");
    }

    #[test]
    fn sanity() -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            rusqlite::trace::config_log(Some(log_handler)).unwrap();
        }

        struct H {}
        impl Hooks for H {
            fn open(&mut self, path: &Option<&str>, opts: &OpenOpts) {
                let path = path.unwrap();
                if path == "main.db" {
                    assert!(!opts.delete_on_close());
                    assert_eq!(opts.kind(), OpenKind::MainDb);
                    assert_eq!(
                        opts.mode(),
                        OpenMode::ReadWrite { create: CreateMode::Create }
                    );
                } else if path == "main.db-journal" {
                    assert!(!opts.delete_on_close());
                    assert_eq!(opts.kind(), OpenKind::MainJournal);
                    assert_eq!(
                        opts.mode(),
                        OpenMode::ReadWrite { create: CreateMode::Create }
                    );
                } else {
                    panic!("unexpected path: {}", path);
                }
            }
        }

        let shared = Arc::new(Mutex::new(MockState::new(Box::new(H {}))));
        let vfs = MockVfs::new(shared.clone());
        let logger = register_static(
            CString::new("mock").unwrap(),
            vfs,
            RegisterOpts { make_default: true },
        )
        .map_err(|_| "failed to register vfs")?;

        // setup the logger
        shared.lock().setup_logger(logger);

        // create a sqlite connection using the mock vfs
        let conn = Connection::open_with_flags_and_vfs(
            "main.db",
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "mock",
        )?;

        conn.execute("create table t (val int)", [])?;
        conn.execute("insert into t (val) values (1)", [])?;
        conn.execute("insert into t (val) values (2)", [])?;

        conn.execute("pragma mock_test", [])?;

        let n: i64 = conn.query_row("select sum(val) from t", [], |row| row.get(0))?;
        assert_eq!(n, 3);

        // the blob api is interesting and stress tests reading/writing pages and journaling
        conn.execute("create table b (data blob)", [])?;
        println!("inserting zero blob");
        conn.execute("insert into b values (zeroblob(8192))", [])?;
        let rowid = conn.last_insert_rowid();
        let mut blob = conn.blob_open(rusqlite::MAIN_DB, "b", "data", rowid, false)?;

        // write some data to the blob
        println!("writing to blob");
        let n = blob.write(b"hello")?;
        assert_eq!(n, 5);

        blob.close()?;

        // query the table for the blob and print it
        let mut stmt = conn.prepare("select data from b")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let data: Vec<u8> = row.get(0)?;
            assert_eq!(&data[0..5], b"hello");
        }
        drop(rows);
        drop(stmt);

        conn.close().expect("failed to close connection");

        Ok(())
    }
}
