#[cfg(unix)]
pub fn get() -> usize {
    unsafe {
        let v = libc::sysconf(libc::_SC_PAGESIZE);
        if v <= 0 {
            4096
        } else {
            v as usize
        }
    }
}

#[cfg(windows)]
pub fn get() -> usize {
    4096
}

#[cfg(unix)]
pub fn get_granularity() -> usize {
    get()
}

#[cfg(windows)]
pub fn get_granularity() -> usize {
    get()
}
