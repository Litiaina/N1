use crate::error::BlockManagerError;
use crate::types::DeviceHeader;
use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::ptr::NonNull;

struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

impl AlignedBuffer {
    fn zeroed(len: usize, align: usize) -> Result<Self, BlockManagerError> {
        if len == 0 {
            return Err(BlockManagerError::InvalidArgument(
                "aligned buffer length must be > 0".to_string(),
            ));
        }
        let layout = Layout::from_size_align(len, align).map_err(|_| {
            BlockManagerError::InvalidArgument(format!(
                "invalid aligned buffer layout len={} align={}",
                len, align
            ))
        })?;
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            BlockManagerError::InvalidArgument("aligned allocation returned null".to_string())
        })?;
        Ok(Self { ptr, len, layout })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

pub(crate) fn validate_block_size(block_size: u32) -> Result<u32, BlockManagerError> {
    if block_size < 512 || !block_size.is_power_of_two() {
        return Err(BlockManagerError::InvalidArgument(format!(
            "block_size must be power-of-two >= 512, got {}",
            block_size
        )));
    }
    Ok(block_size)
}

pub(crate) fn div_ceil(value: u64, divisor: u64) -> u64 {
    value.saturating_add(divisor.saturating_sub(1)) / divisor
}

pub(crate) fn block_offset(header: &DeviceHeader, block: u64) -> u64 {
    block.saturating_mul(u64::from(header.block_size))
}

pub(crate) fn open_direct_file(path: &Path) -> Result<File, BlockManagerError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    Ok(file)
}

pub(crate) fn zero_region(
    file: &File,
    block_size: u32,
    offset: u64,
    len: u64,
) -> Result<(), BlockManagerError> {
    let buf = vec![0u8; block_size as usize];
    let mut written = 0u64;
    while written < len {
        let to_write = (len - written).min(buf.len() as u64) as usize;
        write_all_at(file, &buf[..to_write], offset + written)?;
        written += to_write as u64;
    }
    Ok(())
}

fn align_down(value: u64, align: u64) -> u64 {
    value - (value % align)
}

fn align_up(value: u64, align: u64) -> u64 {
    if value % align == 0 {
        value
    } else {
        value + (align - (value % align))
    }
}

fn is_direct_compatible(block_size: u32, offset: u64, buf: &[u8]) -> bool {
    let align = block_size as usize;
    offset % u64::from(block_size) == 0
        && buf.len() % align == 0
        && (buf.as_ptr() as usize) % align == 0
}

pub(crate) fn read_exact_at_direct(
    file: &File,
    block_size: u32,
    offset: u64,
    buf: &mut [u8],
) -> Result<(), BlockManagerError> {
    if buf.is_empty() {
        return Ok(());
    }
    if is_direct_compatible(block_size, offset, buf) {
        return read_exact_at(file, buf, offset);
    }
    let align = u64::from(block_size);
    let aligned_offset = align_down(offset, align);
    let prefix = (offset - aligned_offset) as usize;
    let aligned_len = align_up(prefix as u64 + buf.len() as u64, align) as usize;
    let mut scratch = AlignedBuffer::zeroed(aligned_len, block_size as usize)?;
    read_exact_at(file, scratch.as_mut_slice(), aligned_offset)?;
    buf.copy_from_slice(&scratch.as_slice()[prefix..prefix + buf.len()]);
    Ok(())
}

pub(crate) fn write_all_at_direct(
    file: &File,
    block_size: u32,
    offset: u64,
    buf: &[u8],
) -> Result<(), BlockManagerError> {
    if buf.is_empty() {
        return Ok(());
    }
    if is_direct_compatible(block_size, offset, buf) {
        return write_all_at(file, buf, offset);
    }
    let align = u64::from(block_size);
    let aligned_offset = align_down(offset, align);
    let prefix = (offset - aligned_offset) as usize;
    let aligned_len = align_up(prefix as u64 + buf.len() as u64, align) as usize;
    let mut scratch = AlignedBuffer::zeroed(aligned_len, block_size as usize)?;

    if prefix != 0 || buf.len() != aligned_len {
        read_exact_at(file, scratch.as_mut_slice(), aligned_offset)?;
    }

    scratch.as_mut_slice()[prefix..prefix + buf.len()].copy_from_slice(buf);
    write_all_at(file, scratch.as_slice(), aligned_offset)
}

pub(crate) fn write_all_at(file: &File, buf: &[u8], offset: u64) -> Result<(), BlockManagerError> {
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.write_at(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(BlockManagerError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "write_at returned 0",
            )));
        }
        written += n;
    }
    Ok(())
}

pub(crate) fn read_exact_at(
    file: &File,
    buf: &mut [u8],
    offset: u64,
) -> Result<(), BlockManagerError> {
    let mut read = 0usize;
    while read < buf.len() {
        let n = file.read_at(&mut buf[read..], offset + read as u64)?;
        if n == 0 {
            return Err(BlockManagerError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read_at hit eof",
            )));
        }
        read += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_direct_compatible;

    #[test]
    fn direct_compatibility_requires_offset_len_and_pointer_alignment() {
        let mut aligned = vec![0u8; 8192 + 4096];
        let ptr = aligned.as_mut_ptr() as usize;
        let offset = (4096 - (ptr % 4096)) % 4096;
        let aligned_slice = &aligned[offset..offset + 4096];
        assert!(is_direct_compatible(4096, 0, aligned_slice));
        assert!(!is_direct_compatible(4096, 512, aligned_slice));
        assert!(!is_direct_compatible(4096, 0, &aligned_slice[..2048]));
        assert!(!is_direct_compatible(4096, 0, &aligned_slice[1..]));
    }
}
