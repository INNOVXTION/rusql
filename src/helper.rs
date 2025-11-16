use crate::database::node::Node;
use crate::database::types::PAGE_SIZE;
use crate::errors::Error;
use tracing::error;
use tracing::instrument;

/// assumes little endian
///
/// converts a [u8] slice to u16
pub(crate) fn slice_to_u16(data: &Node, pos: usize) -> Result<u16, Error> {
    if pos > PAGE_SIZE {
        error!("pos idx {} exceeded page size", pos);
        return Err(Error::IndexError);
    }
    data.0
        .get(pos..pos + 2)
        .and_then(|x| x.try_into().ok())
        .map(|buf: [u8; 2]| u16::from_le_bytes(buf))
        .ok_or(Error::IntCastingError(None))
}
/// assumes little endian
///
/// converts a [u8] slice to u64
pub(crate) fn slice_to_u64(data: &Node, pos: usize) -> Result<u64, Error> {
    if pos > PAGE_SIZE {
        error!("pos idx {} exceeded page size", pos);
        return Err(Error::IndexError);
    }
    data.0
        .get(pos..pos + 8)
        .and_then(|x| x.try_into().ok())
        .map(|buf: [u8; 8]| u64::from_le_bytes(buf))
        .ok_or(Error::IntCastingError(None))
}
pub(crate) fn write_u16(data: &mut Node, pos: usize, value: u16) -> Result<(), Error> {
    if pos > PAGE_SIZE {
        error!("pos idx {} exceeded page size", pos);
        return Err(Error::IndexError);
    }
    data.0[pos..pos + 2].copy_from_slice(&value.to_le_bytes());
    Ok(())
}
pub(crate) fn from_usize(n: usize) -> u16 {
    if n > u16::MAX as usize {
        panic!("casting error");
    }
    n as u16
}
