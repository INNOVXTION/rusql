use crate::database::node::Node;
use crate::database::pager::Pointer;
use crate::database::types::NODE_SIZE;
use crate::errors::Error;
use tracing::error;

/// assumes little endian
///
/// reads a [u8] slice to u16
pub(crate) fn slice_to_u16(data: &Node, pos: usize) -> Result<u16, Error> {
    if pos > NODE_SIZE {
        error!("slice_to_u16: pos idx {} exceeded node size", pos);
        return Err(Error::IndexError);
    }
    data.0
        .get(pos..pos + 2)
        .and_then(|x| x.try_into().ok())
        .map(|buf: [u8; 2]| u16::from_le_bytes(buf))
        .ok_or(Error::IntCastError(None))
}
/// assumes little endian
///
/// reads a [u8] slice to u64
pub(crate) fn slice_to_pointer(data: &Node, pos: usize) -> Result<Pointer, Error> {
    if pos > NODE_SIZE {
        error!("slice_to_u64: pos idx {} exceeded node size", pos);
        return Err(Error::IndexError);
    }
    data.0
        .get(pos..pos + 8)
        .and_then(|x| x.try_into().ok())
        .map(|buf: [u8; 8]| Pointer::from(u64::from_le_bytes(buf)))
        .ok_or(Error::IntCastError(None))
}

/// writes u16 to node
pub(crate) fn write_u16(data: &mut Node, pos: usize, value: u16) -> Result<(), Error> {
    if pos > NODE_SIZE {
        error!("pos idx {} exceeded node size", pos);
        return Err(Error::IndexError);
    }
    data.0[pos..pos + 2].copy_from_slice(&value.to_le_bytes());
    Ok(())
}
/// writes Pointer to node
pub(crate) fn write_pointer(data: &mut Node, pos: usize, ptr: Pointer) -> Result<(), Error> {
    if pos > NODE_SIZE {
        error!("pos idx {} exceeded node size", pos);
        return Err(Error::IndexError);
    }
    data.0[pos..pos + 8].copy_from_slice(&ptr.to_slice());
    Ok(())
}

/// casts usize to u16
pub(crate) fn from_usize(n: usize) -> u16 {
    if n > u16::MAX as usize {
        error!("casting error");
        panic!();
    }
    n as u16
}
