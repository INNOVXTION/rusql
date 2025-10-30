use crate::pager::node::Node;
use crate::errors::Error;
use tracing::{instrument,};

// assumes little endian
// converts a slice to a u16 value
#[instrument]
pub(crate) fn slice_to_u16(data: &Node, pos: usize) -> Result<u16, Error> {
    data.0.get(pos..pos + 2)
        .and_then(|x|x.try_into().ok())
        .map(|buf: [u8; 2]| u16::from_le_bytes(buf))
        .ok_or(Error::CastingError)
}
// converts a slice to a u64 value
#[instrument]
pub(crate) fn slice_to_u64(data: &Node, pos: usize) -> Result<u64, Error> {
    data.0.get(pos..pos + 8)
        .and_then(|x|x.try_into().ok())
        .map(|buf: [u8; 8]| u64::from_le_bytes(buf))
        .ok_or(Error::CastingError)
}