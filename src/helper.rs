use crate::pager::node::PAGE_SIZE;
use crate::{pager::node::Node};
use crate::errors::Error;
use tracing::{instrument,};

/// assumes little endian
/// 
/// converts a [u8] slice to u16 
#[instrument]
pub(crate) fn slice_to_u16(data: &Node, pos: usize) -> Result<u16, Error> {
    if pos > PAGE_SIZE {
        return Err(Error::IndexError)
    }
    data.0.get(pos..pos + 2)
        .and_then(|x|x.try_into().ok())
        .map(|buf: [u8; 2]| u16::from_le_bytes(buf))
        .ok_or(Error::IntCastingError(None))
}
/// assumes little endian
/// 
/// converts a [u8] slice to u64 
#[instrument]
pub(crate) fn slice_to_u64(data: &Node, pos: usize) -> Result<u64, Error> {
    if pos > PAGE_SIZE {
        return Err(Error::IndexError)
    }
    data.0.get(pos..pos + 8)
        .and_then(|x|x.try_into().ok())
        .map(|buf: [u8; 8]| u64::from_le_bytes(buf))
        .ok_or(Error::IntCastingError(None))
}
#[instrument]
pub(crate) fn write_u16(data: &mut Node, pos: usize, value: u16) -> Result<(), Error> {
    if pos > PAGE_SIZE {
        return Err(Error::IndexError)
    }
    data.0[pos..pos + 2].copy_from_slice(&value.to_le_bytes());
    Ok(())
}
#[instrument]
pub(crate) fn from_usize(n: usize) -> u16 {
    if n > u16::MAX as usize {
        panic!("casting error");
    }
    n as u16
}


// // retrieve page content from page number
// #[instrument]
// pub fn get_node(page_number: u64) -> Result<Self, Error> {
//     let mut file = File::open("database.rdb")?;
//     let mut new_page = Node::new();
//     file.seek(io::SeekFrom::Start(PAGE_SIZE as u64 * page_number))?;
//     file.read(&mut *new_page.0)?;
//     Ok(new_page)
// }