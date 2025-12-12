/*
 * helper functions for decoding purposes
 */

use std::rc::Rc;

use crate::database::tables::tables::DataCell;

/// length prefix for strings is u32
pub(crate) const STR_PRE_LEN: usize = 4;
/// length of integer it 8 bytes
pub(crate) const INT_LEN: usize = 8;
pub(crate) const IDX_LEN: usize = 8;

/// converts a String to bytes with a 4 byte length number + utf8 character
/// ```
/// let key = format!("{}{}{}", 5, "column1", "column2").encode();
/// let val = format!("{}", "some data").encode();
/// assert_eq!(key.len(), 19);
/// assert_eq!(val.len(), 13);
/// assert_eq!(String::decode(&key), "5column1column2");
/// assert_eq!(String::decode(&val), "some data");
/// ```
pub(super) trait StringCodec {
    fn encode(&self) -> Rc<[u8]>;
    fn into_cell(self) -> DataCell;

    fn decode(data: &[u8]) -> String;
    fn from_cell(cell: DataCell) -> String;
}

impl StringCodec for String {
    fn encode(&self) -> Rc<[u8]> {
        let len = self.len();
        let buf = Rc::<[u8]>::new_zeroed_slice(len + STR_PRE_LEN);
        let mut buf = unsafe { buf.assume_init() };
        let buf_ref = Rc::get_mut(&mut buf).unwrap();
        buf_ref[..STR_PRE_LEN].copy_from_slice(&(len as u32).to_le_bytes());
        buf_ref[STR_PRE_LEN..].copy_from_slice(self.as_bytes());
        buf
    }
    fn into_cell(self) -> DataCell {
        DataCell::Str(self)
    }

    fn decode(data: &[u8]) -> String {
        let mut buf = [0u8; STR_PRE_LEN];
        buf.copy_from_slice(&data[0..STR_PRE_LEN]);
        let len = u32::from_le_bytes(buf) as usize;
        assert_eq!(data.len(), len + STR_PRE_LEN);
        // SAFETY: we encode in UTF-8
        unsafe { String::from_utf8_unchecked(data[STR_PRE_LEN..STR_PRE_LEN + len].to_vec()) }
    }
    fn from_cell(cell: DataCell) -> String {
        if let DataCell::Str(s) = cell {
            s
        } else {
            unreachable!()
        }
    }
}

// converts an Integer to little endian bytes
pub(super) trait IntegerCodec {
    fn encode(&self) -> Rc<[u8; INT_LEN]>;
    fn decode(data: &[u8]) -> Self;
}

impl IntegerCodec for i64 {
    fn encode(&self) -> Rc<[u8; INT_LEN]> {
        Rc::from(self.to_le_bytes())
    }

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), INT_LEN);
        let mut buf = [0u8; INT_LEN];
        buf.copy_from_slice(data);
        i64::from_le_bytes(buf)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn codec_test() {
        let key = format!("{}{}{}", 5, "column1", "column2").encode();
        let val = format!("{}", "some data").encode();
        assert_eq!(key.len(), 19);
        assert_eq!(val.len(), 13);
        assert_eq!(String::decode(&key), "5column1column2");
        assert_eq!(String::decode(&val), "some data");

        let mut buf = [0u8; 24];
        let v1: i64 = 5;
        let v2: i64 = 9;
        let v3: i64 = 13;

        buf[0..8].copy_from_slice(&(*v1.encode()));
        buf[8..16].copy_from_slice(&(*v2.encode()));
        buf[16..].copy_from_slice(&(*v3.encode()));

        let v1 = i64::decode(&buf[..8]);
        let v2 = i64::decode(&buf[8..16]);
        let v3 = i64::decode(&buf[16..]);

        assert_eq!(v1, 5);
        assert_eq!(v2, 9);
        assert_eq!(v3, 13);

        let str = "primary key";
        let id: i64 = -10;
        let mut buf: Vec<u8> = vec![0; INT_LEN + str.len() + STR_PRE_LEN];

        buf[..INT_LEN].copy_from_slice(&(*id.encode()));
        buf[INT_LEN..].copy_from_slice(&str.to_string().encode());
        let decode = format!(
            "{}{}",
            i64::decode(&buf[0..INT_LEN]),
            String::decode(&buf[INT_LEN..])
        );
        assert_eq!(decode, "-10primary key");
    }
}
