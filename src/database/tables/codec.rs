/*
 * helper functions for decoding purposes
 */

use std::rc::Rc;

use crate::database::tables::tables::{DataCell, TypeCol};

/*
Key-Value LayoutV2:
--------KEY---------|-----VAL----|
[HEADER][KEY1][KEY2]|[VAL1][VAL2]|

HEADER:
[2B Header Size][1B K1 type][1B K2 type][...]
 */

pub const TYPE_LEN: usize = 1;

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
    // encodes a string to the following forma: [1B Type][2B len][nB str...]
    fn encode(&self) -> Rc<[u8]> {
        let len = self.len();
        let buf = Rc::<[u8]>::new_zeroed_slice(TYPE_LEN + len + STR_PRE_LEN);

        let mut buf = unsafe { buf.assume_init() };
        let buf_ref = Rc::get_mut(&mut buf).unwrap();

        buf_ref[0] = TypeCol::BYTES as u8;
        buf_ref[1..1 + STR_PRE_LEN].copy_from_slice(&(len as u32).to_le_bytes());
        buf_ref[1 + STR_PRE_LEN..].copy_from_slice(self.as_bytes());

        assert_eq!(buf.len(), TYPE_LEN + len + STR_PRE_LEN);
        buf
    }
    fn into_cell(self) -> DataCell {
        DataCell::Str(self)
    }

    /// input assumes presence of type byte, doesnt include it in the output
    fn decode(data: &[u8]) -> String {
        assert_eq!(data[0], TypeCol::BYTES as u8);

        let len =
            u32::from_le_bytes(data[TYPE_LEN..TYPE_LEN + STR_PRE_LEN].try_into().unwrap()) as usize;

        assert_eq!(data.len(), TYPE_LEN + len + STR_PRE_LEN);
        // SAFETY: we encode in UTF-8
        unsafe {
            String::from_utf8_unchecked(
                data[TYPE_LEN + STR_PRE_LEN..TYPE_LEN + STR_PRE_LEN + len].to_vec(),
            )
        }
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
    fn encode(&self) -> Rc<[u8; TYPE_LEN + INT_LEN]>;
    fn decode(data: &[u8]) -> Self;
}

impl IntegerCodec for i64 {
    // encodes a string to the following forma: [1B Type][8B i64 le Int]
    fn encode(&self) -> Rc<[u8; TYPE_LEN + INT_LEN]> {
        let mut buf = [0u8; TYPE_LEN + INT_LEN];

        buf[0] = TypeCol::INTEGER as u8;
        buf[TYPE_LEN..].copy_from_slice(&self.to_le_bytes());

        Rc::from(buf)
    }

    /// input assumes presence of type byte
    fn decode(data: &[u8]) -> Self {
        assert_eq!(data[0], TypeCol::INTEGER as u8);
        assert_eq!(data.len(), TYPE_LEN + INT_LEN);
        i64::from_le_bytes(data[TYPE_LEN..TYPE_LEN + INT_LEN].try_into().unwrap())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn codec1() {
        let key = format!("{}{}{}", 5, "column1", "column2").encode();
        let val = format!("{}", "some data").encode();
        assert_eq!(key.len(), 20);
        assert_eq!(val.len(), 14);
        assert_eq!(String::decode(&key), "5column1column2");
        assert_eq!(String::decode(&val), "some data");

        let mut buf = [0u8; (TYPE_LEN + INT_LEN) * 3];
        let v1: i64 = 5;
        let v2: i64 = 9;
        let v3: i64 = 13;

        buf[..TYPE_LEN + INT_LEN].copy_from_slice(&(*v1.encode()));
        buf[TYPE_LEN + INT_LEN..(TYPE_LEN + INT_LEN) * 2].copy_from_slice(&(*v2.encode()));
        buf[(TYPE_LEN + INT_LEN) * 2..].copy_from_slice(&(*v3.encode()));

        let v1 = i64::decode(&buf[..TYPE_LEN + INT_LEN]);
        let v2 = i64::decode(&buf[TYPE_LEN + INT_LEN..(TYPE_LEN + INT_LEN) * 2]);
        let v3 = i64::decode(&buf[(TYPE_LEN + INT_LEN) * 2..]);

        assert_eq!(v1, 5);
        assert_eq!(v2, 9);
        assert_eq!(v3, 13);

        let str = "primary key";
        let id: i64 = -10;
        let mut buf: Vec<u8> = Vec::new();

        buf.extend_from_slice(&(*id.encode()));
        buf.extend_from_slice(&str.to_string().encode());
        assert_eq!(
            buf.len(),
            TYPE_LEN + INT_LEN + TYPE_LEN + STR_PRE_LEN + str.len()
        );

        let decode = format!(
            "{}{}",
            i64::decode(&buf[0..TYPE_LEN + INT_LEN]),
            String::decode(&buf[TYPE_LEN + INT_LEN..])
        );
        assert_eq!(decode, "-10primary key");
    }
}
