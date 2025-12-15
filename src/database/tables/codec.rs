/*
 * helper functions for encoding/decoding of strings and integer
 */

use std::rc::Rc;

use super::tables::TypeCol;

/*
Key-Value LayoutV2 (WIP):
--------KEY---------|-----VAL----|
[HEADER][KEY1][KEY2]|[VAL1][VAL2]|

HEADER:
[2B Header Size][1B K1 type][1B K2 type][...]

Key-Value LayoutV1 (current):
-----KEY----|-----VAL----|
[KEY1][KEY2]|[VAL1][VAL2]|

example:
[INT ][STR ]
[1B TYPE][8B INT][1B TYPE][2B STRLEN][nB STR]
 */

pub const TYPE_LEN: usize = std::mem::size_of::<u8>();
pub const HEADER_SIZE: usize = std::mem::size_of::<u16>();

/// length prefix for strings is u32
pub(crate) const STR_PRE_LEN: usize = std::mem::size_of::<u32>();
/// length of integer it 8 bytes
pub(crate) const INT_LEN: usize = std::mem::size_of::<u64>();
pub(crate) const IDX_LEN: usize = std::mem::size_of::<u64>();
pub(crate) const TID_LEN: usize = std::mem::size_of::<u64>();

/// converts a String to bytes with a 4 byte length number + utf8 character
/// ```
/// let key = format!("{}{}{}", 5, "column1", "column2").encode();
/// let val = format!("{}", "some data").encode();
/// assert_eq!(key.len(), 20);
/// assert_eq!(val.len(), 14);
/// assert_eq!(String::decode(&key), "5column1column2");
/// assert_eq!(String::decode(&val), "some data");
/// ```
pub(super) trait Codec {
    fn encode(&self) -> Rc<[u8]>;
    fn decode(data: &[u8]) -> Self;
}

impl Codec for String {
    /// encodes a string to the following format:
    ///
    /// [1B Type] [4B len] [nB str]
    fn encode(&self) -> Rc<[u8]> {
        let len = self.len();
        let buf = Rc::<[u8]>::new_zeroed_slice(TYPE_LEN + len + STR_PRE_LEN);

        // SAFETY: array of u8 set to 0 qualifies as initialized
        let mut buf = unsafe { buf.assume_init() };
        let buf_ref = Rc::get_mut(&mut buf).unwrap();

        buf_ref[0] = TypeCol::BYTES as u8;
        buf_ref[TYPE_LEN..TYPE_LEN + STR_PRE_LEN].copy_from_slice(&(len as u32).to_le_bytes());
        buf_ref[TYPE_LEN + STR_PRE_LEN..].copy_from_slice(self.as_bytes());

        assert_eq!(buf.len(), TYPE_LEN + len + STR_PRE_LEN);
        buf
    }

    /// input assumes presence of type byte, doesnt include it in the output
    ///
    /// makes an allocation
    fn decode(data: &[u8]) -> String {
        assert_eq!(data[0], TypeCol::BYTES as u8);

        let len =
            u32::from_le_bytes(data[TYPE_LEN..TYPE_LEN + STR_PRE_LEN].try_into().unwrap()) as usize;

        assert!(data.len() >= TYPE_LEN + len + STR_PRE_LEN);
        // SAFETY: we encode in UTF-8
        unsafe {
            String::from_utf8_unchecked(
                data[TYPE_LEN + STR_PRE_LEN..TYPE_LEN + STR_PRE_LEN + len].to_vec(),
            )
        }
    }
}

impl Codec for i64 {
    // encodes a string to the following forma: [1B Type][8B i64 le Int]
    fn encode(&self) -> Rc<[u8]> {
        let mut buf = [0u8; TYPE_LEN + INT_LEN];

        buf[0] = TypeCol::INTEGER as u8;
        buf[TYPE_LEN..].copy_from_slice(&self.to_le_bytes());

        let out = Rc::new(buf);
        assert_eq!(out.len(), TYPE_LEN + INT_LEN);
        out
    }

    /// input assumes presence of type byte
    fn decode(data: &[u8]) -> Self {
        assert_eq!(data[0], TypeCol::INTEGER as u8);
        assert!(data.len() >= TYPE_LEN + INT_LEN);
        i64::from_le_bytes(data[TYPE_LEN..TYPE_LEN + INT_LEN].try_into().unwrap())
    }
}

/// utility functions with cursor functionality
pub(super) trait NumEncode {
    fn encode_i64(self, value: i64) -> Self;
}
impl NumEncode for &mut [u8] {
    fn encode_i64(self, value: i64) -> Self {
        let (head, tail) = self.split_at_mut(std::mem::size_of::<i64>() + TYPE_LEN);
        head.copy_from_slice(&value.encode());

        // head[0] = TypeCol::INTEGER as u8;
        // head[TYPE_LEN..].copy_from_slice(&value.to_le_bytes());
        // assert_eq!(head.len(), TYPE_LEN + INT_LEN);

        tail
    }
}

/// utility functions with cursor functionality
pub(super) trait NumDecode {
    fn decode_i64(&mut self) -> i64;
    fn read_i64(&mut self) -> i64;
    fn read_u32(&mut self) -> u32;
    fn read_u8(&mut self) -> u8;
}

impl NumDecode for &[u8] {
    /// moves the slice like a cursor
    ///
    /// considers encoding schema
    fn decode_i64(&mut self) -> i64 {
        assert_eq!(self[0], TypeCol::INTEGER as u8);
        let (head, tail) = self.split_at(std::mem::size_of::<i64>() + TYPE_LEN);
        *self = tail;
        i64::from_le_bytes(
            head[TYPE_LEN..TYPE_LEN + INT_LEN]
                .try_into()
                .expect("cast error read_u64"),
        )
    }
    /// moves the slice like a cursor
    ///
    /// warning: this function does not consider the type bit like decode()!
    fn read_i64(&mut self) -> i64 {
        let (head, tail) = self.split_at(std::mem::size_of::<i64>());
        *self = tail;
        i64::from_le_bytes(head.try_into().expect("cast error read_u64"))
    }

    /// moves the slice like a cursor
    ///  
    /// warning: this function does not consider the type bit like decode()!
    fn read_u32(&mut self) -> u32 {
        let (head, tail) = self.split_at(std::mem::size_of::<u32>());
        *self = tail;
        u32::from_le_bytes(head.try_into().expect("cast error read_u32"))
    }

    /// moves the slice like a cursor
    ///
    /// warning: this function does not consider the type bit like decode()!
    fn read_u8(&mut self) -> u8 {
        let (head, tail) = self.split_at(std::mem::size_of::<u8>());
        *self = tail;
        head[0]
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

    #[test]
    fn codec2() {
        let i1: i64 = 10;
        let i2: i64 = 5;
        let i3: i64 = 3;

        let mut buf = [0u8; (std::mem::size_of::<i64>() + TYPE_LEN) * 3];
        let write_slice = &mut buf[..];

        write_slice.encode_i64(i1).encode_i64(i2).encode_i64(i3);

        let mut read_slice = &buf[..];

        assert_eq!(read_slice.decode_i64(), 10);
        assert_eq!(read_slice.decode_i64(), 5);
        assert_eq!(read_slice.decode_i64(), 3);
    }
}
