/*
 * helper functions for encoding/decoding of strings and integer
 */

use std::rc::Rc;

use tracing::debug;

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

pub(crate) const TYPE_LEN: usize = std::mem::size_of::<u8>();
pub(crate) const HEADER_SIZE: usize = std::mem::size_of::<u16>();

pub(crate) const STR_PRE_LEN: usize = std::mem::size_of::<u32>();
pub(crate) const INT_LEN: usize = std::mem::size_of::<u64>();
pub(crate) const IDX_LEN: usize = std::mem::size_of::<u64>();
pub(crate) const TID_LEN: usize = std::mem::size_of::<u32>();
pub(crate) const PREFIX_LEN: usize = std::mem::size_of::<u16>();

pub(super) trait Codec {
    fn encode(&self) -> Rc<[u8]>;
    fn decode(data: &[u8]) -> Self;
}

impl Codec for String {
    /// output layout:
    ///
    /// [1B u8] [4B u32] [nB UTF8]
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

    /// assumes the the following layout:
    ///
    /// [1B u8] [4B u32] [nB UTF8]
    ///
    /// makes an allocation
    fn decode(data: &[u8]) -> String {
        debug_assert_eq!(data[0], TypeCol::BYTES as u8);

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
    /// output layout:
    ///
    /// (1B Type)(8B i64 le Int)
    fn encode(&self) -> Rc<[u8]> {
        let mut buf = [0u8; TYPE_LEN + INT_LEN];

        buf[0] = TypeCol::INTEGER as u8;
        buf[TYPE_LEN..].copy_from_slice(&self.to_le_bytes());

        let out = Rc::new(buf);
        debug_assert_eq!(out.len(), TYPE_LEN + INT_LEN);
        out
    }

    /// expected byte layout:
    ///
    /// (1B Type)(8B i64 le Int)
    fn decode(data: &[u8]) -> Self {
        debug_assert_eq!(data[0], TypeCol::INTEGER as u8);
        debug_assert!(data.len() >= TYPE_LEN + INT_LEN);
        i64::from_le_bytes(data[TYPE_LEN..TYPE_LEN + INT_LEN].try_into().unwrap())
    }
}

/// utility functions with cursor functionality
pub(crate) trait NumEncode {
    fn write_bytes(self, value: &[u8]) -> Self;

    fn write_i64(self, value: i64) -> Self;
    fn write_u64(self, value: u64) -> Self;
    fn write_u32(self, value: u32) -> Self;
    fn write_u16(self, value: u16) -> Self;
    fn write_u8(self, value: u8) -> Self;
}
impl NumEncode for &mut [u8] {
    fn write_bytes(self, value: &[u8]) -> Self {
        let (head, tail) = self.split_at_mut(value.len());
        head.copy_from_slice(value);
        tail
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn write_i64(self, value: i64) -> Self {
        let (head, tail) = self.split_at_mut(std::mem::size_of::<i64>());
        head.copy_from_slice(&value.to_le_bytes());
        tail
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn write_u64(self, value: u64) -> Self {
        let (head, tail) = self.split_at_mut(std::mem::size_of::<u64>());
        head.copy_from_slice(&value.to_le_bytes());
        tail
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn write_u32(self, value: u32) -> Self {
        let (head, tail) = self.split_at_mut(std::mem::size_of::<u32>());
        head.copy_from_slice(&value.to_le_bytes());
        tail
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn write_u16(self, value: u16) -> Self {
        let (head, tail) = self.split_at_mut(std::mem::size_of::<u16>());
        head.copy_from_slice(&value.to_le_bytes());
        tail
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn write_u8(self, value: u8) -> Self {
        let (head, tail) = self.split_at_mut(std::mem::size_of::<u8>());
        head.copy_from_slice(&value.to_le_bytes());
        tail
    }
}

/// utility functions with cursor functionality
pub(crate) trait NumDecode {
    fn read_bytes(&mut self, len: usize) -> &[u8];

    fn read_i64(&mut self) -> i64;
    fn read_u64(&mut self) -> u64;
    fn read_u32(&mut self) -> u32;
    fn read_u16(&mut self) -> u16;
    fn read_u8(&mut self) -> u8;
}

impl NumDecode for &[u8] {
    /// moves the slice like a cursor
    fn read_bytes(&mut self, len: usize) -> &[u8] {
        let (head, tail) = self.split_at(len);
        *self = tail;
        head
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn read_u64(&mut self) -> u64 {
        let (head, tail) = self.split_at(std::mem::size_of::<u64>());
        *self = tail;
        u64::from_le_bytes(head.try_into().expect("cast error read_u64"))
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn read_i64(&mut self) -> i64 {
        let (head, tail) = self.split_at(std::mem::size_of::<i64>());
        *self = tail;
        i64::from_le_bytes(head.try_into().expect("cast error read_u64"))
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn read_u32(&mut self) -> u32 {
        let (head, tail) = self.split_at(std::mem::size_of::<u32>());
        *self = tail;
        u32::from_le_bytes(head.try_into().expect("cast error read_u32"))
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn read_u16(&mut self) -> u16 {
        let (head, tail) = self.split_at(std::mem::size_of::<u16>());
        *self = tail;
        u16::from_le_bytes(head.try_into().expect("cast error read_u16"))
    }
    /// moves the slice like a cursor, warning: this function does not consider the type bit like decode()!
    fn read_u8(&mut self) -> u8 {
        let (head, tail) = self.split_at(std::mem::size_of::<u8>());
        *self = tail;
        head[0]
    }
}

#[cfg(test)]
mod test {
    use crate::database::tables::{Key, Value};
    use std::error::Error;
    use test_log::test;

    use super::*;

    #[test]
    fn codec1() -> Result<(), Box<dyn Error>> {
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
        Ok(())
    }

    #[test]
    fn codec3() -> Result<(), Box<dyn Error>> {
        let i1: u16 = 5;
        let i2: u32 = 7;
        let i3: u64 = 9;
        let mut buf = [0u8; 2 + 4 + 8];

        buf.write_u16(i1).write_u32(i2).write_u64(i3);

        let mut r_slice = &buf[..];
        assert_eq!(r_slice.read_u16(), 5);
        assert_eq!(r_slice.read_u32(), 7);
        assert_eq!(r_slice.read_u64(), 9);
        Ok(())
    }

    #[test]
    fn codec4() -> Result<(), Box<dyn Error>> {
        let i1: u16 = 5;
        let i2: u32 = 7;
        let i3: u64 = 9;
        let mut buf = [0u8; 2 + 4 + 8 + 100];
        let key: Key = "hello".into();
        let val: Value = "world".into();

        assert_eq!(key.to_string(), "1 hello");

        let w_slice = &mut buf[..];
        w_slice
            .write_u16(i1)
            .write_u32(i2)
            .write_u64(i3)
            .write_bytes(key.as_slice())
            .write_bytes(val.as_slice());

        let mut r_slice = &buf[..];
        assert_eq!(r_slice.read_u16(), 5);
        assert_eq!(r_slice.read_u32(), 7);
        assert_eq!(r_slice.read_u64(), 9);

        assert_eq!(
            Key::from_encoded_slice(r_slice.read_bytes(key.len())).to_string(),
            "1 hello"
        );
        assert_eq!(
            Value::from_encoded_slice(r_slice.read_bytes(val.len())).to_string(),
            "world"
        );
        Ok(())
    }

    #[test]
    fn codec6() -> Result<(), Box<dyn Error>> {
        let i1: u16 = 5;
        let mut buf = [0u8; 100];
        let slice = &mut buf[..];
        let key: Key = Key::from_unencoded_str(format!("{i1}"));
        slice.write_bytes(key.as_slice());

        assert_eq!(
            Key::from_encoded_slice((&buf[..]).read_bytes(key.len())).to_string(),
            "1 5"
        );
        Ok(())
    }
}
