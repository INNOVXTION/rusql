/*
 * helper functions for decoding purposes
 */

use std::rc::Rc;

// converts a String to bytes with a 4 byte length number + utf8 character
pub(super) trait StringCodec {
    fn encode(self) -> Rc<[u8]>;
    fn decode(data: &[u8]) -> String;
}

impl StringCodec for String {
    fn encode(self) -> Rc<[u8]> {
        let len = self.len();
        let mut buf = Vec::<u8>::with_capacity(len + 4);
        buf[..4].copy_from_slice(&(len as u32).to_be_bytes());
        buf[4..].copy_from_slice(&self.into_bytes());
        Rc::from(buf)
    }

    fn decode(data: &[u8]) -> String {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&data[..4]);
        let len = u32::from_le_bytes(buf) as usize;
        assert_eq!(data.len(), len + 4);
        // SAFETY: we encode in UTF-8
        unsafe { String::from_utf8_unchecked(data[4..4 + len].to_vec()) }
    }
}

// converts an Integer to little endian bytes
pub(super) trait IntegerCodec {
    fn encode(&self) -> Rc<[u8; 8]>;
    fn decode(data: &[u8]) -> Self;
}

impl IntegerCodec for i64 {
    fn encode(&self) -> Rc<[u8; 8]> {
        Rc::from(self.to_le_bytes())
    }

    fn decode(data: &[u8]) -> Self {
        assert_eq!(data.len(), 8);
        let mut buf = [0u8; 8];
        buf.copy_from_slice(data);
        i64::from_le_bytes(buf)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn codec_test() {
        let key = format!("{}{}{}", 5, "column1", "column2").encode();
        let val = format!("{}", "some data").encode();
        assert_eq!(key.len(), 19);
        assert_eq!(val.len(), 13);
        assert_eq!(String::decode(&key), "5column1column2");
        assert_eq!(String::decode(&val), "some data");

        let mut block = [0u8; 24];
        let v1 = 5i64;
        let v2 = 9i64;
        let v3 = 13i64;

        block[0..8].copy_from_slice(&(*v1.encode()));
        block[8..16].copy_from_slice(&(*v2.encode()));
        block[16..].copy_from_slice(&(*v3.encode()));

        let v1 = i64::decode(&block[..8]);
        let v2 = i64::decode(&block[8..16]);
        let v3 = i64::decode(&block[16..]);

        assert_eq!(v1, 5);
        assert_eq!(v2, 9);
        assert_eq!(v3, 13);
    }
}
