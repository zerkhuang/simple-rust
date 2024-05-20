use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError, RespFrame, SimpleString};

use super::{extract_len_and_end, extract_nth, CRLF_LEN};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespMap(pub(crate) BTreeMap<String, RespFrame>);

// - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
impl RespEncoder for RespMap {
    fn encode(&self) -> Vec<u8> {
        let mut encoded = format!("%{}\r\n", self.len()).into_bytes();
        for (key, value) in &self.0 {
            encoded.extend_from_slice(&SimpleString::new(key).encode());
            encoded.extend_from_slice(&value.encode());
        }
        encoded
    }
}

impl RespDecoder for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let total = Self::expect_length(buf)?;
        if buf.len() < total {
            return Err(RespError::Incomplete);
        }

        let nth = extract_nth(buf, Self::PREFIX)?;
        let mut map = Self::new();
        for _ in 0..nth {
            let key = SimpleString::decode(buf)?;
            let value = RespFrame::decode(buf)?;
            map.0.insert(key.0, value);
        }
        Ok(map)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (len, end) = extract_len_and_end(buf)?;
        let mut total = end + CRLF_LEN;
        for _ in 0..len {
            let key_len = RespFrame::expect_length(&buf[total..])?;
            let value_len = RespFrame::expect_length(&buf[total + key_len..])?;
            total += key_len + value_len;
        }
        Ok(total)
    }
}

impl RespMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }
}

impl Default for RespMap {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for RespMap {
    type Target = BTreeMap<String, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::{BulkString, RespDouble};

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_map_encode() {
        let mut frame = RespMap::new();
        frame.insert(
            "hello".to_string(),
            BulkString::new("world".to_string()).into(),
        );
        frame.insert("foo".to_string(), RespDouble::new(-123456.789).into());
        assert_eq!(
            frame.encode(),
            b"%2\r\n+foo\r\n,-123456.789\r\n+hello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_map_decode() -> Result<()> {
        let mut buf = BytesMut::from("%2\r\n+get\r\n$5\r\nhello\r\n+set\r\n$5\r\nworld\r\n");
        let frame = RespMap::decode(&mut buf)?;
        let mut map = RespMap::new();
        map.0.insert("get".to_string(), b"hello".into());
        map.0.insert("set".to_string(), b"world".into());
        assert_eq!(frame, map);

        Ok(())
    }
}
