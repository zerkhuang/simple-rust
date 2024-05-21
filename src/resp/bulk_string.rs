use std::ops::Deref;

use bytes::{Buf, BytesMut};

use crate::{RespDecoder, RespEncoder, RespError};

use super::{extract_data, extract_length_data, find_crlf, CRLF, CRLF_LEN};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Hash)]
pub struct BulkString(pub(crate) Vec<u8>);

// - bulk string: "$<length>\r\n<data>\r\n"
// - null bulk string: "$-1\r\n"
impl RespEncoder for BulkString {
    fn encode(&self) -> Vec<u8> {
        if self.is_empty() {
            return "$-1\r\n".to_string().into_bytes();
        }
        format!("${}\r\n{}\r\n", self.len(), String::from_utf8_lossy(self)).into_bytes()
    }
}

impl RespDecoder for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let len_data = extract_length_data(buf, Self::PREFIX)?;
        if len_data == "-1" {
            buf.advance(3 + CRLF_LEN);
            return Ok(BulkString::new(""));
        }
        let len = len_data
            .parse::<usize>()
            .map_err(|_| RespError::InvalidFrameLength)?;
        let data = extract_data(
            buf,
            format!("{}{}{}", Self::PREFIX, len_data, CRLF).as_str(),
        )?;
        if data.len() != len {
            return Err(RespError::InvalidFrameLength);
        }
        Ok(BulkString::new(data))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let len_end = find_crlf(buf, 1, 1).ok_or(RespError::Incomplete)?;
        let data_start = len_end + CRLF_LEN;
        if &buf[1..len_end] == b"-1" {
            return Ok(data_start);
        }
        let end = find_crlf(&buf[data_start..], 1, 0).ok_or(RespError::Incomplete)?;
        Ok(data_start + end + CRLF_LEN)
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        Self(s.into())
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&[u8]> for BulkString {
    fn from(s: &[u8]) -> Self {
        Self(s.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(s: &[u8; N]) -> Self {
        Self(s.to_vec())
    }
}

impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_bulk_string_encode() {
        let frame = BulkString::new(b"Hello");
        assert_eq!(frame.encode(), b"$5\r\nHello\r\n");
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame = BulkString::new("");
        assert_eq!(frame.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$13\r\nHello, world!\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"Hello, world!".to_vec()));

        let mut buf = BytesMut::from("$13\r\nHello, world\r\n");
        let frame = BulkString::decode(&mut buf);
        assert_eq!(frame, Err(RespError::InvalidFrameLength));

        Ok(())
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$-1\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(""));

        let mut buf = BytesMut::from("$-2\r\n");
        let frame = BulkString::decode(&mut buf);
        assert_eq!(frame, Err(RespError::InvalidFrameLength));
        Ok(())
    }
}
