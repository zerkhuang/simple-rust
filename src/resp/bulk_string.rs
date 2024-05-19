use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError};

use super::{extract_fixed_data, extract_sized_data};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct BulkString(pub(crate) Vec<u8>);

// - bulk string: "$<length>\r\n<data>\r\n"
impl RespEncoder for BulkString {
    fn encode(&self) -> Vec<u8> {
        format!("${}\r\n{}\r\n", self.len(), String::from_utf8_lossy(self)).into_bytes()
    }
}

impl RespDecoder for BulkString {
    const PREFIX: &'static str = "$";
    const N_CRLF: usize = 2;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_sized_data(buf, Self::PREFIX)?;
        Ok(BulkString::new(data))
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct NullBulkString;

// - null bulk string: "$-1\r\n"
impl RespEncoder for NullBulkString {
    fn encode(&self) -> Vec<u8> {
        "$-1\r\n".to_string().into_bytes()
    }
}

impl RespDecoder for NullBulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, Self::PREFIX, "-1", "NullBulkString")?;
        Ok(Self)
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
        let frame = NullBulkString;
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
        let frame = NullBulkString::decode(&mut buf)?;
        assert_eq!(frame, NullBulkString);

        let mut buf = BytesMut::from("$-2\r\n");
        let frame = NullBulkString::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid(
                "NullBulkString expected: -1, got: -2".to_string()
            ))
        );
        Ok(())
    }
}
