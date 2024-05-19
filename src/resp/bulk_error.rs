use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError};

use super::extract_sized_data;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct BulkError(pub(crate) Vec<u8>);

// - bulk error: "!<length>\r\n<error>\r\n"
impl RespEncoder for BulkError {
    fn encode(&self) -> Vec<u8> {
        format!(
            "!{}\r\n{}\r\n",
            self.0.len(),
            String::from_utf8_lossy(&self.0)
        )
        .into_bytes()
    }
}

impl RespDecoder for BulkError {
    const PREFIX: &'static str = "!";
    const N_CRLF: usize = 2;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_sized_data(buf, Self::PREFIX)?;
        Ok(BulkError::new(data))
    }
}

impl BulkError {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        Self(s.into())
    }
}

impl Deref for BulkError {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_bulk_error_encode() {
        let frame = BulkError::new(b"Error message");
        assert_eq!(frame.encode(), b"!13\r\nError message\r\n");
    }

    #[test]
    fn test_bulk_error_decode() -> Result<()> {
        let mut buf = BytesMut::from("!13\r\nError message\r\n");
        let frame = BulkError::decode(&mut buf)?;
        assert_eq!(frame, BulkError::new(b"Error message".to_vec()));

        let mut buf = BytesMut::from("!12\r\nError message\r\n");
        let frame = BulkError::decode(&mut buf);
        assert_eq!(frame, Err(RespError::InvalidFrameLength));

        Ok(())
    }
}
