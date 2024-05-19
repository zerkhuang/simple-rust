use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError};

use super::extract_data;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct SimpleError(pub(crate) String);

// - error: "-Error message\r\n"
impl RespEncoder for SimpleError {
    fn encode(&self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

impl RespDecoder for SimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = SimpleError::new(data);
        Ok(frame)
    }
}

impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl Deref for SimpleError {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_simple_error_encode() {
        let frame = SimpleError::new("Error message");
        assert_eq!(frame.encode(), b"-Error message\r\n");
    }

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::from("-Error message\r\n");
        let frame = SimpleError::decode(&mut buf)?;
        assert_eq!(frame, SimpleError::new("Error message".to_string()));
        Ok(())
    }
}
