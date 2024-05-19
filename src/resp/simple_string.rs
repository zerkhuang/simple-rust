use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError};

use super::extract_data;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct SimpleString(pub(crate) String);

// - simple string: "+OK\r\n"
impl RespEncoder for SimpleString {
    fn encode(&self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl RespDecoder for SimpleString {
    const PREFIX: &'static str = "+";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = SimpleString::new(data);
        Ok(frame)
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for SimpleString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_simple_string_encode() {
        let frame = SimpleString::new("OK");
        assert_eq!(frame.encode(), b"+OK\r\n");
    }

    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("+OK\r\n");
        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("OK".to_string()));

        let mut buf = BytesMut::from("+OK\r");
        let frame = SimpleString::decode(&mut buf);
        assert_eq!(frame, Err(RespError::Incomplete));

        let mut buf = BytesMut::from("-OK\r\n");
        let frame = SimpleString::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::InvalidFrameType(
                "Invalid frame: b\"-OK\\r\\n\"".to_string()
            ))
        );
        Ok(())
    }
}
