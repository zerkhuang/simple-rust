use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError};

use super::extract_fixed_data;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespNull;

// - null: "_\r\n"
impl RespEncoder for RespNull {
    fn encode(&self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespDecoder for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, Self::PREFIX, "", "NullArray")?;
        Ok(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_null_encode() {
        let frame = RespNull;
        assert_eq!(frame.encode(), b"_\r\n");
    }

    #[test]
    fn test_null_decode() -> Result<()> {
        let mut buf = BytesMut::from("_\r\n");
        let frame = RespNull::decode(&mut buf)?;
        assert_eq!(frame, RespNull);

        let mut buf = BytesMut::from("_x\r\n");
        let frame = RespNull::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid(
                "NullArray expected: , got: x".to_string()
            ))
        );
        Ok(())
    }
}
