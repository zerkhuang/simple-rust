use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError};

use super::extract_data;

// - integer: ":[<+|->]<value>\r\n"
impl RespEncoder for i64 {
    fn encode(&self) -> Vec<u8> {
        let sign = if *self < 0 { "" } else { "+" };
        format!(":{}{}\r\n", sign, self).into_bytes()
    }
}

impl RespDecoder for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = data
            .parse::<i64>()
            .map_err(|_| RespError::Invalid(format!("Parse failed: {:?}", data)))?;
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_integer_encode() {
        let frame = 123;
        assert_eq!(frame.encode(), b":+123\r\n");

        let frame = -123;
        assert_eq!(frame.encode(), b":-123\r\n");
    }

    #[test]
    fn test_integer_decode() -> Result<()> {
        let mut buf = BytesMut::from(":123\r\n");
        let frame = i64::decode(&mut buf)?;
        assert_eq!(frame, 123);

        let mut buf = BytesMut::from(":xxx\r\n");
        let frame = i64::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid("Parse failed: \"xxx\"".to_string()))
        );

        Ok(())
    }
}
