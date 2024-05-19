use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError};

use super::extract_data;

// - boolean: "#<t|f>\r\n"
impl RespEncoder for bool {
    fn encode(&self) -> Vec<u8> {
        format!("#{}\r\n", if *self { "t" } else { "f" }).into_bytes()
    }
}

impl RespDecoder for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = match data.as_str() {
            "t" => true,
            "f" => false,
            _ => return Err(RespError::Invalid(format!("Invalid bool: {:?}", data))),
        };
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_boolean_encode() {
        let frame = true;
        assert_eq!(frame.encode(), b"#t\r\n");

        let frame = false;
        assert_eq!(frame.encode(), b"#f\r\n");
    }

    #[test]
    fn test_bool_decode() -> Result<()> {
        let mut buf = BytesMut::from("#t\r\n");
        let frame = bool::decode(&mut buf)?;
        assert!(frame);

        let mut buf = BytesMut::from("#f\r\n");
        let frame = bool::decode(&mut buf)?;
        assert!(!frame);

        let mut buf = BytesMut::from("#x\r\n");
        let frame = bool::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid("Invalid bool: \"x\"".to_string()))
        );
        Ok(())
    }
}
