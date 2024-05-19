use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError};

use super::extract_data;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespDouble(pub(crate) String);

// - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
impl RespEncoder for RespDouble {
    fn encode(&self) -> Vec<u8> {
        format!(",{}\r\n", self.0).into_bytes()
    }
}

impl RespDecoder for RespDouble {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = data
            .parse::<f64>()
            .map_err(|_| RespError::Invalid(format!("Parse failed: {:?}", data)))?;
        Ok(RespDouble::new(frame))
    }
}

impl RespDouble {
    pub fn new(s: f64) -> Self {
        let s = if s.abs() > 1e8 {
            format!("{:+e}", s)
        } else {
            format!("{:+}", s)
        };
        Self(s)
    }
}

impl Deref for RespDouble {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<f64> for RespDouble {
    fn from(s: f64) -> Self {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_double_encode() {
        let frame = RespDouble::new(123.456);
        assert_eq!(frame.encode(), b",+123.456\r\n");

        let frame = RespDouble::new(-123.456);
        assert_eq!(frame.encode(), b",-123.456\r\n");

        let frame = RespDouble::new(1.23456e8);
        assert_eq!(frame.encode(), b",+1.23456e8\r\n");

        let frame = RespDouble::new(-1.23456e8);
        assert_eq!(frame.encode(), b",-1.23456e8\r\n");
    }

    #[test]
    fn test_f64_decode() -> Result<()> {
        let mut buf = BytesMut::from(",+123.45\r\n");
        let frame = RespDouble::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(123.45));

        let mut buf = BytesMut::from(",+1.23456e8\r\n");
        let frame = RespDouble::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(1.23456e8));

        let mut buf = BytesMut::from(",+123.45x\r\n");
        let frame = RespDouble::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid("Parse failed: \"+123.45x\"".to_string()))
        );
        Ok(())
    }
}
