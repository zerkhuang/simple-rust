use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError, RespFrame};

use super::{extract_fixed_data, extract_length, extract_nth_and_position, CRLF_LEN};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespArray(pub(crate) Vec<RespFrame>);

// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
//         - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
impl RespEncoder for RespArray {
    fn encode(&self) -> Vec<u8> {
        let mut encoded = format!("*{}\r\n", self.len()).into_bytes();
        for frame in &self.0 {
            encoded.extend_from_slice(&frame.encode());
        }
        encoded
    }
}

impl RespDecoder for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let total = Self::expect_length(buf)?;
        if buf.len() < total {
            return Err(RespError::Incomplete);
        }
        let nth = extract_length(buf, Self::PREFIX)?;
        let mut frames = Vec::with_capacity(nth);
        for _ in 0..nth {
            let frame = RespFrame::decode(buf)?;
            frames.push(frame);
        }
        Ok(RespArray::new(frames))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (nth, position) = extract_nth_and_position(buf)?;
        let mut total = position + CRLF_LEN;
        for _ in 0..nth {
            let frame_len = RespFrame::expect_length(&buf[total..])?;
            total += frame_len;
        }
        Ok(total)
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespNullArray;

// - null array: "*-1\r\n"
impl RespEncoder for RespNullArray {
    fn encode(&self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

impl RespDecoder for RespNullArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, Self::PREFIX, "-1", "NullArray")?;
        Ok(Self)
    }
}

impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        Self(s.into())
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::SimpleString;

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_array_encode() {
        let frame = RespArray::new(vec![b"get".into(), SimpleString::new("hello").into()]);
        assert_eq!(frame.encode(), b"*2\r\n$3\r\nget\r\n+hello\r\n");
    }

    #[test]
    fn test_null_array_encode() {
        let frame = RespNullArray;
        assert_eq!(frame.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new(vec![b"get".into(), b"hello".into()]));

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nset\r\n");
        let ret = RespArray::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::Incomplete);

        buf.extend_from_slice(b"$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new([b"set".into(), b"hello".into()]));

        Ok(())
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let frame = RespNullArray::decode(&mut buf)?;
        assert_eq!(frame, RespNullArray);

        let mut buf = BytesMut::from("*-2\r\n");
        let frame = RespNullArray::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid(
                "NullArray expected: -1, got: -2".to_string()
            ))
        );
        Ok(())
    }
}
