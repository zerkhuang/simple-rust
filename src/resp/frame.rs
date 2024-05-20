use bytes::BytesMut;
use enum_dispatch::enum_dispatch;

use crate::{
    BulkError, BulkString, RespArray, RespDecoder, RespDouble, RespError, RespMap, RespNull,
    RespSet, SimpleError, SimpleString,
};

#[enum_dispatch(RespEncoder)]
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub enum RespFrame {
    SimpleString(SimpleString),
    Error(SimpleError),
    BulkError(BulkError),
    Integer(i64),
    BulkString(BulkString),
    Array(RespArray),
    Null(RespNull),
    Boolean(bool),
    Double(RespDouble),
    Map(RespMap),
    Set(RespSet),
}

impl RespDecoder for RespFrame {
    const PREFIX: &'static str = "";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        // 使用迭代器方式可以避免buf长度为0时的panic
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => SimpleString::decode(buf).map(RespFrame::SimpleString),
            Some(b'-') => SimpleError::decode(buf).map(RespFrame::Error),
            Some(b'!') => BulkError::decode(buf).map(RespFrame::BulkError),
            Some(b':') => i64::decode(buf).map(RespFrame::Integer),
            Some(b'$') => BulkString::decode(buf).map(RespFrame::BulkString),
            Some(b'_') => RespNull::decode(buf).map(RespFrame::Null),
            Some(b'#') => bool::decode(buf).map(RespFrame::Boolean),
            Some(b',') => RespDouble::decode(buf).map(RespFrame::Double),
            Some(b'*') => RespArray::decode(buf).map(RespFrame::Array),
            Some(b'%') => {
                let frame = RespMap::decode(buf)?;
                Ok(RespFrame::Map(frame))
            }
            Some(b'~') => {
                let frame = RespSet::decode(buf)?;
                Ok(RespFrame::Set(frame))
            }
            None => Err(RespError::Incomplete),
            _ => Err(RespError::InvalidFrameType(format!(
                "Invalid frame: {:?}",
                buf
            ))),
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        if buf.len() < 3 {
            return Err(RespError::Incomplete);
        }
        match buf[0] {
            b'+' => SimpleString::expect_length(buf),
            b'-' => SimpleError::expect_length(buf),
            b'!' => BulkError::expect_length(buf),
            b':' => i64::expect_length(buf),
            b'$' => BulkString::expect_length(buf),
            b'*' => RespArray::expect_length(buf),
            b'_' => RespNull::expect_length(buf),
            b'#' => bool::expect_length(buf),
            b',' => RespDouble::expect_length(buf),
            b'%' => RespMap::expect_length(buf),
            b'~' => RespSet::expect_length(buf),
            _ => Err(RespError::InvalidFrameType(format!(
                "Invalid frame: {:?}",
                buf
            ))),
        }
    }
}

impl From<&str> for RespFrame {
    fn from(s: &str) -> Self {
        SimpleString::from(s).into()
    }
}

impl From<&[u8]> for RespFrame {
    fn from(s: &[u8]) -> Self {
        BulkString::from(s).into()
    }
}

impl<const N: usize> From<&[u8; N]> for RespFrame {
    fn from(s: &[u8; N]) -> Self {
        BulkString::from(s).into()
    }
}

impl From<f64> for RespFrame {
    fn from(s: f64) -> Self {
        s.into()
    }
}
