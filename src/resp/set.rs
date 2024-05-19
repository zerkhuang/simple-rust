use std::{
    collections::BTreeSet,
    ops::{Deref, DerefMut},
};

use bytes::BytesMut;

use crate::{RespDecoder, RespEncoder, RespError, RespFrame};

use super::{extract_length, extract_nth_and_position, CRLF_LEN};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespSet(pub(crate) BTreeSet<RespFrame>);

// - set: "~<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncoder for RespSet {
    fn encode(&self) -> Vec<u8> {
        let mut encoded = format!("~{}\r\n", self.len()).into_bytes();
        for frame in &self.0 {
            encoded.extend_from_slice(&frame.encode());
        }
        encoded
    }
}

impl RespDecoder for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let total = Self::expect_length(buf)?;
        if buf.len() < total {
            return Err(RespError::Incomplete);
        }
        let nth = extract_length(buf, Self::PREFIX)?;
        let mut frames = RespSet::new();
        for _ in 0..nth {
            let frame = RespFrame::decode(buf)?;
            frames.insert(frame);
        }
        Ok(frames)
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

impl Default for RespSet {
    fn default() -> Self {
        Self::new()
    }
}

impl RespSet {
    pub fn new() -> Self {
        Self(BTreeSet::new())
    }
}

impl Deref for RespSet {
    type Target = BTreeSet<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::RespArray;

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_set_encode() {
        let mut set = RespSet::new();
        set.insert(RespArray::new(vec![1234.into(), true.into()]).into());
        set.insert(b"world".into());
        set.insert(b"world".into());
        assert_eq!(
            &set.encode(),
            b"~2\r\n$5\r\nworld\r\n*2\r\n:+1234\r\n#t\r\n"
        );
    }

    #[test]
    fn test_set_decode() -> Result<()> {
        let mut buf = BytesMut::from("~2\r\n$3\r\nget\r\n$5\r\nhello\r\n");
        let frame: RespSet = RespSet::decode(&mut buf)?;
        let mut set = RespSet::new();
        set.insert(b"get".into());
        set.insert(b"hello".into());
        assert_eq!(frame, set);

        Ok(())
    }
}
