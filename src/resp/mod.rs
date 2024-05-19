mod array;
mod bool;
mod bulk_error;
mod bulk_string;
mod double;
mod frame;
mod integer;
mod map;
mod null;
mod set;
mod simple_error;
mod simple_string;

use bytes::{Buf as _, BytesMut};
use enum_dispatch::enum_dispatch;
use thiserror::Error;

pub use self::{
    array::{RespArray, RespNullArray},
    bulk_error::BulkError,
    bulk_string::{BulkString, NullBulkString},
    double::RespDouble,
    frame::RespFrame,
    map::RespMap,
    null::RespNull,
    set::RespSet,
    simple_error::SimpleError,
    simple_string::SimpleString,
};

const CRLF_LEN: usize = 2;

#[enum_dispatch]
pub trait RespEncoder {
    fn encode(&self) -> Vec<u8>;
}

pub trait RespDecoder: Sized {
    const PREFIX: &'static str;
    const N_CRLF: usize = 1;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, Self::N_CRLF).ok_or(RespError::Incomplete)?;
        Ok(end + CRLF_LEN)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RespError {
    #[error("Frame is incomplete")]
    Incomplete,
    #[error("Invalid frame: {0}")]
    Invalid(String),
    #[error("Invalid frame length")]
    InvalidFrameLength,
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
}

fn find_crlf(buf: &[u8], nth: usize) -> Option<usize> {
    let mut count = 0;
    for i in 0..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            count += 1;
            if count == nth {
                return Some(i);
            }
        }
    }
    None
}

fn validate_frame_data(buf: &mut BytesMut, prefix: &str) -> Result<(), RespError> {
    if buf.len() < CRLF_LEN + prefix.len() {
        return Err(RespError::Incomplete);
    }
    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "Invalid frame: {:?}",
            buf
        )));
    }
    Ok(())
}

fn extract_nth_and_position(buf: &[u8]) -> Result<(usize, usize), RespError> {
    let position = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
    let nth = String::from_utf8_lossy(&buf[1..position])
        .parse::<usize>()
        .map_err(|_| RespError::Invalid(String::from_utf8_lossy(buf).to_string()))?;
    Ok((nth, position))
}

fn extract_data(buf: &mut BytesMut, prefix: &str) -> Result<String, RespError> {
    validate_frame_data(buf, prefix)?;
    let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
    let data = buf.split_to(end + CRLF_LEN);
    let s = String::from_utf8_lossy(&data[prefix.len()..end]).to_string();
    Ok(s)
}

fn extract_length(buf: &mut BytesMut, prefix: &str) -> Result<usize, RespError> {
    let data = extract_data(buf, prefix)?;
    let len = data
        .parse::<usize>()
        .map_err(|_| RespError::InvalidFrameLength)?;
    Ok(len)
}

fn extract_sized_data(buf: &mut BytesMut, prefix: &str) -> Result<String, RespError> {
    let len = extract_length(buf, prefix)?;
    let data = extract_data(buf, "")?;
    if data.len() != len {
        return Err(RespError::InvalidFrameLength);
    }
    Ok(data)
}

fn extract_fixed_data(
    buf: &mut BytesMut,
    prefix: &str,
    except_data: &str,
    frame_type: &str,
) -> Result<(), RespError> {
    validate_frame_data(buf, prefix)?;
    let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
    if &buf[prefix.len()..end] != except_data.as_bytes() {
        return Err(RespError::Invalid(format!(
            "{} expected: {}, got: {}",
            frame_type,
            except_data,
            String::from_utf8_lossy(&buf[prefix.len()..end])
        )));
    }
    buf.advance(end + CRLF_LEN);
    Ok(())
}
