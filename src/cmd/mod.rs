mod hmap;
mod map;

use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;
use thiserror::Error;

use crate::{Backend, RespArray, RespError, RespFrame, SimpleString};

pub use self::{
    hmap::{HGet, HGetAll, HSet},
    map::{Get, Set},
};

// lazy_static 懒加载
lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("OK").into();
}

#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(&self, backend: &Backend) -> RespFrame;
}

#[enum_dispatch(CommandExecutor)]
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid command arguments: {0}")]
    InvalidArguments(String),

    #[error("{0}")]
    RespError(#[from] RespError),

    #[error("{0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

impl TryFrom<RespArray> for Command {
    type Error = CommandError;

    fn try_from(array: RespArray) -> Result<Self, Self::Error> {
        match array.first() {
            Some(RespFrame::BulkString(cmd)) => match cmd.as_ref() {
                b"get" => Ok(Get::try_from(array)?.into()),
                b"set" => Ok(Set::try_from(array)?.into()),
                b"hget" => Ok(HGet::try_from(array)?.into()),
                b"hset" => Ok(HSet::try_from(array)?.into()),
                b"hgetall" => Ok(HGetAll::try_from(array)?.into()),
                _ => Err(CommandError::InvalidCommand(format!(
                    "Invalid command: {}",
                    String::from_utf8_lossy(cmd)
                ))),
            },
            _ => Err(CommandError::InvalidCommand(
                "Command must be a BulkString frame".to_string(),
            )),
        }
    }
}

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;

    fn try_from(frame: RespFrame) -> Result<Self, Self::Error> {
        match frame {
            RespFrame::Array(array) => Command::try_from(array),
            _ => Err(CommandError::InvalidCommand(
                "Command must be an Array frame".to_string(),
            )),
        }
    }
}

fn validate_command(
    frames: &RespArray,
    keys: &[&'static str],
    n_args: usize,
) -> Result<(), CommandError> {
    if frames.len() != keys.len() + n_args {
        return Err(CommandError::InvalidArguments(format!(
            "Expected {} arguments, got {}",
            n_args,
            frames.len() - keys.len()
        )));
    }
    // 校验 keys 是否匹配
    for (i, key) in keys.iter().enumerate() {
        match frames[i] {
            RespFrame::BulkString(ref cmd) => {
                if cmd.as_ref().to_ascii_lowercase() != key.as_bytes() {
                    return Err(CommandError::InvalidCommand(format!(
                        "Expected key {}, got {}",
                        key,
                        String::from_utf8_lossy(cmd)
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must be a BulkString frame".to_string(),
                ));
            }
        }
    }
    Ok(())
}

pub fn extract_args(frames: RespArray, start: usize) -> Result<Vec<RespFrame>, CommandError> {
    Ok(frames.0.into_iter().skip(start).collect::<Vec<RespFrame>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RespDecoder, RespNull};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_command() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let cmd: Command = frame.try_into()?;

        let backend = Backend::new();

        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Null(RespNull));

        Ok(())
    }
}
