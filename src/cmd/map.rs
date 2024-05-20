use crate::{Backend, RespArray, RespFrame, RespNull};

use super::{extract_args, validate_command, CommandError, CommandExecutor, RESP_OK};
//     - GET key ("*2\r\n$3\r\nget\r\n$5\r\nhello\r\n")
#[derive(Debug)]
pub struct Get {
    key: String,
}

//     - SET key val ("*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

impl CommandExecutor for Get {
    fn execute(&self, backend: &Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for Set {
    fn execute(&self, backend: &Backend) -> RespFrame {
        backend.set(self.key.clone(), self.value.clone());
        RESP_OK.clone()
    }
}

// 2\r\n$3\r\nget\r\n$5\r\nhello\r\n
impl TryFrom<RespArray> for Get {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["get"], 1)?;
        if arr.len() != 2 {
            return Err(CommandError::InvalidCommand(
                "GET command must have 2 arguments".to_string(),
            ));
        }

        let mut args = extract_args(arr, 1)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Self {
                key: String::from_utf8(key.0)?, // 这里抛出std::string::FromUtf8Error，所以 CommandError 也需要有个 Utf8Error，否者转换不了
            }),
            _ => Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        }
    }
}

// "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
impl TryFrom<RespArray> for Set {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["set"], 2)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        };

        let value = match args.next() {
            Some(value) => value,
            _ => return Err(CommandError::InvalidArguments("Invalid Value".to_string())),
        };

        Ok(Self { key, value })
    }
}

#[cfg(test)]
mod tests {
    use crate::RespDecoder;

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_get_try_from() -> Result<()> {
        let mut buf = BytesMut::from("*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let get: Get = frame.try_into()?;
        assert_eq!(get.key, "hello");

        Ok(())
    }

    #[test]
    fn test_set_try_from() -> Result<()> {
        let mut buf = BytesMut::from("*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let set: Set = frame.try_into()?;
        assert_eq!(set.key, "hello");
        assert_eq!(set.value, RespFrame::BulkString(b"world".into()));

        Ok(())
    }

    #[test]
    fn test_set_get_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = Set {
            key: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = Get {
            key: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString(b"world".into()));

        Ok(())
    }
}
