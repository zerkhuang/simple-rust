use anyhow::Result;

use crate::{Backend, BulkString, RespArray, RespFrame};

use super::{extract_args, validate_command, CommandError, CommandExecutor};

// echo message *2\r\n$4\r\necho\r\n$5\r\nhello\r\n
#[derive(Debug)]
pub struct Echo {
    message: String,
}

impl CommandExecutor for Echo {
    fn execute(&self, _backend: &Backend) -> RespFrame {
        RespFrame::BulkString(BulkString::new(self.message.to_string()))
    }
}

impl TryFrom<RespArray> for Echo {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["echo"], 1)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(message)) => Ok(Self {
                message: String::from_utf8(message.0)?,
            }),
            _ => Err(CommandError::InvalidArguments(
                "Invalid message".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RespDecoder;

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_echo_command() -> Result<()> {
        let mut buf = BytesMut::from("*2\r\n$4\r\necho\r\n$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let cmd = Echo::try_from(frame)?;

        assert_eq!(cmd.message, "hello");

        Ok(())
    }
}
