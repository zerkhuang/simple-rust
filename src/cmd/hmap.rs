use crate::{Backend, RespArray, RespFrame, RespMap, RespNull};

use super::{
    extract_args, validate_command, CommandError, CommandExecutor, HGet, HGetAll, HSet, RESP_OK,
};

impl CommandExecutor for HGet {
    fn execute(&self, backend: &Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for HSet {
    fn execute(&self, backend: &Backend) -> RespFrame {
        backend.hset(self.key.clone(), self.field.clone(), self.value.clone());
        RESP_OK.clone()
    }
}

impl CommandExecutor for HGetAll {
    fn execute(&self, backend: &Backend) -> RespFrame {
        match backend.hgetall(&self.key) {
            Some(value) => {
                let mut map = RespMap::default();
                for v in value.iter() {
                    let key = v.key().to_owned();
                    let value = v.value().clone();
                    map.insert(key, value);
                }
                map.into()
            }
            None => RespFrame::Null(RespNull),
        }
    }
}

// *3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n
impl TryFrom<RespArray> for HGet {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["hget"], 2)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        let field = match args.next() {
            Some(RespFrame::BulkString(field)) => String::from_utf8(field.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        };

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Field".to_string())),
        };

        Ok(Self { key, field })
    }
}

// *4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n
impl TryFrom<RespArray> for HSet {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["hset"], 3)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        };

        let field = match args.next() {
            Some(RespFrame::BulkString(field)) => String::from_utf8(field.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Field".to_string())),
        };

        let value = match args.next() {
            Some(value) => value,
            _ => return Err(CommandError::InvalidArguments("Invalid Value".to_string())),
        };

        Ok(Self { key, field, value })
    }
}

// *2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n
impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["hgetall"], 1)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        };

        Ok(Self { key })
    }
}

#[cfg(test)]
mod tests {
    use crate::RespDecoder;

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_hget_try_from() -> Result<()> {
        let mut buf = BytesMut::from("*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let hget: HGet = frame.try_into()?;
        assert_eq!(hget.key, "hello");
        assert_eq!(hget.field, "map");

        Ok(())
    }

    #[test]
    fn test_hset_try_from() -> Result<()> {
        let mut buf =
            BytesMut::from("*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let hset: HSet = frame.try_into()?;
        assert_eq!(hset.key, "map");
        assert_eq!(hset.field, "hello");
        assert_eq!(hset.value, RespFrame::BulkString(b"world".into()));

        Ok(())
    }

    #[test]
    fn test_hgetall_try_from() -> Result<()> {
        let mut buf = BytesMut::from("*2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let hgetall: HGetAll = frame.try_into()?;
        assert_eq!(hgetall.key, "map");

        Ok(())
    }

    #[test]
    fn test_hset_hget_hgetall_commands() -> Result<()> {
        let backend = crate::Backend::new();
        let cmd = HSet {
            key: "map".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = HSet {
            key: "map".to_string(),
            field: "hello1".to_string(),
            value: RespFrame::BulkString(b"world1".into()),
        };
        cmd.execute(&backend);

        let cmd = HGet {
            key: "map".to_string(),
            field: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString(b"world".into()));

        let cmd = HGetAll {
            key: "map".to_string(),
        };
        let result = cmd.execute(&backend);
        let mut expected = RespMap::new();
        expected.insert("hello".to_string(), RespFrame::BulkString(b"world".into()));
        expected.insert(
            "hello1".to_string(),
            RespFrame::BulkString(b"world1".into()),
        );
        assert_eq!(result, expected.into());
        Ok(())
    }
}
