use crate::{Backend, BulkString, RespArray, RespFrame, RespNull};

use super::{extract_args, validate_command, CommandError, CommandExecutor, RESP_OK};

//     - HGET key field
//         - ("*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n")
#[derive(Debug)]
pub struct HGet {
    key: String,
    field: String,
}

//     - HSET key field val
//         - ("*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
#[derive(Debug)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

//     - HGETALL key
//         - ("*2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n")
#[derive(Debug)]
pub struct HGetAll {
    key: String,
    sort: bool,
}

#[derive(Debug)]
pub struct HMGet {
    key: String,
    fields: Vec<String>,
}

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
                let mut data = Vec::with_capacity(value.len());

                for v in value.iter() {
                    data.push((v.key().to_owned(), v.value().clone()));
                }

                if self.sort {
                    data.sort_by(|a, b| a.0.cmp(&b.0));
                }

                let frames = data
                    .into_iter()
                    .flat_map(|(k, v)| vec![BulkString::new(k).into(), v])
                    .collect::<Vec<RespFrame>>();

                RespArray::new(frames).into()
            }
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for HMGet {
    fn execute(&self, backend: &Backend) -> RespFrame {
        let mut data = Vec::with_capacity(self.fields.len());

        for field in self.fields.iter() {
            match backend.hget(&self.key, field) {
                Some(value) => data.push(value),
                None => data.push(RespFrame::Null(RespNull)),
            }
        }
        RespArray::new(data).into()
    }
}

// *3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n
impl TryFrom<RespArray> for HGet {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["hget"], 2)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        };

        let field = match args.next() {
            Some(RespFrame::BulkString(field)) => String::from_utf8(field.0)?,
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

        Ok(Self { key, sort: false })
    }
}

impl TryFrom<RespArray> for HMGet {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        let arr_len = arr.len();
        validate_command(&arr, &["hmget"], arr_len - 1)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        };

        let mut fields = Vec::with_capacity(arr_len - 1);
        loop {
            let arg = args.next();
            match arg {
                Some(RespFrame::BulkString(field)) => fields.push(String::from_utf8(field.0)?),
                None => break,
                _ => return Err(CommandError::InvalidArguments("Invalid Field".to_string())),
            }
        }

        Ok(Self { key, fields })
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
        assert_eq!(hget.key, "map");
        assert_eq!(hget.field, "hello");

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
            sort: true,
        };
        let result = cmd.execute(&backend);
        let expected = RespArray::new(vec![
            b"hello".into(),
            b"world".into(),
            b"hello1".into(),
            b"world1".into(),
        ]);
        assert_eq!(result, expected.into());
        Ok(())
    }

    #[test]
    fn test_hmget_try_from() -> Result<()> {
        let mut buf =
            BytesMut::from("*4\r\n$5\r\nhmget\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let hmget: HMGet = frame.try_into()?;
        assert_eq!(hmget.key, "map");
        assert_eq!(hmget.fields, vec!["hello", "world"]);

        Ok(())
    }

    #[test]
    fn test_hmget_command() -> Result<()> {
        let backend = crate::Backend::new();
        let cmd = HSet {
            key: "map".to_string(),
            field: "field".to_string(),
            value: RespFrame::BulkString(b"hello".into()),
        };
        cmd.execute(&backend);

        let cmd = HSet {
            key: "map".to_string(),
            field: "field2".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        cmd.execute(&backend);

        let cmd = HMGet {
            key: "map".to_string(),
            fields: vec![
                "field".to_string(),
                "field2".to_string(),
                "field3".to_string(),
            ],
        };

        let result = cmd.execute(&backend);
        let expected = RespArray::new(vec![
            RespFrame::BulkString(b"hello".into()),
            RespFrame::BulkString(b"world".into()),
            RespFrame::Null(RespNull),
        ]);

        assert_eq!(result, expected.into());

        Ok(())
    }
}
