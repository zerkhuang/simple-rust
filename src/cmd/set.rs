use crate::{Backend, RespArray, RespFrame};

use super::{extract_args, validate_command, CommandError, CommandExecutor, RESP_OK};

// sadd key member
// "*3\r\n$4\r\nsadd\r\n$5\r\nmyset\r\n$3\r\none\r\n"
#[derive(Debug)]
pub struct SAdd {
    key: String,
    members: Vec<RespFrame>,
}

impl CommandExecutor for SAdd {
    fn execute(&self, backend: &Backend) -> RespFrame {
        let set = backend.set.entry(self.key.clone()).or_default();
        for member in self.members.iter() {
            set.insert(member.clone());
        }
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for SAdd {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        let len = arr.len();
        validate_command(&arr, &["sadd"], len - 1)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        };

        let mut members = Vec::with_capacity(args.len());

        loop {
            match args.next() {
                Some(RespFrame::BulkString(member)) => {
                    members.push(member.into());
                }
                None => break,
                _ => return Err(CommandError::InvalidArguments("Invalid Member".to_string())),
            }
        }

        Ok(Self { key, members })
    }
}

// sismember key member
// "*3\r\n$9\r\nsismember\r\n$5\r\nmyset\r\n$3\r\none\r\n"
#[derive(Debug)]
pub struct SIsMember {
    key: String,
    member: RespFrame,
}

impl CommandExecutor for SIsMember {
    fn execute(&self, backend: &Backend) -> RespFrame {
        let set = backend.set.get(&self.key);
        match set {
            Some(set) => {
                if set.contains(&self.member) {
                    RespFrame::Integer(1)
                } else {
                    RespFrame::Integer(0)
                }
            }
            None => RespFrame::Integer(0),
        }
    }
}

impl TryFrom<RespArray> for SIsMember {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["sismember"], 2)?;

        let mut args = extract_args(arr, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArguments("Invalid Key".to_string())),
        };

        let member = match args.next() {
            Some(RespFrame::BulkString(member)) => RespFrame::BulkString(member),
            _ => return Err(CommandError::InvalidArguments("Invalid Member".to_string())),
        };

        Ok(Self { key, member })
    }
}

#[cfg(test)]
mod tests {
    use crate::RespDecoder;

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_sadd_try_from() -> Result<()> {
        let mut buf = BytesMut::from("*3\r\n$4\r\nsadd\r\n$5\r\nmyset\r\n$3\r\none\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let cmd = SAdd::try_from(frame)?;

        assert_eq!(cmd.key, "myset");
        assert_eq!(cmd.members.len(), 1);

        Ok(())
    }

    #[test]
    fn test_sismember_try_from() -> Result<()> {
        let mut buf = BytesMut::from("*3\r\n$9\r\nsismember\r\n$5\r\nmyset\r\n$3\r\none\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let cmd = SIsMember::try_from(frame)?;

        assert_eq!(cmd.key, "myset");

        Ok(())
    }

    #[test]
    fn test_sadd_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = SAdd {
            key: "myset".to_string(),
            members: vec![RespFrame::BulkString(b"one".into())],
        };

        let ret = cmd.execute(&backend);
        assert_eq!(ret, RESP_OK.clone());

        Ok(())
    }

    #[test]
    fn test_sismember_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = SAdd {
            key: "myset".to_string(),
            members: vec![RespFrame::BulkString(b"one".into())],
        };

        let ret = cmd.execute(&backend);
        assert_eq!(ret, RESP_OK.clone());

        let cmd = SIsMember {
            key: "myset".to_string(),
            member: RespFrame::BulkString(b"one".into()),
        };

        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let cmd = SIsMember {
            key: "myset".to_string(),
            member: RespFrame::BulkString(b"two".into()),
        };

        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(0));

        Ok(())
    }
}
