// serialize/deserialize Frame
//     - simple string: "+OK\r\n"
//     - error: "-Error message\r\n"
//     - bulk error: "!<length>\r\n<error>\r\n"
//     - integer: ":[<+|->]<value>\r\n"
//     - bulk string: "$<length>\r\n<data>\r\n"
//     - null bulk string: "$-1\r\n"
//     - null array: "*-1\r\n"
//     - null: "_\r\n"
//     - boolean: "#<t|f>\r\n"
//     - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
//     - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
//         - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
//     - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
//     - set: "~<number-of-elements>\r\n<element-1>...<element-n>"

use anyhow::Result;
use bytes::BytesMut;

use crate::{
    BulkError, BulkString, NullBulkString, RespArray, RespDecoder, RespDouble, RespError,
    RespFrame, RespMap, RespNull, RespNullArray, RespSet, SimpleError, SimpleString,
};

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
            Some(b'$') => match NullBulkString::decode(buf) {
                Ok(frame) => Ok(RespFrame::NullBulkString(frame)),
                Err(RespError::Incomplete) => Err(RespError::Incomplete),
                _ => {
                    let frame = BulkString::decode(buf)?;
                    Ok(RespFrame::BulkString(frame))
                }
            },
            Some(b'_') => RespNull::decode(buf).map(RespFrame::Null),
            Some(b'#') => bool::decode(buf).map(RespFrame::Boolean),
            Some(b',') => RespDouble::decode(buf).map(RespFrame::Double),
            Some(b'*') => match RespNullArray::decode(buf).map(RespFrame::NullArray) {
                Ok(frame) => Ok(frame),
                Err(RespError::Incomplete) => Err(RespError::Incomplete),
                _ => {
                    let frame = RespArray::decode(buf)?;
                    Ok(RespFrame::Array(frame))
                }
            },
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
            b'$' => {
                if &buf[1..3] == b"-1" {
                    return NullBulkString::expect_length(buf);
                }
                BulkString::expect_length(buf)
            }
            b'*' => {
                if &buf[1..3] == b"-1" {
                    return RespNullArray::expect_length(buf);
                }
                RespArray::expect_length(buf)
            }
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

// - simple string: "+OK\r\n"
impl RespDecoder for SimpleString {
    const PREFIX: &'static str = "+";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let data = &buf[1..&buf.len() - 2];
        let frame = SimpleString::new(String::from_utf8_lossy(data).to_string());
        Ok(frame)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - error: "-Error message\r\n"
impl RespDecoder for SimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let data = &buf[1..&buf.len() - 2];
        let frame = SimpleError::new(String::from_utf8_lossy(data).to_string());
        Ok(frame)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - bulk error: "!<length>\r\n<error>\r\n"
impl RespDecoder for BulkError {
    const PREFIX: &'static str = "!";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let frame_len = Self::expect_length(buf)?;
        let (len_end, len) = parse_length(&buf[..frame_len])?;
        let data = &buf[len_end + 2..len_end + 2 + len];
        let frame = BulkError::new(data.to_vec());
        Ok(frame)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 2).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - integer: ":[<+|->]<value>\r\n"
impl RespDecoder for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let data = &buf[1..&buf.len() - 2];
        let frame = String::from_utf8_lossy(data)
            .parse::<i64>()
            .map_err(|_| RespError::Invalid(format!("Parse failed: {:?}", buf)))?;
        Ok(frame)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - bulk string: "$<length>\r\n<data>\r\n"
impl RespDecoder for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let frame_len = Self::expect_length(buf)?;
        let (len_end, len) = parse_length(&buf[..frame_len])?;
        let data = &buf[len_end + 2..len_end + 2 + len];
        let frame = BulkString::new(data.to_vec());
        Ok(frame)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 2).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - null bulk string: "$-1\r\n"
impl RespDecoder for NullBulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let data = &buf[1..&buf.len() - 2];
        if data != b"-1" {
            return Err(RespError::Invalid(String::from_utf8_lossy(buf).to_string()));
        }
        Ok(NullBulkString)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - null array: "*-1\r\n"
impl RespDecoder for RespNullArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let data = &buf[1..&buf.len() - 2];
        if data != b"-1" {
            return Err(RespError::Invalid(String::from_utf8_lossy(buf).to_string()));
        }
        Ok(Self)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - null: "_\r\n"
impl RespDecoder for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        if buf.len() != 3 {
            return Err(RespError::Invalid(String::from_utf8_lossy(buf).to_string()));
        }
        Ok(Self)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - boolean: "#<t|f>\r\n"
impl RespDecoder for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let data = &buf[1..&buf.len() - 2];
        let frame = match data {
            b"t" => true,
            b"f" => false,
            _ => return Err(RespError::Invalid(String::from_utf8_lossy(buf).to_string())),
        };
        Ok(frame)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
impl RespDecoder for RespDouble {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let data = &buf[1..&buf.len() - 2];
        let frame = String::from_utf8_lossy(data)
            .parse::<f64>()
            .map_err(|_| RespError::Invalid(format!("Parse failed: {:?}", buf)))?;
        Ok(RespDouble::new(frame))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        Ok(end + 2)
    }
}

// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
//         - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
impl RespDecoder for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;

        let nth_len = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        let nth = String::from_utf8_lossy(&buf[1..nth_len])
            .parse::<usize>()
            .map_err(|_| RespError::Invalid(String::from_utf8_lossy(buf).to_string()))?;
        let mut frames = Vec::with_capacity(nth_len);
        let mut start = nth_len + 2;
        for _ in 0..nth {
            let frame_len = RespFrame::expect_length(&buf[start..])?;
            let frame = RespFrame::decode(&mut BytesMut::from(&buf[start..start + frame_len]))?;
            frames.push(frame);
            start += frame_len;
        }
        Ok(RespArray::new(frames))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let nth_len = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        let nth = String::from_utf8_lossy(&buf[1..nth_len])
            .parse::<usize>()
            .map_err(|_| RespError::Invalid(String::from_utf8_lossy(buf).to_string()))?;

        let mut total = nth_len + 2;
        for _ in 0..nth {
            let frame_len = RespFrame::expect_length(&buf[total..])?;
            total += frame_len;
        }
        Ok(total)
    }
}

// - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
impl RespDecoder for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;

        let nth_len = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        let nth = String::from_utf8_lossy(&buf[1..nth_len])
            .parse::<usize>()
            .map_err(|_| RespError::Invalid(String::from_utf8_lossy(buf).to_string()))?;
        let mut map = Self::new();
        let mut start = nth_len + 2;
        for _ in 0..nth {
            let key_len = SimpleString::expect_length(&buf[start..])?;
            let key = SimpleString::decode(&mut BytesMut::from(&buf[start..start + key_len]))?;
            let value_len = RespFrame::expect_length(&buf[start + key_len..])?;
            let value = RespFrame::decode(&mut BytesMut::from(
                &buf[start + key_len..start + key_len + value_len],
            ))?;
            map.0.insert(key.0, value);
            start += key_len + value_len;
        }
        Ok(map)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let nth_len = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        let nth = String::from_utf8_lossy(&buf[1..nth_len])
            .parse::<usize>()
            .map_err(|_| RespError::Invalid(String::from_utf8_lossy(buf).to_string()))?;

        let mut total = nth_len + 2;
        for _ in 0..nth {
            let key_len = RespFrame::expect_length(&buf[total..])?;
            let value_len = RespFrame::expect_length(&buf[total + key_len..])?;
            total += key_len + value_len;
        }
        Ok(total)
    }
}

// - set: "~<number-of-elements>\r\n<element-1>...<element-n>"
impl RespDecoder for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        validate_frame_data(buf, Self::PREFIX)?;
        let nth_len = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        let nth = String::from_utf8_lossy(&buf[1..nth_len])
            .parse::<usize>()
            .map_err(|_| RespError::Invalid(String::from_utf8_lossy(buf).to_string()))?;
        let mut frames = RespSet::new();
        let mut start = nth_len + 2;
        for _ in 0..nth {
            let frame_len = RespFrame::expect_length(&buf[start..])?;
            let frame = RespFrame::decode(&mut BytesMut::from(&buf[start..start + frame_len]))?;
            frames.insert(frame);
            start += frame_len;
        }
        Ok(frames)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let nth_len = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
        let nth = String::from_utf8_lossy(&buf[1..nth_len])
            .parse::<usize>()
            .map_err(|_| RespError::Invalid(String::from_utf8_lossy(buf).to_string()))?;

        let mut total = nth_len + 2;
        for _ in 0..nth {
            let frame_len = RespFrame::expect_length(&buf[total..])?;
            total += frame_len;
        }
        Ok(total)
    }
}

fn validate_frame_data(buf: &mut BytesMut, prefix: &str) -> Result<(), RespError> {
    if buf.len() < 3 {
        return Err(RespError::Incomplete);
    }
    if &buf[..1] != prefix.as_bytes() {
        return Err(RespError::InvalidFrameType(format!(
            "Invalid frame: {:?}",
            buf
        )));
    }

    if (buf[buf.len() - 2], buf[buf.len() - 1]) != (b'\r', b'\n') {
        return Err(RespError::Incomplete);
    }

    Ok(())
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

fn parse_length(buf: &[u8]) -> Result<(usize, usize), RespError> {
    let len_end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
    let len = String::from_utf8_lossy(&buf[1..len_end])
        .parse::<usize>()
        .map_err(|_| RespError::InvalidFrameLength)?;

    if (len_end + 2 + len + 2) != buf.len() {
        return Err(RespError::InvalidFrameLength);
    }
    Ok((len_end, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("+OK\r\n");
        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("OK".to_string()));

        let mut buf = BytesMut::from("+OK\r");
        let frame = SimpleString::decode(&mut buf);
        assert_eq!(frame, Err(RespError::Incomplete));

        let mut buf = BytesMut::from("-OK\r\n");
        let frame = SimpleString::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::InvalidFrameType(
                "Invalid frame: b\"-OK\\r\\n\"".to_string()
            ))
        );
        Ok(())
    }

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::from("-Error message\r\n");
        let frame = SimpleError::decode(&mut buf)?;
        assert_eq!(frame, SimpleError::new("Error message".to_string()));
        Ok(())
    }

    #[test]
    fn test_bulk_error_decode() -> Result<()> {
        let mut buf = BytesMut::from("!13\r\nError message\r\n");
        let frame = BulkError::decode(&mut buf)?;
        assert_eq!(frame, BulkError::new(b"Error message".to_vec()));

        let mut buf = BytesMut::from("!12\r\nError message\r\n");
        let frame = BulkError::decode(&mut buf);
        assert_eq!(frame, Err(RespError::InvalidFrameLength));

        Ok(())
    }

    #[test]
    fn test_integer_decode() -> Result<()> {
        let mut buf = BytesMut::from(":123\r\n");
        let frame = i64::decode(&mut buf)?;
        assert_eq!(frame, 123);

        let mut buf = BytesMut::from(":xxx\r\n");
        let frame = i64::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid(
                "Parse failed: b\":xxx\\r\\n\"".to_string()
            ))
        );

        Ok(())
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$13\r\nHello, world!\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"Hello, world!".to_vec()));

        let mut buf = BytesMut::from("$13\r\nHello, world\r\n");
        let frame = BulkString::decode(&mut buf);
        assert_eq!(frame, Err(RespError::InvalidFrameLength));

        Ok(())
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$-1\r\n");
        let frame = NullBulkString::decode(&mut buf)?;
        assert_eq!(frame, NullBulkString);

        let mut buf = BytesMut::from("$-2\r\n");
        let frame = NullBulkString::decode(&mut buf);
        assert_eq!(frame, Err(RespError::Invalid("$-2\r\n".to_string())));
        Ok(())
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let frame = RespNullArray::decode(&mut buf)?;
        assert_eq!(frame, RespNullArray);

        let mut buf = BytesMut::from("*-2\r\n");
        let frame = RespNullArray::decode(&mut buf);
        assert_eq!(frame, Err(RespError::Invalid("*-2\r\n".to_string())));
        Ok(())
    }

    #[test]
    fn test_null_decode() -> Result<()> {
        let mut buf = BytesMut::from("_\r\n");
        let frame = RespNull::decode(&mut buf)?;
        assert_eq!(frame, RespNull);

        let mut buf = BytesMut::from("_x\r\n");
        let frame = RespNull::decode(&mut buf);
        assert_eq!(frame, Err(RespError::Invalid("_x\r\n".to_string())));
        Ok(())
    }

    #[test]
    fn test_bool_decode() -> Result<()> {
        let mut buf = BytesMut::from("#t\r\n");
        let frame = bool::decode(&mut buf)?;
        assert!(frame);

        let mut buf = BytesMut::from("#f\r\n");
        let frame = bool::decode(&mut buf)?;
        assert!(!frame);

        let mut buf = BytesMut::from("#x\r\n");
        let frame = bool::decode(&mut buf);
        assert_eq!(frame, Err(RespError::Invalid("#x\r\n".to_string())));
        Ok(())
    }

    #[test]
    fn test_f64_decode() -> Result<()> {
        let mut buf = BytesMut::from(",+123.45\r\n");
        let frame = RespDouble::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(123.45));

        let mut buf = BytesMut::from(",+1.23456e8\r\n");
        let frame = RespDouble::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(1.23456e8));

        let mut buf = BytesMut::from(",+123.45x\r\n");
        let frame = RespDouble::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid(
                "Parse failed: b\",+123.45x\\r\\n\"".to_string()
            ))
        );
        Ok(())
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespArray::new(vec![
                BulkString::new(b"get".to_vec()).into(),
                BulkString::new(b"hello".to_vec()).into()
            ])
        );

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
    fn test_map_decode() -> Result<()> {
        let mut buf = BytesMut::from("%2\r\n+get\r\n$5\r\nhello\r\n+set\r\n$5\r\nworld\r\n");
        let frame = RespMap::decode(&mut buf)?;
        let mut map = RespMap::new();
        map.0
            .insert("get".to_string(), BulkString::new(b"hello".to_vec()).into());
        map.0
            .insert("set".to_string(), BulkString::new(b"world".to_vec()).into());
        assert_eq!(frame, map);

        Ok(())
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
