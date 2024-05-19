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
use bytes::{Buf, BytesMut};

use crate::{
    BulkError, BulkString, NullBulkString, RespArray, RespDecoder, RespDouble, RespError,
    RespFrame, RespMap, RespNull, RespNullArray, RespSet, SimpleError, SimpleString,
};

use super::{find_crlf, CRLF_LEN};

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
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = SimpleString::new(data);
        Ok(frame)
    }
}

// - error: "-Error message\r\n"
impl RespDecoder for SimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = SimpleError::new(data);
        Ok(frame)
    }
}

// - bulk error: "!<length>\r\n<error>\r\n"
impl RespDecoder for BulkError {
    const PREFIX: &'static str = "!";
    const N_CRLF: usize = 2;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let len = extract_length(buf, Self::PREFIX)?;
        let data = extract_data(buf, "")?;
        if data.len() != len {
            return Err(RespError::InvalidFrameLength);
        }
        Ok(BulkError::new(data))
    }
}

// - integer: ":[<+|->]<value>\r\n"
impl RespDecoder for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = data
            .parse::<i64>()
            .map_err(|_| RespError::Invalid(format!("Parse failed: {:?}", data)))?;
        Ok(frame)
    }
}

// - bulk string: "$<length>\r\n<data>\r\n"
impl RespDecoder for BulkString {
    const PREFIX: &'static str = "$";
    const N_CRLF: usize = 2;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let len = extract_length(buf, Self::PREFIX)?;
        let data = extract_data(buf, "")?;
        if data.len() != len {
            return Err(RespError::InvalidFrameLength);
        }
        Ok(BulkString::new(data))
    }
}

// - null bulk string: "$-1\r\n"
impl RespDecoder for NullBulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, Self::PREFIX, "-1", "NullBulkString")?;
        Ok(Self)
    }
}

// - null array: "*-1\r\n"
impl RespDecoder for RespNullArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, Self::PREFIX, "-1", "NullArray")?;
        Ok(Self)
    }
}

// - null: "_\r\n"
impl RespDecoder for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, Self::PREFIX, "", "NullArray")?;
        Ok(Self)
    }
}

// - boolean: "#<t|f>\r\n"
impl RespDecoder for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = match data.as_str() {
            "t" => true,
            "f" => false,
            _ => return Err(RespError::Invalid(format!("Invalid bool: {:?}", data))),
        };
        Ok(frame)
    }
}

// - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
impl RespDecoder for RespDouble {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let data = extract_data(buf, Self::PREFIX)?;
        let frame = data
            .parse::<f64>()
            .map_err(|_| RespError::Invalid(format!("Parse failed: {:?}", data)))?;
        Ok(RespDouble::new(frame))
    }
}

// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
//         - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
impl RespDecoder for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let total = Self::expect_length(buf)?;
        if buf.len() < total {
            return Err(RespError::Incomplete);
        }
        let nth = extract_length(buf, Self::PREFIX)?;
        let mut frames = Vec::with_capacity(nth);
        for _ in 0..nth {
            let frame = RespFrame::decode(buf)?;
            frames.push(frame);
        }
        Ok(RespArray::new(frames))
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

// - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
impl RespDecoder for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let total = Self::expect_length(buf)?;
        if buf.len() < total {
            return Err(RespError::Incomplete);
        }

        let nth = extract_length(buf, Self::PREFIX)?;
        let mut map = Self::new();
        for _ in 0..nth {
            let key = SimpleString::decode(buf)?;
            let value = RespFrame::decode(buf)?;
            map.0.insert(key.0, value);
        }
        Ok(map)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (nth, position) = extract_nth_and_position(buf)?;
        let mut total = position + CRLF_LEN;
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
            Err(RespError::Invalid("Parse failed: \"xxx\"".to_string()))
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
        assert_eq!(
            frame,
            Err(RespError::Invalid(
                "NullBulkString expected: -1, got: -2".to_string()
            ))
        );
        Ok(())
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let frame = RespNullArray::decode(&mut buf)?;
        assert_eq!(frame, RespNullArray);

        let mut buf = BytesMut::from("*-2\r\n");
        let frame = RespNullArray::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid(
                "NullArray expected: -1, got: -2".to_string()
            ))
        );
        Ok(())
    }

    #[test]
    fn test_null_decode() -> Result<()> {
        let mut buf = BytesMut::from("_\r\n");
        let frame = RespNull::decode(&mut buf)?;
        assert_eq!(frame, RespNull);

        let mut buf = BytesMut::from("_x\r\n");
        let frame = RespNull::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::Invalid(
                "NullArray expected: , got: x".to_string()
            ))
        );
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
        assert_eq!(
            frame,
            Err(RespError::Invalid("Invalid bool: \"x\"".to_string()))
        );
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
            Err(RespError::Invalid("Parse failed: \"+123.45x\"".to_string()))
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
