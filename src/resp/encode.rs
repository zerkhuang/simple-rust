// serialize/deserialize Frame
//     - simple string: "+OK\r\n"
//     - error: "-Error message\r\n"
//     - bulk error: "!<length>\r\n<error>\r\n"
//     - integer: ":[<+|->]<value>\r\n"
//     - bulk string: "$<length>\r\n<data>\r\n"
//     - null bulk string: "$-1\r\n"
//     - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
//         - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
//     - null array: "*-1\r\n"
//     - null: "_\r\n"
//     - boolean: "#<t|f>\r\n"
//     - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
//     - big number: "([+|-]<number>\r\n" // TODO: implement
//     - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
//     - set: "~<number-of-elements>\r\n<element-1>...<element-n>"

use crate::{
    BulkError, BulkString, NullBulkString, RespArray, RespDouble, RespEncoder, RespMap, RespNull,
    RespNullArray, RespSet, SimpleError, SimpleString,
};

// - simple string: "+OK\r\n"
impl RespEncoder for SimpleString {
    fn encode(&self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

// - error: "-Error message\r\n"
impl RespEncoder for SimpleError {
    fn encode(&self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

// - bulk error: "!<length>\r\n<error>\r\n"
impl RespEncoder for BulkError {
    fn encode(&self) -> Vec<u8> {
        format!(
            "!{}\r\n{}\r\n",
            self.0.len(),
            String::from_utf8_lossy(&self.0)
        )
        .into_bytes()
    }
}

// - integer: ":[<+|->]<value>\r\n"
impl RespEncoder for i64 {
    fn encode(&self) -> Vec<u8> {
        let sign = if *self < 0 { "" } else { "+" };
        format!(":{}{}\r\n", sign, self).into_bytes()
    }
}

// - bulk string: "$<length>\r\n<data>\r\n"
impl RespEncoder for BulkString {
    fn encode(&self) -> Vec<u8> {
        format!("${}\r\n{}\r\n", self.len(), String::from_utf8_lossy(self)).into_bytes()
    }
}

// - null bulk string: "$-1\r\n"
impl RespEncoder for NullBulkString {
    fn encode(&self) -> Vec<u8> {
        "$-1\r\n".to_string().into_bytes()
    }
}

// - null: "_\r\n"
impl RespEncoder for RespNull {
    fn encode(&self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

// - boolean: "#<t|f>\r\n"
impl RespEncoder for bool {
    fn encode(&self) -> Vec<u8> {
        format!("#{}\r\n", if *self { "t" } else { "f" }).into_bytes()
    }
}

// - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
impl RespEncoder for RespDouble {
    fn encode(&self) -> Vec<u8> {
        format!(",{}\r\n", self.0).into_bytes()
    }
}

// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
//         - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
impl RespEncoder for RespArray {
    fn encode(&self) -> Vec<u8> {
        let mut encoded = format!("*{}\r\n", self.len()).into_bytes();
        for frame in &self.0 {
            encoded.extend_from_slice(&frame.encode());
        }
        encoded
    }
}

// - null array: "*-1\r\n"
impl RespEncoder for RespNullArray {
    fn encode(&self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

// - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
impl RespEncoder for RespMap {
    fn encode(&self) -> Vec<u8> {
        let mut encoded = format!("%{}\r\n", self.len()).into_bytes();
        for (key, value) in &self.0 {
            encoded.extend_from_slice(&SimpleString::new(key).encode());
            encoded.extend_from_slice(&value.encode());
        }
        encoded
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string_encode() {
        let frame = SimpleString::new("OK");
        assert_eq!(frame.encode(), b"+OK\r\n");
    }

    #[test]
    fn test_simple_error_encode() {
        let frame = SimpleError::new("Error message");
        assert_eq!(frame.encode(), b"-Error message\r\n");
    }

    #[test]
    fn test_bulk_error_encode() {
        let frame = BulkError::new(b"Error message");
        assert_eq!(frame.encode(), b"!13\r\nError message\r\n");
    }

    #[test]
    fn test_integer_encode() {
        let frame = 123;
        assert_eq!(frame.encode(), b":+123\r\n");

        let frame = -123;
        assert_eq!(frame.encode(), b":-123\r\n");
    }

    #[test]
    fn test_bulk_string_encode() {
        let frame = BulkString::new(b"Hello");
        assert_eq!(frame.encode(), b"$5\r\nHello\r\n");
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame = NullBulkString;
        assert_eq!(frame.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_null_encode() {
        let frame = RespNull;
        assert_eq!(frame.encode(), b"_\r\n");
    }

    #[test]
    fn test_boolean_encode() {
        let frame = true;
        assert_eq!(frame.encode(), b"#t\r\n");

        let frame = false;
        assert_eq!(frame.encode(), b"#f\r\n");
    }

    #[test]
    fn test_double_encode() {
        let frame = RespDouble::new(123.456);
        assert_eq!(frame.encode(), b",+123.456\r\n");

        let frame = RespDouble::new(-123.456);
        assert_eq!(frame.encode(), b",-123.456\r\n");

        let frame = RespDouble::new(1.23456e8);
        assert_eq!(frame.encode(), b",+1.23456e8\r\n");

        let frame = RespDouble::new(-1.23456e8);
        assert_eq!(frame.encode(), b",-1.23456e8\r\n");
    }

    #[test]
    fn test_array_encode() {
        let frame = RespArray::new(vec![
            BulkString::new(b"get").into(),
            SimpleString::new("hello").into(),
        ]);
        assert_eq!(frame.encode(), b"*2\r\n$3\r\nget\r\n+hello\r\n");
    }

    #[test]
    fn test_null_array_encode() {
        let frame = RespNullArray;
        assert_eq!(frame.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_map_encode() {
        let mut frame = RespMap::new();
        frame.insert(
            "hello".to_string(),
            BulkString::new("world".to_string()).into(),
        );
        frame.insert("foo".to_string(), RespDouble::new(-123456.789).into());
        assert_eq!(
            frame.encode(),
            b"%2\r\n+foo\r\n,-123456.789\r\n+hello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_set_encode() {
        let mut set = RespSet::new();
        set.insert(RespArray::new(vec![1234.into(), true.into()]).into());
        set.insert(BulkString::new("world".to_string()).into());
        set.insert(BulkString::new("world".to_string()).into());
        assert_eq!(
            &set.encode(),
            b"~2\r\n$5\r\nworld\r\n*2\r\n:+1234\r\n#t\r\n"
        );
    }
}
