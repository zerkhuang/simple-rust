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

mod encode;

use enum_dispatch::enum_dispatch;

use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

#[enum_dispatch]
pub trait RespEncoder {
    fn encode(&self) -> Vec<u8>;
}

#[enum_dispatch(RespEncoder)]
pub enum RespFrame {
    SimpleString(SimpleString),
    Error(SimpleError),
    BulkError(BulkError),
    Integer(i64),
    BulkString(BulkString),
    NullBulkString(NullBulkString),
    Array(RespArray),
    NullArray(RespNullArray),
    Null(RespNull),
    Boolean(bool),
    Double(f64),
    Map(RespMap),
    Set(RespSet),
}

pub struct SimpleString(String);
pub struct SimpleError(String);
pub struct BulkError(Vec<u8>);
pub struct BulkString(Vec<u8>);
pub struct NullBulkString;
pub struct RespArray(Vec<RespFrame>);
pub struct RespNullArray;
pub struct RespNull;
pub struct RespMap(BTreeMap<String, RespFrame>);
pub struct RespSet(Vec<RespFrame>);

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for SimpleError {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for BulkError {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespMap {
    type Target = BTreeMap<String, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for RespSet {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl BulkError {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        Self(s.into())
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        Self(s.into())
    }
}

impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        Self(s.into())
    }
}

impl Default for RespMap {
    fn default() -> Self {
        Self::new()
    }
}

impl RespMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }
}

impl RespSet {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        Self(s.into())
    }
}
