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

mod decode;
mod encode;

use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use thiserror::Error;

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Deref, DerefMut},
};

#[enum_dispatch]
pub trait RespEncoder {
    fn encode(&self) -> Vec<u8>;
}

pub trait RespDecoder: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RespError {
    #[error("Frame is incomplete")]
    Incomplete,
    #[error("Invalid frame: {0}")]
    Invalid(String),
    #[error("Invalid frame length")]
    InvalidFrameLength,
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
}

#[enum_dispatch(RespEncoder)]
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
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
    Double(RespDouble),
    Map(RespMap),
    Set(RespSet),
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct SimpleString(pub(crate) String);
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct SimpleError(pub(crate) String);
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct BulkError(pub(crate) Vec<u8>);
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct BulkString(pub(crate) Vec<u8>);
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct NullBulkString;
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespArray(pub(crate) Vec<RespFrame>);
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespNullArray;
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespNull;
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespDouble(pub(crate) String);
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespMap(pub(crate) BTreeMap<String, RespFrame>);
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
pub struct RespSet(pub(crate) BTreeSet<RespFrame>);

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

impl Deref for RespDouble {
    type Target = String;

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
    type Target = BTreeSet<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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

impl RespDouble {
    pub fn new(s: f64) -> Self {
        let s = if s.abs() > 1e8 {
            format!("{:+e}", s)
        } else {
            format!("{:+}", s)
        };
        Self(s)
    }
}

impl Default for RespMap {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for RespSet {
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
    pub fn new() -> Self {
        Self(BTreeSet::new())
    }
}

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<&str> for RespFrame {
    fn from(s: &str) -> Self {
        SimpleString::from(s).into()
    }
}

impl From<&[u8]> for BulkString {
    fn from(s: &[u8]) -> Self {
        Self(s.to_vec())
    }
}

impl From<&[u8]> for RespFrame {
    fn from(s: &[u8]) -> Self {
        BulkString::from(s).into()
    }
}

impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(s: &[u8; N]) -> Self {
        Self(s.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for RespFrame {
    fn from(s: &[u8; N]) -> Self {
        BulkString::from(s).into()
    }
}

impl From<f64> for RespDouble {
    fn from(s: f64) -> Self {
        Self::new(s)
    }
}

impl From<f64> for RespFrame {
    fn from(s: f64) -> Self {
        s.into()
    }
}

impl AsRef<str> for SimpleString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
