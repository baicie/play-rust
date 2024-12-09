use crate::error::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DbsyncType {
    Integer,  // INT, BIGINT 等
    Float,    // FLOAT, DOUBLE 等
    Decimal,  // DECIMAL, NUMERIC 等
    String,   // VARCHAR, TEXT 等
    DateTime, // TIMESTAMP, DATETIME 等
    Boolean,  // BOOLEAN, TINYINT(1) 等
    Binary,   // BLOB, BYTEA 等
    Null,     // NULL 值
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DbsyncValue {
    Null,
    Integer(i64),
    Float(f64),
    Decimal(String),
    String(String),
    DateTime(i64),
    Boolean(bool),
    Binary(Vec<u8>),
}

pub trait TypeMapper {
    fn to_dbsync_type(&self, source_type: &str) -> Result<DbsyncType>;
    fn to_target_type(&self, dbsync_type: &DbsyncType) -> Result<String>;
}

pub trait TypeConverter {
    fn to_dbsync_value(&self, value: &str, source_type: &str) -> Result<DbsyncValue>;
    fn from_dbsync_value(&self, value: &DbsyncValue, target_type: &str) -> Result<String>;
}
