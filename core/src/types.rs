use crate::error::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DbsyncType {
    // 整数类型
    TinyInt,  // MySQL TINYINT
    SmallInt, // SMALLINT
    Int,      // INT
    BigInt,   // BIGINT

    // 浮点数类型
    Float,  // FLOAT
    Double, // DOUBLE

    // 精确数值类型
    Decimal(u8, u8), // DECIMAL(precision, scale)
    Numeric(u8, u8), // NUMERIC(precision, scale)

    // 字符串类型
    Char(u32),    // CHAR(n)
    VarChar(u32), // VARCHAR(n)
    Text,         // TEXT

    // 时间类型
    Date,      // DATE
    Time,      // TIME
    DateTime,  // DATETIME
    Timestamp, // TIMESTAMP

    // 其他类型
    Boolean,     // BOOLEAN/TINYINT(1)
    Binary(u32), // BINARY/VARBINARY(n)
    Blob,        // BLOB
    Json,        // JSON

    // 特殊类型
    Null, // NULL 值
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
    type Column;
    type Row;

    fn to_dbsync_value(
        &self,
        column: &Self::Column,
        row: &Self::Row,
        type_name: &DbsyncType,
    ) -> Result<DbsyncValue>;
    fn from_dbsync_value(&self, value: &DbsyncValue, target_type: &DbsyncType) -> Result<String>;
}
