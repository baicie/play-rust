use dbsync_core::{
    error::{Error, Result},
    types::{DbsyncType, DbsyncValue, TypeConverter, TypeMapper},
};
use hex;
use sqlx::mysql::{MySqlColumn, MySqlRow};
use sqlx::Column;
use sqlx::Row;

#[derive(Clone)]
pub struct MySQLTypeMapper;

impl TypeMapper for MySQLTypeMapper {
    fn to_dbsync_type(&self, source_type: &str) -> Result<DbsyncType> {
        match source_type.to_uppercase().as_str() {
            "TINYINT" => Ok(DbsyncType::TinyInt),
            "SMALLINT" => Ok(DbsyncType::SmallInt),
            "MEDIUMINT" | "INT" => Ok(DbsyncType::Int),
            "BIGINT" => Ok(DbsyncType::BigInt),
            "FLOAT" => Ok(DbsyncType::Float),
            "DOUBLE" => Ok(DbsyncType::Double),
            "DECIMAL" | "NUMERIC" => Ok(DbsyncType::Decimal(20, 6)),
            "CHAR" => Ok(DbsyncType::Char(255)),
            "VARCHAR" => Ok(DbsyncType::VarChar(255)),
            "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => Ok(DbsyncType::Text),
            "DATE" => Ok(DbsyncType::Date),
            "TIME" => Ok(DbsyncType::Time),
            "DATETIME" => Ok(DbsyncType::DateTime),
            "TIMESTAMP" => Ok(DbsyncType::Timestamp),
            "BOOLEAN" | "TINYINT(1)" => Ok(DbsyncType::Boolean),
            "BINARY" | "VARBINARY" => Ok(DbsyncType::Binary(255)),
            "BLOB" => Ok(DbsyncType::Blob),
            "JSON" => Ok(DbsyncType::Json),
            _ => Err(Error::Type(format!("Unknown MySQL type: {}", source_type))),
        }
    }

    fn to_target_type(&self, dbsync_type: &DbsyncType) -> Result<String> {
        match dbsync_type {
            DbsyncType::TinyInt => Ok("TINYINT".to_string()),
            DbsyncType::SmallInt => Ok("SMALLINT".to_string()),
            DbsyncType::Int => Ok("INT".to_string()),
            DbsyncType::BigInt => Ok("BIGINT".to_string()),
            DbsyncType::Float => Ok("FLOAT".to_string()),
            DbsyncType::Double => Ok("DOUBLE".to_string()),
            DbsyncType::Decimal(p, s) => Ok(format!("DECIMAL({},{})", p, s)),
            DbsyncType::Numeric(p, s) => Ok(format!("NUMERIC({},{})", p, s)),
            DbsyncType::Char(n) => Ok(format!("CHAR({})", n)),
            DbsyncType::VarChar(n) => Ok(format!("VARCHAR({})", n)),
            DbsyncType::Text => Ok("TEXT".to_string()),
            DbsyncType::Date => Ok("DATE".to_string()),
            DbsyncType::Time => Ok("TIME".to_string()),
            DbsyncType::DateTime => Ok("DATETIME".to_string()),
            DbsyncType::Timestamp => Ok("TIMESTAMP".to_string()),
            DbsyncType::Boolean => Ok("TINYINT(1)".to_string()),
            DbsyncType::Binary(n) => Ok(format!("VARBINARY({})", n)),
            DbsyncType::Blob => Ok("BLOB".to_string()),
            DbsyncType::Json => Ok("JSON".to_string()),
            DbsyncType::Null => Ok("NULL".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct MySQLValueConverter;

impl TypeConverter for MySQLValueConverter {
    type Column = MySqlColumn;
    type Row = MySqlRow;

    fn to_dbsync_value(
        &self,
        column: &Self::Column,
        row: &Self::Row,
        dbsync_type: &DbsyncType,
    ) -> Result<DbsyncValue> {
        match dbsync_type {
            DbsyncType::TinyInt | DbsyncType::SmallInt | DbsyncType::Int | DbsyncType::BigInt => {
                row.try_get::<i64, _>(column.ordinal())
                    .map(DbsyncValue::Integer)
                    .map_err(|e| Error::Type(e.to_string()))
            }

            DbsyncType::Float | DbsyncType::Double => row
                .try_get::<f64, _>(column.ordinal())
                .map(DbsyncValue::Float)
                .map_err(|e| Error::Type(e.to_string())),

            DbsyncType::Decimal(_, _) | DbsyncType::Numeric(_, _) => row
                .try_get::<rust_decimal::Decimal, _>(column.ordinal())
                .map(|d| DbsyncValue::Decimal(d.to_string()))
                .map_err(|e| Error::Type(e.to_string())),

            DbsyncType::Char(_) | DbsyncType::VarChar(_) | DbsyncType::Text => row
                .try_get::<String, _>(column.ordinal())
                .map(DbsyncValue::String)
                .map_err(|e| Error::Type(e.to_string())),

            DbsyncType::Date | DbsyncType::Time | DbsyncType::DateTime | DbsyncType::Timestamp => {
                row.try_get::<chrono::NaiveDateTime, _>(column.ordinal())
                    .or_else(|_| {
                        row.try_get::<chrono::DateTime<chrono::Utc>, _>(column.ordinal())
                            .map(|dt| dt.naive_utc())
                    })
                    .map(|dt| DbsyncValue::DateTime(dt.timestamp()))
                    .map_err(|e| Error::Type(e.to_string()))
            }

            DbsyncType::Boolean => row
                .try_get::<bool, _>(column.ordinal())
                .map(DbsyncValue::Boolean)
                .map_err(|e| Error::Type(e.to_string())),

            DbsyncType::Binary(_) | DbsyncType::Blob => row
                .try_get::<Vec<u8>, _>(column.ordinal())
                .map(DbsyncValue::Binary)
                .map_err(|e| Error::Type(e.to_string())),

            DbsyncType::Json => row
                .try_get::<String, _>(column.ordinal())
                .map(DbsyncValue::String)
                .map_err(|e| Error::Type(e.to_string())),

            DbsyncType::Null => Ok(DbsyncValue::Null),
        }
    }

    fn from_dbsync_value(&self, value: &DbsyncValue, target_type: &DbsyncType) -> Result<String> {
        match (value, target_type) {
            (DbsyncValue::Null, _) => Ok("NULL".to_string()),

            // 整数类型
            (
                DbsyncValue::Integer(i),
                DbsyncType::TinyInt | DbsyncType::SmallInt | DbsyncType::Int | DbsyncType::BigInt,
            ) => Ok(i.to_string()),

            // 浮点类型
            (DbsyncValue::Float(f), DbsyncType::Float | DbsyncType::Double) => Ok(f.to_string()),

            // 精确数值类型
            (DbsyncValue::Decimal(d), DbsyncType::Decimal(_, _) | DbsyncType::Numeric(_, _)) => {
                Ok(d.to_string())
            }

            // 字符串类型
            (
                DbsyncValue::String(s),
                DbsyncType::Char(_) | DbsyncType::VarChar(_) | DbsyncType::Text,
            ) => Ok(format!("{}", s)),

            (DbsyncValue::DateTime(ts), DbsyncType::DateTime) => {
                // 将 Unix 时间戳转换为 MySQL 格式的日期时间字符串
                let dt = chrono::NaiveDateTime::from_timestamp_opt(*ts, 0)
                    .ok_or_else(|| Error::Type("Invalid timestamp".to_string()))?;
                Ok(format!("{}", dt.format("%Y-%m-%d %H:%M:%S")))
            }

            (
                DbsyncValue::DateTime(ts),
                DbsyncType::DateTime | DbsyncType::Timestamp | DbsyncType::Date | DbsyncType::Time,
            ) => {
                let dt = chrono::NaiveDateTime::from_timestamp_opt(*ts, 0)
                    .ok_or_else(|| Error::Type("Invalid timestamp".to_string()))?;
                Ok(format!("{}", dt.format("%Y-%m-%d %H:%M:%S")))
            }

            // 布尔类型
            (DbsyncValue::Boolean(b), DbsyncType::Boolean) => {
                Ok(if *b { "1" } else { "0" }.to_string())
            }

            // 二进制类型
            (DbsyncValue::Binary(b), DbsyncType::Binary(_) | DbsyncType::Blob) => {
                Ok(format!("0x{}", hex::encode(b)))
            }

            // JSON 类型
            (DbsyncValue::String(s), DbsyncType::Json) => Ok(format!("'{}'", s)),

            // 类型不匹配的情况
            (v, t) => Err(Error::Type(format!(
                "Type mismatch: value {:?} cannot be converted to target type {:?}",
                v, t
            ))),
        }
    }
}
