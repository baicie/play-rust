use dbsync_core::{
    error::{Error, Result},
    types::{DbsyncType, DbsyncValue, TypeConverter, TypeMapper},
};
use hex;

#[derive(Clone)]
pub struct MySQLTypeMapper;

impl TypeMapper for MySQLTypeMapper {
    fn to_dbsync_type(&self, source_type: &str) -> Result<DbsyncType> {
        match source_type.to_uppercase().as_str() {
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => Ok(DbsyncType::Integer),
            "FLOAT" | "DOUBLE" => Ok(DbsyncType::Float),
            "DECIMAL" | "NUMERIC" => Ok(DbsyncType::Decimal),
            "CHAR" | "VARCHAR" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => Ok(DbsyncType::String),
            "TIMESTAMP" | "DATETIME" => Ok(DbsyncType::DateTime),
            "BOOLEAN" | "TINYINT(1)" => Ok(DbsyncType::Boolean),
            "BINARY" | "VARBINARY" | "BLOB" => Ok(DbsyncType::Binary),
            _ => Err(Error::Type(format!("Unknown MySQL type: {}", source_type))),
        }
    }

    fn to_target_type(&self, dbsync_type: &DbsyncType) -> Result<String> {
        match dbsync_type {
            DbsyncType::Integer => Ok("INT".to_string()),
            DbsyncType::Float => Ok("DOUBLE".to_string()),
            DbsyncType::Decimal => Ok("DECIMAL(20,6)".to_string()),
            DbsyncType::String => Ok("VARCHAR(255)".to_string()),
            DbsyncType::DateTime => Ok("DATETIME".to_string()),
            DbsyncType::Boolean => Ok("TINYINT(1)".to_string()),
            DbsyncType::Binary => Ok("BLOB".to_string()),
            DbsyncType::Null => Ok("NULL".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct MySQLValueConverter;

impl TypeConverter for MySQLValueConverter {
    fn to_dbsync_value(&self, value: &str, type_name: &str) -> Result<DbsyncValue> {
        if value == "NULL" || value == "Null" {
            return Ok(DbsyncValue::Null);
        }

        match type_name {
            "INT" | "BIGINT" => value
                .parse()
                .map(DbsyncValue::Integer)
                .map_err(|e| Error::Type(e.to_string())),
            "VARCHAR" | "TEXT" | "CHAR" => Ok(DbsyncValue::String(value.to_string())),
            "FLOAT" | "DOUBLE" | "DECIMAL" => value
                .parse()
                .map(DbsyncValue::Float)
                .map_err(|e| Error::Type(e.to_string())),
            "DATETIME" | "TIMESTAMP" => Ok(DbsyncValue::String(value.to_string())),
            "BOOLEAN" => value
                .parse()
                .map(DbsyncValue::Boolean)
                .map_err(|e| Error::Type(e.to_string())),
            "BINARY" | "VARBINARY" | "BLOB" => hex::decode(value)
                .map(DbsyncValue::Binary)
                .map_err(|e| Error::Type(e.to_string())),
            _ => Ok(DbsyncValue::String(value.to_string())),
        }
    }

    fn from_dbsync_value(&self, value: &DbsyncValue, target_type: &str) -> Result<String> {
        match value {
            DbsyncValue::Null => Ok("NULL".to_string()),
            DbsyncValue::Integer(i) => match target_type {
                "INT" | "BIGINT" => Ok(i.to_string()),
                _ => Ok(format!("'{}'", i)),
            },
            DbsyncValue::Float(f) => Ok(f.to_string()),
            DbsyncValue::String(s) => match target_type {
                "TIMESTAMP" | "DATETIME" => {
                    if s == "Null" {
                        Ok("NULL".to_string())
                    } else {
                        Ok(format!("'{}'", s))
                    }
                }
                _ => Ok(format!("'{}'", s)),
            },
            DbsyncValue::Boolean(b) => Ok(if *b { "1" } else { "0" }.to_string()),
            DbsyncValue::Decimal(d) => Ok(d.clone()),
            DbsyncValue::DateTime(ts) => Ok(format!("'{}'", ts)),
            DbsyncValue::Binary(b) => Ok(format!("0x{}", hex::encode(b))),
        }
    }
}
