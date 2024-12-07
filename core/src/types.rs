use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnifiedType {
    String,
    Integer,
    Float,
    Boolean,
    DateTime,
    Binary,
    Null,
}

pub trait TypeMapper {
    fn get_target_type(&self, source_type: &str) -> &str;
}

pub struct MySQLToPostgresMapper {
    type_mappings: HashMap<String, String>,
}

impl Default for MySQLToPostgresMapper {
    fn default() -> Self {
        let mut mappings = HashMap::new();
        // MySQL 到 PostgreSQL 的类型映射
        mappings.insert("TINYINT".to_string(), "SMALLINT".to_string());
        mappings.insert("SMALLINT".to_string(), "SMALLINT".to_string());
        mappings.insert("MEDIUMINT".to_string(), "INTEGER".to_string());
        mappings.insert("INT".to_string(), "INTEGER".to_string());
        mappings.insert("BIGINT".to_string(), "BIGINT".to_string());
        mappings.insert("FLOAT".to_string(), "REAL".to_string());
        mappings.insert("DOUBLE".to_string(), "DOUBLE PRECISION".to_string());
        mappings.insert("DECIMAL".to_string(), "NUMERIC".to_string());
        mappings.insert("CHAR".to_string(), "CHAR".to_string());
        mappings.insert("VARCHAR".to_string(), "VARCHAR".to_string());
        mappings.insert("TEXT".to_string(), "TEXT".to_string());
        mappings.insert("MEDIUMTEXT".to_string(), "TEXT".to_string());
        mappings.insert("LONGTEXT".to_string(), "TEXT".to_string());
        mappings.insert("DATE".to_string(), "DATE".to_string());
        mappings.insert("TIME".to_string(), "TIME".to_string());
        mappings.insert("DATETIME".to_string(), "TIMESTAMP".to_string());
        mappings.insert("TIMESTAMP".to_string(), "TIMESTAMP".to_string());
        mappings.insert("BOOLEAN".to_string(), "BOOLEAN".to_string());
        mappings.insert("TINYINT(1)".to_string(), "BOOLEAN".to_string());
        mappings.insert("BINARY".to_string(), "BYTEA".to_string());
        mappings.insert("VARBINARY".to_string(), "BYTEA".to_string());
        mappings.insert("BLOB".to_string(), "BYTEA".to_string());
        Self {
            type_mappings: mappings,
        }
    }
}

impl TypeMapper for MySQLToPostgresMapper {
    fn get_target_type(&self, source_type: &str) -> &str {
        self.type_mappings
            .get(source_type)
            .map(|s| s.as_str())
            .unwrap_or("TEXT") // 默认映射到 TEXT
    }
}

pub trait TypeConverter {
    fn convert_to_value(&self, raw_value: &str, source_type: &str) -> Result<serde_json::Value>;
}
