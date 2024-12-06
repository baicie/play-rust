use dbsync_core::{Error, Result};
use jsonschema::Validator;
use serde::{Deserialize, Serialize};
use serde_json::Value;

lazy_static::lazy_static! {
    static ref SOURCE_SCHEMA: Validator = {
        let schema = include_str!("../schema/source.json");
        let schema = serde_json::from_str(schema).unwrap();
        Validator::new(&schema).unwrap()
    };
    static ref SINK_SCHEMA: Validator = {
        let schema = include_str!("../schema/sink.json");
        let schema = serde_json::from_str(schema).unwrap();
        Validator::new(&schema).unwrap()
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySQLSourceConfig {
    pub url: String,
    pub table: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySQLSinkConfig {
    pub url: String,
    pub table: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_batch_size() -> usize {
    1000
}
fn default_max_connections() -> usize {
    10
}

impl MySQLSourceConfig {
    pub fn from_json(value: Value) -> Result<Self> {
        // 首先验证 schema
        if let Err(errors) = SOURCE_SCHEMA.validate(&value) {
            return Err(Error::Config(format!(
                "Schema validation failed: {:?}",
                errors
            )));
        }

        // 然后解析配置
        serde_json::from_value(value)
            .map_err(|e| Error::Config(format!("Invalid MySQL config: {}", e)))
    }
}

impl MySQLSinkConfig {
    pub fn from_json(value: Value) -> Result<Self> {
        // 首先验证 schema
        if let Err(error) = SINK_SCHEMA.validate(&value) {
            return Err(Error::Config(format!(
                "Schema validation failed: {}",
                error
            )));
        }

        // 然后解析配置
        serde_json::from_value(value)
            .map_err(|e| Error::Config(format!("Invalid MySQL config: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_source_config_validation() {
        // 有效配置
        let valid_config = json!({
            "url": "mysql://user:pass@localhost:3306/db",
            "table": "users",
            "batch_size": 500
        });
        assert!(MySQLSourceConfig::from_json(valid_config).is_ok());

        // 缺少必需字段
        let invalid_config = json!({
            "url": "mysql://user:pass@localhost:3306/db"
        });
        assert!(MySQLSourceConfig::from_json(invalid_config).is_err());

        // 无效的 URL 格式
        let invalid_url = json!({
            "url": "postgresql://wrong",
            "table": "users"
        });
        assert!(MySQLSourceConfig::from_json(invalid_url).is_err());
    }

    #[test]
    fn test_sink_config_validation() {
        // 有效配置
        let valid_config = json!({
            "url": "mysql://user:pass@localhost:3306/db",
            "table": "users",
            "max_connections": 5
        });
        assert!(MySQLSinkConfig::from_json(valid_config).is_ok());

        // 无效的连接数
        let invalid_connections = json!({
            "url": "mysql://user:pass@localhost:3306/db",
            "table": "users",
            "max_connections": 0
        });
        assert!(MySQLSinkConfig::from_json(invalid_connections).is_err());
    }
}
