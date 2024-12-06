use dbsync_core::{connector::ConnectorConfig, Result};
use serde_json::json;
use std::collections::HashMap;
use std::env;

// 测试辅助函数
pub fn setup_mysql_env() -> String {
    env::var("TEST_MYSQL_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost:3306/test".to_string())
}

// 创建 MySQL source 配置
pub fn create_mysql_source_config(url: &str, table: &str) -> ConnectorConfig {
    ConnectorConfig {
        name: "test_mysql_source".to_string(),
        properties: HashMap::from_iter(vec![
            ("url".to_string(), json!(url)),
            ("table".to_string(), json!(table)),
            ("batch_size".to_string(), json!(100)),
        ]),
    }
}

// 创建 MySQL sink 配置
pub fn create_mysql_sink_config(url: &str, table: &str) -> ConnectorConfig {
    ConnectorConfig {
        name: "test_mysql_sink".to_string(),
        properties: HashMap::from_iter(vec![
            ("url".to_string(), json!(url)),
            ("table".to_string(), json!(table)),
            ("max_connections".to_string(), json!(5)),
        ]),
    }
}
