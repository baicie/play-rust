use dbsync_core::connector::ConnectorConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub job_name: String,
    pub source: ConnectorConfig,
    pub sink: ConnectorConfig,
    #[serde(default)] // 使用默认值(空Vec)
    pub transforms: Vec<TransformConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransformConfig {
    pub transform_type: String,
    pub properties: serde_json::Value,
}

impl Config {
    pub fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }
}
