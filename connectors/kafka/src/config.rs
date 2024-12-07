use dbsync_core::error::{Error, Result};
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
pub struct KafkaSourceConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    pub brokers: String,
    pub topic: String,
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
}

fn default_batch_size() -> usize {
    1000
}
fn default_buffer_size() -> usize {
    10000
}

impl KafkaSourceConfig {
    pub fn from_json(value: Value) -> Result<Self> {
        if let Err(error) = SOURCE_SCHEMA.validate(&value) {
            return Err(Error::Config(format!(
                "Schema validation failed: {}",
                error
            )));
        }
        serde_json::from_value(value)
            .map_err(|e| Error::Config(format!("Invalid Kafka config: {}", e)))
    }
}

impl KafkaSinkConfig {
    pub fn from_json(value: Value) -> Result<Self> {
        if let Err(error) = SINK_SCHEMA.validate(&value) {
            return Err(Error::Config(format!(
                "Schema validation failed: {}",
                error
            )));
        }
        serde_json::from_value(value)
            .map_err(|e| Error::Config(format!("Invalid Kafka config: {}", e)))
    }
}
