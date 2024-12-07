use crate::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub fields: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataBatch {
    pub records: Vec<Record>,
}

#[async_trait]
pub trait Source: Send + Sync {
    fn clone_box(&self) -> Box<dyn Source>;
    async fn init(&mut self) -> Result<()>;
    async fn read_batch(&mut self, batch_size: usize) -> Result<Option<DataBatch>>;
    async fn close(&mut self) -> Result<()>;
    async fn get_schema(&self) -> Result<String>;
    async fn get_total_records(&self) -> Result<i64>;
    async fn get_id_range(&self) -> Result<(i64, i64)>;
    async fn read_batch_range(&mut self, start_id: i64, end_id: i64) -> Result<Option<DataBatch>>;
}

#[async_trait]
pub trait Sink: Send + Sync {
    fn clone_box(&self) -> Box<dyn Sink>;
    async fn init(&mut self) -> Result<()>;
    async fn write_batch(&mut self, batch: DataBatch) -> Result<()>;
    async fn commit(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

#[async_trait]
pub trait Transform: Send + Sync + Clone {
    async fn transform(&mut self, batch: DataBatch) -> Result<DataBatch>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    pub name: String,
    pub connector_type: String,
    pub properties: HashMap<String, serde_json::Value>,
}

impl From<serde_json::Map<String, serde_json::Value>> for Record {
    fn from(map: serde_json::Map<String, serde_json::Value>) -> Self {
        Record {
            fields: HashMap::from_iter(map),
        }
    }
}
