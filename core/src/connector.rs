use crate::Result;
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
    async fn init(&mut self) -> Result<()>;
    async fn read_batch(&mut self, batch_size: usize) -> Result<Option<DataBatch>>;
    async fn close(&mut self) -> Result<()>;
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn init(&mut self) -> Result<()>;
    async fn write_batch(&mut self, batch: DataBatch) -> Result<()>;
    async fn commit(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

#[async_trait]
pub trait Transform: Send + Sync {
    async fn transform(&mut self, batch: DataBatch) -> Result<DataBatch>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    pub name: String,
    pub properties: HashMap<String, serde_json::Value>,
}
