use crate::error::Result;
use crate::{DbsyncType, DbsyncValue};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    pub name: String,
    pub connector_type: String,
    pub properties: HashMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct DataBatch {
    pub records: Vec<Record>,
}

#[derive(Clone, Debug)]
pub struct Record {
    pub fields: HashMap<String, (DbsyncValue, DbsyncType)>,
}

// 基础 Source trait
#[async_trait]
pub trait Source: Send + Sync + Any {
    async fn init(&mut self, ctx: &mut Context) -> Result<()>;
    async fn read_batch(&mut self, batch_size: usize) -> Result<Option<DataBatch>>;
    async fn close(&mut self) -> Result<()>;
    fn clone_box(&self) -> Box<dyn Source>;
    fn as_any(&self) -> &dyn Any;

    // 默认返回 None 表示不支持分片
    fn as_sharded(&mut self) -> Option<&mut dyn ShardedSource> {
        None
    }
}

// 分片功能 trait
#[async_trait]
pub trait ShardedSource: Send + Sync {
    async fn get_total_records(&self) -> Result<i64>;
    async fn get_id_range(&self) -> Result<(i64, i64)>;
    async fn read_batch_range(&mut self, start_id: i64, end_id: i64) -> Result<Option<DataBatch>>;
    async fn get_schema(&self) -> Result<String>;
}

// 基础 Sink trait
#[async_trait]
pub trait Sink: Send + Sync {
    async fn init(&mut self, ctx: &mut Context) -> Result<()>;
    async fn write_batch(&mut self, batch: DataBatch) -> Result<()>;
    async fn commit(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    fn clone_box(&self) -> Box<dyn Sink>;
}

// 支持分片的 Sink trait
#[async_trait]
pub trait ShardedSink: Sink {
    // 获取可用的写入线程数
    async fn get_worker_count(&self) -> Result<usize>;

    // 获取每个分片的大小建议
    async fn get_shard_size(&self) -> Result<usize>;

    // 并行写入批次
    async fn write_batch_sharded(
        &mut self,
        batches: Vec<DataBatch>,
        worker_id: usize,
    ) -> Result<()>;

    // 获取目标表结构
    async fn get_target_schema(&self) -> Result<String>;
}

#[async_trait]
pub trait Transform: Send + Sync + Clone + 'static {
    async fn transform(&mut self, batch: DataBatch) -> Result<DataBatch>;
}

// 支持连接池的 Sink trait
#[async_trait]
pub trait PooledSink: Sink {
    // 获取连接池状态
    async fn get_pool_stats(&self) -> Result<PoolStats>;

    // 调整连接池大小
    async fn set_pool_size(&mut self, size: u32) -> Result<()>;

    // 批量写入(带连接池)
    async fn write_batch_pooled(&mut self, batch: DataBatch, conn_id: usize) -> Result<()>;

    // 获取可用连接数
    async fn get_available_connections(&self) -> Result<usize>;

    // 创建目标表
    async fn create_table(&mut self, schema: &str, mode: SaveMode) -> Result<()>;
}

// 连接池统计信息
pub struct PoolStats {
    pub max_connections: u32,
    pub active_connections: u32,
    pub idle_connections: u32,
    pub in_use_connections: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SaveMode {
    Overwrite, // 删除重建
               // 后续以添加其他模式
               // Append,    // 追
               // ErrorIfExists, // 已存在则报错
               // Ignore,    // 已存在则跳过
}

#[async_trait]
pub trait ShardedSourceExt: Source + ShardedSource {}

// 自动为所有同时实现了 Source 和 ShardedSource 的类型实现 ShardedSourceExt
impl<T: Source + ShardedSource> ShardedSourceExt for T {}

pub struct Context {
    pub schema: Option<String>,
    pub properties: HashMap<String, Value>,
}

impl Context {
    pub fn new() -> Self {
        Self {
            schema: None,
            properties: HashMap::new(),
        }
    }

    pub fn set_schema(&mut self, schema: String) {
        self.schema = Some(schema);
    }
}
