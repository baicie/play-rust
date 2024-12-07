use crate::{
    connector::{DataBatch, Sink, Source, Transform},
    error::Result,
    metrics::Metrics,
};
use futures::StreamExt;
use std::mem;
use tokio::sync::broadcast;
use tracing::info;

pub struct JobConfig {
    pub batch_size: usize,
    pub channel_size: usize,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            batch_size: 10000,
            channel_size: 10,
        }
    }
}

pub struct SyncJob<T: Transform + 'static> {
    source: Box<dyn Source>,
    transforms: Vec<T>,
    sink: Box<dyn Sink>,
    config: JobConfig,
    metrics: Metrics,
}

impl<T: Transform + 'static> SyncJob<T> {
    pub fn new(source: Box<dyn Source>, transforms: Vec<T>, sink: Box<dyn Sink>) -> Self {
        Self {
            source,
            transforms,
            sink,
            config: JobConfig::default(),
            metrics: Metrics::new(),
        }
    }

    pub fn with_config(mut self, config: JobConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting sync job");
        self.metrics.start_job().await;

        // 初始化连接器
        self.source.init().await?;
        self.sink.init().await?;

        // 创建个写入通道
        let (tx, _rx) = broadcast::channel(self.config.channel_size);
        let sink_pool_size = 5; // 写入并发数

        // 创建多个 sink 实例
        let sink_pool = (0..sink_pool_size)
            .map(|_| {
                let mut sink = self.sink.clone_box();
                let metrics = self.metrics.clone();
                let mut rx = tx.subscribe();

                tokio::spawn(async move {
                    while let Ok(batch) = rx.recv().await {
                        sink.write_batch(batch).await?;
                        metrics.record_write_batch().await;
                    }
                    Ok::<_, crate::Error>(())
                })
            })
            .collect::<Vec<_>>();

        // 并行读取和转换
        let read_handle = {
            let mut source = mem::replace(&mut self.source, Box::new(EmptySource));
            let metrics = self.metrics.clone();
            let batch_size = self.config.batch_size;
            let mut transforms = self.transforms.clone();

            tokio::spawn(async move {
                while let Some(mut batch) = source.read_batch(batch_size).await? {
                    metrics.record_read_batch(&batch).await;

                    for transform in transforms.iter_mut() {
                        batch = transform.transform(batch).await?;
                        metrics.record_transform_batch(&batch).await;
                    }

                    if tx.send(batch).is_err() {
                        break;
                    }
                }
                Ok::<_, crate::Error>(())
            })
        };

        // 等待所有任务完成
        read_handle.await??;
        for handle in sink_pool {
            handle.await??;
        }

        // 提交并关闭
        self.sink.commit().await?;
        self.source.close().await?;
        self.sink.close().await?;

        info!("Sync job completed");
        self.metrics.end_job().await;
        self.metrics.print_summary().await;

        Ok(())
    }
}

// 添加一个空的 Source 实现
struct EmptySource;

#[async_trait::async_trait]
impl Source for EmptySource {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn read_batch(&mut self, _batch_size: usize) -> Result<Option<DataBatch>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn get_schema(&self) -> Result<String> {
        Ok("CREATE TABLE empty ()".to_string())
    }
}
