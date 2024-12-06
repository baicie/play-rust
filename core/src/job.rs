use crate::{
    connector::{DataBatch, Sink, Source, Transform},
    metrics::Metrics,
    Result,
};
use std::mem;
use tokio::sync::mpsc;
use tracing::info;

pub struct JobConfig {
    pub batch_size: usize,
    pub channel_size: usize,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            channel_size: 10,
        }
    }
}

pub struct SyncJob {
    source: Box<dyn Source>,
    transforms: Vec<Box<dyn Transform>>,
    sink: Box<dyn Sink>,
    config: JobConfig,
    metrics: Metrics,
}

impl SyncJob {
    pub fn new(
        source: Box<dyn Source>,
        transforms: Vec<Box<dyn Transform>>,
        sink: Box<dyn Sink>,
    ) -> Self {
        Self {
            source,
            transforms,
            sink,
            config: JobConfig::default(),
            metrics: Metrics::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting sync job");
        self.metrics.start_job().await;

        // 初始化连接器
        self.source.init().await?;
        self.sink.init().await?;

        // 创建通道
        let (tx, mut rx) = mpsc::channel(self.config.channel_size);

        // 启动读取任务
        let read_handle = {
            // 使用 replace 替换 source，放入一个空盒子
            let mut source = mem::replace(&mut self.source, Box::new(EmptySource));
            let metrics = self.metrics.clone();
            let batch_size = self.config.batch_size;

            tokio::spawn(async move {
                while let Some(batch) = source.read_batch(batch_size).await? {
                    metrics.record_read_batch(&batch).await;
                    if tx.send(batch).await.is_err() {
                        break;
                    }
                }
                Ok::<_, crate::Error>(())
            })
        };

        // 处理数据
        while let Some(mut batch) = rx.recv().await {
            // 应用转换
            for transform in &mut self.transforms {
                batch = transform.transform(batch).await?;
                self.metrics.record_transform_batch(&batch).await;
            }

            // 写入数据
            self.sink.write_batch(batch).await?;
            self.metrics.record_write_batch().await;
        }

        // 等待读取任务完成
        read_handle.await??;

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
}
