use crate::{
    connector::{DataBatch, Sink, Source, Transform},
    error::{Error, Result},
    metrics::Metrics,
};
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

        // 初始化并获取数据范���
        self.source.init().await?;
        self.sink.init().await?;

        // 假设获取到总记录数和ID范围
        let total_records = self.source.get_total_records().await?;
        let (min_id, max_id) = self.source.get_id_range().await?;

        let worker_count = 3;
        let mut handles = Vec::new();

        // 计算每个工作线程的ID范围
        let chunk_size = (max_id - min_id) / worker_count as i64;

        for worker_id in 0..worker_count {
            let mut source = self.source.clone_box();
            let mut sink = self.sink.clone_box();
            let mut transforms = self.transforms.clone();
            let metrics = self.metrics.clone();

            // 计算该工作线程的ID范围
            let start_id = min_id + (worker_id as i64 * chunk_size);
            let end_id = if worker_id == worker_count - 1 {
                max_id
            } else {
                min_id + ((worker_id + 1) as i64 * chunk_size)
            };

            let handle = tokio::spawn(async move {
                source.init().await?;

                // 使用ID范围读取数据
                while let Some(mut batch) = source.read_batch_range(start_id, end_id).await? {
                    metrics.record_read_batch(&batch).await;

                    for transform in transforms.iter_mut() {
                        batch = transform.transform(batch).await?;
                        metrics.record_transform_batch(&batch).await;
                    }

                    sink.write_batch(batch).await?;
                    metrics.record_write_batch().await;
                }

                source.close().await?;
                Ok::<_, Error>(())
            });
            handles.push(handle);
        }

        // 等待所有工作线程完成
        for handle in handles {
            handle.await??;
        }

        // 关闭连接器
        self.sink.commit().await?;
        self.sink.close().await?;
        self.source.close().await?;

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
    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(EmptySource)
    }

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

    async fn get_total_records(&self) -> Result<i64> {
        Ok(0)
    }

    async fn get_id_range(&self) -> Result<(i64, i64)> {
        Ok((0, 0))
    }

    async fn read_batch_range(
        &mut self,
        _start_id: i64,
        _end_id: i64,
    ) -> Result<Option<DataBatch>> {
        Ok(None)
    }
}
