use crate::{
    connector::{Context, ShardedSourceExt, Sink, Transform},
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
            batch_size: 1000,
            channel_size: 10,
        }
    }
}

pub struct SyncJob<T: Transform> {
    source: Box<dyn ShardedSourceExt>,
    transforms: Vec<T>,
    sink: Box<dyn Sink>,
    config: JobConfig,
    metrics: Metrics,
}

impl<T: Transform> SyncJob<T> {
    pub fn new(source: Box<dyn ShardedSourceExt>, transforms: Vec<T>, sink: Box<dyn Sink>) -> Self {
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

        // 创建上下文
        let mut ctx = Context::new();

        // 初始化 source 并获取表结构
        info!("Initializing source");
        self.source.init(&mut ctx).await?;
        let schema = self.source.get_schema().await?;
        ctx.set_schema(schema);

        // 初始化 sink (只执行一次建表)
        info!("Initializing sink");
        self.sink.init(&mut ctx).await?;

        // Get ranges first
        let sharded = self.source.as_sharded().unwrap();
        let (min_id, max_id) = sharded.get_id_range().await?;
        drop(sharded);

        // 读取所有数据
        let mut source = self.source.clone_box();
        let sharded = source.as_sharded().unwrap();
        let mut batches = Vec::new();
        let mut current_id = min_id;

        while current_id < max_id {
            if let Some(mut batch) = sharded.read_batch_range(current_id, max_id).await? {
                self.metrics.record_read_batch(&batch).await;

                for transform in self.transforms.iter_mut() {
                    batch = transform.transform(batch).await?;
                    self.metrics.record_transform_batch(&batch).await;
                }
                batches.push(batch);

                // 更新当前ID
                current_id += 1000; // 使用批次大小作为增量
            } else {
                break;
            }
        }

        // 读取完成后打印详细信息
        info!("Total batches: {}", batches.len());

        info!(
            "Total records read: {}",
            batches.iter().map(|b| b.records.len()).sum::<usize>()
        );

        // 并行写入数据
        let worker_count = 3;
        let mut handles = Vec::new();
        let batch_size = batches.len() / worker_count;

        for worker_id in 0..worker_count {
            let mut sink = self.sink.clone_box();
            let metrics = self.metrics.clone();
            let start = worker_id * batch_size;
            let end = if worker_id == worker_count - 1 {
                batches.len()
            } else {
                (worker_id + 1) * batch_size
            };
            let worker_batches = batches[start..end].to_vec();

            let handle = tokio::spawn(async move {
                for batch in worker_batches {
                    sink.write_batch(batch).await?;
                    metrics.record_write_batch().await;
                }
                Ok::<_, Error>(())
            });
            handles.push(handle);
        }

        // 等待所有写入任务完成
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
