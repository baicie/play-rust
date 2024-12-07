use async_trait::async_trait;
use dbsync_core::{
    connector::{ConnectorConfig, DataBatch, Sink},
    error::{Error, Result},
};
use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    ClientConfig,
};
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
pub struct KafkaSink {
    config: ConnectorConfig,
    producer: Option<FutureProducer>,
    topic: String,
}

impl KafkaSink {
    pub fn new(config: ConnectorConfig) -> Self {
        Self {
            config,
            producer: None,
            topic: String::new(),
        }
    }

    fn create_producer_config(&self) -> Result<ClientConfig> {
        let mut config = ClientConfig::new();

        config.set(
            "bootstrap.servers",
            self.config
                .properties
                .get("bootstrap.servers")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::Config("Kafka bootstrap.servers not provided".into()))?,
        );

        // 设置其他生产者配置
        if let Some(acks) = self.config.properties.get("acks") {
            config.set("acks", acks.as_str().unwrap_or("all"));
        }

        Ok(config)
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn init(&mut self) -> Result<()> {
        let config = self.create_producer_config()?;

        self.topic = self
            .config
            .properties
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Config("Kafka topic not provided".into()))?
            .to_string();

        info!("Connecting to Kafka topic: {}", self.topic);

        let producer: FutureProducer = config
            .create()
            .map_err(|e| Error::Connection(e.to_string()))?;

        self.producer = Some(producer);

        Ok(())
    }

    async fn write_batch(&mut self, batch: DataBatch) -> Result<()> {
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        for record in batch.records {
            let value = serde_json::to_vec(&record)
                .map_err(|e| Error::Write(format!("Failed to serialize record: {}", e)))?;
            let record = FutureRecord::<(), [u8]>::to(&self.topic).payload(&value);

            producer
                .send(record, Duration::from_secs(0))
                .await
                .map_err(|(e, _)| Error::Write(format!("Failed to send message: {}", e)))?;
        }

        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        // Kafka producer 自动提交
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(producer) = self.producer.take() {
            info!("Closing Kafka producer");
            producer
                .flush(Duration::from_secs(5))
                .map_err(|e| Error::Write(format!("Failed to flush producer: {}", e)))?;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }
}
