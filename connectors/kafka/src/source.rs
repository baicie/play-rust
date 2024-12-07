use async_trait::async_trait;
use dbsync_core::{
    connector::{ConnectorConfig, DataBatch, Record, Source},
    error::{Error, Result},
};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

pub struct KafkaSource {
    config: ConnectorConfig,
    consumer: Option<StreamConsumer>,
}

impl KafkaSource {
    pub fn new(config: ConnectorConfig) -> Self {
        Self {
            config,
            consumer: None,
        }
    }

    fn create_consumer_config(&self) -> Result<ClientConfig> {
        let mut config = ClientConfig::new();

        // 设置必要的配置
        config.set(
            "bootstrap.servers",
            self.config
                .properties
                .get("bootstrap.servers")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::Config("Kafka bootstrap.servers not provided".into()))?,
        );

        config.set(
            "group.id",
            self.config
                .properties
                .get("group.id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::Config("Kafka group.id not provided".into()))?,
        );

        // 设置其��可选配置
        if let Some(auto_offset_reset) = self.config.properties.get("auto.offset.reset") {
            config.set(
                "auto.offset.reset",
                auto_offset_reset.as_str().unwrap_or("earliest"),
            );
        }

        Ok(config)
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn init(&mut self) -> Result<()> {
        let config = self.create_consumer_config()?;

        let topic = self
            .config
            .properties
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Config("Kafka topic not provided".into()))?;

        info!("Connecting to Kafka topic: {}", topic);

        let consumer: StreamConsumer = config
            .create()
            .map_err(|e| Error::Connection(e.to_string()))?;

        consumer
            .subscribe(&[topic])
            .map_err(|e| Error::Connection(e.to_string()))?;

        self.consumer = Some(consumer);

        Ok(())
    }

    async fn read_batch(&mut self, batch_size: usize) -> Result<Option<DataBatch>> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        let mut records = Vec::with_capacity(batch_size);
        let timeout = Duration::from_secs(1);

        for _ in 0..batch_size {
            match consumer.recv().await {
                Ok(msg) => {
                    let payload = msg
                        .payload()
                        .ok_or_else(|| Error::Read("Empty message payload".into()))?;

                    let value: serde_json::Value = serde_json::from_slice(payload)
                        .map_err(|e| Error::Read(format!("Invalid JSON: {}", e)))?;

                    records.push(Record {
                        fields: HashMap::from_iter(
                            value
                                .as_object()
                                .ok_or_else(|| Error::Read("Message is not a JSON object".into()))?
                                .clone(),
                        ),
                    });
                }
                Err(e) => {
                    if records.is_empty() {
                        return Ok(None);
                    }
                    break;
                }
            }
        }

        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(DataBatch { records }))
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(consumer) = self.consumer.take() {
            info!("Closing Kafka consumer");
            consumer.unsubscribe();
        }
        Ok(())
    }
}
