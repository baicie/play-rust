mod config;
mod sink;
mod source;

pub use config::{KafkaSinkConfig, KafkaSourceConfig};
pub use sink::KafkaSink;
pub use source::KafkaSource;

#[derive(Debug)]
pub(crate) struct KafkaCommon {
    // Kafka 共享功能
}
