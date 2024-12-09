use crate::{
    connector::{ConnectorConfig, ShardedSourceExt, Sink},
    error::Result,
};
use std::collections::HashMap;

type SourceFactory =
    Box<dyn Fn(ConnectorConfig) -> Result<Box<dyn ShardedSourceExt>> + Send + Sync>;
type SinkFactory = Box<dyn Fn(ConnectorConfig) -> Result<Box<dyn Sink>> + Send + Sync>;

pub struct PluginRegistry {
    sources: HashMap<String, SourceFactory>,
    sinks: HashMap<String, SinkFactory>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
        }
    }

    pub fn register_source(&mut self, name: &str, factory: SourceFactory) {
        self.sources.insert(name.to_string(), factory);
    }

    pub fn register_sink(&mut self, name: &str, factory: SinkFactory) {
        self.sinks.insert(name.to_string(), factory);
    }

    pub fn get_source_factory(&self, name: &str) -> Option<&SourceFactory> {
        self.sources.get(name)
    }

    pub fn get_sink_factory(&self, name: &str) -> Option<&SinkFactory> {
        self.sinks.get(name)
    }
}
