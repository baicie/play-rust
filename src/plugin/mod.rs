mod loader;
mod registry;

pub use loader::PluginLoader;
pub use registry::PluginRegistry;

use dbsync_core::{connector::ConnectorConfig, Error, Result, Sink, Source, Transform};
use std::collections::HashMap;

pub struct PluginManager {
    registry: PluginRegistry,
    loader: PluginLoader,
}

impl PluginManager {
    pub fn new() -> Self {
        Self {
            registry: PluginRegistry::new(),
            loader: PluginLoader::new(),
        }
    }

    pub fn register_source<F>(&mut self, name: &str, factory: F)
    where
        F: Fn(ConnectorConfig) -> Result<Box<dyn Source>> + Send + Sync + 'static,
    {
        self.registry.register_source(name, Box::new(factory));
    }

    pub fn register_sink<F>(&mut self, name: &str, factory: F)
    where
        F: Fn(ConnectorConfig) -> Result<Box<dyn Sink>> + Send + Sync + 'static,
    {
        self.registry.register_sink(name, Box::new(factory));
    }

    pub fn create_source(&self, config: ConnectorConfig) -> Result<Box<dyn Source>> {
        let factory = self
            .registry
            .get_source_factory(&config.connector_type)
            .ok_or_else(|| {
                Error::Config(format!("Unknown source type: {}", config.connector_type))
            })?;
        factory(config)
    }

    pub fn create_sink(&self, config: ConnectorConfig) -> Result<Box<dyn Sink>> {
        let factory = self
            .registry
            .get_sink_factory(&config.connector_type)
            .ok_or_else(|| {
                Error::Config(format!("Unknown sink type: {}", config.connector_type))
            })?;
        factory(config)
    }
}
