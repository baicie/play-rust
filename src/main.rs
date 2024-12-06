// src/main.rs
mod config;
mod plugin;
mod runtime;

use config::Config;
use dbsync_core::{connector::Record, error::Error, job::SyncJob};
use dbsync_mysql::{MySQLSink, MySQLSource};
use dbsync_postgres::{PostgresSink, PostgresSource};
use dbsync_transforms::get_transform_factory;
use plugin::PluginManager;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    info!("Starting dbsync");

    // 初始化插件管理器
    let mut plugin_manager = PluginManager::new();

    // 注册内置连接器
    plugin_manager.register_source("mysql", |config| Ok(Box::new(MySQLSource::new(config)?)));
    plugin_manager.register_source("postgres", |config| {
        Ok(Box::new(PostgresSource::new(config)?))
    });
    plugin_manager.register_sink("mysql", |config| Ok(Box::new(MySQLSink::new(config)?)));
    plugin_manager.register_sink("postgres", |config| {
        Ok(Box::new(PostgresSink::new(config)?))
    });

    // 注册转换器
    dbsync_transforms::register_transforms();

    // 加载配置
    let config = std::fs::read_to_string("config.json")?;
    let config: Config = serde_json::from_str(&config)?;

    // 通过插件系统创建连接器
    let source = plugin_manager.create_source(config.source)?;
    let sink = plugin_manager.create_sink(config.sink)?;

    // 创建转换器
    let transforms = config
        .transforms
        .into_iter()
        .map(|transform_config| {
            let factory =
                get_transform_factory(&transform_config.transform_type).ok_or_else(|| {
                    Error::Config(format!(
                        "Unknown transform type: {}",
                        transform_config.transform_type
                    ))
                })?;

            factory(Record::from(
                transform_config
                    .properties
                    .as_object()
                    .ok_or_else(|| {
                        Error::Config("Transform properties must be an object".to_string())
                    })?
                    .clone(),
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?;

    // 创建作业
    let mut job = SyncJob::new(source, transforms, sink);

    // 运行作业
    info!("Starting sync job");
    if let Err(e) = job.run().await {
        error!("Job failed: {}", e);
        return Err(e.into());
    }

    info!("Sync job completed successfully");
    Ok(())
}
