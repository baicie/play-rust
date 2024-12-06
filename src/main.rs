// src/main.rs
mod config;
mod plugin;
mod runtime;

use config::Config;
use dbsync_core::{connector::ConnectorConfig, job::SyncJob};
use dbsync_transforms::get_transform_factory;
use plugin::PluginManager;
use runtime::context::RuntimeContext;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    info!("Starting dbsync");

    // 初始化插件管理器
    let plugin_manager = PluginManager::new();

    // 加载配置
    let config = std::fs::read_to_string("config.json")?;
    let config: Config = serde_json::from_str(&config)?;

    // 创建连接器
    let source = dbsync_mysql::MySQLSource::new(config.source);
    let sink = dbsync_postgres::PostgresSink::new(config.sink);

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

            factory(transform_config.properties)
        })
        .collect::<Result<Vec<_>>>()?;

    // 创建作业
    let job = SyncJob::new(Box::new(source), transforms, Box::new(sink));

    // 运行作业
    info!("Starting sync job");
    if let Err(e) = job.run().await {
        error!("Job failed: {}", e);
        return Err(e.into());
    }

    info!("Sync job completed successfully");
    Ok(())
}
