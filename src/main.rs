mod cli;

use dbsync_core::plugin::PluginManager;
use dbsync_core::{Config, SyncJob};
use dbsync_transforms::FieldRenameTransform;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析配置并获取计时标志
    let (config, timing) = cli::parse_config()?;
    let timer = cli::Timer::new(timing);

    info!("Starting dbsync...");

    // 读取配置文件
    let config_content = std::fs::read_to_string("config.json")?;
    let config: Config = serde_json::from_str(&config_content)?;
    info!("Loaded configuration: {:?}", config);

    // 创建插件管理器
    let mut plugin_manager = PluginManager::new();
    info!("Initializing plugins...");

    // 注册内置连接器
    plugin_manager.register_source("mysql", |config| {
        info!("Creating MySQL source");
        Ok(Box::new(dbsync_mysql::MySQLSource::new(config)?))
    });
    plugin_manager.register_sink("mysql", |config| {
        info!("Creating MySQL sink");
        Ok(Box::new(dbsync_mysql::MySQLSink::new(config)?))
    });

    // 创建并运行任务
    info!("Creating source connector...");
    let source = plugin_manager.create_source(config.source)?;

    info!("Creating sink connector...");
    let sink = plugin_manager.create_sink(config.sink)?;

    info!("Creating sync job...");
    let mut job = SyncJob::new(source, vec![], sink);

    info!("Starting sync job...");
    job.run().await?;

    // 打印耗时
    timer.print_elapsed("Sync job");
    Ok(())
}
