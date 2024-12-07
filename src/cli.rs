use clap::Parser;
use dbsync_core::Config;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.json")]
    config: PathBuf,

    /// 日志级别
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

pub fn parse_config() -> Result<Config, Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    // 解析命令行参数
    let cli = Cli::parse();

    // 读取配置文件
    let config_content = std::fs::read_to_string(cli.config)?;
    let config: Config = serde_json::from_str(&config_content)?;

    Ok(config)
}
