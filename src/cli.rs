use clap::Parser;
use dbsync_core::Config;
use std::{path::PathBuf, time::Instant};
use tracing::info;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.json")]
    config: PathBuf,

    /// 日志级别
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// 是否开启计时
    #[arg(short, long)]
    timing: bool,
}

pub fn parse_config() -> Result<(Config, bool), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt().with_env_filter("info").init();

    // 解析命令行参数
    let cli = Cli::parse();

    // 读取配置文件
    let config_content = std::fs::read_to_string(cli.config)?;
    let config: Config = serde_json::from_str(&config_content)?;

    Ok((config, cli.timing))
}

pub struct Timer {
    start: Instant,
    enabled: bool,
}

impl Timer {
    pub fn new(enabled: bool) -> Self {
        Self {
            start: Instant::now(),
            enabled,
        }
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }

    pub fn print_elapsed(&self, task: &str) {
        if self.enabled {
            info!("{} completed in {} ms", task, self.elapsed_ms());
        }
    }
}
