mod cli;

use dbsync_core::{Config, Runtime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析命令行参数和环境变量
    let config = cli::parse_config()?;

    // 初始化并运行
    let runtime = Runtime::new();
    runtime.run().await?;

    Ok(())
}
