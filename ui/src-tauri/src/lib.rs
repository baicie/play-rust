// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

use dbsync_core::plugin::PluginManager;
use dbsync_core::{job::SyncJob, Config};
use serde::Serialize;
use sysinfo::System;

#[derive(Serialize)]
pub struct SystemMetrics {
    cpu_usage: f32,
    memory_usage: f32,
    disk_usage: f32,
    timestamp: i64,
}

#[tauri::command]
async fn get_system_metrics() -> Result<SystemMetrics, String> {
    let mut sys = System::new_all();
    sys.refresh_all();

    // CPU 使用率
    let cpu_usage =
        sys.cpus().iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;

    // 内存使用率
    let memory_usage = sys.used_memory() as f32 / sys.total_memory() as f32 * 100.0;

    // 磁盘使用率 - 使用可用的 API
    let disk_usage = 0.0; // 暂时不计算磁盘使用率

    Ok(SystemMetrics {
        cpu_usage,
        memory_usage,
        disk_usage,
        timestamp: chrono::Utc::now().timestamp_millis(),
    })
}

#[tauri::command]
async fn create_sync_job(config: Config) -> Result<(), String> {
    // 调用 dbsync 创建任务
    let mut plugin_manager = PluginManager::new();

    // 注册内置连接器
    plugin_manager.register_source("mysql", |config| {
        Ok(Box::new(dbsync_mysql::MySQLSource::new(config)?))
    });
    plugin_manager.register_sink("mysql", |config| {
        Ok(Box::new(dbsync_mysql::MySQLSink::new(config)?))
    });

    // 创建并运行任务
    let source = plugin_manager
        .create_source(config.source)
        .map_err(|e| e.to_string())?;
    let sink = plugin_manager
        .create_sink(config.sink)
        .map_err(|e| e.to_string())?;

    let mut job = SyncJob::new(source, vec![], sink);
    job.run().await.map_err(|e| e.to_string())?;

    Ok(())
}

#[tauri::command]
async fn get_available_connectors() -> Result<Vec<ConnectorInfo>, String> {
    Ok(vec![
        ConnectorInfo {
            name: "mysql".into(),
            connector_type: "source".into(),
            schema: include_str!("../../../connectors/mysql/schema/source.json").into(),
        },
        ConnectorInfo {
            name: "mysql".into(),
            connector_type: "sink".into(),
            schema: include_str!("../../../connectors/mysql/schema/sink.json").into(),
        },
        // ... 添加其他连接器
    ])
}

#[derive(Serialize)]
struct ConnectorInfo {
    name: String,
    connector_type: String,
    schema: String,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            greet,
            get_system_metrics,
            create_sync_job,
            get_available_connectors
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
