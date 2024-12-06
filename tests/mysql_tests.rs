mod common;

use dbsync_core::{connector::DataBatch, Result};
use dbsync_mysql::{MySQLSink, MySQLSource};
use sqlx::mysql::MySqlPoolOptions;
use std::time::Duration;

#[tokio::test]
async fn test_mysql_sync() -> Result<()> {
    let url = common::setup_mysql_env();
    let pool = MySqlPoolOptions::new()
        .connect_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .expect("Failed to connect to MySQL");

    // 创建测试表
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS source_table (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            age INT
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS target_table (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            age INT
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // 插入测试数据
    sqlx::query(
        "INSERT INTO source_table (id, name, age) VALUES
        (1, 'Alice', 20),
        (2, 'Bob', 25),
        (3, 'Charlie', 30)",
    )
    .execute(&pool)
    .await
    .unwrap();

    // 创建并初始化 source
    let source_config = common::create_mysql_source_config(&url, "source_table");
    let mut source = MySQLSource::new(source_config)?;
    source.init().await?;

    // 创建并初始化 sink
    let sink_config = common::create_mysql_sink_config(&url, "target_table");
    let mut sink = MySQLSink::new(sink_config)?;
    sink.init().await?;

    // 执行同步
    while let Some(batch) = source.read_batch(100).await? {
        sink.write_batch(batch).await?;
    }

    // 验证同步结果
    let rows: Vec<(i32, String, i32)> =
        sqlx::query_as("SELECT id, name, age FROM target_table ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "Alice".to_string(), 20));
    assert_eq!(rows[1], (2, "Bob".to_string(), 25));
    assert_eq!(rows[2], (3, "Charlie".to_string(), 30));

    // 清理测试数据
    sqlx::query("DROP TABLE source_table")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP TABLE target_table")
        .execute(&pool)
        .await
        .unwrap();

    Ok(())
}
