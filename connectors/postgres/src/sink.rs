use crate::config::PostgresSinkConfig;
use async_trait::async_trait;
use dbsync_core::{
    connector::{ConnectorConfig, DataBatch, Sink},
    Error, Result,
};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tracing::info;

pub struct PostgresSink {
    config: PostgresSinkConfig,
    pool: Option<PgPool>,
}

impl PostgresSink {
    pub fn new(config: ConnectorConfig) -> Result<Self> {
        let config = PostgresSinkConfig::from_json(serde_json::Value::Object(
            serde_json::Map::from_iter(config.properties),
        ))?;
        Ok(Self { config, pool: None })
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn init(&mut self) -> Result<()> {
        let url = &self.config.url;
        info!("Connecting to PostgreSQL database: {}", url);

        let pool = PgPoolOptions::new()
            .max_connections(self.config.max_connections as u32)
            .connect(url)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        self.pool = Some(pool);
        Ok(())
    }

    async fn write_batch(&mut self, batch: DataBatch) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        let table = &self.config.table;

        // 开始事务
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| Error::Write(e.to_string()))?;

        for record in batch.records {
            let (columns, values): (Vec<_>, Vec<_>) = record.fields.into_iter().unzip();

            let placeholders = (0..values.len())
                .map(|i| format!("${}", i + 1))
                .collect::<Vec<_>>()
                .join(", ");

            let query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table,
                columns.join(", "),
                placeholders
            );

            let mut query = sqlx::query(&query);
            for value in values {
                query = query.bind(value.to_string());
            }

            query
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Write(e.to_string()))?;
        }

        tx.commit().await.map_err(|e| Error::Write(e.to_string()))?;

        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            info!("Closing PostgreSQL connection");
            pool.close().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsync_core::connector::Record;
    use serde_json::json;
    use std::collections::HashMap;

    async fn setup_test_db() -> PgPool {
        let url = std::env::var("TEST_POSTGRES_URL")
            .unwrap_or_else(|_| "postgres://postgres:password@localhost:5432/test".to_string());

        PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .expect("Failed to connect to PostgreSQL")
    }

    #[tokio::test]
    async fn test_write_batch() {
        let pool = setup_test_db().await;

        // 创建测试表
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS test_sink (
                id INT,
                name VARCHAR(255),
                age INT
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        // 创建并初始化 sink
        let config = ConnectorConfig {
            name: "test_sink".to_string(),
            properties: HashMap::from_iter(vec![
                (
                    "url".to_string(),
                    json!("postgres://postgres:password@localhost:5432/test"),
                ),
                ("table".to_string(), json!("test_sink")),
                ("max_connections".to_string(), json!(5)),
            ]),
        };

        let mut sink = PostgresSink::new(config).unwrap();
        sink.init().await.unwrap();

        // 准备测试数据
        let batch = DataBatch {
            records: vec![
                Record {
                    fields: HashMap::from_iter(vec![
                        ("id".to_string(), json!(1)),
                        ("name".to_string(), json!("Alice")),
                        ("age".to_string(), json!(20)),
                    ]),
                },
                Record {
                    fields: HashMap::from_iter(vec![
                        ("id".to_string(), json!(2)),
                        ("name".to_string(), json!("Bob")),
                        ("age".to_string(), json!(25)),
                    ]),
                },
            ],
        };

        // 写入数据
        sink.write_batch(batch).await.unwrap();

        // 验证写入的数据
        let rows: Vec<(i32, String, i32)> = sqlx::query_as("SELECT id, name, age FROM test_sink")
            .fetch_all(&pool)
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, "Alice".to_string(), 20));
        assert_eq!(rows[1], (2, "Bob".to_string(), 25));

        // 清理测试数据
        sqlx::query("DROP TABLE test_sink")
            .execute(&pool)
            .await
            .unwrap();
    }
}
