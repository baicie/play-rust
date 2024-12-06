use crate::config::MySQLSourceConfig;
use async_trait::async_trait;
use dbsync_core::{
    connector::{ConnectorConfig, DataBatch, Record, Source},
    Error, Result,
};
use sqlx::{mysql::MySqlPool, Column, Row, TypeInfo};
use std::collections::HashMap;
use tracing::info;

pub struct MySQLSource {
    config: MySQLSourceConfig,
    pool: Option<MySqlPool>,
    current_offset: i64,
}

impl MySQLSource {
    pub fn new(config: ConnectorConfig) -> Result<Self> {
        let config = MySQLSourceConfig::from_json(serde_json::Value::Object(
            serde_json::Map::from_iter(config.properties),
        ))?;
        Ok(Self {
            config,
            pool: None,
            current_offset: 0,
        })
    }
}

#[async_trait]
impl Source for MySQLSource {
    async fn init(&mut self) -> Result<()> {
        let url = &self.config.url;
        info!("Connecting to MySQL database: {}", url);

        self.pool = Some(
            MySqlPool::connect(url)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?,
        );

        Ok(())
    }

    async fn read_batch(&mut self, batch_size: usize) -> Result<Option<DataBatch>> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        let actual_batch_size = batch_size.min(self.config.batch_size);

        let query = format!("SELECT * FROM {} LIMIT ? OFFSET ?", self.config.table);

        let rows = sqlx::query(&query)
            .bind(actual_batch_size as i64)
            .bind(self.current_offset)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Read(e.to_string()))?;

        if rows.is_empty() {
            return Ok(None);
        }

        let len = rows.len();
        let records = rows
            .into_iter()
            .map(|row| {
                let mut fields = HashMap::new();
                // 获取列名
                let columns = row.columns();
                for col in columns {
                    let name = col.name().to_string();
                    let value = match col.type_info().name() {
                        "INT" | "BIGINT" => row
                            .try_get::<i64, _>(col.ordinal())
                            .map(|n| serde_json::Value::Number(n.into()))
                            .unwrap_or(serde_json::Value::Null),
                        "VARCHAR" | "TEXT" => row
                            .try_get::<String, _>(col.ordinal())
                            .map(serde_json::Value::String)
                            .unwrap_or(serde_json::Value::Null),
                        "FLOAT" | "DOUBLE" => row
                            .try_get::<f64, _>(col.ordinal())
                            .map(|n| {
                                serde_json::Number::from_f64(n)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .unwrap_or(serde_json::Value::Null),
                        "BOOLEAN" => row
                            .try_get::<bool, _>(col.ordinal())
                            .map(serde_json::Value::Bool)
                            .unwrap_or(serde_json::Value::Null),
                        _ => serde_json::Value::Null,
                    };
                    fields.insert(name, value);
                }
                Record { fields }
            })
            .collect();

        self.current_offset += len as i64;
        Ok(Some(DataBatch { records }))
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            info!("Closing MySQL connection");
            pool.close().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsync_core::connector::ConnectorConfig;
    use serde_json::json;
    use sqlx::mysql::MySqlPoolOptions;

    // 创建测试数据库连接
    async fn setup_test_db() -> MySqlPool {
        let url = std::env::var("TEST_MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost:3306/test".to_string());

        MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .expect("Failed to connect to test database")
    }

    #[tokio::test]
    async fn test_source_init() {
        let config = ConnectorConfig {
            name: "test_source".to_string(),
            properties: HashMap::from_iter(vec![
                (
                    "url".to_string(),
                    json!("mysql://root:password@localhost:3306/test"),
                ),
                ("table".to_string(), json!("test_table")),
                ("batch_size".to_string(), json!(100)),
            ]),
        };

        let mut source = MySQLSource::new(config).unwrap();
        assert!(source.init().await.is_ok());
    }

    #[tokio::test]
    async fn test_read_batch() {
        let pool = setup_test_db().await;

        // 创建测试表并插入数据
        sqlx::query(
            "CREATE TEMPORARY TABLE test_table (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                age INT
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO test_table (id, name, age) VALUES
            (1, 'Alice', 20),
            (2, 'Bob', 25),
            (3, 'Charlie', 30)",
        )
        .execute(&pool)
        .await
        .unwrap();

        // 创建并初始化 source
        let config = ConnectorConfig {
            name: "test_source".to_string(),
            properties: HashMap::from_iter(vec![
                (
                    "url".to_string(),
                    json!("mysql://root:password@localhost:3306/test"),
                ),
                ("table".to_string(), json!("test_table")),
                ("batch_size".to_string(), json!(2)),
            ]),
        };

        let mut source = MySQLSource::new(config).unwrap();
        source.init().await.unwrap();

        // 读取第一批数据
        let batch = source.read_batch(2).await.unwrap().unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(
            batch.records[0]
                .fields
                .get("name")
                .unwrap()
                .as_str()
                .unwrap(),
            "Alice"
        );

        // 读取第二批数据
        let batch = source.read_batch(2).await.unwrap().unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(
            batch.records[0]
                .fields
                .get("name")
                .unwrap()
                .as_str()
                .unwrap(),
            "Charlie"
        );

        // 确认没有更多数据
        assert!(source.read_batch(2).await.unwrap().is_none());
    }
}
