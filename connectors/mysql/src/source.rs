use crate::config::MySQLSourceConfig;
use crate::type_converter::MySQLTypeConverter;
use async_trait::async_trait;
use dbsync_core::{
    connector::{ConnectorConfig, DataBatch, Record, Source},
    error::{Error, Result},
    types::TypeConverter,
};
use sqlx::{mysql::MySqlPool, Column, Row, TypeInfo};
use std::collections::HashMap;
use tracing::info;

#[derive(Clone)]
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

        let actual_batch_size = batch_size.min(1000);

        let offset = self.current_offset;
        let query = format!(
            "SELECT * FROM {} LIMIT {} OFFSET {}",
            self.config.table, actual_batch_size, offset
        );

        let rows = sqlx::query(&query)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Read(e.to_string()))?;

        if rows.is_empty() {
            return Ok(None);
        }

        self.current_offset += rows.len() as i64;

        let type_converter = MySQLTypeConverter;
        let records = rows
            .into_iter()
            .map(|row| {
                let mut fields = HashMap::new();
                let columns = row.columns();

                for col in columns {
                    let name = col.name().to_string();
                    let type_name = col.type_info().name();

                    // 获取原始字符串值
                    let raw_value = match type_name {
                        "INT" | "BIGINT" => {
                            row.try_get::<i64, _>(col.ordinal()).map(|v| v.to_string())
                        }
                        "VARCHAR" | "TEXT" => row.try_get::<String, _>(col.ordinal()),
                        "FLOAT" | "DOUBLE" => {
                            row.try_get::<f64, _>(col.ordinal()).map(|v| v.to_string())
                        }
                        "BOOLEAN" => row.try_get::<bool, _>(col.ordinal()).map(|v| v.to_string()),
                        _ => row.try_get::<String, _>(col.ordinal()),
                    }
                    .unwrap_or_else(|_| "NULL".to_string());

                    // 转换为统一的 JSON 值
                    let value = type_converter.convert_to_value(&raw_value, type_name)?;
                    fields.insert(name, value);
                }
                Ok(Record { fields })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(DataBatch { records }))
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            info!("Closing MySQL connection");
            pool.close().await;
        }
        Ok(())
    }

    async fn get_schema(&self) -> Result<String> {
        let pool = MySqlPool::connect(&self.config.url)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let rows = sqlx::query(&format!("SHOW CREATE TABLE `{}`", self.config.table))
            .fetch_all(&pool)
            .await
            .map_err(|e| Error::Read(e.to_string()))?;

        if let Some(row) = rows.get(0) {
            let create_table: String = row.get(1);
            Ok(create_table.replace(&self.config.table, "source_table"))
        } else {
            Err(Error::Read("Failed to get table schema".into()))
        }
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }

    async fn get_total_records(&self) -> Result<i64> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;
        let row = sqlx::query(&format!(
            "SELECT COUNT(*) as count FROM {}",
            self.config.table
        ))
        .fetch_one(pool)
        .await
        .map_err(|e| Error::Read(e.to_string()))?;
        Ok(row.get::<i64, _>("count"))
    }

    async fn get_id_range(&self) -> Result<(i64, i64)> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;
        let row = sqlx::query(&format!(
            "SELECT MIN(id) as min_id, MAX(id) as max_id FROM {}",
            self.config.table
        ))
        .fetch_one(pool)
        .await
        .map_err(|e| Error::Read(e.to_string()))?;
        Ok((row.get("min_id"), row.get("max_id")))
    }

    async fn read_batch_range(&mut self, start_id: i64, end_id: i64) -> Result<Option<DataBatch>> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        let query = format!(
            "SELECT * FROM {} WHERE id >= ? AND id < ? LIMIT ?",
            self.config.table
        );

        let rows = sqlx::query(&query)
            .bind(start_id)
            .bind(end_id)
            .bind(self.config.batch_size as i64)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Read(e.to_string()))?;

        if rows.is_empty() {
            return Ok(None);
        }

        let type_converter = MySQLTypeConverter;
        let records = rows
            .into_iter()
            .map(|row| {
                let mut fields = HashMap::new();
                let columns = row.columns();

                for col in columns {
                    let name = col.name().to_string();
                    let type_name = col.type_info().name();

                    // 获取原始字符串值
                    let raw_value = match type_name {
                        "INT" | "BIGINT" => {
                            row.try_get::<i64, _>(col.ordinal()).map(|v| v.to_string())
                        }
                        "VARCHAR" | "TEXT" => row.try_get::<String, _>(col.ordinal()),
                        "FLOAT" | "DOUBLE" => {
                            row.try_get::<f64, _>(col.ordinal()).map(|v| v.to_string())
                        }
                        "BOOLEAN" => row.try_get::<bool, _>(col.ordinal()).map(|v| v.to_string()),
                        _ => row.try_get::<String, _>(col.ordinal()),
                    }
                    .unwrap_or_else(|_| "NULL".to_string());

                    // 转换为统一的 JSON 值
                    let value = type_converter.convert_to_value(&raw_value, type_name)?;
                    fields.insert(name, value);
                }
                Ok(Record { fields })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(DataBatch { records }))
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
            connector_type: "mysql".to_string(),
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
            connector_type: "mysql".to_string(),
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
