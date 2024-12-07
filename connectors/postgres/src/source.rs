use crate::config::PostgresSourceConfig;
use async_trait::async_trait;
use dbsync_core::{
    connector::{ConnectorConfig, DataBatch, Record, Source},
    error::{Error, Result},
};
use sqlx::{postgres::PgPool, Column, Row, TypeInfo};
use std::collections::HashMap;
use tracing::info;

#[derive(Clone)]
pub struct PostgresSource {
    config: PostgresSourceConfig,
    pool: Option<PgPool>,
    current_offset: i64,
}

impl PostgresSource {
    pub fn new(config: ConnectorConfig) -> Result<Self> {
        let config = PostgresSourceConfig::from_json(serde_json::Value::Object(
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
impl Source for PostgresSource {
    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }

    async fn init(&mut self) -> Result<()> {
        let url = &self.config.url;
        info!("Connecting to PostgreSQL database: {}", url);

        self.pool = Some(
            PgPool::connect(url)
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

        let query = format!("SELECT * FROM {} LIMIT $1 OFFSET $2", self.config.table);

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
                let columns = row.columns();
                for col in columns {
                    let name = col.name().to_string();
                    let value = match col.type_info().name() {
                        "INT4" | "INT8" => row
                            .try_get::<i64, _>(col.ordinal())
                            .map(|n| serde_json::Value::Number(n.into()))
                            .unwrap_or(serde_json::Value::Null),
                        "VARCHAR" | "TEXT" => row
                            .try_get::<String, _>(col.ordinal())
                            .map(serde_json::Value::String)
                            .unwrap_or(serde_json::Value::Null),
                        "FLOAT4" | "FLOAT8" => row
                            .try_get::<f64, _>(col.ordinal())
                            .map(|n| {
                                serde_json::Number::from_f64(n)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .unwrap_or(serde_json::Value::Null),
                        "BOOL" => row
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
            info!("Closing PostgreSQL connection");
            pool.close().await;
        }
        Ok(())
    }

    async fn get_schema(&self) -> Result<String> {
        let pool = PgPool::connect(&self.config.url)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let rows = sqlx::query(
            "SELECT table_definition FROM information_schema.tables WHERE table_name = $1",
        )
        .bind(&self.config.table)
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::Read(e.to_string()))?;

        if let Some(row) = rows.get(0) {
            let create_table: String = row.get("table_definition");
            Ok(create_table.replace(&self.config.table, "source_table"))
        } else {
            Err(Error::Read("Failed to get table schema".into()))
        }
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
            "SELECT * FROM {} WHERE id >= $1 AND id < $2 LIMIT $3",
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

        let records = rows
            .into_iter()
            .map(|row| {
                let mut fields = HashMap::new();
                let columns = row.columns();
                for col in columns {
                    let name = col.name().to_string();
                    let value = match col.type_info().name() {
                        "INT4" | "INT8" => row
                            .try_get::<i64, _>(col.ordinal())
                            .map(|n| serde_json::Value::Number(n.into()))
                            .unwrap_or(serde_json::Value::Null),
                        "VARCHAR" | "TEXT" => row
                            .try_get::<String, _>(col.ordinal())
                            .map(serde_json::Value::String)
                            .unwrap_or(serde_json::Value::Null),
                        "FLOAT4" | "FLOAT8" => row
                            .try_get::<f64, _>(col.ordinal())
                            .map(|n| {
                                serde_json::Number::from_f64(n)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .unwrap_or(serde_json::Value::Null),
                        "BOOL" => row
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

        Ok(Some(DataBatch { records }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;

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
    async fn test_source_init() {
        let config = ConnectorConfig {
            name: "test_source".to_string(),
            connector_type: "postgres".to_string(),
            properties: HashMap::from_iter(vec![
                (
                    "url".to_string(),
                    json!("postgres://postgres:password@localhost:5432/test"),
                ),
                ("table".to_string(), json!("test_table")),
                ("batch_size".to_string(), json!(100)),
            ]),
        };

        let mut source = PostgresSource::new(config).unwrap();
        assert!(source.init().await.is_ok());
    }
}
