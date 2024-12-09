use crate::config::MySQLSourceConfig;
use crate::type_converter::{MySQLTypeMapper, MySQLValueConverter};
use async_trait::async_trait;
use dbsync_core::connector::Context;
use dbsync_core::{
    connector::{ConnectorConfig, DataBatch, Record, ShardedSource, Source},
    error::{Error, Result},
    types::{DbsyncType, TypeConverter, TypeMapper},
};
use sqlx::{mysql::MySqlPool, Column, Row};
use std::any::Any;
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Clone)]
pub struct MySQLSource {
    config: MySQLSourceConfig,
    pool: Option<MySqlPool>,
    type_mapper: MySQLTypeMapper,
    value_converter: MySQLValueConverter,
}

impl MySQLSource {
    pub fn new(config: ConnectorConfig) -> Result<Self> {
        Ok(Self {
            config: MySQLSourceConfig::from_json(serde_json::Value::Object(
                serde_json::Map::from_iter(config.properties),
            ))?,
            pool: None,
            type_mapper: MySQLTypeMapper,
            value_converter: MySQLValueConverter,
        })
    }

    async fn get_column_types(
        &self,
        pool: &MySqlPool,
    ) -> Result<HashMap<String, (String, DbsyncType)>> {
        let query = r#"
            SELECT 
                COLUMN_NAME,
                CAST(DATA_TYPE AS CHAR) as COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        "#;

        let rows = sqlx::query(query)
            .bind(&self.config.table)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Read(e.to_string()))?;

        let mut column_types = HashMap::new();
        for row in rows {
            let column_name: String = row.get("COLUMN_NAME");
            let column_type: String = row.get("COLUMN_TYPE");
            let dbsync_type = self.type_mapper.to_dbsync_type(&column_type)?;
            column_types.insert(column_name, (column_type, dbsync_type));
        }

        Ok(column_types)
    }

    async fn get_create_table_sql(&self, pool: &MySqlPool) -> Result<String> {
        let query = "SHOW CREATE TABLE ".to_string() + &self.config.table;
        let row = sqlx::query(&query)
            .fetch_one(pool)
            .await
            .map_err(|e| Error::Read(e.to_string()))?;

        let create_table: String = row.get(1);
        Ok(create_table.replace(&self.config.table, "target_table"))
    }
}

#[async_trait]
impl Source for MySQLSource {
    async fn init(&mut self, ctx: &mut Context) -> Result<()> {
        let url = &self.config.url;
        info!("Connecting to MySQL source: {}", url);

        let pool = MySqlPool::connect(url)
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to MySQL source: {}", e)))?;

        // 获取表结构并放入 context
        let schema = self.get_create_table_sql(&pool).await?;
        ctx.set_schema(schema);

        info!("Successfully connected to MySQL source");
        self.pool = Some(pool);
        Ok(())
    }

    async fn read_batch(&mut self, batch_size: usize) -> Result<Option<DataBatch>> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        let column_types = self.get_column_types(pool).await?;
        let query = format!("SELECT * FROM {} LIMIT {}", self.config.table, batch_size);

        let rows = sqlx::query(&query)
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
                for col in row.columns() {
                    let name = col.name().to_string();
                    let (type_name, _) = column_types
                        .get(&name)
                        .ok_or_else(|| Error::Type(format!("Unknown column: {}", name)))?;

                    let raw_value = match type_name.as_str() {
                        "INT" | "BIGINT" => row
                            .try_get::<i64, _>(col.ordinal())
                            .map(|v| v.to_string())
                            .unwrap_or_else(|_| "NULL".to_string()),
                        "FLOAT" | "DOUBLE" | "DECIMAL" => row
                            .try_get::<f64, _>(col.ordinal())
                            .map(|v| v.to_string())
                            .unwrap_or_else(|_| "NULL".to_string()),
                        _ => row
                            .try_get::<Option<String>, _>(col.ordinal())
                            .unwrap()
                            .unwrap_or_else(|| "NULL".to_string()),
                    };

                    let dbsync_value = self
                        .value_converter
                        .to_dbsync_value(&raw_value, type_name)?;
                    fields.insert(name, serde_json::to_value(&dbsync_value)?);
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

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_sharded(&mut self) -> Option<&mut dyn ShardedSource> {
        Some(self)
    }
}

#[async_trait]
impl ShardedSource for MySQLSource {
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

        let column_types = self.get_column_types(pool).await?;
        let query = format!(
            "SELECT * FROM {} WHERE id >= {} AND id < {} LIMIT 1000",
            self.config.table, start_id, end_id
        );

        let rows = sqlx::query(&query)
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
                for col in row.columns() {
                    let name = col.name().to_string();
                    let (type_name, _) = column_types
                        .get(&name)
                        .ok_or_else(|| Error::Type(format!("Unknown column: {}", name)))?;

                    let raw_value = row
                        .try_get::<Option<String>, _>(col.ordinal())
                        .unwrap()
                        .unwrap_or_else(|| "NULL".to_string());

                    let dbsync_value = self
                        .value_converter
                        .to_dbsync_value(&raw_value, type_name)?;
                    fields.insert(name, serde_json::to_value(&dbsync_value)?);
                }
                println!("fields: {:?}", fields);
                Ok(Record { fields })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(DataBatch { records }))
    }

    async fn get_schema(&self) -> Result<String> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        // 获取表的创建语句
        let query = format!("SHOW CREATE TABLE {}", self.config.table);
        let row = sqlx::query(&query)
            .fetch_one(pool)
            .await
            .map_err(|e| Error::Read(e.to_string()))?;

        let create_table: String = row.get(1); // 第二列是 Create Table 语句

        // 替换表名为 target_table
        Ok(create_table.replace(&self.config.table, "target_table"))
    }
}
