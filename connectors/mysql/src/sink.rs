use crate::{config::MySQLSinkConfig, type_converter::MySQLValueConverter};
use async_trait::async_trait;
use dbsync_core::{
    connector::{
        ConnectorConfig, Context, DataBatch, PoolStats, PooledSink, SaveMode, ShardedSink, Sink,
    },
    error::{Error, Result},
    types::TypeConverter,
};
use sqlx::{
    mysql::{MySqlPool, MySqlPoolOptions},
    Row,
};
use tracing::{error, info};

#[derive(Clone)]
pub struct MySQLSink {
    config: MySQLSinkConfig,
    pool: Option<MySqlPool>,
    value_converter: MySQLValueConverter,
}

impl MySQLSink {
    pub fn new(config: ConnectorConfig) -> Result<Self> {
        Ok(Self {
            config: MySQLSinkConfig::from_json(serde_json::Value::Object(
                serde_json::Map::from_iter(config.properties),
            ))?,
            pool: None,
            // type_mapper: MySQLTypeMapper,
            value_converter: MySQLValueConverter,
        })
    }
}

#[async_trait]
impl Sink for MySQLSink {
    async fn init(&mut self, ctx: &mut Context) -> Result<()> {
        let url = &self.config.url;
        info!("Connecting to MySQL sink: {}", url);

        match MySqlPoolOptions::new()
            .max_connections(self.config.max_connections as u32)
            .connect(url)
            .await
        {
            Ok(pool) => {
                info!("Successfully connected to MySQL sink");
                self.pool = Some(pool);

                // 从上下文获取表结构
                if let Some(schema) = &ctx.schema {
                    self.create_table(schema, SaveMode::Overwrite).await?;
                }

                Ok(())
            }
            Err(e) => {
                let err = Error::Connection(format!("Failed to connect to MySQL sink: {}", e));
                error!("{}", err);
                Err(err)
            }
        }
    }

    async fn write_batch(&mut self, batch: DataBatch) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| Error::Write(e.to_string()))?;

        for record in batch.records {
            let (columns, values): (Vec<_>, Vec<_>) = record.fields.into_iter().unzip();
            let placeholders = (0..values.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");

            let query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.config.table,
                columns.join(", "),
                placeholders
            );

            let mut query_builder = sqlx::query(&query);
            for (field_value, field_type) in values {
                query_builder = query_builder.bind(
                    self.value_converter
                        .from_dbsync_value(&field_value, &field_type)?,
                );
            }

            query_builder
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
            info!("Closing MySQL connection");
            pool.close().await;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl ShardedSink for MySQLSink {
    async fn get_worker_count(&self) -> Result<usize> {
        Ok(self.config.max_connections as usize / 2)
    }

    async fn get_shard_size(&self) -> Result<usize> {
        Ok(1000) // 每个分片1000条记录
    }

    async fn write_batch_sharded(
        &mut self,
        batches: Vec<DataBatch>,
        worker_id: usize,
    ) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        for batch in batches {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Write(format!("Worker {}: {}", worker_id, e)))?;

            for record in batch.records {
                let (columns, values): (Vec<_>, Vec<_>) = record.fields.into_iter().unzip();
                let placeholders = (0..values.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(", ");

                let query = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.config.table,
                    columns.join(", "),
                    placeholders
                );

                let mut query_builder = sqlx::query(&query);
                for (field_value, field_type) in values {
                    query_builder = query_builder.bind(
                        self.value_converter
                            .from_dbsync_value(&field_value, &field_type)?,
                    );
                }

                query_builder
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Write(format!("Worker {}: {}", worker_id, e)))?;
            }

            tx.commit()
                .await
                .map_err(|e| Error::Write(format!("Worker {}: {}", worker_id, e)))?;
        }

        Ok(())
    }

    async fn get_target_schema(&self) -> Result<String> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        let schema = sqlx::query(
            r#"
            SELECT 
                GROUP_CONCAT(
                    CONCAT(
                        COLUMN_NAME, ' ',
                        COLUMN_TYPE,
                        IF(IS_NULLABLE = 'NO', ' NOT NULL', ''),
                        IFNULL(CONCAT(' DEFAULT ', COLUMN_DEFAULT), '')
                    )
                    ORDER BY ORDINAL_POSITION
                    SEPARATOR ',\n  '
                ) as columns
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = ?
            "#,
        )
        .bind(&self.config.table)
        .fetch_one(pool)
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;

        let columns: String = schema.get("columns");
        Ok(format!("CREATE TABLE target_table (\n  {}\n)", columns))
    }
}

#[async_trait]
impl PooledSink for MySQLSink {
    async fn get_pool_stats(&self) -> Result<PoolStats> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        Ok(PoolStats {
            max_connections: self.config.max_connections as u32,
            active_connections: pool.size() as u32,
            idle_connections: 0,
            in_use_connections: 0,
        })
    }

    async fn set_pool_size(&mut self, size: u32) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }

        let pool = MySqlPoolOptions::new()
            .max_connections(size)
            .connect(&self.config.url)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        self.config.max_connections = size as usize;
        self.pool = Some(pool);
        Ok(())
    }

    async fn write_batch_pooled(&mut self, batch: DataBatch, conn_id: usize) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| Error::Write(format!("Connection {}: {}", conn_id, e)))?;

        for record in batch.records {
            let (columns, values): (Vec<_>, Vec<_>) = record.fields.into_iter().unzip();
            let placeholders = (0..values.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");

            let query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.config.table,
                columns.join(", "),
                placeholders
            );

            let mut query_builder = sqlx::query(&query);
            for (field_value, field_type) in values {
                query_builder = query_builder.bind(
                    self.value_converter
                        .from_dbsync_value(&field_value, &field_type)?,
                );
            }

            query_builder
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Write(format!("Connection {}: {}", conn_id, e)))?;
        }

        tx.commit()
            .await
            .map_err(|e| Error::Write(format!("Connection {}: {}", conn_id, e)))?;

        Ok(())
    }

    async fn get_available_connections(&self) -> Result<usize> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        Ok(pool.size() as usize) // 只返回总连接数
    }

    async fn create_table(&mut self, schema: &str, mode: SaveMode) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".into()))?;

        if mode == SaveMode::Overwrite {
            info!("Dropping existing table if exists: {}", self.config.table);
            sqlx::query(&format!("DROP TABLE IF EXISTS {}", self.config.table))
                .execute(pool)
                .await
                .map_err(|e| Error::Write(e.to_string()))?;
        }

        // 如果没有提供 schema，使用默认表结构
        let create_table = if schema.is_empty() {
            format!(
                "CREATE TABLE {} (id BIGINT PRIMARY KEY, name VARCHAR(255))",
                self.config.table
            )
        } else {
            schema.replace("target_table", &self.config.table)
        };

        info!("Creating table with SQL: {}", create_table);
        sqlx::query(&create_table)
            .execute(pool)
            .await
            .map_err(|e| Error::Write(e.to_string()))?;

        Ok(())
    }
}
