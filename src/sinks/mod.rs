use async_trait::async_trait;
use serde_json::Value;
use sqlx::{mysql::MySqlPoolOptions, Error};

pub struct MySQLSink {
    url: String,
    table: String,
}

impl MySQLSink {
    pub fn new(url: String, table: String) -> Self {
        Self { url, table }
    }
}

#[async_trait]
pub trait DataSink {
    async fn write_data(&self, data: Vec<Value>) -> Result<(), Error>;
}

#[async_trait]
impl DataSink for MySQLSink {
    async fn write_data(&self, data: Vec<Value>) -> Result<(), Error> {
        // 创建数据库连接池
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&self.url)
            .await?;

        // 检查表是否存在，如果不存在则创建表
        let create_table_query = format!(
            "CREATE TABLE IF NOT EXISTS {} (id INT AUTO_INCREMENT PRIMARY KEY)",
            self.table
        );
        sqlx::query(&create_table_query).execute(&pool).await?;

        // 遍历数据并插入到 MySQL 表中
        for value in data {
            if let Value::Object(map) = &value {
                // 动态添加缺失的列
                for (key, val) in map {
                    let column_exists_query = format!(
                        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}' AND column_name = '{}'",
                        self.table, key
                    );
                    let column_exists: (i64,) = sqlx::query_as(&column_exists_query)
                        .fetch_one(&pool)
                        .await?;

                    if column_exists.0 == 0 {
                        let alter_table_query = format!(
                            "ALTER TABLE {} ADD COLUMN {} {}",
                            self.table,
                            key,
                            match val {
                                Value::String(_) => "TEXT",
                                Value::Number(_) => "DOUBLE",
                                Value::Bool(_) => "BOOLEAN",
                                _ => "TEXT",
                            }
                        );
                        sqlx::query(&alter_table_query).execute(&pool).await?;
                    }
                }

                // 构建插入查询
                let columns: Vec<String> = map.keys().cloned().collect();
                let values: Vec<String> = map
                    .values()
                    .map(|v| match v {
                        Value::String(s) => format!("'{}'", s),
                        Value::Number(n) => format!("{}", n),
                        Value::Bool(b) => format!("{}", b),
                        _ => "NULL".to_string(),
                    })
                    .collect();
                let insert_query = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.table,
                    columns.join(", "),
                    values.join(", ")
                );
                sqlx::query(&insert_query).execute(&pool).await?;
            }
        }

        Ok(())
    }
}
