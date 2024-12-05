use async_trait::async_trait;
use serde_json::{Map, Value};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    Column, Error, Row,
};

pub struct PostgresSource {
    url: String,
    table: String,
}

impl PostgresSource {
    pub fn new(url: String, table: String) -> Self {
        Self { url, table }
    }
}

#[async_trait]
pub trait DataSource {
    async fn read_data(&self) -> Result<Vec<Value>, Error>;
}

#[async_trait]
impl DataSource for PostgresSource {
    async fn read_data(&self) -> Result<Vec<Value>, Error> {
        // 创建数据库连接池
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.url)
            .await?;

        // 查询指定表的数据
        let rows = sqlx::query(&format!("SELECT * FROM {}", self.table))
            .fetch_all(&pool)
            .await?;

        let mut result = Vec::new();

        // 将每行数据转换为 JSON 值
        for row in rows {
            let json_value = row_to_json(&row)?;
            println!("{:?}", json_value);
            result.push(json_value);
        }

        Ok(result)
    }
}

fn row_to_json(row: &PgRow) -> Result<Value, Error> {
    let mut map = Map::new();
    for column in row.columns() {
        let key = column.name().to_string();
        let value: Value = match column.type_info().to_string().as_str() {
            "BOOL" => row.try_get::<bool, _>(&*key)?.into(),
            "INT4" => row.try_get::<i32, _>(&*key)?.into(),
            "TEXT" | "VARCHAR" => row.try_get::<String, _>(&*key)?.into(),
            // Add more types as needed
            _ => Value::Null,
        };
        map.insert(key, value);
    }
    Ok(Value::Object(map))
}
