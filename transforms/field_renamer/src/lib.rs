use async_trait::async_trait;
use dbsync_core::{
    connector::{DataBatch, Transform},
    Error, Result,
};
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

lazy_static::lazy_static! {
    static ref SCHEMA: JSONSchema = {
        let schema = include_str!("../schema.json");
        let schema = serde_json::from_str(schema).unwrap();
        JSONSchema::compile(&schema).unwrap()
    };
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    mappings: HashMap<String, String>,
}

pub struct FieldRenamer {
    config: Config,
}

// 工厂函数
pub fn create(config: serde_json::Value) -> Result<Box<dyn Transform>> {
    // 验证配置
    if let Err(errors) = SCHEMA.validate(&config) {
        let errors: Vec<_> = errors.collect();
        return Err(Error::Config(format!(
            "Invalid field rename config: {:?}",
            errors
        )));
    }

    // 解析配置
    let config: Config = serde_json::from_value(config)
        .map_err(|e| Error::Config(format!("Failed to parse field rename config: {}", e)))?;

    Ok(Box::new(FieldRenamer { config }))
}

#[async_trait]
impl Transform for FieldRenamer {
    async fn transform(&mut self, mut batch: DataBatch) -> Result<DataBatch> {
        for record in &mut batch.records {
            let mut new_fields = HashMap::new();

            for (old_name, value) in record.fields.drain() {
                let new_name = self
                    .config
                    .mappings
                    .get(&old_name)
                    .map(|s| s.to_string())
                    .unwrap_or(old_name);

                new_fields.insert(new_name, value);
            }

            record.fields = new_fields;
        }

        Ok(batch)
    }
}
