use dbsync_core::{
    error::{Error, Result},
    types::TypeConverter,
};

pub struct MySQLTypeConverter;

impl TypeConverter for MySQLTypeConverter {
    fn convert_to_value(&self, raw_value: &str, source_type: &str) -> Result<serde_json::Value> {
        match source_type {
            "INT" | "BIGINT" => raw_value
                .parse::<i64>()
                .map(|n| serde_json::Value::Number(n.into()))
                .map_err(|e| {
                    Error::Type(format!(
                        "Failed to convert '{}' to integer: {}",
                        raw_value, e
                    ))
                }),
            "VARCHAR" | "TEXT" => Ok(serde_json::Value::String(raw_value.to_string())),
            "FLOAT" | "DOUBLE" => raw_value
                .parse::<f64>()
                .and_then(|n| Ok(serde_json::Number::from_f64(n).unwrap()))
                .map(serde_json::Value::Number)
                .map_err(|e| {
                    Error::Type(format!("Failed to convert '{}' to float: {}", raw_value, e))
                }),
            "BOOLEAN" | "TINYINT(1)" => match raw_value.to_lowercase().as_str() {
                "1" | "true" => Ok(serde_json::Value::Bool(true)),
                "0" | "false" => Ok(serde_json::Value::Bool(false)),
                _ => Err(Error::Type(format!("Invalid boolean value: {}", raw_value))),
            },
            _ => Ok(serde_json::Value::String(raw_value.to_string())),
        }
    }
}
