use async_trait::async_trait;
use dbsync_core::connector::DataBatch;
use dbsync_core::{
    connector::{Record, Transform},
    error::{Error, Result},
};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;

lazy_static! {
    static ref TRANSFORM_FACTORIES: Mutex<HashMap<String, Box<dyn Fn(Record) -> Result<FieldRenameTransform> + Send + Sync>>> =
        Mutex::new(HashMap::new());
}

fn register_transform<F>(name: &str, factory: F)
where
    F: Fn(Record) -> Result<FieldRenameTransform> + Send + Sync + 'static,
{
    TRANSFORM_FACTORIES
        .lock()
        .unwrap()
        .insert(name.to_string(), Box::new(factory));
}

pub fn get_transform_factory(
    transform_type: &str,
) -> Option<Box<dyn Fn(Record) -> Result<FieldRenameTransform>>> {
    // TODO: 实现转换器工厂
    None
}

pub fn register_transforms() {
    register_transform("field_rename", create_field_rename_transform);
}

fn create_field_rename_transform(record: Record) -> Result<FieldRenameTransform> {
    let mappings = record
        .fields
        .get("mappings")
        .and_then(|v| v.as_object())
        .ok_or_else(|| Error::Config("field_rename transform requires mappings".to_string()))?
        .iter()
        .map(|(k, v)| {
            v.as_str()
                .map(|s| (k.clone(), s.to_string()))
                .ok_or_else(|| Error::Config("mapping values must be strings".to_string()))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    Ok(FieldRenameTransform { mappings })
}

#[derive(Clone)]
pub struct FieldRenameTransform {
    mappings: HashMap<String, String>,
}

#[async_trait]
impl Transform for FieldRenameTransform {
    async fn transform(&mut self, batch: DataBatch) -> Result<DataBatch> {
        let records = batch
            .records
            .into_iter()
            .map(|mut record| {
                for (old_name, new_name) in &self.mappings {
                    if let Some(value) = record.fields.remove(old_name) {
                        record.fields.insert(new_name.clone(), value);
                    }
                }
                record
            })
            .collect();

        Ok(DataBatch { records })
    }
}
