use dbsync_core::{connector::Record, Result};

pub fn get_transform_factory(
    _transform_type: &str,
) -> Option<Box<dyn Fn(Record) -> Result<Record>>> {
    // TODO: 实现转换器工厂
    None
}
