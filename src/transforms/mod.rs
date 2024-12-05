pub trait Transformer {
    fn transform(&self, data: Vec<serde_json::Value>) -> Vec<serde_json::Value>;
}

pub struct IdentityTransformer;

impl Transformer for IdentityTransformer {
    fn transform(&self, data: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
        data // 不做任何转换
    }
}
