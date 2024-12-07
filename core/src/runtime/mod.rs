pub mod context;

use crate::error::Result;
use context::RuntimeContext;

pub struct Runtime {
    context: RuntimeContext,
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            context: RuntimeContext::new(),
        }
    }

    pub async fn run(&self) -> Result<()> {
        // TODO: 实现运行时管理
        Ok(())
    }
}
