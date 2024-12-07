use crate::metrics::Metrics;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct RuntimeContext {
    pub metrics: Metrics,
    pub shutdown: Arc<Mutex<bool>>,
}

impl RuntimeContext {
    pub fn new() -> Self {
        Self {
            metrics: Metrics::new(),
            shutdown: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn request_shutdown(&self) {
        let mut shutdown = self.shutdown.lock().await;
        *shutdown = true;
    }

    pub async fn should_shutdown(&self) -> bool {
        *self.shutdown.lock().await
    }
}
