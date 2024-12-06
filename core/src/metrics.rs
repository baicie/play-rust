use crate::connector::DataBatch;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<Mutex<MetricsInner>>,
}

struct MetricsInner {
    start_time: Option<Instant>,
    end_time: Option<Instant>,
    records_read: u64,
    records_transformed: u64,
    batches_written: u64,
    errors: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsInner {
                start_time: None,
                end_time: None,
                records_read: 0,
                records_transformed: 0,
                batches_written: 0,
                errors: 0,
            })),
        }
    }

    pub async fn start_job(&self) {
        let mut inner = self.inner.lock().await;
        inner.start_time = Some(Instant::now());
        info!("Job started");
    }

    pub async fn end_job(&self) {
        let mut inner = self.inner.lock().await;
        inner.end_time = Some(Instant::now());
        info!("Job ended");
    }

    pub async fn record_read_batch(&self, batch: &DataBatch) {
        let mut inner = self.inner.lock().await;
        inner.records_read += batch.records.len() as u64;
        info!(records = batch.records.len(), "Batch read");
    }

    pub async fn record_transform_batch(&self, batch: &DataBatch) {
        let mut inner = self.inner.lock().await;
        inner.records_transformed += batch.records.len() as u64;
        info!(records = batch.records.len(), "Batch transformed");
    }

    pub async fn record_write_batch(&self) {
        let mut inner = self.inner.lock().await;
        inner.batches_written += 1;
        info!(batches = inner.batches_written, "Batch written");
    }

    pub async fn record_error(&self) {
        let mut inner = self.inner.lock().await;
        inner.errors += 1;
        warn!(errors = inner.errors, "Error occurred");
    }

    pub async fn print_summary(&self) {
        let inner = self.inner.lock().await;
        let duration = inner
            .end_time
            .unwrap()
            .duration_since(inner.start_time.unwrap());

        info!(
            duration = ?duration,
            records_read = inner.records_read,
            records_transformed = inner.records_transformed,
            batches_written = inner.batches_written,
            errors = inner.errors,
            throughput = (inner.records_read as f64 / duration.as_secs_f64()),
            "Job Summary"
        );
    }
}
