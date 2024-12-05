use sqlx::Error;

use crate::{sinks::DataSink, sources::DataSource, transforms::Transformer};

pub struct SyncJob<S, T, K> {
    source: S,
    transformer: T,
    sink: K,
}

impl<S, T, K> SyncJob<S, T, K>
where
    S: DataSource,
    T: Transformer,
    K: DataSink,
{
    pub fn new(source: S, transformer: T, sink: K) -> Self {
        SyncJob {
            source,
            transformer,
            sink,
        }
    }

    pub async fn run(&self) -> Result<(), Error> {
        let data = self.source.read_data().await?;
        let transformed_data = self.transformer.transform(data);
        self.sink.write_data(transformed_data).await?;
        Ok(())
    }
}
