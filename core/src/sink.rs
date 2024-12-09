#[async_trait]
impl Sink for MySQLSink {
    async fn init(&mut self, ctx: &Context) -> Result<()> {
        let url = &self.config.url;
        info!("Connecting to MySQL sink: {}", url);

        match MySqlPoolOptions::new()
            .max_connections(self.config.max_connections as u32)
            .connect(url)
            .await
        {
            Ok(pool) => {
                info!("Successfully connected to MySQL sink");
                self.pool = Some(pool);

                // 从上下文获取表结构
                if let Some(schema) = &ctx.schema {
                    self.create_table(schema, SaveMode::Overwrite).await?;
                }

                Ok(())
            }
            Err(e) => {
                let err = Error::Connection(format!("Failed to connect to MySQL sink: {}", e));
                error!("{}", err);
                Err(err)
            }
        }
    }
    // ... other methods ...
}
