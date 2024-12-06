use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Read error: {0}")]
    Read(String),

    #[error("Write error: {0}")]
    Write(String),

    #[error("Transform error: {0}")]
    Transform(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Task error: {0}")]
    Task(#[from] tokio::task::JoinError),
}
