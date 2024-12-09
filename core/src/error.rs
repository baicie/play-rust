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

    #[error("Type conversion error: {0}")]
    Type(String),

    #[error("Other error: {0}")]
    Other(String),

    #[error("Channel error: {0}")]
    Channel(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Type(err.to_string())
    }
}
