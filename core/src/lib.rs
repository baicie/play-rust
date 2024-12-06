pub mod connector;
pub mod error;
pub mod job;
pub mod metrics;

pub use connector::{Sink, Source, Transform};
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
