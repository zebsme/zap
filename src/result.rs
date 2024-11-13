use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    /// The system has been used in an unsupported way.
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
    /// An unexpected bug has happened. Please open an issue on github!
    #[error("Unexpected bug: {0}")]
    ReportableBug(String),
    /// A read or write error has happened when interacting with the file
    /// system.
    #[error("IO Error")]
    Io(#[from] io::Error),
}
