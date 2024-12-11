mod batch;
pub mod db;
mod index;
mod io;
mod merge;
pub mod options;
mod result;
mod storage;
pub use self::{
    index::KeyDirEntry,
    options::Opts,
    result::{Error, Result},
    storage::State,
};
