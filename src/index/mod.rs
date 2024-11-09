mod btree;
mod hashmap;
use crate::{KeyDirEntry, Result};
use bytes::Bytes;

#[allow(dead_code)]
pub(crate) trait Indexer: Send + Sync {
    fn put(&self, key: Vec<u8>, entry: KeyDirEntry) -> Option<KeyDirEntry>;

    fn get(&self, key: Vec<u8>) -> Option<KeyDirEntry>;

    fn delete(&self, key: Vec<u8>) -> Option<KeyDirEntry>;

    fn list_keys(&self) -> Result<Vec<Bytes>>;
}
