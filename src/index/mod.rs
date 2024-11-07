mod btree;
mod hashmap;
use crate::error::IndexError;
use bytes::Bytes;

#[allow(dead_code)]
pub(crate) trait Indexer {
    fn put(&self, key: Vec<u8>, entry: KeyDirEntry) -> Option<KeyDirEntry>;

    fn get(&self, key: Vec<u8>) -> Option<KeyDirEntry>;

    fn delete(&self, key: Vec<u8>) -> Option<KeyDirEntry>;

    fn list_keys(&self) -> Result<Vec<Bytes>, IndexError>;
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct KeyDirEntry {
    file_id: u32,
    offset: u64,
    size: u32,
}
