mod btree;
mod hashmap;
mod keydir;
pub use keydir::KeyDirEntry;

use crate::Result;
use btree::BTree;
use bytes::Bytes;
use enum_dispatch::enum_dispatch;
use hashmap::HashMap;

#[allow(dead_code)]
#[enum_dispatch(IndexMode)]
pub(crate) trait Indexer: Send + Sync {
    fn put(&self, key: Vec<u8>, entry: KeyDirEntry) -> Option<KeyDirEntry>;

    fn get(&self, key: &[u8]) -> Option<KeyDirEntry>;

    fn delete(&self, key: &[u8]) -> Option<KeyDirEntry>;

    fn list_keys(&self) -> Result<Vec<Bytes>>;
}

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum IndexMode {
    HashMap,
    BTree,
}
