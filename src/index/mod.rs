mod btree;
mod hashmap;
mod keydir;
pub use btree::BTree;
pub use hashmap::HashMap;
pub use keydir::KeyDirEntry;

use crate::Result;
use bytes::Bytes;
use enum_dispatch::enum_dispatch;

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
    HashMap(HashMap),
    BTree(BTree),
}
