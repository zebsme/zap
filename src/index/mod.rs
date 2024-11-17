mod btree;
mod hashmap;
mod keydir;
pub use btree::BTree;
use btree::BTreeIterator;
pub use hashmap::HashMap;
use hashmap::HashMapIterator;
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

    fn iter(&self) -> IndexIteratorMode;
}

#[enum_dispatch(IndexIteratorMode)]
pub trait IndexIterator: Sync + Send {
    fn rewind(&mut self);

    fn seek(&mut self, key: Vec<u8>);

    fn next(&mut self) -> Option<(&Vec<u8>, &KeyDirEntry)>;
}

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum IndexMode {
    HashMap(HashMap),
    BTree(BTree),
}

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum IndexIteratorMode {
    HashMap(HashMapIterator),
    BTree(BTreeIterator),
}
