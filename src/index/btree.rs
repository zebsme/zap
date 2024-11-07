use super::{Indexer, KeyDirEntry};
use crate::error::IndexError;
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::BTreeMap, ops::Deref, sync::Arc};

#[allow(dead_code)]
pub struct BTree(Arc<RwLock<BTreeMap<Vec<u8>, KeyDirEntry>>>);

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, entry: KeyDirEntry) -> Option<KeyDirEntry> {
        let mut write_guard = self.0.write();
        write_guard.insert(key, entry)
    }

    fn get(&self, key: Vec<u8>) -> Option<KeyDirEntry> {
        let read_guard = self.0.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> Option<KeyDirEntry> {
        let mut write_guard = self.write();
        write_guard.remove(&key)
    }

    fn list_keys(&self) -> Result<Vec<Bytes>, IndexError> {
        Ok(self
            .read()
            .iter()
            .map(|(k, _)| Bytes::copy_from_slice(&k))
            .collect::<Vec<Bytes>>())
    }
}

#[allow(dead_code)]
impl BTree {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(BTreeMap::new())))
    }
}

impl Deref for BTree {
    type Target = Arc<RwLock<BTreeMap<Vec<u8>, KeyDirEntry>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::*;
    // generate random file_id and size
    fn random_u32() -> u32 {
        let mut rng = rand::thread_rng();
        rng.gen()
    }
    // generate random offset
    fn random_u64() -> u64 {
        let mut rng = rand::thread_rng();
        rng.gen()
    }

    #[test]
    fn test_btree_put_new_entry() {
        let btree = BTree::new();

        let key = b"key".to_vec();
        let value = KeyDirEntry {
            file_id: random_u32(),
            offset: random_u64(),
            size: random_u32(),
        };

        let result = btree.put(key.clone(), value);
        assert!(result.is_none(), "Expected None, got {:?}", result);

        let retrieved = btree.get(key).unwrap();
        assert_eq!(retrieved.file_id, value.file_id);
        assert_eq!(retrieved.offset, value.offset);
        assert_eq!(retrieved.size, value.size);
    }

    #[test]
    fn test_btree_put_update_existing_entry() {
        let btree = BTree::new();

        let key = b"key".to_vec();

        let value1 = KeyDirEntry {
            file_id: random_u32(),
            offset: random_u64(),
            size: random_u32(),
        };

        let value2 = KeyDirEntry {
            file_id: random_u32(),
            offset: random_u64(),
            size: random_u32(),
        };

        btree.put(key.clone(), value1);
        let result = btree.put(key.clone(), value2);
        assert!(result.is_some(), "Expected Some, got None");

        let retrieved = result.unwrap();
        assert_eq!(retrieved.file_id, value1.file_id);
        assert_eq!(retrieved.offset, value1.offset);
        assert_eq!(retrieved.size, value1.size);
    }

    #[test]
    fn test_btree_get_existing_entry() {
        let btree = BTree::new();

        let key1 = b"apple".to_vec();
        let value1 = KeyDirEntry {
            file_id: random_u32(),
            offset: random_u64(),
            size: random_u32(),
        };
        let key2 = b"banana".to_vec();
        let value2 = KeyDirEntry {
            file_id: random_u32(),
            offset: random_u64(),
            size: random_u32(),
        };

        btree.put(key1.clone(), value1);
        btree.put(key2.clone(), value2);

        let retrieved1 = btree.get(key1).unwrap();
        assert_eq!(retrieved1.file_id, value1.file_id);
        assert_eq!(retrieved1.offset, value1.offset);
        assert_eq!(retrieved1.size, value1.size);

        let retrieved2 = btree.get(key2).unwrap();
        assert_eq!(retrieved2.file_id, value2.file_id);
        assert_eq!(retrieved2.offset, value2.offset);
        assert_eq!(retrieved2.size, value2.size);
    }

    #[test]
    fn test_btree_get_non_existing_entry() {
        let btree = BTree::new();

        let key = b"key".to_vec();

        let result = btree.get(key.clone());
        assert!(result.is_none(), "Expected None, got {:?}", result);
    }

    #[test]
    fn test_btree_delete_existing_entry() {
        let btree = BTree::new();

        let key1 = b"apple".to_vec();
        let value1 = KeyDirEntry {
            file_id: random_u32(),
            offset: random_u64(),
            size: random_u32(),
        };
        let key2 = b"banana".to_vec();
        let value2 = KeyDirEntry {
            file_id: random_u32(),
            offset: random_u64(),
            size: random_u32(),
        };

        btree.put(key1.clone(), value1);
        btree.put(key2.clone(), value2);

        let deleted1 = btree.delete(key1.clone());
        assert!(deleted1.is_some(), "Expected Some, got None");
        assert_eq!(deleted1.unwrap().file_id, value1.file_id);
        assert_eq!(deleted1.unwrap().offset, value1.offset);
        assert_eq!(deleted1.unwrap().size, value1.size);

        let deleted2 = btree.delete(key2.clone());
        assert!(deleted2.is_some(), "Expected Some, got None");
        assert_eq!(deleted2.unwrap().file_id, value2.file_id);
        assert_eq!(deleted2.unwrap().offset, value2.offset);
        assert_eq!(deleted2.unwrap().size, value2.size);
    }

    #[test]
    fn test_btree_delete_non_existing_entry() {
        let btree = BTree::new();

        let key = b"key".to_vec();

        let result = btree.delete(key.clone());
        assert!(result.is_none(), "Expected None, got {:?}", result);
    }
}
