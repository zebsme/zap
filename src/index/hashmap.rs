use super::{Indexer, KeyDirEntry};
use crate::error::IndexError;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

pub struct HashMap(Arc<DashMap<Vec<u8>, KeyDirEntry>>);

impl Indexer for HashMap {
    fn put(&self, key: Vec<u8>, entry: KeyDirEntry) -> Option<KeyDirEntry> {
        self.0.insert(key, entry)
    }

    fn get(&self, key: Vec<u8>) -> Option<KeyDirEntry> {
        self.0.get(&key).map(|r| r.value().clone())
    }

    fn delete(&self, key: Vec<u8>) -> Option<KeyDirEntry> {
        self.0.remove(&key).map(|(_, v)| v)
    }

    fn list_keys(&self) -> Result<Vec<bytes::Bytes>, IndexError> {
        Ok(self
            .0
            .iter()
            .map(|r| Bytes::copy_from_slice(&r.key()))
            .collect::<Vec<Bytes>>())
    }
}

#[allow(dead_code)]
impl HashMap {
    fn new() -> Self {
        Self(Arc::new(DashMap::new()))
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
    fn test_hashmap_put_new_entry() {
        let map = HashMap::new();

        let key = b"key".to_vec();
        let value = KeyDirEntry {
            file_id: random_u32(),
            offset: random_u64(),
            size: random_u32(),
        };

        let result = map.put(key.clone(), value);
        assert!(result.is_none(), "Expected None, got {:?}", result);

        let retrieved = map.get(key).unwrap();
        assert_eq!(retrieved.file_id, value.file_id);
        assert_eq!(retrieved.offset, value.offset);
        assert_eq!(retrieved.size, value.size);
    }

    #[test]
    fn test_hashmap_put_update_existing_entry() {
        let map = HashMap::new();

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

        map.put(key.clone(), value1);
        let result = map.put(key.clone(), value2);
        assert!(result.is_some(), "Expected Some, got None");

        let retrieved = result.unwrap();
        assert_eq!(retrieved.file_id, value1.file_id);
        assert_eq!(retrieved.offset, value1.offset);
        assert_eq!(retrieved.size, value1.size);
    }

    #[test]
    fn test_hashmap_get_existing_entry() {
        let map = HashMap::new();

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

        map.put(key1.clone(), value1);
        map.put(key2.clone(), value2);

        let retrieved1 = map.get(key1).unwrap();
        assert_eq!(retrieved1.file_id, value1.file_id);
        assert_eq!(retrieved1.offset, value1.offset);
        assert_eq!(retrieved1.size, value1.size);

        let retrieved2 = map.get(key2).unwrap();
        assert_eq!(retrieved2.file_id, value2.file_id);
        assert_eq!(retrieved2.offset, value2.offset);
        assert_eq!(retrieved2.size, value2.size);
    }

    #[test]
    fn test_hashmap_get_non_existing_entry() {
        let map = HashMap::new();

        let key = b"key".to_vec();

        let result = map.get(key.clone());
        assert!(result.is_none(), "Expected None, got {:?}", result);
    }

    #[test]
    fn test_hashmap_delete_existing_entry() {
        let map = HashMap::new();

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

        map.put(key1.clone(), value1);
        map.put(key2.clone(), value2);

        let deleted1 = map.delete(key1.clone());
        assert!(deleted1.is_some(), "Expected Some, got None");
        assert_eq!(deleted1.unwrap().file_id, value1.file_id);
        assert_eq!(deleted1.unwrap().offset, value1.offset);
        assert_eq!(deleted1.unwrap().size, value1.size);

        let deleted2 = map.delete(key2.clone());
        assert!(deleted2.is_some(), "Expected Some, got None");
        assert_eq!(deleted2.unwrap().file_id, value2.file_id);
        assert_eq!(deleted2.unwrap().offset, value2.offset);
        assert_eq!(deleted2.unwrap().size, value2.size);
    }

    #[test]
    fn test_hashmap_delete_non_existing_entry() {
        let map = HashMap::new();

        let key = b"key".to_vec();

        let result = map.delete(key.clone());
        assert!(result.is_none(), "Expected None, got {:?}", result);
    }
}
