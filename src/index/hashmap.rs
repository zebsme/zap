use super::Indexer;
use crate::{KeyDirEntry, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct HashMap(Arc<DashMap<Vec<u8>, KeyDirEntry>>);

impl Indexer for HashMap {
    fn put(&self, key: Vec<u8>, entry: KeyDirEntry) -> Option<KeyDirEntry> {
        self.0.insert(key, entry)
    }

    fn get(&self, key: &[u8]) -> Option<KeyDirEntry> {
        self.0.get(key).map(|r| *r.value())
    }

    fn delete(&self, key: &[u8]) -> Option<KeyDirEntry> {
        self.0.remove(key).map(|(_, v)| v)
    }

    fn list_keys(&self) -> Result<Vec<Bytes>> {
        Ok(self
            .0
            .iter()
            .map(|r| Bytes::copy_from_slice(r.key()))
            .collect::<Vec<Bytes>>())
    }
}

#[allow(dead_code)]
impl HashMap {
    pub fn new() -> Self {
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
        let value = KeyDirEntry::new(random_u32(), random_u64(), random_u32());

        let result = map.put(key.clone(), value);
        assert!(result.is_none(), "Expected None, got {:?}", result);

        let retrieved = map.get(&key).unwrap();
        assert_eq!(retrieved.get_file_id(), value.get_file_id());
        assert_eq!(retrieved.get_offset(), value.get_offset());
        assert_eq!(retrieved.get_size(), value.get_size());
    }

    #[test]
    fn test_hashmap_put_update_existing_entry() {
        let map = HashMap::new();

        let key = b"key".to_vec();

        let value1 = KeyDirEntry::new(random_u32(), random_u64(), random_u32());

        let value2 = KeyDirEntry::new(random_u32(), random_u64(), random_u32());

        map.put(key.clone(), value1);
        let result = map.put(key.clone(), value2);
        assert!(result.is_some(), "Expected Some, got None");

        let retrieved = result.unwrap();
        assert_eq!(retrieved.get_file_id(), value1.get_file_id());
        assert_eq!(retrieved.get_offset(), value1.get_offset());
        assert_eq!(retrieved.get_size(), value1.get_size());
    }

    #[test]
    fn test_hashmap_get_existing_entry() {
        let map = HashMap::new();

        let apple = b"apple".to_vec();
        let apple_entry = KeyDirEntry::new(random_u32(), random_u64(), random_u32());

        let banana = b"banana".to_vec();
        let banana_entry = KeyDirEntry::new(random_u32(), random_u64(), random_u32());

        map.put(apple.clone(), apple_entry);
        map.put(banana.clone(), banana_entry);

        match map.get(&apple) {
            Some(retrieved) => {
                assert_eq!(retrieved.get_file_id(), apple_entry.get_file_id());
                assert_eq!(retrieved.get_offset(), apple_entry.get_offset());
                assert_eq!(retrieved.get_size(), apple_entry.get_size());
            }
            None => panic!("Expected Some, got None"),
        }

        match map.get(&banana) {
            Some(retrieved) => {
                assert_eq!(retrieved.get_file_id(), banana_entry.get_file_id());
                assert_eq!(retrieved.get_offset(), banana_entry.get_offset());
                assert_eq!(retrieved.get_size(), banana_entry.get_size());
            }
            None => panic!("Expected Some, got None"),
        }
    }

    #[test]
    fn test_hashmap_get_non_existing_entry() {
        let map = HashMap::new();

        let key = b"key".to_vec();

        let result = map.get(&key);
        assert!(result.is_none(), "Expected None, got {:?}", result);
    }

    #[test]
    fn test_hashmap_delete_existing_entry() {
        let map = HashMap::new();

        let apple = b"apple".to_vec();
        let apple_entry = KeyDirEntry::new(random_u32(), random_u64(), random_u32());

        let banana = b"banana".to_vec();
        let banana_entry = KeyDirEntry::new(random_u32(), random_u64(), random_u32());

        map.put(apple.clone(), apple_entry);
        map.put(banana.clone(), banana_entry);

        match map.delete(&apple) {
            Some(deleted_entry) => {
                assert_eq!(deleted_entry.get_file_id(), apple_entry.get_file_id());
                assert_eq!(deleted_entry.get_offset(), apple_entry.get_offset());
                assert_eq!(deleted_entry.get_size(), apple_entry.get_size());
            }
            None => panic!("Expected Some, got None"),
        }

        match map.delete(&banana) {
            Some(deleted_entry) => {
                assert_eq!(deleted_entry.get_file_id(), banana_entry.get_file_id());
                assert_eq!(deleted_entry.get_offset(), banana_entry.get_offset());
                assert_eq!(deleted_entry.get_size(), banana_entry.get_size());
            }
            None => panic!("Expected Some, got None"),
        }
    }

    #[test]
    fn test_hashmap_delete_non_existing_entry() {
        let map = HashMap::new();

        let key = b"key".to_vec();

        let result = map.delete(&key);
        assert!(result.is_none(), "Expected None, got {:?}", result);
    }
}
