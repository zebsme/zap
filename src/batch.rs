use crate::db::Db;
use crate::index::Indexer;
use crate::{storage::DataEntry, Result};
use crate::{Error, KeyDirEntry, State};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use prost::{decode_length_delimiter, encode_length_delimiter};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

const COMMITTED_KEY: &[u8] = b"__COMMITTED__";

#[allow(dead_code)]
pub struct WriteBatch<'a> {
    db: &'a Db,
    pending_writes: Arc<DashMap<Vec<u8>, DataEntry>>,
    opts: WriteBatchOptions,
}

pub struct WriteBatchOptions {
    pub max_batch_num: usize,

    pub sync_writes: bool,
}

#[allow(dead_code)]
impl Db {
    pub fn new_write_batch(&self, opts: WriteBatchOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            pending_writes: Arc::new(DashMap::new()),
            db: self,
            opts,
        })
    }
}
#[allow(dead_code)]
impl WriteBatch<'_> {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::Unsupported("Key is required".to_string()));
        }

        let entry = DataEntry::new(key.clone(), value, State::Active);

        self.pending_writes.insert(key.into(), entry);

        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::Unsupported("Key is required".to_string()));
        }

        let index_pos = self.db.ctx.index.get(&key);
        if index_pos.is_none() {
            if self.pending_writes.contains_key(&key.to_vec()) {
                self.pending_writes.remove(&key.to_vec());
            }
            return Ok(());
        }

        let record = DataEntry::new(key.clone(), Vec::new(), State::Inactive);

        self.pending_writes.insert(key.to_vec(), record);

        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        if self.pending_writes.len() == 0 {
            return Ok(());
        }
        if self.pending_writes.len() > self.opts.max_batch_num {
            return Err(Error::Unsupported("Exceeds max batch number".to_string()));
        }

        let _lock = self.db.batch_commit_lock.lock();
        // Add a lock to ensure that only one batch is committed at a time

        let seq_no = self.db.sequence_number.fetch_add(1, Ordering::SeqCst);

        let keydir_entries = self.pending_writes.iter().try_fold(
            HashMap::new(),
            |mut acc, r| -> Result<HashMap<Vec<u8>, KeyDirEntry>> {
                let item = r.value();
                let entry = DataEntry::new(
                    encode_transaction_key(item.get_key().clone(), seq_no),
                    item.get_value().clone(),
                    item.get_state(),
                );

                let keydir_entry = self.db.append_entry(&entry)?;
                acc.insert(item.get_key().clone(), keydir_entry);
                Ok(acc)
            },
        )?;

        let committed_entry = DataEntry::new(
            encode_transaction_key(COMMITTED_KEY.to_vec(), seq_no),
            Vec::new(),
            State::Committed,
        );
        self.db.append_entry(&committed_entry)?;

        if self.opts.sync_writes {
            self.db.sync()?;
        }

        self.pending_writes.iter().for_each(|r| {
            let item = r.value();
            if item.is_active() {
                let keydir_entry = keydir_entries.get(item.get_key()).unwrap();
                self.db.ctx.index.put(item.get_key().clone(), *keydir_entry);
            }
        });

        self.pending_writes.clear();

        Ok(())
    }
}

pub(crate) fn encode_transaction_key(key: Vec<u8>, seq_no: u32) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no as usize, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}

pub(crate) fn decode_transaction_key(key: Vec<u8>) -> (Vec<u8>, u32) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf).unwrap();
    (buf.to_vec(), seq_no as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    #[test]
    fn test_write_batch() -> Result<()> {
        let opts = Opts::new(
            256,
            1024,
            false,
            true,
            "/tmp/write_batch".to_string(),
            1024 * 1024,
        );
        let db = Db::open(&opts)?;
        // let opts = WriteBatchOptions {
        //     max_batch_num: 10,
        //     sync_writes: true,
        // };
        // let write_batch = match db.new_write_batch(opts){
        //     Ok(wb) => wb,
        //     Err(e) => return Err(e),
        // };
        // for i in 0..10  {
        //     let key = Bytes::from(format!("key{}", i));
        //     let value = Bytes::from(format!("value{}", i));
        //     write_batch.put(key, value)?;
        // }
        // write_batch.commit()?;

        for i in 0..10 {
            let key = Bytes::from(format!("key{}", i));
            // let value = Bytes::from(format!("value{}", i));
            assert!(db.get(key.clone()).is_err());
        }
        Ok(())
    }
}
