use crate::{
    index::Indexer,
    io::StandardIO,
    options::{Context, Opts},
    storage::{DataEntry, FileHandle},
    Error, KeyDirEntry, Result, State,
};
use bytes::Bytes;
use dashmap::DashMap;
use std::{
    io::ErrorKind,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicU32, Arc},
};
use std::{path::Path, sync::atomic::Ordering};

#[allow(dead_code)]
pub struct Db {
    ctx: Context,
    active_file: FileHandle,
    inactive_files: Arc<DashMap<u32, FileHandle>>,
    file_id: AtomicU32,
}

#[allow(dead_code)]
impl Db {
    fn open(&self) -> Result<()> {
        //Validate options
        validate_options(&self.ctx.opts)?;
        //Hint File

        Ok(())
    }

    fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        // Check read-only state
        if self.ctx.opts.read_only {
            return Err(Error::Io(ErrorKind::PermissionDenied.into()));
        }

        // Validate sizes
        if key.is_empty() || key.len() > self.ctx.opts.max_key_size {
            return Err(Error::Unsupported(format!(
                "limited max_key_size: {}, actual key size:{}",
                self.ctx.opts.max_key_size,
                key.len()
            )));
        }
        if value.len() > self.ctx.opts.max_value_size {
            return Err(Error::Unsupported(format!(
                "limited max_value_size: {}, actual value size:{}",
                self.ctx.opts.max_key_size,
                key.len()
            )));
        }

        let dir_path = self.ctx.opts.dir_path.clone();

        // Create entry
        let entry = DataEntry::new(key.clone(), value, State::Active);

        let encoded_entry = entry.encode()?;

        let record_len = encoded_entry.len() as u64;

        if self.get_offset() + record_len > self.ctx.opts.data_file_size {
            // persist current active file
            self.sync()?;

            let current_fid = self.file_id.fetch_add(1, Ordering::SeqCst);

            self.inactive_files
                .insert(current_fid, self.active_file.clone());
            // create new file
            let new_file = FileHandle::new(
                current_fid + 1,
                StandardIO::new(Path::new(&dir_path).join(format!(
                    "{}-{}",
                    self.ctx.opts.file_prefix,
                    self.file_id.load(Ordering::SeqCst)
                )))?
                .into(),
            );
            self.active_file = new_file;
        }

        // Append entry to data file
        let written = self.write(&encoded_entry)?;

        let keydir_entry = KeyDirEntry::new(
            self.file_id.load(Ordering::SeqCst),
            //offset is not active_file offset
            self.active_file.get_offset() - written as u64,
            encoded_entry.len() as u32,
        );

        self.ctx.index.put(key.into(), keydir_entry);

        Ok(())
    }

    fn read(&self, key: Bytes) -> Result<Vec<u8>> {
        // Validate key
        if key.is_empty() || key.len() > self.ctx.opts.max_key_size {
            return Err(Error::Unsupported(format!(
                "limited max_key_size: {}, actual key size:{}",
                self.ctx.opts.max_key_size,
                key.len()
            )));
        }

        match self.ctx.index.get(&key) {
            Some(entry) => {
                let data_entry = self.read_data_entry(entry)?;
                return Ok(data_entry.get_value().clone());
            }
            None => Err(Error::Unsupported(
                "Db read error: Key not found".to_string(),
            )),
        }
    }

    fn read_data_entry(&self, entry: KeyDirEntry) -> Result<DataEntry> {
        // Get file_id, offset, length
        let file_id = entry.get_file_id();
        let offset = entry.get_offset();
        // Read from active file
        let data_entry = if file_id == self.file_id.load(Ordering::SeqCst) {
            self.extract_data_entry(offset)?
        } else {
            // Read from inactive file
            match self.inactive_files.get(&file_id) {
                Some(inactive_file) => inactive_file.extract_data_entry(offset)?,
                None => {
                    return Err(Error::Unsupported(
                        "Db read error: File not found".to_string(),
                    ))
                }
            }
        };
        if !data_entry.is_active() {
            return Err(Error::Unsupported(
                "Db read error: Entry removed".to_string(),
            ));
        }
        Ok(data_entry)
    }
}

fn validate_options(options: &Opts) -> Result<()> {
    if options.max_key_size == 0 {
        return Err(Error::Unsupported(
            "validate options error: max_key_size is required to be greater than 0".to_string(),
        ));
    }

    if options.max_value_size == 0 {
        return Err(Error::Unsupported(
            "validate options error: max_value_size is required to be greater than 0".to_string(),
        ));
    }

    if options.data_file_size == 0 {
        return Err(Error::Unsupported(
            "validate options error: data_file_size is required to be greater than 0".to_string(),
        ));
    }

    if options.dir_path.is_empty() {
        return Err(Error::Unsupported(
            "validate options error: dir_path is required".to_string(),
        ));
    }

    Ok(())
}

impl Deref for Db {
    type Target = FileHandle;

    fn deref(&self) -> &Self::Target {
        &self.active_file
    }
}

impl DerefMut for Db {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.active_file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{IOHandler, IO};
    use bytes::Bytes;
    use std::thread;

    #[test]
    fn test_single_thread_put_and_read() {
        let io: IO = StandardIO::new("/tmp/put_and_read").unwrap().into();
        let file_id = io.get_file_id();
        let mut db = Db {
            ctx: Context::new(Opts::new(
                256,
                1024,
                false,
                true,
                "/tmp".to_string(),
                "put_and_read".to_string(),
                1024 * 1024,
            )),
            active_file: FileHandle::new(file_id, io),
            inactive_files: Arc::new(DashMap::new()),
            file_id: AtomicU32::from(file_id),
        };

        for i in 1..100000 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            match db.put(key.clone(), value.clone()) {
                Ok(_) => println!("put success: key: {:?}, value: {:?}", key, value),
                Err(e) => println!("put error: {:?}", e),
            }
            assert_eq!(db.read(key.clone()).unwrap(), value);
        }
    }

    #[test]
    fn test_concurrent_read() -> anyhow::Result<()> {
        // Setup test DB
        let io: IO = StandardIO::new("/tmp/concurrent_read").unwrap().into();
        let file_id = io.get_file_id();
        let mut db = Db {
            ctx: Context::new(Opts::new(
                256,
                1024,
                false,
                true,
                "/tmp".to_string(),
                "concurrent_read".to_string(),
                1024 * 1024,
            )),
            active_file: FileHandle::new(file_id, io),
            inactive_files: Arc::new(DashMap::new()),
            file_id: AtomicU32::from(file_id),
        };

        // Insert test data
        for i in 0..1000000 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            db.put(key.clone(), value.clone())?;
        }

        // Create shared DB reference
        let db = Arc::new(db);
        let start = std::time::Instant::now();

        // Spawn multiple reader threads
        let mut handles = vec![];
        for i in 0..1000 {
            let db = db.clone();
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));

            let handle = thread::spawn(move || {
                let read_value = db.read(key.clone()).unwrap();
                assert_eq!(read_value, value, "Read value mismatch in thread {}", i);
            });
            handles.push(handle);
        }

        // Wait for all reads to complete
        for handle in handles {
            handle
                .join()
                .map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))?;
        }

        let duration = start.elapsed();
        println!("All concurrent reads completed in {:?}", duration);

        Ok(())
    }
}
