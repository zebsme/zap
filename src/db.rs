use crate::{
    index::Indexer,
    io::StandardIO,
    options::{Context, Opts},
    storage::{DataEntry, FileHandle},
    Error, KeyDirEntry, Result, State,
};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::Ordering;
use std::{
    io::ErrorKind,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicU32, Arc},
};

#[allow(dead_code)]
pub struct Db {
    options: Opts,
    ctx: Context,
    active_file: FileHandle,
    inactive_files: Arc<DashMap<u32, FileHandle>>,
    file_id: AtomicU32,
}

#[allow(dead_code)]
impl Db {
    fn open(&self) -> Result<()> {
        //Validate options
        validate_options(&self.options)?;
        //Hint File

        Ok(())
    }

    fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        // Check read-only state
        if self.options.read_only {
            return Err(Error::Io(ErrorKind::PermissionDenied.into()));
        }

        // Validate sizes
        if key.is_empty() || key.len() > self.options.max_key_size {
            return Err(Error::Unsupported(format!(
                "limited max_key_size: {}, actual key size:{}",
                self.options.max_key_size,
                key.len()
            )));
        }
        if value.len() > self.options.max_value_size {
            return Err(Error::Unsupported(format!(
                "limited max_value_size: {}, actual value size:{}",
                self.options.max_key_size,
                key.len()
            )));
        }

        let dir_path = self.options.dir_path.clone();

        // Create entry
        let entry = DataEntry::new(key.clone(), value, State::Active);

        let encoded_entry = entry.encode()?;

        let record_len = encoded_entry.len() as u64;

        if self.get_offset() + record_len > self.options.data_file_size {
            // persist current active file
            self.sync()?;

            let current_fid = self.file_id.fetch_add(1, Ordering::SeqCst);

            self.inactive_files
                .insert(current_fid, self.active_file.clone());
            // create new file
            let new_file = FileHandle::new(
                current_fid + 1,
                StandardIO::new(format!(
                    "{}/default{}",
                    dir_path,
                    self.file_id.load(Ordering::SeqCst)
                ))?
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
        if key.is_empty() || key.len() > self.options.max_key_size {
            return Err(Error::Unsupported(format!(
                "limited max_key_size: {}, actual key size:{}",
                self.options.max_key_size,
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
    use crate::io::{IOHandler, IO};

    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_single_thread_put_and_read() {
        let io: IO = StandardIO::new("/tmp/default").unwrap().into();
        let file_id = io.get_file_id();
        let mut db = Db {
            options: Opts::default(),
            ctx: Context::new(io.get_file_id()),
            active_file: FileHandle::new(file_id, io),
            inactive_files: Arc::new(DashMap::new()),
            file_id: AtomicU32::from(file_id),
        };

        for i in 1..100000 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            println!("key: {:?}, value: {:?}", key, value);
            println!("{}", i);
            match db.put(key.clone(), value.clone()) {
                Ok(_) => println!("put success: key: {:?}, value: {:?}", key, value),
                Err(e) => println!("put error: {:?}", e),
            }
            assert_eq!(db.read(key.clone()).unwrap(), value);
        }
    }
}
