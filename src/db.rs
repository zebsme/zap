use crate::{
    index::{HashMap, Indexer},
    io::StandardIO,
    options::{Context, Opts},
    storage::{DataEntry, FileHandle},
    Error, KeyDirEntry, Result, State,
};
use bytes::Bytes;
use dashmap::DashMap;
use std::{
    fs::{create_dir_all, read_dir},
    io::ErrorKind,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicU32, Arc},
};
use std::{path::Path, sync::atomic::Ordering};

const FILE_SUFFIX: &str = ".db";
const INITIAL_FILE_ID: u32 = 0;
#[derive(Debug)]
pub struct Db {
    ctx: Context,
    active_file: FileHandle,
    inactive_files: Arc<DashMap<u32, FileHandle>>,
    file_id: AtomicU32,
}

#[allow(dead_code)]
impl Db {
    fn open(opts: &Opts) -> Result<Self> {
        //Validate options
        validate_options(opts)?;

        //Get iterator of all files in the directory
        let dir_path = opts.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = create_dir_all(&opts.dir_path) {
                return Err(Error::Io(e));
            }
        }

        // return_dir will return an error in the following situations, but is not limited to just these cases:
        // 1. The provided path doesn't exist.
        // 2. The process lacks permissions to view the contents.
        // 3. The path points at a non-directory file.
        // we already checked if the path is a directory and created it if it doesn't exist
        let dir_iter = match read_dir(&dir_path) {
            Ok(iter) => iter,
            Err(_) => return Err(Error::Io(ErrorKind::PermissionDenied.into())),
        };

        // TODO: Check the directory if it is already being used by another db

        // Load all file_ids
        let mut file_ids = dir_iter
            .filter_map(|file| {
                if let Ok(file) = file {
                    let file_name = file.file_name().into_string().unwrap();
                    if file_name.ends_with(FILE_SUFFIX) {
                        let file_id = file_name.split(".").next().unwrap();
                        let file_id = file_id.parse::<u32>().unwrap();
                        Some(file_id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<u32>>();

        // Ensure that the file_ids are in order
        file_ids.sort();
        // Create file_handles
        let mut file_handles = file_ids
            .iter()
            .map(|file_id| {
                let filehandle = FileHandle::new(
                    *file_id,
                    StandardIO::new(
                        Path::new(&opts.dir_path).join(format!("{}{}", file_id, FILE_SUFFIX)),
                    )
                    .unwrap()
                    .into(),
                );
                filehandle
            })
            .collect::<Vec<FileHandle>>();

        let inactive_files = DashMap::new();
        let index = HashMap::new();
        let mut deleted_keys = Vec::new();
        let active_file = match file_handles.pop() {
            Some(active_file) => {
                for file in file_handles.iter().rev() {
                    Self::process_file_handle(file, &index, &mut deleted_keys);
                    inactive_files.insert(file.get_file_id(), file.clone());
                }
                Self::process_file_handle(&active_file, &index, &mut deleted_keys);
                active_file
            }
            None => FileHandle::new(
                INITIAL_FILE_ID,
                StandardIO::new(
                    Path::new(&dir_path).join(format!("{}{}", INITIAL_FILE_ID, FILE_SUFFIX,)),
                )?
                .into(),
            ),
        };

        //Delete index keys after the insertions are finished
        for key in deleted_keys {
            index.delete(&key);
        }

        let file_id = active_file.get_file_id();
        let db = Db {
            ctx: Context::new(opts, index),
            active_file,
            inactive_files: Arc::new(inactive_files),
            file_id: AtomicU32::from(file_id),
        };
        Ok(db)
    }

    /// Processes a file handle and loads its entries into the index.
    ///
    /// This function reads all entries from the specified file handle, updates the index with active entries,
    /// and collects deleted keys for later removal.
    fn process_file_handle(file: &FileHandle, index: &HashMap, deleted_keys: &mut Vec<Vec<u8>>) {
        let mut offset = 0;
        let file_id = file.get_file_id();
        while let Ok((data_entry, size)) = file.extract_data_entry(offset) {
            let keydir_entry = KeyDirEntry::new(file_id, offset, size as u32);
            match data_entry.get_state() {
                State::Active => {
                    index.put(data_entry.get_key().clone(), keydir_entry);
                }
                State::Inactive => {
                    deleted_keys.push(data_entry.get_key().clone());
                }
            }
            offset += size as u64;
        }
        file.set_offset(offset);
    }

    fn delete(&mut self, key: Bytes) -> Result<()> {
        // Check read-only state
        if self.ctx.opts.read_only {
            return Err(Error::Io(ErrorKind::PermissionDenied.into()));
        }

        // Validate key
        if key.is_empty() {
            return Err(Error::Unsupported("Key is required".to_string()));
        }

        if key.len() > self.ctx.opts.max_key_size {
            return Err(Error::Unsupported(format!(
                "limited max_key_size: {}, actual key size:{}",
                self.ctx.opts.max_key_size,
                key.len()
            )));
        }

        // Get keydir_entry
        if self.ctx.index.get(&key).is_none() {
            return Ok(());
        }

        // Mark entry as deleted
        let deleted_entry = DataEntry::new(key.clone(), Vec::new(), State::Inactive);
        self.append_entry(&deleted_entry)?;

        // Remove key from index
        self.ctx.index.delete(&key);

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

        // Append entry to data file
        let entry = DataEntry::new(key.clone(), value, State::Active);
        let keydir_entry = self.append_entry(&entry)?;

        self.ctx.index.put(key.into(), keydir_entry);

        Ok(())
    }

    fn append_entry(&mut self, entry: &DataEntry) -> Result<KeyDirEntry> {
        let encoded_entry = entry.encode()?;
        let dir_path = self.ctx.opts.dir_path.clone();
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
                    "{}{}",
                    self.file_id.load(Ordering::SeqCst),
                    FILE_SUFFIX,
                )))?
                .into(),
            );
            self.active_file = new_file;
        }

        // Append entry to data file
        let written = self.write(&encoded_entry)?;

        Ok(KeyDirEntry::new(
            self.file_id.load(Ordering::SeqCst),
            //offset is not active_file offset
            self.active_file.get_offset() - written as u64,
            encoded_entry.len() as u32,
        ))
    }

    fn get(&self, key: Bytes) -> Result<Vec<u8>> {
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
        let (data_entry, _) = if file_id == self.file_id.load(Ordering::SeqCst) {
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

    pub fn sync(&mut self) -> Result<()> {
        self.active_file.sync()
    }

    pub fn close(&mut self) -> Result<()> {
        if !self.ctx.opts.dir_path.is_dir() {
            return Ok(());
        }

        self.sync()?;

        Ok(())
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

    match options.dir_path.to_str() {
        Some(path) => {
            if path.is_empty() {
                return Err(Error::Unsupported(
                    "validate options error: dir_path is required".to_string(),
                ));
            }
        }
        None => {
            return Err(Error::Unsupported(
                "validate options error: dir_path is required".to_string(),
            ));
        }
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
    use std::thread;

    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_open_db() -> Result<()> {
        let opts = Opts::new(
            256,
            1024,
            false,
            true,
            "/tmp/open_db".to_string(),
            1024 * 1024,
        );

        let db = Db::open(&opts)?;

        for i in 1..100 {
            let key = Bytes::from(format!("key{}", i));
            assert_eq!(
                db.get(key.clone()).unwrap_err().to_string(),
                Error::Unsupported("Db read error: Key not found".to_string()).to_string()
            );
        }

        for i in 101..100000 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            match db.get(key.clone()) {
                Ok(read_value) => assert_eq!(value, read_value),
                Err(e) => {
                    println!("read error: key: {:?}, error: {:?}", key, e);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_single_thread_put_and_read() -> Result<()> {
        let opts = Opts::new(
            256,
            1024,
            false,
            true,
            "/tmp/put_and_read".to_string(),
            1024 * 1024,
        );
        let mut db = Db::open(&opts)?;

        for i in 1..100000 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            match db.put(key.clone(), value.clone()) {
                Ok(_) => println!("put success: key: {:?}, value: {:?}", key, value),
                Err(e) => return Err(e),
            }
            assert_eq!(db.get(key.clone()).unwrap(), value);
        }
        Ok(())
    }

    #[test]
    fn test_concurrent_read() -> anyhow::Result<()> {
        let opts = Opts::new(
            256,
            1024,
            false,
            true,
            "/tmp/concurrent_read".to_string(),
            1024 * 1024,
        );
        let db = Db::open(&opts)?;

        // Create shared DB reference
        let db = Arc::new(db);
        let start = std::time::Instant::now();

        // Spawn multiple reader threads
        let mut handles = vec![];
        for i in 1..1000 {
            let db = db.clone();
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));

            let handle = thread::spawn(move || match db.get(key.clone()) {
                Ok(read_value) => {
                    assert_eq!(read_value, value, "Read value mismatch in thread {}", i)
                }
                Err(e) => println!("read error: key: {:?}, error: {:?}", key, e),
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

    #[test]
    fn test_delete() -> Result<()> {
        let opts = Opts::new(
            256,
            1024,
            false,
            true,
            "/tmp/delete".to_string(),
            1024 * 1024,
        );
        let mut db = Db::open(&opts)?;

        for i in 1..1000000 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            match db.put(key.clone(), value.clone()) {
                Ok(_) => println!("put success: key: {:?}, value: {:?}", key, value),
                Err(e) => return Err(e),
            }
        }

        for i in 1..100 {
            let key = Bytes::from(format!("key{}", i));
            match db.delete(key.clone()) {
                Ok(_) => println!("delete success: key: {:?}", key),
                Err(e) => return Err(e),
            }
            assert_eq!(
                db.get(key.clone()).unwrap_err().to_string(),
                Error::Unsupported("Db read error: Key not found".to_string()).to_string()
            );
        }

        for i in 1..100 {
            let key = Bytes::from(format!("key{}", i));
            assert_eq!(
                db.get(key.clone()).unwrap_err().to_string(),
                Error::Unsupported("Db read error: Key not found".to_string()).to_string()
            );
        }
        Ok(())
    }
    #[test]
    fn test_sync() -> Result<()> {
        let opts = Opts::new(256, 1024, false, true, "/tmp/sync".to_string(), 1024 * 1024);
        let mut db = Db::open(&opts).expect("failed to open engine");

        let key = Bytes::from("key");
        let value = Bytes::from("value");
        db.put(key.clone(), value)?;

        let close_res = db.sync();
        assert!(close_res.is_ok());

        Ok(())
    }
    #[test]
    fn test_close() -> Result<()> {
        let opts = Opts::new(
            256,
            1024,
            false,
            true,
            "/tmp/close".to_string(),
            1024 * 1024,
        );
        let mut db = Db::open(&opts)?;

        let key = Bytes::from("key");
        let value = Bytes::from("value");
        db.put(key, value)?;

        let close_res = db.close();
        assert!(close_res.is_ok());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
        Ok(())
    }
}
