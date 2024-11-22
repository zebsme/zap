use crate::{
    batch::{decode_transaction_key, encode_transaction_key},
    index::{HashMap, Indexer},
    io::{MmapIO, StandardIO},
    merge::MERGE_FINISHED_FILE,
    options::{Context, Opts},
    storage::{decode_keydir_entry, DataEntry, FileHandle, HintFile, HINT_FILE_NAME},
    Error, KeyDirEntry, Result, State,
};
use bytes::Bytes;
use dashmap::DashMap;
use fs2::FileExt;
use parking_lot::{Mutex, RwLock};
use std::{
    fs::{self, create_dir_all, read_dir, remove_dir_all, File},
    io::ErrorKind,
    sync::{atomic::AtomicU32, Arc},
};
use std::{path::Path, sync::atomic::Ordering};

const FILE_SUFFIX: &str = ".db";
const INITIAL_FILE_ID: u32 = 0;
const FILE_LOCK: &str = "file.lock";
pub(crate) const NON_COMMITTED: u32 = 0;
#[derive(Debug)]
pub struct Db {
    pub ctx: Context,
    pub active_file: Arc<RwLock<FileHandle>>,
    pub inactive_files: Arc<DashMap<u32, FileHandle>>,
    file_id: AtomicU32,
    pub sequence_number: Arc<AtomicU32>,
    pub batch_commit_lock: Mutex<()>,
    lock_file: File,
}

#[allow(dead_code)]
impl Db {
    pub fn open(opts: &Opts) -> Result<Self> {
        //Validate options
        validate_options(opts)?;

        let dir_path = opts.dir_path.clone();
        //Get iterator of all files in the directory
        if !dir_path.is_dir() {
            if let Err(e) = create_dir_all(&opts.dir_path) {
                return Err(Error::Io(e));
            }
        }

        // Check if the directory is already in use
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(dir_path.join(FILE_LOCK))?;
        if lock_file.try_lock_exclusive().is_err() {
            return Err(Error::Unsupported("Database is already in use".to_string()));
        }

        process_merge_files(&dir_path)?;

        // return_dir will return an error in the following situations, but is not limited to just these cases:
        // 1. The provided path doesn't exist.
        // 2. The process lacks permissions to view the contents.
        // 3. The path points at a non-directory file.
        // we already checked if the path is a directory and created it if it doesn't exist
        let dir_iter = match read_dir(&dir_path) {
            Ok(iter) => iter,
            Err(_) => return Err(Error::Io(ErrorKind::PermissionDenied.into())),
        };

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
                    MmapIO::new(
                        &Path::new(&opts.dir_path).join(format!("{}{}", file_id, FILE_SUFFIX)),
                    )
                    .unwrap()
                    .into(),
                );
                filehandle
            })
            .collect::<Vec<FileHandle>>();

        let inactive_files = DashMap::new();
        let index = HashMap::new();
        let mut current_sequence_number = NON_COMMITTED;
        let active_file = match file_handles.pop() {
            Some(active_file) => {
                for file in file_handles.iter() {
                    Self::process_file_handle(file, &index, &mut current_sequence_number);
                    inactive_files.insert(file.get_file_id(), file.clone());
                }
                Self::process_file_handle(&active_file, &index, &mut current_sequence_number);
                active_file
            }
            None => FileHandle::new(
                INITIAL_FILE_ID,
                MmapIO::new(
                    &Path::new(&dir_path).join(format!("{}{}", INITIAL_FILE_ID, FILE_SUFFIX,)),
                )?
                .into(),
            ),
        };

        let file_id = active_file.get_file_id();
        let db = Db {
            ctx: Context::new(opts, index),
            active_file: Arc::new(RwLock::new(active_file)),
            inactive_files: Arc::new(inactive_files),
            file_id: AtomicU32::from(file_id),
            sequence_number: Arc::new(AtomicU32::new(current_sequence_number + 1)),
            batch_commit_lock: Mutex::new(()),
            lock_file,
        };

        let mut write_guard = db.active_file.write();
        write_guard.set_io(&dir_path)?;
        drop(write_guard);

        for file in db.inactive_files.iter() {
            let mut file = file.value().to_owned();
            file.set_io(&dir_path)?;
        }

        db.load_index_from_hint_file()?;

        Ok(db)
    }

    /// Processes a file handle and loads its entries into the index.
    ///
    /// This function reads all entries from the specified file handle, updates the index with active entries,
    /// and collects deleted keys for later removal.
    fn process_file_handle(file: &FileHandle, index: &HashMap, current_sequence_number: &mut u32) {
        let mut transactions: std::collections::HashMap<u32, Vec<(DataEntry, KeyDirEntry)>> =
            std::collections::HashMap::new();
        let mut offset = 0;
        let file_id = file.get_file_id();
        while let Ok((mut data_entry, size)) = file.extract_data_entry(offset) {
            let keydir_entry = KeyDirEntry::new(file_id, offset, size as u32);
            let (key, seq_no) = decode_transaction_key(data_entry.get_key().clone());
            if seq_no == NON_COMMITTED {
                match data_entry.get_state() {
                    State::Active => {
                        index.put(key, keydir_entry);
                    }
                    _ => {
                        index.delete(&key);
                    }
                }
            } else if data_entry.get_state() == State::Committed {
                let entry = transactions.get(&seq_no).unwrap();
                entry.iter().for_each(|(data_entry, keydir_entry)| {
                    index.put(data_entry.get_key().clone(), *keydir_entry);
                    match data_entry.get_state() {
                        State::Active => {
                            index.put(data_entry.get_key().clone(), *keydir_entry);
                        }
                        _ => {
                            index.delete(&key);
                        }
                    }
                });
                transactions.remove(&seq_no);
            } else {
                data_entry.set_key(key);
                transactions
                    .entry(seq_no)
                    .or_default()
                    .push((data_entry, keydir_entry));
            }
            if *current_sequence_number < seq_no {
                *current_sequence_number = seq_no;
            }
            offset += size as u64;
        }
        file.set_offset(offset);
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
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
        let deleted_entry = DataEntry::new(
            encode_transaction_key(key.clone().into(), NON_COMMITTED),
            Vec::new(),
            State::Inactive,
        );
        self.append_entry(&deleted_entry)?;

        // Remove key from index
        self.ctx.index.delete(&key);

        Ok(())
    }

    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
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
        let entry = DataEntry::new(
            encode_transaction_key(key.clone().into(), NON_COMMITTED),
            value,
            State::Active,
        );
        let keydir_entry = self.append_entry(&entry)?;

        self.ctx.index.put(key.into(), keydir_entry);

        Ok(())
    }

    pub fn append_entry(&self, entry: &DataEntry) -> Result<KeyDirEntry> {
        let encoded_entry = entry.encode()?;
        let dir_path = self.ctx.opts.dir_path.clone();
        let record_len = encoded_entry.len() as u64;
        let mut write_guard = self.active_file.write();
        if write_guard.get_offset() + record_len > self.ctx.opts.data_file_size {
            // persist current active file
            write_guard.sync()?;

            let current_fid = self.file_id.fetch_add(1, Ordering::SeqCst);

            self.inactive_files.insert(current_fid, write_guard.clone());
            // create new file
            let new_file = FileHandle::new(
                current_fid + 1,
                StandardIO::new(&Path::new(&dir_path).join(format!(
                    "{}{}",
                    self.file_id.load(Ordering::SeqCst),
                    FILE_SUFFIX,
                )))?
                .into(),
            );
            *write_guard = new_file;
        }

        // Append entry to data file
        let written = write_guard.write(&encoded_entry)?;

        Ok(KeyDirEntry::new(
            self.file_id.load(Ordering::SeqCst),
            //offset is not active_file offset
            write_guard.get_offset() - written as u64,
            encoded_entry.len() as u32,
        ))
    }

    pub fn rotate_active_file(&self) -> Result<()> {
        // persist current active file
        let mut write_guard = self.active_file.write();
        write_guard.sync()?;

        let current_fid = self.file_id.fetch_add(1, Ordering::SeqCst);

        self.inactive_files.insert(current_fid, write_guard.clone());
        // create new file
        let new_file = FileHandle::new(
            current_fid + 1,
            StandardIO::new(&Path::new(&self.ctx.opts.dir_path).join(format!(
                "{}{}",
                self.file_id.load(Ordering::SeqCst),
                FILE_SUFFIX,
            )))?
            .into(),
        );
        *write_guard = new_file;
        Ok(())
    }
    pub fn get(&self, key: Bytes) -> Result<Vec<u8>> {
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
            let read_guard = self.active_file.read();
            read_guard.extract_data_entry(offset)?
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

    pub(crate) fn load_index_from_hint_file(&self) -> Result<()> {
        let hint_file_name = self.ctx.opts.dir_path.join(HINT_FILE_NAME);

        if !hint_file_name.is_file() {
            return Ok(());
        }

        let hint_file = HintFile::new(&self.ctx.opts.dir_path);
        let mut offset = 0;
        loop {
            let (entry, size) = match hint_file.extract_data_entry(offset) {
                Ok((entry, size)) => (entry, size),
                Err(e) => {
                    if let Error::Io(ref io_error) = e {
                        if io_error.kind() == ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(e);
                }
            };

            let keydir_entry = decode_keydir_entry(entry.get_value().clone())?;

            self.ctx.index.put(entry.get_key().clone(), keydir_entry);
            offset += size as u64;
        }
        Ok(())
    }
    pub fn sync(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }

    pub fn close(&mut self) -> Result<()> {
        if !self.ctx.opts.dir_path.is_dir() {
            return Ok(());
        }

        self.sync()?;

        self.lock_file.unlock()?;

        Ok(())
    }

    pub fn back_up(&self, dir_path: &Path) -> Result<()> {
        copy_recursive(&self.ctx.opts.dir_path, dir_path)?;
        Ok(())
    }
}

fn copy_recursive(src: &Path, dst: &Path) -> Result<()> {
    if !dst.exists() {
        create_dir_all(dst)?;
    }
    for dentry in read_dir(src)? {
        let dentry = dentry?;
        let src_path = dentry.path();
        if src_path.file_name().unwrap() == FILE_LOCK {
            continue;
        }
        let dst_path = dst.join(dentry.file_name());
        if dentry.file_type()?.is_dir() {
            copy_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn process_merge_files(dir_path: &Path) -> Result<()> {
    // Handle merge
    // Step 1: Check if the merge directory exists
    let filename = dir_path.file_name().unwrap();
    let mut merge_dir = dir_path.to_path_buf();
    merge_dir.set_file_name(format!("{}-merge", filename.to_string_lossy()));
    let mut unmerged_file_id: u32 = 0;
    let mut merge_file_names = Vec::new();
    match read_dir(merge_dir.clone()) {
        Ok(dir) => {
            // Check if the merge finished
            let merge_file = MERGE_FINISHED_FILE.to_string();
            if merge_dir.join(merge_file.clone()).is_file() {
                // Merge is finished, load the merged file
                let file_handle = FileHandle::new(
                    0,
                    StandardIO::new(&merge_dir.join(merge_file.clone()))
                        .unwrap()
                        .into(),
                );
                let entry = match file_handle.extract_data_entry(0) {
                    Ok((entry, _)) => entry,
                    Err(_) => {
                        remove_dir_all(merge_dir)?;
                        return Ok(());
                    }
                };
                //Parse from bytes to u32
                let s = String::from_utf8_lossy(entry.get_value());
                unmerged_file_id = s.parse::<u32>().unwrap();
                // Handle files in directory use while let
                for file in dir {
                    let file = file?;
                    merge_file_names.push(file.file_name());
                }
            }
        }
        Err(_) => {
            return Ok(());
        }
    }
    for file_id in 0..unmerged_file_id {
        let file = dir_path.join(format!("{}{}", file_id, FILE_SUFFIX));
        if file.is_file() {
            fs::remove_file(file)?;
        }
    }

    for file_name in merge_file_names {
        fs::rename(merge_dir.join(file_name.clone()), dir_path.join(file_name))?;
    }

    fs::remove_dir_all(merge_dir.clone())?;
    Ok(())
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

impl Drop for Db {
    fn drop(&mut self) {
        self.close().expect("failed to close db");
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

        for i in 1..10000 {
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
        println!("db: {:?}", db);
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

    #[test]
    fn test_back_up() -> Result<()> {
        let opts = Opts::new(
            256,
            1024,
            false,
            true,
            "/tmp/test_back_up".to_string(),
            1024 * 1024,
        );
        let mut db = Db::open(&opts)?;

        let key = Bytes::from("key");
        let value = Bytes::from("value");
        db.put(key.clone(), value)?;

        let back_up_path = "/tmp/back_up_test";
        let back_up_res = db.back_up(Path::new(back_up_path));
        assert!(back_up_res.is_ok());

        Ok(())
    }
}
