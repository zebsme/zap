use crate::batch::{decode_transaction_key, encode_transaction_key};
use crate::db::{Db, NON_COMMITTED};
use crate::index::Indexer;
use crate::io::StandardIO;
use crate::storage::{DataEntry, FileHandle, HintFile};
use crate::{Error, Result, State};

const MERGE_FINISHED_FILE: &str = "merge_finished";
const MERGE_FINISHED_KEY: &str = "__MERGE_FINISHED__";

#[allow(dead_code)]
impl Db {
    pub fn merge(&mut self) -> Result<()> {
        let read_guard = self.active_file.read();
        if read_guard.get_offset() == 0 && self.inactive_files.len() == 0 {
            return Err(Error::Unsupported("Merge when db is empty".to_string()));
        }

        let mut opts = self.ctx.opts.clone();
        let filename = opts.dir_path.file_name().unwrap();
        opts.dir_path
            .set_file_name(format!("{}-merge", filename.to_string_lossy()));
        let merge_db = Db::open(&opts)?;

        // Get Filehandles that need to be merged
        let mut file_handles = Vec::new();

        self.inactive_files.iter().for_each(|file| {
            file_handles.push((file.get_file_id(), file.clone()));
        });

        file_handles.push((read_guard.get_file_id(), read_guard.clone()));

        file_handles.sort_by(|a, b| a.0.cmp(&b.0));

        drop(read_guard);
        self.rotate_active_file()?;

        let mut hint_file = HintFile::new(&merge_db.ctx.opts.dir_path);
        for (_, file) in file_handles.iter() {
            let mut offset = 0;
            loop {
                let (mut entry, size) = match file.extract_data_entry(offset) {
                    Ok((entry, size)) => (entry, size),
                    Err(_) => {
                        //FIXME: == cannot be applied to result::Error
                        // if e == Error::Io(ErrorKind::UnexpectedEof.into()) {
                        //     break;
                        // };
                        break;
                    }
                };
                let (key, _) = decode_transaction_key(entry.get_key().clone());
                if let Some(keydir_entry) = self.ctx.index.get(&key) {
                    if keydir_entry.get_file_id() == file.get_file_id()
                        && keydir_entry.get_offset() == offset
                    {
                        let key = encode_transaction_key(key, NON_COMMITTED);
                        entry.set_key(key.clone());
                        let keydir_entry = merge_db.append_entry(&entry)?;
                        hint_file.write_entry(key, &keydir_entry)?;
                    }
                }
                offset += size as u64;
            }
        }

        merge_db.sync()?;
        hint_file.sync()?;

        let unmerged_file_id = file_handles.last().unwrap().0 + 1;
        let mut merge_finished_file = FileHandle::new(
            0,
            StandardIO::new(merge_db.ctx.opts.dir_path.join(MERGE_FINISHED_FILE))
                .unwrap()
                .into(),
        );

        let entry = DataEntry::new(
            MERGE_FINISHED_KEY,
            unmerged_file_id.to_string().into_bytes(),
            State::Active,
        );

        let enc_record = entry.encode()?;
        merge_finished_file.write(&enc_record)?;
        merge_finished_file.sync()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::*;
    #[test]
    fn test_merge() -> Result<()> {
        let opts = Opts::new(
            256,
            1024,
            false,
            true,
            "/tmp/test_merge".to_string(),
            1024 * 1024,
        );
        let mut db = Db::open(&opts)?;
        for i in 0..1000 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            db.put(key, value)?;
        }

        db.merge()?;

        Ok(())
    }
}
