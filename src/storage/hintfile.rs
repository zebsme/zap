use crate::{io::StandardIO, KeyDirEntry, Result};
use std::{
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

use super::{DataEntry, FileHandle, State};

const HINT_FILE_NAME: &str = "hint";
pub struct HintFile(FileHandle);

impl HintFile {
    pub fn new(dir_path: &PathBuf) -> HintFile {
        HintFile(FileHandle::new(
            0,
            StandardIO::new(Path::new(dir_path).join(HINT_FILE_NAME))
                .unwrap()
                .into(),
        ))
    }

    pub fn write_entry(&mut self, key: Vec<u8>, keydir_entry: &KeyDirEntry) -> Result<()> {
        let entry = DataEntry::new(key, keydir_entry.encode(), State::Active);
        let encoded_entry = entry.encode()?;
        self.write(&encoded_entry)?;
        Ok(())
    }
}

impl Deref for HintFile {
    type Target = FileHandle;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for HintFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
