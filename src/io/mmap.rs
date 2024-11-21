use crate::{Error, Result};
use memmap2::Mmap;
use parking_lot::Mutex;
use std::{fs::OpenOptions, io::ErrorKind, path::Path, sync::Arc};

use super::IOHandler;

#[derive(Debug, Clone)]
pub struct MmapIO {
    mmap: Arc<Mutex<Mmap>>,
}

#[allow(dead_code)]
impl MmapIO {
    pub fn new(file_name: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_name)?;

        let mmap = unsafe { Mmap::map(&file)? };

        Ok(MmapIO {
            mmap: Arc::new(Mutex::new(mmap)),
        })
    }
}

#[allow(dead_code)]
impl IOHandler for MmapIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let mmap_buffer = self.mmap.lock();
        let end = offset + buf.len() as u64;
        if end > mmap_buffer.len() as u64 {
            return Err(Error::Io(ErrorKind::UnexpectedEof.into()));
        }
        let val = &mmap_buffer[offset as usize..end as usize];
        buf.copy_from_slice(val);

        Ok(val.len())
    }

    fn write(&mut self, _buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    fn get_file_id(&self) -> u32 {
        unimplemented!()
    }
}
