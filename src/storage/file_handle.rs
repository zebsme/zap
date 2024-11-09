use crate::{
    io::{IOHandler, IO},
    Result,
};
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};

#[allow(dead_code)]
pub struct FileHandle {
    data: Arc<Datafile>,
    io: IO,
}

#[allow(dead_code)]
pub struct Datafile {
    file_id: AtomicU32,
    offset: AtomicU64,
}

#[allow(dead_code)]
impl FileHandle {
    pub fn new(file_id: u32, io: IO) -> Self {
        Self {
            data: Arc::new(Datafile::new(file_id)),
            io,
        }
    }

    // Delegate IO operations to the internal IO implementation
    pub fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.data.offset.store(offset, Ordering::Release);
        match &self.io {
            IO::Standard(io) => io.read(buf, offset),
        }
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let current_offset = self.data.offset.load(Ordering::Relaxed);
        let written = match &mut self.io {
            IO::Standard(io) => io.write(buf)?,
        };
        self.data
            .offset
            .store(current_offset + written as u64, Ordering::Release);
        Ok(written)
    }
}

#[allow(dead_code)]
impl Datafile {
    fn new(id: u32) -> Self {
        Self {
            file_id: AtomicU32::new(id),
            offset: AtomicU64::new(0),
        }
    }

    fn get_file_id(&self) -> u32 {
        self.file_id.load(Ordering::Relaxed)
    }

    fn get_offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    fn set_offset(&self, new_offset: u64) {
        self.offset.store(new_offset, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::StandardIO;
    use anyhow::Result;
    use parking_lot::Mutex;
    use std::thread;

    // Test Datafile basics
    #[test]
    fn test_datafile_new() {
        let file = Datafile::new(21);
        assert_eq!(file.get_file_id(), 21);
        assert_eq!(file.get_offset(), 0);
    }

    #[test]
    fn test_set_get_offset() {
        let file = Datafile::new(1);
        file.set_offset(100);
        assert_eq!(file.get_offset(), 100);
    }

    // Test FileHandle operations
    #[test]
    fn test_filehandle_new() -> Result<()> {
        let io: IO = StandardIO::new("/tmp/test_filehandle_new")?.into();
        let handle = FileHandle::new(42, io);
        assert_eq!(handle.data.get_file_id(), 42);
        Ok(())
    }

    #[test]
    fn test_filehandle_read() -> Result<()> {
        let io: IO = StandardIO::new("/tmp/test_filehandle_read")?.into();
        let handle = FileHandle::new(1, io);
        let mut buf = vec![0; 10];
        let res = handle.read(&mut buf, 100);
        assert!(res.is_ok());
        assert_eq!(handle.data.get_offset(), 100);
        Ok(())
    }
    #[test]
    fn test_concurrent_filehandle_updates() -> Result<()> {
        let io: IO = match StandardIO::new("/tmp/test_concurrent") {
            Ok(io) => io.into(),
            Err(e) => return Err(e.into()),
        };
        let handle = Arc::new(FileHandle::new(1, io));
        let expected_offsets: Vec<u64> = (0..10).map(|i| i * 100).collect();
        let actual_offsets = Arc::new(Mutex::new(Vec::new()));

        let threads: Vec<_> = (0..10)
            .map(|i| {
                let handle = handle.clone();
                let offsets = actual_offsets.clone();
                thread::spawn(move || -> Result<()> {
                    let mut buf = vec![0; 100];
                    handle.read(&mut buf, i * 100)?;
                    offsets.lock().push(handle.data.get_offset());
                    Ok(())
                })
            })
            .collect();

        for handle in threads {
            handle.join().expect("Thread panicked")?;
        }

        let final_offsets = actual_offsets.lock();
        for expected in expected_offsets {
            assert!(final_offsets.contains(&expected));
        }
        assert_eq!(final_offsets.len(), 10);

        Ok(())
    }
}
