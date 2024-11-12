use bytes::{Buf, BufMut, BytesMut};
use prost::length_delimiter_len;

use crate::{
    io::{IOHandler, IO},
    Result,
};
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};

use super::DataEntry;

#[derive(Debug)]
pub struct FileHandle {
    data: Arc<Datafile>,
    io: IO,
}

#[derive(Debug)]
struct Datafile {
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

    pub fn sync(&self) -> Result<()> {
        match &self.io {
            IO::Standard(io) => io.sync(),
        }
    }

    pub fn get_offset(&self) -> u64 {
        self.data.get_offset()
    }

    pub fn write_data_entry() -> Result<()> {
        Ok(())
    }
    pub fn extract_data_entry(&self, offset: u64) -> Result<DataEntry> {
        let header_buf = BytesMut::with_capacity(
            std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2,
        );
        let (key_size, value_size, actual_header_size, state) =
            DataEntry::decode_header(header_buf)?;

        // Read key and value，last 4 bytes crc
        let mut body_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.read(&mut body_buf, offset + actual_header_size as u64)?;

        body_buf.advance(key_size + value_size);
        let data_entry = DataEntry::decode(body_buf, key_size, value_size, state)?;

        Ok(data_entry)
    }

    fn encode_data_entry(&self, data_entry: DataEntry) -> Result<BytesMut> {
        let mut buf = BytesMut::with_capacity(
            std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2,
        );

        buf.put_u8(data_entry.get_state() as u8);
        buf.put_u32(data_entry.get_key().len() as u32);
        buf.put_u32(data_entry.get_value().len() as u32);

        buf.put(data_entry.get_key().as_slice());
        buf.put(data_entry.get_value().as_ref());
        buf.put_u32(data_entry.get_crc()?);

        Ok(buf)
    }
}

// Manual Clone implementation for FileHandle
impl Clone for FileHandle {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            io: self.io.clone(),
        }
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
            Err(e) => return Err(e),
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

    // //TODO: Add implementation
    // #[test]
    // fn test_extract_data_entry() -> Result<()> {
    //     let io: IO = StandardIO::new("/tmp/test_extract_data_entry")?.into();
    //     let mut handle = FileHandle::new(1, io);
    //     let mut buf = vec![0; 10];
    //     let res = handle.read(&mut buf, 100);

    //     //Initialize data entry
    //     let data_entry = DataEntry::new(
    //         "key".as_bytes().to_vec(),
    //         "value".as_bytes().to_vec(),
    //         State::Active,
    //     );
    //     handle.write(&data_entry.encode()?)?;
    //     assert!(res.is_ok());
    //     assert_eq!(handle.data.get_offset(), 100);
    //     handle.extract_data_entry(0)?;
    //     // assert data entry

    //     Ok(())
    // }
}
