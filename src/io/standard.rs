use super::IOHandler;
use crate::{Error, Result};
use parking_lot::RwLock;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::{fd::AsRawFd, unix::fs::FileExt},
    path::Path,
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct StandardIO {
    fd: Arc<RwLock<File>>,
}

#[allow(dead_code)]
impl StandardIO {
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;
        Ok(StandardIO {
            fd: Arc::new(RwLock::new(file)),
        })
    }
}

impl IOHandler for StandardIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        read_guard.read_at(buf, offset).map_err(Error::from)
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        write_guard.write(buf).map_err(Error::from)
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read();
        read_guard.sync_all().map_err(Error::from)
    }
    fn get_file_id(&self) -> u32 {
        let read_guard = self.fd.read();
        read_guard.as_raw_fd() as u32
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn check_and_delete(path: &Path) {
        if path.exists() {
            fs::remove_file(path).unwrap()
        }
    }

    #[test]
    fn test_standard_io_write() {
        let path = Path::new("/tmp/test_standard_io_write");
        check_and_delete(path);
        let res = StandardIO::new(path);
        assert!(res.is_ok());
        let mut io = res.ok().unwrap();

        let apple = io.write("apple".as_bytes());
        assert!(apple.is_ok());
        assert_eq!(5, apple.ok().unwrap());

        let banana = io.write("banana".as_bytes());
        assert!(banana.is_ok());
        assert_eq!(6, banana.ok().unwrap());
    }

    #[test]
    fn test_standard_io_read() {
        let path = Path::new("/tmp/test_standard_io_read");
        check_and_delete(path);
        let res = StandardIO::new(path);
        assert!(res.is_ok());
        let mut io = res.ok().unwrap();

        let apple = io.write("apple".as_bytes());
        assert!(apple.is_ok());
        assert_eq!(5, apple.ok().unwrap());

        let banana = io.write("banana".as_bytes());
        assert!(banana.is_ok());
        assert_eq!(6, banana.ok().unwrap());

        let mut apple_buf = [0u8; 5];
        let read_apple = io.read(&mut apple_buf, 0);
        assert!(read_apple.is_ok());
        assert_eq!(5, read_apple.ok().unwrap());
        assert_eq!(apple_buf.as_slice(), b"apple");
        let mut banana_buf = [0u8; 6];
        let read_banana = io.read(&mut banana_buf, 5);
        assert!(read_banana.is_ok());
        assert_eq!(banana_buf.as_slice(), b"banana");
        assert_eq!(6, read_banana.ok().unwrap());
    }

    #[test]
    fn test_file_io_sync() {
        let path = Path::new("/tmp/test_file_io_sync");
        check_and_delete(path);
        let res = StandardIO::new(path);
        assert!(res.is_ok());
        let mut io = res.ok().unwrap();

        let apple = io.write("apple".as_bytes());
        assert!(apple.is_ok());
        assert_eq!(5, apple.ok().unwrap());

        let banana = io.write("banana".as_bytes());
        assert!(banana.is_ok());
        assert_eq!(6, banana.ok().unwrap());

        let sync_res = io.sync();
        assert!(sync_res.is_ok());
    }
}
