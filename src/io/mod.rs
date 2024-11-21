mod mmap;
mod standard;
use crate::result::Result;
use enum_dispatch::enum_dispatch;
pub use mmap::MmapIO;
pub use standard::StandardIO;

#[derive(Debug, Clone)]
#[enum_dispatch]
pub enum IO {
    Standard(StandardIO),
    Mmap(MmapIO),
}

#[enum_dispatch(IO)]
pub trait IOHandler: Send + Sync {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;
    fn get_file_id(&self) -> u32;
}
