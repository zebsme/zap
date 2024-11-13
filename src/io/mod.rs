mod standard;
use enum_dispatch::enum_dispatch;
pub use standard::StandardIO;

use crate::result::Result;

#[derive(Debug, Clone)]
#[enum_dispatch]
pub enum IO {
    Standard(StandardIO),
}

#[enum_dispatch(IO)]
pub trait IOHandler: Send + Sync {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;
    fn get_file_id(&self) -> u32;
}
