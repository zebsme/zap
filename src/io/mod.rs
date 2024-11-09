mod standard;
use enum_dispatch::enum_dispatch;
pub use standard::StandardIO;

use crate::result::Result;

#[enum_dispatch]
pub enum IO {
    Standard(StandardIO),
}

#[enum_dispatch(IO)]
pub trait IOHandler {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;
}
