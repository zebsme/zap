use crate::index::{HashMap, IndexMode};

#[allow(dead_code)]
pub struct Opts {
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub read_only: bool,
    pub sync_writes: bool,
    pub dir_path: String,
    pub data_file_size: u64,
}

#[allow(dead_code)]
pub struct Context {
    pub index: IndexMode,
    pub opts: Opts,
    pub current_file_id: u32,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            max_key_size: 256,
            max_value_size: 1024,
            read_only: false,
            sync_writes: true,
            dir_path: String::from("/tmp"),
            data_file_size: 1024 * 1024 * 1024,
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Context {
            index: HashMap::new().into(),
            opts: Opts::default(),
            current_file_id: 1,
        }
    }
}

#[allow(dead_code)]
impl Context {
    pub fn new(file_id: u32) -> Self {
        Self {
            index: HashMap::new().into(),
            opts: Opts::default(),
            current_file_id: file_id,
        }
    }
}
