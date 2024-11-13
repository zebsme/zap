use crate::index::{HashMap, IndexMode};

#[allow(dead_code)]
pub struct Opts {
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub read_only: bool,
    pub sync_writes: bool,
    pub dir_path: String,
    pub file_prefix: String,
    pub data_file_size: u64,
}

#[allow(dead_code)]
pub struct Context {
    pub index: IndexMode,
    pub opts: Opts,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            max_key_size: 256,
            max_value_size: 1024,
            read_only: false,
            sync_writes: true,
            dir_path: String::from("/tmp"),
            file_prefix: String::from("default"),
            data_file_size: 1024 * 1024 * 1024,
        }
    }
}

impl Opts {
    pub fn new(
        max_key_size: usize,
        max_value_size: usize,
        read_only: bool,
        sync_writes: bool,
        dir_path: String,
        file_prefix: String,
        data_file_size: u64,
    ) -> Self {
        Self {
            max_key_size,
            max_value_size,
            read_only,
            sync_writes,
            dir_path,
            file_prefix,
            data_file_size,
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Context {
            index: HashMap::new().into(),
            opts: Opts::default(),
        }
    }
}

#[allow(dead_code)]
impl Context {
    pub fn new(opts: Opts) -> Self {
        Self {
            index: HashMap::new().into(),
            opts,
        }
    }
}
