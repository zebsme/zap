use crate::index::IndexMode;

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
