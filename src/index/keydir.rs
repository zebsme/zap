#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyDirEntry {
    file_id: u32,
    offset: u64,
    size: u32,
}

impl KeyDirEntry {
    pub fn new(file_id: u32, offset: u64, size: u32) -> Self {
        Self {
            file_id,
            offset,
            size,
        }
    }

    pub fn get_file_id(&self) -> u32 {
        self.file_id
    }

    pub fn get_offset(&self) -> u64 {
        self.offset
    }

    pub fn get_size(&self) -> u32 {
        self.size
    }
}
