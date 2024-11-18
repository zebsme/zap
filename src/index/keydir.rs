use bytes::BytesMut;
use prost::encoding::encode_varint;
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

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_varint(self.file_id as u64, &mut buf);
        encode_varint(self.offset, &mut buf);
        encode_varint(self.size as u64, &mut buf);
        buf.to_vec()
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
