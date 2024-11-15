use std::io::ErrorKind;

use bytes::{Buf, BufMut, BytesMut};
use prost::{decode_length_delimiter, encode_length_delimiter, length_delimiter_len};

use crate::Error;
use crate::Result;

#[derive(Debug)]
pub struct DataEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    state: State,
}

#[derive(Debug, Clone)]
pub enum State {
    Active,
    Inactive,
}

impl From<u8> for State {
    fn from(v: u8) -> Self {
        match v {
            0 => State::Active,
            1 => State::Inactive,
            //TODO: should panic?
            _ => panic!("Invalid state value"),
        }
    }
}
#[allow(dead_code)]
impl DataEntry {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>, state: State) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            state,
        }
    }
    pub fn set_key(&mut self, key: impl Into<Vec<u8>>) {
        self.key = key.into();
    }

    pub fn get_key(&self) -> &Vec<u8> {
        &self.key
    }

    pub fn set_value(&mut self, value: impl Into<Vec<u8>>) {
        self.value = value.into();
    }

    pub fn get_value(&self) -> &Vec<u8> {
        &self.value
    }

    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }

    pub fn get_state(&self) -> State {
        self.state.clone()
    }

    pub fn get_crc(&self) -> Result<u32> {
        let (_, crc) = self.encode_and_get_crc()?;
        Ok(crc)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        let (data_entry, _) = self.encode_and_get_crc()?;
        Ok(data_entry)
    }

    pub fn encode_and_get_crc(&self) -> Result<(Vec<u8>, u32)> {
        let key_size = self.key.len();
        let value_size = self.value.len();
        // If key_size and value_size are both 0, it means invalid data
        if key_size == 0 && value_size == 0 {
            return Err(Error::Io(ErrorKind::UnexpectedEof.into()));
        }
        let mut buf = BytesMut::new();
        buf.reserve(
            std::mem::size_of::<u8>()
                + length_delimiter_len(self.key.len())
                + length_delimiter_len(self.value.len())
                + self.key.len()
                + self.value.len()
                + 4,
        );

        buf.put_u8(self.state.clone() as u8);

        // Store key size and value size
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        // Store key and value data
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // Calculate crc
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);
        Ok((buf.into(), crc))
    }

    pub fn decode_header(mut header_buf: BytesMut) -> Result<(usize, usize, usize, u8)> {
        //FIXME: when call put function
        let state = header_buf.get_u8();

        // Get actual header size
        // Read key_size and value_size
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        // If key_size and value_size are both 0, it means the end of the file
        if key_size == 0 && value_size == 0 {
            return Err(Error::Io(ErrorKind::UnexpectedEof.into()));
        }

        // Get actual header size
        let actual_header_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;
        Ok((key_size, value_size, actual_header_size, state))
    }

    pub fn decode(
        mut body_buf: BytesMut,
        key_size: usize,
        value_size: usize,
        state: u8,
    ) -> Result<Self> {
        let data_entry = DataEntry::new(
            body_buf.get(..key_size).unwrap().to_vec(),
            body_buf.get(key_size..body_buf.len() - 4).unwrap().to_vec(),
            State::from(state),
        );

        body_buf.advance(key_size + value_size);
        // Verify CRC
        if body_buf.get_u32() != data_entry.get_crc()? {
            return Err(Error::Unsupported("CRC check failed".to_string()));
        }
        Ok(data_entry)
    }

    pub fn is_active(&self) -> bool {
        match self.state {
            State::Active => true,
            State::Inactive => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    #[test]
    fn test_decode() -> Result<()> {
        //Initialize data entry
        let key = "key".as_bytes();
        let value = "value".as_bytes();
        let state = State::Active;
        let data_entry = DataEntry::new(key, value, state);
        let mut encoded_entry = BytesMut::new();
        encoded_entry.extend(data_entry.encode()?);
        let mut header_buf = BytesMut::new();
        header_buf.extend(vec![0, 3, 5]);
        let (key_size, value_size, _, state) = DataEntry::decode_header(header_buf)?;
        let mut body_buf = BytesMut::new();
        body_buf.extend(vec![107, 101, 121, 118, 97, 108, 117, 101, 105, 80, 99, 47]);
        let decoded_entry = DataEntry::decode(body_buf, key_size, value_size, state)?;
        assert_eq!(decoded_entry.get_key(), data_entry.get_key());
        assert_eq!(decoded_entry.get_value(), data_entry.get_value());
        assert_eq!(
            decoded_entry.get_state() as u8,
            data_entry.get_state() as u8
        );
        Ok(())
    }
    #[test]
    fn test_encode() -> Result<()> {
        let key = "key".as_bytes();
        let value = "value".as_bytes();
        let state = State::Active;
        let data_entry = DataEntry::new(key, value, state);
        let mut encoded_entry = BytesMut::new();
        encoded_entry.extend(data_entry.encode()?);
        let buf = b"\0\x03\x05keyvalue";
        let mut hash = crc32fast::Hasher::new();
        hash.update(buf);
        let crc = hash.finalize();

        let mut entry = BytesMut::new();
        entry.extend(buf);
        entry.put_u32(crc);
        assert_eq!(encoded_entry, entry);
        Ok(())
    }
}
