use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Result};

#[derive(Debug)]
struct Entry {
    timestamp: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Entry {
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        let key_len = self.key.len() as u16;
        let val_len = self.value.len() as u32;

        buf.write_u32::<LittleEndian>(0)?;
        buf.write_u64::<LittleEndian>(self.timestamp)?;
        buf.write_u16::<LittleEndian>(key_len)?;
        buf.write_u32::<LittleEndian>(val_len)?;
        buf.extend(&self.key);
        buf.extend(&self.value);

        let crc = crc32fast::hash(&buf[4..]);
        (&mut buf[0..4]).write_u32::<LittleEndian>(crc)?;

        Ok(buf)
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        let mut cursor = Cursor::new(bytes);

        let crc = cursor.read_u32::<LittleEndian>().ok()?;
        let timestamp = cursor.read_u64::<LittleEndian>().ok()?;
        let key_len = cursor.read_u16::<LittleEndian>().ok()? as usize;
        let val_len = cursor.read_u32::<LittleEndian>().ok()? as usize;

        let mut key = vec![0u8; key_len];
        cursor.read_exact(&mut key).ok()?;
        let mut value = vec![0u8; val_len];
        cursor.read_exact(&mut value).ok()?;

        let actual_crc = crc32fast::hash(&bytes[4..]);
        if actual_crc != crc {
            return None;
        }

        Some(Self {
            timestamp,
            key,
            value,
        })
    }
}
