use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Result};

#[derive(Debug)]
struct Entry {
    timestamp: u32,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Entry {
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        let key_sz = self.key.len() as u32;
        let val_sz = self.value.len() as u32;

        buf.write_u32::<LittleEndian>(0)?;
        buf.write_u32::<LittleEndian>(self.timestamp)?;
        buf.write_u32::<LittleEndian>(key_sz)?;
        buf.write_u32::<LittleEndian>(val_sz)?;
        buf.extend(&self.key);
        buf.extend(&self.value);

        let crc = crc32fast::hash(&buf[4..]);
        (&mut buf[0..4]).write_u32::<LittleEndian>(crc)?;

        Ok(buf)
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        let mut cursor = Cursor::new(bytes);

        let crc = cursor.read_u32::<LittleEndian>().ok()?;
        let timestamp = cursor.read_u32::<LittleEndian>().ok()?;
        let key_sz = cursor.read_u32::<LittleEndian>().ok()? as usize;
        let val_sz = cursor.read_u32::<LittleEndian>().ok()? as usize;

        let mut key = vec![0u8; key_sz];
        cursor.read_exact(&mut key).ok()?;

        let mut value = vec![0u8; val_sz];
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
