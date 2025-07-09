use std::collections::HashMap;

#[derive(Debug)]
struct Record {
    file_id: u32,
    val_sz: u32,
    timestamp: u32,
    offset: u64,
}

type KeyDir = HashMap<Vec<u8>, Record>;
