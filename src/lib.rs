use std::sync::RwLock;
use std::{collections::HashMap, fs::File};

struct Record {
    timestamp: u32,
    key: Vec<u8>,
    value: Vec<u8>,
}

struct Hint {
    timestamp: u32,
    key: Vec<u8>,
    val_sz: u32,
    offset: u32,
}

struct Entry {
    file_id: u32,
    val_sz: u32,
    timestamp: u32,
    offset: u32,
}

struct State {
    memtable: HashMap<Vec<u8>, Entry>,
    active_file_size: u64,
    active_file: File,
    active_file_id: u64,
}

pub struct Config {
    pub dir: String,
    pub writer: bool,
    pub max_active_file_size: u64,
    pub maximum_files_before_merge: usize,
}

pub struct Bitcask {
    state: RwLock<State>,
    config: Config,
}

impl Bitcask {
    pub fn open() {
        todo!()
    }

    pub fn get() {
        todo!()
    }

    pub fn put() {
        todo!()
    }

    pub fn delete() {
        todo!()
    }

    pub fn list_keys() {
        todo!()
    }

    pub fn fold() {
        todo!()
    }

    pub fn merge() {
        todo!()
    }

    pub fn sync() {
        todo!()
    }

    pub fn close() {
        todo!()
    }
}
