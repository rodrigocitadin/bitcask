use std::fs::OpenOptions;
use std::path::Path;
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
    active_file_size: u32,
    active_file_id: u32,
    active_file: File,
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
    pub fn open(config: Config) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&config.dir)?;

        let files: Vec<String> = std::fs::read_dir(&config.dir)
            .unwrap()
            .map(|f| f.unwrap().path().display().to_string())
            .filter(|f| f.contains("data"))
            .collect();

        let active_file_id = files
            .iter()
            .map(|f| f.split('.').last().unwrap().parse::<u32>().unwrap())
            .max()
            .unwrap_or(0)
            + 1;

        let active_file_path = Path::new(&config.dir).join(format!("data.{}", active_file_id));

        let active_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(config.writer)
            .open(active_file_path)?;

        let state = RwLock::new(State {
            active_file_id,
            active_file,
            active_file_size: 0,
            memtable: HashMap::new(),
        });

        Self { state, config }
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
