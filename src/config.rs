#[derive(Debug)]
pub struct Config {
    pub dir: String,
    pub writer: bool,
    pub max_active_file_size: u64,
    pub maximum_files_before_merge: usize,
}
