#[derive(Debug, Clone)]
pub struct DumpConfig {
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
    pub database: Option<u8>,
    pub filter: String,
    pub output_file: String,
    pub batch_size: usize,
    pub workers: usize,
    pub scan_size: usize,
    pub silent: bool,
    pub output_format: OutputFormat,
}

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Commands,
    Resp,
}
