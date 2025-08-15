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

#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Commands,
    Resp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dump_config_defaults() {
        let config = DumpConfig {
            host: "localhost".to_string(),
            port: 6379,
            password: None,
            database: None,
            filter: "*".to_string(),
            output_file: "out.resp".to_string(),
            batch_size: 1000,
            workers: 10,
            scan_size: 1000,
            silent: false,
            output_format: OutputFormat::Resp,
        };
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6379);
        assert_eq!(config.filter, "*");
        assert_eq!(config.output_format, OutputFormat::Resp);
    }

    #[test]
    fn test_output_format_enum() {
        let fmt = OutputFormat::Commands;
        match fmt {
            OutputFormat::Commands => assert!(true),
            OutputFormat::Resp => assert!(false, "Should be Commands"),
        }
    }
}
