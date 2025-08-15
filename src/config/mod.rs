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
    fn test_dump_config_with_auth() {
        let config = DumpConfig {
            host: "redis.example.com".to_string(),
            port: 6380,
            password: Some("secret123".to_string()),
            database: Some(5),
            filter: "user:*".to_string(),
            output_file: "user_data.resp".to_string(),
            batch_size: 500,
            workers: 5,
            scan_size: 2000,
            silent: true,
            output_format: OutputFormat::Commands,
        };

        assert_eq!(config.host, "redis.example.com");
        assert_eq!(config.port, 6380);
        assert_eq!(config.password, Some("secret123".to_string()));
        assert_eq!(config.database, Some(5));
        assert_eq!(config.filter, "user:*");
        assert_eq!(config.output_file, "user_data.resp");
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.workers, 5);
        assert_eq!(config.scan_size, 2000);
        assert!(config.silent);
        assert_eq!(config.output_format, OutputFormat::Commands);
    }

    #[test]
    fn test_output_format_equality() {
        assert_eq!(OutputFormat::Resp, OutputFormat::Resp);
        assert_eq!(OutputFormat::Commands, OutputFormat::Commands);
        assert_ne!(OutputFormat::Resp, OutputFormat::Commands);
        assert_ne!(OutputFormat::Commands, OutputFormat::Resp);
    }

    #[test]
    fn test_output_format_debug() {
        let resp_format = OutputFormat::Resp;
        let commands_format = OutputFormat::Commands;

        assert_eq!(format!("{:?}", resp_format), "Resp");
        assert_eq!(format!("{:?}", commands_format), "Commands");
    }

    #[test]
    fn test_config_clone() {
        let original = DumpConfig {
            host: "localhost".to_string(),
            port: 6379,
            password: Some("password".to_string()),
            database: Some(1),
            filter: "test:*".to_string(),
            output_file: "test.resp".to_string(),
            batch_size: 100,
            workers: 2,
            scan_size: 50,
            silent: false,
            output_format: OutputFormat::Resp,
        };

        let cloned = original.clone();

        assert_eq!(original.host, cloned.host);
        assert_eq!(original.port, cloned.port);
        assert_eq!(original.password, cloned.password);
        assert_eq!(original.database, cloned.database);
        assert_eq!(original.filter, cloned.filter);
        assert_eq!(original.output_file, cloned.output_file);
        assert_eq!(original.batch_size, cloned.batch_size);
        assert_eq!(original.workers, cloned.workers);
        assert_eq!(original.scan_size, cloned.scan_size);
        assert_eq!(original.silent, cloned.silent);
        assert_eq!(original.output_format, cloned.output_format);
    }

    #[test]
    fn test_config_validation_edge_cases() {
        // Test minimum values
        let config = DumpConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            password: None,
            database: Some(0),
            filter: "".to_string(), // Empty filter
            output_file: "min.resp".to_string(),
            batch_size: 1,
            workers: 1,
            scan_size: 1,
            silent: false,
            output_format: OutputFormat::Resp,
        };

        assert_eq!(config.port, 1);
        assert_eq!(config.database, Some(0));
        assert_eq!(config.filter, "");
        assert_eq!(config.batch_size, 1);
        assert_eq!(config.workers, 1);
        assert_eq!(config.scan_size, 1);
    }

    #[test]
    fn test_config_max_values() {
        // Test maximum reasonable values
        let config = DumpConfig {
            host: "very.long.redis.hostname.example.com".to_string(),
            port: 65535,
            password: Some("very_long_password_with_special_chars_!@#$%^&*()".to_string()),
            database: Some(15), // Redis default max DB is 15
            filter: "*:*:*:*:*".to_string(),
            output_file: "very_long_output_filename_with_path.resp".to_string(),
            batch_size: 10000,
            workers: 100,
            scan_size: 10000,
            silent: true,
            output_format: OutputFormat::Commands,
        };

        assert_eq!(config.port, 65535);
        assert_eq!(config.database, Some(15));
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.workers, 100);
        assert_eq!(config.scan_size, 10000);
    }

    #[test]
    fn test_output_format_patterns() {
        // Test various filter patterns
        let patterns = vec![
            "*",
            "user:*",
            "*:session",
            "cache:user:*",
            "prefix:*:suffix",
            "exact_key",
            "*key*",
            "???",
            "key[0-9]",
            "*{tag}*",
        ];

        for pattern in patterns {
            let config = DumpConfig {
                host: "localhost".to_string(),
                port: 6379,
                password: None,
                database: None,
                filter: pattern.to_string(),
                output_file: "test.resp".to_string(),
                batch_size: 1000,
                workers: 10,
                scan_size: 1000,
                silent: false,
                output_format: OutputFormat::Resp,
            };
            assert_eq!(config.filter, pattern);
        }
    }
}
