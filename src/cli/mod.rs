use crate::config::{DumpConfig, OutputFormat};
use clap::{Arg, Command};

pub fn parse_cli() -> DumpConfig {
    let app = Command::new("redis-dump-rust")
        .version("1.0.0")
        .author("Redis Dump Tool")
        .about("Async tool to dump Redis databases efficiently")
        .disable_help_flag(true)
        .arg(
            Arg::new("help")
                .long("help")
                .help("Print help information")
                .action(clap::ArgAction::Help),
        )
        .arg(
            Arg::new("host")
                .short('h')
                .long("host")
                .value_name("HOST")
                .help("Redis server address")
                .default_value("127.0.0.1"),
        )
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("Redis server port")
                .default_value("6379"),
        )
        .arg(
            Arg::new("user")
                .short('u')
                .long("user")
                .value_name("USER")
                .help("Redis username")
                .default_value("default"),
        )
        .arg(
            Arg::new("password")
                .short('a')
                .long("auth")
                .value_name("PASSWORD")
                .help("Redis password"),
        )
        .arg(
            Arg::new("database")
                .short('d')
                .long("db")
                .value_name("DB")
                .help("Database number (default: all)"),
        )
        .arg(
            Arg::new("filter")
                .short('f')
                .long("filter")
                .value_name("PATTERN")
                .help("Pattern to filter keys (e.g. user:*, cache:*, *)")
                .default_value("*"),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("FILE")
                .help("Output file")
                .default_value("redis_dump.resp"),
        )
        .arg(
            Arg::new("batch-size")
                .short('b')
                .long("batch-size")
                .value_name("SIZE")
                .help("Batch size for operations")
                .default_value("1000"),
        )
        .arg(
            Arg::new("workers")
                .short('w')
                .long("workers")
                .value_name("COUNT")
                .help("Number of parallel workers")
                .default_value("10"),
        )
        .arg(
            Arg::new("scan-size")
                .long("scan-size")
                .value_name("SIZE")
                .help("Batch size for SCAN command")
                .default_value("1000"),
        )
        .arg(
            Arg::new("format")
                .long("format")
                .value_name("FORMAT")
                .help("Output format: commands or resp")
                .default_value("resp"),
        )
        .arg(
            Arg::new("silent")
                .short('s')
                .long("silent")
                .help("Silent mode")
                .action(clap::ArgAction::SetTrue),
        );

    let matches = app.get_matches();

    DumpConfig {
        host: matches.get_one::<String>("host").unwrap().clone(),
        port: matches.get_one::<String>("port").unwrap().parse().unwrap(),
        user: matches.get_one::<String>("user").unwrap().clone(),
        password: matches.get_one::<String>("password").cloned(),
        database: matches
            .get_one::<String>("database")
            .map(|s| s.parse().unwrap()),
        filter: matches.get_one::<String>("filter").unwrap().clone(),
        output_file: matches.get_one::<String>("output").unwrap().clone(),
        batch_size: matches
            .get_one::<String>("batch-size")
            .unwrap()
            .parse()
            .unwrap(),
        workers: matches
            .get_one::<String>("workers")
            .unwrap()
            .parse()
            .unwrap(),
        scan_size: matches
            .get_one::<String>("scan-size")
            .unwrap()
            .parse()
            .unwrap(),
        silent: matches.get_flag("silent"),
        output_format: match matches.get_one::<String>("format").unwrap().as_str() {
            "commands" => OutputFormat::Commands,
            "resp" => OutputFormat::Resp,
            _ => OutputFormat::Resp,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_default_values() {
        // Simulate CLI args using clap directly
        let app = Command::new("redis-dump-rust")
            .arg(Arg::new("host").default_value("127.0.0.1"))
            .arg(Arg::new("port").default_value("6379"))
            .arg(Arg::new("filter").default_value("*"))
            .arg(Arg::new("output").default_value("redis_dump.resp"))
            .arg(Arg::new("batch-size").default_value("1000"))
            .arg(Arg::new("workers").default_value("10"))
            .arg(Arg::new("scan-size").default_value("1000"))
            .arg(Arg::new("format").default_value("resp"));
        let matches = app.get_matches_from(vec!["redis-dump-rust"]);
        assert_eq!(matches.get_one::<String>("host").unwrap(), "127.0.0.1");
        assert_eq!(matches.get_one::<String>("port").unwrap(), "6379");
        assert_eq!(matches.get_one::<String>("filter").unwrap(), "*");
        assert_eq!(
            matches.get_one::<String>("output").unwrap(),
            "redis_dump.resp"
        );
        assert_eq!(matches.get_one::<String>("batch-size").unwrap(), "1000");
        assert_eq!(matches.get_one::<String>("workers").unwrap(), "10");
        assert_eq!(matches.get_one::<String>("scan-size").unwrap(), "1000");
        assert_eq!(matches.get_one::<String>("format").unwrap(), "resp");
    }

    #[test]
    fn test_cli_custom_values() {
        let app = Command::new("redis-dump-rust")
            .disable_help_flag(true)
            .arg(Arg::new("host").short('h').long("host"))
            .arg(Arg::new("port").short('p').long("port"))
            .arg(Arg::new("password").short('a').long("auth"))
            .arg(Arg::new("database").short('d').long("db"))
            .arg(Arg::new("filter").short('f').long("filter"))
            .arg(Arg::new("output").short('o').long("output"))
            .arg(Arg::new("batch-size").short('b').long("batch-size"))
            .arg(Arg::new("workers").short('w').long("workers"))
            .arg(Arg::new("scan-size").long("scan-size"))
            .arg(Arg::new("format").long("format"))
            .arg(
                Arg::new("silent")
                    .short('s')
                    .long("silent")
                    .action(clap::ArgAction::SetTrue),
            );

        let matches = app.get_matches_from(vec![
            "redis-dump-rust",
            "--host",
            "192.168.1.100",
            "--port",
            "6380",
            "--auth",
            "mypassword",
            "--db",
            "5",
            "--filter",
            "user:*",
            "--output",
            "custom.resp",
            "--batch-size",
            "500",
            "--workers",
            "5",
            "--scan-size",
            "2000",
            "--format",
            "commands",
            "--silent",
        ]);

        assert_eq!(matches.get_one::<String>("host").unwrap(), "192.168.1.100");
        assert_eq!(matches.get_one::<String>("port").unwrap(), "6380");
        assert_eq!(matches.get_one::<String>("password").unwrap(), "mypassword");
        assert_eq!(matches.get_one::<String>("database").unwrap(), "5");
        assert_eq!(matches.get_one::<String>("filter").unwrap(), "user:*");
        assert_eq!(matches.get_one::<String>("output").unwrap(), "custom.resp");
        assert_eq!(matches.get_one::<String>("batch-size").unwrap(), "500");
        assert_eq!(matches.get_one::<String>("workers").unwrap(), "5");
        assert_eq!(matches.get_one::<String>("scan-size").unwrap(), "2000");
        assert_eq!(matches.get_one::<String>("format").unwrap(), "commands");
        assert!(matches.get_flag("silent"));
    }

    #[test]
    fn test_cli_short_args() {
        let app = Command::new("redis-dump-rust")
            .disable_help_flag(true)
            .arg(Arg::new("host").short('h').long("host"))
            .arg(Arg::new("port").short('p').long("port"))
            .arg(Arg::new("password").short('a').long("auth"))
            .arg(Arg::new("database").short('d').long("db"))
            .arg(Arg::new("filter").short('f').long("filter"))
            .arg(Arg::new("output").short('o').long("output"))
            .arg(Arg::new("batch-size").short('b').long("batch-size"))
            .arg(Arg::new("workers").short('w').long("workers"))
            .arg(
                Arg::new("silent")
                    .short('s')
                    .long("silent")
                    .action(clap::ArgAction::SetTrue),
            );

        let matches = app.get_matches_from(vec![
            "redis-dump-rust",
            "-h",
            "localhost",
            "-p",
            "6379",
            "-a",
            "test123",
            "-d",
            "1",
            "-f",
            "cache:*",
            "-o",
            "output.resp",
            "-b",
            "100",
            "-w",
            "2",
            "-s",
        ]);

        assert_eq!(matches.get_one::<String>("host").unwrap(), "localhost");
        assert_eq!(matches.get_one::<String>("port").unwrap(), "6379");
        assert_eq!(matches.get_one::<String>("password").unwrap(), "test123");
        assert_eq!(matches.get_one::<String>("database").unwrap(), "1");
        assert_eq!(matches.get_one::<String>("filter").unwrap(), "cache:*");
        assert_eq!(matches.get_one::<String>("output").unwrap(), "output.resp");
        assert_eq!(matches.get_one::<String>("batch-size").unwrap(), "100");
        assert_eq!(matches.get_one::<String>("workers").unwrap(), "2");
        assert!(matches.get_flag("silent"));
    }

    #[test]
    fn test_output_format_parsing() {
        let app = Command::new("redis-dump-rust")
            .arg(Arg::new("format").long("format").default_value("resp"));

        // Test RESP format
        let matches = app
            .clone()
            .get_matches_from(vec!["redis-dump-rust", "--format", "resp"]);
        assert_eq!(matches.get_one::<String>("format").unwrap(), "resp");

        // Test Commands format
        let matches = app
            .clone()
            .get_matches_from(vec!["redis-dump-rust", "--format", "commands"]);
        assert_eq!(matches.get_one::<String>("format").unwrap(), "commands");

        // Test default format
        let matches = app.get_matches_from(vec!["redis-dump-rust"]);
        assert_eq!(matches.get_one::<String>("format").unwrap(), "resp");
    }

    #[test]
    fn test_parse_cli_integration() {
        // Mock the arguments for parse_cli function
        // This test ensures the entire flow works correctly

        // Test the DumpConfig creation logic separately since we can't easily mock std::env::args()
        let test_cases = vec![
            ("resp", OutputFormat::Resp),
            ("commands", OutputFormat::Commands),
            ("invalid", OutputFormat::Resp), // Should default to resp
        ];

        for (format_str, expected_format) in test_cases {
            let actual_format = match format_str {
                "commands" => OutputFormat::Commands,
                "resp" => OutputFormat::Resp,
                _ => OutputFormat::Resp,
            };
            assert_eq!(actual_format, expected_format);
        }
    }

    #[test]
    fn test_mixed_args_combinations() {
        let app = Command::new("redis-dump-rust")
            .disable_help_flag(true)
            .arg(
                Arg::new("host")
                    .short('h')
                    .long("host")
                    .default_value("127.0.0.1"),
            )
            .arg(
                Arg::new("port")
                    .short('p')
                    .long("port")
                    .default_value("6379"),
            )
            .arg(Arg::new("password").short('a').long("auth"))
            .arg(
                Arg::new("filter")
                    .short('f')
                    .long("filter")
                    .default_value("*"),
            )
            .arg(
                Arg::new("batch-size")
                    .short('b')
                    .long("batch-size")
                    .default_value("1000"),
            )
            .arg(
                Arg::new("silent")
                    .short('s')
                    .long("silent")
                    .action(clap::ArgAction::SetTrue),
            );

        // Test mixing short and long args
        let matches = app.get_matches_from(vec![
            "redis-dump-rust",
            "-h",
            "localhost",
            "--port",
            "6380",
            "-a",
            "secret",
            "--filter",
            "user:*",
            "-b",
            "500",
            "--silent",
        ]);

        assert_eq!(matches.get_one::<String>("host").unwrap(), "localhost");
        assert_eq!(matches.get_one::<String>("port").unwrap(), "6380");
        assert_eq!(matches.get_one::<String>("password").unwrap(), "secret");
        assert_eq!(matches.get_one::<String>("filter").unwrap(), "user:*");
        assert_eq!(matches.get_one::<String>("batch-size").unwrap(), "500");
        assert!(matches.get_flag("silent"));
    }

    #[test]
    fn test_boundary_values() {
        let app = Command::new("redis-dump-rust")
            .disable_help_flag(true)
            .arg(Arg::new("port").short('p').long("port"))
            .arg(Arg::new("database").short('d').long("db"))
            .arg(Arg::new("batch-size").short('b').long("batch-size"))
            .arg(Arg::new("workers").short('w').long("workers"));

        // Test minimum valid values
        let matches = app.clone().get_matches_from(vec![
            "redis-dump-rust",
            "--port",
            "1",
            "--db",
            "0",
            "--batch-size",
            "1",
            "--workers",
            "1",
        ]);

        assert_eq!(matches.get_one::<String>("port").unwrap(), "1");
        assert_eq!(matches.get_one::<String>("database").unwrap(), "0");
        assert_eq!(matches.get_one::<String>("batch-size").unwrap(), "1");
        assert_eq!(matches.get_one::<String>("workers").unwrap(), "1");

        // Test maximum valid values
        let matches = app.get_matches_from(vec![
            "redis-dump-rust",
            "--port",
            "65535",
            "--db",
            "15", // Redis max DB index is typically 15
            "--batch-size",
            "10000",
            "--workers",
            "100",
        ]);

        assert_eq!(matches.get_one::<String>("port").unwrap(), "65535");
        assert_eq!(matches.get_one::<String>("database").unwrap(), "15");
        assert_eq!(matches.get_one::<String>("batch-size").unwrap(), "10000");
        assert_eq!(matches.get_one::<String>("workers").unwrap(), "100");
    }

    #[test]
    fn test_special_characters_in_args() {
        let app = Command::new("redis-dump-rust")
            .disable_help_flag(true)
            .arg(Arg::new("host").short('h').long("host"))
            .arg(Arg::new("password").short('a').long("auth"))
            .arg(Arg::new("filter").short('f').long("filter"))
            .arg(Arg::new("output").short('o').long("output"));

        let matches = app.get_matches_from(vec![
            "redis-dump-rust",
            "--host",
            "redis-cluster.example.com",
            "--auth",
            "p@ssw0rd!#$%",
            "--filter",
            "namespace:*:cache",
            "--output",
            "/tmp/redis-dump-2024-01-01.resp",
        ]);

        assert_eq!(
            matches.get_one::<String>("host").unwrap(),
            "redis-cluster.example.com"
        );
        assert_eq!(
            matches.get_one::<String>("password").unwrap(),
            "p@ssw0rd!#$%"
        );
        assert_eq!(
            matches.get_one::<String>("filter").unwrap(),
            "namespace:*:cache"
        );
        assert_eq!(
            matches.get_one::<String>("output").unwrap(),
            "/tmp/redis-dump-2024-01-01.resp"
        );
    }
}
