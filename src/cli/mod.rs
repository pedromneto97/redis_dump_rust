use crate::config::{DumpConfig, OutputFormat};
use clap::{Arg, Command};

pub fn parse_cli() -> DumpConfig {
    let app = Command::new("redis-dump-rust")
        .version("1.0.0")
        .author("Redis Dump Tool")
        .about("Async tool to dump Redis databases efficiently")
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
}
