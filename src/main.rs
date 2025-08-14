use anyhow::Result;
use clap::{Arg, Command};
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
struct DumpConfig {
    host: String,
    port: u16,
    password: Option<String>,
    database: Option<u8>,
    filter: String,
    output_file: String,
    batch_size: usize,
    workers: usize,
    scan_size: usize,
    silent: bool,
    output_format: OutputFormat,
}

#[derive(Debug, Clone)]
enum OutputFormat {
    Commands,
    Resp,
}

#[derive(Debug)]
struct DumpProgress {
    total_keys: u64,
    processed_keys: Arc<RwLock<u64>>,
    start_time: std::time::Instant,
    progress_bar: Arc<ProgressBar>,
}

impl DumpProgress {
    fn new(total_keys: u64, silent: bool) -> Self {
        let progress_bar = if silent {
            ProgressBar::hidden()
        } else {
            ProgressBar::new(total_keys)
        };

        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta}) | {msg}")
                .unwrap()
                .progress_chars("#>-"),
        );

        progress_bar.set_message("Starting dump...");

        Self {
            total_keys,
            processed_keys: Arc::new(RwLock::new(0)),
            start_time: std::time::Instant::now(),
            progress_bar: Arc::new(progress_bar),
        }
    }

    async fn increment(&self, batch_size: usize) {
        let mut processed = self.processed_keys.write().await;
        *processed += batch_size as u64;
        let current = *processed;
        drop(processed);

        self.progress_bar.set_position(current);

        let elapsed = self.start_time.elapsed();
        let keys_per_second = if elapsed.as_secs() > 0 {
            current as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let percentage = (current as f64 / self.total_keys as f64) * 100.0;

        self.progress_bar.set_message(format!(
            "{:.1}% | {:.0} keys/s | {} processed",
            percentage, keys_per_second, current
        ));
    }

    fn finish(&self, message: &str) {
        let elapsed = self.start_time.elapsed();
        let final_message = format!(
            "{} | Total time: {:.2}s | Avg rate: {:.0} keys/s",
            message,
            elapsed.as_secs_f64(),
            self.total_keys as f64 / elapsed.as_secs_f64()
        );
        self.progress_bar.finish_with_message(final_message);
    }

    fn update_stage(&self, stage: &str) {
        self.progress_bar.set_message(format!("Stage: {}", stage));
    }
}

fn format_resp_command(parts: &[&str]) -> String {
    let mut result = String::new();

    result.push_str(&format!("*{}\r\n", parts.len()));

    for part in parts {
        result.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }

    result
}

fn parse_redis_command(command: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current_part = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let mut chars = command.trim().chars().peekable();

    while let Some(ch) = chars.next() {
        if escape_next {
            current_part.push(ch);
            escape_next = false;
        } else if ch == '\\' {
            escape_next = true;
        } else if ch == '"' {
            in_quotes = !in_quotes;
        } else if ch.is_whitespace() && !in_quotes {
            if !current_part.is_empty() {
                parts.push(current_part.clone());
                current_part.clear();
            }
        } else {
            current_part.push(ch);
        }
    }

    if !current_part.is_empty() {
        parts.push(current_part);
    }

    parts
}

fn format_command_output(command: &str, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Commands => format!("{}\n", command),
        OutputFormat::Resp => {
            let parts = parse_redis_command(command);
            let part_refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            format_resp_command(&part_refs)
        }
    }
}

async fn connect_redis(config: &DumpConfig) -> Result<redis::aio::MultiplexedConnection> {
    let redis_url = if let Some(password) = &config.password {
        format!(
            "redis://{}:{}@{}:{}",
            password, password, config.host, config.port
        )
    } else {
        format!("redis://{}:{}", config.host, config.port)
    };

    let client = redis::Client::open(redis_url)?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    if let Some(db) = config.database {
        let _: () = redis::cmd("SELECT")
            .arg(db)
            .query_async(&mut connection)
            .await?;
    }

    Ok(connection)
}

async fn scan_keys(
    connection: &mut redis::aio::MultiplexedConnection,
    pattern: &str,
    scan_size: usize,
) -> Result<Vec<String>> {
    let mut cursor = 0;
    let mut all_keys = Vec::new();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(scan_size)
            .query_async(connection)
            .await?;

        all_keys.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    Ok(all_keys)
}

async fn generate_redis_commands_batch_optimized(
    connection: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<String>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut all_commands = Vec::with_capacity(keys.len() * 2);

    let mut type_ttl_pipe = redis::pipe();
    for key in keys {
        type_ttl_pipe.cmd("TYPE").arg(key).cmd("TTL").arg(key);
    }

    let type_ttl_results: Vec<(String, i64)> = type_ttl_pipe.query_async(connection).await?;

    let mut string_keys = Vec::new();
    let mut other_keys = Vec::new();
    let mut key_ttls = HashMap::with_capacity(keys.len());

    for (key, (key_type, ttl)) in keys.iter().zip(type_ttl_results.iter()) {
        if *ttl > 0 {
            key_ttls.insert(key.clone(), *ttl);
        }

        if key_type == "string" {
            string_keys.push(key);
        } else {
            other_keys.push((key, key_type));
        }
    }

    if !string_keys.is_empty() {
        let mut string_pipe = redis::pipe();
        for key in &string_keys {
            string_pipe.get(*key);
        }
        let string_values: Vec<String> = string_pipe.query_async(connection).await?;

        for (key, value) in string_keys.iter().zip(string_values.iter()) {
            if let Some(ttl) = key_ttls.get(*key) {
                all_commands.push(format!(
                    "SETEX \"{}\" {} \"{}\"",
                    key.replace("\"", "\\\""),
                    ttl,
                    value.replace("\"", "\\\"")
                ));
            } else {
                all_commands.push(format!(
                    "SET \"{}\" \"{}\"",
                    key.replace("\"", "\\\""),
                    value.replace("\"", "\\\"")
                ));
            }
        }
    }

    for (key, key_type) in other_keys {
        let mut commands = Vec::new();

        match key_type.as_str() {
            "list" => {
                let items: Vec<String> = redis::cmd("LRANGE")
                    .arg(key)
                    .arg(0)
                    .arg(-1)
                    .query_async(connection)
                    .await
                    .unwrap_or_default();
                if !items.is_empty() {
                    let quoted_items: Vec<String> = items
                        .iter()
                        .map(|item| format!("\"{}\"", item.replace("\"", "\\\"")))
                        .collect();
                    commands.push(format!("RPUSH \"{}\" {}", key, quoted_items.join(" ")));
                }
            }
            "set" => {
                let items: Vec<String> = redis::cmd("SMEMBERS")
                    .arg(key)
                    .query_async(connection)
                    .await
                    .unwrap_or_default();
                if !items.is_empty() {
                    let quoted_items: Vec<String> = items
                        .iter()
                        .map(|item| format!("\"{}\"", item.replace("\"", "\\\"")))
                        .collect();
                    commands.push(format!("SADD \"{}\" {}", key, quoted_items.join(" ")));
                }
            }
            "zset" => {
                let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
                    .arg(key)
                    .arg(0)
                    .arg(-1)
                    .arg("WITHSCORES")
                    .query_async(connection)
                    .await
                    .unwrap_or_default();
                if !items.is_empty() {
                    let score_member_pairs: Vec<String> = items
                        .iter()
                        .map(|(member, score)| {
                            format!("{} \"{}\"", score, member.replace("\"", "\\\""))
                        })
                        .collect();
                    commands.push(format!("ZADD \"{}\" {}", key, score_member_pairs.join(" ")));
                }
            }
            "hash" => {
                let hash_data: HashMap<String, String> =
                    connection.hgetall(key).await.unwrap_or_default();
                if !hash_data.is_empty() {
                    let field_value_pairs: Vec<String> = hash_data
                        .iter()
                        .map(|(field, value)| {
                            format!(
                                "\"{}\" \"{}\"",
                                field.replace("\"", "\\\""),
                                value.replace("\"", "\\\"")
                            )
                        })
                        .collect();
                    commands.push(format!("HMSET \"{}\" {}", key, field_value_pairs.join(" ")));
                }
            }
            _ => {}
        }

        all_commands.extend(commands);

        if let Some(ttl) = key_ttls.get(key) {
            match key_type.as_str() {
                "list" | "set" | "zset" | "hash" => {
                    all_commands.push(format!("EXPIRE \"{}\" {}", key, ttl));
                }
                _ => {}
            }
        }
    }

    Ok(all_commands)
}

async fn dump_keys(
    keys: Vec<String>,
    config: &DumpConfig,
    output_file: &str,
    group_name: &str,
) -> Result<()> {
    if keys.is_empty() {
        if !config.silent {
            println!("⚠️  No keys found for group: {}", group_name);
        }
        return Ok(());
    }

    if !config.silent {
        println!(
            "🏗️  Starting processing of {} keys for: {}",
            keys.len(),
            output_file
        );
    }

    let progress = Arc::new(DumpProgress::new(keys.len() as u64, config.silent));

    let file = File::create(output_file).await?;
    let writer = Arc::new(Mutex::new(BufWriter::new(file)));

    if !config.silent {
        println!("📝 Output file created: {}", output_file);
    }

    progress.update_stage("Distributing work among workers");

    let chunk_size = (keys.len() + config.workers - 1) / config.workers;
    let key_chunks: Vec<Vec<String>> = keys
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    if !config.silent {
        println!(
            "⚙️  Starting {} workers with ~{} keys each",
            key_chunks.len(),
            chunk_size
        );
    }

    progress.update_stage("Processing keys");

    let mut workers = Vec::new();
    for (worker_id, chunk) in key_chunks.into_iter().enumerate() {
        if chunk.is_empty() {
            continue;
        }

        let worker_config = config.clone();
        let worker_progress = progress.clone();
        let worker_writer = writer.clone();

        let worker = tokio::spawn(async move {
            let mut connection = match connect_redis(&worker_config).await {
                Ok(conn) => conn,
                Err(e) => {
                    if !worker_config.silent {
                        eprintln!(
                            "❌ Worker {} - Erro ao conectar ao Redis: {}",
                            worker_id + 1,
                            e
                        );
                    }
                    return Ok::<(), anyhow::Error>(());
                }
            };

            let pipeline_batch_size = 50;
            let mut command_buffer = Vec::new();

            for batch in chunk.chunks(pipeline_batch_size) {
                match generate_redis_commands_batch_optimized(&mut connection, batch).await {
                    Ok(commands) => {
                        command_buffer.extend(commands);

                        // Escrever em batches quando o buffer atingir o tamanho desejado
                        if command_buffer.len() >= worker_config.batch_size {
                            let mut writer_guard = worker_writer.lock().await;
                            for command in &command_buffer {
                                let formatted_command =
                                    format_command_output(command, &worker_config.output_format);
                                if let Err(e) =
                                    writer_guard.write_all(formatted_command.as_bytes()).await
                                {
                                    if !worker_config.silent {
                                        eprintln!(
                                            "❌ Worker {} - Erro ao escrever comando: {}",
                                            worker_id + 1,
                                            e
                                        );
                                    }
                                }
                            }
                            drop(writer_guard);
                            command_buffer.clear();
                        }
                    }
                    Err(e) => {
                        if !worker_config.silent {
                            eprintln!(
                                "❌ Worker {} - Erro ao gerar comandos para lote: {}",
                                worker_id + 1,
                                e
                            );
                        }
                    }
                }

                worker_progress.increment(batch.len()).await;
            }

            if !command_buffer.is_empty() {
                let mut writer_guard = worker_writer.lock().await;
                for command in &command_buffer {
                    let formatted_command =
                        format_command_output(command, &worker_config.output_format);
                    if let Err(e) = writer_guard.write_all(formatted_command.as_bytes()).await {
                        if !worker_config.silent {
                            eprintln!(
                                "❌ Worker {} - Erro ao escrever comando: {}",
                                worker_id + 1,
                                e
                            );
                        }
                    }
                }
                drop(writer_guard);
            }

            Ok::<(), anyhow::Error>(())
        });
        workers.push(worker);
    }

    progress.update_stage("Waiting for workers to finish");

    let results = join_all(workers).await;
    let mut failed_workers = 0;
    for (idx, result) in results.into_iter().enumerate() {
        if let Err(e) = result {
            failed_workers += 1;
            if !config.silent {
                eprintln!("❌ Worker {} falhou: {}", idx + 1, e);
            }
        } else if let Ok(Err(e)) = result {
            failed_workers += 1;
            if !config.silent {
                eprintln!("❌ Worker {} erro: {}", idx + 1, e);
            }
        }
    }

    progress.update_stage("Finalizing file");

    let mut writer_guard = writer.lock().await;
    writer_guard.flush().await?;
    drop(writer_guard);

    let success_message = if failed_workers > 0 {
        format!("Dump completed with {} workers failed", failed_workers)
    } else {
        "Dump completed - all workers finished successfully".to_string()
    };

    progress.finish(&success_message);

    if !config.silent {
        println!(
            "✅ Dump for group '{}' saved to: {}",
            group_name, output_file
        );
        if failed_workers > 0 {
            println!("⚠️  {} worker(s) failed during processing", failed_workers);
        }
    }

    Ok(())
}

async fn run_dump(config: DumpConfig) -> Result<()> {
    if !config.silent {
        println!("🚀 Starting Redis Dump Tool");
        println!(
            "📡 Connecting to Redis at {}:{}...",
            config.host, config.port
        );
    }

    let mut connection = connect_redis(&config).await?;

    if !config.silent {
        println!("✅ Connection established successfully!");
        println!("🔍 Fetching keys with filter: '{}'", config.filter);
    }

    let all_keys = scan_keys(&mut connection, &config.filter, config.scan_size).await?;

    if all_keys.is_empty() {
        println!("❌ No keys found with filter: {}", config.filter);
        return Ok(());
    }

    if !config.silent {
        println!("📊 Dump statistics:");
        println!("   • Total keys found: {}", all_keys.len());
        println!("   • Parallel workers: {}", config.workers);
        println!("   • Batch size: {}", config.batch_size);
        println!("   • SCAN size: {}", config.scan_size);
        println!("   • Output file: {}", config.output_file);
        println!("   • Preserve TTL: ✅ Always enabled");
        println!();
    }

    dump_keys(all_keys, &config, &config.output_file, "filtered keys").await?;

    if !config.silent {
        println!("🎉 Dump completed successfully!");
        println!("📁 File saved: {}", config.output_file);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
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

    let config = DumpConfig {
        host: matches.get_one::<String>("host").unwrap().clone(),
        port: matches.get_one::<String>("port").unwrap().parse()?,
        password: matches.get_one::<String>("password").cloned(),
        database: matches
            .get_one::<String>("database")
            .map(|s| s.parse())
            .transpose()?,
        filter: matches.get_one::<String>("filter").unwrap().clone(),
        output_file: matches.get_one::<String>("output").unwrap().clone(),
        batch_size: matches.get_one::<String>("batch-size").unwrap().parse()?,
        workers: matches.get_one::<String>("workers").unwrap().parse()?,
        scan_size: matches.get_one::<String>("scan-size").unwrap().parse()?,
        silent: matches.get_flag("silent"),
        output_format: match matches.get_one::<String>("format").unwrap().as_str() {
            "commands" => OutputFormat::Commands,
            "resp" => OutputFormat::Resp,
            _ => OutputFormat::Resp, // Default to RESP
        },
    };

    run_dump(config).await
}
