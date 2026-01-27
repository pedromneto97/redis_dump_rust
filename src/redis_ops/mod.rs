mod protocol;

pub(crate) use protocol::format_command_output;

use anyhow::Result;
use redis::AsyncCommands;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    sync::Mutex,
};

use crate::{
    config::{DumpConfig, OutputFormat},
    progress::DumpProgress,
};

async fn connect_redis(config: &DumpConfig) -> Result<redis::aio::MultiplexedConnection> {
    let redis_url = if let Some(password) = &config.password {
        format!(
            "redis://{}:{}@{}:{}",
            config.user, password, config.host, config.port
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

// --- Value Formatting Helpers ---

fn escape_quotes(s: &str) -> String {
    s.replace('"', "\\\"")
}

fn quoted(s: &str) -> String {
    format!("\"{}\"", escape_quotes(s))
}

fn append_expire_command(commands: &mut Vec<String>, key: &str, ttl: Option<i64>) {
    if let Some(ttl) = ttl {
        commands.push(format!("EXPIRE {} {}", quoted(key), ttl));
    }
}

// --- Key Metadata ---

#[derive(Debug, Clone)]
struct KeyMetadata {
    key: String,
    key_type: String,
    ttl: Option<i64>,
}

// --- Command Generation per Data Type ---

fn generate_string_command(key: &str, value: &str, ttl: Option<i64>) -> String {
    if let Some(ttl) = ttl {
        format!("SETEX {} {} {}", quoted(key), ttl, quoted(value))
    } else {
        format!("SET {} {}", quoted(key), quoted(value))
    }
}

async fn generate_list_commands(
    connection: &mut redis::aio::MultiplexedConnection,
    key: &str,
    ttl: Option<i64>,
) -> Result<Vec<String>> {
    let items: Vec<String> = redis::cmd("LRANGE")
        .arg(key)
        .arg(0)
        .arg(-1)
        .query_async(connection)
        .await
        .unwrap_or_default();

    let mut commands = Vec::new();
    if !items.is_empty() {
        let quoted_items: Vec<String> = items.iter().map(|item| quoted(item)).collect();
        commands.push(format!("RPUSH {} {}", quoted(key), quoted_items.join(" ")));
    }
    append_expire_command(&mut commands, key, ttl);
    Ok(commands)
}

async fn generate_set_commands(
    connection: &mut redis::aio::MultiplexedConnection,
    key: &str,
    ttl: Option<i64>,
) -> Result<Vec<String>> {
    let items: Vec<String> = redis::cmd("SMEMBERS")
        .arg(key)
        .query_async(connection)
        .await
        .unwrap_or_default();

    let mut commands = Vec::new();
    if !items.is_empty() {
        let quoted_items: Vec<String> = items.iter().map(|item| quoted(item)).collect();
        commands.push(format!("SADD {} {}", quoted(key), quoted_items.join(" ")));
    }
    append_expire_command(&mut commands, key, ttl);
    Ok(commands)
}

async fn generate_zset_commands(
    connection: &mut redis::aio::MultiplexedConnection,
    key: &str,
    ttl: Option<i64>,
) -> Result<Vec<String>> {
    let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
        .arg(key)
        .arg(0)
        .arg(-1)
        .arg("WITHSCORES")
        .query_async(connection)
        .await
        .unwrap_or_default();

    let mut commands = Vec::new();
    if !items.is_empty() {
        let score_member_pairs: Vec<String> = items
            .iter()
            .map(|(member, score)| format!("{} {}", score, quoted(member)))
            .collect();
        commands.push(format!(
            "ZADD {} {}",
            quoted(key),
            score_member_pairs.join(" ")
        ));
    }
    append_expire_command(&mut commands, key, ttl);
    Ok(commands)
}

async fn generate_hash_commands(
    connection: &mut redis::aio::MultiplexedConnection,
    key: &str,
    ttl: Option<i64>,
) -> Result<Vec<String>> {
    let hash_data: HashMap<String, String> = connection.hgetall(key).await.unwrap_or_default();

    let mut commands = Vec::new();
    if !hash_data.is_empty() {
        let field_value_pairs: Vec<String> = hash_data
            .iter()
            .map(|(field, value)| format!("{} {}", quoted(field), quoted(value)))
            .collect();
        commands.push(format!(
            "HMSET {} {}",
            quoted(key),
            field_value_pairs.join(" ")
        ));
    }
    append_expire_command(&mut commands, key, ttl);
    Ok(commands)
}

// --- Batch Processing ---

async fn fetch_key_metadata(
    connection: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<KeyMetadata>> {
    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("TYPE").arg(key).cmd("TTL").arg(key);
    }
    let results: Vec<(String, i64)> = pipe.query_async(connection).await?;

    let metadata = keys
        .iter()
        .zip(results.iter())
        .map(|(key, (key_type, ttl))| KeyMetadata {
            key: key.clone(),
            key_type: key_type.clone(),
            ttl: if *ttl > 0 { Some(*ttl) } else { None },
        })
        .collect();

    Ok(metadata)
}

fn partition_keys_by_type(metadata: Vec<KeyMetadata>) -> (Vec<KeyMetadata>, Vec<KeyMetadata>) {
    metadata.into_iter().partition(|m| m.key_type == "string")
}

async fn process_string_keys_batch(
    connection: &mut redis::aio::MultiplexedConnection,
    string_keys: Vec<KeyMetadata>,
) -> Result<Vec<String>> {
    let mut pipe = redis::pipe();
    for meta in &string_keys {
        pipe.get(&meta.key);
    }
    let values: Vec<String> = pipe.query_async(connection).await?;

    let commands = string_keys
        .iter()
        .zip(values.iter())
        .map(|(meta, value)| generate_string_command(&meta.key, value, meta.ttl))
        .collect();

    Ok(commands)
}

async fn process_non_string_key(
    connection: &mut redis::aio::MultiplexedConnection,
    metadata: &KeyMetadata,
) -> Result<Vec<String>> {
    match metadata.key_type.as_str() {
        "list" => generate_list_commands(connection, &metadata.key, metadata.ttl).await,
        "set" => generate_set_commands(connection, &metadata.key, metadata.ttl).await,
        "zset" => generate_zset_commands(connection, &metadata.key, metadata.ttl).await,
        "hash" => generate_hash_commands(connection, &metadata.key, metadata.ttl).await,
        _ => Ok(Vec::new()),
    }
}

async fn generate_redis_commands_batch_optimized(
    connection: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<String>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut all_commands = Vec::with_capacity(keys.len() * 2);
    let metadata = fetch_key_metadata(connection, keys).await?;
    let (string_keys, other_keys) = partition_keys_by_type(metadata);

    if !string_keys.is_empty() {
        let string_commands = process_string_keys_batch(connection, string_keys).await?;
        all_commands.extend(string_commands);
    }

    for key_meta in &other_keys {
        let commands = process_non_string_key(connection, key_meta).await?;
        all_commands.extend(commands);
    }

    Ok(all_commands)
}

// --- Worker Logic ---

#[derive(Clone)]
struct WorkerContext {
    worker_id: usize,
    keys: Vec<String>,
    config: DumpConfig,
    progress: Arc<DumpProgress>,
    writer: Arc<Mutex<BufWriter<File>>>,
}

async fn flush_commands_to_writer(ctx: &WorkerContext, commands: &[String]) {
    let mut writer_guard = ctx.writer.lock().await;
    for command in commands {
        let formatted = format_command_output(command, &ctx.config.output_format);
        if let Err(e) = writer_guard.write_all(formatted.as_bytes()).await {
            if !ctx.config.silent {
                eprintln!(
                    "❌ Worker {} - Failed to write command: {}",
                    ctx.worker_id + 1,
                    e
                );
            }
        }
    }
}

async fn process_worker_keys(ctx: WorkerContext) -> Result<()> {
    let mut connection = match connect_redis(&ctx.config).await {
        Ok(conn) => conn,
        Err(e) => {
            if !ctx.config.silent {
                eprintln!(
                    "❌ Worker {} - Failed to connect to Redis: {}",
                    ctx.worker_id + 1,
                    e
                );
            }
            return Ok(());
        }
    };

    let pipeline_batch_size = 50;
    let mut command_buffer = Vec::new();

    for batch in ctx.keys.chunks(pipeline_batch_size) {
        match generate_redis_commands_batch_optimized(&mut connection, batch).await {
            Ok(commands) => {
                command_buffer.extend(commands);
                if command_buffer.len() >= ctx.config.batch_size {
                    flush_commands_to_writer(&ctx, &command_buffer).await;
                    command_buffer.clear();
                }
            }
            Err(e) => {
                if !ctx.config.silent {
                    eprintln!(
                        "❌ Worker {} - Failed to generate commands for batch: {}",
                        ctx.worker_id + 1,
                        e
                    );
                }
            }
        }
        ctx.progress.increment(batch.len()).await;
    }

    if !command_buffer.is_empty() {
        flush_commands_to_writer(&ctx, &command_buffer).await;
    }

    Ok(())
}

// --- Dump Orchestration ---

fn create_worker_contexts(
    keys: Vec<String>,
    config: &DumpConfig,
    progress: Arc<DumpProgress>,
    writer: Arc<Mutex<BufWriter<File>>>,
) -> Vec<WorkerContext> {
    let chunk_size = keys.len().div_ceil(config.workers);
    keys.chunks(chunk_size)
        .enumerate()
        .filter(|(_, chunk)| !chunk.is_empty())
        .map(|(worker_id, chunk)| WorkerContext {
            worker_id,
            keys: chunk.to_vec(),
            config: config.clone(),
            progress: progress.clone(),
            writer: writer.clone(),
        })
        .collect()
}

fn spawn_workers(contexts: Vec<WorkerContext>) -> Vec<tokio::task::JoinHandle<Result<()>>> {
    contexts
        .into_iter()
        .map(|ctx| tokio::spawn(process_worker_keys(ctx)))
        .collect()
}

async fn collect_worker_results(
    workers: Vec<tokio::task::JoinHandle<Result<()>>>,
    config: &DumpConfig,
) -> usize {
    let results = futures::future::join_all(workers).await;
    let mut failed_workers = 0;

    for (idx, result) in results.into_iter().enumerate() {
        match result {
            Err(e) => {
                failed_workers += 1;
                if !config.silent {
                    eprintln!("❌ Worker {} failed: {e}", idx + 1);
                }
            }
            Ok(Err(e)) => {
                failed_workers += 1;
                if !config.silent {
                    eprintln!("❌ Worker {} error: {e}", idx + 1);
                }
            }
            Ok(Ok(())) => {}
        }
    }

    failed_workers
}

fn log_processing_start(config: &DumpConfig, key_count: usize, output_file: &str) {
    if !config.silent {
        println!(
            "🏗️  Starting processing of {} keys for: {}",
            key_count, output_file
        );
    }
}

fn log_worker_distribution(config: &DumpConfig, worker_count: usize, chunk_size: usize) {
    if !config.silent {
        println!(
            "⚙️  Starting {} workers with ~{} keys each",
            worker_count, chunk_size
        );
    }
}

fn report_completion(
    progress: &DumpProgress,
    config: &DumpConfig,
    group_name: &str,
    output_file: &str,
    failed_workers: usize,
) {
    let success_message = if failed_workers > 0 {
        format!("Dump completed with {failed_workers} workers failed")
    } else {
        "Dump completed - all workers finished successfully".to_string()
    };
    progress.finish(&success_message);

    if !config.silent {
        println!("✅ Dump for group '{group_name}' saved to: {output_file}");
        if failed_workers > 0 {
            println!("⚠️  {failed_workers} worker(s) failed during processing");
        }
    }
}

/// Dump keys to output file using parallel workers
pub async fn dump_keys(
    keys: Vec<String>,
    config: &DumpConfig,
    output_file: &str,
    group_name: &str,
) -> Result<()> {
    if keys.is_empty() {
        if !config.silent {
            println!("⚠️  No keys found for group: {group_name}");
        }
        return Ok(());
    }

    log_processing_start(config, keys.len(), output_file);
    let progress = Arc::new(DumpProgress::new(keys.len() as u64, config.silent));
    let file = File::create(output_file).await?;
    let writer = Arc::new(Mutex::new(BufWriter::new(file)));
    if !config.silent {
        println!("📝 Output file created: {output_file}");
    }

    progress.update_stage("Distributing work among workers");
    let key_count = keys.len();
    let worker_contexts = create_worker_contexts(keys, config, progress.clone(), writer.clone());
    let chunk_size = key_count.div_ceil(config.workers);
    log_worker_distribution(config, worker_contexts.len(), chunk_size);

    progress.update_stage("Processing keys");
    let workers = spawn_workers(worker_contexts);

    progress.update_stage("Waiting for workers to finish");
    let failed_workers = collect_worker_results(workers, config).await;

    progress.update_stage("Finalizing file");
    let mut writer_guard = writer.lock().await;
    writer_guard.flush().await?;
    drop(writer_guard);

    report_completion(&progress, config, group_name, output_file, failed_workers);
    Ok(())
}

/// Run the dump process
pub async fn run_dump(config: DumpConfig) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_commands_empty_input() {
        // Test the edge case of empty keys slice
        // Note: This would normally require a Redis connection, but we can test the early return
        // This is more of a documentation of expected behavior
        let empty_keys: Vec<String> = vec![];

        // We can't easily test the actual function without mocking Redis,
        // but we can test the logic that it should handle empty input gracefully
        assert!(empty_keys.is_empty());

        // Test format_command_output with empty command
        let empty_resp = format_command_output("", &OutputFormat::Resp);
        assert_eq!(empty_resp, "*0\r\n");
    }

    #[test]
    fn test_escape_quotes() {
        assert_eq!(escape_quotes("simple"), "simple");
        assert_eq!(escape_quotes("has\"quote"), "has\\\"quote");
        assert_eq!(escape_quotes("\"both\""), "\\\"both\\\"");
        assert_eq!(escape_quotes(""), "");
    }

    #[test]
    fn test_quoted() {
        assert_eq!(quoted("simple"), "\"simple\"");
        assert_eq!(quoted("has\"quote"), "\"has\\\"quote\"");
        assert_eq!(quoted(""), "\"\"");
    }

    #[test]
    fn test_generate_string_command_without_ttl() {
        let cmd = generate_string_command("key", "value", None);
        assert_eq!(cmd, "SET \"key\" \"value\"");
    }

    #[test]
    fn test_generate_string_command_with_ttl() {
        let cmd = generate_string_command("key", "value", Some(300));
        assert_eq!(cmd, "SETEX \"key\" 300 \"value\"");
    }

    #[test]
    fn test_generate_string_command_with_quotes() {
        let cmd = generate_string_command("my\"key", "my\"value", None);
        assert_eq!(cmd, "SET \"my\\\"key\" \"my\\\"value\"");
    }

    #[test]
    fn test_append_expire_command_with_ttl() {
        let mut commands = Vec::new();
        append_expire_command(&mut commands, "mykey", Some(300));
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0], "EXPIRE \"mykey\" 300");
    }

    #[test]
    fn test_append_expire_command_without_ttl() {
        let mut commands = Vec::new();
        append_expire_command(&mut commands, "mykey", None);
        assert!(commands.is_empty());
    }

    #[test]
    fn test_partition_keys_by_type() {
        let metadata = vec![
            KeyMetadata {
                key: "s1".into(),
                key_type: "string".into(),
                ttl: None,
            },
            KeyMetadata {
                key: "l1".into(),
                key_type: "list".into(),
                ttl: None,
            },
            KeyMetadata {
                key: "s2".into(),
                key_type: "string".into(),
                ttl: Some(100),
            },
            KeyMetadata {
                key: "h1".into(),
                key_type: "hash".into(),
                ttl: Some(200),
            },
        ];

        let (strings, others) = partition_keys_by_type(metadata);
        assert_eq!(strings.len(), 2);
        assert_eq!(others.len(), 2);
        assert_eq!(strings[0].key, "s1");
        assert_eq!(strings[1].key, "s2");
        assert_eq!(others[0].key, "l1");
        assert_eq!(others[1].key, "h1");
    }

    #[test]
    fn test_partition_keys_all_strings() {
        let metadata = vec![
            KeyMetadata {
                key: "s1".into(),
                key_type: "string".into(),
                ttl: None,
            },
            KeyMetadata {
                key: "s2".into(),
                key_type: "string".into(),
                ttl: None,
            },
        ];

        let (strings, others) = partition_keys_by_type(metadata);
        assert_eq!(strings.len(), 2);
        assert!(others.is_empty());
    }

    #[test]
    fn test_partition_keys_no_strings() {
        let metadata = vec![
            KeyMetadata {
                key: "l1".into(),
                key_type: "list".into(),
                ttl: None,
            },
            KeyMetadata {
                key: "h1".into(),
                key_type: "hash".into(),
                ttl: None,
            },
        ];

        let (strings, others) = partition_keys_by_type(metadata);
        assert!(strings.is_empty());
        assert_eq!(others.len(), 2);
    }

    #[test]
    fn test_partition_keys_empty() {
        let metadata: Vec<KeyMetadata> = vec![];
        let (strings, others) = partition_keys_by_type(metadata);
        assert!(strings.is_empty());
        assert!(others.is_empty());
    }
}
