use anyhow::Result;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

use crate::{
    config::{DumpConfig, OutputFormat},
    progress::DumpProgress,
};

pub async fn connect_redis(config: &DumpConfig) -> Result<redis::aio::MultiplexedConnection> {
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

pub async fn scan_keys(
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

pub fn format_resp_command(parts: &[&str]) -> String {
    let mut result = String::new();
    result.push_str(&format!("*{}\r\n", parts.len()));
    for part in parts {
        result.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    result
}

pub fn parse_redis_command(command: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current_part = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let chars = command.trim().chars().peekable();
    for ch in chars {
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

pub fn format_command_output(command: &str, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Commands => format!("{command}\n"),
        OutputFormat::Resp => {
            let parts = parse_redis_command(command);
            let part_refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            format_resp_command(&part_refs)
        }
    }
}

pub async fn generate_redis_commands_batch_optimized(
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
                    commands.push(format!("HMSET \"{key}\" {}", field_value_pairs.join(" ")));
                }
            }
            _ => {}
        }
        all_commands.extend(commands);
        if let Some(ttl) = key_ttls.get(key) {
            match key_type.as_str() {
                "list" | "set" | "zset" | "hash" => {
                    all_commands.push(format!("EXPIRE \"{key}\" {ttl}"));
                }
                _ => {}
            }
        }
    }
    Ok(all_commands)
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
        println!("📝 Output file created: {output_file}");
    }
    progress.update_stage("Distributing work among workers");
    let chunk_size = keys.len().div_ceil(config.workers);
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
                                "❌ Worker {} - Erro ao escrever comando: {e}",
                                worker_id + 1,
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
    let results = futures::future::join_all(workers).await;
    let mut failed_workers = 0;
    for (idx, result) in results.into_iter().enumerate() {
        if let Err(e) = result {
            failed_workers += 1;
            if !config.silent {
                eprintln!("❌ Worker {} falhou: {e}", idx + 1);
            }
        } else if let Ok(Err(e)) = result {
            failed_workers += 1;
            if !config.silent {
                eprintln!("❌ Worker {} erro: {e}", idx + 1);
            }
        }
    }
    progress.update_stage("Finalizing file");
    let mut writer_guard = writer.lock().await;
    writer_guard.flush().await?;
    drop(writer_guard);
    let success_message = if failed_workers > 0 {
        format!("Dump completed with {failed_workers} workers failed")
    } else {
        "Dump completed - all workers finished successfully".to_string()
    };
    progress.finish(&success_message);
    if !config.silent {
        println!("✅ Dump for group '{group_name}' saved to: {output_file}",);
        if failed_workers > 0 {
            println!("⚠️  {failed_workers} worker(s) failed during processing");
        }
    }
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
        println!("� Dump statistics:");
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
    use crate::config::OutputFormat;

    #[test]
    fn test_format_resp_command_basic() {
        let parts = vec!["SET", "key", "value"];
        let resp = format_resp_command(&parts);
        assert!(resp.starts_with("*3\r\n"));
        assert!(resp.contains("$3\r\nSET\r\n"));
    }

    #[test]
    fn test_parse_redis_command_quotes_and_whitespace() {
        let cmd = "SET \"key name\" \"value\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key name", "value"]);
    }

    #[test]
    fn test_format_command_output_resp() {
        let cmd = "SET key value";
        let out = format_command_output(cmd, &OutputFormat::Resp);
        assert!(out.contains("*3\r\n"));
    }

    #[test]
    fn test_format_command_output_commands() {
        let cmd = "SET key value";
        let out = format_command_output(cmd, &OutputFormat::Commands);
        assert_eq!(out, "SET key value\n");
    }
}
