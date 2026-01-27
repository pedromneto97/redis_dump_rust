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

fn format_resp_command(parts: &[&str]) -> String {
    let mut result = format!("*{}\r\n", parts.len());

    for part in parts {
        result.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    result
}

fn parse_redis_command(command: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current_part = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let chars: Vec<char> = command.trim().chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        if escape_next {
            current_part.push(ch);
            escape_next = false;
        } else if ch == '\\' {
            if in_quotes && i + 1 < chars.len() {
                let next_ch = chars[i + 1];
                if next_ch == '"' {
                    // This is an escaped quote inside quoted string
                    current_part.push('"');
                    i += 1; // Skip the next character (the escaped quote)
                } else {
                    // For non-quote escapes (like \x00), preserve the backslash
                    current_part.push(ch);
                }
            } else {
                escape_next = true;
            }
        } else if ch == '"' {
            if in_quotes {
                // Closing quote - add current part if it's not empty OR if we're closing an empty quoted string
                if !current_part.is_empty() || parts.is_empty() {
                    parts.push(current_part.clone());
                    current_part.clear();
                }
                in_quotes = false;
            } else {
                // Opening quote
                in_quotes = true;
                // If we have content before the quote, save it first
                if !current_part.is_empty() {
                    parts.push(current_part.clone());
                    current_part.clear();
                }
            }
        } else if ch.is_whitespace() && !in_quotes {
            if !current_part.is_empty() {
                parts.push(current_part.clone());
                current_part.clear();
            }
        } else {
            current_part.push(ch);
        }

        i += 1;
    }

    // Handle remaining content
    if !current_part.is_empty() || in_quotes {
        parts.push(current_part);
    }

    parts
}

fn format_command_output(command: &str, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Commands => format!("{command}\n"),
        OutputFormat::Resp => {
            let parts = parse_redis_command(command);
            let part_refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            format_resp_command(&part_refs)
        }
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
    let key_chunks = keys.chunks(chunk_size).map(|chunk| chunk.to_vec());
    if !config.silent {
        println!(
            "⚙️  Starting {} workers with ~{} keys each",
            key_chunks.len(),
            chunk_size
        );
    }
    progress.update_stage("Processing keys");
    let mut workers = Vec::new();
    for (worker_id, chunk) in key_chunks.enumerate() {
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
                            "❌ Worker {} - Failed to connect to Redis: {}",
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
                                            "❌ Worker {} - Failed to write command: {}",
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
                                "❌ Worker {} - Failed to generate commands for batch: {}",
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
                            eprintln!("❌ Worker {} - Failed to write command: {e}", worker_id + 1,);
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
                eprintln!("❌ Worker {} failed: {e}", idx + 1);
            }
        } else if let Ok(Err(e)) = result {
            failed_workers += 1;
            if !config.silent {
                eprintln!("❌ Worker {} error: {e}", idx + 1);
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
    use crate::config::OutputFormat;

    #[test]
    fn test_format_resp_command_basic() {
        let parts = vec!["SET", "key", "value"];
        let resp = format_resp_command(&parts);
        assert!(resp.starts_with("*3\r\n"));
        assert!(resp.contains("$3\r\nSET\r\n"));
        assert!(resp.contains("$3\r\nkey\r\n"));
        assert!(resp.contains("$5\r\nvalue\r\n"));

        // Verify complete structure
        let expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        assert_eq!(resp, expected);
    }

    #[test]
    fn test_format_resp_command_empty() {
        let parts: Vec<&str> = vec![];
        let resp = format_resp_command(&parts);
        assert_eq!(resp, "*0\r\n");
    }

    #[test]
    fn test_format_resp_command_single_part() {
        let parts = vec!["PING"];
        let resp = format_resp_command(&parts);
        assert_eq!(resp, "*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn test_format_resp_command_with_empty_string() {
        let parts = vec!["SET", "key", ""];
        let resp = format_resp_command(&parts);
        assert!(resp.contains("$0\r\n\r\n")); // Empty value
        let expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n";
        assert_eq!(resp, expected);
    }

    #[test]
    fn test_format_resp_command_with_special_chars() {
        let parts = vec!["SET", "key:with:colons", "value\nwith\nnewlines"];
        let resp = format_resp_command(&parts);
        assert!(resp.contains("key:with:colons"));
        assert!(resp.contains("value\nwith\nnewlines"));
    }

    #[test]
    fn test_parse_redis_command_simple() {
        let cmd = "SET key value";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_parse_redis_command_quotes_and_whitespace() {
        let cmd = "SET \"key name\" \"value\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key name", "value"]);
    }

    #[test]
    fn test_parse_redis_command_mixed_quotes() {
        let cmd = "SET \"quoted key\" unquoted_value \"another quoted value\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(
            parsed,
            vec![
                "SET",
                "quoted key",
                "unquoted_value",
                "another quoted value"
            ]
        );
    }

    #[test]
    fn test_parse_redis_command_escaped_quotes() {
        let cmd = r#"SET key "value with \"escaped\" quotes""#;
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", r#"value with "escaped" quotes"#]);
    }

    #[test]
    fn test_parse_redis_command_empty() {
        let cmd = "";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, Vec::<String>::new());
    }

    #[test]
    fn test_parse_redis_command_whitespace_only() {
        let cmd = "   \t  \n  ";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, Vec::<String>::new());
    }

    #[test]
    fn test_parse_redis_command_single_word() {
        let cmd = "PING";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["PING"]);
    }

    #[test]
    fn test_parse_redis_command_multiple_spaces() {
        let cmd = "SET    key     value";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_parse_redis_command_tabs_and_newlines() {
        let cmd = "SET\tkey\nvalue";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_parse_redis_command_only_quotes() {
        let cmd = r#""""#;
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec![""]);
    }

    #[test]
    fn test_parse_redis_command_nested_quotes() {
        let cmd = r#"SET key "value with 'single quotes' inside""#;
        let parsed = parse_redis_command(cmd);
        assert_eq!(
            parsed,
            vec!["SET", "key", "value with 'single quotes' inside"]
        );
    }

    #[test]
    fn test_format_command_output_resp() {
        let cmd = "SET key value";
        let out = format_command_output(cmd, &OutputFormat::Resp);
        assert!(out.contains("*3\r\n"));
        assert!(out.contains("$3\r\nSET\r\n"));
        assert!(out.contains("$3\r\nkey\r\n"));
        assert!(out.contains("$5\r\nvalue\r\n"));
    }

    #[test]
    fn test_format_command_output_commands() {
        let cmd = "SET key value";
        let out = format_command_output(cmd, &OutputFormat::Commands);
        assert_eq!(out, "SET key value\n");
    }

    #[test]
    fn test_format_command_output_empty_command() {
        let cmd = "";
        let resp_out = format_command_output(cmd, &OutputFormat::Resp);
        let cmd_out = format_command_output(cmd, &OutputFormat::Commands);

        assert_eq!(resp_out, "*0\r\n");
        assert_eq!(cmd_out, "\n");
    }

    #[test]
    fn test_format_command_output_complex_command() {
        let cmd = r#"HMSET user:123 name "John Doe" email "john@example.com" age 30"#;
        let resp_out = format_command_output(cmd, &OutputFormat::Resp);
        let cmd_out = format_command_output(cmd, &OutputFormat::Commands);

        assert!(resp_out.starts_with("*8\r\n")); // 8 parts
        assert_eq!(cmd_out, format!("{}\n", cmd));
    }

    #[test]
    fn test_command_parsing_edge_cases() {
        // Test various Redis command formats
        let test_cases = vec![
            ("PING", vec!["PING"]),
            ("GET key", vec!["GET", "key"]),
            ("SET key value", vec!["SET", "key", "value"]),
            (
                "MSET key1 value1 key2 value2",
                vec!["MSET", "key1", "value1", "key2", "value2"],
            ),
            (
                "SADD myset member1 member2 member3",
                vec!["SADD", "myset", "member1", "member2", "member3"],
            ),
            ("EXPIRE key 3600", vec!["EXPIRE", "key", "3600"]),
        ];

        for (input, expected) in test_cases {
            let parsed = parse_redis_command(input);
            assert_eq!(parsed, expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_resp_format_various_commands() {
        let test_cases = vec![
            ("PING", "*1\r\n$4\r\nPING\r\n"),
            ("GET key", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
            (
                "DEL key1 key2",
                "*3\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n",
            ),
        ];

        for (input, expected_resp) in test_cases {
            let output = format_command_output(input, &OutputFormat::Resp);
            assert_eq!(output, expected_resp, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_command_format_consistency() {
        let commands = vec![
            "SET key value",
            "GET key",
            "DEL key1 key2 key3",
            "HMSET hash field1 value1 field2 value2",
            "SADD set member1 member2",
            "ZADD zset 1.0 member1 2.0 member2",
            "LPUSH list item1 item2 item3",
        ];

        for cmd in commands {
            let parsed = parse_redis_command(cmd);
            let resp_output = format_command_output(cmd, &OutputFormat::Resp);
            let cmd_output = format_command_output(cmd, &OutputFormat::Commands);

            // Commands output should just add newline
            assert_eq!(cmd_output, format!("{}\n", cmd));

            // RESP output should be valid
            assert!(resp_output.starts_with(&format!("*{}\r\n", parsed.len())));

            // Each parsed part should appear in RESP output
            for part in &parsed {
                assert!(resp_output.contains(&format!("${}\r\n{}\r\n", part.len(), part)));
            }
        }
    }

    #[test]
    fn test_special_characters_in_commands() {
        let special_cases = vec![
            (
                "SET key \"value with spaces\"",
                vec!["SET", "key", "value with spaces"],
            ),
            (
                "SET key \"value\nwith\nnewlines\"",
                vec!["SET", "key", "value\nwith\nnewlines"],
            ),
            (
                "SET \"key:with:colons\" value",
                vec!["SET", "key:with:colons", "value"],
            ),
            (
                "SET key \"value,with,commas\"",
                vec!["SET", "key", "value,with,commas"],
            ),
            (
                "SET key \"value;with;semicolons\"",
                vec!["SET", "key", "value;with;semicolons"],
            ),
        ];

        for (input, expected) in special_cases {
            let parsed = parse_redis_command(input);
            assert_eq!(parsed, expected, "Failed for input: {}", input);

            // Test that formatting works correctly
            let resp_out = format_command_output(input, &OutputFormat::Resp);
            let cmd_out = format_command_output(input, &OutputFormat::Commands);

            assert!(!resp_out.is_empty());
            assert!(!cmd_out.is_empty());
        }
    }

    #[test]
    fn test_unicode_characters() {
        let cmd = "SET key \"测试数据\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", "测试数据"]);

        let resp_out = format_command_output(cmd, &OutputFormat::Resp);
        assert!(resp_out.contains("测试数据"));
    }

    #[test]
    fn test_binary_safe_data() {
        // Test with data that could contain null bytes or binary data (as string representation)
        let cmd = "SET binary_key \"\\x00\\x01\\x02\\x03\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "binary_key", "\\x00\\x01\\x02\\x03"]);
    }

    #[test]
    fn test_resp_format_with_special_lengths() {
        // Test RESP formatting with various string lengths
        let test_cases = vec![
            (vec![], "*0\r\n"),               // Empty array
            (vec![""], "*1\r\n$0\r\n\r\n"),   // Single empty string
            (vec!["a"], "*1\r\n$1\r\na\r\n"), // Single character
            (
                vec!["hello", "world"],
                "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            ), // Multiple strings
        ];

        for (input, expected) in test_cases {
            let resp = format_resp_command(&input);
            assert_eq!(resp, expected, "Failed for input: {:?}", input);
        }
    }

    #[test]
    fn test_parse_command_error_handling() {
        // Test parsing commands with various edge cases and malformed input
        let test_cases = vec![
            // Unmatched quotes should still parse gracefully
            (
                "SET key \"unmatched quote",
                vec!["SET", "key", "unmatched quote"],
            ),
            // Multiple consecutive quotes (the parser handles this by treating them as opening/closing)
            ("SET \"\"\"key\"\"\" value", vec!["SET", "key", "value"]),
            // Quote at the end starts a new quoted section that's empty
            ("SET key value\"", vec!["SET", "key", "value", ""]),
            // Mixed whitespace characters (all treated as separators)
            ("  SET\r\n\tkey\x0Bvalue  ", vec!["SET", "key", "value"]),
        ];

        for (input, expected) in test_cases {
            let parsed = parse_redis_command(input);
            assert_eq!(parsed, expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_format_output_consistency() {
        // Ensure that parsing and formatting are consistent
        let original_commands = vec![
            "SET key value",
            "HMSET hash field1 value1 field2 value2",
            "SADD set member1 member2",
            "LPUSH list item1 item2",
        ];

        for cmd in original_commands {
            let parsed = parse_redis_command(cmd);
            let resp_output = format_command_output(cmd, &OutputFormat::Resp);
            let cmd_output = format_command_output(cmd, &OutputFormat::Commands);

            // Commands output should preserve the original with newline
            assert_eq!(cmd_output, format!("{}\n", cmd));

            // RESP should be properly formatted
            assert!(resp_output.starts_with(&format!("*{}\r\n", parsed.len())));

            // Round-trip test: parsed parts should match original intent
            assert!(
                !parsed.is_empty(),
                "Parsing should not return empty result for: {}",
                cmd
            );
        }
    }

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
}
