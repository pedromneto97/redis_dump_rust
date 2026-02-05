use anyhow::Result;
use redis::AsyncCommands;
use std::collections::HashMap;

use crate::redis_ops::formatting::{append_expire_command, quoted};

pub(crate) fn generate_string_command(key: &str, value: &str, ttl: Option<i64>) -> String {
    if let Some(ttl) = ttl {
        format!("SETEX {} {} {}", quoted(key), ttl, quoted(value))
    } else {
        format!("SET {} {}", quoted(key), quoted(value))
    }
}

pub(crate) async fn generate_list_commands(
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

pub(crate) async fn generate_set_commands(
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

pub(crate) async fn generate_zset_commands(
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

pub(crate) async fn generate_hash_commands(
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
