use anyhow::Result;

use super::commands::{
    generate_hash_commands, generate_list_commands, generate_set_commands, generate_string_command,
    generate_zset_commands,
};

#[derive(Debug, Clone)]
pub(crate) struct KeyMetadata {
    pub(crate) key: String,
    pub(crate) key_type: String,
    pub(crate) ttl: Option<i64>,
}

pub(crate) async fn fetch_key_metadata(
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

pub(crate) fn partition_keys_by_type(
    metadata: Vec<KeyMetadata>,
) -> (Vec<KeyMetadata>, Vec<KeyMetadata>) {
    metadata.into_iter().partition(|m| m.key_type == "string")
}

pub(crate) async fn process_string_keys_batch(
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

pub(crate) async fn process_non_string_key(
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

pub(crate) async fn generate_redis_commands_batch_optimized(
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

#[cfg(test)]
mod tests {
    use super::*;

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
