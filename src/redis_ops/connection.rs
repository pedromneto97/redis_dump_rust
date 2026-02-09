use anyhow::Result;

use crate::config::DumpConfig;

pub(crate) async fn connect_redis(
    config: &DumpConfig,
) -> Result<redis::aio::MultiplexedConnection> {
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

pub(crate) async fn scan_keys(
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

pub(crate) async fn discover_databases(
    connection: &mut redis::aio::MultiplexedConnection,
) -> Result<Vec<u8>> {
    let info: String = redis::cmd("INFO")
        .arg("keyspace")
        .query_async(connection)
        .await?;
    let mut databases = Vec::new();

    for line in info.lines() {
        if let Some(rest) = line.strip_prefix("db") {
            let mut parts = rest.splitn(2, ':');
            if let Some(db_str) = parts.next() {
                if let Ok(db) = db_str.parse::<u8>() {
                    databases.push(db);
                }
            }
        }
    }

    databases.sort_unstable();
    databases.dedup();
    Ok(databases)
}
