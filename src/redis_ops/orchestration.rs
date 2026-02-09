use anyhow::Result;
use std::sync::Arc;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    sync::Mutex,
};

use crate::{
    config::{resolve_output_path, DumpConfig},
    progress::DumpProgress,
    redis_ops::{
        connection::{connect_redis, discover_databases, scan_keys},
        worker::{collect_worker_results, create_worker_contexts, spawn_workers},
    },
};

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

async fn dump_database(config: &DumpConfig, output_file: &str, group_name: &str) -> Result<()> {
    let mut connection = connect_redis(config).await?;
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
        println!("   • Output file: {}", output_file);
        println!("   • Preserve TTL: ✅ Always enabled");
        println!();
    }

    dump_keys(all_keys, config, output_file, group_name).await?;
    if !config.silent {
        println!("🎉 Dump completed successfully!");
        println!("📁 File saved: {}", output_file);
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
    if let Some(db) = config.database {
        if !config.silent {
            println!("🗂️  Target database: {}", db);
        }
        dump_database(&config, &config.output_file, "filtered keys").await?;
        return Ok(());
    }

    let mut connection = connect_redis(&config).await?;
    let databases = discover_databases(&mut connection).await?;
    if databases.is_empty() {
        println!("❌ No keys found with filter: {}", config.filter);
        return Ok(());
    }

    if databases.len() == 1 {
        let db = databases[0];
        if !config.silent {
            println!("🗂️  Target database: {}", db);
        }
        let mut db_config = config.clone();
        db_config.database = Some(db);
        dump_database(&db_config, &config.output_file, "filtered keys").await?;
        return Ok(());
    }

    for db in databases {
        let mut db_config = config.clone();
        db_config.database = Some(db);
        let output_file = resolve_output_path(&config.output_file, db);
        if !db_config.silent {
            println!("🗂️  Dumping database {} -> {}", db, output_file);
        }
        dump_database(&db_config, &output_file, "filtered keys").await?;
    }

    Ok(())
}
