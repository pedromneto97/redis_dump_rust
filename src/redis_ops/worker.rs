use anyhow::Result;
use std::sync::Arc;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    sync::Mutex,
};

use crate::{
    config::DumpConfig,
    progress::DumpProgress,
};

use super::batch::generate_redis_commands_batch_optimized;
use super::connection::connect_redis;
use super::protocol::format_command_output;

#[derive(Clone)]
pub(crate) struct WorkerContext {
    pub(crate) worker_id: usize,
    pub(crate) keys: Vec<String>,
    pub(crate) config: DumpConfig,
    pub(crate) progress: Arc<DumpProgress>,
    pub(crate) writer: Arc<Mutex<BufWriter<File>>>,
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

pub(crate) fn create_worker_contexts(
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

pub(crate) fn spawn_workers(
    contexts: Vec<WorkerContext>,
) -> Vec<tokio::task::JoinHandle<Result<()>>> {
    contexts
        .into_iter()
        .map(|ctx| tokio::spawn(process_worker_keys(ctx)))
        .collect()
}

pub(crate) async fn collect_worker_results(
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
