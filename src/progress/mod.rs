use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct DumpProgress {
    pub total_keys: u64,
    pub processed_keys: Arc<RwLock<u64>>,
    pub start_time: std::time::Instant,
    pub progress_bar: Arc<ProgressBar>,
}

impl DumpProgress {
    pub fn new(total_keys: u64, silent: bool) -> Self {
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

    pub async fn increment(&self, batch_size: usize) {
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

    pub fn finish(&self, message: &str) {
        let elapsed = self.start_time.elapsed();
        let final_message = format!(
            "{} | Total time: {:.2}s | Avg rate: {:.0} keys/s",
            message,
            elapsed.as_secs_f64(),
            self.total_keys as f64 / elapsed.as_secs_f64()
        );
        self.progress_bar.finish_with_message(final_message);
    }

    pub fn update_stage(&self, stage: &str) {
        self.progress_bar.set_message(format!("Stage: {}", stage));
    }
}
