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
            "{percentage:.1}% | {keys_per_second:.0} keys/s | {current} processed"
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
        self.progress_bar.set_message(format!("Stage: {stage}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[test]
    fn test_new_progress_bar_not_silent() {
        let progress = DumpProgress::new(100, false);
        assert_eq!(progress.total_keys, 100);
        assert!(!progress.progress_bar.is_hidden());
    }

    #[test]
    fn test_new_progress_bar_silent() {
        let progress = DumpProgress::new(50, true);
        assert_eq!(progress.total_keys, 50);
        assert!(progress.progress_bar.is_hidden());
    }

    #[test]
    fn test_progress_initialization() {
        let progress = DumpProgress::new(1000, false);
        assert_eq!(progress.total_keys, 1000);

        // Check initial state
        let elapsed = progress.start_time.elapsed();
        assert!(elapsed.as_millis() < 100); // Should be very recent
    }

    #[tokio::test]
    async fn test_progress_increment() {
        let progress = DumpProgress::new(100, true); // Use silent to avoid output in tests

        // Initial processed count should be 0
        let initial_count = *progress.processed_keys.read().await;
        assert_eq!(initial_count, 0);

        // Increment by 10
        progress.increment(10).await;
        let count_after_increment = *progress.processed_keys.read().await;
        assert_eq!(count_after_increment, 10);

        // Increment by another 5
        progress.increment(5).await;
        let final_count = *progress.processed_keys.read().await;
        assert_eq!(final_count, 15);
    }

    #[tokio::test]
    async fn test_progress_increment_beyond_total() {
        let progress = DumpProgress::new(10, true);

        // Increment beyond total
        progress.increment(15).await;
        let count = *progress.processed_keys.read().await;
        assert_eq!(count, 15); // Should allow going over total
    }

    #[tokio::test]
    async fn test_progress_multiple_increments() {
        let progress = DumpProgress::new(1000, true);

        // Simulate multiple worker increments
        let increments = vec![10usize, 25usize, 5usize, 100usize, 50usize];
        let expected_total: u64 = increments.iter().map(|&x| x as u64).sum();

        for increment in increments {
            progress.increment(increment).await;
        }

        let final_count = *progress.processed_keys.read().await;
        assert_eq!(final_count, expected_total);
    }

    #[test]
    fn test_progress_finish() {
        let progress = DumpProgress::new(100, true);

        // This should not panic
        progress.finish("Test completed");

        // Progress bar should be finished (we can't easily test the message without output)
        assert!(progress.progress_bar.is_finished());
    }

    #[test]
    fn test_progress_update_stage() {
        let progress = DumpProgress::new(100, true);

        // This should not panic
        progress.update_stage("Testing stage");
        progress.update_stage("Another stage");
        progress.update_stage("Final stage");
    }

    #[tokio::test]
    async fn test_progress_timing() {
        let progress = DumpProgress::new(100, true);

        let start_time = progress.start_time;

        // Small delay to ensure some time passes
        sleep(Duration::from_millis(10)).await;

        progress.increment(10).await;

        let elapsed_after_increment = start_time.elapsed();
        assert!(elapsed_after_increment.as_millis() >= 10);
    }

    #[test]
    fn test_progress_zero_keys() {
        let progress = DumpProgress::new(0, false);
        assert_eq!(progress.total_keys, 0);
    }

    #[test]
    fn test_progress_large_number_of_keys() {
        let progress = DumpProgress::new(u64::MAX, true);
        assert_eq!(progress.total_keys, u64::MAX);
    }

    #[tokio::test]
    async fn test_progress_concurrent_access() {
        use std::sync::Arc;
        use tokio::task;

        let progress = Arc::new(DumpProgress::new(1000, true));

        let mut handles = Vec::new();

        // Spawn multiple tasks that increment progress
        for i in 0..10 {
            let progress_clone = progress.clone();
            let handle = task::spawn(async move {
                progress_clone.increment(i + 1).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.expect("Task should complete successfully");
        }

        // Calculate expected total: 1 + 2 + 3 + ... + 10 = 55
        let expected_total = (1..=10).sum::<u64>();
        let final_count = *progress.processed_keys.read().await;
        assert_eq!(final_count, expected_total);
    }

    #[test]
    fn test_progress_debug_output() {
        let progress = DumpProgress::new(100, true);
        let debug_str = format!("{:?}", progress);
        assert!(debug_str.contains("DumpProgress"));
        assert!(debug_str.contains("total_keys: 100"));
    }
}
