use std::thread::sleep;
use std::time::Duration;

use rand::Rng;
use rand::rngs::ThreadRng;
use tracing::debug;

struct Default {
    limit: u32,
    attempts: u32,
    base_delay: u64,
    rng: ThreadRng,
}

pub trait Delay {
    fn wait(&mut self);
    fn retry(&mut self) -> bool;
}

impl Delay for Default {
    fn wait(&mut self) {
        let jitter = self.rng.random_range(100..200);
        let time = Duration::from_millis(jitter << self.attempts);
        debug!("waiting for {time:?} milliseconds before retrying");
        sleep(time);
    }

    fn retry(&mut self) -> bool {
        if self.attempts < self.limit {
            self.attempts += 1;
            return true;
        }
        false
    }
}

pub struct Backoff;

impl Backoff {
    /// default retry config: 5 retry attempts, jittered exponential backoff
    pub fn default() -> impl Delay {
        Default {
            limit: 5,
            attempts: 0,
            base_delay: 1,
            rng: rand::rng(),
        }
    }
}

pub fn retry(mut c: impl Delay, mut f: impl FnMut() -> RetryStatus) -> RetryResult {
    while c.retry() {
        c.wait();
        if f() == RetryStatus::Break {
            return RetryResult::Success;
        }
    }
    RetryResult::AttemptsExceeded
}

#[derive(Debug, PartialEq)]
pub enum RetryStatus {
    Continue,
    Break,
}

#[derive(Debug, PartialEq)]
pub enum RetryResult {
    Success,
    AttemptsExceeded,
}
