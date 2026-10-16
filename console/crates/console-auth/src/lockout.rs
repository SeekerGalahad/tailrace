//! In-memory brute-force lockout (§4.1): 5 consecutive failures per username
//! lock the account for 60 seconds. Not persisted — process memory only, as
//! specified.

use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct LockState {
    failures: u32,
    locked_until: Option<Instant>,
}

#[derive(Debug)]
pub struct LoginLockout {
    max_failures: u32,
    lock_duration: Duration,
    inner: std::sync::Mutex<HashMap<String, LockState>>,
}

/// Outcome of checking whether a login attempt may proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockCheck {
    Allowed,
    Locked { retry_in_secs: u64 },
}

impl LoginLockout {
    pub fn new() -> Self {
        Self {
            max_failures: 5,
            lock_duration: Duration::from_secs(60),
            inner: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Check-and-advance: a locked username is rejected; an expired lock
    /// resets the failure counter.
    pub fn check(&self, username: &str, now: Instant) -> LockCheck {
        let mut map = self.inner.lock().unwrap();
        let Some(state) = map.get_mut(username) else {
            return LockCheck::Allowed;
        };
        match state.locked_until {
            Some(until) if now < until => LockCheck::Locked {
                retry_in_secs: (until - now).as_secs().max(1),
            },
            Some(_) => {
                // lock expired: start over
                state.locked_until = None;
                state.failures = 0;
                LockCheck::Allowed
            }
            None => LockCheck::Allowed,
        }
    }

    /// Record a failed password attempt; locks when the counter reaches
    /// `max_failures`.
    pub fn record_failure(&self, username: &str, now: Instant) {
        let mut map = self.inner.lock().unwrap();
        let state = map.entry(username.to_string()).or_insert(LockState {
            failures: 0,
            locked_until: None,
        });
        state.failures += 1;
        if state.failures >= self.max_failures {
            state.locked_until = Some(now + self.lock_duration);
        }
    }

    /// Successful login clears the counter.
    pub fn record_success(&self, username: &str) {
        self.inner.lock().unwrap().remove(username);
    }
}

impl Default for LoginLockout {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_failures_lock_for_60s() {
        let lock = LoginLockout::new();
        let t0 = Instant::now();
        for i in 0..4 {
            lock.record_failure("admin", t0);
            assert_eq!(
                lock.check("admin", t0),
                LockCheck::Allowed,
                "not yet locked at {i}"
            );
        }
        lock.record_failure("admin", t0); // 5th failure
        assert_eq!(
            lock.check("admin", t0),
            LockCheck::Locked { retry_in_secs: 60 },
            "locked right at threshold"
        );
        // still locked 59s later
        assert!(matches!(
            lock.check("admin", t0 + Duration::from_secs(59)),
            LockCheck::Locked { .. }
        ));
        // expired -> allowed again and counter reset (one failure won't lock)
        assert_eq!(
            lock.check("admin", t0 + Duration::from_secs(61)),
            LockCheck::Allowed
        );
        lock.record_failure("admin", t0 + Duration::from_secs(61));
        assert_eq!(
            lock.check("admin", t0 + Duration::from_secs(62)),
            LockCheck::Allowed
        );
    }

    #[test]
    fn success_resets_counter() {
        let lock = LoginLockout::new();
        let t0 = Instant::now();
        for _ in 0..4 {
            lock.record_failure("u", t0);
        }
        lock.record_success("u");
        for _ in 0..4 {
            lock.record_failure("u", t0); // four more after reset — still under threshold
        }
        assert_eq!(lock.check("u", t0), LockCheck::Allowed);
    }
}
