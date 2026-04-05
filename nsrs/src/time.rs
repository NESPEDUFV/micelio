use futures::FutureExt;
use std::{
    pin::Pin,
    sync::LazyLock,
    task::{Context, Poll},
    time::Duration,
};

const SIM_DTZERO: LazyLock<chrono::DateTime<chrono::Utc>> = LazyLock::new(|| {
    let env_t = std::env::var("SIM_DTZERO").ok();
    let t = env_t
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("2026-01-01T00:00:00.0Z");
    chrono::DateTime::parse_from_rfc3339(t)
        .expect("date time should be properly set")
        .to_utc()
});

/// Current simulation virtual time, expressed in UTC DateTime.
pub fn datetime_now() -> chrono::DateTime<chrono::Utc> {
    *SIM_DTZERO + now()
}

/// Current simulation virtual time.
pub fn now() -> Duration {
    let t = crate::ffi::now();
    Duration::from_secs_f64(t)
}

/// Current simulation virtual time, plus the elapse real time since the
/// beginning of the current async execution.
pub fn now_delta() -> Duration {
    now() + elapsed()
}

/// Real time elapsed since the beginning of the current async execution.
pub fn elapsed() -> Duration {
    crate::RUNTIME.with(|r| r.t0().elapsed())
}

/// Puts the current async task to sleep for the specified duration.
pub fn sleep(dt: Duration) -> impl Future<Output = ()> + Send + Sync + 'static {
    Sleep {
        deadline: now() + elapsed() + dt,
        event_key: None,
    }
}

pub(crate) struct Sleep {
    deadline: Duration,
    event_key: Option<usize>,
}

impl Future for Sleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = now();
        if now >= self.deadline {
            if let Some(key) = self.event_key {
                let mut reactor = crate::runtime::REACTOR.lock().expect("should get lock");
                reactor.deregister_waker(key);
            }
            Poll::Ready(())
        } else {
            if self.event_key.is_none() {
                let mut reactor = crate::runtime::REACTOR.lock().expect("should get lock");
                let dt = self.deadline - now;
                let key = reactor.next_key();
                self.event_key = Some(key);
                reactor.register_waker(key, cx.waker().clone());
                crate::ffi::schedule_awake(key, dt.as_secs_f64());
            }
            Poll::Pending
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(key) = self.event_key {
            let mut reactor = crate::runtime::REACTOR.lock().expect("should get lock");
            reactor.deregister_waker(key);
        }
    }
}

pub async fn timeout<F, T>(dt: Duration, future: F) -> Option<T>
where
    F: Future<Output = T>,
{
    let t = sleep(dt);
    futures::select! {
        value = future.fuse() => Some(value),
        _ = t.fuse() => None,
    }
}
