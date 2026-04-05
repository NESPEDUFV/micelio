//! Synchronization mechanisms.
use futures::{
    FutureExt,
    lock::{Mutex, MutexGuard},
};
use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    task::{Context, Poll, Waker},
};

#[derive(Clone)]
pub struct Barrier {
    wait_for: usize,
    wakers: Arc<StdMutex<Vec<Waker>>>,
}

impl Barrier {
    pub fn new(n: usize) -> Self {
        Self {
            wait_for: n,
            wakers: Default::default(),
        }
    }

    pub fn wait<'a>(&'a self) -> impl Future<Output = ()> + Send + Sync + 'a {
        BarrierWait(self)
    }

    pub fn reset(&self) {
        self.wakers.lock().expect("should get the lock").clear();
    }
}

struct BarrierWait<'a>(&'a Barrier);

impl<'a> Future for BarrierWait<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut wakers = self.0.wakers.lock().expect("should get the lock");
        if wakers.len().saturating_add(1) >= self.0.wait_for {
            for waker in wakers.iter() {
                waker.wake_by_ref();
            }
            Poll::Ready(())
        } else {
            wakers.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

pub struct AsyncMap<K, V> {
    values: Mutex<HashMap<K, V>>,
    wakers: StdMutex<HashMap<K, Vec<Waker>>>,
}

impl<K: Eq + Hash + Clone + Debug, V> Default for AsyncMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone + Debug, V> From<HashMap<K, V>> for AsyncMap<K, V> {
    fn from(value: HashMap<K, V>) -> Self {
        Self {
            values: Mutex::new(value),
            wakers: Default::default(),
        }
    }
}

impl<K: Eq + Hash + Clone + Debug + Debug, V: Debug> AsyncMap<K, V> {
    pub async fn debug(&self) {
        crate::log!("[AsyncMap] {:?}", self.values.lock().await);
    }
}

impl<K: Eq + Hash + Clone + Debug, V> AsyncMap<K, V> {
    pub fn new() -> Self {
        Self {
            values: Default::default(),
            wakers: Default::default(),
        }
    }

    pub fn get<'m, 'k>(&'m self, key: &'k K) -> impl Future<Output = AsyncMapGuard<'m, 'k, K, V>> {
        MapReadWait { map: self, key }
    }

    pub fn insert(&self, key: K, value: V) -> impl Future<Output = ()> {
        let mut wm = self.wakers.lock().expect("lock");
        if let Some(wakers) = wm.remove(&key) {
            for w in wakers {
                w.wake();
            }
        }
        drop(wm);
        async {
            self.values.lock().await.insert(key, value);
        }
    }

    pub async fn remove(&self, key: &K) -> Option<V> {
        self.values.lock().await.remove(key)
    }
}

struct MapReadWait<'m, 'k, K, V> {
    map: &'m AsyncMap<K, V>,
    key: &'k K,
}

impl<'m, 'k, K: Eq + Hash + Clone + Debug, V> Future for MapReadWait<'m, 'k, K, V> {
    type Output = AsyncMapGuard<'m, 'k, K, V>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Poll::Ready(values) = self.map.values.lock().poll_unpin(cx) else {
            return Poll::Pending;
        };

        if values.contains_key(self.key) {
            Poll::Ready(AsyncMapGuard {
                guard: values,
                key: self.key,
            })
        } else {
            self.map
                .wakers
                .lock()
                .expect("lock")
                .entry(self.key.clone())
                .or_default()
                .push(cx.waker().clone());
            Poll::Pending
        }
    }
}

pub struct AsyncMapGuard<'m, 'k, K, V> {
    guard: MutexGuard<'m, HashMap<K, V>>,
    key: &'k K,
}

impl<'m, 'k, K: Eq + Hash, V> Deref for AsyncMapGuard<'m, 'k, K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.guard
            .get(self.key)
            .expect("guard is only created when the key is in the map")
    }
}

impl<'m, 'k, K: Eq + Hash, V> DerefMut for AsyncMapGuard<'m, 'k, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .get_mut(self.key)
            .expect("guard is only created when the key is in the map")
    }
}
