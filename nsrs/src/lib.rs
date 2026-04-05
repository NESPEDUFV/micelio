pub mod net;
pub mod runtime;
pub mod sync;
pub mod time;

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use futures::{StreamExt, stream::FuturesUnordered};
pub use runtime::Runtime;

use crate::runtime::{REACTOR, RUNTIME};

#[cxx::bridge(namespace = "nsrs")]
pub mod ffi {
    enum IpAddrType {
        None = 0,
        V4 = 4,
        V6 = 16,
    }

    struct SocketAddr {
        version: IpAddrType,
        host: String,
        port: u16,
    }

    #[derive(Debug)]
    enum SocketErrno {
        ERROR_NOTERROR,
        ERROR_ISCONN,
        ERROR_NOTCONN,
        ERROR_MSGSIZE,
        ERROR_AGAIN,
        ERROR_SHUTDOWN,
        ERROR_OPNOTSUPP,
        ERROR_AFNOSUPPORT,
        ERROR_INVAL,
        ERROR_BADF,
        ERROR_NOROUTETOHOST,
        ERROR_NODEV,
        ERROR_ADDRNOTAVAIL,
        ERROR_ADDRINUSE,
    }

    extern "Rust" {
        pub fn run();
        pub fn wake(key: usize);
    }

    unsafe extern "C++" {
        include!("nsrs/include/runtime.h");

        pub fn now() -> f64;
        pub fn stop(delay: f64);
        pub fn stop_now();
        pub fn schedule_awake(key: usize, dt: f64);
        pub fn get_context() -> u32;

        type Ns3UdpSocket;

        #[Self = "Ns3UdpSocket"]
        pub fn create(node_id: u32) -> Result<SharedPtr<Ns3UdpSocket>>;
        pub fn bind(self: Pin<&mut Ns3UdpSocket>, addr: SocketAddr) -> Result<()>;
        pub fn connect(self: Pin<&mut Ns3UdpSocket>, addr: SocketAddr) -> Result<()>;
        pub fn send(self: Pin<&mut Ns3UdpSocket>, buf: &[u8]) -> Result<i32>;
        pub fn send_to(self: Pin<&mut Ns3UdpSocket>, buf: &[u8], addr: SocketAddr) -> Result<i32>;
        pub fn recv_from(
            self: Pin<&mut Ns3UdpSocket>,
            buf: &mut [u8],
            addr: &mut SocketAddr,
        ) -> Result<i32>;
        pub fn set_recv_key(self: Pin<&mut Ns3UdpSocket>, key: usize);

        type Ns3TcpSocket;

        #[Self = "Ns3TcpSocket"]
        pub fn create(node_id: u32) -> Result<UniquePtr<Ns3TcpSocket>>;
        pub fn bind(self: Pin<&mut Ns3TcpSocket>, addr: SocketAddr) -> Result<()>;
        pub fn connect(self: Pin<&mut Ns3TcpSocket>, addr: SocketAddr) -> Result<()>;
        pub fn peer_addr(self: &Ns3TcpSocket) -> SocketAddr;
        pub fn local_addr(self: &Ns3TcpSocket) -> SocketAddr;
        pub fn get_nodeid(self: &Ns3TcpSocket) -> u32;
        pub fn get_errno(self: &Ns3TcpSocket) -> SocketErrno;
        pub fn send(self: Pin<&mut Ns3TcpSocket>, buf: &[u8]) -> Result<i32>;
        pub fn recv(self: Pin<&mut Ns3TcpSocket>, buf: &mut [u8]) -> Result<i32>;
        pub fn close(self: Pin<&mut Ns3TcpSocket>) -> Result<()>;
        pub fn clear_callbacks(self: Pin<&mut Ns3TcpSocket>);

        pub fn push_accept_key(self: Pin<&mut Ns3TcpSocket>, key: usize);
        pub fn push_connect_key(self: Pin<&mut Ns3TcpSocket>, key: usize);
        pub fn push_send_key(self: Pin<&mut Ns3TcpSocket>, key: usize);
        pub fn push_recv_key(self: Pin<&mut Ns3TcpSocket>, key: usize);
        pub fn push_sent_key(self: Pin<&mut Ns3TcpSocket>, key: usize);
        pub fn pop_accepted(self: Pin<&mut Ns3TcpSocket>) -> UniquePtr<Ns3TcpSocket>;
        pub fn get_pending(self: &Ns3TcpSocket) -> usize;
        pub fn get_connected_status(self: &Ns3TcpSocket) -> i8;
    }
}

pub fn stop(delay: Duration) {
    ffi::stop(delay.as_secs_f64());
}

pub fn stop_now() {
    ffi::stop_now();
}

pub fn run() {
    RUNTIME.with(|r| r.run());
}

pub(crate) fn wake(key: usize) {
    let reactor = REACTOR.lock().expect("should get lock");
    reactor.wake(key);
}

pub fn spawn(future: impl Future<Output = ()> + Send + 'static) {
    RUNTIME.with(|r| r.spawn(future));
}

pub fn spawn_on_context(ctx: u32, future: impl Future<Output = ()> + Send + 'static) {
    RUNTIME.with(|r| r.spawn_on_context(ctx, future));
}

pub fn context() -> u32 {
    RUNTIME.with(|r| r.context())
}

#[must_use]
pub async fn join_all_with_timeout<I, F>(dt: Duration, iter: I) -> Joined<F::Output>
where
    I: IntoIterator<Item = F>,
    F: Future,
{
    let mut futures: FuturesUnordered<_> = iter.into_iter().map(|f| time::timeout(dt, f)).collect();
    let mut results = Vec::new();
    while let Some(res) = futures.next().await {
        results.push(res);
    }
    Joined(results)
}

pub struct Joined<T>(Vec<Option<T>>);

impl<T> Deref for Joined<T> {
    type Target = Vec<Option<T>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Joined<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Joined<T> {
    pub fn into_inner(self) -> Vec<Option<T>> {
        self.0
    }

    pub fn into_done(self) -> impl Iterator<Item = T> {
        self.0.into_iter().filter_map(|r| r)
    }
}

impl<T, E> Joined<Result<T, E>> {
    pub fn into_done_ok<C>(self) -> Result<C, E>
    where
        C: FromIterator<T>,
    {
        self.into_done().collect()
    }

    pub fn into_all_ok<C>(self, timed_out: fn() -> E) -> Result<C, E>
    where
        C: FromIterator<T>,
    {
        self.0
            .into_iter()
            .map(|r| match r {
                Some(Ok(v)) => Ok(v),
                Some(Err(e)) => Err(e),
                None => Err(timed_out()),
            })
            .collect()
    }

    pub fn all_ok(self, timed_out: fn() -> E) -> Result<(), E> {
        for result in self.0.into_iter() {
            match result {
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e),
                None => return Err(timed_out())
            }
        }
        Ok(())
    }
}

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {{
        let now = $crate::time::datetime_now();
        let node = $crate::context();
        println!("[{:3}][{:12}] {}", node, now, format!($($arg)*));
    }};
}
