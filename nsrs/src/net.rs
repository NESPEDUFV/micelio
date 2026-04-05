use crate::ffi;
use std::{
    io,
    net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

impl From<SocketAddr> for ffi::SocketAddr {
    fn from(value: SocketAddr) -> Self {
        let version = match &value {
            SocketAddr::V4(_) => ffi::IpAddrType::V4,
            SocketAddr::V6(_) => ffi::IpAddrType::V6,
        };
        Self {
            version,
            host: value.ip().to_string(),
            port: value.port(),
        }
    }
}

impl TryInto<SocketAddr> for ffi::SocketAddr {
    type Error = AddrParseError;

    fn try_into(self) -> Result<SocketAddr, Self::Error> {
        match self.version {
            ffi::IpAddrType::V4 => Ok(SocketAddr::new(IpAddr::V4(self.host.parse()?), self.port)),
            ffi::IpAddrType::V6 => Ok(SocketAddr::new(IpAddr::V6(self.host.parse()?), self.port)),
            _ => unreachable!("respect the ffi::SocketAddr enum!"),
        }
    }
}

/// Interface to an NS3 UdpSocket.
pub struct Ns3UdpSocket {
    inner: cxx::SharedPtr<ffi::Ns3UdpSocket>,
}

// SAFETY: this is only reasonable because the underlying NS3 simulation is single threaded
unsafe impl Send for ffi::Ns3UdpSocket {}
unsafe impl Sync for ffi::Ns3UdpSocket {}

impl Ns3UdpSocket {
    pub fn bind(addr: Option<SocketAddr>) -> io::Result<Self> {
        let node_id = crate::context();
        let mut inner = ffi::Ns3UdpSocket::create(node_id).map_err(io::Error::other)?;
        let addr = addr.unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));
        unsafe { inner.pin_mut_unchecked() }
            .bind(addr.into())
            .map_err(io::Error::other)?;
        Ok(Self { inner })
    }

    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<()> {
        unsafe { self.inner.pin_mut_unchecked() }
            .connect(addr.into())
            .map_err(io::Error::other)
    }

    pub fn recv_from<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send + Sync + 'a {
        async {
            crate::time::sleep(Duration::ZERO).await;
            UdpRecvFrom {
                socket: self,
                buffer: buf,
                event_key: None,
            }
            .await
        }
    }

    pub async fn send(&mut self, buf: &[u8]) -> io::Result<usize> {
        crate::time::sleep(Duration::ZERO).await;
        let sent = unsafe { self.inner.pin_mut_unchecked() }
            .send(buf)
            .map_err(io::Error::other)?;
        if sent >= 0 {
            Ok(sent as usize)
        } else {
            Err(io::Error::other(format!(
                "not enough space in buffer: {sent}"
            )))
        }
    }

    pub async fn send_to(&mut self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        crate::time::sleep(Duration::ZERO).await;
        let sent = unsafe { self.inner.pin_mut_unchecked() }
            .send_to(buf, addr.into())
            .map_err(io::Error::other)?;
        if sent >= 0 {
            Ok(sent as usize)
        } else {
            Err(io::Error::other(format!(
                "not enough space in buffer: {sent}"
            )))
        }
    }
}

#[pin_project::pin_project]
struct UdpRecvFrom<'a> {
    socket: &'a mut Ns3UdpSocket,
    buffer: &'a mut [u8],
    event_key: Option<usize>,
}

impl<'a> Future for UdpRecvFrom<'a> {
    type Output = io::Result<(usize, SocketAddr)>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut addr = ffi::SocketAddr {
            version: ffi::IpAddrType::V4,
            host: String::new(),
            port: 0,
        };
        let this = self.project();
        let n = unsafe { this.socket.inner.pin_mut_unchecked() }
            .recv_from(this.buffer, &mut addr)
            .map_err(io::Error::other)?;
        if n > 0 {
            deregister_waker(this.event_key);
            let addr: SocketAddr = addr.try_into().map_err(io::Error::other)?;
            Poll::Ready(Ok((n as usize, addr)))
        } else {
            let key = register_waker(this.event_key, cx.waker().clone());
            unsafe { this.socket.inner.pin_mut_unchecked() }.set_recv_key(key);
            Poll::Pending
        }
    }
}

/// Interface to a server NS3 TcpSocket.
pub struct Ns3TcpListener {
    inner: cxx::UniquePtr<ffi::Ns3TcpSocket>,
}

/// Interface to an NS3 TcpSocket.
pub struct Ns3TcpStream {
    inner: cxx::UniquePtr<ffi::Ns3TcpSocket>,
    debug: bool,
}

// SAFETY: this is only reasonable because the underlying NS3 simulation is single threaded
unsafe impl Send for ffi::Ns3TcpSocket {}
unsafe impl Sync for ffi::Ns3TcpSocket {}

impl Ns3TcpListener {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let node_id = crate::context();
        let mut inner = ffi::Ns3TcpSocket::create(node_id).map_err(io::Error::other)?;
        inner
            .as_mut()
            .expect("not null")
            .bind(addr.into())
            .map_err(io::Error::other)?;
        Ok(Self { inner })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner
            .local_addr()
            .try_into()
            .expect("local addr should be valid")
    }

    pub fn accept<'a>(
        &'a mut self,
    ) -> impl Future<Output = io::Result<(Ns3TcpStream, SocketAddr)>> + Send + Sync + 'a {
        TcpAccept {
            socket: self,
            event_key: None,
        }
    }
}

struct TcpAccept<'a> {
    socket: &'a mut Ns3TcpListener,
    event_key: Option<usize>,
}

impl<'a> Future for TcpAccept<'a> {
    type Output = io::Result<(Ns3TcpStream, SocketAddr)>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let accepted = self.socket.inner.as_mut().expect("not null").pop_accepted();
        match accepted.as_ref() {
            Some(socket) => {
                deregister_waker(&mut self.event_key);
                let peer_addr: SocketAddr =
                    socket.peer_addr().try_into().map_err(io::Error::other)?;
                Poll::Ready(Ok((Ns3TcpStream::new(accepted), peer_addr)))
            }
            None => {
                let key = register_waker(&mut self.event_key, cx.waker().clone());
                self.socket
                    .inner
                    .as_mut()
                    .expect("not null")
                    .push_accept_key(key);
                Poll::Pending
            }
        }
    }
}

impl Ns3TcpStream {
    fn new(inner: cxx::UniquePtr<ffi::Ns3TcpSocket>) -> Self {
        Self {
            inner,
            debug: false,
        }
    }

    pub fn set_debug(&mut self, debug: bool) {
        self.debug = debug;
    }

    pub fn connect(addr: SocketAddr) -> impl Future<Output = io::Result<Self>> + Send + Sync {
        async move {
            let node_id = crate::context();
            let mut socket = ffi::Ns3TcpSocket::create(node_id).map_err(io::Error::other)?;
            socket
                .as_mut()
                .expect("not null")
                .connect(addr.into())
                .map_err(io::Error::other)?;
            TcpConnect {
                socket,
                event_key: None,
            }
            .await
        }
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.inner
            .as_mut()
            .expect("not null")
            .close()
            .map_err(io::Error::other)
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner
            .local_addr()
            .try_into()
            .map_err(io::Error::other)
            .expect("local addr should be valid")
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.inner
            .peer_addr()
            .try_into()
            .map_err(io::Error::other)
            .expect("local addr should be valid")
    }

    pub fn node_id(&self) -> u32 {
        self.inner.get_nodeid()
    }

    pub fn recv<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = io::Result<usize>> + Send + Sync + 'a {
        async move {
            crate::time::sleep(Duration::ZERO).await;
            TcpRecv {
                socket: self,
                buffer: buf,
                event_key: None,
            }
            .await
        }
    }

    pub async fn recv_all<'a>(&'a mut self, buf: &'a mut [u8]) -> io::Result<()> {
        let mut recd = 0;
        let total = buf.len();
        if self.debug {
            crate::log!("[Ns3TcpStream::recv_all] total: {total}");
        }
        while recd < total {
            let n = self.recv(&mut buf[recd..]).await?;
            recd += n;
            if self.debug {
                crate::log!("[Ns3TcpStream::recv_all] recd: {recd}");
            }
        }
        Ok(())
    }

    pub fn send<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = io::Result<usize>> + Send + Sync + 'a {
        async move {
            crate::time::sleep(Duration::ZERO).await;
            TcpSend {
                socket: self,
                buffer: buf,
                event_key: None,
            }
            .await
        }
    }

    pub async fn send_all<'a>(&'a mut self, buf: &'a [u8]) -> io::Result<()> {
        let mut sent = 0;
        let total = buf.len();
        while sent < total {
            let n = self.send(&buf[sent..]).await?;
            if n == 0 {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            sent += n;
        }
        Ok(())
    }

    pub fn flush<'a>(&'a mut self) -> impl Future<Output = io::Result<()>> + Send + Sync + 'a {
        TcpFlush {
            socket: self,
            event_key: None,
        }
    }
}

impl Drop for Ns3TcpStream {
    fn drop(&mut self) {
        self.inner.as_mut().expect("not null").clear_callbacks();
    }
}

struct TcpConnect {
    socket: cxx::UniquePtr<ffi::Ns3TcpSocket>,
    event_key: Option<usize>,
}

impl Future for TcpConnect {
    type Output = io::Result<Ns3TcpStream>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.socket.is_null() {
            panic!("should not poll TcpConnect after being Ready(Ok(...))");
        }
        match self
            .socket
            .as_mut()
            .expect("not null")
            .get_connected_status()
        {
            0 => {
                let key = register_waker(&mut self.event_key, cx.waker().clone());
                self.socket
                    .as_mut()
                    .expect("not null")
                    .push_connect_key(key);
                Poll::Pending
            }
            1 => {
                let socket = std::mem::replace(&mut self.socket, cxx::UniquePtr::null());
                deregister_waker(&mut self.event_key);
                Poll::Ready(Ok(Ns3TcpStream::new(socket)))
            }
            _ => {
                deregister_waker(&mut self.event_key);
                Poll::Ready(Err(io::ErrorKind::ConnectionRefused.into()))
            }
        }
    }
}

#[pin_project::pin_project]
struct TcpRecv<'a> {
    socket: &'a mut Ns3TcpStream,
    buffer: &'a mut [u8],
    event_key: Option<usize>,
}

impl<'a> Future for TcpRecv<'a> {
    type Output = io::Result<usize>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this
            .socket
            .inner
            .as_mut()
            .expect("not null")
            .recv(this.buffer)
        {
            Ok(0) => {
                let errno = this.socket.inner.get_errno();
                if this.socket.debug {
                    crate::log!("[TcpRecv] got 0 bytes from {} (errno {errno:?}) => Pending", this.socket.peer_addr());
                }
                let key = register_waker(this.event_key, cx.waker().clone());
                this.socket
                    .inner
                    .as_mut()
                    .expect("not null")
                    .push_recv_key(key);
                Poll::Pending
            }
            Ok(n) => {
                let errno = this.socket.inner.get_errno();
                if this.socket.debug {
                    crate::log!("[TcpRecv] got {n} bytes from {} (errno {errno:?}) => Ready", this.socket.peer_addr());
                }
                deregister_waker(this.event_key);
                Poll::Ready(Ok(n as usize))
            }
            Err(e) => {
                let errno = this.socket.inner.get_errno();
                if this.socket.debug {
                    crate::log!("[TcpRecv] got error {e} from {} (errno {errno:?}) => Ready Err", this.socket.peer_addr());
                }
                deregister_waker(this.event_key);
                Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)))
            }
        }
    }
}

#[pin_project::pin_project]
struct TcpSend<'a> {
    socket: &'a mut Ns3TcpStream,
    buffer: &'a [u8],
    event_key: Option<usize>,
}

impl<'a> Future for TcpSend<'a> {
    type Output = io::Result<usize>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this
            .socket
            .inner
            .as_mut()
            .expect("not null")
            .send(this.buffer)
        {
            Ok(n) => {
                if this.socket.debug {
                    crate::log!("[TcpSend] accepted {n} bytes to send to {} => Ready", this.socket.peer_addr());
                }
                deregister_waker(this.event_key);
                Poll::Ready(Ok(n as usize))
            }
            Err(e) => match this.socket.inner.get_errno() {
                ffi::SocketErrno::ERROR_AGAIN => {
                    if this.socket.debug {
                        crate::log!("[TcpSend] got ERROR_AGAIN from {}, retry later => Pending", this.socket.peer_addr());
                    }
                    let key = register_waker(this.event_key, cx.waker().clone());
                    this.socket
                        .inner
                        .as_mut()
                        .expect("not null")
                        .push_send_key(key);
                    Poll::Pending
                }
                _ => {
                    if this.socket.debug {
                        crate::log!("[TcpSend] ouch, error from {}: {e} => Ready Err", this.socket.peer_addr());
                    }
                    deregister_waker(this.event_key);
                    Poll::Ready(Err(io::Error::other(e)))
                }
            },
        }
    }
}

#[pin_project::pin_project]
struct TcpFlush<'a> {
    socket: &'a mut Ns3TcpStream,
    event_key: Option<usize>,
}

impl<'a> Future for TcpFlush<'a> {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if this.socket.inner.get_pending() > 0 {
            let key = register_waker(this.event_key, cx.waker().clone());
            this.socket
                .inner
                .as_mut()
                .expect("not null")
                .push_sent_key(key);
            Poll::Pending
        } else {
            deregister_waker(this.event_key);
            Poll::Ready(Ok(()))
        }
    }
}

fn deregister_waker(event_key: &mut Option<usize>) {
    if let Some(key) = event_key {
        let mut reactor = crate::runtime::REACTOR.lock().expect("should get lock");
        reactor.deregister_waker(*key);
        *event_key = None;
    }
}

fn register_waker(event_key: &mut Option<usize>, waker: Waker) -> usize {
    let mut reactor = crate::runtime::REACTOR.lock().expect("should get lock");
    let key = event_key.unwrap_or_else(|| {
        let key = reactor.next_key();
        *event_key = Some(key);
        key
    });
    reactor.register_waker(key, waker);
    key
}
