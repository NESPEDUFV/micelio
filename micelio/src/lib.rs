//! Micelio: MIddleware for Context rEasoning through federated Learning in the IOT computing continuum

pub mod cloud;
#[cfg(feature = "coap")]
pub mod coap;
pub mod dto;
pub mod edge;
pub mod error;
pub mod fl;
pub mod fog;
pub mod kdb;
pub mod vocab;

use futures::lock::Mutex;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Connection(Arc<Mutex<nsrs::net::Ns3TcpStream>>);

impl Connection {
    pub async fn to(addr: std::net::SocketAddr) -> std::io::Result<Self> {
        let stream = nsrs::net::Ns3TcpStream::connect(addr).await?;
        Ok(stream.into())
    }
}

impl From<nsrs::net::Ns3TcpStream> for Connection {
    fn from(value: nsrs::net::Ns3TcpStream) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }
}
