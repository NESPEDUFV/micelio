use coap_lite::{CoapRequest, Packet};
use nsrs::net::Ns3TcpStream;
use std::{io, net::SocketAddr, sync::Arc};

pub struct CoapTcpPush {
    addr: SocketAddr,
    hello: Vec<u8>,
    retries: usize,
}

impl CoapTcpPush {
    pub fn new(addr: SocketAddr, hello: Vec<u8>) -> Self {
        Self {
            addr,
            hello,
            retries: 0,
        }
    }

    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries = retries;
        self
    }

    pub async fn run<Handler, Ret>(self, recv_coap: Handler) -> io::Result<()>
    where
        Handler: Fn(Box<CoapRequest<()>>) -> Ret + Sync + Send + 'static,
        Ret: Future<Output = Box<CoapRequest<()>>> + Send,
    {
        let handler = Arc::new(recv_coap);
        for _ in 0..=self.retries {
            match self.run_inner(handler.clone()).await {
                Ok(_) => break,
                Err(e) => nsrs::log!("[CoapTcpPush] error: {e}"),
            }
        }
        Ok(())
    }

    async fn run_inner<Handler, Ret>(&self, handler: Arc<Handler>) -> io::Result<()>
    where
        Handler: Fn(Box<CoapRequest<()>>) -> Ret + Sync + Send + 'static,
        Ret: Future<Output = Box<CoapRequest<()>>> + Send,
    {
        let peer = self.addr;
        let mut stream = Ns3TcpStream::connect(peer).await?;
        stream.send_all(&self.hello.len().to_le_bytes()).await?;
        stream.send_all(&self.hello).await?;
        stream.flush().await?;
        nsrs::log!("[CoapTcpPush] listening from {:?}...", peer);
        let mut len_buf = [0u8; 8];
        let mut buf = [0u8; Packet::MAX_SIZE];
        while let Ok(_) = stream.recv_all(&mut len_buf).await {
            let t0 = nsrs::time::now_delta();
            let n = usize::from_le_bytes(len_buf).min(buf.len());
            if n == 0 {
                continue;
            }
            stream.recv_all(&mut buf[..n]).await?;
            nsrs::log!("[metrics/CoapTcpPush][peer={peer}] received bytes: {}", n);
            let packet = Packet::from_bytes(&buf[..n]).map_err(io::Error::other)?;
            let request = Box::new(CoapRequest::from_packet(packet, ()));
            let request = handler(request).await;
            let Some(response) = request.response else {
                continue;
            };
            let data = response.message.to_bytes().map_err(io::Error::other)?;
            let data_size = data.len();
            stream.send_all(&data_size.to_le_bytes()).await?;
            stream.send_all(&data).await?;
            stream.flush().await?;
            nsrs::log!("[metrics/CoapTcpPush][peer={peer}] sent bytes: {data_size}");
            nsrs::log!(
                "[metrics/CoapTcpPush][peer={peer}] latency (s): {}",
                (nsrs::time::now_delta() - t0).as_secs_f64()
            );
        }
        Ok(())
    }
}
