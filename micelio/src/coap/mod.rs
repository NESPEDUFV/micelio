mod push;
mod server;

use crate::Connection;
use coap_lite::error::IncompatibleOptionValueFormat;
use coap_lite::option_value::OptionValueString;
use coap_lite::{
    CoapOption, CoapRequest, CoapResponse, ContentFormat, Packet, RequestType as Method,
};
pub use push::CoapTcpPush;
use serde::Deserialize;
use serde::{Serialize, de::DeserializeOwned};
pub(crate) use server::{
    CoapResult, CoapTcpServer, NoReturn, RawResponse, WithStatus, deser_path, deser_payload, routes,
};
use std::io;
use thiserror::Error;

pub trait CoapRequestExt {
    fn get_path_part(&self, index: usize) -> Result<String, CoapPathError>;
}

impl<S> CoapRequestExt for CoapRequest<S> {
    fn get_path_part(&self, index: usize) -> Result<String, CoapPathError> {
        let parts = self
            .message
            .get_options_as::<OptionValueString>(CoapOption::UriPath)
            .ok_or(CoapPathError::NoPath)?;
        let part = parts
            .into_iter()
            .nth(index)
            .ok_or(CoapPathError::NoPathPart(index))??;
        Ok(part.0)
    }
}

#[derive(Debug, Error)]
pub enum CoapPathError {
    #[error("no path option")]
    NoPath,
    #[error("bad option format: {0}")]
    BadFormat(#[source] IncompatibleOptionValueFormat),
    #[error("missing path part at index {0}")]
    NoPathPart(usize),
}

impl From<IncompatibleOptionValueFormat> for CoapPathError {
    fn from(value: IncompatibleOptionValueFormat) -> Self {
        Self::BadFormat(value)
    }
}

#[derive(Debug, Deserialize, Error)]
#[error("{error}")]
pub(crate) struct GenericError {
    error: String,
}

impl<S: Into<String>> From<S> for GenericError {
    fn from(value: S) -> Self {
        GenericError {
            error: value.into(),
        }
    }
}

impl Connection {
    pub async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        path: impl AsRef<str>,
        payload: &impl Serialize,
    ) -> io::Result<T> {
        let mut buffer = Vec::new();
        ciborium::into_writer(payload, &mut buffer).map_err(io::Error::other)?;
        self.send_raw(method, path, buffer, Some(ContentFormat::ApplicationCBOR))
            .await
    }

    pub async fn send_raw<T: DeserializeOwned>(
        &self,
        method: Method,
        path: impl AsRef<str>,
        payload: Vec<u8>,
        format: Option<ContentFormat>,
    ) -> io::Result<T> {
        let response_payload = self
            .send_raw_recv_raw(method, path, payload, format)
            .await?;
        ciborium::from_reader(response_payload.as_slice()).map_err(io::Error::other)
    }

    pub async fn send_raw_recv_raw(
        &self,
        method: Method,
        path: impl AsRef<str>,
        payload: Vec<u8>,
        format: Option<ContentFormat>,
    ) -> io::Result<Vec<u8>> {
        let mut request: CoapRequest<()> = CoapRequest::new();
        let path = path.as_ref();
        request.set_method(method);
        request.set_path(path);
        request
            .message
            .set_content_format(format.unwrap_or(ContentFormat::ApplicationOctetStream));
        request.message.payload = payload;
        let mut response = {
            let t0 = nsrs::time::now_delta();
            let mut stream = self.0.lock().await;
            let peer = stream.peer_addr();
            let data = request.message.to_bytes().map_err(io::Error::other)?;
            let data_size = data.len();
            stream.send_all(&data_size.to_le_bytes()).await?;
            stream.send_all(&data).await?;
            stream.flush().await?;
            nsrs::log!("[metrics/Connection][peer={peer}] sent bytes: {data_size}");
            let mut n_buf = [0u8; 8];
            stream.recv_all(&mut n_buf).await?;
            let n = usize::from_le_bytes(n_buf).min(Packet::MAX_SIZE);
            let mut buf = vec![0; n];
            stream.recv_all(&mut buf).await?;
            nsrs::log!("[metrics/Connection][peer={peer}] received bytes: {n}");
            nsrs::log!(
                "[metrics/Connection][peer={peer}] latency (s): {}",
                (nsrs::time::now_delta() - t0).as_secs_f64()
            );
            CoapResponse {
                message: Packet::from_bytes(&buf).map_err(io::Error::other)?,
            }
        };
        if response.get_status().is_error() {
            let e: Result<GenericError, _> =
                ciborium::from_reader(response.message.payload.as_slice());
            Err(io::Error::other(format!(
                "error {:?}: {}",
                response.get_status(),
                e.as_ref().map(|e| e.error.as_str()).unwrap_or(""),
            )))
        } else {
            Ok(std::mem::take(&mut response.message.payload))
        }
    }

    pub async fn close(self) -> io::Result<()> {
        let mut stream = self.0.lock().await;
        stream.close()
    }

    pub async fn debug(&self, on: bool) {
        self.0.lock().await.set_debug(on);
    }
}
