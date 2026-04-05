use crate::Connection;
use ciborium::cbor;
use coap_lite::{CoapRequest, CoapResponse, Packet, ResponseType as Status};
use nsrs::net::Ns3TcpListener;
use serde::Serialize;
use std::fmt::Display;
use std::{net::SocketAddr, sync::Arc};

macro_rules! unwrap_or_return {
    ($stream:expr; $val:expr; $inspect:expr) => {
        if $val.inspect_err($inspect).is_err() {
            $stream.close().unwrap_or_default();
            return;
        }
    };
}

pub struct CoapTcpServer {
    addr: SocketAddr,
}

impl CoapTcpServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn run<Handler, Ret>(self, recv_coap: Handler)
    where
        Handler: Fn(Box<CoapRequest<Connection>>) -> Ret + Sync + Send + 'static,
        Ret: Future<Output = Box<CoapRequest<Connection>>> + Send,
    {
        let Ok(mut listener) = Ns3TcpListener::bind(self.addr) else {
            nsrs::log!("[CoapTcpServer] failed to bind listener socket");
            return;
        };
        nsrs::log!("[CoapTcpServer] listening on addr {:?}", self.addr);
        let handler = Arc::new(recv_coap);
        let mut len_buf = [0u8; 8];
        let mut buf = [0u8; Packet::MAX_SIZE];
        while let Ok((mut stream, peer)) = listener.accept().await {
            let handler = handler.clone();
            nsrs::spawn(async move {
                let t0 = nsrs::time::now_delta();
                let Ok(_) = stream.recv_all(&mut len_buf).await else {
                    nsrs::log!("[CoapTcpServer] failed to get total message size");
                    return;
                };
                let n = usize::from_le_bytes(len_buf).min(buf.len());
                if n == 0 {
                    return;
                }
                let Ok(_) = stream.recv_all(&mut buf[..n]).await else {
                    nsrs::log!("[CoapTcpServer] failed to get message");
                    return;
                };
                let packet = match Packet::from_bytes(&buf[..n]) {
                    Ok(packet) => packet,
                    Err(e) => {
                        nsrs::log!("[CoapTcpServer] failed to parse message into coap packet: {e}");
                        return;
                    }
                };
                let request = Box::new(CoapRequest::from_packet(packet, stream.into()));
                let request = handler(request).await;
                let Some(response) = request.response else {
                    nsrs::log!(
                        "[metrics/CoapTcpServer][peer={peer}] latency (s): {}",
                        (nsrs::time::now_delta() - t0).as_secs_f64()
                    );
                    return;
                };

                if let Some(Connection(stream)) = request.source {
                    let mut stream = stream.lock().await;
                    let data = match response.message.to_bytes() {
                        Ok(payload) => payload,
                        Err(e) => {
                            nsrs::log!("[CoapTcpServer] failed to reply to {peer:?}: {e}");
                            let mut message = Packet::new();
                            message.payload = b"{\"error\": \"response too large\"}".into();
                            let mut error_response = CoapResponse { message };
                            error_response.set_status(Status::InternalServerError);
                            error_response
                                .message
                                .to_bytes()
                                .expect("error response should be valid")
                        }
                    };
                    let data_size = data.len();
                    unwrap_or_return!(stream; stream.send_all(&data_size.to_le_bytes()).await; |e| {
                        nsrs::log!("[CoapTcpServer] failed to reply to {peer:?}: {e}");
                    });
                    unwrap_or_return!(stream; stream.send_all(&data).await; |e| {
                        nsrs::log!("[CoapTcpServer] failed to reply to {peer:?}: {e}");
                    });
                    unwrap_or_return!(stream; stream.flush().await; |e| {
                        nsrs::log!("[CoapTcpServer] failed to reply to {peer:?}: {e}");
                    });
                    // unwrap_or_return!(stream; stream.close(); |e| {
                    //     nsrs::log!("[CoapTcpServer] failed to close conn with {addr:?}: {e}");
                    // });
                    nsrs::log!("[metrics/CoapTcpServer][peer={peer}] sent bytes: {data_size}");
                }
                nsrs::log!(
                    "[metrics/CoapTcpServer][peer={peer}] latency (s): {}",
                    (nsrs::time::now_delta() - t0).as_secs_f64()
                );
            });
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct NoReturn;

pub(crate) struct WithStatus<R> {
    pub status: Status,
    pub result: R,
}

pub(crate) struct RawResponse(pub Vec<u8>);

pub(crate) trait CoapResult {
    fn apply_coap_result<S>(self, request: &mut CoapRequest<S>);
}

impl CoapResult for NoReturn {
    fn apply_coap_result<S>(self, request: &mut CoapRequest<S>) {
        request.response = None;
    }
}

impl<E: Display> CoapResult for Result<NoReturn, E> {
    fn apply_coap_result<S>(self, request: &mut CoapRequest<S>) {
        match self {
            Ok(no) => no.apply_coap_result(request),
            Err(e) => {
                request.response.as_mut().map(|response| {
                    response.set_status(Status::BadRequest);
                    ciborium::into_writer(
                        &ciborium::cbor!({"error" => e.to_string()}).unwrap(),
                        &mut response.message.payload,
                    )
                    .unwrap_or_default();
                });
            }
        }
    }
}

impl<T: Serialize, E: Display> CoapResult for Result<T, E> {
    fn apply_coap_result<S>(self, request: &mut CoapRequest<S>) {
        match self {
            Ok(data) => {
                request.response.as_mut().map(|response| {
                    response.set_status(Status::Changed);
                    ciborium::into_writer(&data, &mut response.message.payload).unwrap_or_default();
                });
            }
            Err(e) => {
                request.response.as_mut().map(|response| {
                    response.set_status(Status::BadRequest);
                    ciborium::into_writer(
                        &ciborium::cbor!({"error" => e.to_string()}).unwrap(),
                        &mut response.message.payload,
                    )
                    .unwrap_or_default();
                });
            }
        }
    }
}

impl<E: Display> CoapResult for Result<RawResponse, E> {
    fn apply_coap_result<S>(self, request: &mut CoapRequest<S>) {
        match self {
            Ok(data) => {
                request.response.as_mut().map(|response| {
                    response.set_status(Status::Changed);
                    response.message.payload = data.0;
                });
            }
            Err(e) => {
                request.response.as_mut().map(|response| {
                    response.set_status(Status::BadRequest);
                    ciborium::into_writer(
                        &ciborium::cbor!({"error" => e.to_string()}).unwrap(),
                        &mut response.message.payload,
                    )
                    .unwrap_or_default();
                });
            }
        }
    }
}

impl<R: CoapResult> CoapResult for WithStatus<R> {
    fn apply_coap_result<S>(self, request: &mut CoapRequest<S>) {
        self.result.apply_coap_result(request);
        request.response.as_mut().map(|response| {
            response.set_status(self.status);
        });
    }
}

impl CoapResult for Status {
    fn apply_coap_result<S>(self, request: &mut CoapRequest<S>) {
        request.response.as_mut().map(|response| {
            response.set_status(self);
        });
    }
}

impl CoapResult for Vec<u8> {
    fn apply_coap_result<S>(self, request: &mut CoapRequest<S>) {
        request.response.as_mut().map(|response| {
            response.set_status(Status::Changed);
            response.message.payload = self;
        });
    }
}

#[macro_export]
macro_rules! deser_path {
    ($request:ident, $index:literal) => {
        match $request.get_path_part($index) {
            Ok(part) => match part.parse() {
                Ok(value) => value,
                Err(e) => {
                    let r: Result<(), _> = Err(e);
                    r.apply_coap_result(&mut $request);
                    return $request;
                }
            },
            Err(e) => {
                let r: Result<(), _> = Err(e);
                r.apply_coap_result(&mut $request);
                return $request;
            }
        }
    };
}

#[macro_export]
macro_rules! deser_payload {
    ($request:ident) => {
        match ::ciborium::from_reader($request.message.payload.as_slice()) {
            Ok(payload) => payload,
            Err(e) => {{
                use ciborium::cbor;
                $request.response.as_mut().map(|response| {
                    response.set_status(::coap_lite::ResponseType::BadRequest);
                    ::ciborium::into_writer(
                        &ciborium::cbor!({"error" => e.to_string()}).unwrap(),
                        &mut response.message.payload,
                    )
                    .unwrap_or_default();
                });
                return $request;
            }}
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! routes {
    (
        $request:ident;
        $($Method:ident $path:literal => $route:expr);+
    ) => {{
        let expected_path = $request.get_path_part(0).unwrap_or_default();
        match ($request.get_method(), expected_path.as_str()) {
            $((&::coap_lite::RequestType::$Method, $path) => {
                let result = $route;
                result.apply_coap_result(&mut $request);
            },)*
            (method, path) => {
                let result = $crate::coap::WithStatus {
                    status: ::coap_lite::ResponseType::MethodNotAllowed,
                    result: Err($crate::coap::GenericError::from(::std::format!("{method:?} {path}"))) as Result<(), _>,
                };
                result.apply_coap_result(&mut $request)
            },
        }
        $request
    }};
}
pub use deser_path;
pub use deser_payload;
pub use routes;
