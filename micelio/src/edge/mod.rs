pub mod client;
pub mod fl_client;
pub mod ml_registry;

use coap_lite::CoapRequest;
use coap_lite::RequestType as Method;
use serde::Serialize;

pub(crate) fn hello_msg<T: Serialize>(payload: &T) -> Vec<u8> {
    let mut hello: CoapRequest<()> = CoapRequest::new();
    hello.set_method(Method::Put);
    hello.set_path("connection");
    ciborium::into_writer(payload, &mut hello.message.payload)
        .expect("message should be correctly encoded");
    hello
        .message
        .to_bytes()
        .expect("message should be correctly encoded")
}
