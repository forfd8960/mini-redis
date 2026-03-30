use redis_protocol::resp2::types::{BytesFrame, OwnedFrame};
use tokio_util::bytes::Bytes;

pub fn encode_response(_frame: OwnedFrame) -> BytesFrame {
    BytesFrame::SimpleString(Bytes::from(format!("{}", "OK")))
}
