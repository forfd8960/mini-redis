use crate::value::{HashValue, ListValue, RedisValue, SetValue, SortedSetValue, StringValue};
use anyhow::{Result, anyhow};
use redis_protocol::resp2::types::BytesFrame;
use tokio_util::bytes::Bytes;

pub fn encode_ok() -> BytesFrame {
    BytesFrame::SimpleString(Bytes::from(format!("{}", "OK")))
}

pub fn encode_value(value: RedisValue) -> Result<BytesFrame> {
    match value {
        RedisValue::String(v) => Ok(encode_string(v)),
        RedisValue::List(v) => Ok(encode_list(v)),
        RedisValue::Set(v) => Ok(encode_set(v)),
        RedisValue::Hash(v) => Ok(encode_hash(v)),
        RedisValue::Nil => Ok(encode_nil()),
        _ => Err(anyhow!("{:?} not supported", value)),
    }
}

pub fn encode_simple_string(s: String) -> BytesFrame {
    BytesFrame::BulkString(s.into())
}

pub fn encode_simple_strings(s: Vec<String>) -> BytesFrame {
    BytesFrame::Array(
        s.into_iter()
            .map(|s| BytesFrame::BulkString(s.into()))
            .collect(),
    )
}

pub fn encode_string(s_v: StringValue) -> BytesFrame {
    match s_v {
        StringValue::Int(i) => encode_integer(i),
        StringValue::Raw(s) => BytesFrame::BulkString(s.into()),
    }
}

pub fn encode_strings(strs: Vec<Option<StringValue>>) -> BytesFrame {
    BytesFrame::Array(
        strs.into_iter()
            .map(|s| match s {
                Some(v) => encode_string(v),
                None => encode_nil(),
            })
            .collect(),
    )
}

pub fn encode_list(list_v: ListValue) -> BytesFrame {
    BytesFrame::Array(
        list_v
            .items
            .into_iter()
            .map(|e| BytesFrame::BulkString(e.into()))
            .collect(),
    )
}

pub fn encode_set(set_v: SetValue) -> BytesFrame {
    BytesFrame::Array(
        set_v
            .items
            .into_iter()
            .map(|(key, _)| BytesFrame::BulkString(key.into()))
            .collect(),
    )
}

pub fn encode_hash(hash_v: HashValue) -> BytesFrame {
    let mut arr = Vec::with_capacity(hash_v.items.len() * 2);
    for (k, v) in hash_v.items {
        arr.push(BytesFrame::BulkString(k.into()));
        arr.push(BytesFrame::BulkString(v.into()));
    }
    BytesFrame::Array(arr)
}

pub fn encode_integer(v: i64) -> BytesFrame {
    BytesFrame::Integer(v)
}

pub fn encode_error(err_msg: &str) -> BytesFrame {
    BytesFrame::Error(err_msg.into())
}

pub fn encode_nil() -> BytesFrame {
    BytesFrame::Null
}

pub fn encode_sorted_set(sorted_set: SortedSetValue) -> BytesFrame {
    let mut arr = Vec::with_capacity(sorted_set.members.len() * 2);
    for (member, score) in sorted_set.members {
        arr.push(BytesFrame::BulkString(member.into()));
        arr.push(BytesFrame::BulkString(score.to_string().into()));
    }
    BytesFrame::Array(arr)
}
