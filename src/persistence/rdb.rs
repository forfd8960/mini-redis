use std::{fs, path::PathBuf};

use crc::Crc;
use dashmap::DashMap;

use crate::{
    errors::RedisError,
    value::{
        RedisValue, StringValue, hash::HashValue, list::ListValue, set::SetValue,
        sorted_set::SortedSetValue,
    },
};

/*
[52 45 44 49 53]          # Magic string "REDIS"
[version: 4 bytes]        # RDB version, e.g., "0011" (version 11)
[aux fields]              # Metadata (redis-ver, used-mem, etc.)
[DB selector + key-value pairs]*  # One or more databases
[EOF opcode]              # 0xFF
[8-byte CRC64 checksum]  # Integrity check
*/

pub fn write_rdb(
    data: DashMap<String, RedisValue>,
    expire_table: DashMap<String, u64>,
    file: PathBuf,
) -> Result<(), RedisError> {
    let mut buf = Vec::new();

    encode_redis_version(&mut buf);
    encode_aux_field(&mut buf);
    encode_db_selector_and_redize(&mut buf, data.len(), expire_table.len());
    encode_key_val_entries(&mut buf, data);

    buf.push(0xFF);

    let crc64 = calculate_crc64(&buf);
    buf.extend_from_slice(&crc64.to_be_bytes());

    fs::write(file, &buf)?;
    Ok(())
}

fn encode_redis_version(buf: &mut Vec<u8>) {
    buf.extend_from_slice("REDIS".as_bytes());
    buf.extend_from_slice("0011".as_bytes());
}

fn encode_length(buf: &mut Vec<u8>, length: usize) {
    if length < 64 {
        buf.push(length as u8);
    } else if length < 16384 {
        buf.push(((length >> 8) as u8) | 0x40);
        buf.push((length & 0xFF) as u8);
    } else {
        buf.push(0x80);
        buf.extend_from_slice(&(length as u32).to_be_bytes());
    }
}

fn encode_aux_field(buf: &mut Vec<u8>) {
    // write 0xFA
    buf.push(0xFA);

    // write redis-ver
    encode_string(buf, "redis-ver");
    encode_string(buf, "0.0.1");

    //write used-mem
    encode_string(buf, "used-mem");
    encode_string(buf, "0");
}

fn encode_db_selector_and_redize(buf: &mut Vec<u8>, table_size: usize, expire_tb_size: usize) {
    buf.push(0xFE);
    buf.push(0x00);

    buf.push(0xFB);
    encode_length(buf, table_size);
    encode_length(buf, expire_tb_size);
}

fn encode_key_val_entries(buf: &mut Vec<u8>, data: DashMap<String, RedisValue>) {
    for entry in data.iter() {
        let key = entry.key();
        let value = entry.value();

        match value {
            RedisValue::String(val) => {
                buf.push(0x00);
                encode_string(buf, key);
                match val {
                    StringValue::Int(iv) => encode_string(buf, &iv.to_string()),
                    StringValue::Raw(rv) => encode_string(buf, rv),
                }
            }

            RedisValue::List(list) => {
                buf.push(0x01);
                encode_string(buf, key);
                encode_list(buf, list);
            }

            RedisValue::Set(set) => {
                buf.push(0x02);
                encode_string(buf, key);
                encode_set(buf, set);
            }

            RedisValue::SortedSet(set) => {
                buf.push(0x03);
                encode_string(buf, key);
                encode_sorted_set(buf, set);
            }

            RedisValue::Hash(hash) => {
                buf.push(0x04);
                encode_string(buf, key);
                encode_Hash(buf, hash);
            }
            RedisValue::Nil => {}
        }
    }
}

fn encode_string(buf: &mut Vec<u8>, value: &str) {
    encode_length(buf, value.len());
    buf.extend_from_slice(value.as_bytes());
}

fn encode_list(buf: &mut Vec<u8>, list_val: &ListValue) {
    buf.push(0x13);
    encode_length(buf, list_val.len());
    for item in &list_val.items {
        encode_string(buf, &item);
    }
}

fn encode_Hash(buf: &mut Vec<u8>, hash_val: &HashValue) {
    encode_length(buf, hash_val.len());
    for (key, value) in &hash_val.items {
        encode_string_key_value(buf, &key, &value);
    }
}

fn encode_set(buf: &mut Vec<u8>, set_val: &SetValue) {
    encode_length(buf, set_val.len());
    for item in &set_val.items {
        encode_string(buf, item);
    }
}

fn encode_sorted_set(buf: &mut Vec<u8>, sorted_set_val: &SortedSetValue) {
    encode_length(buf, sorted_set_val.len());
    for (member, score) in &sorted_set_val.members {
        encode_string(buf, member);
        encode_string(buf, &score.to_string());
    }
}

fn encode_string_key_value(buf: &mut Vec<u8>, key: &str, value: &str) {
    encode_string(buf, key);
    encode_string(buf, value);
}

fn calculate_crc64(buf: &Vec<u8>) -> u64 {
    Crc::<u64>::new(&crc::CRC_64_REDIS).checksum(buf)
}
