use crate::{command::{BasicCommand, Command, SetOptions, StringCommand}, errors::RedisError};


// decode commands from the Redis protocol
pub fn decode_frame(frame: &str) -> Result<Command, RedisError> {
    unimplemented!()
}

fn decode_string_command(parts: &[&str]) -> Result<Command, RedisError> {
    unimplemented!()
}

fn decode_hash_command(parts: &[&str]) -> Result<Command, RedisError> {
    unimplemented!()
}

fn decode_list_command(parts: &[&str]) -> Result<Command, RedisError> {
    unimplemented!()
}

fn decode_set_command(parts: &[&str]) -> Result<Command, RedisError> {
    unimplemented!()
}

fn decode_sorted_set_command(parts: &[&str]) -> Result<Command, RedisError> {
    unimplemented!()
}