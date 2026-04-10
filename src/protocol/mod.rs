// protocol module contains the code for encoding and decoding the Redis protocol

use crate::{command::Command, errors::RedisError};

pub mod decoder;
pub mod encoder;
pub mod hash;
pub mod list;
pub mod set;
pub mod sorted_set;

pub type CommandResult = Result<Command, RedisError>;
