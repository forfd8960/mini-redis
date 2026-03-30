#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    #[error("Command error: {0}")]
    CommandError(String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Persistence error: {0}")]
    PersistenceError(String),
}
