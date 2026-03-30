use futures::{SinkExt, StreamExt};
use redis_protocol::{
    codec::{Resp2, resp2_encode_command},
    resp2::types::BytesFrame,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{Layer as _, fmt::Layer, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let layer = Layer::new().with_filter(LevelFilter::INFO);
    tracing_subscriber::registry().with(layer).init();

    // Connect to Redis server
    let stream = TcpStream::connect("127.0.0.1:6869").await?;

    // Create framed stream with our RESP codec
    let mut framed = Framed::new(stream, Resp2::default());

    send_ttl_to_key(&mut framed).await?;

    Ok(())
}

async fn send_ttl_to_key(
    framed: &mut Framed<TcpStream, Resp2>,
) -> Result<(), Box<dyn std::error::Error>> {
    send_cmds(framed, vec!["TTL anotherkey", "TYPE anotherkey"]).await?;
    Ok(())
}

async fn send_string_cmds(
    framed: &mut Framed<TcpStream, Resp2>,
) -> Result<(), Box<dyn std::error::Error>> {
    send_cmds(
        framed,
        vec![
            "SET mykey myvalue",
            "GET mykey",
            "SET anotherkey anothervalue EX 600 NX GET",
            "GET anotherkey",
            "MSET key1 value1 key2 value2",
            "MGET key1 key2 key3", // key3 does not exist
        ],
    )
    .await?;
    Ok(())
}

async fn send_set_cmds(
    framed: &mut Framed<TcpStream, Resp2>,
) -> Result<(), Box<dyn std::error::Error>> {
    send_cmds(
        framed,
        vec![
            "SADD myset hello",
            "SADD myset world",
            "SADD myset hello", // duplicate
            "SCARD myset",
            "SMEMBERS myset",
            "SISMEMBER myset hello",
            "SISMEMBER myset foo",
            "SREM myset hello",
            "SMEMBERS myset",
        ],
    )
    .await?;
    Ok(())
}

async fn send_basic_cmds(
    framed: &mut Framed<TcpStream, Resp2>,
) -> Result<(), Box<dyn std::error::Error>> {
    send_cmds(
        framed,
        vec![
            "SADD myset hello world",
            "SET mykey myvalue",
            "HSET myhash field1 value1",
            "LPUSH mylist value1 value2",
            "ZADD myzset 1 one 2 two 3 three",
            "PING",
            "PING Hello,World!",
            "ECHO Hello,Echo!",
            "EXISTS mykey myset myhash mylist myzset",
            "TYPE mylist",
            "KEYS my*",
        ],
    )
    .await?;
    Ok(())
}

async fn send_hash_cmds(
    framed: &mut Framed<TcpStream, Resp2>,
) -> Result<(), Box<dyn std::error::Error>> {
    send_cmds(
        framed,
        vec![
            "HSET myhash field1 value1",
            "HSET myhash field2 value2",
            "HGET myhash field1",
            "HGET myhash field2",
            "HGET myhash field3", // non-existing field
            "HMSET alice:1 name Alice age 30 city Wonderland",
            "HMGET alice:1 name age city country", // country does not exist
            "HLEN myhash",
            "HKEYS myhash",
            "HVALS myhash",
            "HGETALL myhash",
            "HEXISTS myhash field1",
            "HEXISTS myhash field3",
            "HDEL myhash field1",
            "HGETALL myhash",
        ],
    )
    .await?;

    Ok(())
}

async fn send_sorted_set_cmds(
    framed: &mut Framed<TcpStream, Resp2>,
) -> Result<(), Box<dyn std::error::Error>> {
    send_cmds(
        framed,
        vec![
            "ZADD myzset 1 one",
            "ZADD myzset 2 two",
            "ZADD myzset 3 three",
            "ZCARD myzset",
            "ZRANGE myzset 0 -1 WITHSCORES",
            "ZRANGE myzset 0 1",
            "ZREM myzset two",
            "ZRANGE myzset 0 -1 WITHSCORES",
        ],
    )
    .await?;
    Ok(())
}

async fn send_cmds(
    framed: &mut Framed<TcpStream, Resp2>,
    cmds: Vec<&'static str>,
) -> Result<(), Box<dyn std::error::Error>> {
    for cmd_str in cmds {
        let cmd = resp2_encode_command(cmd_str);

        framed.send(cmd.clone()).await?;
        // Read the response
        if let Some(response) = framed.next().await {
            match response? {
                BytesFrame::Array(data) => info!("Cmd: {:?}, Received: {:?}", cmd, data),
                BytesFrame::BulkString(data) => info!("Cmd: {:?}, Received: {:?}", cmd, data),
                BytesFrame::Error(e) => println!("Error: {}", e),
                other => info!("Cmd: {:?}, Received: {:?}", cmd, other),
            }
        }
    }

    Ok(())
}
