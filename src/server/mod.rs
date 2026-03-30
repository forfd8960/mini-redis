use anyhow::Result;
use futures::SinkExt;
use futures::StreamExt;
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::OwnedFrame;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;

use crate::protocol::decoder::decode_frame;
use crate::protocol::encoder::encode_response;

pub const INFO: &str = r#"
 ███████████████████████████████████████████████████████████████████████
 ██                                                                   ██
 ██   ███╗   ███╗██╗███╗   ██╗██╗                                     ██
 ██   ████╗ ████║██║████╗  ██║██║                                     ██
 ██   ██╔████╔██║██║██╔██╗ ██║██║                                     ██
 ██   ██║╚██╔╝██║██║██║╚██╗██║██║                                     ██
 ██   ██║ ╚═╝ ██║██║██║ ╚████║██║                                     ██
 ██   ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝                                     ██
 ██                                                                   ██
 ██   ██████╗ ███████╗██████╗ ██╗███████╗                             ██
 ██   ██╔══██╗██╔════╝██╔══██╗██║██╔════╝                             ██
 ██   ██████╔╝█████╗  ██║  ██║██║███████╗                             ██
 ██   ██╔══██╗██╔══╝  ██║  ██║██║╚════██║                             ██
 ██   ██║  ██║███████╗██████╔╝██║███████║                             ██
 ██   ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═╝╚══════╝                             ██
 ██                                                                   ██
 ███████████████████████████████████████████████████████████████████████

"#;

pub async fn run() -> Result<()> {
    let addr = "0.0.0.0:6869";
    let listener = TcpListener::bind(addr).await?;
    println!("Server is running on {}", addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New client connected: {}", addr);
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket).await {
                eprintln!("Error handling client {}: {:?}", addr, e);
            }
        });
    }
}

async fn handle_client(socket: TcpStream) -> Result<()> {
    let (read_half, write_half) = socket.into_split();

    let mut framed_read = FramedRead::new(read_half, Resp2::default());
    let mut framed_write = FramedWrite::new(write_half, Resp2::default());

    loop {
        match framed_read.next().await {
            Some(Ok(bs_frame)) => {
                let owned_cmd = bs_frame.to_owned_frame();
                match decode_frame(owned_cmd) {
                    Ok(cmd) => {
                        println!("Decoded command: {:?}", cmd);
                        // Here you would execute the command and generate a response
                        let encoded =
                            encode_response(OwnedFrame::SimpleString("OK".as_bytes().to_vec()));
                        framed_write.send(encoded).await?;
                    }
                    Err(e) => {
                        eprintln!("Error decoding command: {:?}", e);
                        let encoded = encode_response(OwnedFrame::Error("OK".to_string()));
                        framed_write.send(encoded).await?;
                        continue;
                    }
                }
            }
            Some(Err(e)) => {
                eprintln!("Error decoding command: {:?}", e);
                let encoded = encode_response(OwnedFrame::Error(e.to_string()));
                framed_write.send(encoded).await?;
            }
            None => {
                break; // client disconnected
            }
        }
    }

    Ok(())
}
