use anyhow;
use futures::{SinkExt, StreamExt};
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use rustyline;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

fn main() -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut rl = rustyline::DefaultEditor::new()?;
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                println!("Input: {}", line);

                let trimmed_cmds = trim_cmds(&line);
                println!("Trimmed commands: {:?}", trimmed_cmds);
                call_redis_server(&rt, trimmed_cmds);
            }
            Err(_) => break,
        }
    }

    Ok(())
}

// lpush words "today is good day" "keep going" -> ["lpush", "words", "today is good day", "keep going"]
fn trim_cmds(cmds_str: &str) -> Vec<String> {
    let mut cmds = Vec::new();
    let mut current_cmd = String::new();

    let mut idx = 0;
    loop {
        match cmds_str.chars().nth(idx) {
            Some(c) => {
                if c == '\\' && idx + 1 < cmds_str.len() {
                    // Handle escaped characters
                    if let Some(next_char) = cmds_str.chars().nth(idx + 1) {
                        current_cmd.push(next_char);
                        idx += 2; // Skip the escape character and the next character
                        continue;
                    }
                }

                if c == '"' {
                    let mut start = idx + 1;
                    let mut start_c = cmds_str.chars().nth(start);
                    while start < cmds_str.len() && start_c != Some('"') {
                        current_cmd.push(start_c.unwrap());
                        start += 1;
                        if start < cmds_str.len() {
                            start_c = cmds_str.chars().nth(start);
                        }
                    }

                    if start >= cmds_str.len() && start_c != Some('"') {
                        eprintln!("bad command: missing closing quote");
                        break;
                    }

                    if start < cmds_str.len() && start_c == Some('"') {
                        // Skip the closing quote
                        start += 1;
                    }

                    cmds.push(current_cmd.clone());
                    current_cmd.clear();
                    idx = start; // Move the main index to the end of the quoted section
                    continue;
                } else if c.is_whitespace() {
                    if !current_cmd.is_empty() {
                        cmds.push(current_cmd.clone());
                        current_cmd.clear();
                    }
                    idx += 1;
                } else {
                    current_cmd.push(c);
                    idx += 1;
                }
            }
            None => break,
        }
    }

    if !current_cmd.is_empty() {
        cmds.push(current_cmd);
    }

    cmds.into_iter().map(|s| s.trim().to_string()).collect()
}

fn call_redis_server<'a>(rt: &tokio::runtime::Runtime, cmds: Vec<String>) {
    let _ = rt.block_on(async {
        // Connect to Redis server
        let stream = TcpStream::connect("127.0.0.1:6869").await;
        match stream {
            Ok(s) => {
                // Create framed stream with our RESP codec
                let mut framed = Framed::new(s, Resp2::default());
                match send_cmds(&mut framed, cmds).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error sending commands: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("Failed to connect to Redis server: {}", e);
                return;
            }
        }
    });
}

async fn send_cmds<'a>(
    framed: &mut Framed<TcpStream, Resp2>,
    cmds: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cmd = BytesFrame::Array(
        cmds.into_iter()
            .map(|s| BytesFrame::BulkString(s.as_bytes().to_vec().into()))
            .collect(),
    );

    framed.send(cmd.clone()).await?;
    // Read the response
    if let Some(response) = framed.next().await {
        print_result(response?, false, 0);
    }

    Ok(())
}

fn print_result(response: BytesFrame, with_idx: bool, idx: i64) {
    match response {
        BytesFrame::Array(data) => {
            for (i, item) in data.into_iter().enumerate() {
                print_result(item, true, (i + 1) as i64);
            }
        }

        BytesFrame::BulkString(data) => {
            if with_idx {
                println!("{}): \"{}\"", idx, String::from_utf8_lossy(&data));
            } else {
                println!("\"{}\"", String::from_utf8_lossy(&data));
            }
        }
        BytesFrame::Error(e) => {
            if with_idx {
                println!("{}): {}", idx, e);
            } else {
                println!("{}", e);
            }
        }
        BytesFrame::Integer(intg) => {
            if with_idx {
                println!("{}): {}", idx, intg);
            } else {
                println!("{}", intg);
            }
        }
        BytesFrame::SimpleString(s) => {
            if with_idx {
                println!("{}): \"{}\"", idx, String::from_utf8_lossy(&s));
            } else {
                println!("\"{}\"", String::from_utf8_lossy(&s));
            }
        }
        BytesFrame::Null => {
            if with_idx {
                println!("{}): (nil)", idx);
            } else {
                println!("(nil)");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_cmds() {
        let input = r#"lpush words "today is good day" "keep going""#;
        let expected = vec![
            "lpush".to_string(),
            "words".to_string(),
            "today is good day".to_string(),
            "keep going".to_string(),
        ];
        assert_eq!(trim_cmds(input), expected);
    }

    #[test]
    fn test_trim_cmds_with_extra_spaces() {
        let input = r#"lpush content-list "building mini redis" "building mini terminal" "building rate limiter""#;
        let expected = vec![
            "lpush".to_string(),
            "content-list".to_string(),
            "building mini redis".to_string(),
            "building mini terminal".to_string(),
            "building rate limiter".to_string(),
        ];
        assert_eq!(trim_cmds(input), expected);
    }

    #[test]
    fn test_trim_cmds_lrange() {
        let input = r#"lrange content 0 -1"#;
        println!("Input: {}", input.len());

        let expected = vec![
            "lrange".to_string(),
            "content".to_string(),
            "0".to_string(),
            "-1".to_string(),
        ];
        assert_eq!(trim_cmds(input), expected);
    }

    #[test]
    fn test_trim_cmds_with_escaped_quotes() {
        let input = r#"set key "value with \"escaped quotes\"""#;
        let expected = vec![
            "set".to_string(),
            "key".to_string(),
            r#"value with "escaped quotes""#.to_string(),
        ];
        assert_eq!(trim_cmds(input), expected);
    }
}
