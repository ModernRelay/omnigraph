#![allow(dead_code)]

use serde_json::Value;
use std::io::{BufRead, BufReader, Read};
use std::thread::sleep;
use std::time::Duration;

/// A bounded HTTP fixture for actual CLI processes. It records every request,
/// including unexpected follow-up calls, without requiring a managed service.
pub struct IntentApiFixture {
    pub origin: String,
    requests: std::sync::Arc<std::sync::Mutex<Vec<IntentRequest>>>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    reply_count: usize,
}

#[derive(Debug, Clone)]
pub struct IntentRequest {
    pub method: String,
    pub path: String,
    pub headers: std::collections::BTreeMap<String, String>,
    pub body: Value,
}

pub struct IntentReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl IntentReply {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }
}

impl IntentApiFixture {
    pub fn new(replies: Vec<IntentReply>) -> Self {
        use std::io::Write;
        use std::sync::atomic::Ordering;
        use std::sync::{Arc, Mutex};

        let reply_count = replies.len();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut replies = std::collections::VecDeque::from(replies);
            while !stopped.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("fixture accept: {error}"),
                };
                // Accepted sockets can inherit nonblocking mode on BSD/macOS.
                // The listener polls, but each request uses bounded blocking I/O.
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let request_line: Vec<_> = line.split_whitespace().map(str::to_owned).collect();
                assert_eq!(request_line.len(), 3, "{request_line:?}");
                let mut headers = std::collections::BTreeMap::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    let (key, value) = line.trim_end().split_once(':').unwrap();
                    headers.insert(key.to_ascii_lowercase(), value.trim().to_string());
                    assert!(headers.len() < 100, "fixture request header bound");
                }
                let length = headers
                    .get("content-length")
                    .map_or(0, |n| n.parse::<usize>().unwrap());
                assert!(length <= 1024 * 1024, "fixture request body bound");
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                received.lock().unwrap().push(IntentRequest {
                    method: request_line[0].clone(),
                    path: request_line[1].clone(),
                    headers,
                    body: if body.is_empty() {
                        Value::Null
                    } else {
                        serde_json::from_slice(&body).unwrap()
                    },
                });
                let reply = replies.pop_front().unwrap_or_else(|| {
                    IntentReply::json(500, serde_json::json!({"type":"unexpected_request"}))
                });
                let mut response = format!(
                    "HTTP/1.1 {} Fixture\r\nConnection: close\r\nContent-Type: application/json\r\n",
                    reply.status
                );
                if !reply
                    .headers
                    .iter()
                    .any(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                {
                    response.push_str(&format!("Content-Length: {}\r\n", reply.body.len()));
                }
                for (name, value) in reply.headers {
                    response.push_str(&format!("{name}: {value}\r\n"));
                }
                response.push_str("\r\n");
                // Redirect/size refusals may close before reading the body.
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(&reply.body);
            }
        });
        Self {
            origin,
            requests,
            stop,
            thread: Some(thread),
            reply_count,
        }
    }

    pub fn requests(&self) -> Vec<IntentRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn assert_complete(&self) {
        assert_eq!(
            self.requests().len(),
            self.reply_count,
            "HTTP fixture request/reply count"
        );
    }
}

impl Drop for IntentApiFixture {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let result = thread.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}
