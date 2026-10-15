#![allow(dead_code)]

//! A Unix-domain socket in front of a TCP HTTP server, the shape of a local
//! gateway that exposes a server to a sandboxed client through one socket
//! path. Bytes pass through unchanged in both directions, so the client and
//! the server speak HTTP/1.1 to each other exactly as they would over TCP.

use std::io;
use std::net::{Shutdown, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{JoinHandle, sleep};
use std::time::Duration;

pub struct UnixGateway {
    pub socket: PathBuf,
    connections: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    _dir: tempfile::TempDir,
}

impl UnixGateway {
    /// Listen on a fresh socket and forward every connection to `origin`
    /// (`http://HOST:PORT`). The socket's directory name holds a space, as
    /// an application-support directory does, so a client must decode the
    /// percent-escaped path of its `unix://` URL.
    pub fn new(origin: &str) -> Self {
        let upstream = origin
            .strip_prefix("http://")
            .expect("the gateway forwards to a plain TCP origin")
            .trim_end_matches('/')
            .to_owned();
        // A socket path is bounded (104 bytes on macOS), so keep it short.
        let dir = tempfile::Builder::new().prefix("og").tempdir().unwrap();
        let parent = dir.path().join("gate way");
        std::fs::create_dir(&parent).unwrap();
        let socket = parent.join("s.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::spawn(move || {
            while !stopped.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((client, _)) => {
                        accepted.fetch_add(1, Ordering::SeqCst);
                        let upstream = upstream.clone();
                        std::thread::spawn(move || forward(client, &upstream));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("gateway accept: {error}"),
                }
            }
        });
        Self {
            socket,
            connections,
            stop,
            thread: Some(thread),
            _dir: dir,
        }
    }

    /// The `--server` value naming this socket, written as a user would type
    /// it: the raw path, space included.
    pub fn url(&self) -> String {
        format!("unix://{}", self.socket.display())
    }

    /// Connections accepted so far; each one carried client traffic.
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

/// Copy each direction until its sender finishes, then pass the half-close on,
/// so a response ending its connection still ends the client's read.
fn forward(client: UnixStream, upstream: &str) {
    // An accepted socket can inherit the listener's nonblocking mode.
    client.set_nonblocking(false).unwrap();
    let server = TcpStream::connect(upstream).unwrap();
    let (mut client_reader, mut server_writer) =
        (client.try_clone().unwrap(), server.try_clone().unwrap());
    let request = std::thread::spawn(move || {
        let _ = io::copy(&mut client_reader, &mut server_writer);
        let _ = server_writer.shutdown(Shutdown::Write);
    });
    let (mut server_reader, mut client_writer) = (server, client);
    let _ = io::copy(&mut server_reader, &mut client_writer);
    let _ = client_writer.shutdown(Shutdown::Write);
    let _ = request.join();
}

impl Drop for UnixGateway {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
