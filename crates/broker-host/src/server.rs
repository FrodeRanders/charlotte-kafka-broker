//! Plain TCP front end for the broker engine.
//!
//! The listener accepts Kafka connections, reads one length-prefixed request
//! frame at a time, hands it to the engine, and writes the length-prefixed
//! response. The accept loop is non-blocking with a short sleep so the server
//! can shut down deterministically in tests; connection handling uses one
//! thread per connection.
//!
//! This front end is for host development and conformance testing. The EL0
//! service will reuse the engine with a `tcpip` socket capability instead.

use std::{
    io::{
        self,
        Read,
        Write,
    },
    net::{
        SocketAddr,
        TcpListener,
        TcpStream,
    },
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    thread::{
        self,
        JoinHandle,
    },
    time::Duration,
};

use broker_engine::Engine;
use broker_wire::protocol::MAX_FRAME_LEN;

const ACCEPT_BACKOFF: Duration = Duration::from_millis(1);

/// A running TCP broker front end.
pub struct ServerHandle {
    local_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl ServerHandle {
    /// The address the listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting connections and joins the accept thread.
    ///
    /// Connection threads finish on their own when clients close; they are
    /// detached rather than joined so shutdown cannot block on a stalled
    /// client.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Binds `listen`, starts accepting, and returns a shutdown handle.
///
/// # Errors
///
/// Returns the bind or thread-spawn error.
pub fn start(engine: Arc<Engine>, listen: SocketAddr) -> io::Result<ServerHandle> {
    let listener = TcpListener::bind(listen)?;
    let local_addr = listener.local_addr()?;
    listener.set_nonblocking(true)?;

    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let accept = thread::Builder::new()
        .name("broker-accept".into())
        .spawn(move || accept_loop(listener, engine, thread_stop))?;

    Ok(ServerHandle {
        local_addr,
        stop,
        accept: Some(accept),
    })
}

fn accept_loop(listener: TcpListener, engine: Arc<Engine>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                let engine = Arc::clone(&engine);
                let _ = thread::Builder::new()
                    .name("broker-conn".into())
                    .spawn(move || serve_connection(stream, &engine));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_BACKOFF)
            }
            Err(_) => break,
        }
    }
}

fn serve_connection(mut stream: TcpStream, engine: &Engine) {
    let _ = stream.set_nodelay(true);
    // BSD-derived platforms may hand accept() a socket that inherited the
    // listener's non-blocking flag; connection reads must block.
    if stream.set_nonblocking(false).is_err() {
        return;
    }
    loop {
        let mut prefix = [0u8; 4];
        if stream.read_exact(&mut prefix).is_err() {
            return;
        }
        let length = i32::from_be_bytes(prefix);
        if length <= 0 || length as usize > MAX_FRAME_LEN {
            return;
        }

        let mut frame = vec![0u8; 4 + length as usize];
        frame[..4].copy_from_slice(&prefix);
        if stream.read_exact(&mut frame[4..]).is_err() {
            return;
        }

        let Ok(response) = engine.handle_frame(&frame) else {
            return;
        };
        if stream.write_all(&response).is_err() {
            return;
        }
        let _ = stream.flush();
    }
}
