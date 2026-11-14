//! Bounded CQ-driven Kafka demo. Socket borrows stay in the application scope.
#![cfg_attr(not(test), no_std)]
#![cfg_attr(not(test), no_main)]
extern crate alloc;
use alloc::{
    vec,
    vec::Vec,
};
use core::time::Duration;

use broker_engine::{
    BrokerIdentity,
    Engine,
    EngineConfig,
};
use broker_runtime::{
    Broker,
    BrokerConfig,
    TopicSpec,
};
use charlotte_app::{
    AppExit,
    Application,
    Context,
    deadline::Deadline,
    logln,
    owned::{
        CallResult,
        Connection,
        Endpoint,
        PendingCall,
    },
    socket::{
        self,
        OwnedSocket,
        SocketError,
    },
    socket_send,
};
use charlotte_launch::deployment;
use sitas_charlotte::CharlotteReactor;
use sitas_core::reactor_backend::ReactorBackend;

const PORT: u16 = 9092;
const MAX_SESSIONS: usize = 8;
const MAX_FRAME: usize = 1024 * 1024;
const REQUEST_MS: u64 = 30_000;
const STARTUP_MS: u64 = 5_000;
const ADVERTISE_HOST: &[u8] = match option_env!("BROKER_ADVERTISE_HOST") {
    Some(host) => host.as_bytes(),
    None => b"0.0.0.0",
};
const ADVERTISE_PORT: i32 = match option_env!("BROKER_ADVERTISE_PORT") {
    Some(port) => decimal_port(port),
    None => PORT as i32,
};
const fn decimal_port(text: &str) -> i32 {
    let bytes = text.as_bytes();
    let mut value = 0i32;
    let mut index = 0;
    while index < bytes.len() {
        let digit = bytes[index];
        if digit < b'0' || digit > b'9' || value > 6553 {
            return PORT as i32;
        }
        value = value * 10 + (digit - b'0') as i32;
        index += 1;
    }
    if value > 0 && value <= 65535 {
        value
    } else {
        PORT as i32
    }
}
#[derive(Debug)]
pub enum Error {
    Launch(charlotte_app::LaunchError),
    Broker(broker_runtime::BrokerError),
    Grant(charlotte_app::grant_client::Error),
    Socket(SocketError),
    Ipc(catten_rt::owned::IpcError),
    Memory(catten_rt::owned::MemoryError),
    Observation(catten_rt::call::ObservationError),
    InvalidReply,
    Capacity,
}
impl From<SocketError> for Error {
    fn from(e: SocketError) -> Self {
        Self::Socket(e)
    }
}

pub fn run(ctx: &Context) -> Result<AppExit, Error> {
    let app = Application::from_context(ctx).map_err(Error::Launch)?;
    let tcp = app
        .acquire(b"tcpip", deployment::CLIENT_RIGHTS, &Deadline::after(STARTUP_MS))
        .map_err(Error::Grant)?;
    let reactor = CharlotteReactor::new(0);
    let broker = Broker::start(
        &reactor,
        BrokerConfig::new(
            2,
            vec![TopicSpec {
                name: b"events".to_vec(),
                partitions: 4,
            }],
        )
        .with_partition_max_bytes(64 * 1024),
    )
    .map_err(Error::Broker)?;
    let engine = Engine::new(
        broker,
        EngineConfig::new(BrokerIdentity::new(0, ADVERTISE_HOST, ADVERTISE_PORT)),
    );
    let endpoint = Endpoint::create(charlotte_app::name(b"KBRK"), 1, 4).map_err(Error::Ipc)?;
    let outcome = serve(&app, &reactor, &engine, &tcp, &endpoint);
    // Network scopes have cancelled accepted calls and closed sockets first.
    // A failed drain retains the engine/worker owners through domain abort.
    if let Err(error) = engine.broker().shutdown() {
        logln!("[broker] worker drain rejected: {:?}", error);
        catten_rt::domain_abort();
    }
    logln!("[broker] partition workers joined; network scope released");
    outcome
}

fn wait(reactor: &CharlotteReactor, timeout_ms: Option<u64>) {
    if let Err(error) = reactor.wait(&[], &[], timeout_ms.map(Duration::from_millis)) {
        logln!("[broker] CQ wait rejected: {:?}", error);
        catten_rt::domain_abort();
    }
}
fn start_listener<'a>(
    tcp: &'a Connection,
    reactor: &CharlotteReactor,
) -> Result<OwnedSocket<'a>, Error> {
    let deadline = Deadline::after(STARTUP_MS);
    let mut opening =
        OwnedSocket::open_call(tcp.as_ref(), socket::DOMAIN_TCP).map_err(Error::Ipc)?;
    opening.bind_cq(0).map_err(Error::Ipc)?;
    let socket = loop {
        if let Some(socket) = opening.poll()? {
            break socket;
        }
        if deadline.expired() {
            // Remote creation is uncertain: keep the entire opening owner.
            catten_rt::domain_abort();
        }
        wait(reactor, Some(deadline.remaining_ms()));
    };
    let pending = socket.listen_call(PORT)?;
    let reply = pending.observe_until(&deadline).cancel_on_failure().map_err(Error::Observation)?;
    scalar_zero(reply)?;
    Ok(socket)
}
fn scalar_zero(reply: CallResult) -> Result<(), Error> {
    if reply.result != 0 || reply.memory.is_some() || reply.connection.is_some() {
        Err(Error::InvalidReply)
    } else {
        Ok(())
    }
}

// Field order cancels the accepted call before closing its socket.
struct Accepting<'a> {
    pending: PendingCall<'static>,
    socket: OwnedSocket<'a>,
}
fn serve(
    app: &Application<'_>,
    reactor: &CharlotteReactor,
    engine: &Engine,
    tcp: &Connection,
    endpoint: &Endpoint,
) -> Result<AppExit, Error> {
    let mut sessions = Vec::new();
    sessions.try_reserve_exact(MAX_SESSIONS).map_err(|_| Error::Capacity)?;
    let name = app.artifact_name().map_err(Error::Grant)?;
    let mut accepting: Option<Accepting<'_>> = None;
    let mut published = false;
    loop {
        if let Some(request) = app.context().lifecycle().shutdown_requested() {
            // Declaration order is not a shutdown protocol: drop explicitly.
            drop(accepting);
            drop(sessions);
            return Ok(AppExit::Shutdown(request));
        }
        if accepting.is_none() && sessions.len() < MAX_SESSIONS {
            let socket = start_listener(tcp, reactor)?;
            if !published {
                app.publish(&name, endpoint, &Deadline::after(STARTUP_MS)).map_err(Error::Grant)?;
                published = true;
                logln!("[broker] listening on tcp port {}; readiness published", PORT);
            }
            let mut pending = socket.accept_call().map_err(Error::Ipc)?;
            pending.bind_cq(0).map_err(Error::Ipc)?;
            accepting = Some(Accepting {
                pending,
                socket,
            });
        }
        let mut progressed = false;
        if let Some(listener) = accepting.as_mut() {
            if let Some(reply) = listener.pending.poll().map_err(Error::Ipc)? {
                scalar_zero(reply)?;
                let listener = accepting.take().expect("observed listener");
                sessions.push(Session::new(listener.socket));
                logln!("[broker] accepted connection; active={}", sessions.len());
                progressed = true;
            }
        }
        let mut index = 0;
        while index < sessions.len() {
            match sessions[index].progress(engine) {
                Ok(Progress::Pending) => index += 1,
                Ok(Progress::Advanced) => {
                    progressed = true;
                    index += 1;
                }
                Ok(Progress::Closed) => {
                    drop(sessions.swap_remove(index));
                    progressed = true;
                }
                Err(error) => {
                    logln!("[broker] session closed: {:?}", error);
                    drop(sessions.swap_remove(index));
                    progressed = true;
                }
            }
        }
        if progressed {
            continue;
        }
        // Backend is the sole CQ0 ring consumer. Hints never adopt results.
        let timeout = sessions.iter().map(|s| s.deadline.remaining_ms()).min();
        wait(reactor, timeout);
    }
}

enum Progress {
    Pending,
    Advanced,
    Closed,
}
enum Phase {
    Receive,
    Send {
        offered: usize,
    },
}
struct Session<'a> {
    pending: Option<PendingCall<'static>>,
    socket: OwnedSocket<'a>,
    input: Vec<u8>,
    response: Vec<u8>,
    sent: usize,
    phase: Phase,
    deadline: Deadline,
}
impl<'a> Session<'a> {
    fn new(socket: OwnedSocket<'a>) -> Self {
        Self {
            pending: None,
            socket,
            input: Vec::new(),
            response: Vec::new(),
            sent: 0,
            phase: Phase::Receive,
            deadline: Deadline::after(REQUEST_MS),
        }
    }

    fn progress(&mut self, engine: &Engine) -> Result<Progress, Error> {
        // Recheck terminal state before applying a local deadline.
        if let Some(pending) = self.pending.as_mut() {
            if let Some(reply) = pending.poll().map_err(Error::Ipc)? {
                self.pending = None;
                match self.phase {
                    Phase::Receive => {
                        let Some(chunk) = OwnedSocket::decode_receive(reply)? else {
                            return Ok(Progress::Closed);
                        };
                        let (memory, len) = chunk.into_parts();
                        let mapping =
                            memory.map_read_only().map_err(|(_, error)| Error::Memory(error))?;
                        if self.input.len().saturating_add(len) > MAX_FRAME + 4 {
                            return Err(Error::Capacity);
                        }
                        self.input.try_reserve(len).map_err(|_| Error::Capacity)?;
                        self.input.extend_from_slice(&mapping.as_slice()[..len]);
                    }
                    Phase::Send {
                        offered,
                    } => {
                        if reply.memory.is_some()
                            || reply.connection.is_some()
                            || reply.result <= 0
                            || reply.result as usize > offered
                        {
                            return Err(Error::InvalidReply);
                        }
                        self.sent += reply.result as usize;
                        if self.sent == self.response.len() {
                            self.response.clear();
                            self.sent = 0;
                            self.phase = Phase::Receive;
                            self.deadline = Deadline::after(REQUEST_MS);
                        }
                    }
                }
                return Ok(Progress::Advanced);
            }
            return Ok(if self.deadline.expired() {
                Progress::Closed
            } else {
                Progress::Pending
            });
        }
        if self.deadline.expired() {
            return Ok(Progress::Closed);
        }
        if matches!(self.phase, Phase::Receive) {
            if let Some(total) = complete_frame(&self.input)? {
                self.response =
                    engine.handle_frame(&self.input[..total]).map_err(|_| Error::InvalidReply)?;
                self.input.drain(..total);
                self.phase = Phase::Send {
                    offered: 0,
                };
            }
        }
        let mut pending = match self.phase {
            Phase::Receive => self.socket.receive_call()?,
            Phase::Send {
                ..
            } => {
                let length = (self.response.len() - self.sent).min(socket_send::MAX_PAYLOAD);
                self.phase = Phase::Send {
                    offered: length,
                };
                self.socket
                    .send_call(&self.response[self.sent..self.sent + length], &self.deadline)?
            }
        };
        pending.bind_cq(0).map_err(Error::Ipc)?;
        self.pending = Some(pending);
        Ok(Progress::Advanced)
    }
}
#[cfg(not(test))]
charlotte_app::entry!(run);

fn complete_frame(input: &[u8]) -> Result<Option<usize>, Error> {
    let Some(header) = input.get(..4) else {
        return Ok(None);
    };
    let length = i32::from_be_bytes(header.try_into().unwrap());
    if length <= 0 || length as usize > MAX_FRAME {
        return Err(Error::InvalidReply);
    }
    let total = 4 + length as usize;
    Ok((input.len() >= total).then_some(total))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_and_pipelined_frames_keep_their_boundaries() {
        assert!(complete_frame(&[]).unwrap().is_none());
        assert!(complete_frame(&[0, 0, 0]).unwrap().is_none());
        assert!(complete_frame(&[0, 0, 0, 2, 9]).unwrap().is_none());
        assert_eq!(complete_frame(&[0, 0, 0, 2, 9, 8, 0, 0, 0, 1, 7]).unwrap(), Some(6));
    }
    #[test]
    fn invalid_frame_lengths_reject_before_buffer_growth() {
        for length in [0i32, -1, MAX_FRAME as i32 + 1, i32::MAX] {
            assert!(complete_frame(&length.to_be_bytes()).is_err());
        }
    }
    #[test]
    fn advertised_ports_reject_overflow_and_non_decimal_input() {
        assert_eq!(decimal_port("19099"), 19099);
        for port in ["", "0", "65536", "999999999999999999999", "90x2"] {
            assert_eq!(decimal_port(port), PORT as i32);
        }
    }
}
