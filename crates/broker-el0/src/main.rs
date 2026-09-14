//! CharlotteOS EL0 Kafka-subset broker image.
//!
//! The image starts one broker over a `CharlotteReactor`, acquires only the
//! `tcpip` connection named by its signed deployment descriptor, publishes an
//! endpoint under its artifact name for readiness, and then serves Kafka
//! frames from accepted TCP connections through `broker-engine`.
//!
//! The service must be launched through the signed deployment path: a legacy
//! launch has no descriptor, so it only resolves `tcpip` through the name
//! service and cannot publish readiness.

#![no_std]
#![no_main]

extern crate alloc;

use alloc::{
    boxed::Box,
    sync::Arc,
    vec,
    vec::Vec,
};
use core::sync::atomic::{
    AtomicU64,
    Ordering,
};

use broker_engine::{
    BrokerIdentity,
    Engine,
    EngineConfig,
};
use broker_runtime::{
    Broker,
    BrokerConfig,
    SessionId,
    SessionShardLayout,
    TopicSpec,
};
use catten_rt::{
    Context,
    ShutdownRequest,
    domain_abort,
    logln,
    owned::{
        Connection,
        Endpoint,
        LaunchMemoryRef,
        OwnedMemory,
    },
};
use catten_services::{
    grant_client,
    sleep_ms,
    socket,
    socket::OwnedSocket,
    wait_for_registered_name_owned,
};
use charlotte_launch::deployment;
use sitas_charlotte::CharlotteReactor;
use sitas_core::{
    placement::ShardPlacement,
    shard_runtime::ShardRuntime,
};

const SHARD_COUNT: usize = 2;
/// Logical protocol-state shards. They may share LPs with partition shards;
/// partition ownership remains independent and is still selected per
/// `(topic, partition)` by `broker-runtime`.
const SESSION_SHARD_COUNT: usize = 2;
const LISTEN_PORT: u16 = 9092;
const ACCEPT_POLL_MS: u64 = 25;
const MAX_FRAME_LEN: usize = 1024 * 1024;
const TOPIC: &[u8] = b"events";
const PARTITIONS: i32 = 4;
const PARTITION_MAX_BYTES: usize = 64 * 1024;
const HEARTBEAT_POLLS: u64 = 400;
const INTERFACE: u64 = catten_services::name(b"KBRK");
const VERSION: u32 = 1;

const STAGE_ACCEPTED: u64 = 1;
const STAGE_RECEIVED: u64 = 2;
const STAGE_DISPATCHING: u64 = 3;
const STAGE_HANDLED: u64 = 4;
const STAGE_SENDING: u64 = 5;
const STAGE_SENT: u64 = 6;
const STAGE_RECEIVE_ERROR: u64 = 7;
const STAGE_ENGINE_ERROR: u64 = 8;
const STAGE_SEND_ERROR: u64 = 9;
const STAGE_CLOSED: u64 = 10;

#[derive(Default)]
struct Diagnostics {
    next_connection: AtomicU64,
    accepted: AtomicU64,
    active: AtomicU64,
    chunks: AtomicU64,
    frames: AtomicU64,
    handled: AtomicU64,
    sent: AtomicU64,
    receive_errors: AtomicU64,
    engine_errors: AtomicU64,
    send_errors: AtomicU64,
    progress: AtomicU64,
    last_connection: AtomicU64,
    last_stage: AtomicU64,
    last_api_key: AtomicU64,
    last_api_version: AtomicU64,
    last_correlation: AtomicU64,
}

impl Diagnostics {
    fn accepted(&self) -> u64 {
        let connection = self.next_connection.fetch_add(1, Ordering::Relaxed) + 1;
        self.accepted.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::Relaxed);
        self.mark(connection, STAGE_ACCEPTED);
        connection
    }

    fn mark(&self, connection: u64, stage: u64) {
        self.last_connection.store(connection, Ordering::Relaxed);
        self.last_stage.store(stage, Ordering::Relaxed);
        self.progress.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_frame(&self, connection: u64, frame: &[u8]) {
        if frame.len() >= 12 {
            self.last_api_key
                .store(u16::from_be_bytes([frame[4], frame[5]]).into(), Ordering::Relaxed);
            self.last_api_version
                .store(u16::from_be_bytes([frame[6], frame[7]]).into(), Ordering::Relaxed);
            self.last_correlation.store(
                u32::from_be_bytes([frame[8], frame[9], frame[10], frame[11]]).into(),
                Ordering::Relaxed,
            );
        }
        self.mark(connection, STAGE_DISPATCHING);
    }

    fn heartbeat(&self) {
        logln!(
            "[broker] hb accepted={} active={} chunks={} frames={} handled={} sent={} \
             errors={}/{}/{} progress={} last={}:{} api={}/{} corr={}",
            self.accepted.load(Ordering::Relaxed),
            self.active.load(Ordering::Relaxed),
            self.chunks.load(Ordering::Relaxed),
            self.frames.load(Ordering::Relaxed),
            self.handled.load(Ordering::Relaxed),
            self.sent.load(Ordering::Relaxed),
            self.receive_errors.load(Ordering::Relaxed),
            self.engine_errors.load(Ordering::Relaxed),
            self.send_errors.load(Ordering::Relaxed),
            self.progress.load(Ordering::Relaxed),
            self.last_connection.load(Ordering::Relaxed),
            self.last_stage.load(Ordering::Relaxed),
            self.last_api_key.load(Ordering::Relaxed),
            self.last_api_version.load(Ordering::Relaxed),
            self.last_correlation.load(Ordering::Relaxed),
        );
    }
}

struct ActiveConnection<'a> {
    diagnostics: &'a Diagnostics,
    connection: u64,
}

impl Drop for ActiveConnection<'_> {
    fn drop(&mut self) {
        self.diagnostics.active.fetch_sub(1, Ordering::Relaxed);
        self.diagnostics.mark(self.connection, STAGE_CLOSED);
        logln!("[broker] connection {} closed", self.connection);
    }
}

/// Address reported in metadata. `BROKER_ADVERTISE_HOST`/`BROKER_ADVERTISE_PORT`
/// override it at build time so a broker behind a host forward can advertise
/// the client-reachable endpoint.
const ADVERTISE_HOST: &[u8] = match option_env!("BROKER_ADVERTISE_HOST") {
    Some(host) => host.as_bytes(),
    None => b"0.0.0.0",
};
const ADVERTISE_PORT: i32 = match option_env!("BROKER_ADVERTISE_PORT") {
    Some(port) => decimal_port(port),
    None => LISTEN_PORT as i32,
};

const fn decimal_port(text: &str) -> i32 {
    let bytes = text.as_bytes();
    let mut value = 0i32;
    let mut index = 0;
    while index < bytes.len() {
        let digit = bytes[index];
        if digit < b'0' || digit > b'9' {
            return LISTEN_PORT as i32;
        }
        value = value * 10 + (digit - b'0') as i32;
        index += 1;
    }
    if value > 0 && value <= 65535 {
        value
    } else {
        LISTEN_PORT as i32
    }
}

fn main(ctx: Context) -> ! {
    serve(&ctx).complete()
}

fn serve(ctx: &Context) -> ShutdownRequest {
    let reactor = CharlotteReactor::new(0);
    let broker = Broker::start(
        &reactor,
        BrokerConfig::new(
            SHARD_COUNT,
            vec![TopicSpec {
                name: TOPIC.to_vec(),
                partitions: PARTITIONS,
            }],
        )
        .with_partition_max_bytes(PARTITION_MAX_BYTES),
    )
    .unwrap_or_else(|error| {
        logln!("[broker] shard startup failed: {:?}", error);
        domain_abort()
    });
    let engine = Arc::new(Engine::new(
        broker,
        EngineConfig::new(BrokerIdentity::new(0, ADVERTISE_HOST, ADVERTISE_PORT)),
    ));
    let diagnostics = Arc::new(Diagnostics::default());
    let session_layout = SessionShardLayout::new(0, SESSION_SHARD_COUNT);
    logln!("[broker] {} partition shards started", SHARD_COUNT);
    logln!("[broker] {} logical session shards available", session_layout.shard_count());

    let bootstrap = ctx.bootstrap_connection().unwrap_or_else(|| domain_abort());
    let endpoint = Endpoint::create(INTERFACE, VERSION, 4).unwrap_or_else(|_| domain_abort());

    let tcp = match ctx.profile_memory() {
        Some(descriptor) => {
            let name = descriptor_name(&descriptor);
            let tcp =
                grant_client::acquire(bootstrap, &descriptor, b"tcpip", deployment::CLIENT_RIGHTS)
                    .unwrap_or_else(|error| {
                        logln!("[broker] tcpip grant failed: {:?}", error);
                        domain_abort()
                    });
            grant_client::publish(bootstrap, &descriptor, &name, &endpoint).unwrap_or_else(
                |error| {
                    logln!("[broker] readiness publish failed: {:?}", error);
                    domain_abort()
                },
            );
            logln!("[broker] published readiness for {:?}", core::str::from_utf8(&name));
            tcp
        }
        None => {
            let (_, tcp) =
                wait_for_registered_name_owned(bootstrap, catten_services::name(b"tcpip"))
                    .unwrap_or_else(|| {
                        logln!("[broker] name-service tcpip lookup failed");
                        domain_abort()
                    });
            tcp
        }
    };

    // One connection handler thread is spawned per accepted connection, and
    // each handler borrows the tcpip connection. Leaking this one capability
    // for the process lifetime gives the handlers a 'static borrow instead of
    // duplicating or reference-counting the underlying capability.
    let tcp: &'static Connection = Box::leak(Box::new(tcp));

    logln!("[broker] serving kafka on tcp port {}", LISTEN_PORT);
    accept_loop(ctx, &reactor, &engine, &diagnostics, tcp, &endpoint, session_layout)
}

fn descriptor_name(descriptor: &LaunchMemoryRef<'_>) -> Vec<u8> {
    let mapping = descriptor.map_read_only().unwrap_or_else(|_| domain_abort());
    let decoded = deployment::decode(mapping.as_slice()).unwrap_or_else(|| domain_abort());
    Vec::from(decoded.artifact_name)
}

fn accept_loop(
    ctx: &Context,
    reactor: &CharlotteReactor,
    engine: &Arc<Engine>,
    diagnostics: &Arc<Diagnostics>,
    tcp: &'static Connection,
    _readiness: &Endpoint,
    session_layout: SessionShardLayout,
) -> ShutdownRequest {
    let mut heartbeat_polls = 0u64;
    loop {
        if let Some(request) = ctx.lifecycle().shutdown_requested() {
            return request;
        }

        let listener =
            OwnedSocket::open(tcp.as_ref(), socket::DOMAIN_TCP).unwrap_or_else(|_| domain_abort());
        let listen_result = listen(tcp, &listener);
        if listen_result != 0 {
            logln!("[broker] listen on port {} failed ({})", LISTEN_PORT, listen_result);
            sleep_ms(ACCEPT_POLL_MS);
            continue;
        }
        logln!("[broker] listening on tcp port {}", LISTEN_PORT);

        loop {
            if let Some(request) = ctx.lifecycle().shutdown_requested() {
                return request;
            }
            let result = listener
                .call(socket::OP_ACCEPT, listener.id())
                .unwrap_or_else(|_| domain_abort())
                .wait()
                .unwrap_or_else(|_| domain_abort())
                .result;
            if result == 0 {
                break;
            }
            if result != socket::ERR_WOULD_BLOCK {
                logln!("[broker] accept failed with {}", result);
                domain_abort();
            }
            sleep_ms(ACCEPT_POLL_MS);
            heartbeat_polls += 1;
            if heartbeat_polls == HEARTBEAT_POLLS {
                diagnostics.heartbeat();
                heartbeat_polls = 0;
            }
        }

        let connection = diagnostics.accepted();
        let session_id = SessionId::new(connection);
        let session_shard = session_layout.shard_for(session_id);
        logln!("[broker] accepted connection {}", connection);
        logln!("[broker] session {} assigned to shard {}", session_id.get(), session_shard.0);
        let engine = Arc::clone(engine);
        let diagnostics = Arc::clone(diagnostics);
        let ctx = *ctx;
        let _ = reactor.spawn_shard(
            session_shard,
            ShardPlacement::Sequential,
            Box::new(move || {
                let _ = serve_connection(&ctx, &engine, &diagnostics, connection, listener);
            }),
        );
    }
}

fn listen(tcp: &Connection, listener: &OwnedSocket<'_>) -> i64 {
    let port_page = OwnedMemory::allocate(1).unwrap_or_else(|_| domain_abort());
    let mut mapping = port_page.map_writable().unwrap_or_else(|_| domain_abort());
    mapping.as_mut_slice()[..2].copy_from_slice(&LISTEN_PORT.to_le_bytes());
    let port_page = mapping.unmap().unwrap_or_else(|_| domain_abort());
    tcp.as_ref()
        .call_move(socket::OP_LISTEN, listener.id(), port_page)
        .unwrap_or_else(|_| domain_abort())
        .wait()
        .unwrap_or_else(|_| domain_abort())
        .result
}

fn serve_connection(
    ctx: &Context,
    engine: &Engine,
    diagnostics: &Diagnostics,
    connection: u64,
    socket: OwnedSocket<'_>,
) -> Option<ShutdownRequest> {
    let _active = ActiveConnection {
        diagnostics,
        connection,
    };
    let mut buffer: Vec<u8> = Vec::new();
    loop {
        if let Some(request) = ctx.lifecycle().shutdown_requested() {
            return Some(request);
        }

        match socket.receive_timeout(1, ACCEPT_POLL_MS) {
            Ok(Some(chunk)) => {
                diagnostics.chunks.fetch_add(1, Ordering::Relaxed);
                diagnostics.mark(connection, STAGE_RECEIVED);
                let (memory, len) = chunk.into_parts();
                if let Ok(mapping) = memory.map_read_only() {
                    buffer.extend_from_slice(&mapping.as_slice()[..len]);
                }
            }
            Ok(None) => return None,
            Err(socket::SocketError::RetryExhausted) => continue,
            Err(_) => {
                diagnostics.receive_errors.fetch_add(1, Ordering::Relaxed);
                diagnostics.mark(connection, STAGE_RECEIVE_ERROR);
                return None;
            }
        }

        while buffer.len() >= 4 {
            let length = i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            if length <= 0 || length as usize > MAX_FRAME_LEN {
                return None;
            }
            let total = 4 + length as usize;
            if buffer.len() < total {
                break;
            }
            let frame: Vec<u8> = buffer.drain(..total).collect();
            diagnostics.frames.fetch_add(1, Ordering::Relaxed);
            diagnostics.mark_frame(connection, &frame);
            match engine.handle_frame(&frame) {
                Ok(response) => {
                    diagnostics.handled.fetch_add(1, Ordering::Relaxed);
                    diagnostics.mark(connection, STAGE_HANDLED);
                    diagnostics.mark(connection, STAGE_SENDING);
                    if socket.send_all(&response, 1200, ACCEPT_POLL_MS).is_err() {
                        diagnostics.send_errors.fetch_add(1, Ordering::Relaxed);
                        diagnostics.mark(connection, STAGE_SEND_ERROR);
                        return None;
                    }
                    diagnostics.sent.fetch_add(1, Ordering::Relaxed);
                    diagnostics.mark(connection, STAGE_SENT);
                }
                Err(_) => {
                    diagnostics.engine_errors.fetch_add(1, Ordering::Relaxed);
                    diagnostics.mark(connection, STAGE_ENGINE_ERROR);
                    return None;
                }
            }
        }
    }
}

catten_rt::entry!(main);
