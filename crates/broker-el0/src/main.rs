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
    shard::ShardId,
    shard_runtime::ShardRuntime,
};

const SHARD_COUNT: usize = 2;
const LISTEN_PORT: u16 = 9092;
const ACCEPT_POLL_MS: u64 = 25;
const MAX_FRAME_LEN: usize = 1024 * 1024;
const TOPIC: &[u8] = b"events";
const PARTITIONS: i32 = 4;
const PARTITION_MAX_BYTES: usize = 64 * 1024;
const INTERFACE: u64 = catten_services::name(b"KBRK");
const VERSION: u32 = 1;

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
    logln!("[broker] {} partition shards started", SHARD_COUNT);

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
    accept_loop(ctx, &reactor, &engine, tcp, &endpoint)
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
    tcp: &'static Connection,
    _readiness: &Endpoint,
) -> ShutdownRequest {
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
        }

        let engine = Arc::clone(engine);
        let ctx = *ctx;
        let _ = reactor.spawn_shard(
            ShardId(0),
            ShardPlacement::Sequential,
            Box::new(move || {
                let _ = serve_connection(&ctx, &engine, listener);
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
    socket: OwnedSocket<'_>,
) -> Option<ShutdownRequest> {
    let mut buffer: Vec<u8> = Vec::new();
    loop {
        if let Some(request) = ctx.lifecycle().shutdown_requested() {
            return Some(request);
        }

        match socket.receive_timeout(1, ACCEPT_POLL_MS) {
            Ok(Some(chunk)) => {
                let (memory, len) = chunk.into_parts();
                if let Ok(mapping) = memory.map_read_only() {
                    buffer.extend_from_slice(&mapping.as_slice()[..len]);
                }
            }
            Ok(None) => return None,
            Err(socket::SocketError::RetryExhausted) => continue,
            Err(_) => return None,
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
            match engine.handle_frame(&frame) {
                Ok(response) => {
                    if socket.send_all(&response, 1200, ACCEPT_POLL_MS).is_err() {
                        return None;
                    }
                }
                Err(_) => return None,
            }
        }
    }
}

catten_rt::entry!(main);
