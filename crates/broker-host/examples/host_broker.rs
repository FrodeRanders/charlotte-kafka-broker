//! Runs a plain TCP broker on `127.0.0.1:9092` with one `events` topic.
//!
//! This is a development front end: it exists so the broker can be exercised
//! from a host client without booting CharlotteOS.

use std::{
    net::SocketAddr,
    sync::Arc,
};

use broker_engine::{
    BrokerIdentity,
    Engine,
    EngineConfig,
};
use broker_host::start;
use broker_runtime::{
    Broker,
    BrokerConfig,
    TopicSpec,
};
use sitas_unix::UnixRuntime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = UnixRuntime::new();
    let broker = Broker::start(
        &runtime,
        BrokerConfig::new(
            4,
            vec![TopicSpec {
                name: b"events".to_vec(),
                partitions: 4,
            }],
        ),
    )?;
    let engine = Arc::new(Engine::new(
        broker,
        EngineConfig::new(BrokerIdentity::new(0, b"127.0.0.1", 9092)),
    ));

    let server = start(engine, "127.0.0.1:9092".parse::<SocketAddr>()?)?;
    println!("charlotte kafka broker listening on {}", server.local_addr());

    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
