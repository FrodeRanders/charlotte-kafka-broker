//! Host Kafka-subset smoke client for a deployed EL0 broker.
//!
//! Connects to a plaintext broker address (typically a QEMU SLIRP host
//! forward), builds every request with the pinned CharlotteOS client codec,
//! and parses every response with the same codec.
//!
//! Usage: `cargo run --example remote_smoke -- 127.0.0.1:19099`

use std::{
    env,
    io::{
        Read,
        Write,
    },
    net::TcpStream,
    process,
    time::Duration,
};

use charlotte_kafka as client;

const EVENTS: &[u8] = b"events";

fn main() {
    let address = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:19099".to_owned());
    match run(&address) {
        Ok(summary) => println!(">>> remote smoke ok: {summary}"),
        Err(error) => {
            eprintln!(">>> remote smoke failed: {error}");
            process::exit(1);
        }
    }
}

fn run(address: &str) -> Result<String, String> {
    let mut stream =
        TcpStream::connect(address).map_err(|error| format!("connect {address}: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|error| format!("set read timeout: {error}"))?;

    let request = client::api_versions_request(1, b"remote-smoke").map_err(debug)?;
    let response = round_trip(&mut stream, &request)?;
    let versions = client::parse_api_versions(&response, 1).map_err(debug)?;
    if !versions.supports(client::api::PRODUCE, client::version::PRODUCE)
        || !versions.supports(client::api::FETCH, client::version::FETCH)
    {
        return Err("broker does not advertise the expected subset".to_owned());
    }

    let topics: &[&[u8]] = &[EVENTS];
    let request = client::metadata_request_many(2, b"remote-smoke", topics).map_err(debug)?;
    let response = round_trip(&mut stream, &request)?;
    let metadata = client::parse_metadata_many(&response, 2).map_err(debug)?;
    let events = metadata
        .topics
        .iter()
        .find(|topic| topic.topic == EVENTS)
        .ok_or_else(|| "metadata response omitted events".to_owned())?;
    if events.error != client::NO_ERROR || events.partitions.is_empty() {
        return Err(format!("events metadata error {}", events.error));
    }

    let batch = client::encode_record_batch(
        &[client::RecordInput {
            timestamp_ms: 1_000,
            key: Some(b"smoke"),
            value: Some(b"remote"),
        }],
        client::ProducerIdentity {
            producer_id: -1,
            producer_epoch: -1,
        },
        -1,
        false,
    )
    .map_err(debug)?;
    let request = client::produce_request(3, b"remote-smoke", None, EVENTS, 0, &batch, 30_000)
        .map_err(debug)?;
    let response = round_trip(&mut stream, &request)?;
    let produced = client::parse_produce(&response, 3, EVENTS, 0).map_err(debug)?;
    if produced.error != client::NO_ERROR {
        return Err(format!("produce error {}", produced.error));
    }

    let fetch = client::Fetch {
        topic: EVENTS,
        partition: 0,
        offset: produced.base_offset,
        max_wait_ms: 0,
        max_bytes: 65_536,
        read_committed: true,
    };
    let request = client::fetch_request(4, b"remote-smoke", fetch).map_err(debug)?;
    let response = round_trip(&mut stream, &request)?;
    let fetched = client::parse_fetch(&response, 4, EVENTS, 0).map_err(debug)?;
    if fetched.error != client::NO_ERROR || fetched.records.len() != 1 {
        return Err(format!("fetch error {} records {}", fetched.error, fetched.records.len()));
    }
    if fetched.records[0].value.as_deref() != Some(b"remote".as_slice()) {
        return Err("fetched record value mismatch".to_owned());
    }

    let request =
        client::list_offsets_request(5, b"remote-smoke", EVENTS, 0, false).map_err(debug)?;
    let response = round_trip(&mut stream, &request)?;
    let latest = client::parse_list_offsets(&response, 5, EVENTS, 0).map_err(debug)?;
    if latest <= produced.base_offset {
        return Err(format!("latest offset {latest} did not advance"));
    }

    Ok(format!(
        "partition 0 base offset {}, high watermark {}, latest {}",
        produced.base_offset, fetched.high_watermark, latest
    ))
}

fn round_trip(stream: &mut TcpStream, request: &[u8]) -> Result<Vec<u8>, String> {
    stream.write_all(request).map_err(|error| format!("write request: {error}"))?;
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).map_err(|error| format!("read length: {error}"))?;
    let length = i32::from_be_bytes(prefix);
    if length <= 0 || length as usize > 1024 * 1024 {
        return Err(format!("invalid response length {length}"));
    }
    let mut response = vec![0u8; 4 + length as usize];
    response[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut response[4..]).map_err(|error| format!("read response: {error}"))?;
    Ok(response)
}

fn debug(error: client::Error) -> String {
    format!("{error:?}")
}
