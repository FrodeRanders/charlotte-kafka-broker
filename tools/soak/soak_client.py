#!/usr/bin/env python3
"""Bounded-memory load loop for the CharlotteOS Kafka broker.

Offers an aggregate paced record stream to one topic partition from a pool of
independent producer connections, consumes the records back, and verifies log
continuity, value integrity, and per-producer sequence monotonicity. The client
is intentionally independent of the broker implementation: it uses kafka-python
and treats the broker as a black box.

A single synchronous producer is limited to one record per broker round trip,
so `--rate` is only reachable when `--producers` supplies enough concurrency to
cover the broker's request latency. Each worker paces its share of the
aggregate rate and blocks on its own acknowledgements, while a single consumer
verifies the interleaved log.

It tolerates transient broker errors, reconnects, and retention-induced gaps
(the broker bounds each partition to a byte budget and drops the oldest
batches). A gap is reported and the consumer resynchronizes instead of
aborting.
"""

import argparse
import signal
import struct
import sys
import threading
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError

VALUE_PADDING = b"\xab" * 24
VALUE_HEADER = struct.Struct(">QQ")
VALUE_SIZE = VALUE_HEADER.size + len(VALUE_PADDING)
STOP = threading.Event()


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="127.0.0.1:19099")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--duration", type=float, default=0.0, help="seconds; 0 runs until signalled")
    parser.add_argument("--rate", type=float, default=20.0, help="aggregate records per second offered by all producers")
    parser.add_argument("--producers", type=int, default=4, help="concurrent producer connections")
    parser.add_argument("--max-errors", type=int, default=100)
    parser.add_argument("--status-interval", type=float, default=30.0)
    parser.add_argument("--api-version", default=None, help="e.g. 0.11.0; auto-negotiated when unset")
    parser.add_argument("--client-id", default="charlotte-soak")
    args = parser.parse_args()
    if args.producers < 1:
        parser.error("--producers must be at least 1")
    return args


def api_version_tuple(text):
    if not text:
        return None
    return tuple(int(part) for part in text.split("."))


def handle_signal(_signum, _frame):
    STOP.set()


def make_producer(args, index):
    return KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        client_id=f"{args.client_id}-p{index}",
        acks=1,
        max_block_ms=10_000,
        request_timeout_ms=30_000,
        api_version=api_version_tuple(args.api_version),
    )


def make_consumer(args, topic_partition):
    consumer = KafkaConsumer(
        bootstrap_servers=[args.bootstrap],
        client_id=f"{args.client_id}-consumer",
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1_000,
        request_timeout_ms=30_000,
        api_version=api_version_tuple(args.api_version),
    )
    consumer.assign([topic_partition])
    return consumer


def value_for(worker: int, sequence: int) -> bytes:
    return VALUE_HEADER.pack(worker, sequence) + VALUE_PADDING


def note_error(stats, lock, message):
    with lock:
        stats["errors"] += 1
        count = stats["errors"]
    log(f"{message} ({count} error(s))")
    return count


def producer_worker(index, args, stats, lock):
    interval = args.producers / args.rate if args.rate > 0 else 0.0
    sequence = 0
    last_offset = None
    producer = None
    started = time.monotonic()
    try:
        while not STOP.is_set():
            if producer is None:
                try:
                    producer = make_producer(args, index)
                except KafkaError as error:
                    count = note_error(
                        stats,
                        lock,
                        f"producer {index}: reconnect failed: {error}",
                    )
                    if count > args.max_errors:
                        STOP.set()
                        break
                    time.sleep(1.0)
                    continue
            if interval > 0:
                drift = started + sequence * interval - time.monotonic()
                while drift > 0 and not STOP.is_set():
                    time.sleep(min(drift, 1.0))
                    drift = started + sequence * interval - time.monotonic()
                if STOP.is_set():
                    break
            try:
                future = producer.send(
                    args.topic,
                    value=value_for(index, sequence),
                    partition=args.partition,
                )
                metadata = future.get(timeout=30)
                if last_offset is not None and metadata.offset <= last_offset:
                    with lock:
                        stats["errors"] += 1
                        count = stats["errors"]
                    log(
                        f"producer {index}: offset moved backwards: "
                        f"{metadata.offset} after {last_offset} ({count} error(s))"
                    )
                    if count > args.max_errors:
                        STOP.set()
                        break
                last_offset = metadata.offset
                with lock:
                    stats["produced"] += 1
                sequence += 1
            except KafkaError as error:
                count = note_error(stats, lock, f"producer {index}: produce error: {error}")
                if count > args.max_errors:
                    log(f"producer {index}: too many errors; stopping")
                    STOP.set()
                    break
                try:
                    producer.close(timeout=1)
                except KafkaError:
                    pass
                producer = None
                time.sleep(1.0)
    finally:
        if producer is not None:
            try:
                producer.flush(timeout=5)
                producer.close(timeout=5)
            except KafkaError:
                pass


def consumer_loop(args, topic_partition, consumer, stats, lock, started, start_offset):
    expected = start_offset
    last_sequence = {}
    next_status = started + args.status_interval
    while not STOP.is_set():
        if args.duration > 0 and time.monotonic() - started >= args.duration:
            break

        try:
            batches = consumer.poll(timeout_ms=200, max_records=1_000)
        except KafkaError as error:
            count = note_error(stats, lock, f"fetch error: {error}")
            if count > args.max_errors:
                break
            time.sleep(1.0)
            try:
                consumer.close()
            except KafkaError:
                pass
            while not STOP.is_set():
                try:
                    consumer = make_consumer(args, topic_partition)
                    consumer.seek(topic_partition, expected)
                    break
                except KafkaError as reconnect_error:
                    count = note_error(
                        stats,
                        lock,
                        f"consumer reconnect failed: {reconnect_error}",
                    )
                    if count > args.max_errors:
                        STOP.set()
                        break
                    time.sleep(1.0)
            continue

        for record in batches.get(topic_partition, []):
            with lock:
                stats["consumed"] += 1
            if record.offset != expected:
                with lock:
                    stats["gaps"] += 1
                    gaps = stats["gaps"]
                log(
                    f"retention or ordering gap: expected offset {expected}, "
                    f"got {record.offset} ({gaps} gap(s))"
                )
                expected = record.offset
            valid = (
                record.value is not None
                and len(record.value) == VALUE_SIZE
                and VALUE_HEADER.unpack(record.value[: VALUE_HEADER.size])[0] < args.producers
            )
            if valid:
                worker, sequence = VALUE_HEADER.unpack(record.value[: VALUE_HEADER.size])
                valid = record.value == value_for(worker, sequence)
            if not valid:
                with lock:
                    stats["errors"] += 1
                    count = stats["errors"]
                log(
                    f"value mismatch at offset {record.offset}: "
                    f"{record.value!r} does not decode to a known producer"
                )
                if count > args.max_errors:
                    STOP.set()
                    break
            elif last_sequence.get(worker, -1) >= sequence:
                with lock:
                    stats["errors"] += 1
                    count = stats["errors"]
                log(
                    f"duplicate or reordered record at offset {record.offset}: "
                    f"producer {worker} sequence {sequence} after {last_sequence[worker]}"
                )
                if count > args.max_errors:
                    STOP.set()
                    break
            else:
                last_sequence[worker] = sequence
            expected = record.offset + 1

        now = time.monotonic()
        if now >= next_status:
            with lock:
                produced = stats["produced"]
                consumed = stats["consumed"]
                gaps = stats["gaps"]
                errors = stats["errors"]
            elapsed = now - started
            log(
                f"status produced={produced} consumed={consumed} "
                f"lag={max(produced - consumed, 0)} gaps={gaps} errors={errors} "
                f"rate={produced / elapsed:.1f}/s elapsed={elapsed:.0f}s"
            )
            next_status = now + args.status_interval

    STOP.set()


def main() -> int:
    args = parse_args()
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    topic_partition = TopicPartition(args.topic, args.partition)
    consumer = make_consumer(args, topic_partition)

    end_offsets = consumer.end_offsets([topic_partition])
    start_offset = end_offsets.get(topic_partition, 0)
    consumer.seek(topic_partition, start_offset)

    stats = {"produced": 0, "consumed": 0, "gaps": 0, "errors": 0}
    lock = threading.Lock()
    started = time.monotonic()

    log(
        f"soak start bootstrap={args.bootstrap} topic={args.topic} "
        f"partition={args.partition} start_offset={start_offset} "
        f"producers={args.producers} rate={args.rate}/s"
    )

    workers = [
        threading.Thread(
            target=producer_worker,
            args=(index, args, stats, lock),
            name=f"soak-producer-{index}",
            daemon=True,
        )
        for index in range(args.producers)
    ]
    for worker in workers:
        worker.start()

    consumer_loop(args, topic_partition, consumer, stats, lock, started, start_offset)
    STOP.set()
    for worker in workers:
        worker.join(timeout=35)

    try:
        consumer.close()
    except KafkaError:
        pass

    with lock:
        produced = stats["produced"]
        consumed = stats["consumed"]
        gaps = stats["gaps"]
        errors = stats["errors"]
    elapsed = time.monotonic() - started
    log(
        f"soak stop produced={produced} consumed={consumed} gaps={gaps} "
        f"errors={errors} elapsed={elapsed:.0f}s"
    )
    return 1 if errors > args.max_errors else 0


if __name__ == "__main__":
    sys.exit(main())
