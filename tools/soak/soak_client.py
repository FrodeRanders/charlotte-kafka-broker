#!/usr/bin/env python3
"""Bounded-memory load loop for the CharlotteOS Kafka broker.

Produces records whose first eight bytes encode the expected log offset and
consumes them back, verifying that every value matches its offset. The client
is intentionally independent of the broker implementation: it uses kafka-python
and treats the broker as a black box.

It tolerates transient broker errors, reconnects, and retention-induced gaps
(the broker bounds each partition to a byte budget and drops the oldest
batches). A gap is reported and the consumer resynchronizes instead of
aborting.
"""

import argparse
import signal
import struct
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError

VALUE_PADDING = b"\xab" * 24
RUNNING = True


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="127.0.0.1:19099")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--duration", type=float, default=0.0, help="seconds; 0 runs until signalled")
    parser.add_argument("--rate", type=float, default=20.0, help="records per second")
    parser.add_argument("--max-errors", type=int, default=100)
    parser.add_argument("--status-interval", type=float, default=30.0)
    parser.add_argument("--api-version", default=None, help="e.g. 0.11.0; auto-negotiated when unset")
    parser.add_argument("--client-id", default="charlotte-soak")
    return parser.parse_args()


def api_version_tuple(text):
    if not text:
        return None
    return tuple(int(part) for part in text.split("."))


def handle_signal(_signum, _frame):
    global RUNNING
    RUNNING = False


def make_producer(args):
    return KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        client_id=args.client_id,
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


def value_for(sequence: int) -> bytes:
    return struct.pack(">Q", sequence) + VALUE_PADDING


def main() -> int:
    global RUNNING
    args = parse_args()
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    topic_partition = TopicPartition(args.topic, args.partition)
    producer = make_producer(args)
    consumer = make_consumer(args, topic_partition)

    end_offsets = consumer.end_offsets([topic_partition])
    sequence = end_offsets.get(topic_partition, 0)
    expected = sequence
    consumer.seek(topic_partition, expected)

    produced = 0
    consumed = 0
    gaps = 0
    errors = 0
    started = time.monotonic()
    next_status = started + args.status_interval
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    log(
        f"soak start bootstrap={args.bootstrap} topic={args.topic} "
        f"partition={args.partition} start_offset={sequence} rate={args.rate}/s"
    )

    while RUNNING:
        if args.duration > 0 and time.monotonic() - started >= args.duration:
            break

        target = started + produced / args.rate if args.rate > 0 else time.monotonic()

        try:
            future = producer.send(
                args.topic, value=value_for(sequence), partition=args.partition
            )
            metadata = future.get(timeout=30)
            if metadata.offset != sequence:
                log(f"offset mismatch: sent seq={sequence} assigned={metadata.offset}")
                sequence = metadata.offset
            produced += 1
            sequence += 1
        except KafkaError as error:
            errors += 1
            log(f"produce error ({errors}): {error}")
            if errors > args.max_errors:
                log("too many produce errors; aborting")
                break
            time.sleep(1.0)
            try:
                producer.close(timeout=1)
            except KafkaError:
                pass
            producer = make_producer(args)
            continue

        try:
            batches = consumer.poll(timeout_ms=0, max_records=1_000)
        except KafkaError as error:
            errors += 1
            log(f"fetch error ({errors}): {error}")
            if errors > args.max_errors:
                break
            time.sleep(1.0)
            try:
                consumer.close()
            except KafkaError:
                pass
            consumer = make_consumer(args, topic_partition)
            continue

        for record in batches.get(topic_partition, []):
            consumed += 1
            if record.offset != expected:
                gaps += 1
                log(
                    f"retention or ordering gap: expected offset {expected}, "
                    f"got {record.offset} ({gaps} gap(s))"
                )
                expected = record.offset
            want = value_for(record.offset)
            if record.value != want:
                errors += 1
                log(
                    f"value mismatch at offset {record.offset}: "
                    f"{record.value!r} != {want!r}"
                )
                if errors > args.max_errors:
                    RUNNING = False
                    break
            expected = record.offset + 1

        now = time.monotonic()
        if now >= next_status:
            elapsed = now - started
            lag = sequence - expected
            log(
                f"status produced={produced} consumed={consumed} lag={lag} "
                f"gaps={gaps} errors={errors} rate={produced / elapsed:.1f}/s "
                f"elapsed={elapsed:.0f}s"
            )
            next_status = now + args.status_interval

        if interval > 0:
            drift = target + interval - time.monotonic()
            if drift > 0:
                time.sleep(min(drift, interval))

    try:
        producer.flush(timeout=5)
        producer.close(timeout=5)
    except KafkaError:
        pass
    try:
        consumer.close()
    except KafkaError:
        pass

    elapsed = time.monotonic() - started
    log(
        f"soak stop produced={produced} consumed={consumed} gaps={gaps} "
        f"errors={errors} elapsed={elapsed:.0f}s"
    )
    return 1 if errors > args.max_errors else 0


if __name__ == "__main__":
    sys.exit(main())
