//! Benchmarks for the read-side zstd decompression-context question
//! (foxglove/mcap#1847: "rust: should IndexedReader reuse its zstd
//! decompression context?").
//!
//! `IndexedReader::insert_chunk_record_data()` decompressed each zstd chunk
//! with the one-shot `zstd::zstd_safe::decompress()` (baseline), which builds
//! and tears down a fresh decompression context for every chunk. This bench
//! compares that against a reused caller-owned `DCtx`.
//!
//! Two angles:
//! 1. `decompress_setup`: isolates the per-chunk cost -- decompressing one
//!    fixed chunk-sized payload with a fresh context each time vs a single
//!    reused `DCtx`. The per-chunk setup delta is the whole hypothesis.
//! 2. `indexed_reader`: end-to-end indexed reads over generated files with a
//!    small-chunk workload (many chunks, where per-chunk setup dominates) and
//!    a default-chunk workload (few ~1 MiB chunks, where setup should barely
//!    matter). The reader-level result decides whether the code change is
//!    worth making.
//!
//! Methodology: run `cargo bench --bench zstd_reuse` (default features) on the
//! baseline and again with the reuse patch, on the same machine back to back.
//! Files are generated in-memory; the read path drives the public sans-io
//! `IndexedReader` exactly as a real consumer would (seek, then
//! `insert_chunk_record_data`).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mcap::{sans_io, Channel, Message, Schema};
use std::borrow::Cow;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

/// Semi-compressible pseudo-random payload, like sensor data: 6 bits of
/// entropy per byte, so zstd does real work instead of memcpys.
fn zstd_payload(uncompressed_len: usize) -> (Vec<u8>, Vec<u8>) {
    let mut data = Vec::with_capacity(uncompressed_len);
    let mut x: u64 = 0x1234_5678_9abc_def0;
    for _ in 0..uncompressed_len {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        data.push(((x >> 33) as u8) & 0x3f);
    }
    let compressed = zstd::encode_all(Cursor::new(&data), 3).expect("zstd compress failed");
    (compressed, data)
}

/// One-shot `zstd_safe::decompress()` (what `insert_chunk_record_data` does
/// today) vs a reused caller-owned `DCtx`, on chunk-sized payloads.
fn bench_decompress_setup(c: &mut Criterion) {
    for &size in &[4 * 1024usize, 64 * 1024, 1024 * 1024] {
        let (compressed, _uncompressed) = zstd_payload(size);
        let mut group = c.benchmark_group("zstd_decompress_setup");
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("oneshot", size), &compressed, |b, src| {
            b.iter(|| {
                // mirrors insert_chunk_record_data: fresh slot buffer with
                // capacity, then one-shot decompress.
                let mut dst = Vec::with_capacity(size);
                let n = zstd::zstd_safe::decompress(&mut dst, src).expect("decompress failed");
                std::hint::black_box((n, dst));
            })
        });
        group.bench_with_input(
            BenchmarkId::new("reused_dctx", size),
            &compressed,
            |b, src| {
                let mut dctx = zstd::zstd_safe::DCtx::create();
                b.iter(|| {
                    let mut dst = Vec::with_capacity(size);
                    let n = dctx.decompress(&mut dst, src).expect("decompress failed");
                    std::hint::black_box((n, dst));
                })
            },
        );
        group.finish();
    }
}

fn create_test_mcap(n: usize, chunk_size: Option<u64>) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = mcap::WriteOptions::new()
            .compression(Some(mcap::Compression::Zstd))
            .chunk_size(chunk_size)
            .profile("fooey")
            .create(Cursor::new(&mut buffer))
            .unwrap();
        // Mock message data to align with reader benchmarks in benches/reader.rs
        const MESSAGE_DATA: &[u8] = &[42; 10];

        let schema = Arc::new(Schema {
            id: 1,
            name: "TestSchema".to_string(),
            encoding: "raw".to_string(),
            data: Cow::Borrowed(b"{}"),
        });

        let channel = Arc::new(Channel {
            id: 0,
            topic: "test_topic".to_string(),
            message_encoding: "raw".to_string(),
            metadata: Default::default(),
            schema: Some(schema),
        });

        for i in 0..n {
            let message = Message {
                channel: channel.clone(),
                sequence: i as u32,
                log_time: i as u64,
                publish_time: i as u64,
                data: Cow::Borrowed(MESSAGE_DATA),
            };
            writer.write(&message).unwrap();
        }

        writer.finish().unwrap();
    }
    buffer
}

fn load_summary(file: &mut Cursor<&[u8]>) -> mcap::Summary {
    use std::io::{Read, Seek};
    let mut reader = sans_io::SummaryReader::new();
    while let Some(event) = reader.next_event() {
        match event.expect("next event failed") {
            sans_io::SummaryReadEvent::ReadRequest(n) => {
                let read = file.read(reader.insert(n)).expect("read failed");
                reader.notify_read(read);
            }
            sans_io::SummaryReadEvent::SeekRequest(pos) => {
                reader.notify_seeked(file.seek(pos).expect("seek failed"));
            }
        }
    }
    reader.finish().unwrap()
}

/// Drive a full indexed read of every message, like benches/reader.rs.
fn read_all_indexed(mcap_data: &[u8]) -> usize {
    use std::io::{Read, Seek};
    let mut file = Cursor::new(mcap_data);
    let summary = load_summary(&mut file);
    let mut reader = sans_io::IndexedReader::new(&summary).expect("could not build reader");
    let mut data_buf = Vec::new();
    let mut count = 0;
    while let Some(event) = reader.next_event() {
        match event.expect("next event failed") {
            sans_io::IndexedReadEvent::Message { header, data } => {
                data_buf.resize(data.len(), 0);
                data_buf.copy_from_slice(data);
                let message = mcap::Message {
                    channel: summary.channels.get(&header.channel_id).unwrap().clone(),
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data: Cow::Borrowed(&data_buf),
                };
                std::hint::black_box(message);
                count += 1;
            }
            sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                file.seek(std::io::SeekFrom::Start(offset))
                    .expect("failed to seek");
                data_buf.resize(length, 0);
                file.read_exact(&mut data_buf).expect("failed to read");
                reader
                    .insert_chunk_record_data(offset, &data_buf)
                    .expect("failed to insert");
            }
        }
    }
    count
}

/// End-to-end indexed reads: many small chunks vs few default-size chunks.
fn bench_indexed_reader(c: &mut Criterion) {
    // ~5 MB of messages in ~4 KiB chunks: per-chunk setup cost dominates.
    let mcap_small_chunks = create_test_mcap(100_000, Some(4 * 1024));
    // ~10 MB of messages in default 1 MiB chunks: setup should barely matter.
    let mcap_default_chunks = create_test_mcap(200_000, None);

    let mut group = c.benchmark_group("indexed_reader_zstd");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("small_chunks_4kib", |b| {
        b.iter(|| std::hint::black_box(read_all_indexed(&mcap_small_chunks)));
    });
    group.bench_function("default_chunks_1mib", |b| {
        b.iter(|| std::hint::black_box(read_all_indexed(&mcap_default_chunks)));
    });
    group.finish();
}

criterion_group!(benches, bench_decompress_setup, bench_indexed_reader);
criterion_main!(benches);
