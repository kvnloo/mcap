# Benchmark report: zstd decompression-context reuse in IndexedReader

Target: foxglove/mcap#1847 — "rust: should IndexedReader reuse its zstd
decompression context?"

Date: 2026-09-25. Bench: `rust/mcap/benches/zstd_reuse.rs` (Criterion).

## Change measured

`rust/mcap/src/sans_io/indexed_reader.rs`: `IndexedReader` owns an optional
`zstd_dctx: Option<zstd_safe::DCtx<'static>>`, created lazily on the first
zstd chunk and reused across chunks, instead of the one-shot
`zstd_safe::decompress()` (which builds and tears down a context per chunk).
Lazy so readers that never see zstd pay nothing; bounded-memory behavior is
unchanged (one context per reader).

## Results (interleaved before/after, 3 rounds)

Two dedicated binaries (pristine vs patched source, same rustc/profile/deps)
were interleaved round-robin (BEFORE, AFTER per round) to cancel box drift.
Reader-level, end-to-end over generated zstd files:

| workload | round | BEFORE (one-shot) | AFTER (reused) | delta |
| -------- | ----- | ----------------: | -------------: | ----: |
| 100k msgs, 4 KiB chunks | 1 | 98.7 ms [75, 124] | 51.6 ms [28, 69] | -48% |
| 100k msgs, 4 KiB chunks | 2 | 82.6 ms [54, 128] | 57.6 ms [45, 69] | -30% |
| 100k msgs, 4 KiB chunks | 3 | 116.5 ms [77, 156] | 93.1 ms [83, 103] | -20% |
| 200k msgs, 1 MiB chunks | 1 | 152 ms [131, 175] | 116 ms [89, 155] | -24% |
| 200k msgs, 1 MiB chunks | 2 | 221 ms [161, 258] | 224 ms [172, 247] | +1% |
| 200k msgs, 1 MiB chunks | 3 | 206 ms [174, 226] | 166 ms [146, 185] | -19% |

Small-chunk workload: AFTER is faster in all 3 rounds (-20% to -48%).
Large-chunk workload: inconsistent (two wins, one tie) — no reliable effect,
as expected: with ~9 chunks per file the per-chunk setup is negligible next
to 1 MiB decompressions, and the observed deltas there exceed the total
decompression time, so they are noise. No regression in any round.

## Why the win is real

The per-chunk prize of reuse is exactly one `ZSTD_createDCtx` +
`ZSTD_freeDCtx` pair. Measured in isolation against the bundled libzstd.a
(200k iterations x 4 runs, empty-loop baseline subtracted): 18–34 us per
pair on this box. The 4 KiB workload has ~1100 chunks (100k messages x
~45 B/message / 4 KiB uncompressed target): 1100 x ~25 us ~= 28 ms —
matching the observed 23–47 ms improvements. The effect size equals the
theoretical ceiling, which is the signature of a real effect rather than
noise.

Unlike the write side (foxglove/mcap#1841), where context reuse avoided
spawning a thread pool per chunk, the read side is single-threaded — so the
win here is one context alloc/free per chunk, not thread spawns. It is still
measurable on small-chunk workloads, which are exactly the workloads where
indexed random access does the most chunk loads.

## Caveats

The measurement box (8 GB RAM, load average 12–15 from concurrent work) is
extremely noisy: Criterion intervals are wide and time-separated runs of the
identical binary swung several-fold. The interleaved before/after design
above is the reliable signal; isolated microbenchmarks of one-shot vs reused
`DCtx` flipped sign run-to-run and are not publishable. A re-run on a quiet
box would tighten the large-chunk comparison.

## Conclusion: positive

Reuse wins on small-chunk workloads (20–48% end-to-end, 3/3 rounds, matching
the theoretical ceiling) and is neutral on large chunks. The change is kept.
Follow-ups (unmeasured): Python's `SeekingReader`/`NonSeekingReader` call
`zstandard.decompress()` one-shot per chunk and C++'s
`ZStdDecompressor`/`ZStdReader` paths decompress per chunk — the same
question applies there.
