# Cache benchmark

`cache_bench` records a component-level baseline for the phase-one read-cache paths. It uses an in-memory object store
and the real `OriginMissReader`, `ReadPlanner`, and `CacheReadPipeline`; it does not measure a deployed 3FS cluster or
an external S3 service.

Build and run it with:

```bash
cmake --build build --target cache_bench -j 32
build/bin/cache_bench \
  --iterations=100 \
  --object_size=4194304 \
  --read_size=262144 \
  --block_size=65536 \
  --output=/tmp/hf3fs-cache-bench.csv
```

The CSV contains throughput, P50/P99 latency, origin request count, and hit ratio for direct cold origin reads,
foreground misses, warm hits, mixed reads, and a native-memory baseline. Results are observational only: phase one has
no performance pass threshold.
