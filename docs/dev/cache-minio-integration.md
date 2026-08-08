# MinIO read-cache integration test

The `test_cache_minio` suite exercises the real AWS SDK transport against an isolated MinIO bucket. It uploads a fixed
boundary matrix (0, 1, block-1, block, block+1, and multi-block objects), then covers cold reads, background fill,
zero-origin-request warm reads, mixed reads, refresh fencing, version mismatch, capacity bypass, cleanup fallback, and
the Phase 4 multipart create/part/complete/HEAD-recovery/abort/read-after-publish lifecycle.

## Prerequisites

- Build dependencies from the repository development guide.
- AWS SDK for C++ with the S3 component and its CMake package.
- A disposable MinIO endpoint. The test creates and removes its own process-scoped bucket.

Configure with both cache switches enabled:

```bash
cmake -S . -B build \
  -DHF3FS_ENABLE_CACHE=ON \
  -DHF3FS_ENABLE_CACHE_INTEGRATION_TESTS=ON \
  -DCMAKE_CXX_COMPILER=clang++-14 \
  -DCMAKE_C_COMPILER=clang-14 \
  -DSHUFFLE_METHOD=g++11
cmake --build build --target test_cache_minio -j 32
```

Provide credentials only through the environment. The endpoint should normally be `host:port`; TLS is controlled
separately.

```bash
export HF3FS_CACHE_MINIO_ENDPOINT=127.0.0.1:9000
export HF3FS_CACHE_MINIO_ACCESS_KEY=minioadmin
export HF3FS_CACHE_MINIO_SECRET_KEY=minioadmin
export HF3FS_CACHE_MINIO_REGION=us-east-1
export HF3FS_CACHE_MINIO_USE_TLS=0
ctest --test-dir build -R '^test_cache_minio$' --output-on-failure
```

When the target is present but endpoint credentials are absent, every scenario reports `SKIPPED` explicitly. This is
not release evidence: Phase 4 qualification requires all scenarios to execute against a credentialed disposable MinIO
bucket. Endpoint or protocol failures after credentials are supplied remain hard test failures. The test output never
prints credentials, bucket contents, or signed requests.
