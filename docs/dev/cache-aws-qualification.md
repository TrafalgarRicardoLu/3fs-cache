# AWS S3 read-cache release qualification

`test_cache_aws_s3` is a release gate, not a normal credentialed CI test. In an ordinary cache-enabled build it is
registered and reports `SKIPPED`. It performs network operations only when `HF3FS_CACHE_AWS_QUALIFICATION=1` is set.

The suite requires two disposable buckets in the same region:

- a versioned bucket, used to verify that a pinned VersionId remains readable after a newer object version is written;
- an unversioned bucket, used to verify that a stale strong ETag is rejected through `If-Match`.

The test writes only beneath a process-scoped `hf3fs-cache-qualification/` prefix and removes every created object or
version during suite teardown. Do not use production buckets.

## Build

```bash
cmake -S . -B build \
  -DHF3FS_ENABLE_CACHE=ON \
  -DCMAKE_CXX_COMPILER=clang++-14 \
  -DCMAKE_C_COMPILER=clang-14 \
  -DSHUFFLE_METHOD=g++11
cmake --build build --target test_cache_aws_s3 -j 32
```

## Run a qualification

Inject credentials through the standard AWS provider chain, workload identity, or the optional environment fields
below. Never place real account identifiers, bucket names, or credentials in repository files.

```bash
export HF3FS_CACHE_AWS_QUALIFICATION=1
export HF3FS_CACHE_AWS_REGION=us-east-1
export HF3FS_CACHE_AWS_VERSIONED_BUCKET=...       # disposable, versioning enabled
export HF3FS_CACHE_AWS_ETAG_BUCKET=...            # disposable, versioning disabled
export HF3FS_CACHE_AWS_QUALIFICATION_LOG=/tmp/cache-aws-qualification.log

# Optional for static temporary credentials:
export HF3FS_CACHE_AWS_ACCESS_KEY=...
export HF3FS_CACHE_AWS_SECRET_KEY=...
export HF3FS_CACHE_AWS_SESSION_TOKEN=...

ctest --test-dir build -R '^test_cache_aws_s3$' --output-on-failure
```

For an S3-compatible qualification endpoint, also set `HF3FS_CACHE_AWS_ENDPOINT`,
`HF3FS_CACHE_AWS_USE_TLS`, and `HF3FS_CACHE_AWS_PATH_STYLE` as appropriate.

Archive both the CTest output and the sanitized qualification log. The latter records the two scenario results plus
the number of objects considered and any cleanup failures; it contains no bucket, account, credential, or object-key
values. A release qualification is complete only when both scenarios pass and `cleanup_failures=0`.
