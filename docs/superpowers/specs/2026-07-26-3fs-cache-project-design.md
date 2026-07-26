# 3FS Cache Project Design

## Identity

The project name is **3FS Cache**, and its repository slug is `3fs-cache`. It deliberately retains the 3FS lineage while
making the intended cache semantics explicit. It is independently maintained and must not imply affiliation with or
endorsement by DeepSeek.

## Semantic direction

3FS Cache will evolve from 3FS's persistent-file-system semantics toward a distributed cache in which:

- an underlying persistent storage system remains the source of truth;
- cached data can be evicted according to policy;
- evicted or lost cached data can be reconstructed from the persistent source; and
- the initial codebase may retain persistent 3FS behavior while this transition is incomplete.

## Repository and licensing

The repository preserves the complete source history so attribution and upstream comparisons remain available. DeepSeek's
2025 MIT copyright notice remains intact, and the maintainer's 2026 copyright notice covers subsequent modifications. The
original DeepSeek repository is configured as `upstream`.

Third-party copyright headers and license files are preserved. A complete third-party notice audit is required before
shipping compiled binaries, packages, or container images.

## Initial scope

This repository setup changes project identity and documentation only. It does not claim that cache semantics are already
implemented, and it does not rename internal `hf3fs` namespaces, protocols, binaries, or compatibility identifiers.
