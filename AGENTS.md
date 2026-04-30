# Repository Guidelines

## Project Structure & Module Organization
3FS is a C++20/C/Rust distributed file system with Python utilities. Core services live under `src/`, grouped by subsystem such as `storage`, `meta`, `mgmtd`, `client`, `fuse`, `common`, and `lib`. Unit and integration tests mirror those areas under `tests/`; FUSE scenario tests and TOML fixtures are in `tests/fuse/`. Benchmarks are in `benchmarks/`, formal P specifications are in `specs/`, runtime examples are in `configs/`, deployment notes are in `deploy/`, and user documentation/assets are in `docs/`. Vendored dependencies are under `third_party/`; do not edit them unless the change is explicitly about vendored code.

## Build, Test, and Development Commands
- `git submodule update --init --recursive && ./patches/apply.sh`: initialize dependencies after cloning.
- `cargo build --release`: build Rust workspace members.
- `cmake -S . -B build -DCMAKE_CXX_COMPILER=clang++-14 -DCMAKE_C_COMPILER=clang-14 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DSHUFFLE_METHOD=g++11`: configure an out-of-source CMake build. Choose `g++10`, `g++11`, or `stdshuffle` deliberately for cluster compatibility.
- `cmake --build build -j 32`: compile binaries, libraries, tests, and benchmarks.
- `ctest --test-dir build --output-on-failure`: run registered CTest targets.
- `cmake --build build --target check-format`: validate C++ formatting; use `format` to rewrite.

## Coding Style & Naming Conventions
C++ uses `.clang-format` based on Google style: 2-space indentation, 120-column limit, C++20, right-aligned pointers, and sorted includes except generated FlatBuffers headers. Keep subsystem naming consistent with existing files: classes and types use `PascalCase`, tests commonly use `Test*.cc`, and CMake test binaries use `test_<area>`. Rust follows edition 2021 with MSRV 1.85.0. Python packages are lowercase modules under `hf3fs*`.

## Testing Guidelines
Most C++ tests use GoogleTest and are registered through `target_add_test(...)` in `tests/*/CMakeLists.txt`. Add tests near the subsystem under change and prefer focused `TEST`/`TEST_F` cases that reproduce behavior. Some tests require FoundationDB variables such as `FDB_UNITTEST_CLUSTER`; FUSE tests use `tests/fuse/run.sh` and local config templates.

## Commit & Pull Request Guidelines
History uses short imperative subjects, often with optional scopes, for example `fix(sync): ...` or `chore: ...`; many merged commits include PR numbers. Keep commits focused, mention compatibility-sensitive options such as `SHUFFLE_METHOD`, and include tests run in the PR description. Link related issues and add logs or screenshots only when they clarify runtime or UI behavior.

## Agent-Specific Instructions
Make surgical changes, match existing style, and avoid opportunistic refactors. Do not rewrite generated files, vendored dependencies, or unrelated documentation. Preserve user work in the tree and verify changes with the narrowest relevant build, test, or formatting command before reporting completion.
