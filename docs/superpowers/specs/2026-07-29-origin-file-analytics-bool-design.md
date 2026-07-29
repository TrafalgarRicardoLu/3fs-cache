# OriginFile analytics bool visitor compatibility design

## Context

The phase-one cache work added the `superseded` and `cacheAdmissionDisabled` boolean fields to `meta::OriginFile`.
`meta::Inode` analytics traversal now reaches those fields. The production `SerdeSchemaBuilder` already maps `bool` to
Parquet `BOOLEAN`, but the test-only `DebugStructVisitor` deliberately deletes unsupported types and lacks a `bool`
specialization. As a result, the cache-enabled all-target build fails while compiling `test_analytics`.

## Design

Add an explicit `visit<bool>` specialization to `DebugStructVisitor`, matching its existing fixed-width arithmetic
specializations. Keep boolean dispatch in `BaseStructVisitor` and the production Parquet mapping unchanged.

The existing `TestSerdeStructVisitor.Inode` traversal is the regression test: because `InodeData` includes
`OriginFile`, successful compilation and execution prove that both new boolean fields are accepted. The existing
`TestSerdeSchemaBuilder.Inode` test continues to verify the production schema builder path.

## Scope

This change does not alter `OriginFile`, its serialized representation, analytics schemas, or visitor dispatch rules. It
does not introduce a generic fallback for unknown arithmetic types. The test visitor remains fail-closed for every type
without an explicit supported mapping.

## Validation

1. Build and run `test_analytics`.
2. Resume the cache-enabled all-target build with the CI-pinned libfuse 3.16.2 environment.
3. Run the cache-focused CTest targets to ensure the test-only change has no cache behavior impact.

