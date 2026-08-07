# Cache Phase 4 FDB keyspace

Record date: 2026-08-07

Phase 4 reserves the following new, globally unique four-byte prefix. Existing prefixes and encodings are unchanged.

| Table | Prefix | Key suffix | Maximum value |
| --- | --- | --- | --- |
| Upload Job | `UPJB` | 16-byte Upload Job UUID | 96 KiB |

Upload Job keys use opaque, stable UUID bytes and are ordered bytewise for cursor pagination. Decoders require the exact
key length, the `UPJB` prefix, and a nonzero Job ID. Values are Serde-encoded `UploadJobRecord` objects; the store checks
the logical part-count and tag bounds as well as the serialized 96 KiB limit before mutation, keeping values below the
100,000-byte FoundationDB value limit.

`UPJB` is disjoint from all Phase 3 and earlier cache tables, including `PFJB`, `PFPL`, `PNBL`, and `PNOW`.
