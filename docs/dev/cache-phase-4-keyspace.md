# Cache Phase 4 FDB keyspace

Record date: 2026-08-10

Phase 4 and its post-acceptance recovery hardening reserve the following globally unique four-byte prefixes. Existing
canonical records and encodings are unchanged; the new secondary indexes can be rebuilt from their canonical tables.

| Table | Prefix | Key suffix | Value |
| --- | --- | --- | --- |
| Cache Block state index | `CBST` | state `u8` + inode `u64` + block `u32` | `CacheBlockRecord` |
| Cache Block index migration | `CMIG` | schema `u8` | one-byte completion marker |
| Pin owner lease | `PNOL` | owner kind `u8` + owner UUID | `PinOwnerLease` |
| Upload Job | `UPJB` | 16-byte Upload Job UUID | `UploadJobRecord`, at most 96 KiB |
| Active Upload Job index | `UPAJ` | 16-byte Upload Job UUID | `UploadJobRecord`, at most 96 KiB |
| OPEN upload lease index | `UPOL` | lease expiry `u64` + Upload Job UUID | `UploadJobRecord`, at most 96 KiB |
| Upload state index | `UPSJ` | state `u8` + Upload Job UUID | `UploadJobRecord`, at most 96 KiB |
| Upload index migration | `UPMG` | schema `u8` | one-byte completion marker |

Upload Job keys use opaque, stable UUID bytes and are ordered bytewise for cursor pagination. Decoders require the exact
key length, the `UPJB` prefix, and a nonzero Job ID. Values are Serde-encoded `UploadJobRecord` objects; the store checks
the logical part-count and tag bounds as well as the serialized 96 KiB limit before mutation, keeping values below the
100,000-byte FoundationDB value limit.

`CBST` is transactionally maintained with `CBLK`. Before `CMIG` exists, reconcile scans canonical `CBLK` records,
backfills every record in each fetched range, and writes the marker only after the canonical range is exhausted. Once
the marker exists, recovery and reconcile use the state index instead of scanning the canonical table.

`UPAJ`, `UPOL`, and `UPSJ` are transactionally maintained with `UPJB`. The startup upload recovery scan backfills old
jobs page by page and writes `UPMG` only after the terminal page. `UPOL` orders OPEN jobs by their persisted writer
lease deadline, while `UPSJ` supports bounded state selection. Index entries contain the complete fenced record so a
stale or mismatched key/value pair fails as corruption instead of being silently accepted.

`PNOL` replaces periodic per-block pin renewal for active job owners. Existing block and owner pin indexes remain the
source of block membership; owner-lease renewal only extends the common liveness fence.

All prefixes above are disjoint from the Phase 3 tables `PFJB`, `PFPL`, `PNBL`, and `PNOW`, the earlier cache tables
`CBLK`, `CCAP`, `CREF`, `CCLN`, `CEVC`, and `CEVD`, and all non-cache tables.
