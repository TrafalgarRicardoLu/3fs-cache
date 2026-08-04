# Cache Phase 3 FDB keyspace

Record date: 2026-08-04

Phase 3 reserves four new, globally unique four-byte prefixes. Existing prefixes and their encodings are unchanged.

| Table | Prefix | Key suffix | Maximum value |
| --- | --- | --- | --- |
| Prefetch Job | `PFJB` | 16-byte Job UUID | 96 KiB |
| Prefetch Plan | `PFPL` | 16-byte Job UUID + big-endian inode `u64` + big-endian block `u32` | 16 KiB |
| Pin by block | `PNBL` | big-endian inode `u64` + big-endian block `u32` + owner kind `u8` + owner UUID | 16 KiB |
| Pin by owner | `PNOW` | owner kind `u8` + owner UUID + big-endian inode `u64` + big-endian block `u32` | 16 KiB |

`PrefetchJobSpec` limits the combined source payload to 64 KiB, leaving room for Serde framing, counters, cursor, error,
and future appended fields below the 100,000-byte FoundationDB value limit. Store implementations must reject the
serialized value before mutation if it exceeds the table limit above; they must not rely only on the logical field
limits.

All key decoders require the exact key length, expected prefix, nonzero IDs, valid block identity, and valid pin owner.
The numeric block components use big-endian order so a bytewise FDB range scan has `(inode, block)` ordering. Job UUID
bytes are opaque and stable. The two pin indexes encode the same identity in different orders and must be mutated in one
transaction.

Prefix ranges are disjoint because every table begins with a distinct fixed four-byte prefix. No Phase 3 prefix reuses
or changes `CBLK`, `CCAP`, `CREF`, `CCLN`, `CEVC`, `CEVD`, or any non-cache table.
