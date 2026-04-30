# RoutingInfo, ChainTable, and File Routing

This note summarizes how a file offset is routed to storage chains in 3FS.

## RoutingInfo vs. ChainTable

`RoutingInfo` is the client-visible routing snapshot fetched from mgmtd. It contains the global routing version and all routing objects needed by clients:

```text
RoutingInfo
  routingInfoVersion
  bootstrapping
  nodes: NodeId -> NodeInfo
  targets: TargetId -> TargetInfo
  chains: ChainId -> ChainInfo
  chainTables: ChainTableId -> ChainTableVersion -> ChainTable
```

`ChainTable` is only one part of `RoutingInfo`. It maps logical chain indexes used by file layouts to real `ChainId`s:

```text
ChainTable
  chainTableId
  chainTableVersion
  chains: vector<ChainId>
  desc
```

`ChainInfo` then describes a concrete chain: its `chainId`, `chainVersion`, target replicas, and preferred target order.

## Version Semantics

`RoutingInfoVersion` is the version of the whole routing snapshot. It changes when mgmtd publishes a newer routing view.

`ChainTableVersion` is scoped to one `ChainTable`. A file layout stores both `tableId` and `tableVersion`; when `tableVersion == 0`, lookup uses the latest version of that table.

## From File Offset to ChainRef

A file stores a `Layout` in its inode. The important fields are:

```text
tableId
tableVersion
chunkSize
stripeSize
chains
```

`chunkSize` answers: which chunk contains this byte offset?

```text
chunkIndex = offset / chunkSize
```

`stripeSize` answers: across how many chain indexes do chunks rotate?

```text
stripe = chunkIndex % stripeSize
```

The layout then builds:

```text
ChainRef(tableId, tableVersion, chains[stripe])
```

For example, with `chunkSize = 512 KiB`, `stripeSize = 4`, and `chains = [1, 2, 3, 4]`:

```text
chunk 0 -> stripe 0 -> ChainRef(..., 1)
chunk 1 -> stripe 1 -> ChainRef(..., 2)
chunk 2 -> stripe 2 -> ChainRef(..., 3)
chunk 3 -> stripe 3 -> ChainRef(..., 4)
chunk 4 -> stripe 0 -> ChainRef(..., 1)
```

## From ChainRef to ChainId

`RoutingInfo::getChainId(ChainRef)` decodes the reference:

```text
tid = ChainTableId
tv = ChainTableVersion
index = logical chain index
```

If `tid == 0 && tv == 0`, `index` is treated directly as a `ChainId`.

Otherwise, `RoutingInfo` finds the matching `ChainTable`. Then it converts the logical index to a vector position:

```text
position = (index - 1) % table.chains.size()
chainId = table.chains[position]
```

The index is 1-based. `index == 0` is invalid.

## Why Both Chunk and Stripe Exist

`chunkSize` defines the storage/data unit for a file. It affects chunk id calculation and IO splitting.

`stripeSize` defines the file's distribution width across chain indexes. It affects parallelism and load distribution. A larger stripe size spreads consecutive chunks over more chains.

In short:

```text
chunkSize: byte offset -> chunk number
stripeSize: chunk number -> chain index slot
ChainTable: chain index -> ChainId
RoutingInfo: ChainId -> targets and nodes
```

## RoutingInfo Refresh

The mgmtd client auto-refreshes routing info by default every `10s`. The background task runs `refreshRoutingInfo(false)`.

This is not a full fetch every time. The client sends its current `routingInfoVersion`; if the server has the same version, it returns no routing payload. A full `RoutingInfo` is returned only when the client is behind, or when callers force refresh with `refreshRoutingInfo(true)`.

## Code Pointers

- `src/fbs/mgmtd/RoutingInfo.h`: `RoutingInfo`, `chainTables`, `chains`, lookup helpers.
- `src/fbs/mgmtd/ChainTable.h`: `ChainTable` fields.
- `src/fbs/mgmtd/ChainInfo.h`: concrete chain replica information.
- `src/fbs/mgmtd/RoutingInfo.cc`: `getChainTable()` and `getChainId()`.
- `src/fbs/meta/Schema.h`: file `Layout` fields.
- `src/fbs/meta/Schema.cc`: `File::getChainId()` and `Layout::getChainOfChunk()`.
- `src/client/mgmtd/MgmtdClient.h`: default `auto_refresh_interval = 10_s`.
- `src/client/mgmtd/MgmtdClient.cc`: auto refresh and versioned fetch logic.
