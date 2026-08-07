#ifndef DEFINE_PREFIX
#define DEFINE_PREFIX(...)
#endif

// clang-format off
DEFINE_PREFIX(Inode,            "INOD")
DEFINE_PREFIX(Dentry,           "DENT")
DEFINE_PREFIX(MetaDistributor,  "META")
DEFINE_PREFIX(User,             "USER")
DEFINE_PREFIX(NodeTable,        "NODE")
// Single is used for some single keys (not tables)
DEFINE_PREFIX(Single,           "SING")
DEFINE_PREFIX(ChainTable,       "CHIT")
DEFINE_PREFIX(ChainInfo,        "CHIF")
DEFINE_PREFIX(InodeSession,     "INOS")
DEFINE_PREFIX(ClientSession,    "CLIS") // deperated
DEFINE_PREFIX(MetaIdempotent,   "IDEM")
DEFINE_PREFIX(UniversalTags,    "UTGS")
DEFINE_PREFIX(Config,           "CONF")
DEFINE_PREFIX(TargetInfo,       "TGIF")
DEFINE_PREFIX(CacheBlock,       "CBLK")
DEFINE_PREFIX(CacheCapacity,    "CCAP")
DEFINE_PREFIX(CacheRefresh,     "CREF")
DEFINE_PREFIX(CacheCleanup,     "CCLN")
DEFINE_PREFIX(CacheEventCursor, "CEVC")
DEFINE_PREFIX(CacheEventDead,   "CEVD")
DEFINE_PREFIX(PrefetchJob,      "PFJB")
DEFINE_PREFIX(PrefetchPlan,     "PFPL")
DEFINE_PREFIX(PinByBlock,       "PNBL")
DEFINE_PREFIX(PinByOwner,       "PNOW")
DEFINE_PREFIX(UploadJob,        "UPJB")
DEFINE_PREFIX(KvTable,          "KVTB")
DEFINE_PREFIX(KvNamespace,      "KVNS")
DEFINE_PREFIX(KvWorkerGroup,    "KVWG")
// clang-format on

#undef DEFINE_PREFIX
