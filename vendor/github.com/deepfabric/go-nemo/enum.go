package gonemo

type DBType int

const (
	kNONE_DB DBType = iota
	kKV_DB
	kHASH_DB
	kLIST_DB
	kZSET_DB
	kSET_DB
	kALL
)

type BitOpType int

const (
	kBitOpNot BitOpType = iota
	kBitOpAnd
	kBitOpOr
	kBitOpXor
	kBitOpDefault
)

type Aggregate int

const (
	SUM Aggregate = iota
	MIN
	MAX
)

const (
	ALL_DB  string = "all"
	KV_DB   string = "kv"
	HASH_DB string = "hash"
	LIST_DB string = "list"
	ZSET_DB string = "zset"
	SET_DB  string = "set"
	META_DB string = "meta"
	RAFT_DB string = "raft"
)
