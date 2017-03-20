package pdserver

import (
	"github.com/deepfabric/elasticell/pkg/storage/meta"
)

type storeRuntime struct {
	store *meta.StoreMeta
}

type storeCache struct {
	stores map[uint64]*storeRuntime
}

func newStoreRuntime(store *meta.StoreMeta) *storeRuntime {
	return &storeRuntime{
		store: store,
	}
}

func newStoreCache() *storeCache {
	sc := new(storeCache)
	sc.stores = make(map[uint64]*storeRuntime)

	return sc
}
