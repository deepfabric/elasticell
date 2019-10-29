package slab

type Pool interface {
	Alloc(int) []byte
	Free([]byte)
}

type NoPool struct{}

func (p *NoPool) Alloc(size int) []byte {
	return make([]byte, size)
}

func (p *NoPool) Free(_ []byte) {}

var _ Pool = (*NoPool)(nil)
var _ Pool = (*ChanPool)(nil)
var _ Pool = (*SyncPool)(nil)
var _ Pool = (*AtomPool)(nil)
