package raftstore

import (
	"bytes"

	"github.com/pilosa/pilosa/roaring"
)

const (
	opAdd = iota
	opRemove
	opClear
	opDel
)

type bitmapBatch struct {
	bitmaps       [][]byte
	bitmapAdds    []*roaring.Bitmap
	bitmapRemoves []*roaring.Bitmap
	ops           [][]int
}

func (rb *bitmapBatch) add(bm []byte, values ...uint64) {
	for idx, key := range rb.bitmaps {
		if bytes.Compare(key, bm) == 0 {
			rb.ops[idx] = append(rb.ops[idx], opAdd)
			rb.appendAdds(idx, values...)
			return
		}
	}

	value := acquireBitmap()
	value.Add(values...)

	rb.ops = append(rb.ops, []int{opAdd})
	rb.bitmaps = append(rb.bitmaps, bm)
	rb.bitmapAdds = append(rb.bitmapAdds, value)
	rb.bitmapRemoves = append(rb.bitmapRemoves, nil)
}

func (rb *bitmapBatch) remove(bm []byte, values ...uint64) {
	for idx, key := range rb.bitmaps {
		if bytes.Compare(key, bm) == 0 {
			rb.ops[idx] = append(rb.ops[idx], opRemove)
			rb.appendRemoves(idx, values...)
			return
		}
	}

	value := acquireBitmap()
	value.Add(values...)

	rb.ops = append(rb.ops, []int{opRemove})
	rb.bitmaps = append(rb.bitmaps, bm)
	rb.bitmapAdds = append(rb.bitmapAdds, nil)
	rb.bitmapRemoves = append(rb.bitmapRemoves, value)
}

func (rb *bitmapBatch) clear(bm []byte) {
	rb.clean(bm, opClear)
}

func (rb *bitmapBatch) del(bm []byte) {
	rb.clean(bm, opDel)
}

func (rb *bitmapBatch) clean(bm []byte, op int) {
	for idx, key := range rb.bitmaps {
		if bytes.Compare(key, bm) == 0 {
			rb.ops[idx] = append(rb.ops[idx], op)

			if rb.bitmapAdds[idx] != nil {
				releaseBitmap(rb.bitmapAdds[idx])
				rb.bitmapAdds[idx] = nil
			}

			if rb.bitmapRemoves[idx] != nil {
				releaseBitmap(rb.bitmapRemoves[idx])
				rb.bitmapRemoves[idx] = nil
			}
			return
		}
	}

	rb.ops = append(rb.ops, []int{op})
	rb.bitmaps = append(rb.bitmaps, bm)
	rb.bitmapAdds = append(rb.bitmapAdds, nil)
	rb.bitmapRemoves = append(rb.bitmapRemoves, nil)
}

func (rb *bitmapBatch) appendAdds(idx int, values ...uint64) {
	if rb.bitmapAdds[idx] == nil {
		rb.bitmapAdds[idx] = acquireBitmap()
	}

	rb.bitmapAdds[idx].Add(values...)
}

func (rb *bitmapBatch) appendRemoves(idx int, values ...uint64) {
	if rb.bitmapRemoves[idx] == nil {
		rb.bitmapRemoves[idx] = acquireBitmap()
	}

	rb.bitmapRemoves[idx].Add(values...)
}

func (rb *bitmapBatch) hasBatch() bool {
	return len(rb.bitmaps) > 0
}

func (rb *bitmapBatch) do(id uint64, store *Store) (uint64, error) {
	size := uint64(0)
	buf := bytes.NewBuffer(nil)
	bm := roaring.NewBTreeBitmap()
	eng := store.getKVEngine(id)
	wb := eng.NewWriteBatch()
	for idx, ops := range rb.ops {
		key := rb.bitmaps[idx]
		if ops[len(ops)-1] == opDel {
			err := wb.Delete(key)
			if err != nil {
				return 0, err
			}

			continue
		}

		value, err := eng.Get(key)
		if err != nil {
			return 0, err
		}

		bm.Containers.Reset()
		if len(value) > 0 {
			_, _, err = bm.ImportRoaringBits(value, false, false, 0)
			if err != nil {
				return 0, err
			}
		}

		for _, op := range ops {
			switch op {
			case opAdd:
				bm = bm.Union(rb.bitmapAdds[idx])
				break
			case opRemove:
				bm = bm.Xor(rb.bitmapRemoves[idx])
				break
			case opClear:
				bm.Containers.Reset()
				break
			case opDel:
				bm.Containers.Reset()
				break
			}
		}

		buf.Reset()
		_, err = bm.WriteTo(buf)
		if err != nil {
			return 0, err
		}

		err = wb.Set(key, buf.Bytes())
		if err != nil {
			return 0, err
		}

		diff := len(buf.Bytes()) - len(value)
		if diff > 0 {
			size += uint64(diff)
		}

	}

	return size, eng.Write(wb)
}

func (rb *bitmapBatch) reset() {
	for _, bm := range rb.bitmapAdds {
		if bm != nil {
			releaseBitmap(bm)
		}
	}

	for _, bm := range rb.bitmapRemoves {
		if bm != nil {
			releaseBitmap(bm)
		}
	}

	rb.ops = rb.ops[:0]
	rb.bitmaps = rb.bitmaps[:0]
	rb.bitmapAdds = rb.bitmapAdds[:0]
	rb.bitmapRemoves = rb.bitmapRemoves[:0]
}
