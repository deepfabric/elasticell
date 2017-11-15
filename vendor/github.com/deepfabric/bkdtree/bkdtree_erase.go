package bkdtree

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
)

//Erase erases given point.
func (bkd *BkdTree) Erase(point Point) (found bool, err error) {
	bkd.rwlock.Lock()
	defer bkd.rwlock.Unlock()
	if !bkd.open {
		err = errors.Errorf("(*BkdTree).Erase is not allowed at closed state")
		return
	}

	//Query T0M with p; if found, delete it and return.
	found = bkd.eraseT0M(point)
	if found {
		bkd.NumPoints--
		return
	}

	//Query each non-empty tree in the forest with p; if found, delete it and return
	for i := 0; i < len(bkd.trees); i++ {
		found, err = bkd.eraseTi(point, i)
		if err != nil {
			return
		} else if found {
			bkd.NumPoints--
			return
		}
	}
	return
}

func (bkd *BkdTree) eraseT0M(point Point) (found bool) {
	pae := PointArrayExt{
		data:        bkd.t0m.data,
		numPoints:   int(bkd.t0m.meta.NumPoints),
		byDim:       0, //not used
		bytesPerDim: bkd.BytesPerDim,
		numDims:     bkd.NumDims,
		pointSize:   bkd.pointSize,
	}
	found = pae.Erase(point)
	if found {
		bkd.t0m.meta.NumPoints--
		writeMetaNumPoints(bkd.t0m.data, &bkd.t0m.meta)
	}
	return
}

func (bkd *BkdTree) eraseTi(point Point, idx int) (found bool, err error) {
	if bkd.trees[idx].meta.NumPoints <= 0 {
		return
	}

	//depth-first erasing from the root node
	meta := &bkd.trees[idx].meta
	found, err = bkd.eraseNode(point, bkd.trees[idx].data, meta, int(meta.RootOff))
	if err != nil {
		return
	}
	if found {
		bkd.trees[idx].meta.NumPoints--
		writeMetaNumPoints(bkd.trees[idx].data, &bkd.trees[idx].meta)
		return
	}
	return
}

func (bkd *BkdTree) eraseNode(point Point, data []byte, meta *KdTreeExtMeta, nodeOffset int) (found bool, err error) {
	var node KdTreeExtIntraNode
	br := bytes.NewReader(data[nodeOffset:])
	err = node.Read(br)
	if err != nil {
		return
	}
	for i, child := range node.Children {
		if child.NumPoints <= 0 {
			continue
		}
		if child.Offset < meta.PointsOffEnd {
			//leaf node
			pae := PointArrayExt{
				data:        data[int(child.Offset):],
				numPoints:   int(child.NumPoints),
				byDim:       0, //not used
				bytesPerDim: bkd.BytesPerDim,
				numDims:     bkd.NumDims,
				pointSize:   bkd.pointSize,
			}
			found = pae.Erase(point)
		} else {
			//intra node
			found, err = bkd.eraseNode(point, data, meta, int(child.Offset))
		}
		if err != nil {
			return
		}
		if found {
			child.NumPoints--
			//Attention: offset calculation shall be synced with KdTreeExtIntraNode definion.
			off := nodeOffset + 8*int(node.NumStrips) + 16*i + 8
			binary.BigEndian.PutUint64(data[off:], child.NumPoints)
			break
		}
	}
	return
}
