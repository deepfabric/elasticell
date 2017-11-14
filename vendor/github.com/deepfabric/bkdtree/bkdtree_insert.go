package bkdtree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"github.com/pkg/errors"
)

//Insert inserts given point. Fail if the tree is full.
func (bkd *BkdTree) Insert(point Point) (err error) {
	bkd.rwlock.Lock()
	defer bkd.rwlock.Unlock()
	if !bkd.open {
		err = errors.Errorf("(*BkdTree).Inset is not allowed at closed state")
		return
	}

	//insert into in-memory buffer t0m. If t0m is not full, return.
	bkd.insertT0M(point)
	bkd.NumPoints++
	if int(bkd.t0m.meta.NumPoints) < bkd.t0mCap {
		return
	}
	//find the smallest index k in [0, len(trees)) at which trees[k] is empty, or its capacity is no less than the sum of size of t0m + trees[0:k+1]
	k := bkd.getMinCompactPos()
	if k == len(bkd.trees) {
		kd := BkdSubTree{
			meta: KdTreeExtMeta{
				PointsOffEnd: 0,
				RootOff:      0,
				NumPoints:    0,
				LeafCap:      uint16(bkd.leafCap),
				IntraCap:     uint16(bkd.intraCap),
				NumDims:      uint8(bkd.NumDims),
				BytesPerDim:  uint8(bkd.BytesPerDim),
				PointSize:    uint8(bkd.pointSize),
				FormatVer:    0,
			},
		}
		bkd.trees = append(bkd.trees, kd)
	}

	err = bkd.compactTo(k)
	return
}

//caclulate the min compoint position. Returns len(bkd.trees) if not found.
func (bkd *BkdTree) getMinCompactPos() (k int) {
	//find the smallest index k in [0, len(trees)) at which trees[k] is empty, or its capacity is no less than the sum of size of t0m + trees[0:k+1]
	sum := int(bkd.t0m.meta.NumPoints)
	for k = 0; k < len(bkd.trees); k++ {
		if bkd.trees[k].meta.NumPoints == 0 {
			return
		}
		sum += int(bkd.trees[k].meta.NumPoints)
		capK := bkd.t0mCap << uint(k)
		if capK >= sum {
			return
		}
	}
	return
}

//compact T0M and trees[0:k+1] into tree[k]. Assumes write lock has been acquired.
func (bkd *BkdTree) compactTo(k int) (err error) {
	//extract all points from t0m and trees[0:k+1] into a file F
	fpK := bkd.TiPath(k)
	tmpFpK := fpK + ".tmp"
	tmpFK, err := os.OpenFile(tmpFpK, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	defer tmpFK.Close()

	err = bkd.extractT0M(tmpFK)
	if err != nil {
		return
	}
	for i := 0; i <= k; i++ {
		err = bkd.extractTi(tmpFK, i)
		if err != nil {
			return
		}
	}
	meta, err := bkd.bulkLoad(tmpFK)
	if err != nil {
		return
	}

	//empty T0M and Ti, 0<=i<k
	bkd.clearT0M()
	for i := 0; i <= k; i++ {
		if bkd.trees[i].meta.NumPoints <= 0 {
			continue
		} else if err = FileMunmap(bkd.trees[i].data); err != nil {
			return
		} else if err = bkd.trees[i].f.Close(); err != nil {
			err = errors.Wrap(err, "")
			return
		} else if err = os.Remove(bkd.trees[i].f.Name()); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		bkd.trees[i].meta.NumPoints = 0
	}
	if err = os.Rename(tmpFpK, fpK); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	fK, err := os.OpenFile(fpK, os.O_RDWR, 0600)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	data, err := FileMmap(fK)
	if err != nil {
		return
	}
	bkd.trees[k] = BkdSubTree{
		meta: *meta,
		f:    fK,
		data: data,
	}
	return
}

func writeMetaNumPoints(data []byte, meta *KdTreeExtMeta) {
	off := len(data) - KdTreeExtMetaSize
	off += int(unsafe.Offsetof(meta.NumPoints))
	binary.BigEndian.PutUint64(data[off:], meta.NumPoints)
}

func (bkd *BkdTree) insertT0M(point Point) {
	pae := PointArrayExt{
		data:        bkd.t0m.data,
		numPoints:   int(bkd.t0m.meta.NumPoints),
		byDim:       0, //not used
		bytesPerDim: bkd.BytesPerDim,
		numDims:     bkd.NumDims,
		pointSize:   bkd.pointSize,
	}
	pae.Append(point)
	bkd.t0m.meta.NumPoints++
	writeMetaNumPoints(bkd.t0m.data, &bkd.t0m.meta)
}

func (bkd *BkdTree) clearT0M() {
	bkd.t0m.meta.NumPoints = 0
	writeMetaNumPoints(bkd.t0m.data, &bkd.t0m.meta)
}

func (bkd *BkdTree) extractT0M(tmpF *os.File) (err error) {
	size := int(bkd.t0m.meta.NumPoints) * bkd.pointSize
	_, err = tmpF.Write(bkd.t0m.data[:size])
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (bkd *BkdTree) extractTi(dstF *os.File, idx int) (err error) {
	if bkd.trees[idx].meta.NumPoints <= 0 {
		return
	}
	srcF, err := os.Open(bkd.TiPath(idx))
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	defer srcF.Close()

	//depth-first extracting from the root node
	meta := &bkd.trees[idx].meta
	err = bkd.extractNode(dstF, bkd.trees[idx].data, meta, int(meta.RootOff))
	return
}

func (bkd *BkdTree) extractNode(dstF *os.File, data []byte, meta *KdTreeExtMeta, nodeOffset int) (err error) {
	var node KdTreeExtIntraNode
	bf := bytes.NewReader(data[nodeOffset:])
	err = node.Read(bf)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	for _, child := range node.Children {
		if child.Offset < meta.PointsOffEnd {
			//leaf node
			length := int(child.NumPoints) * int(meta.PointSize)
			_, err = dstF.Write(data[int(child.Offset) : int(child.Offset)+length])
			if err != nil {
				err = errors.Wrap(err, "")
				return
			}
		} else {
			//intra node
			err = bkd.extractNode(dstF, data, meta, int(child.Offset))
			if err != nil {
				return
			}
		}
	}
	return
}

func (bkd *BkdTree) bulkLoad(tmpF *os.File) (meta *KdTreeExtMeta, err error) {
	pointsOffEnd, err := tmpF.Seek(0, 1) //get current position
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	var data []byte
	if data, err = FileMmap(tmpF); err != nil {
		return
	}
	defer FileMunmap(data)

	numPoints := int(pointsOffEnd / int64(bkd.pointSize))
	rootOff, err1 := bkd.createKdTreeExt(tmpF, data, 0, numPoints, 0)
	if err1 != nil {
		err = err1
		return
	}
	//record meta info at end
	meta = &KdTreeExtMeta{
		PointsOffEnd: uint64(pointsOffEnd),
		RootOff:      uint64(rootOff),
		NumPoints:    uint64(numPoints),
		LeafCap:      uint16(bkd.leafCap),
		IntraCap:     uint16(bkd.intraCap),
		NumDims:      uint8(bkd.NumDims),
		BytesPerDim:  uint8(bkd.BytesPerDim),
		PointSize:    uint8(bkd.pointSize),
		FormatVer:    0,
	}
	err = binary.Write(tmpF, binary.BigEndian, meta)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func getCurrentOffset(f *os.File) (offset int64, err error) {
	offset, err = f.Seek(0, 1) //get current position
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (bkd *BkdTree) createKdTreeExt(tmpF *os.File, data []byte, begin, end, depth int) (offset int64, err error) {
	if begin >= end {
		err = errors.New(fmt.Sprintf("assertion begin>=end failed, begin %v, end %v", begin, end))
		return
	}

	splitDim := depth % bkd.NumDims
	numStrips := (end - begin + bkd.leafCap - 1) / bkd.leafCap
	if numStrips > bkd.intraCap {
		numStrips = bkd.intraCap
	}

	pae := PointArrayExt{
		data:        data[begin*bkd.pointSize:],
		numPoints:   end - begin,
		byDim:       splitDim,
		bytesPerDim: bkd.BytesPerDim,
		numDims:     bkd.NumDims,
		pointSize:   bkd.pointSize,
	}
	splitValues, splitPoses := SplitPoints(&pae, numStrips)

	children := make([]KdTreeExtNodeInfo, 0, numStrips)
	var childOffset int64
	for strip := 0; strip < numStrips; strip++ {
		posBegin := begin
		if strip != 0 {
			posBegin = begin + splitPoses[strip-1]
		}
		posEnd := end
		if strip != numStrips-1 {
			posEnd = begin + splitPoses[strip]
		}
		if posEnd-posBegin <= bkd.leafCap {
			info := KdTreeExtNodeInfo{
				Offset:    uint64(posBegin * bkd.pointSize),
				NumPoints: uint64(posEnd - posBegin),
			}
			children = append(children, info)
		} else {
			childOffset, err = bkd.createKdTreeExt(tmpF, data, posBegin, posEnd, depth+1)
			if err != nil {
				return
			}
			info := KdTreeExtNodeInfo{
				Offset:    uint64(childOffset),
				NumPoints: uint64(posEnd - posBegin),
			}
			children = append(children, info)
		}
	}

	offset, err = getCurrentOffset(tmpF)
	if err != nil {
		return
	}

	node := &KdTreeExtIntraNode{
		SplitDim:    uint32(splitDim),
		NumStrips:   uint32(numStrips),
		SplitValues: splitValues,
		Children:    children,
	}
	err = node.Write(tmpF)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}
