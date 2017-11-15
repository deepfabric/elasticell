package bkdtree

import (
	"bytes"

	"github.com/pkg/errors"
)

//Intersect does window query
func (bkd *BkdTree) Intersect(visitor IntersectVisitor) (err error) {
	bkd.rwlock.RLock()
	defer bkd.rwlock.RUnlock()
	if !bkd.open {
		err = errors.Errorf("(*BkdTree).Intersect is not allowed at closed state")
		return
	}

	bkd.intersectT0M(visitor)
	for i := 0; i < len(bkd.trees); i++ {
		err = bkd.intersectTi(visitor, i)
		if err != nil {
			return
		}
	}
	return
}

func (bkd *BkdTree) intersectT0M(visitor IntersectVisitor) {
	lowP := visitor.GetLowPoint()
	highP := visitor.GetHighPoint()
	pae := PointArrayExt{
		data:        bkd.t0m.data,
		numPoints:   int(bkd.t0m.meta.NumPoints),
		byDim:       0, //not used
		bytesPerDim: bkd.BytesPerDim,
		numDims:     bkd.NumDims,
		pointSize:   bkd.pointSize,
	}
	for i := 0; i < pae.numPoints; i++ {
		point := pae.GetPoint(i)
		if point.Inside(lowP, highP) {
			visitor.VisitPoint(point)
		}
	}
	return
}

func (bkd *BkdTree) intersectTi(visitor IntersectVisitor, idx int) (err error) {
	if bkd.trees[idx].meta.NumPoints <= 0 {
		return
	}
	//depth-first visiting from the root node
	meta := &bkd.trees[idx].meta
	err = bkd.intersectNode(visitor, bkd.trees[idx].data, meta, int(meta.RootOff))
	return
}

func (bkd *BkdTree) intersectNode(visitor IntersectVisitor, data []byte,
	meta *KdTreeExtMeta, nodeOffset int) (err error) {
	lowP := visitor.GetLowPoint()
	highP := visitor.GetHighPoint()
	var node KdTreeExtIntraNode
	bf := bytes.NewReader(data[nodeOffset:])
	err = node.Read(bf)
	if err != nil {
		return
	}
	lowVal := lowP.Vals[node.SplitDim]
	highVal := highP.Vals[node.SplitDim]
	for i, child := range node.Children {
		if child.NumPoints <= 0 {
			continue
		}
		if i < int(node.NumStrips)-1 && node.SplitValues[i] < lowVal {
			continue
		}
		if i != 0 && node.SplitValues[i-1] > highVal {
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
			for i := 0; i < pae.numPoints; i++ {
				point := pae.GetPoint(i)
				if point.Inside(lowP, highP) {
					visitor.VisitPoint(point)
				}
			}
		} else {
			//intra node
			err = bkd.intersectNode(visitor, data, meta, int(child.Offset))
		}
		if err != nil {
			return
		}
	}
	return
}
