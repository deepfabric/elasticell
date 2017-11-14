package bkdtree

import (
	"math/rand"
	"sort"
)

type U64Slice []uint64

func (a U64Slice) Len() int           { return len(a) }
func (a U64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a U64Slice) Less(i, j int) bool { return a[i] < a[j] }

type KdTreeNode interface {
	intersect(visitor IntersectVisitor, numDims int)
	insert(point Point, numDims int)
	erase(point Point, numDims int) bool
}

type KdTreeIntraNode struct {
	splitDim    int
	splitValues []uint64
	children    []KdTreeNode
}

type KdTreeLeafNode struct {
	points []Point
}

type IntersectVisitor interface {
	GetLowPoint() Point
	GetHighPoint() Point
	VisitPoint(point Point)
}

type IntersectCollector struct {
	LowPoint  Point
	HighPoint Point
	Points    []Point
}

func (d *IntersectCollector) GetLowPoint() Point     { return d.LowPoint }
func (d *IntersectCollector) GetHighPoint() Point    { return d.HighPoint }
func (d *IntersectCollector) VisitPoint(point Point) { d.Points = append(d.Points, point) }

type KdTree struct {
	root     KdTreeNode
	NumDims  int
	leafCap  int // limit of points a leaf node can hold
	intraCap int // limit of children of a intra node can hold
}

func NewKdTree(points []Point, numDims, leafCap, intraCap int) (kd *KdTree) {
	if len(points) == 0 || numDims <= 0 ||
		leafCap <= 0 || leafCap >= int(^uint16(0)) || intraCap <= 2 || intraCap >= int(^uint16(0)) {
		return
	}
	kd = &KdTree{
		root:     createKdTree(points, 0, numDims, leafCap, intraCap),
		NumDims:  numDims,
		leafCap:  leafCap,
		intraCap: intraCap,
	}
	return
}

func createKdTree(points []Point, depth, numDims, leafCap, intraCap int) KdTreeNode {
	if len(points) == 0 {
		return nil
	}
	if len(points) <= leafCap {
		pointsCopy := make([]Point, len(points))
		copy(pointsCopy, points)
		ret := &KdTreeLeafNode{
			points: pointsCopy,
		}
		return ret
	}

	splitDim := depth % numDims
	numStrips := (len(points) + leafCap - 1) / leafCap
	if numStrips > intraCap {
		numStrips = intraCap
	}

	pam := PointArrayMem{
		points: points,
		byDim:  splitDim,
	}

	splitValues, splitPoses := SplitPoints(&pam, numStrips)

	children := make([]KdTreeNode, 0, numStrips)
	for strip := 0; strip < numStrips; strip++ {
		posBegin := 0
		if strip != 0 {
			posBegin = splitPoses[strip-1]
		}
		posEnd := len(points)
		if strip != numStrips-1 {
			posEnd = splitPoses[strip]
		}
		child := createKdTree(points[posBegin:posEnd], depth+1, numDims, leafCap, intraCap)
		children = append(children, child)
	}
	ret := &KdTreeIntraNode{
		splitDim:    splitDim,
		splitValues: splitValues,
		children:    children,
	}
	return ret
}

func (n *KdTreeIntraNode) intersect(visitor IntersectVisitor, numDims int) {
	lowVal := visitor.GetLowPoint().Vals[n.splitDim]
	highVal := visitor.GetHighPoint().Vals[n.splitDim]
	numSplits := len(n.splitValues)
	//calculate children[begin:end) need to visit
	end := sort.Search(numSplits, func(i int) bool { return n.splitValues[i] > highVal })
	begin := sort.Search(end, func(i int) bool { return n.splitValues[i] >= lowVal })
	end++
	for strip := begin; strip < end; strip++ {
		n.children[strip].intersect(visitor, numDims)
	}
}

func (n *KdTreeLeafNode) intersect(visitor IntersectVisitor, numDims int) {
	lowPoint := visitor.GetLowPoint()
	highPoint := visitor.GetHighPoint()
	for _, point := range n.points {
		isInside := point.Inside(lowPoint, highPoint)
		if isInside {
			visitor.VisitPoint(point)
		}
	}
}

func (t *KdTree) Intersect(visitor IntersectVisitor) {
	t.root.intersect(visitor, t.NumDims)
}

func (n *KdTreeIntraNode) insert(point Point, numDims int) {
	lowVal := point.Vals[n.splitDim]
	highVal := lowVal
	numSplits := len(n.splitValues)
	//calculate children[begin:end) need to visit
	end := sort.Search(numSplits, func(i int) bool { return n.splitValues[i] > highVal })
	begin := sort.Search(end, func(i int) bool { return n.splitValues[i] >= lowVal })
	end++
	//if multiple strips could cover the point, select one randomly.
	strip := begin + rand.Intn(end-begin)
	n.children[strip].insert(point, numDims)
}

func (n *KdTreeLeafNode) insert(point Point, numDims int) {
	//append blindly, no rebalance
	n.points = append(n.points, point)
}

func (t *KdTree) Insert(point Point) {
	t.root.insert(point, t.NumDims)
}

func (n *KdTreeIntraNode) erase(point Point, numDims int) (found bool) {
	lowVal := point.Vals[n.splitDim]
	highVal := lowVal
	numSplits := len(n.splitValues)
	//calculate children[begin:end) need to visit
	end := sort.Search(numSplits, func(i int) bool { return n.splitValues[i] > highVal })
	begin := sort.Search(end, func(i int) bool { return n.splitValues[i] >= lowVal })
	end++
	//if multiple strips could cover the point, iterate them. And stop iteration if found at middle way.
	for strip := begin; strip < end; strip++ {
		found = n.children[strip].erase(point, numDims)
		if found {
			break
		}
	}
	return
}

func (n *KdTreeLeafNode) erase(point Point, numDims int) (found bool) {
	found = false
	idx := len(n.points)
	for i, point2 := range n.points {
		//assumes each point's userData is unique
		if point.Equal(point2) {
			idx = i
			break
		}
	}
	if idx < len(n.points) {
		n.points = append(n.points[:idx], n.points[idx+1:]...)
		found = true
	}
	return
}

func (t *KdTree) Erase(point Point) {
	t.root.erase(point, t.NumDims)
}
