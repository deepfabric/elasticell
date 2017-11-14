package bkdtree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/juju/testing/checkers"
	"github.com/pkg/errors"
)

func (n *KdTreeExtIntraNode) equal(n2 *KdTreeExtIntraNode) (res bool) {
	if n.SplitDim != n2.SplitDim || n.NumStrips != n2.NumStrips ||
		len(n.SplitValues) != len(n2.SplitValues) ||
		len(n.Children) != len(n2.Children) {
		res = false
		return
	}
	for i := 0; i < len(n.SplitValues); i++ {
		if n.SplitValues[i] != n2.SplitValues[i] {
			res = false
			return
		}
	}
	for i := 0; i < len(n.Children); i++ {
		if n.Children[i] != n2.Children[i] {
			res = false
			return
		}
	}
	res = true
	return
}

func TestIntraNodeReadWrite(t *testing.T) {
	n := KdTreeExtIntraNode{
		SplitDim:    1,
		NumStrips:   4,
		SplitValues: []uint64{3, 5, 7},
		Children: []KdTreeExtNodeInfo{
			{Offset: 0, NumPoints: 7},
			{Offset: 10, NumPoints: 9},
			{Offset: 20, NumPoints: 137},
			{Offset: 180, NumPoints: 999},
		},
	}
	bf := new(bytes.Buffer)
	if err := n.Write(bf); err != nil {
		t.Fatalf("%+v", err)
	}

	var n2 KdTreeExtIntraNode
	if err := n2.Read(bf); err != nil {
		t.Fatalf("%+v", err)
	}

	if !n.equal(&n2) {
		t.Fatalf("KdTreeExtIntraNode changes after encode and decode: %v, %v", n, n2)
	}
}
func TestBkdInsert(t *testing.T) {
	t0mCap := 1000
	treesCap := 5
	bkdCap := t0mCap<<uint(treesCap) - 1
	leafCap := 50
	intraCap := 4
	numDims := 2
	bytesPerDim := 4
	dir := "/tmp"
	prefix := "bkd"
	bkd, err := NewBkdTree(t0mCap, leafCap, intraCap, numDims, bytesPerDim, dir, prefix)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	maxVal := uint64(1000)
	size := bkdCap + 1
	points := NewRandPoints(numDims, maxVal, size)

	//insert points
	for i := 0; i < bkdCap; i++ {
		err := bkd.Insert(points[i])
		if err != nil {
			t.Fatalf("bkd.Insert failed, i=%v, err: %+v", i, err)
		}
		if bkd.NumPoints != i+1 {
			t.Fatalf("incorrect numPoints. numPoints=%v, i=%v", bkd.NumPoints, i)
		}
		remained := bkd.NumPoints % bkd.t0mCap
		quotient := bkd.NumPoints / bkd.t0mCap
		if int(bkd.t0m.meta.NumPoints) != remained {
			t.Fatalf("bkd.numPoints %d, bkd.t0m %d is incorect, want %d", bkd.NumPoints, int(bkd.t0m.meta.NumPoints), remained)
		}
		for i := 0; i < len(bkd.trees); i++ {
			tiCap := bkd.t0mCap << uint(i)
			want := tiCap * (quotient % 2)
			if bkd.trees[i].meta.NumPoints != uint64(want) {
				t.Fatalf("bkd.numPoints %d, bkd.tree[%d].numPoints %d is incorrect, want %d", bkd.NumPoints, i, bkd.trees[i].meta.NumPoints, want)
			}
			quotient >>= 1
		}
	}
}

func prepareBkdTree(maxVal uint64) (bkd *BkdTree, points []Point, err error) {
	t0mCap := 1000
	treesCap := 5
	bkdCap := t0mCap<<uint(treesCap) - 1
	leafCap := 50
	intraCap := 4
	numDims := 2
	bytesPerDim := 4
	dir := "/tmp"
	prefix := "bkd"
	bkd, err = NewBkdTree(t0mCap, leafCap, intraCap, numDims, bytesPerDim, dir, prefix)
	if err != nil {
		return
	}
	//fmt.Printf("created BkdTree %v\n", bkd)

	size := bkdCap
	points = NewRandPoints(numDims, maxVal, size)
	for i := 0; i < bkdCap; i++ {
		err = bkd.Insert(points[i])
		if err != nil {
			err = errors.Errorf("bkd.Insert failed, i=%v, err: %+v", i, err)
		}
	}
	return
}

func TestBkdIntersect(t *testing.T) {
	var maxVal uint64 = 1000
	bkd, points, err := prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	var lowPoint, highPoint Point
	var visitor *IntersectCollector

	//some intersect
	lowPoint = points[7]
	highPoint = lowPoint
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(visitor.Points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	var matched int
	for _, point := range visitor.Points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
		if point.Equal(lowPoint) {
			matched++
		}
	}
	if matched != 1 {
		t.Errorf("found %d points equal to %v", matched, lowPoint)
	}

	//all intersect
	lowPoint = Point{[]uint64{0, 0, 0}, 0}
	highPoint = Point{[]uint64{maxVal, maxVal, maxVal}, 0}
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(visitor.Points) != len(points) {
		t.Errorf("found %d matchs, want %d", len(visitor.Points), len(points))
	}
}

func verifyBkdMeta(bkd *BkdTree) (err error) {
	cnt := int(bkd.t0m.meta.NumPoints)
	var f *os.File
	for i := 0; i < len(bkd.trees); i++ {
		if bkd.trees[i].meta.NumPoints <= 0 {
			continue
		}
		f, err = os.OpenFile(bkd.TiPath(i), os.O_RDONLY, 0)
		if err != nil {
			return
		}
		_, err = f.Seek(-int64(KdTreeExtMetaSize), 2)
		if err != nil {
			return
		}
		var meta KdTreeExtMeta
		err = binary.Read(f, binary.BigEndian, &meta)
		if err != nil {
			return
		}
		if meta != bkd.trees[i].meta {
			err = errors.Errorf("bkd.trees[%d].meta does not match file content, has %v, want %v", i, bkd.trees[i].meta, meta)
			return
		}
		cnt += int(meta.NumPoints)
	}
	if cnt != bkd.NumPoints {
		err = errors.Errorf("bkd.numPoints does not match file content, has %v, want %v", bkd.NumPoints, cnt)
		return
	}
	return
}

func countPoint(bkd *BkdTree, point Point) (cnt int, err error) {
	lowPoint, highPoint := point, point
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		return
	}
	for _, p := range visitor.Points {
		if p.Equal(point) {
			cnt++
		}
	}
	return
}

func TestBkdErase(t *testing.T) {
	var maxVal uint64 = 1000
	bkd, points, err := prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	var target Point
	var cnt int
	var found bool

	//erase an non-existing point
	target = points[17]
	target.UserData = uint64(len(points))
	found, err = bkd.Erase(target)
	if err != nil {
		t.Fatalf("%v", err)
	} else if found {
		t.Fatalf("point %v found, want non-existing", target)
	}

	//erase an existing point, verify the point really erased.
	target = points[13]
	found, err = bkd.Erase(target)
	if err != nil {
		t.Fatalf("%+v", err)
	} else if !found {
		t.Fatalf("point %v not found", target)
	} else if bkd.NumPoints != len(points)-1 {
		t.Fatalf("incorrect bkd.numPoints %d, want %d", bkd.NumPoints, len(points)-1)
	} else if err = verifyBkdMeta(bkd); err != nil {
		t.Fatalf("%+v", err)
	}

	cnt, err = countPoint(bkd, target)
	if err != nil {
		t.Fatalf("%+v", err)
	} else if cnt != 0 {
		t.Errorf("point %v still exists", target)
	}
	//there's room for insertion
	err = bkd.Insert(target)
	if err != nil {
		t.Fatalf("bkd.Insert failed, err: %+v", err)
	} else if bkd.NumPoints != len(points) {
		t.Fatalf("incorrect bkd.numPoints %d, want %d", bkd.NumPoints, len(points))
	} else if err = verifyBkdMeta(bkd); err != nil {
		t.Fatalf("%+v", err)
	}

	cnt, err = countPoint(bkd, target)
	if err != nil {
		t.Fatalf("%+v", err)
	} else if cnt != 1 {
		t.Errorf("point %v still exists", target)
	}
}

func TestBkdDestroy(t *testing.T) {
	var bkd *BkdTree
	var err error
	var maxVal uint64 = 1000
	bkd, _, err = prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if err = bkd.Destroy(); err != nil {
		t.Fatalf("%+v", err)
	}
	//Verify t0m has been removed
	if _, err = NewBkdTreeExt(bkd.dir, bkd.prefix); err == nil {
		t.Fatalf("%s is still there", bkd.t0m.f.Name())
	}
}

func TestBkdOpenClose(t *testing.T) {
	var bkd, bkd2 *BkdTree
	var err error
	var maxVal uint64 = 1000
	bkd, _, err = prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if err = bkd.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	if bkd2, err = NewBkdTreeExt(bkd.dir, bkd.prefix); err != nil {
		t.Fatalf("%+v", err)
	}
	if err = bkd2.Close(); err != nil {
		t.Fatalf("%+v", err)
	}
	//Compare two structs recursively and record the difference.
	//TODO: How to ignore specific fields effciently?
	bkd2.t0m.data, bkd.t0m.data = make([]byte, 0), make([]byte, 0)
	for i := 0; i < len(bkd.trees); i++ {
		bkd2.trees[i].data, bkd.trees[i].data = make([]byte, 0), make([]byte, 0)
	}
	isEqual, err := checkers.DeepEqual(bkd, bkd2)
	if !isEqual {
		t.Fatalf("bkd changed with close and open. %+v", err)
	}
}

func TestBkdCompact(t *testing.T) {
	var bkd *BkdTree
	var points []Point
	var err error
	var maxVal uint64 = 1000
	bkd, points, err = prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	/*
		points distribution of bkd: T0M(1), 1, 2, 4, 8, 16
		test case:
		1. remove all points of trees[len-2] and trees[len-4].
		2. compact
		3. verify points of T0M and trees 0..len-2(inclusive) are moved to trees[len-2].
	*/
	treeLen := len(bkd.trees)
	tree1End := bkd.t0mCap << uint(treeLen-1)
	tree2End := tree1End + bkd.t0mCap<<uint(treeLen-2)
	tree3End := tree2End + bkd.t0mCap<<uint(treeLen-3)
	tree4End := tree3End + bkd.t0mCap<<uint(treeLen-4)

	sum := int(bkd.t0m.meta.NumPoints)
	for i := 0; i < treeLen; i++ {
		if i == treeLen-4 || i == treeLen-2 || i == treeLen-1 {
			continue
		}
		sum += int(bkd.trees[i].meta.NumPoints)
	}

	for i := tree1End; i < tree2End; i++ {
		found, err := bkd.Erase(points[i])
		if err != nil {
			t.Fatalf("%+v", err)
		} else if !found {
			t.Fatalf("point %v not found", points[i])
		}
	}

	for i := tree3End; i < tree4End; i++ {
		found, err := bkd.Erase(points[i])
		if err != nil {
			t.Fatalf("%+v", err)
		} else if !found {
			t.Fatalf("point %v not found", points[i])
		}
	}

	if err = bkd.Compact(); err != nil {
		t.Fatalf("%+v", err)
	}

	if bkd.t0m.meta.NumPoints != 0 {
		t.Fatalf("bkd.t0m.NumPoints is incorrect, is %d, want %d", bkd.t0m.meta.NumPoints, 0)
	}
	for i := 0; i < treeLen-2; i++ {
		if bkd.trees[i].meta.NumPoints != 0 {
			t.Fatalf("bkd.trees[%d].NumPoints is incorrect, is %d, want %d", i, bkd.trees[i].meta.NumPoints, 0)
		}
	}
	if int(bkd.trees[treeLen-2].meta.NumPoints) != sum {
		t.Fatalf("bkd.trees[%d].NumPoints is incorrect, is %d, want %d", treeLen-2, int(bkd.trees[treeLen-2].meta.NumPoints), sum)
	}
}

func bkdCloser(abort chan interface{}, bkd *BkdTree) {
	var interval time.Duration = 5 * time.Second
FOR_LOOP:
	for {
		select {
		case <-abort:
			break FOR_LOOP //break for
		default: // adding default makes select nonblocking
		}

		if err := bkd.Close(); err != nil {
			panic(err)
		}
		if err := bkd.Open(); err != nil {
			panic(err)
		}
		time.Sleep(interval)
	}
}

func bkdWriter(abort chan interface{}, bkd *BkdTree, points []Point) {
FOR_LOOP:
	for {
		select {
		case <-abort:
			break FOR_LOOP
		default:
		}
		idx := rand.Intn(len(points))
		//TODO: custom error type
		_, _ = bkd.Erase(points[idx])
		_ = bkd.Insert(points[idx])
	}
}

func bkdReader(abort chan interface{}, bkd *BkdTree, points []Point) {
FOR_LOOP:
	for {
		select {
		case <-abort:
			break FOR_LOOP
		default:
		}

		idx1 := rand.Intn(len(points))
		idx2 := rand.Intn(len(points))
		visitor := &IntersectCollector{points[idx1], points[idx2], make([]Point, 0)}
		//TODO: custom error type
		_ = bkd.Intersect(visitor)
	}
}

func TestBkdConcurrentOps(t *testing.T) {
	var bkd *BkdTree
	var points []Point
	var err error
	var maxVal uint64 = 1000
	bkd, points, err = prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	ch := make(chan interface{})
	for i := 0; i < 1; i++ {
		go bkdCloser(ch, bkd)
	}
	for i := 0; i < 3; i++ {
		go bkdWriter(ch, bkd, points)
	}
	for i := 0; i < 5; i++ {
		go bkdReader(ch, bkd, points)
	}
	//sleep a while, send message to abort readers and writers
	time.Sleep(20 * time.Second)
	close(ch)
	time.Sleep(1 * time.Second)
	fmt.Println("children shall all have quited")
}
