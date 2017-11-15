package bkdtree

import (
	"testing"
)

func BenchmarkBkdInsert(b *testing.B) {
	t0mCap := 1000
	leafCap := 50
	intraCap := 4
	numDims := 2
	bytesPerDim := 4
	dir := "/tmp"
	prefix := "bkd"
	bkd, err := NewBkdTree(t0mCap, leafCap, intraCap, numDims, bytesPerDim, dir, prefix)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	//fmt.Printf("created BkdTree %v\n", bkd)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := bkd.Insert(Point{[]uint64{uint64(i), uint64(i)}, uint64(i)})
		if err != nil {
			b.Fatalf("bkd.Insert failed, i=%v, err: %+v", i, err)
		}
	}
	return
}

func BenchmarkBkdErase(b *testing.B) {
	var maxVal uint64 = 1000
	bkd, points, err := prepareBkdTree(maxVal)
	if err != nil {
		b.Fatalf("%+v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = bkd.Erase(points[i])
		if err != nil {
			b.Fatalf("%+v", err)
		}
	}
}

func BenchmarkBkdIntersect(b *testing.B) {
	var maxVal uint64 = 1000
	bkd, points, err := prepareBkdTree(maxVal)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	var lowPoint, highPoint Point
	var visitor *IntersectCollector
	lowPoint = points[7]
	highPoint = lowPoint
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = bkd.Intersect(visitor)
		if err != nil {
			b.Fatalf("%+v", err)
		} else if len(visitor.Points) <= 0 {
			b.Errorf("found 0 matchs, however some expected")
		}
	}
}
