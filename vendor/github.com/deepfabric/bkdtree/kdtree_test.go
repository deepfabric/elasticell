package bkdtree

import (
	"testing"
)

func TestKdIntersectSome(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	leafCap := 50
	intraCap := 4
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, leafCap, intraCap)

	lowPoint := points[0]
	highPoint := points[0]
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0, 1)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.Points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	for _, point := range visitor.Points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
	}
}
func TestKdIntersectAll(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	leafCap := 50
	intraCap := 4
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, leafCap, intraCap)

	lowPoint := Point{[]uint64{0, 0, 0}, 0}
	highPoint := Point{[]uint64{maxVal, maxVal, maxVal}, 0}
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0, size)}
	kdt.Intersect(visitor)
	if len(visitor.Points) != size {
		t.Errorf("found %v matchs, however %v expected", len(visitor.Points), size)
	}
}

func TestKdIntersect(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 100000
	leafCap := 50
	intraCap := 4
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, leafCap, intraCap)

	lowPoint := Point{[]uint64{20, 30, 40}, 0}
	highPoint := Point{[]uint64{maxVal, maxVal, maxVal}, 0}
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	for _, point := range visitor.Points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
	}
}

func TestKdInsert(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	leafCap := 50
	intraCap := 4
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, leafCap, intraCap)

	newPoint := Point{[]uint64{40, 30, 20}, maxVal} //use unique userData
	kdt.Insert(newPoint)

	lowPoint := newPoint
	highPoint := newPoint
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.Points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	numMatchs := 0
	for _, point := range visitor.Points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
		if point.UserData == newPoint.UserData {
			numMatchs++
		}
	}
	if numMatchs != 1 {
		t.Errorf("found %v matchs, however 1 expected", numMatchs)
	}
}

func TestKdErase(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	leafCap := 50
	intraCap := 4
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, leafCap, intraCap)

	kdt.Erase(points[0])

	lowPoint := points[0]
	highPoint := points[0]
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.Points) != 0 {
		t.Errorf("found %v matchs, however 0 expected", len(visitor.Points))
	}
}
