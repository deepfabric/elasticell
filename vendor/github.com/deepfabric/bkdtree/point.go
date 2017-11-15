package bkdtree

import (
	"encoding/binary"
	"sort"

	datastructures "github.com/deepfabric/go-datastructures"
	"github.com/keegancsmith/nth"
)

type Point struct {
	Vals     []uint64
	UserData uint64
}

type PointArray interface {
	sort.Interface
	GetPoint(idx int) Point
	GetValue(idx int) uint64
	SubArray(begin, end int) PointArray
	Erase(point Point) bool
	Append(point Point)
}

type PointArrayMem struct {
	points []Point
	byDim  int
}

type PointArrayExt struct {
	data        []byte
	numPoints   int
	byDim       int
	bytesPerDim int
	numDims     int
	pointSize   int
}

// Compare is part of datastructures.Comparable interface
func (p Point) Compare(other datastructures.Comparable) int {
	rhs := other.(Point)
	for dim := 0; dim < len(p.Vals); dim++ {
		if p.Vals[dim] != rhs.Vals[dim] {
			return int(p.Vals[dim] - rhs.Vals[dim])
		}
	}
	return int(p.UserData - rhs.UserData)
}

func (p *Point) Inside(lowPoint, highPoint Point) (isInside bool) {
	for dim := 0; dim < len(p.Vals); dim++ {
		if p.Vals[dim] < lowPoint.Vals[dim] || p.Vals[dim] > highPoint.Vals[dim] {
			return
		}
	}
	isInside = true
	return
}

func (p Point) LessThan(rhs Point) (res bool) {
	for dim := 0; dim < len(p.Vals); dim++ {
		if p.Vals[dim] != rhs.Vals[dim] {
			return p.Vals[dim] < rhs.Vals[dim]
		}
	}
	return p.UserData < rhs.UserData
}

func (p *Point) Equal(rhs Point) (res bool) {
	if p.UserData != rhs.UserData || len(p.Vals) != len(rhs.Vals) {
		return
	}
	for dim := 0; dim < len(p.Vals); dim++ {
		if p.Vals[dim] != rhs.Vals[dim] {
			return
		}
	}
	res = true
	return
}

//Encode encode in place. Refers to binary.Write impl in standard library.
//len(b) shall be no less than bytesPerDim*numDims+8
func (p *Point) Encode(b []byte, bytesPerDim int) {
	numDims := len(p.Vals)
	for i := 0; i < numDims; i++ {
		switch bytesPerDim {
		case 1:
			b[i] = byte(p.Vals[i])
		case 2:
			binary.BigEndian.PutUint16(b[2*i:], uint16(p.Vals[i]))
		case 4:
			binary.BigEndian.PutUint32(b[4*i:], uint32(p.Vals[i]))
		case 8:
			binary.BigEndian.PutUint64(b[8*i:], p.Vals[i])
		}
	}
	binary.BigEndian.PutUint64(b[numDims*bytesPerDim:], p.UserData)
	return
}

func (p *Point) Decode(b []byte, numDims int, bytesPerDim int) {
	p.Vals = make([]uint64, numDims)
	for i := 0; i < numDims; i++ {
		switch bytesPerDim {
		case 1:
			p.Vals[i] = uint64(b[i])
		case 2:
			p.Vals[i] = uint64(binary.BigEndian.Uint16(b[2*i:]))
		case 4:
			p.Vals[i] = uint64(binary.BigEndian.Uint32(b[4*i:]))
		case 8:
			p.Vals[i] = binary.BigEndian.Uint64(b[8*i:])
		}
	}
	p.UserData = binary.BigEndian.Uint64(b[numDims*bytesPerDim:])
	return
}

// Len is part of sort.Interface.
func (s *PointArrayMem) Len() int {
	return len(s.points)
}

// Swap is part of sort.Interface.
func (s *PointArrayMem) Swap(i, j int) {
	s.points[i], s.points[j] = s.points[j], s.points[i]
}

// Less is part of sort.Interface.
func (s *PointArrayMem) Less(i, j int) bool {
	return s.points[i].Vals[s.byDim] < s.points[j].Vals[s.byDim]
}

func (s *PointArrayMem) GetPoint(idx int) (point Point) {
	point = s.points[idx]
	return
}

func (s *PointArrayMem) GetValue(idx int) (val uint64) {
	val = s.points[idx].Vals[s.byDim]
	return
}

func (s *PointArrayMem) SubArray(begin, end int) (sub PointArray) {
	sub = &PointArrayMem{
		points: s.points[begin:end],
		byDim:  s.byDim,
	}
	return
}

func (s *PointArrayMem) Erase(point Point) (found bool) {
	idx := 0
	for i, point2 := range s.points {
		//assumes each point's userData is unique
		if point.Equal(point2) {
			idx = i
			found = true
			break
		}
	}
	if found {
		s.points = append(s.points[:idx], s.points[idx+1:]...)
	}
	return
}

func (s *PointArrayMem) Append(point Point) {
	s.points = append(s.points, point)
}

func (s *PointArrayMem) ToExt(bytesPerDim int) (pae *PointArrayExt) {
	numDims := len(s.points[0].Vals)
	pointSize := numDims*bytesPerDim + 8
	size := len(s.points) * pointSize
	data := make([]byte, size)
	pae = &PointArrayExt{
		data:        data,
		numPoints:   len(s.points),
		byDim:       s.byDim,
		numDims:     numDims,
		bytesPerDim: bytesPerDim,
		pointSize:   pointSize,
	}
	for i, point := range s.points {
		point.Encode(data[i*pointSize:], bytesPerDim)
	}
	return
}

// Len is part of sort.Interface.
func (s *PointArrayExt) Len() int {
	return s.numPoints
}

// Swap is part of sort.Interface.
func (s *PointArrayExt) Swap(i, j int) {
	offI := i * s.pointSize
	offJ := j * s.pointSize
	for idx := 0; idx < s.pointSize; idx++ {
		s.data[offI+idx], s.data[offJ+idx] = s.data[offJ+idx], s.data[offI+idx]
	}
}

// Less is part of sort.Interface.
func (s *PointArrayExt) Less(i, j int) bool {
	valI := s.GetValue(i)
	valJ := s.GetValue(j)
	return valI < valJ
}

func (s *PointArrayExt) GetPoint(i int) (point Point) {
	point.Decode(s.data[i*s.pointSize:], s.numDims, s.bytesPerDim)
	return
}

func (s *PointArrayExt) GetValue(i int) (val uint64) {
	offI := i * s.pointSize
	switch s.bytesPerDim {
	case 1:
		val = uint64(s.data[offI+s.byDim])
	case 2:
		val = uint64(binary.BigEndian.Uint16(s.data[offI+2*s.byDim:]))
	case 4:
		val = uint64(binary.BigEndian.Uint32(s.data[offI+4*s.byDim:]))
	case 8:
		val = binary.BigEndian.Uint64(s.data[offI+8*s.byDim:])
	}
	return
}

func (s *PointArrayExt) SubArray(begin, end int) (sub PointArray) {
	sub = &PointArrayExt{
		data:        s.data[begin*s.pointSize : end*s.pointSize],
		numPoints:   end - begin,
		byDim:       s.byDim,
		bytesPerDim: s.bytesPerDim,
		numDims:     s.numDims,
		pointSize:   s.pointSize,
	}
	return
}

func (s *PointArrayExt) Erase(point Point) (found bool) {
	var i int
	for i = 0; i < s.numPoints; i++ {
		pI := s.GetPoint(i)
		//assumes each point's userData is unique
		found = point.Equal(pI)
		if found {
			break
		}
	}
	if found {
		//replace the matched point with the last point and decrease the array length
		offI := i * s.pointSize
		offJ := (s.numPoints - 1) * s.pointSize
		for idx := 0; idx < s.pointSize; idx++ {
			s.data[offI+idx] = s.data[offJ+idx]
			s.data[offJ+idx] = 0
		}
		s.numPoints--
	}
	return
}

func (s *PointArrayExt) Append(point Point) {
	off := s.numPoints * s.pointSize
	point.Encode(s.data[off:], s.bytesPerDim)
}

func (s *PointArrayExt) ToMem() (pam *PointArrayMem) {
	points := make([]Point, s.numPoints)
	for i := 0; i < s.numPoints; i++ {
		p := s.GetPoint(i)
		points[i] = p
	}
	pam = &PointArrayMem{
		points: points,
		byDim:  s.byDim,
	}
	return
}

// SplitPoints splits points per byDim
func SplitPoints(points PointArray, numStrips int) (splitValues []uint64, splitPoses []int) {
	if numStrips <= 1 {
		return
	}
	splitPos := points.Len() / 2
	nth.Element(points, splitPos)
	splitValue := points.GetValue(splitPos)

	numStrips1 := (numStrips + 1) / 2
	numStrips2 := numStrips - numStrips1
	splitValues1, splitPoses1 := SplitPoints(points.SubArray(0, splitPos), numStrips1)
	splitValues = append(splitValues, splitValues1...)
	splitPoses = append(splitPoses, splitPoses1...)
	splitValues = append(splitValues, splitValue)
	splitPoses = append(splitPoses, splitPos)
	splitValues2, splitPoses2 := SplitPoints(points.SubArray(splitPos, points.Len()), numStrips2)
	splitValues = append(splitValues, splitValues2...)
	for i := 0; i < len(splitPoses2); i++ {
		splitPoses = append(splitPoses, splitPos+splitPoses2[i])
	}
	return
}
