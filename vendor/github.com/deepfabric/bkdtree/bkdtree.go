package bkdtree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/pkg/errors"
)

type KdTreeExtNodeInfo struct {
	Offset    uint64 //offset in file. It's a leaf if less than pointsOffEnd, otherwise an intra node.
	NumPoints uint64 //number of points of subtree rooted at this node
}

const KdTreeExtNodeInfoSize int64 = 8 + 8

//KdTreeExtIntraNode is struct of intra node.
/**
 * invariants:
 * 1. NumStrips == 1 + len(SplitValues) == len(Children).
 * 2. values in SplitValues are in non-decreasing order.
 * 3. offset in Children are in increasing order.
 */
type KdTreeExtIntraNode struct {
	SplitDim    uint32
	NumStrips   uint32
	SplitValues []uint64
	Children    []KdTreeExtNodeInfo
}

// KdTreeExtMeta is persisted at the end of file.
/**
 * Some fields are redundant in order to make the file be self-descriptive.
 * Attention:
 * 1. Keep all fields exported to allow one invoke of binary.Read() to parse the whole struct.
 * 2. Keep KdTreeExtMeta be 4 bytes aligned.
 * 3. Keep formatVer one byte, and be the last member.
 * 4. Keep KdMetaSize be sizeof(KdTreeExtMeta);
 */
type KdTreeExtMeta struct {
	PointsOffEnd uint64 //the offset end of points
	RootOff      uint64 //the offset of root KdTreeExtIntraNode
	NumPoints    uint64 //the current number of points. Deleting points could trigger rebuilding the tree.
	LeafCap      uint16
	IntraCap     uint16
	NumDims      uint8
	BytesPerDim  uint8
	PointSize    uint8
	FormatVer    uint8 //the file format version. shall be the last byte of the file.
}

//KdTreeExtMetaSize is sizeof(KdTreeExtMeta)
const KdTreeExtMetaSize int = 8*3 + 4 + 4

type BkdSubTree struct {
	meta KdTreeExtMeta
	f    *os.File
	data []byte //file content via mmap
}

//BkdTree is a BKD tree
type BkdTree struct {
	t0mCap      int // M in the paper, the capacity of in-memory buffer
	leafCap     int // limit of points a leaf node can hold
	intraCap    int // limit of children of a intra node can hold
	NumDims     int // number of point dimensions
	BytesPerDim int // number of bytes of each encoded dimension
	pointSize   int
	dir         string //directory of files which hold the persisted kdtrees
	prefix      string //prefix of file names
	NumPoints   int
	t0m         BkdSubTree // T0M in the paper, in-memory buffer.
	trees       []BkdSubTree
	rwlock      sync.RWMutex //reader: Intersect, GetCap. writers: Insert, Erase, Open, Close, Destroy, Compact.
	open        bool         //closed: allow Open, Close; open: allow all operations except Open.
}

func (n *KdTreeExtIntraNode) Read(r io.Reader) (err error) {
	//According to https://golang.org/pkg/encoding/binary/#Read,
	//"Data must be a pointer to a fixed-size value or a slice of fixed-size values."
	//Slice shall be adjusted to the expected length before calling binary.Read().
	err = binary.Read(r, binary.BigEndian, &n.SplitDim)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = binary.Read(r, binary.BigEndian, &n.NumStrips)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	n.SplitValues = make([]uint64, n.NumStrips-1)
	err = binary.Read(r, binary.BigEndian, &n.SplitValues)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	n.Children = make([]KdTreeExtNodeInfo, n.NumStrips)
	err = binary.Read(r, binary.BigEndian, &n.Children) //TODO: why n.children doesn't work?
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (n *KdTreeExtIntraNode) Write(w io.Writer) (err error) {
	//According to https://golang.org/pkg/encoding/binary/#Write,
	//"Data must be a fixed-size value or a slice of fixed-size values, or a pointer to such data."
	//Structs with slice members can not be used with binary.Write. Slice members shall be write explictly.
	err = binary.Write(w, binary.BigEndian, &n.SplitDim)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = binary.Write(w, binary.BigEndian, &n.NumStrips)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = binary.Write(w, binary.BigEndian, &n.SplitValues)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = binary.Write(w, binary.BigEndian, &n.Children)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

//NewBkdTree creates a BKDTree. This is used for construct a BkdTree from scratch. Existing files, if any, will be removed.
func NewBkdTree(t0mCap, leafCap, intraCap, numDims, bytesPerDim int, dir, prefix string) (bkd *BkdTree, err error) {
	if t0mCap <= 0 || leafCap <= 0 || leafCap >= int(^uint16(0)) || intraCap <= 2 ||
		numDims <= 0 || (bytesPerDim != 1 && bytesPerDim != 2 && bytesPerDim != 4 && bytesPerDim != 8) {
		err = errors.Errorf("invalid parameter")
		return
	}
	bkd = &BkdTree{
		t0mCap:      t0mCap,
		leafCap:     leafCap,
		intraCap:    intraCap,
		NumDims:     numDims,
		BytesPerDim: bytesPerDim,
		pointSize:   numDims*bytesPerDim + 8,
		dir:         dir,
		prefix:      prefix,
		//t0m is initialized later
		trees: make([]BkdSubTree, 0),
	}
	if err = bkd.initT0M(); err != nil {
		return
	}
	if err = rmTreeList(dir, prefix); err != nil {
		return
	}
	bkd.open = true
	return
}

//Destroy close and remove all files
func (bkd *BkdTree) Destroy() (err error) {
	bkd.rwlock.Lock()
	defer bkd.rwlock.Unlock()
	if err = bkd.close(); err != nil {
		return
	}
	if err = rmTreeList(bkd.dir, bkd.prefix); err != nil {
		return
	}
	if err = os.Remove(bkd.T0mPath()); err != nil {
		return
	}
	return
}

//Close close unmap and all files
func (bkd *BkdTree) Close() (err error) {
	bkd.rwlock.Lock()
	defer bkd.rwlock.Unlock()
	err = bkd.close()
	return
}

//close close all files without holding the write lock
func (bkd *BkdTree) close() (err error) {
	if !bkd.open {
		return
	}
	bkd.open = false

	if err = FileMunmap(bkd.t0m.data); err != nil {
		return
	} else if err = bkd.t0m.f.Close(); err != nil {
		return
	}
	for i := 0; i < len(bkd.trees); i++ {
		if bkd.trees[i].meta.NumPoints == 0 {
			continue
		}
		if err = FileMunmap(bkd.trees[i].data); err != nil {
			return
		} else if err = bkd.trees[i].f.Close(); err != nil {
			return
		}
	}
	return
}

//NewBkdTreeExt create a BKdTree based on exisiting files.
func NewBkdTreeExt(dir, prefix string) (bkd *BkdTree, err error) {
	bkd = &BkdTree{
		dir:    dir,
		prefix: prefix,
	}
	err = bkd.Open()
	return
}

//Open open existing files. Assumes dir and prefix are already poluplated.
func (bkd *BkdTree) Open() (err error) {
	bkd.rwlock.Lock()
	defer bkd.rwlock.Unlock()
	if bkd.open {
		err = errors.Errorf("(*BkdTree).Open is not allowed at open state")
		return
	}

	var nums []int
	if err = bkd.openT0M(); err != nil {
		return
	}
	bkd.NumPoints = int(bkd.t0m.meta.NumPoints)
	if nums, err = getTreeList(bkd.dir, bkd.prefix); err != nil {
		return
	}
	for _, num := range nums {
		if len(bkd.trees) <= num+1 {
			delta := num + 1 - len(bkd.trees)
			for i := 0; i < delta; i++ {
				kd := BkdSubTree{
					meta: KdTreeExtMeta{}, //NumPoints default value zero is good
				}
				bkd.trees = append(bkd.trees, kd)
			}
		}
		fp := bkd.TiPath(num)
		if err = bkd.trees[num].open(fp); err != nil {
			return
		}
		bkd.NumPoints += int(bkd.trees[num].meta.NumPoints)
	}
	bkd.open = true
	return
}

//T0mPath returns T0M path
func (bkd *BkdTree) T0mPath() string {
	fpT0M := filepath.Join(bkd.dir, fmt.Sprintf("%s_t0m", bkd.prefix))
	return fpT0M
}

//TiPath returns Ti path
func (bkd *BkdTree) TiPath(i int) string {
	fpTi := filepath.Join(bkd.dir, fmt.Sprintf("%s_t%d", bkd.prefix, i))
	return fpTi
}

func (bkd *BkdTree) initT0M() (err error) {
	if err = os.MkdirAll(bkd.dir, 0700); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	fT0M, err1 := os.OpenFile(bkd.T0mPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err1 != nil {
		err = errors.Wrap(err1, "")
		return
	}
	meta := KdTreeExtMeta{
		PointsOffEnd: uint64(bkd.pointSize * bkd.t0mCap),
		RootOff:      0, //not used in T0M
		NumPoints:    0,
		LeafCap:      uint16(bkd.leafCap),
		IntraCap:     uint16(bkd.intraCap),
		NumDims:      uint8(bkd.NumDims),
		BytesPerDim:  uint8(bkd.BytesPerDim),
		PointSize:    uint8(bkd.pointSize),
		FormatVer:    0,
	}
	buf := make([]byte, meta.PointsOffEnd)
	if _, err = fT0M.Write(buf); err != nil {
		err = errors.Wrap(err, "")
	}
	if err = binary.Write(fT0M, binary.BigEndian, &meta); err != nil {
		err = errors.Wrap(err, "")
	}
	data, err := FileMmap(fT0M)
	if err != nil {
		return
	}
	bkd.t0m = BkdSubTree{
		meta: meta,
		f:    fT0M,
		data: data,
	}
	return
}

func (bkd *BkdTree) openT0M() (err error) {
	bkd.t0m = BkdSubTree{}
	if err = bkd.t0m.open(bkd.T0mPath()); err != nil {
		return
	}
	bkd.t0mCap = (len(bkd.t0m.data) - KdTreeExtMetaSize) / int(bkd.t0m.meta.PointSize)
	bkd.NumDims = int(bkd.t0m.meta.NumDims)
	bkd.BytesPerDim = int(bkd.t0m.meta.BytesPerDim)
	bkd.pointSize = int(bkd.t0m.meta.PointSize)
	bkd.leafCap = int(bkd.t0m.meta.LeafCap)
	bkd.intraCap = int(bkd.t0m.meta.IntraCap)
	return
}

func (bst *BkdSubTree) open(fp string) (err error) {
	if bst.f, err = os.OpenFile(fp, os.O_RDWR, 0600); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if bst.data, err = FileMmap(bst.f); err != nil {
		return
	}
	br := bytes.NewReader(bst.data[len(bst.data)-KdTreeExtMetaSize:])
	if err = binary.Read(br, binary.BigEndian, &bst.meta); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func getTreeList(dir, prefix string) (numList []int, err error) {
	var matches [][]string
	var num int
	patt := fmt.Sprintf("^%s_t(?P<num>[0-9]+)$", prefix)
	if matches, err = FilepathGlob(dir, patt); err != nil {
		return
	}
	for _, match := range matches {
		num, err = strconv.Atoi(match[1])
		if err != nil {
			err = errors.Wrap(err, "")
			return
		}
		numList = append(numList, num)
	}
	sort.Ints(numList)
	return
}

func rmTreeList(dir, prefix string) (err error) {
	patt := fmt.Sprintf("^%s_t(?P<num>[0-9]+)$", prefix)
	err = FilepathGlobRm(dir, patt)
	return
}
