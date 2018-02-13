package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/stretchr/testify/require"
)

func TestTextFrameParseWords(t *testing.T) {
	text := "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it? cindex为若干路径创建索引。索引是trigram倒排表。trigram是UTF-8文档中的连续3字节(可以是中英文混合)。posting list就是文档ID列表，将它们的delta以变长编码方式存放。整个索引存储在一个文件，在read时mmap到内存。所以索引尺寸受限于RAM。"
	expect := "go/s/standard/library/does/not/have/a/function/solely/intended/to/check/if/a/file/exists/or/not/like/python/s/os/path/exists/what/is/the/idiomatic/way/to/do/it/cindex/为/若/干/路/径/创/建/索/引/索/引/是/trigram/倒/排/表/trigram/是/utf/8/文/档/中/的/连/续/3/字/节/可/以/是/中/英/文/混/合/posting/list/就/是/文/档/id/列/表/将/它/们/的/delta/以/变/长/编/码/方/式/存/放/整/个/索/引/存/储/在/一/个/文/件/在/read/时/mmap/到/内/存/所/以/索/引/尺/寸/受/限/于/ram"
	words := ParseWords(text)
	fmt.Printf("text: %v\n", text)
	fmt.Printf("words: %v\n", strings.Join(words, "/"))
	require.Equal(t, expect, strings.Join(words, "/"))
}

func TestTextFrameDoIndex(t *testing.T) {
	var err error
	var found bool
	var f *TextFrame
	var terms []string

	//TESTCASE: query and insert term to an empty dict
	f, err = NewTextFrame("/tmp/text_frame_test", "i", "f", true)
	require.NoError(t, err)
	defer f.Close()

	text := "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
	err = f.DoIndex(3, text)
	require.NoError(t, err)
	fmt.Printf("termdict size: %d\n", f.td.Count())

	terms = []string{"go", "it"}
	for _, term := range terms {
		_, found = f.td.GetTermID(term)
		require.Equal(t, true, found, "Term %s not found", term)
	}

	terms = []string{"java", "php"}
	for _, term := range terms {
		_, found = f.td.GetTermID(term)
		require.Equal(t, false, found, "Term %s found", term)
	}
}

func TestTextFrameQuery(t *testing.T) {
	var err error
	var f *TextFrame
	var terms []string
	var bm *pilosa.Bitmap
	var bits map[uint64][]uint64

	//TESTCASE: query and insert term to an empty dict
	f, err = NewTextFrame("/tmp/text_frame_test", "i", "f", true)
	require.NoError(t, err)
	defer f.Close()

	docIDs := []uint64{1, 10}
	texts := []string{
		"Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it? 你好，世界",
		"This is a listing of successful results of all the various data storage and processing system benchmarks I've conducted using the dataset produced in the Billion Taxi Rides in Redshift blog post. The dataset itself has 1.1 billion records, 51 columns and takes up about 500 GB of disk space uncompressed.",
	}
	for i := 0; i < len(docIDs); i++ {
		err = f.DoIndex(docIDs[i], texts[i])
		require.NoError(t, err)
	}
	fmt.Printf("termdict size after indexing: %d\n", f.td.Count())
	bits, err = f.Bits()
	require.NoError(t, err)
	fmt.Printf("frame bits: %v\n", bits)

	terms = []string{"The", "disk", "standard function", "standard世界！", "你坏"}
	expDocIDs := [][]uint64{[]uint64{1, 10}, []uint64{10}, []uint64{1}, []uint64{1}, []uint64{}}
	for i, term := range terms {
		bm = f.Query(term)
		docIDs = bm.Bits()
		fmt.Printf("found term %s in documents: %v\n", term, docIDs)
		require.Equal(t, expDocIDs[i], docIDs)
	}
}

func TestTextFrameDestroy(t *testing.T) {
	var err error
	var f *TextFrame

	f, err = NewTextFrame("/tmp/text_frame_test", "i", "f", true)
	require.NoError(t, err)
	defer f.Close()

	text := "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
	err = f.DoIndex(3, text)
	require.NoError(t, err)
	fmt.Printf("termdict size: %d\n", f.td.Count())

	err = f.Destroy()
	require.NoError(t, err)

	fps := []string{filepath.Join(f.path, "terms"), filepath.Join(f.path, "fragments")}
	for _, fp := range fps {
		if _, err := os.Stat(fp); err == nil || !os.IsNotExist(err) {
			t.Fatalf("path %s exists, want removed", fp)
		}
	}
	require.Equal(t, uint64(0), f.td.Count())
}

func TestTextFrameGetFragList(t *testing.T) {
	var err error
	var f *TextFrame

	f, err = NewTextFrame("/tmp/text_frame_test", "i", "f", true)
	require.NoError(t, err)
	defer f.Close()

	text := "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it? 你好，世界"
	docIDs := []uint64{
		0,
		1,
		pilosa.SliceWidth,
		pilosa.SliceWidth + 1,
		9 * pilosa.SliceWidth}
	expFragLists := [][]uint64{
		[]uint64{0},
		[]uint64{0},
		[]uint64{0, 1},
		[]uint64{0, 1},
		[]uint64{0, 1, 9}}
	for i := 0; i < len(docIDs); i++ {
		err = f.DoIndex(docIDs[i], text)
		require.NoError(t, err)
		numList := f.GetFragList()
		require.Equal(t, expFragLists[i], numList)
	}

	//clearBit doesn't impact GetFragList
	docIDs = []uint64{
		0,
		1,
		pilosa.SliceWidth,
		pilosa.SliceWidth + 1,
		9 * pilosa.SliceWidth}
	expFragLists = [][]uint64{
		[]uint64{0, 1, 9},
		[]uint64{0, 1, 9},
		[]uint64{0, 1, 9},
		[]uint64{0, 1, 9},
		[]uint64{0, 1, 9}}
	for i := 0; i < len(docIDs); i++ {
		_, err = f.clearBit(0, docIDs[i])
		require.NoError(t, err)
		numList := f.GetFragList()
		require.Equal(t, expFragLists[i], numList)
	}
}

func BenchmarkTextFrameDoIndex(b *testing.B) {
	var err error
	var f *TextFrame
	f, err = NewTextFrame("/tmp/text_frame_test", "i", "f", true)
	require.NoError(b, err)
	defer f.Close()

	b.ResetTimer()
	text := "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it? cindex为若干路径创建索引。索引是trigram倒排表。trigram是UTF-8文档中的连续3字节(可以是中英文混合)。posting list就是文档ID列表，将它们的delta以变长编码方式存放。整个索引存储在一个文件，在read时mmap到内存。所以索引尺寸受限于RAM。"
	for i := 0; i < b.N; i++ {
		err = f.DoIndex(uint64(i), text)
		require.NoError(b, err)
	}
}
