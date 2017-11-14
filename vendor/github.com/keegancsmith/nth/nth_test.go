package nth

import (
	"fmt"
	"sort"
	"testing"
)

var shuffled = []int{10, 14, 6, 7, 16, 12, 9, 0, 8, 4, 11, 5, 15, 1, 2, 13, 3}
var asc = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
var desc = []int{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

func TestElement(t *testing.T) {
	cases := map[string][]int{
		"shuffled": shuffled,
		"asc":      asc,
		"desc":     desc,
	}
	for name, src := range cases {
		data := make([]int, len(src))
		for n := range src {
			copy(data, src)
			Element(sort.IntSlice(data), n)
			if data[n] != n {
				t.Errorf("%s: Element(..., %d) != %d: %v", name, n, n, data)
			}
		}
	}
}

func BenchmarkElement(b *testing.B) {
	data := make([]int, len(shuffled))
	dataS := sort.IntSlice(data)
	for n := 0; n < b.N; n++ {
		copy(data, shuffled)
		Element(dataS, 15)
	}
}

func TestHoarePartition(t *testing.T) {
	testPartition(t, "hoarePartition", hoarePartition)
}

func testPartition(t *testing.T, name string, f func(sort.Interface, int, int, int) int) {
	cases := map[string][]int{
		"shuffled": shuffled,
		"asc":      asc,
		"desc":     desc,
	}
	for dname, src := range cases {
		data := make([]int, len(src))
		for a := 0; a < len(src); a++ {
			for b := a + 1; b <= len(src); b++ {
				for k := a; k < b; k++ {
					copy(data, src)
					p := f(sort.IntSlice(data), k, a, b)
					cname := fmt.Sprintf("%s: %s(..., %d, %d, %d) = %d", dname, name, k, a, b, p)
					if p < a || p >= b {
						t.Errorf("%s not in range [a,b)", cname)
					}
					for i := a; i < p; i++ {
						if data[i] > data[p] {
							t.Errorf("%s not partitioned. A[%d] > A[%d] = A[p]", cname, i, p)
						}
					}
					for i := p + 1; i < b; i++ {
						if data[i] < data[p] {
							t.Errorf("%s not partitioned. A[%d] < A[%d] = A[p]", cname, i, p)
						}
					}
				}
			}
		}
	}
}
