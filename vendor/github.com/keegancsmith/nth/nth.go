package nth

import "sort"

// Element finds the nth rank ordered element and ensures it is at the nth
// position.
func Element(data sort.Interface, n int) {
	l := data.Len()
	if n < 0 || n >= l {
		return
	}
	quickSelectAdaptive(data, n, 0, l)
}

// quickSelectAdaptive is from "Fast Deterministic Selection" by Andrei
// Alexandrescu https://arxiv.org/abs/1606.00484
//
// It is deterministic O(n) selection algorithm tuned for real world
// workloads.
func quickSelectAdaptive(data sort.Interface, k, a, b int) {
	var (
		l int // |A| from the paper
		p int // pivot position
	)
	for {
		l = b - a
		r := float64(k) / float64(l) // r <- real(k) / real(|A|)
		if l < 12 {
			p = hoarePartition(data, a+l/2, a, b) // HoarePartition(A, |A| / 2)
		} else if r < 7.0/16.0 {
			if r < 1.0/12.0 {
				p = repeatedStepFarLeft(data, k, a, b)
			} else {
				p = repeatedStepLeft(data, k, a, b)
			}
		} else if r >= 1.0-7.0/16.0 {
			if r >= 1.0-1.0/12.0 {
				p = repeatedStepFarRight(data, k, a, b)
			} else {
				p = repeatedStepRight(data, k, a, b)
			}
		} else {
			p = repeatedStepImproved(data, k, a, b)
		}
		if p == k {
			return
		}
		if p > k {
			b = p // A <- A[0:p]
		} else {
			// i <- k - p - 1  // TODO what is i?
			a = p + 1 // A <- A[p+1:|A|]
		}
	}
}

func hoarePartition(data sort.Interface, p, begin, end int) int {
	data.Swap(p, begin) // Swap(A[p], A[0])
	a := begin + 1      // a = 1
	b := end - 1        // b = |A| - 1
Loop:
	for {
		for {
			if a > b {
				break Loop
			}
			if !data.Less(a, begin) { // A[a] >= A[0]
				break
			}
			a++
		}
		for data.Less(begin, b) { // A[0] < A[b]
			b--
		}
		if a >= b {
			break
		}
		data.Swap(a, b) // Swap(A[a], A[b])
		a++
		b--
	}
	data.Swap(begin, a-1) // Swap(A[0], A[a-1])
	return a - 1
}

func repeatedStepLeft(data sort.Interface, k, a, b int) int {
	l := b - a  // |A|
	if l < 12 { // |A| < 12
		return hoarePartition(data, a+l/2, a, b) // HoarePartition(A, |A| / 2)
	}
	f := l / 4                 // f <- |A| / 4
	for i := a; i < a+f; i++ { // for i <- 0 through f - 1 do
		lowerMedian4(data, i, i+f, i+2*f, i+3*f) // LowerMedian4(A, i, i+f, i+2f, i+3f)
	}
	f2 := f / 3                       // f' <- f / 3
	for i := a + f; i < a+f+f2; i++ { // for i <- f through f + f' - 1 do
		median3(data, i, i+f2, i+2*f2) // Median3(A, i, i + f', i + 2f')
	}
	newA := a + f
	quickSelect(repeatedStepLeft, data, newA+(k-a)*f2/l, newA, newA+f2)  // QuickSelect(RepeatedStepLeft, A[f:f+f'], kf'/|A|)
	return expandPartition(data, newA, newA+(k-a)*f2/l, newA+f2-1, a, b) // ExpandPartition(A, f, f + kf' / |A|, f + f' - 1)
}

func repeatedStepFarLeft(data sort.Interface, k, a, b int) int {
	l := b - a  // |A|
	if l < 12 { // |A| < 12
		return hoarePartition(data, a+l/2, a, b) // HoarePartition(A, |A| / 2)
	}
	f := l / 4                       // f <- |A| / 4
	for i := a + f; i < a+2*f; i++ { // for i <- f through 2f - 1 do
		lowerMedian4(data, i-f, i, i+f, i+2*f) // LowerMedian4(A, i - f, i, i+f, i+2f)
	}
	f2 := f / 3                       // f' <- f / 3
	for i := a + f; i < a+f+f2; i++ { // for i <- f through f + f' - 1 do
		if data.Less(i+f2, i) { // A[i + f'] < A[i]
			data.Swap(i+f2, i) // Swap(A[i + f'], A[i])
		}
		if data.Less(i+2*f2, i) { // A[i + 2f'] < A[i]
			data.Swap(i+2*f2, i) // Swap(A[i + 2f'], A[i])
		}
	}
	newA := a + f
	quickSelect(repeatedStepFarLeft, data, newA+(k-a)*f2/l, newA, newA+f2) // QuickSelect(RepeatedStepFarLeft, A[f:f+f'], kf'/|A|)
	return expandPartition(data, newA, newA+(k-a)*f2/l, newA+f2-1, a, b)   // ExpandPartition(A, f, f + kf' / |A|, f + f' - 1)
}

// lowerMedian4 places min at data[a] and lower median at data[b] of the four
// values data[a,b,c,d]
func lowerMedian4(data sort.Interface, a, b, c, d int) {
	// TODO is there a way to do less swaps?
	median3(data, a, b, c)
	if data.Less(d, b) {
		data.Swap(d, b)
		if data.Less(b, a) {
			data.Swap(b, a)
		}
	}
}

// median3 sorts the 3 values data[a,b,c] into the indexes a, b, c
func median3(data sort.Interface, a, b, c int) {
	if data.Less(b, a) {
		data.Swap(b, a)
	}
	if data.Less(c, b) {
		data.Swap(c, b)
		if data.Less(b, a) {
			data.Swap(b, a)
		}
	}
}

// quickSelect is the classic quickSelect with the partition function
func quickSelect(partition func(data sort.Interface, k, a, b int) int, data sort.Interface, k, a, b int) {
	for {
		p := partition(data, k, a, b) // partition(A, k)
		if p == k {
			return
		}
		if p > k {
			b = p
		} else {
			// k <- k - p - 1
			a = p + 1 // A <- A[p+1:|A|]
		}
	}
}

// expandPartition(A, a, p, b) is poorly explained in the paper. Essentially
// we want to ensure A[:a] < A[p] and A[p] < A[b:]. Luckily we can do minimal
// swaps by swaping between the two slices A[:a] and A[b:]. The tricky part is
// when we have to move the pivot such that we always have something to swap
// between A[:a] and A[b:].
func expandPartition(data sort.Interface, a, p, b, begin, end int) int {
	// Invariant: data[a:b+1] is partition around data[p]
	// Afterwards: data[begin:end] is partitioned around returned p
	i := begin
	j := end - 1
	for {
		for ; i < a && data.Less(i, p); i++ {
		}
		for ; j > b && !data.Less(j, p); j-- {
		}
		if i == a || j == b {
			break
		}
		data.Swap(i, j)
		i++
		j--
	}
	// Invariant: data[begin:i], data[a:b+1], data[j+1:end] is partitioned around p
	if i != a {
		// We still need to partition data[i:a] around p
		return hoarePartition(data, p, i, a)
	}
	if j != b {
		// We still need to partition data[b:j+1] around p
		return hoarePartition(data, p, b, j+1)
	}
	return p
}

func simplePartition(data sort.Interface, k, a, b int) int {
	p := a
	for i := a + 1; i < b; i++ {
		if data.Less(i, p) {
			data.Swap(p, i)
			p++
			data.Swap(p, i)
		}
	}
	return p
}

// TODO implement
var repeatedStepFarRight = simplePartition
var repeatedStepRight = simplePartition
var repeatedStepImproved = simplePartition
