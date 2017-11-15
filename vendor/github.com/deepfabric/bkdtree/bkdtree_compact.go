package bkdtree

//Compact comopact subtrees if necessary.
func (bkd *BkdTree) Compact() (err error) {
	bkd.rwlock.RLock()
	if !bkd.open {
		bkd.rwlock.RUnlock()
		return
	}
	k := bkd.getMaxCompactPos()
	bkd.rwlock.RUnlock()
	if k < 0 {
		return
	}

	bkd.rwlock.Lock()
	err = bkd.compactTo(k)
	bkd.rwlock.Unlock()
	return
}

//caclulate the max compoint position. Returns -1 if not found.
func (bkd *BkdTree) getMaxCompactPos() (pos int) {
	//find the largest index k in [0, len(trees)) at which trees[k] is empty, or its capacity is no less than the sum of size of t0m + trees[0:k+1]
	pos = -1
	sum := int(bkd.t0m.meta.NumPoints)
	for k := 0; k < len(bkd.trees); k++ {
		if bkd.trees[k].meta.NumPoints == 0 {
			pos = k
			continue
		}
		sum += int(bkd.trees[k].meta.NumPoints)
		capK := bkd.t0mCap << uint(k)
		if capK >= sum {
			pos = k
		}
	}
	return
}
