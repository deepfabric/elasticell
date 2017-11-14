# nth

Fast and memory efficient implementation of a Selection Algorithm in Go.

After calling `nth.Element(data sort.Interface, n int)` everything before
index `n` will be less than `data[n]` and everything after `n` will be greater
than or equal to `data[n]`. IE it partially sorts `data` such that `data[n] ==
sortedData[n]`.

`nth.Element` is an implementation of `QuickSelectAdaptive` from Andrei
Alexandrescu's 2016 paper
[Fast Deterministic Selection](https://arxiv.org/abs/1606.00484).  It is has a
deterministic `O(n)` runtime, `O(1)` space usage. Classical implementations of
Selection Algorithms usually rely on non-determinism (random pivots) or have
high constant factors (median-of-median pivots), or are not as fast as this
algorithm (`IntroSelect`).

The name `nth.Element` is inspired from C++'s `nth_element`, which is also an
implementation of a Selection Algorithm (although most STL implementations use
`IntroSelect`).

**Note:** This is not a complete implementation yet, although already should
be faster than just using `sort.Sort`.
