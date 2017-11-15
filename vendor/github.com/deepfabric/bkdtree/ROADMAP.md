# Roadmap

This document defines the roadmap for BKD tree development.

#### KD tree (memory)
- [D] build - Kdtree in mem
- [D] intersect - Kdtree in mem
- [D] insert - Kdtree in mem
- [D] erase - Kdtree in mem

##### BKD tree (memory + file)
- [D] build
- [D] insert 
- [D] erase
- [D] intersect
- [D] compatible file format to allow multiple versions
- [D] performance optimization - mmap, point encoding/decoding etc.
- [D] disaster recovery - open, close
- [D] concurrent access - singel writer, multiple reader
- [D] concurrent access - background compact
- [ ] make mmap optional
- [ ] custom error type for invalid arguments, not-permitted operations
- [ ] performance optimization - (*PointArrayExt).GetPoint
