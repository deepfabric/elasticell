package cnemo

// #cgo CPPFLAGS: -Iinternal/include
// #cgo CPPFLAGS: -Iinternal/src
// #cgo CPPFLAGS: -Iinternal/3rdparty/nemo-rocksdb/include
// #cgo CPPFLAGS: -Iinternal/3rdparty/nemo-rocksdb/rocksdb/include
// #cgo CPPFLAGS: -Iinternal/3rdparty/nemo-rocksdb/rocksdb
// #cgo CPPFLAGS: -Iinternal/3rdparty/nemo-rocksdb/rocksdb/util
// #cgo CPPFLAGS: -Iinternal/3rdparty/nemo-rocksdb/rocksdb/utilities/merge_operators/string_append
// #cgo CXXFLAGS: -std=c++11
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
import "C"
