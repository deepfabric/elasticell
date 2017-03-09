package server

// import (
// 	"fmt"
// 	"io"
// 	"strconv"

// 	"github.com/deepfabric/elasticell/pkg/redis"
// 	"github.com/fagongzi/goetty"
// )

// type respWriter struct {
// 	session goetty.IOSession
// }

// func (w *respWriter) writeError(err error) {
// 	w.buf.WriteByte('-')
// 	if err != nil {
// 		w.buf.WriteByte(' ')
// 		w.buf.Write(hack.Slice(err.Error()))
// 	}
// 	w.buf.WriteByte(redis.CR)
// 	w.buf.WriteByte(redis.LF)

// 	// w.session.Write(w.buf.ReadAll())
// }

// func (w *respWriter) writeStatus(status string) {
// 	w.buff.WriteByte('+')
// 	w.buff.Write(hack.Slice(status))
// 	w.buff.Write(Delims)
// }

// func (w *respWriter) writeInteger(n int64) {
// 	w.buff.WriteByte(':')
// 	w.buff.Write(num.FormatInt64ToSlice(n))
// 	w.buff.Write(Delims)
// }

// func (w *respWriter) writeBulk(b []byte) {
// 	w.buff.WriteByte('$')
// 	if b == nil {
// 		w.buff.Write(NullBulk)
// 	} else {
// 		w.buff.Write(hack.Slice(strconv.Itoa(len(b))))
// 		w.buff.Write(Delims)
// 		w.buff.Write(b)
// 	}

// 	w.buff.Write(Delims)
// }

// func (w *respWriter) writeArray(lst []interface{}) {
// 	w.buff.WriteByte('*')
// 	if lst == nil {
// 		w.buff.Write(NullArray)
// 		w.buff.Write(Delims)
// 	} else {
// 		w.buff.Write(hack.Slice(strconv.Itoa(len(lst))))
// 		w.buff.Write(Delims)

// 		for i := 0; i < len(lst); i++ {
// 			switch v := lst[i].(type) {
// 			case []interface{}:
// 				w.writeArray(v)
// 			case [][]byte:
// 				w.writeSliceArray(v)
// 			case []byte:
// 				w.writeBulk(v)
// 			case nil:
// 				w.writeBulk(nil)
// 			case int64:
// 				w.writeInteger(v)
// 			case string:
// 				w.writeStatus(v)
// 			case error:
// 				w.writeError(v)
// 			default:
// 				panic(fmt.Sprintf("invalid array type %T %v", lst[i], v))
// 			}
// 		}
// 	}
// }

// func (w *respWriter) writeSliceArray(lst [][]byte) {
// 	w.buff.WriteByte('*')
// 	if lst == nil {
// 		w.buff.Write(NullArray)
// 		w.buff.Write(Delims)
// 	} else {
// 		w.buff.Write(hack.Slice(strconv.Itoa(len(lst))))
// 		w.buff.Write(Delims)

// 		for i := 0; i < len(lst); i++ {
// 			w.writeBulk(lst[i])
// 		}
// 	}
// }

// func (w *respWriter) writeFVPairArray(lst []ledis.FVPair) {
// 	w.buff.WriteByte('*')
// 	if lst == nil {
// 		w.buff.Write(NullArray)
// 		w.buff.Write(Delims)
// 	} else {
// 		w.buff.Write(hack.Slice(strconv.Itoa(len(lst) * 2)))
// 		w.buff.Write(Delims)

// 		for i := 0; i < len(lst); i++ {
// 			w.writeBulk(lst[i].Field)
// 			w.writeBulk(lst[i].Value)
// 		}
// 	}
// }

// func (w *respWriter) writeScorePairArray(lst []ledis.ScorePair, withScores bool) {
// 	w.buff.WriteByte('*')
// 	if lst == nil {
// 		w.buff.Write(NullArray)
// 		w.buff.Write(Delims)
// 	} else {
// 		if withScores {
// 			w.buff.Write(hack.Slice(strconv.Itoa(len(lst) * 2)))
// 			w.buff.Write(Delims)
// 		} else {
// 			w.buff.Write(hack.Slice(strconv.Itoa(len(lst))))
// 			w.buff.Write(Delims)

// 		}

// 		for i := 0; i < len(lst); i++ {
// 			w.writeBulk(lst[i].Member)

// 			if withScores {
// 				w.writeBulk(num.FormatInt64ToSlice(lst[i].Score))
// 			}
// 		}
// 	}
// }

// func (w *respWriter) writeBulkFrom(n int64, rb io.Reader) {
// 	w.buff.WriteByte('$')
// 	w.buff.Write(hack.Slice(strconv.FormatInt(n, 10)))
// 	w.buff.Write(Delims)

// 	io.Copy(w.buff, rb)
// 	w.buff.Write(Delims)
// }

// func (w *respWriter) flush() {
// 	w.buff.Flush()
// }
