package util

import (
	"path/filepath"
)

func ReplaceFpExt(fp string, newExt string) string {
	ext := filepath.Ext(fp)
	newFp := fp
	if len(ext) != 0 {
		newFp = fp[:len(fp)-len(ext)]
	}
	newFp += newExt
	return newFp
}
