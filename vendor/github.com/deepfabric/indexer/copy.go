package indexer

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// https://gist.github.com/r0l1/92462b38df26839a3ca324697c8cba04
/* MIT License
 *
 * Copyright (c) 2017 Roland Singer [roland.singer@desertbit.com]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

// CopyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file. The file mode will be copied from the source and
// the copied data is synced/flushed to stable storage.
func CopyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		err = errors.Wrap(err, "")
		return
	}

	if err = out.Sync(); err != nil {
		err = errors.Wrap(err, "")
		return
	}

	si, err := os.Stat(src)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if err = os.Chmod(dst, si.Mode()); err != nil {
		err = errors.Wrap(err, "")
		return
	}

	return
}

// CopyDir recursively copies a directory tree, attempting to preserve permissions.
// Source directory must exist, destination directory must *not* exist.
// Symlinks are ignored and skipped.
func CopyDir(src string, dst string) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	si, err := os.Stat(src)
	if err != nil {
		err = errors.Wrap(err, "")
		return err
	}
	if !si.IsDir() {
		return errors.Errorf("source %v is not a directory", src)
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		err = errors.Wrap(err, "")
		return
	}
	if err == nil {
		return errors.Errorf("destination %v already exists", dst)
	}

	if err = os.MkdirAll(dst, si.Mode()); err != nil {
		err = errors.Wrap(err, "")
		return
	}

	entries, err := ioutil.ReadDir(src)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			err = CopyDir(srcPath, dstPath)
			if err != nil {
				return
			}
		} else {
			// Skip symlinks.
			if entry.Mode()&os.ModeSymlink != 0 {
				continue
			}

			err = CopyFile(srcPath, dstPath)
			if err != nil {
				return
			}
		}
	}

	return
}
