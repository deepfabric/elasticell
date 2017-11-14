package indexer

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

//TermDict stores terms in a map. Note that the term dict is insertion-only.
type TermDict struct {
	Dir    string
	f      *os.File
	terms  map[string]uint64
	rwlock sync.RWMutex //concurrent access of TermDict
}

//NewTermDict creates and initializes a term dict
func NewTermDict(directory string, overwrite bool) (td *TermDict, err error) {
	if overwrite {
		fp := filepath.Join(directory, "terms")
		if err = os.RemoveAll(fp); err != nil {
			err = errors.Wrap(err, "")
			return
		}
	}
	td = &TermDict{
		Dir: directory,
	}
	err = td.Open()
	return
}

//Open opens an existing term dict
func (td *TermDict) Open() (err error) {
	td.rwlock.Lock()
	defer td.rwlock.Unlock()
	if td.f != nil {
		//TODO: replace panic with log.Fatalf
		panic("td.f shall be nil")
	}
	if err = os.MkdirAll(td.Dir, 0700); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	fp := filepath.Join(td.Dir, "terms")
	if td.f, err = os.OpenFile(fp, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	td.terms = make(map[string]uint64)
	reader := bufio.NewReader(td.f)
	var line string
	var num uint64
	for {
		line, err = reader.ReadString('\n')
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			err = errors.Wrap(err, "")
			return
		}
		tmpTerm := strings.TrimSpace(line)
		td.terms[tmpTerm] = num
		num++
	}
	return
}

//Close clear the dictionary on memory and close file.
func (td *TermDict) Close() (err error) {
	td.rwlock.Lock()
	defer td.rwlock.Unlock()
	err = td.close()
	return
}

func (td *TermDict) close() (err error) {
	if err = td.f.Close(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	td.f = nil
	for term := range td.terms {
		delete(td.terms, term)
	}
	return
}

//Destroy clear the dictionary on memory and disk.
func (td *TermDict) Destroy() (err error) {
	td.rwlock.Lock()
	defer td.rwlock.Unlock()
	if err = td.close(); err != nil {
		return
	}
	fp := filepath.Join(td.Dir, "terms")
	if err = os.Remove(fp); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

//Sync synchronizes terms to disk
func (td *TermDict) Sync() (err error) {
	return td.f.Sync()
}

//CreateTermIfNotExist get id of the given term, will insert the term implicitly if it is not in the dict.
func (td *TermDict) CreateTermIfNotExist(term string) (id uint64, err error) {
	var found bool
	if id, found = td.GetTermID(term); found {
		return id, nil
	}
	td.rwlock.Lock()
	defer td.rwlock.Unlock()
	if id, found = td.terms[term]; found {
		return id, nil
	}
	id = uint64(len(td.terms))
	td.terms[term] = id
	line := term + "\n"
	if _, err = td.f.WriteString(line); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

//GetTermID get id of the given term.
func (td *TermDict) GetTermID(term string) (id uint64, found bool) {
	td.rwlock.RLock()
	id, found = td.terms[term]
	td.rwlock.RUnlock()
	return
}

//CreateTermsIfNotExist is bulk version of CreateTermIfNotExist
func (td *TermDict) CreateTermsIfNotExist(terms []string) (ids []uint64, err error) {
	ids = make([]uint64, len(terms))
	for i, term := range terms {
		if ids[i], err = td.CreateTermIfNotExist(term); err != nil {
			return
		}
	}
	return
}

//Count returns the count of terms
func (td *TermDict) Count() (cnt uint64) {
	td.rwlock.RLock()
	cnt = uint64(len(td.terms))
	td.rwlock.RUnlock()
	return
}
