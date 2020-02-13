package giashard

import (
	"log"
	"path/filepath"
)

// read columns of compressed files containing lines
type ColumnWriter struct {
	cols []string
	writers []*LineWriter
}

// make new column reader for the given directory, which is assumed to have
// files name c1.gz, c2.gz, ... for each element of cols
func NewColumnWriter(dir string, cols ...string) (w *ColumnWriter, err error) {
	writers := make([]*LineWriter, 0, len(cols))
	for _, c := range cols {
		lw, err := NewLineWriter(filepath.Join(dir, c + ".gz"))
		if err != nil {
			for _, lw := range writers {
				if e := lw.Close(); e != nil {
					log.Print(e)
				}
			}
			return nil, err
		}
		writers = append(writers, lw)
	}
	w = &ColumnWriter{cols, writers}
	return
}

// close the underlying readers
func (w *ColumnWriter)Close() (err error) {
	for _, lw := range w.writers {
		if e := lw.Close(); e != nil {
			err = e
		}
	}
	return
}

func (w *ColumnWriter)WriteRow(row map[string][]byte) (err error) {
	for i, c := range(w.cols) {
		e := w.writers[i].WriteLine(row[c])
		if e != nil {
			err = e
			return
		}
	}
	return
}
