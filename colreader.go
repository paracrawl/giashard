package giashard

import (
	"log"
	"path/filepath"
)

// read columns of compressed files containing lines
type ColumnReader struct {
	cols []string
	readers []*LineReader
}

// make new column reader for the given directory, which is assumed to have
// files name c1.gz, c2.gz, ... for each element of cols
func NewColumnReader(dir string, cols ...string) (r *ColumnReader, err error) {
	readers := make([]*LineReader, 0, len(cols))
	for _, c := range cols {
		lr, err := NewLineReader(filepath.Join(dir, c + ".gz"))
		if err != nil {
			for _, lr := range readers {
				if e := lr.Close(); e != nil {
					log.Print(e)
				}
			}
			return nil, err
		}
		readers = append(readers, lr)
	}
	r = &ColumnReader{cols, readers}
	return
}

// close the underlying readers
func (r *ColumnReader)Close() (err error) {
	for _, lr := range r.readers {
		if e := lr.Close(); e != nil {
			err = e
		}
	}
	return
}

func (r *ColumnReader)Rows() (ch chan map[string][]byte) {
	ch = make(chan map[string][]byte)

	srcs := make([]chan []byte, 0, len(r.cols))
	for _, lr := range r.readers {
		srcs = append(srcs, lr.Lines())
	}

	go func() {
		for {
			m := make(map[string][]byte)
			for i, c := range(r.cols) {
				v, ok := <- srcs[i]
				if !ok {
					close(ch)
					return
				}
				m[c] = v
			}
			ch <- m
		}
	}()

	return ch
}
