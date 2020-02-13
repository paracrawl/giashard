package giashard

/*
A batch is a rolling column writer that writes a fixed sized selection of
rows in a numbered subdirectory. For example,

    1/a.gz
    1/b.gz
    1/c.gz
    2/a.gz
    2/b.gz
    2/c.gz

for columns (a, b, c).

The size for a row is computed as max(len(a), len(b), len(c), ...).
*/

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
)

type Batch struct {
	dir  string   // root directory
	number int    // current batch number
	size int64    // batch size
	count int64   // running count
	cols []string // columns
	writer *ColumnWriter
}

func NewBatch(dir string, size int64, cols ...string) (b *Batch, err error) {
/*
	w, err := NewColumnWriter(dir, cols)
	if err != nil {
		return
	}

	b = &Batch{dir, size, 0, w}
*/

	batchno, err := maxbatch(dir)
	if err != nil {
		return
	}

	b = &Batch{dir, batchno, size, 0, cols, nil}

	if err = b.openBatch(); err != nil {
		return
	}

	return
}

func (b *Batch)Close() (err error) {
	if b.writer != nil {
		err = b.writer.Close()
	}
	return
}

func (b *Batch)WriteRow(row map[string][]byte) (err error) {
	// find the size of the row (max of data values)
	rowsize := 0
	for _, v := range row {
		if len(v) > rowsize {
			rowsize = len(v)
		}
	}

	// if we've overflowed past this batch size, close the writer
	// and increment the batch number
	if int64(rowsize) + b.count > b.size {
		log.Printf("Writing row of size %v onto dataset of size %v would exceed %v. Rotating", rowsize, b.count, b.size)
		if b.writer != nil {
			b.writer.Close()
			b.writer = nil
		}
		b.count = 0
		b.number += 1
	}

	// construct a new writer if we need one
	if b.writer == nil {
		err = b.openBatch()
		if err != nil {
			log.Printf("Failure to open batch %s", b.batchPath())
			return
		}
	}

	// and write the row
	if err = b.writer.WriteRow(row); err != nil {
		log.Printf("Error writing row to batch %s", b.batchPath())
		return
	}
	b.count += int64(rowsize)

	return
}

func (b *Batch)batchPath() string {
	return filepath.Join(b.dir, strconv.FormatInt(int64(b.number), 10))
}

// open the batch, and set the count, seeking to the end.
// n.b. here, we use the estimate of batch size. we could, more
// expensively, but more accurately uncompress and read the whole
// shebang.
func (b *Batch)openBatch() (err error) {
	bdir := b.batchPath()
	log.Printf("Opening batch at %s", bdir)
	err = os.MkdirAll(bdir, os.ModePerm)
	if err != nil {
		return
	}
	count, err := batchsize(bdir, b.cols...)
	if err != nil {
		return
	}
	b.count = count
	b.writer, err = NewColumnWriter(bdir, b.cols...)
	return
}

// find the end of the current batch. this means walking directory to find
// the numerically greatest
func maxbatch(dir string) (batchno int, err error) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	finfos, err := f.Readdir(-1)
	f.Close()

	batches := make([]int, 0, len(finfos))
	for _, fi := range finfos {
		i, err := strconv.Atoi(fi.Name())
		if err != nil {
			// not named numerically, just skip
			continue
		}
		batches = append(batches, i)
	}

	// if we have numerically named directories, find the maximum.
	batchno = 1
	if len(batches) > 0 {
		for _, b := range batches {
			if b > batchno {
				batchno = b
			}
		}
	}

	return
}

// estimate the size of the current batch from the current columns
func batchsize(dir string, cols ...string) (size int64, err error) {
	// find the maximum filesize of compressed files
	for _, c := range cols {
		colpath := filepath.Join(dir, c + ".gz")
		fi, err := os.Stat(colpath)
		if err != nil {
			// errors are ok only if none of the files exist. if we
			// found some data, and then see an error, something is
			// wrong
			if size > 0 {
				return 0, err
			}
			err = nil
		} else {
			// no error, record the size
			fsize := fi.Size()
			if fsize > size {
				size = fsize
			}
		}
	}

	// assume a compression factor of 3
	size = size * 3
	return
}
