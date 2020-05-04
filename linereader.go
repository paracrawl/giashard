package giashard

import (
	"bufio"
	"compress/gzip"
	"errors"
	"io"
	"log"
	"os"
)

// support reading a gzip compressed file and sending lines to a channel
type LineReader struct {
	f io.ReadCloser
	z io.ReadCloser
	fatal bool
}

// return an object that will read lines out of the gzip compressed file
func NewLineReader(filename string) (r *LineReader, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return
	}

	z, err := gzip.NewReader(f)
	if err != nil {
		return
	}

	r = &LineReader{f, z, true}
	return
}

// should read errors be fatal (and abort the program with log.Fatalf)
func (r *LineReader)Fatal(flag bool) {
	r.fatal = flag
}

// close the underlying files, of course
func (r *LineReader)Close() (err error) {
	if e := r.z.Close(); e != nil {
		err = e
	}
	if e := r.f.Close(); e != nil {
		err = e
	}
	return
}

// send lines read from file to the channel
func (r *LineReader)Lines() (ch chan []byte) {
	ch = make(chan []byte)
	go func() {
		buf := bufio.NewReader(r.z)

		item := make([]byte, 0, 1024)
		for {
			line, pfx, err := buf.ReadLine()
			// we got some bytes, accumulate
			if len(line) > 0 {
				item = append(item, line...)
			}
			// we're done
			if err != nil {
				if err == io.EOF {
					if len(item) > 0 {
						ch <- item
					}
				} else {
					var perr *os.PathError
					if errors.As(err, &perr) && perr.Err.Error() == "file already closed" {
						// Ignore weird edge case we're we are closing ColReader
						// so quickly we haven't had the time to encounter EOF
						// in this LineReader yet.
					} else if r.fatal {
						log.Fatalf("error reading column: %v", err)
					} else {
						log.Printf("error reading column: %v", err)
					}
				}
				close(ch)
				return
			}
			// if we have a complete line, send it
			if !pfx {
				ch <- item
				item = make([]byte, 0, 1024)
			}
		}
	}()
	return
}
