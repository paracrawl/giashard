package giashard

import (
	"compress/gzip"
	"io"
	"os"
)

type LineWriter struct {
	f io.WriteCloser
	z io.WriteCloser
}

func NewLineWriter(filename string) (w *LineWriter, err error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return
	}
	z, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		f.Close()
		return
	}
	z.Comment = "Written by giashard"

	w = &LineWriter{f, z}
	return
}

func (w *LineWriter)Close() (err error) {
	if e := w.z.Close(); e != nil {
		err = e
	}
	if e := w.f.Close(); e != nil {
		err = e
	}
	return
}

func (w *LineWriter)WriteLine(line []byte) (err error) {
	line = append(line, '\n')
	_, err = w.z.Write(line)
	return
}
