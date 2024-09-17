package giashard

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/klauspost/compress/zstd"
)

// was schema, now hardcoded
type JsonlRecord struct {
	Url  string `json:"u"`
	Text string `json:"text"`
}

// support reading a zstandard-zipped JSONL file and sending lines to channel (from giashard/LineReader)
// f: os.File value, z: zstd.Decoder value, fatal: indicator if read errors be fatal
type JsonlReader struct {
	f     io.ReadCloser
	z     io.ReadCloser
	fatal bool
}

func NewJsonlReader(filename string) (r *JsonlReader, err error) {
	var f *os.File
	var z io.ReadCloser
	var d *zstd.Decoder

	// deal with reading from stdin
	if filename == "-" {
		log.Println("Reading from stdin")
		f = os.Stdin
		z = f // horrible hack
	} else {
		f, err = os.Open(filename)
		if err != nil {
			return
		}
		d, err = zstd.NewReader(f)
		if err != nil {
			return
		}
		z = d.IOReadCloser() // to match LineReader
	}
	r = &JsonlReader{f, z, true}
	return
}

// should read errors be fatal (and abort the program with log.Fatalf)
func (r *JsonlReader) Fatal(flag bool) {
	r.fatal = flag
}

// close the underlying files, ta3ban
func (r *JsonlReader) Close() (err error) {
	if e := r.z.Close(); e != nil {
		err = e
	}
	// if input is stdin, file is already closed
	if r.f != os.Stdin {
		if e := r.f.Close(); e != nil {
			err = e
		}
	}
	return
}

// send records read from file to channel (replaces Lines())
func (r *JsonlReader) Records() (ch chan JsonlRecord) {
	ch = make(chan JsonlRecord)
	decoder := json.NewDecoder(r.z)
	go func() {
		for decoder.More() {
			var record JsonlRecord // alt: decode to map[string][]byte to include all records
			if err := decoder.Decode(&record); err != nil {
				if r.fatal {
					log.Fatalf("Error decoding record: %v", err)
				} else {
					log.Printf("Error decoding record: %v", err)
				}
			}
			if len(record.Text) > 0 {
				ch <- record
			}
		}
		close(ch)
	}()
	return
}

// output: a channel containing map {outputColumnNames: lines}
func (r *JsonlReader) Rows() (ch chan map[string][]byte) {
	ch = make(chan map[string][]byte)
	src := r.Records()
	go func() {
		for {
			m := make(map[string][]byte) // this is output map of rows
			v, ok := <-src
			if !ok {
				close(ch)
				return
			}

			// we base64 encode to match Paracrawl format
			b := []byte(v.Text)
			enc := make([]byte, base64.StdEncoding.EncodedLen((len(b))))
			base64.StdEncoding.Encode(enc, b)

			m["url"] = []byte(v.Url)
			m["text"] = enc
			ch <- m
		}
	}()
	return ch
}
