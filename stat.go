package giashard

import (
	"compress/gzip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type LangStats struct {
	Lines         map[string]int `json:"lines"`
	LinesPerDoc   map[int]int    `json:"linesperdoc"`
	TokensPerLine map[int]int    `json:"tokensperline"`
}

type ShardStats struct {
	Shard   string           `json:"shard"`
	Bytes   map[string]int64 `json:"bytes"`
	Records map[string]int   `json:"records"`
	Native  LangStats        `json:"native"`
	English LangStats        `json:"english"`
}

func (s *ShardStats)openfile(fname string) (ch chan []byte, err error) {
	s.Bytes[fname] = -1
	s.Records[fname] = -1

	fullpath := filepath.Join(s.Shard, fname)
	stat, err := os.Stat(fullpath)
	if err != nil {
		log.Printf("error reading %v", fullpath)
		return
	}
	s.Bytes[fname] = stat.Size()

	r, err := NewLineReader(fullpath)
	if err != nil {
		return
	}

	ch = make(chan []byte)
	go func() {
		for doc := range r.Lines() {
			s.Records[fname] += 1
			ch <- doc
		}
		close(ch)
		r.Close()
	}()

	return
}

func getLines(doc []byte) (lines [][]byte, err error) {
	buf := make([]byte, base64.StdEncoding.DecodedLen(len(doc)))
	n, err := base64.StdEncoding.Decode(buf, doc)
	if err != nil {
		return
	}
	buf = buf[:n]

	lines = bytes.Split(buf, []byte("\n"))
	return
}

func (ls LangStats)countLines(fname string, doc []byte) (lines [][]byte, err error) {
	lines, err = getLines(doc)
	if err != nil {
		return
	}

	n, _ := ls.Lines[fname]
	ls.Lines[fname] = n + len(lines)

	bucket := 5 * int(len(lines) / 5)
	p, _ := ls.LinesPerDoc[bucket]
	ls.LinesPerDoc[bucket] = p + 1

	return
}

func (ls LangStats)countTokens(fname string, doc []byte) (lines [][]byte, err error) {
	lines, err = getLines(doc)
	if err != nil {
		return
	}

	n, _ := ls.Lines[fname]
	ls.Lines[fname] = n + len(lines)

	for _, line := range lines {
		toks := bytes.Split(line, []byte(" "))
		n, _ := ls.TokensPerLine[len(toks)]
		ls.TokensPerLine[len(toks)] = n + 1
	}

	return
}

func NewStats(shard string) *ShardStats {
	s := ShardStats{}
	s.Shard = shard
	s.Bytes = make(map[string]int64)
	s.Records = make(map[string]int)
	s.Native.Lines = make(map[string]int)
	s.Native.LinesPerDoc = make(map[int]int)
	s.Native.TokensPerLine = make(map[int]int)
	s.English.Lines = make(map[string]int)
	s.English.LinesPerDoc = make(map[int]int)
	s.English.TokensPerLine = make(map[int]int)

	return &s
}

func ReadStats(shard string) (stats *ShardStats, err error) {
	fp, err := os.Open(filepath.Join(shard, "stats.json.gz"))
	if err != nil {
		return
	}
	defer fp.Close()

	r, err := gzip.NewReader(fp)
	if err != nil {
		return
	}
	defer r.Close()

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}

	s := ShardStats{}
	err = json.Unmarshal(buf, &s)
	if err != nil {
		return
	}

	stats = &s
	return
}

func (s *ShardStats)Calc() {
	lines, err := s.openfile("mime.gz")
	if err == nil {
		for _ = range lines {
			//
		}
	}

	lines, err = s.openfile("source.gz")
	if err == nil {
		for _ = range lines {
			//
		}
	}

	lines, err = s.openfile("url.gz")
	if err == nil {
		for _ = range lines {
			//
		}
	}

	docs, err := s.openfile("plain_text.gz")
	if err == nil {
		for _ = range docs {
			//
		}
	}

	docs, err = s.openfile("sentences.gz")
	if err == nil {
		for doc := range docs {
			s.Native.countLines("sentences.gz", doc)
		}
	}

	docs, err = s.openfile("tokenised.gz")
	if err == nil {
		for doc := range docs {
			s.Native.countTokens("tokenised.gz", doc)
		}
	}

	docs, err = s.openfile("sentences_en.gz")
	if err == nil {
		for doc := range docs {
			s.English.countLines("sentences_en.gz", doc)
		}
	}

	docs, err = s.openfile("tokenised_en.gz")
	if err == nil {
		for doc := range docs {
			s.English.countTokens("tokenised_en.gz", doc)
		}
	}
}

func (s *ShardStats)Marshal() (buf []byte, err error) {
	return json.Marshal(s)
}

func (s *ShardStats)Write() (err error) {
	buf, err := s.Marshal()
	if err != nil {
		return
	}

	fullpath := filepath.Join(s.Shard, "stats.json.gz")
	fp, err := os.Create(fullpath)
	if err != nil {
		return
	}
	defer fp.Close()

	w, err := gzip.NewWriterLevel(fp, gzip.BestCompression)
	if err != nil {
		return
	}
	defer w.Close()
	w.Comment = "Written by giashard"

	n, err := w.Write(buf)
	if err == nil && n != len(buf) {
		err = errors.New("ShardStats.Save(): short write")
	}

	return
}
