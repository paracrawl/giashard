package main

import (
	"flag"
	"fmt"
	"log"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"github.com/paracrawl/giashard"
)

var outdir string
var shards uint
var batchsize int64
var fileslist string

func init() {
	flag.StringVar(&outdir, "o", ".", "Output location")
	flag.StringVar(&fileslist, "f", "plain_text,url,mime,source", "Files to shard, separated by commas")
	flag.UintVar(&shards, "n", 8, "Number of shards (2^n)")
	flag.Int64Var(&batchsize, "b", 100, "Batch size in MB")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] input directories\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), `Merges the given batches (subshards) into the output directory.`)
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	flag.Parse()

	schema := strings.Split(fileslist, ",")

	maxsize := batchsize * 1024 * 1024

	err := os.MkdirAll(outdir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	bno, err := giashard.Maxbatch(outdir)
	if err != nil {
		log.Fatal(err)
	}
	dst := filepath.Join(outdir, strconv.FormatInt(int64(bno), 10))

	writers := make(map[string]io.WriteCloser)

	for i:=0; i<flag.NArg(); i++ {
		src := flag.Arg(i)

		dsize, err := giashard.Batchsize(dst, schema...)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Destination %v estimated size %v", dst, dsize)

		ssize, err := giashard.Batchsize(src, schema...)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Source %v estimated size %v", src, ssize)

		if dsize + ssize > maxsize {
			log.Printf("Appending would overflow. Rotating.")
			bno += 1
			dst = filepath.Join(outdir, strconv.FormatInt(int64(bno), 10))
			for c, w := range writers {
				if err = w.Close(); err != nil {
					log.Printf("error closing writer for %v: %v ", c, err)
				}
			}
			writers = make(map[string]io.WriteCloser)
		}

		err = os.MkdirAll(dst, os.ModePerm)
		if err != nil {
			log.Fatalf("%v", err)
		}

		for _, c := range schema {
			sfname := filepath.Join(src, c + ".gz")
			sfp, err := os.Open(sfname)
			if err != nil {
				log.Fatal(err)
			}

			dfp, ok := writers[c]
			if !ok {
				dfname := filepath.Join(dst, c + ".gz")
				dfp, err = os.OpenFile(dfname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
				writers[c] = dfp
			}

			io.Copy(dfp, sfp)

			sfp.Close()
		}
	}

	log.Printf("cleaning up.")
	for c, w := range writers {
		if err = w.Close(); err != nil {
			log.Printf("error closing writer for %v: %v ", c, err)
		}
	}
	log.Printf("done.")
}
