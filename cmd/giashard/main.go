package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"github.com/paracrawl/giashard"
)

var outdir string
var shards uint
var batchsize int64

var schema = []string{"url", "mime", "plain_text"}

func init() {
	flag.StringVar(&outdir, "o", ".", "Output location")
	flag.UintVar(&shards, "n", 8, "Number of shards (2^n)")
	flag.Int64Var(&batchsize, "b", 100, "Batch size in MB")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] input directories\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(),
`Shards together the directories give on input. They are assumed to be in the
standard Paracrawl column storage format. The output is a tree of directories
of the form: outdir/shard/batch where shard is computed as a hash of the
significant part of the hostname in a url and batch is approximately fixed size.
`)
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	flag.Parse()

	w, err := giashard.NewShard(outdir, shards, batchsize * 1024 * 1024, "url", append(schema, "source")...)
	if err != nil {
		log.Fatalf("Error opening output shards: %v", err)
	}
	defer w.Close()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Error getting local hostname: %v", err)
	}

	for i:=0; i<flag.NArg(); i++ {
		source := flag.Arg(i)

		r, err := giashard.NewColumnReader(source, schema...)
		if err != nil {
			log.Fatalf("Error opening input reader: %v", err)
		}

		// provenance data - where is this from
		provdata := []byte(fmt.Sprintf("%s:%s", hostname, source))
		for row := range r.Rows() {
			row["source"] = provdata
			if err := w.WriteRow(row); err != nil {
				log.Fatalf("Error writing row: %v", err)
			}
		}

		r.Close()
	}
}
