package main

import (
	"errors"
	"strings"
	"flag"
	"fmt"
	"log"
	"os"
	"github.com/paracrawl/giashard"
)

var outdir string
var shards uint
var batchsize int64
var fileslist string
var domainList string

var schema = []string{"url", "mime", "plain_text"}

func init() {
	flag.StringVar(&outdir, "o", ".", "Output location")
	flag.StringVar(&fileslist, "f", "plain_text,url,mime", "Files to shard, separated by commas")
	flag.UintVar(&shards, "n", 8, "Number of shards (2^n)")
	flag.Int64Var(&batchsize, "b", 100, "Batch size in MB")
	flag.StringVar(&domainList, "d", "", "Additional public suffix entries")
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
	schema = strings.Split(fileslist, ",")

	if domainList != "" {
		count, err := giashard.AddRulesToDefaultList(domainList)
		if err != nil {
			log.Fatalf("Error loading domain list: %v", err)
		} else {
			log.Printf("Loaded %d additional public suffix domains.", count)
		}
	}

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

		log.Printf("Processing input: %v", source)
		r, err := giashard.NewColumnReader(source, schema...)
		if err != nil {
			log.Printf("Error opening input reader: %v", err)
			continue
		}

		// provenance data - where is this from
		provdata := []byte(fmt.Sprintf("%s:%s", hostname, source))
		for row := range r.Rows() {
			row["source"] = provdata
			if err := w.WriteRow(row); err != nil {
				if errors.Is(err, giashard.ShardError) { // not fatal
					log.Print(err)
					continue
				}
				log.Fatalf("Error writing row: %v", err)
			}
		}

		r.Close()
	}
}
