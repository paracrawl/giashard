package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/paracrawl/giashard"
)

var outdir string
var inputslist string
var shards uint
var batchsize int64
var fileslist string
var domainList string
var isjsonl bool

var schema = []string{"url", "mime", "plain_text"}

func init() {
	flag.StringVar(&outdir, "o", ".", "Output location")
	flag.StringVar(&inputslist, "l", "", "Input file listing either directories/files to shard")
	flag.StringVar(&fileslist, "f", "url,mime,plain_text", "Files to shard, separated by commas (ignored if JSONL)")
	flag.UintVar(&shards, "n", 8, "Number of shards (2^n)")
	flag.Int64Var(&batchsize, "b", 100, "Batch size in MB")
	flag.StringVar(&domainList, "d", "", "Additional public suffix entries")
	flag.BoolVar(&isjsonl, "jsonl", false, "Input is in JSONL format (not Paracrawl column storage format)")
	flag.Usage = func() {
		_, err := fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] input directories\n", os.Args[0])
		if err != nil {
			return
		}
		flag.PrintDefaults()
		_, err = fmt.Fprintf(flag.CommandLine.Output(),
			`Shards together the files given on input. They are assumed to be either in the standard 
			Paracrawl column storage format, or in JSONL format with the data to shard given the names 
			"url" and "text" in each record. 
			The output is a tree of directories of the form: outdir/shard/batch where shard is 
			computed as a hash of the significant part of the hostname in a url and batch is 
			approximately fixed size.
`)
		if err != nil {
			return
		}
	}
}

// to deal with two input formats
type Reader interface {
	Rows() chan map[string][]byte
	Close() error
}

func NewReader(source string, schema []string, isjsonl bool) (Reader, error) {
	var r Reader
	var err error

	if isjsonl {
		r, err = giashard.NewJsonlReader(source)
		if err != nil {
			log.Printf("Error opening input reader: %v", err)
			return r, err
		}
		log.Println("Using JSONL reader")
	} else {
		r, err = giashard.NewColumnReader(source, schema...)
		if err != nil {
			log.Printf("Error opening input reader: %v", err)
			return r, err
		}
		log.Println("Using Column reader")
	}

	return r, nil
}

func processfile(source string, schema []string, w *giashard.Shard, hostname string, isjsonl bool) {
	log.Printf("Processing input: %v", source)
	var r Reader
	var err error

	r, err = NewReader(source, schema, isjsonl)
	if err != nil {
		log.Fatal("Error creating Reader:", err)
	}

	// Provenance data tells us origin of a particular output.
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

	err = r.Close()
	if err != nil {
		log.Printf("Error closing reader: %v", err)
		return
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	flag.Parse()
	schema = strings.Split(fileslist, ",")
	if isjsonl {
		schema = []string{"url", "text"} // need a fixed schema for jsonl
	}

	// these are extra top-level domains to pick up e.g. '.com', '.co.uk'
	if domainList != "" {
		count, err := giashard.AddRulesToDefaultList(domainList)
		if err != nil {
			log.Fatalf("Error loading domain list: %v", err)
		} else {
			log.Printf("Loaded %d additional public suffix domains.", count)
		}
	}

	w, err := giashard.NewShard(outdir, shards, batchsize*1024*1024, "url", append(schema, "source")...)
	if err != nil {
		log.Fatalf("Error opening output shards: %v", err)
	}
	defer func(w *giashard.Shard) {
		var err = w.Close()
		if err != nil {

		}
	}(w)

	hostname, err := os.Hostname() // returns hostname reported by the kernel
	if err != nil {
		log.Fatalf("Error getting local hostname: %v", err)
	}

	// read in inputs from command line
	for i := 0; i < flag.NArg(); i++ {
		source := flag.Arg(i)
		processfile(source, schema, w, hostname, isjsonl)
	}

	// read in inputs from text file if specified
	if inputslist != "" {
		file, err := os.Open(inputslist)
		if err != nil {
			log.Fatal(err)
		}
		defer func(file *os.File) {
			var err = file.Close()
			if err != nil {

			}
		}(file)

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			source := scanner.Text()
			processfile(source, schema, w, hostname, isjsonl)
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}
}
