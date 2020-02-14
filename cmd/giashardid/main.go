package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"github.com/paracrawl/giashard"
)

var shards uint
var slugs bool

func init() {
	flag.UintVar(&shards, "n", 8, "Number of shards (2^n)")
	flag.BoolVar(&slugs, "s", false, "Print slugs instead of shards")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] [url]\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "Outputs the shard id for each given URL, one per line")
	}
}

func urls() (ch chan string) {
	ch = make(chan string)
	go func() {
		if flag.NArg() > 0 {
			for i:=0; i<flag.NArg(); i++ {
				ch <- flag.Arg(i)
			}
		} else {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				ch <- scanner.Text()
			}
			if err := scanner.Err(); err != nil {
				log.Fatalf("Error scanning stdin: %v", err)
			}
		}
		close(ch)
	}()
	return
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	flag.Parse()


	for url := range urls() {
		if slugs {
			slug, err := giashard.Slug(url)
			if err != nil {
				log.Fatalf("Error computing slug: %v", err)
			}
			fmt.Println(slug)
		} else {
			shard, err := giashard.ShardId(url, shards)
			if err != nil {
				log.Fatalf("Error computing shard id: %v", err)
			}
			fmt.Println(shard)
		}
	}
}
