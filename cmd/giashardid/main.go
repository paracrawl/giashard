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
func init() {
	flag.UintVar(&shards, "n", 8, "Number of shards (2^n)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] [url]\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "Outputs the shard id for each given URL, one per line")
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	flag.Parse()


	if flag.NArg() > 0 {
		for i:=0; i<flag.NArg(); i++ {
			shard, err := giashard.ShardId(flag.Arg(i), shards)
			if err != nil {
				log.Fatalf("Error computing shard id: %v", err)
			}
			fmt.Println(shard)
		}
	} else {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			shard, err := giashard.ShardId(scanner.Text(), shards)
			if err != nil {
				log.Fatalf("Error computing shard id: %v", err)
			}
			fmt.Println(shard)
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "reading standard input:", err)
		}
	}
}
