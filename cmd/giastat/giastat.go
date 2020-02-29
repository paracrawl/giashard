package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"github.com/paracrawl/giashard"
	"gopkg.in/yaml.v2"
)

var calculate bool
var write bool

func init() {
	flag.BoolVar(&calculate, "r", false, "Force recalculation of statistics")
	flag.BoolVar(&write, "w", false, "Write statistics to shard")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] shard\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "Read, calculate and write shard statistics\n")
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(-1)
	}

	shard := flag.Arg(0)

	var stats *giashard.ShardStats
	var err error

	if calculate {
		stats = giashard.NewStats(shard)
		stats.Calc()
	} else {
		stats, err = giashard.ReadStats(shard)
		if err != nil {
			log.Fatalf("error reading stats: %v", err)
			stats = giashard.NewStats(shard)
		}
	}

	if write {
		if err = stats.Write(); err != nil {
			log.Fatalf("error writing stats: %v", err)
		}
	} else {
		b, err := yaml.Marshal(stats)
		if err != nil {
			log.Fatalf("marshal statistics: %v", err)
		}
		os.Stdout.Write(b)
	}
}
