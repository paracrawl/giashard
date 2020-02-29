package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"github.com/paracrawl/giashard"
	"gopkg.in/yaml.v2"
)

var calculate bool
var write bool
var summary bool
var jsonout bool

func init() {
	flag.BoolVar(&calculate, "c", false, "Force recalculation of statistics")
	flag.BoolVar(&write, "w", false, "Write statistics to shard")
	flag.BoolVar(&summary, "s", false, "Write summary health statistics to stdout")
	flag.BoolVar(&jsonout, "j", false, "Write output in json (as opposed to yaml)")
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

	var marshaller func (interface {}) ([]byte, error)
	if jsonout {
		marshaller = json.Marshal
	} else {
		marshaller = yaml.Marshal
	}

	if write {
		if err = stats.Write(); err != nil {
			log.Fatalf("error writing stats: %v", err)
		}
	} else if summary {
		health := health_tests(stats)
		b, err := marshaller(health)
		if err != nil {
			log.Fatalf("marshal health statistcs: %v", err)
		}
		os.Stdout.Write(b)
	} else {
		b, err := marshaller(stats)
		if err != nil {
			log.Fatalf("marshal statistics: %v", err)
		}
		os.Stdout.Write(b)
	}
}
