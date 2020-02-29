package main

import (
	"github.com/paracrawl/giashard"
)

type HealthTest struct {
	Description string
	Status bool
}

type ShardHealth struct {
	Overall float32
	Tests map[string]HealthTest
}
func health_tests(stats *giashard.ShardStats) *ShardHealth {
	health := make(map[string]HealthTest)

/*
bytes:
	mime.gz: 2574
  plain_text.gz: 2454458
  sentences.gz: 20
  sentences_en.gz: 20
  source.gz: 6202
  tokenised.gz: -1
  tokenised_en.gz: 20
  url.gz: 17417
*/

	// 1. check that the basic files were found
	found := true
	for _, fname := range []string{"mime.gz", "plain_text.gz", "sentences.gz", "source.gz", "url.gz"} {
		b, ok := stats.Bytes[fname]
		if !ok || b < 0 {
			found = false
		}
	}

	english := false

	// 2. check that translated and tokenised files are found
	b, ok := stats.Bytes["sentences_en.gz"] 
	if ok && b > 0 { // if we have this, not english
		health["translated"] = HealthTest{"Translation is present", true}
		found = true
		b, ok = stats.Bytes["tokenised_en.gz"]
		if !ok || b < 0 {
			found = false
		}
		health["tokenised"] = HealthTest{"Tokenisation is present", found}
	} else { // we are dealing with english
		found = true
		english = true
		b, ok = stats.Bytes["tokenised.gz"]
		if !ok || b < 0 {
			found = false
		}
		health["tokenised"] = HealthTest{"Tokenisation is present", found}
	}

	// 3. check the number of records in files all match
	match := true
	nrecs, ok := stats.Records["url.gz"]
	if !ok {
		health["records"] = HealthTest{"URL column corrupt or missing. This is bad", false}
	} else if nrecs < 0 {
		health["records"] = HealthTest{"URL column corrupt or missing. This is bad", false}
	} else {
		for _, records := range stats.Records {
			if records > 0 && records != nrecs {
				match = false
			}
		}
		health["records"] = HealthTest{"Number of records matches", match}
	}

	// 4. check counts of lines match
	if ! english {
		nsents, nsok := stats.Native.Lines["sentences.gz"]
		esents, esok := stats.English.Lines["sentences_en.gz"]
		etoks, etok  := stats.English.Lines["tokenised_en.gz"]
		if !nsok || !esok || !etok {
			health["sentence_count"] = HealthTest{"Sentence count missing", false}
		} else {
			health["sentence_count"] = HealthTest{"Sentence count exists", true}
		}

		if nsents > 0 && nsents == esents {
			health["translation_count"] = HealthTest{"Translation count matches", true}
		} else {
			health["translation_count"] = HealthTest{"Translation count doesn't match", false}
		}

		if nsents > 0 && nsents == etoks {
			health["tokenised_count"] = HealthTest{"Tokenised english count matches", true}
		} else {
			health["tokenised_count"] = HealthTest{"Tokenised english count doesn't match", false}
		}
	} else {
		nsents, nsok := stats.Native.Lines["sentences.gz"]
		etoks, etok  := stats.English.Lines["tokenised.gz"]
		if !nsok || !etok {
			health["sentence_count"] = HealthTest{"Sentence count missing", false}
		} else {
			health["sentence_count"] = HealthTest{"Sentence count exists", true}
		}
		if nsents > 0 && nsents == etoks {
			health["tokenised_count"] = HealthTest{"Tokenised english count matches", true}
		} else {
			health["tokenised_count"] = HealthTest{"Tokenised english count doesn't match", false}
		}
	}

	// 5. check that histograms match...

	ntests := float32(0.0)
	passed := float32(0.0)
	for _, ht := range health {
		ntests += 1
		if ht.Status {
			passed += 1
		}
	}

	sh := ShardHealth{passed/ntests, health}
	return &sh
}
