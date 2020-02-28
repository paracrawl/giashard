package giashard

import (
	"testing"
)

type testcase struct{ url string; n uint; slug string; shard uint64 }
var testcases [5]testcase = [...]testcase{
	// https://github.com/paracrawl/giashard/issues/1
	{"http://www.reddit.com/", 8, "reddit", 249},
	{"http://www.reddit.com/.", 8, "reddit", 249},
	{"http://www.reddit.com.", 8, "reddit", 249},
	{"www.reddit.com/.", 8, "reddit", 249},
	{"www.reddit.com.", 8, "reddit", 249},
}

func TestSlug(t *testing.T) {
	for _, tcase := range testcases {
		slug, err := Slug(tcase.url)
		if err != nil {
			t.Errorf("Slug(%v): error: %v", tcase.url, err)
		} else if slug != tcase.slug {
			t.Errorf("Slug(%v): got %v expected %v", tcase.url, slug, tcase.slug)
		}
	}
}

func TestShardId(t *testing.T) {
	for _, tcase := range testcases {
		shard, err := ShardId(tcase.url, tcase.n)
		if err != nil {
			t.Errorf("ShardId(%v, %v): error: %v", tcase.url, tcase.n, err)
		} else if shard != tcase.shard {
			t.Errorf("ShardId(%v, %v): got %d expected %d", tcase.url, tcase.n, shard, tcase.shard)
		}
	}
}
