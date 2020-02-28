package giashard

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"github.com/weppos/publicsuffix-go/publicsuffix"
)

type Shard struct {
	dir string       // root directory
	n uint           // number of shards (2^n)
	size int64       // batch size
	key  string      // key to use for sharding
	cols []string    // columns
	batches []*Batch
}

var host_re *regexp.Regexp
func init() {
	host_re = regexp.MustCompile(`^([a-zA-Z0-9][a-zA-Z0-9.]*[a-zA-Z0-9]).*`)
}

// disperse records over 2^n shards using key, with batch sizes of size
// this uses the idea of "domain" from publicsuffix, which tries to get the
// most "significant" part of a domain name, stripping prefixes and suffixes
func NewShard(dir string, n uint, size int64, key string, cols ...string) (s *Shard, err error) {
	batches := make([]*Batch, 1 << n)
	s = &Shard{dir, n, size, key, cols, batches}
	return
}

func (s *Shard)Close() (err error) {
	for _, b := range s.batches {
		if b != nil {
			e := b.Close()
			if e != nil {
				err = e
			}
		}
	}
	return
}

func Slug(key string) (slug string, err error) {
	// parse the url to get the domain name
	url, e := url.Parse(key)
	var host string
	if e != nil || len(url.Host) == 0 {
		// if we can't parse it, try to extract something sensible using a regexp
		ms := host_re.FindStringSubmatch(key)
		if len(ms) != 2 {
			err = errors.New(fmt.Sprintf("Unable to determine host using regexp from %v", key))
			return
		}
		host = ms[1]
	} else {
		host = strings.TrimRight(url.Host, ".") // a trailing . will confuse publicsuffix
	}

	// parse the domain name to get the slug
	dn, err := publicsuffix.Parse(host)
	if err != nil {
		// again, if we can't parse it, just keep the whole URL
		slug = host
	} else {
		slug = dn.SLD
	}
	return
}

func ShardId(key string, n uint) (shard uint64, err error) {
	hash := fnv.New64()

	slug, err := Slug(key)
	if err != nil {
		return
	}

	// use the slug to compute the hash
	_, err = hash.Write([]byte(slug))
	if err != nil {
		return
	}

	shard = hash.Sum64() % (1 << n)
	return
}

func (s *Shard)WriteRow(row map[string][]byte) (err error) {
	key  := row[s.key]

	shard, err := ShardId(string(key), s.n)
	if err != nil {
		return
	}

	if s.batches[shard] == nil {
		b, err := s.openShard(shard)
		if err != nil {
			return err
		}
		s.batches[shard] = b
	}

	err = s.batches[shard].WriteRow(row)

	return
}

func (s *Shard)openShard(shard uint64) (b *Batch, err error) {
	sdir := s.shardDir(shard)
	log.Printf("Initialising shard %d at %s", shard, sdir)
	if err = os.MkdirAll(sdir, os.ModePerm); err != nil {
		return
	}

	b, err = NewBatch(sdir, s.size, s.cols...)
	return
}

func (s *Shard)shardDir(n uint64) string {
	return filepath.Join(s.dir, strconv.FormatInt(int64(n), 10))
}
