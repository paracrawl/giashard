package giashard

import (
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

// we need a specific error type to distinguish from cases where we
// just can't figure out what the shard should be because of bad
// input (which could reasonably be skipped or sent to an explicit
// "corrupted" shard) from errors writing to output which should
// generally be fatal
type ShardErr struct {
	s string
	e error
}

var ShardError *ShardErr

func NewShardErr(s string, e error) *ShardErr {
	return &ShardErr{s, e}
}

func (se *ShardErr) Error() (errs string) {
	if se.e == nil {
		errs = se.s
	} else {
		errs = fmt.Sprintf("%s: %v", se.s, se.e)
	}
	return
}

func (se *ShardErr) Is(target error) bool {
	_, ok := target.(*ShardErr)
	return ok
}

func (se *ShardErr) Unwrap() (err error) {
	return se.e
}

var host_re *regexp.Regexp
var path_re *regexp.Regexp
func init() {
	host_re = regexp.MustCompile(`^([a-zA-Z0-9][a-zA-Z0-9\-.]*[a-zA-Z0-9]).*`)
	path_re = regexp.MustCompile(`^([^/]+).*`)
	ShardError = NewShardErr("Unspecified error", nil)
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

func AddRulesToDefaultList(domainList string) (added int, err error) {
	rules, err := publicsuffix.DefaultList.LoadFile(domainList, nil)
	return len(rules), err
}

func Slug(key string) (slug string, err error) {
	// parse the url to get the domain name
	url, e := url.Parse(key)
	var host string
	if e != nil || len(url.Host) == 0 {
		// if we can't parse it, try to extract something sensible using a regexp
		ms := host_re.FindStringSubmatch(key)
		if len(ms) != 2 {
			err = NewShardErr(fmt.Sprintf("Unable to determine host using regexp from %v", key), e)
			return
		}
		host = ms[1]
	} else {
		host = strings.TrimRight(url.Host, ".") // a trailing . will confuse publicsuffix
	}

	// parse the domain name to get the slug
	dn, err := publicsuffix.Parse(host)
	if err != nil {
		// last ditch effort to get something reasonable out of the key
		ms := path_re.FindStringSubmatch(key)
		if len(ms) != 2 || len(ms[1]) == 0 {
			err = NewShardErr(fmt.Sprintf("Unable to determine slug by parsing %v from %v", host, key), err)
		}
		slug = ms[1]
		err = nil
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

// This returns an error of ShardErr kind if the error relates to
// figuring out what shard the data should be in. Generally this should
// not be fatal: no writing will have happened and it is safe to just
// skip to the next row. If a different kind of error is returned, it
// relates to writing the output and should be considered fatal.
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
