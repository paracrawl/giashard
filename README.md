# `giashard`: Sharding for Web-Scale Parallel Text Mining

`giashard` is a tool for batching webcrawled data for later processing. It is designed as part of a corpus creation pipeline in projects like [Paracrawl](https://paracrawl.eu/) and [HPLT](https://hplt-project.org/). 

## Installation

`giashard` is written in Go. To install, you need to clone the repo and then build the application:

```bash
git clone https://github.com/paracrawl/giashard.git
cd giashard/cmd/giashard
go build
```

## Running `giashard`
`giashard` can accept three input formats:
1) A directory (or list of directories) in bitextor/Paracrawl column storage format: each directory contains three files named `url.gz`, `mime.gz` and `plain_text.gz` (by default). A different number of files and different names for these files can be specified with the `-f` flag
2) A zstd-compressed file (or list of files) in the JSONL format where each record contains at minimum one field named `u` containing a URL and one field named `text` containing the extracted content in plain text.
3) An uncompressed stream to stdin in the above JSONL format (indicated by `-` as the input file: e.g. `cat myfile.jsonl | giashard -o myoutput -`)

`giashard` uses the following flags:
- `-o`: Output directory location (default: current directory)
- `-l`: Input file containing a list of files/directories to shard (default: "")
- `-f`: Comma-separated list of files to shard for bitextor/Paracrawl column storage format input (default:`"url,mime,plaintext"`)
- `-n`: Exponent to calculate number of shards (2^n) (default: 8)
- `-b`: Batch size in MB (default: 100)
- `-d`: Additional public suffix entries (default: "")
- `-jsonl`: Boolean indicating data is in JSONL format (default: False)

Example command:
```bash
ls -1d output_wide15_filtered/*/is | xargs giashard/cmd/giashard/giashard -n 8 -o output_wide15_sharded -f text,url -b 1024
```

This runs `giashard` on all Icelandic data in the `output_wide15_filtered` directory (in bitextor/Paracrawl column storage format) where each input directory contains two files: `text.gz` and `url.gz`. It sorts this data into 2^8 numbered shards where shard membership is assigned based on a hash of the URL. The data in each shard is split into numbered batches of approximately 1024MB. Output text is base64 encoded.


## `giashardid`

There is a companion tool called `giashardid` that you can give a URL to either on the command line or stdin, and it will print the shard id that that URL will get sorted to. If you give it the `-s` flag, instead of printing the shard id, it will print the slug derived from the hostname in the URL.

So, for example, we can find out what shard, Google lives in,

    $ giashardid google.com
    48

And then, if we are curious, we can find out what other domains containing Dutch text live in that shard,

    $ find wide00006-shards/nl/48 -name url.gz | xargs cat | gzip -dc | \
        giashardid -s | sort | uniq -c | sort -nr | head -10
   6483 google
    855 paginamarkt
    604 vikingdirect
    592 ajax1
    392 jijislief
    277 ixina
    209 punkyfish
    182 bongo
    154 ooyyo
    150 ledlampendirect

This should be easily installable using

    go get github.com/paracrawl/giashardid/cmd/...

