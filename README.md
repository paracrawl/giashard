## Sharding for Web-Scale Parallel Text Mining

`giashard` is a tool for batching webcrawled data for later processing. As input, it takes a list of files in JSONL format where the source URL of each record has the key `url` and the associated extracted text has the key `text`. Any other key-value pairs in each record are ignored. The program then sorts each record into a shard based on a hash of the URL. The output is a directory of these shards where each shard contains batches of roughly equal size.

Example use:

`giashard -n 8 -b 1024 -o mlt_Latn_output/cc23 cc23/mlt_Latn/batch_1.jsonl.zst`

This takes the Maltese data contained in the batch and spreads it over 2^8 shards (set by `-n`). Each of these shards will contain batches up to 1024MB each (set by `-b`). The numbered shard/batch directories are output to the directory `mlt_Latn_output/cc23` (set by `-o`). 

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

