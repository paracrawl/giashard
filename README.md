## Sharding for Web-Scale Parallel Text Mining

This is the tool that takes a directory (or a list of directories) in
bitextor column storage format (with url.gz, mime.gz, plain_text.gz) and
sorts each row into a shard. Within those shards are batches. It is
called `giashard` and lives in https://github.com/paracrawl/giashard

For example,

    $ giashard -n 8 -b 1024 -o wide00006-shards/ca wide00006-text/WIDE-20120921042920-crawl427/ca

will take all of the Catalàn data in crawl427 and spread it over 2^8
shards. Each of those shards will contain batches of up to 1024MB each.

There is a companion tool called `giashardid` that you can give a URL
to either on the command line or stdin, and it will print the shard id
that that URL will get sorted to. If you give it the `-s` flag, instead
of printing the shard id, it will print the slug derived from the
hostname in the URL.

So, for example, we can find out what shard, Google lives in,

    $ giashardid google.com
    48

And then, if we are curious, we can find out what other domains
containing Dutch text live in that shard,

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

