# linesd

## builds

[![Build Status](https://travis-ci.org/korovkin/limiter.svg)](https://travis-ci.org/korovkin/linesd)

## data aggregation

seemlesly collect and uppload batches of data (lines) to AWS S3 for long term storage
and / or Elastic Search cluster for indexing and searching.

## example

the following example will consume lines from stdin and push batches of 60 lines / 60 seconds worth of data to S3 bucket and/or Elastic Search cluster of your choice.

```
while [ yes ] ; do date; sleep 1; done | \
    go run cmd/linesd/main.go --config config.json \
       --batch_size_seconds 60 \
       --batch_size_lines 60
```

batches of data are accumulated in memory before being uploaded.
https://github.com/prometheus is used for monitoring health and perf of the running process.


## references

1. see: `scribed` - https://github.com/facebookarchive/scribe
2. see: `splunk` - https://www.splunk.com/

## coming soon:

1. binary releases
2. tests
