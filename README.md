# osdump

`osdump` is a high performance tool for extracting documents from OpenSearch indexes and saving them into files. 

## Features

* As high performance as a single worker solution can be
* Opensearch queries are based on `search_after`
* Uses `fastjson` for faster json parsing
* Built-in support for compressing the output using `brotli`
* Has some built-in sanity checks to ensure smooth operation

## Installation

```bash
$ go install github.com/mikkolehtisalo/osdump@latest
```

## Usage

The configuration options:
```bash
$ ~/go/bin/osdump -h
Usage of ./osdump:
  -base string
        opensearch base url (default "https://localhost:9200")
  -brotli
        compress using brotli
  -ca string
        CA certificate (default "ca.pem")
  -file string
        target file for export (default "graylog_0.json")
  -index string
        opensearch index (default "graylog_0")
  -debug
        debug logging (default false)
  -password string
        opensearch user (default "password")
  -quality int
        brotli quality setting (default 4)
  -size int
        search window size (default 1000)
  -user string
        opensearch user (default "graylog")
```

Example run:
```bash
$ ~/go/bin/osdump -user admin -password mysecretpassword -size 1000
2024/12/30 21:08:30 osdump.go:296: Starting to dump graylog_0
2024/12/30 21:08:30 osdump.go:300: Index graylog_0 has 272905 documents to dump
2024/12/30 21:09:53 osdump.go:311: Closed tasks channel
2024/12/30 21:09:53 osdump.go:320: Dumped 272905 records in 82 seconds, average speed 3314/second
2024/12/30 21:09:53 osdump.go:321: Finished dumping graylog_0
```

## Requirements

* Go 1.22+
* Access to an OpenSearch instance

## Limitations

* Large dumps may require large amounts of disk space
* Brotli's performance for compression is abysmal
* Assumes opensearch security is configured (TLS enabled, and username/password required)
* Single worker for querying opensearch, for now

## Contributing

This works for me. If you need more features, or find a bug, please open a pr, or an issue.

## License

osdump is licensed under the MIT License.

