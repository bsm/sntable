# SNTable

[![GoDoc](https://godoc.org/github.com/bsm/sntable?status.svg)](https://godoc.org/github.com/bsm/sntable)
[![Build Status](https://travis-ci.org/bsm/sntable.png?branch=master)](https://travis-ci.org/bsm/sntable)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Custom [SSTable](https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/) implementation
for [Go](https://golang.org). Instead of arbitrary bytes trings, this implementation assumes numeric 8-byte (`uint64`)
keys.

## Examples

**Writer:**

```go
package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/bsm/sntable"
)

func main() {{ "ExampleWriter" | code }}
```

**Reader:**


```go
package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/bsm/sntable"
)

func main() {{ "ExampleReader" | code }}
```

## Stats, Lies, Benchmarks

```sh
$ go version
go version go1.12.5 linux/amd64

$ sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'

$ go test -bench=. -benchmem
goos: linux
goarch: amd64
pkg: github.com/bsm/sntable/bench
Benchmark/bsm/sntable_10M_plain-4         	  500000	      2381 ns/op	     208 B/op	       4 allocs/op
Benchmark/golang/leveldb_10M_plain-4      	  200000	      6763 ns/op	   10301 B/op	       6 allocs/op
Benchmark/syndtr/goleveldb_10M_plain-4    	  300000	      5134 ns/op	     691 B/op	       6 allocs/op
Benchmark/bsm/sntable_10M_snappy-4        	 1000000	      2324 ns/op	     208 B/op	       4 allocs/op
Benchmark/golang/leveldb_10M_snappy-4     	  200000	      6630 ns/op	   10301 B/op	       6 allocs/op
Benchmark/syndtr/goleveldb_10M_snappy-4   	  200000	      5917 ns/op	     696 B/op	       6 allocs/op
```
