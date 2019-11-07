# SNTable

[![GoDoc](https://godoc.org/github.com/bsm/sntable?status.svg)](https://godoc.org/github.com/bsm/sntable)
[![Build Status](https://travis-ci.org/bsm/sntable.png?branch=master)](https://travis-ci.org/bsm/sntable)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Custom [SSTable](https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/) implementation
for [Go](https://golang.org). Instead of arbitrary bytes strings, this implementation assumes numeric 8-byte (`uint64`)
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

func main() {
	// create a file
	f, err := ioutil.TempFile("", "sntable-example")
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	// wrap writer around file, append (neglecting errors for demo purposes)
	w := sntable.NewWriter(f, nil)
	_ = w.Append(101, []byte("foo"))
	_ = w.Append(102, []byte("bar"))
	_ = w.Append(103, []byte("baz"))

	// close writer
	if err := w.Close(); err != nil {
		log.Fatalln(err)
	}

	// explicitly close file
	if err := f.Close(); err != nil {
		log.Fatalln(err)
	}
}
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

func main() {
	// open a file
	f, err := os.Open("mystore.snt")
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	// get file size
	fs, err := f.Stat()
	if err != nil {
		log.Fatalln(err)
	}

	// wrap reader around file
	r, err := sntable.NewReader(f, fs.Size())
	if err != nil {
		log.Fatalln(err)
	}

	val, err := r.Get(101)
	if err == sntable.ErrNotFound {
		log.Println("Key not found")
	} else if err != nil {
		log.Fatalln(err)
	} else {
		log.Printf("Value: %q\n", val)
	}
}
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
