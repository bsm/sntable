package bench_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/bsm/sntable"
	"github.com/golang/leveldb/db"
	leveldb "github.com/golang/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	goleveldb "github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func Benchmark(b *testing.B) {
	b.Run("bsm/sntable 10M plain", func(b *testing.B) {
		benchSnTable(b, 10e6, false)
	})
	b.Run("golang/leveldb 10M plain", func(b *testing.B) {
		benchLevelDB(b, 10e6, false)
	})
	b.Run("syndtr/goleveldb 10M plain", func(b *testing.B) {
		benchGoLevelDB(b, 10e6, false)
	})

	b.Run("bsm/sntable 10M snappy", func(b *testing.B) {
		benchSnTable(b, 10e6, true)
	})
	b.Run("golang/leveldb 10M snappy", func(b *testing.B) {
		benchLevelDB(b, 10e6, true)
	})
	b.Run("syndtr/goleveldb 10M snappy", func(b *testing.B) {
		benchGoLevelDB(b, 10e6, true)
	})
}

func benchSnTable(b *testing.B, numSeeds int, compress bool) {
	fname := createSeedFile(b, "sntable", numSeeds, compress, func(f *os.File) error {
		o := &sntable.WriterOptions{
			BlockSize:            8 * 1024,
			BlockRestartInterval: 1024,
			Compression:          sntable.NoCompression,
		}
		if compress {
			o.Compression = sntable.SnappyCompression
		}
		w := sntable.NewWriter(f, o)
		defer w.Close()

		eachKVPair(b, numSeeds, func(num uint64, val []byte) error {
			return w.Append(num, val)
		})

		return w.Close()
	})

	openSeedFile(b, fname, func(file *os.File, size int64) error {
		read, err := sntable.NewReader(file, size)
		if err != nil {
			b.Fatal(err)
		}

		sink := make([]byte, 0, 256)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := uint64(i % (2 * numSeeds))
			_, err := read.Append(sink[:0], key)
			if err != nil && err != sntable.ErrNotFound {
				b.Fatal(err)
			}
		}
		return nil
	})
}

func benchLevelDB(b *testing.B, numSeeds int, compress bool) {
	fname := createSeedFile(b, "leveldb", numSeeds, compress, func(f *os.File) error {
		o := &db.Options{
			BlockSize:            8 * 1024,
			BlockRestartInterval: 1024,
			Compression:          db.NoCompression,
			WriteBufferSize:      64 * 1024 * 1024,
		}
		if compress {
			o.Compression = db.SnappyCompression
		}
		w := leveldb.NewWriter(f, o)
		defer w.Close()

		eachKVPair(b, numSeeds, func(num uint64, val []byte) error {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, num)
			return w.Set(key, val, nil)
		})

		return w.Close()
	})

	openSeedFile(b, fname, func(file *os.File, _ int64) error {
		read := leveldb.NewReader(file, nil)
		defer read.Close()

		key := make([]byte, 8)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binary.BigEndian.PutUint64(key, uint64(i%(2*numSeeds)))
			_, err := read.Get(key, nil)
			if err != nil && err != db.ErrNotFound {
				b.Fatal(err)
			}
		}
		return nil
	})
}

func benchGoLevelDB(b *testing.B, numSeeds int, compress bool) {
	opts := opt.Options{
		DisableBlockCache:    true,
		BlockCacher:          opt.NoCacher,
		BlockSize:            8 * 1024,
		BlockRestartInterval: 1024,
		Compression:          opt.NoCompression,
		WriteBuffer:          64 * 1024 * 1024,
		Strict:               opt.NoStrict,
	}
	if compress {
		opts.Compression = opt.SnappyCompression
	}

	fname := createSeedFile(b, "goleveldb", numSeeds, compress, func(f *os.File) error {
		w := goleveldb.NewWriter(f, &opts)
		defer w.Close()

		eachKVPair(b, numSeeds, func(num uint64, val []byte) error {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, num)
			return w.Append(key, val)
		})

		return w.Close()
	})

	openSeedFile(b, fname, func(file *os.File, size int64) error {
		pool := util.NewBufferPool(opts.BlockSize)
		defer pool.Close()

		read, err := goleveldb.NewReader(file, size, storage.FileDesc{}, nil, pool, &opts)
		if err != nil {
			b.Fatal(err)
		}
		defer read.Release()

		key := make([]byte, 8)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binary.BigEndian.PutUint64(key, uint64(i%(2*numSeeds)))
			val, err := read.Get(key, nil)
			if err != nil && err != goleveldb.ErrNotFound {
				b.Fatal(err)
			} else if val != nil {
				pool.Put(val)
			}
		}
		return nil
	})
}

// --------------------------------------------------------------------

func createSeedFile(b *testing.B, prefix string, numSeeds int, compress bool, cb func(*os.File) error) string {
	b.Helper()

	suffix := "plain"
	if compress {
		suffix = "snappy"
	}
	fname := fmt.Sprintf("seed.%s.%d.%s", prefix, numSeeds, suffix)
	if _, err := os.Stat(fname); err == nil {
		return fname
	} else if !os.IsNotExist(err) {
		b.Fatal(err)
	}

	f, err := os.Create(fname)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	if err := cb(f); err != nil {
		b.Fatal(err)
	}
	return fname
}

func openSeedFile(b *testing.B, fname string, cb func(*os.File, int64) error) {
	b.Helper()

	file, err := os.Open(fname)
	if err != nil {
		b.Fatal(err)
	}

	stat, err := file.Stat()
	if err != nil {
		b.Fatal(err)
	}

	if err := cb(file, stat.Size()); err != nil {
		b.Fatal(err)
	}

	b.StopTimer()
}

func eachKVPair(b *testing.B, numSeeds int, cb func(uint64, []byte) error) {
	b.Helper()

	rnd := rand.New(rand.NewSource(33))
	val := make([]byte, 128)

	for i := 0; i < numSeeds*2; i += 2 {
		if _, err := rnd.Read(val); err != nil {
			b.Fatal(err)
		}
		if err := cb(uint64(i), val); err != nil {
			b.Fatal(err)
		}
	}
}
