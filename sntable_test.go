package sntable_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/bsm/sntable"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "sntable")
}

// --------------------------------------------------------------------

func seedReader(sz int) (*sntable.Reader, error) {
	buf := new(bytes.Buffer)
	if err := seedTable(buf, sz); err != nil {
		return nil, err
	}
	return sntable.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
}

func seedTable(buf *bytes.Buffer, sz int) error {
	twr := sntable.NewWriter(buf, &sntable.WriterOptions{
		Compression: sntable.NoCompression,
	})
	rnd := rand.New(rand.NewSource(1))
	val := make([]byte, 128)

	for i := 0; i < sz; i++ {
		key := uint64(i * 4)
		if _, err := rnd.Read(val); err != nil {
			return err
		}

		val = append(val[:120], fmt.Sprintf("%08d", key)...)
		if err := twr.Append(key, val); err != nil {
			return err
		}
	}
	return twr.Close()
}
