package sntable_test

import (
	"bytes"
	"math/rand"

	"github.com/bsm/sntable"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Writer", func() {
	var buf *bytes.Buffer
	var subject *sntable.Writer
	var testdata = []byte("testdata")

	BeforeEach(func() {
		buf = new(bytes.Buffer)
		subject = sntable.NewWriter(buf, nil)
	})

	AfterEach(func() {
		_ = subject.Close()
	})

	It("should write empty", func() {
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(Equal(16))
	})

	It("should prevent out-of-order appends", func() {
		Expect(subject.Append(20, testdata)).To(Succeed())
		Expect(subject.Append(19, testdata)).To(MatchError(`sntable: attempted an out-of-order append, 19 must be > 20`))
		Expect(subject.Append(22, testdata)).To(Succeed())
		Expect(subject.Append(20, testdata)).To(MatchError(`sntable: attempted an out-of-order append, 20 must be > 22`))
		Expect(subject.Append(23, testdata)).To(Succeed())
		Expect(subject.Append(23, testdata)).To(MatchError(`sntable: attempted an out-of-order append, 23 must be > 23`))
		Expect(subject.Append(24, testdata)).To(Succeed())
	})

	It("should write (non-compressable)", func() {
		rnd := rand.New(rand.NewSource(1))
		val := make([]byte, 128)

		for key := uint64(0); key < 100000; key += 2 {
			_, err := rnd.Read(val)
			Expect(err).NotTo(HaveOccurred())
			Expect(subject.Append(key, val)).To(Succeed())
		}
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(BeNumerically("~", 6575289, 1024))
		Expect(buf.String()[buf.Len()-8:]).To(Equal("\x47\x27\x86\xBE\x1F\x7a\x65\xDB"))
	})

	It("should write (well-compressable)", func() {
		val := bytes.Repeat(testdata, 16)
		for key := uint64(0); key < 100000; key += 2 {
			Expect(subject.Append(key, val)).To(Succeed())
		}
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(BeNumerically("~", 362538, 1024))
		Expect(buf.String()[buf.Len()-8:]).To(Equal("\x47\x27\x86\xBE\x1F\x7a\x65\xDB"))
	})
})
