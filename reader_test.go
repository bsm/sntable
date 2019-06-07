package sntable_test

import (
	"fmt"

	"github.com/bsm/sntable"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

var _ = Describe("Reader", func() {
	var subject *sntable.Reader

	HavePos := func(n int) types.GomegaMatcher {
		return WithTransform(func(x interface{ Pos() int }) int {
			return x.Pos()
		}, Equal(n))
	}

	// The following will seed 100 keys into 4 blocks:
	//
	// B0:   0..120
	// B1: 124..244
	// B2: 248..368
	// B3: 372..396
	//
	BeforeEach(func() {
		var err error
		subject, err = seedReader(100)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should init", func() {
		Expect(subject.NumBlocks()).To(Equal(4))

		tr10k, err := seedReader(10000)
		Expect(err).NotTo(HaveOccurred())
		Expect(tr10k.NumBlocks()).To(Equal(323))
	})

	It("should Get/Append", func() {
		for i := uint64(0); i <= 396; i += 4 {
			sfx := fmt.Sprintf("%04d", i)
			Expect(subject.Get(i)).To(HaveSuffix(sfx), "for %d", i)
		}

		_, err := subject.Get(1)
		Expect(err).To(MatchError(sntable.ErrNotFound))
		_, err = subject.Get(395)
		Expect(err).To(MatchError(sntable.ErrNotFound))
		_, err = subject.Get(400)
		Expect(err).To(MatchError(sntable.ErrNotFound))
	})

	It("should retrieve blocks", func() {
		b0, err := subject.GetBlock(0)
		Expect(err).NotTo(HaveOccurred())
		Expect(b0.Pos()).To(Equal(0))

		b1, err := subject.GetBlock(1)
		Expect(err).NotTo(HaveOccurred())
		Expect(b1.Pos()).To(Equal(1))

		b0, err = subject.GetBlock(-1)
		Expect(err).NotTo(HaveOccurred())
		Expect(b0.Pos()).To(Equal(0))
	})

	It("should seek blocks", func() {
		Expect(subject.SeekBlock(50)).To(HavePos(0))
		Expect(subject.SeekBlock(120)).To(HavePos(0))
		Expect(subject.SeekBlock(121)).To(HavePos(1))
		Expect(subject.SeekBlock(360)).To(HavePos(2))
		Expect(subject.SeekBlock(370)).To(HavePos(3))
		Expect(subject.SeekBlock(396)).To(HavePos(3))
		Expect(subject.SeekBlock(397)).To(HavePos(4))
		Expect(subject.SeekBlock(1000)).To(HavePos(4))
	})

	Describe("BlockReader", func() {
		var block *sntable.BlockReader

		// B1 (124..244) is split into 2 sections:
		// S0: 124..184
		// S1: 188..244
		BeforeEach(func() {
			var err error
			block, err = subject.GetBlock(1)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should have pos", func() {
			Expect(block.Pos()).To(Equal(1))
		})

		It("should have sections", func() {
			Expect(block.NumSections()).To(Equal(2))
			Expect(block.GetSection(0).Pos()).To(Equal(0))
			Expect(block.GetSection(1).Pos()).To(Equal(1))
			Expect(block.GetSection(2).Pos()).To(Equal(2))
			Expect(block.GetSection(3).Pos()).To(Equal(2))
			Expect(block.GetSection(-1).Pos()).To(Equal(0))
		})

		It("should seek sections", func() {
			Expect(block.SeekSection(0).Pos()).To(Equal(0))
			Expect(block.SeekSection(120).Pos()).To(Equal(0))
			Expect(block.SeekSection(184).Pos()).To(Equal(0))
			Expect(block.SeekSection(187).Pos()).To(Equal(0))
			Expect(block.SeekSection(188).Pos()).To(Equal(1))
			Expect(block.SeekSection(244).Pos()).To(Equal(1))
			Expect(block.SeekSection(245).Pos()).To(Equal(2))
		})
	})

	Describe("SectionReader", func() {
		var section *sntable.SectionReader

		// S1: 188..244
		BeforeEach(func() {
			block, err := subject.GetBlock(1)
			Expect(err).NotTo(HaveOccurred())

			section = block.GetSection(1)
		})

		It("should have pos", func() {
			Expect(section.Pos()).To(Equal(1))
		})

		It("should seek", func() {
			Expect(section.Seek(200)).To(BeTrue())
			Expect(section.Next()).To(BeTrue())
			Expect(section.Key()).To(Equal(uint64(200)))

			Expect(section.Seek(229)).To(BeTrue())
			Expect(section.Next()).To(BeTrue())
			Expect(section.Key()).To(Equal(uint64(232)))
		})

		It("should iterate", func() {
			Expect(section.More()).To(BeTrue())
			Expect(section.Next()).To(BeTrue())
			Expect(section.Key()).To(Equal(uint64(188)))
			Expect(section.Value()).To(HaveSuffix("0188"))

			Expect(section.More()).To(BeTrue())
			Expect(section.Next()).To(BeTrue())
			Expect(section.Key()).To(Equal(uint64(192)))
			Expect(section.Value()).To(HaveSuffix("0192"))

			for i := 0; i < 12; i++ {
				Expect(section.More()).To(BeTrue())
				Expect(section.Next()).To(BeTrue())
			}
			Expect(section.Key()).To(Equal(uint64(240)))
			Expect(section.Value()).To(HaveSuffix("0240"))

			Expect(section.More()).To(BeTrue())
			Expect(section.Next()).To(BeTrue())
			Expect(section.Key()).To(Equal(uint64(244)))
			Expect(section.Value()).To(HaveSuffix("0244"))

			Expect(section.More()).To(BeFalse())
			Expect(section.Next()).To(BeFalse())
		})
	})

	Describe("Iterator", func() {
		It("should iterate from beginning", func() {
			iter, err := subject.Seek(0)
			Expect(err).NotTo(HaveOccurred())
			defer iter.Release()

			Expect(iter.More()).To(BeTrue())
			Expect(iter.Next()).To(BeTrue())
			Expect(iter.Key()).To(Equal(uint64(0)))
			Expect(iter.Value()).To(HaveSuffix("0000"))

			Expect(iter.More()).To(BeTrue())
			Expect(iter.Next()).To(BeTrue())
			Expect(iter.Key()).To(Equal(uint64(4)))
			Expect(iter.Value()).To(HaveSuffix("0004"))

			for i := 0; i < 97; i++ {
				Expect(iter.More()).To(BeTrue())
				Expect(iter.Next()).To(BeTrue())
			}

			Expect(iter.More()).To(BeTrue())
			Expect(iter.Next()).To(BeTrue())
			Expect(iter.Key()).To(Equal(uint64(396)))
			Expect(iter.Value()).To(HaveSuffix("0396"))

			Expect(iter.More()).To(BeFalse())
			Expect(iter.Next()).To(BeFalse())
			Expect(iter.Err()).NotTo(HaveOccurred())
		})

		It("should iterate from middle", func() {
			iter, err := subject.Seek(200)
			Expect(err).NotTo(HaveOccurred())
			defer iter.Release()

			Expect(iter.Next()).To(BeTrue())
			Expect(iter.Key()).To(Equal(uint64(200)))
		})

		It("should iterate from last entry", func() {
			iter, err := subject.Seek(396)
			Expect(err).NotTo(HaveOccurred())
			defer iter.Release()

			Expect(iter.More()).To(BeTrue())
			Expect(iter.Next()).To(BeTrue())
			Expect(iter.Key()).To(Equal(uint64(396)))
			Expect(iter.Value()).To(HaveSuffix("0396"))

			Expect(iter.More()).To(BeFalse())
			Expect(iter.Next()).To(BeFalse())
			Expect(iter.Err()).NotTo(HaveOccurred())
		})

		It("should not iterate when past the end", func() {
			iter, err := subject.Seek(1000)
			Expect(err).NotTo(HaveOccurred())
			defer iter.Release()

			Expect(iter.More()).To(BeFalse())
			Expect(iter.Next()).To(BeFalse())
			Expect(iter.Err()).NotTo(HaveOccurred())
		})
	})
})
