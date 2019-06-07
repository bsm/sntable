package sntable_test

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/bsm/sntable"
)

func ExampleWriter() {
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

func ExampleReader() {
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
