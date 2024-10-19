package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/lima-vm/go-qcow2reader"
	"github.com/lima-vm/go-qcow2reader/convert"
)

func main() {
	var options convert.Options

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS...] SRC TARGET\n", os.Args[0])
		flag.PrintDefaults()
	}

	// These options is mainly for benchmarking the default values. Users are not
	// expected to change them.
	flag.Int64Var(&options.SegmentSize, "segment-size", convert.SegmentSize, "worker segment size in bytes")
	flag.IntVar(&options.BufferSize, "buffer-size", convert.BufferSize, "buffer size in bytes")
	flag.IntVar(&options.Workers, "workers", convert.Workers, "number of workers")

	flag.Parse()
	args := flag.Args()

	if len(args) != 2 {
		flag.Usage()
		os.Exit(1)
	}

	src, err := os.Open(args[0])
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	srcImg, err := qcow2reader.Open(src)
	if err != nil {
		log.Fatal(err)
	}
	defer srcImg.Close()

	dst, err := os.Create(args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer dst.Close()

	if err := dst.Truncate(srcImg.Size()); err != nil {
		log.Fatal(err)
	}

	c := convert.New(options)
	if err := c.Convert(dst, srcImg, srcImg.Size()); err != nil {
		log.Fatal(err)
	}

	if err := dst.Sync(); err != nil {
		log.Fatal(err)
	}

	if err := dst.Close(); err != nil {
		log.Fatal(err)
	}
}
