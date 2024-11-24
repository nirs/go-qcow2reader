package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/lima-vm/go-qcow2reader"
	"github.com/lima-vm/go-qcow2reader/log"
)

const (
	blockSize = 64 * 1024
)

func cmdChecksum(args []string) error {
	var (
		// Required
		filename string

		// Options
		debug bool
	)

	fs := flag.NewFlagSet("blksum", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s checksum [OPTIONS...] FILENAME\n", os.Args[0])
		flag.PrintDefaults()
	}
	fs.BoolVar(&debug, "debug", false, "enable printing debug messages")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if debug {
		log.SetDebugFunc(logDebug)
	}

	switch len(fs.Args()) {
	case 0:
		return errors.New("no file was specified")
	case 1:
		filename = fs.Arg(0)
	default:
		return errors.New("too many files were specified")
	}

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	img, err := qcow2reader.Open(f)
	if err != nil {
		return err
	}
	defer img.Close()

	innerHash := sha256.New()
	outerHash := sha256.New()

	buf := make([]byte, 2*1024*1024)
	zeroBlock := make([]byte, blockSize)

	// Compute zero block hash once.
	innerHash.Write(zeroBlock)
	zeroMd := innerHash.Sum(nil)

	start := int64(0)
	end := img.Size()
	for start < end {
		// Fast path: extents that read as zeros.
		extent, err := img.Extent(start, end-start)
		if err != nil {
			return err
		}
		if extent.Zero {
			for i := int64(0); i < extent.Length; i += blockSize {
				outerHash.Write(zeroMd)
			}
			start += extent.Length
			continue
		}

		// Slow path: allocated extents
		for extent.Length > 0 {
			// The last read may be shorter.
			n := len(buf)
			if extent.Length < int64(len(buf)) {
				n = int(extent.Length)
			}

			// Read next buffer in this extent.
			nr, err := img.ReadAt(buf[:n], start)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}

			// Consume blocks in this buffer
			for bufStart := 0; bufStart < nr; bufStart += blockSize {
				block := buf[bufStart : bufStart+blockSize]
				if bytes.Equal(block, zeroBlock) {
					// Faster path: block full of zeros.
					outerHash.Write(zeroMd)
				} else {
					// Slowest path: block with some data.
					innerHash.Reset()
					innerHash.Write(block)
					outerHash.Write(innerHash.Sum(nil))
				}
			}

			extent.Length -= int64(nr)
			start += int64(nr)
		}
	}

	// Finalize the hash.
	binary.Write(outerHash, binary.LittleEndian, img.Size())

	fmt.Printf("%x  %s\n", outerHash.Sum(nil), filename)

	return nil
}
