package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/midbel/cli"
	"golang.org/x/sync/errgroup"
)

var (
	MMA  = []byte("MMA ")
	CORR = []byte("CORR")
	SYNC = []byte("SYNC")
	RAW  = []byte("RAW ")
	Y800 = []byte("Y800")
	Y16B = []byte("Y16 ")
	Y16L = []byte("Y16L")
	I420 = []byte("I420")
	YUY2 = []byte("YUY2")
	RGB  = []byte("RGB ")
	JPEG = []byte("JPEG")
	PNG  = []byte("PNG ")
	H264 = []byte("H264")
	SVS  = []byte("SVS ")
	TIFF = []byte("TIFF")
)

var digestCommand = &cli.Command{
	Usage: "digest <datadir>",
	Alias: []string{"sum", "cksum"},
	Short: "compute the md5 checksum of all files under the given directory",
	Run:   runDigest,
}

func runDigest(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	sema := make(chan struct{}, 8)
	var grp errgroup.Group
	for p := range retrPaths(cmd.Flag.Arg(0)) {
		sema <- struct{}{}
		p := p
		grp.Go(func() error {
			defer func() {
				<-sema
			}()
			r, err := os.Open(p)
			if err != nil {
				return err
			}
			defer r.Close()

			magic := make([]byte, 4)
			if _, err := r.Read(magic); err != nil {
				return err
			}
			var buffer bytes.Buffer
			if _, err := io.CopyN(&buffer, r, skipBytes(magic)); err != nil {
				return err
			}
			digest := md5.New()
			if _, err := io.Copy(digest, r); err != nil {
				return err
			}
			var (
				unix     uint64
				sequence uint32
			)
			binary.Read(&buffer, binary.BigEndian, &sequence)
			binary.Read(&buffer, binary.BigEndian, &unix)

			n := GPS.Add(time.Duration(unix)).Format("2006-01-02 15:04:05.000")
			log.Printf("%s | %7d | %s | %x | %s", bytes.Trim(magic, "\x00"), sequence, n, digest.Sum(nil), filepath.Base(p))
			return nil
		})
	}
	return grp.Wait()
}

func retrPaths(base string) <-chan string {
  q := make(chan string)
  go func() {
    defer close(q)
    filepath.Walk(base, func(p string, i os.FileInfo, err error) error {
      if err != nil {
        return err
      }
      if i.IsDir() {
        return nil
      }
      if filepath.Ext(p) == ".xml" {
        return nil
      }
      q <- p
      return nil
    })
  }()
  return q
}

func skipBytes(magic []byte) int64 {
	skip := 12
	switch {
	default:
	case bytes.Equal(magic, MMA):
	case bytes.Equal(magic, CORR):
	case bytes.Equal(magic, SYNC):
	case bytes.Equal(magic, RAW):
	case bytes.Equal(magic, SVS):
	case bytes.Equal(magic, Y800):
		skip += 4
	case bytes.Equal(magic, Y16B):
		skip += 4
	case bytes.Equal(magic, Y16L):
		skip += 4
	case bytes.Equal(magic, I420):
		skip += 4
	case bytes.Equal(magic, YUY2):
		skip += 4
	case bytes.Equal(magic, RGB):
		skip += 4
	case bytes.Equal(magic, JPEG):
		skip += 4
	case bytes.Equal(magic, PNG):
		skip += 4
	case bytes.Equal(magic, H264):
		skip += 4
	case bytes.Equal(magic, TIFF):
		skip += 4
	}
	return int64(skip)
}
