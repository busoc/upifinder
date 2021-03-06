package main

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/midbel/cli"
	"github.com/midbel/linewriter"
	"github.com/midbel/xxh"
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

type Digest struct {
	File     string
	Magic    [4]byte
	Sum      []byte
	Time     uint64
	Sequence uint32
}

var digestCommand = &cli.Command{
	Usage: "digest [-c] <datadir>",
	Alias: []string{"sum", "cksum"},
	Short: "compute the md5 checksum of all files under the given directory",
	Run:   runDigest,
}

func runDigest(cmd *cli.Command, args []string) error {
	csv := cmd.Flag.Bool("c", false, "csv")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	line := Line(*csv)
	for d := range retrPaths(cmd.Flag.Arg(0)) {
		w := GPS.Add(time.Duration(d.Time))

		line.AppendBytes(bytes.Trim(d.Magic[:], "\x00"), 4, linewriter.Text)
		line.AppendUint(uint64(d.Sequence), 8, linewriter.AlignRight)
		line.AppendTime(w, time.RFC3339, 0)
		line.AppendBytes(d.Sum, 0, linewriter.Hex)
		line.AppendString(d.File, 0, linewriter.AlignLeft)

		io.Copy(os.Stdout, line)
	}
	return nil
}

func digestReader(r io.Reader) (*Digest, error) {
	var d Digest
	if _, err := r.Read(d.Magic[:]); err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	if _, err := io.CopyN(&buffer, r, skipBytes(d.Magic[:])); err != nil {
		return nil, err
	}
	digest := xxh.New64(0)
	if _, err := io.Copy(digest, r); err != nil {
		return nil, err
	}
	d.Sum = digest.Sum(nil)

	binary.Read(&buffer, binary.BigEndian, &d.Sequence)
	binary.Read(&buffer, binary.BigEndian, &d.Time)

	return &d, nil
}

func retrPaths(base string) <-chan *Digest {
	q := make(chan *Digest)
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
			switch e := filepath.Ext(p); e {
			case ".xml":
			case ".tar":
				r, err := os.Open(p)
				if err != nil {
					return err
				}
				defer r.Close()

				tr := tar.NewReader(r)
				for {
					h, err := tr.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
					if filepath.Ext(h.Name) == ".xml" {
						continue
					}
					d, err := digestReader(io.LimitReader(tr, h.Size))
					if err != nil {
						return err
					}
					d.File = filepath.Base(h.Name)
					q <- d
				}
			default:
				r, err := os.Open(p)
				if err != nil {
					return err
				}
				defer r.Close()
				d, err := digestReader(r)
				if err != nil {
					return err
				}
				d.File = filepath.Base(p)
				q <- d
			}
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
