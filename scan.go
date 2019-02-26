package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type ByFunc func(*File) string

func byUPI(f *File) string {
	return f.String()
}

func bySource(f *File) string {
	return f.Source
}

type File struct {
	Path     string    `json:"path" xml:"path"`
	Source   string    `json:"source" xml:"source"`
	Info     string    `json:"upi" xml:"upi"`
	Size     int64     `json:"size" xml:"size"`
	Sequence uint32    `json:"sequence" xml:"sequence"`
	AcqTime  time.Time `json:"dtstamp" xml:"dtstamp"`
}

func (f *File) Compare(p *File) *Gap {
	if p == nil || f.Sequence == p.Sequence+1 {
		return nil
	}
	if p.AcqTime.After(f.AcqTime) {
		return p.Compare(f)
	}
	g := Gap{
		UPI:    p.String(),
		Starts: p.AcqTime,
		Ends:   f.AcqTime,
		Before: p.Sequence,
		After:  f.Sequence,
	}
	return &g
}

func (f *File) Name() string {
	ps := strings.Split(filepath.Base(f.Path), "_")
	return strings.Join(ps[:len(ps)-3], "_")
}

func (f *File) Valid() bool {
	return filepath.Ext(f.Path) != ".bad"
}

func (f *File) String() string {
	return fmt.Sprintf("%s/%s", f.Source, f.Info)
}

func listPaths(paths []string, period int, dtstart, dtend time.Time) ([]string, error) {
	if period > 0 && !dtstart.IsZero() && !dtend.IsZero() {
		return nil, fmt.Errorf("period can't be set if start and end dates are provided")
	}
	switch {
	default:
		return paths, nil
	case !dtstart.IsZero() && !dtend.IsZero():
	case period > 0 && !dtstart.IsZero() && dtend.IsZero():
		dtend = dtstart.Add(Day * time.Duration(period))
	case period > 0 && dtstart.IsZero() && !dtend.IsZero():
		dtstart = dtend.Add(Day * time.Duration(-period))
	case period > 0 && dtstart.IsZero() && dtend.IsZero():
		dtend = time.Now()
		dtstart = dtend.Add(Day * time.Duration(-period))
	}
	ps := make([]string, 0, len(paths)*DefaultPeriod)
	for dtstart.Before(dtend) {
		y, d := fmt.Sprintf("%04d", dtstart.Year()), fmt.Sprintf("%03d", dtstart.YearDay())
		for _, p := range paths {
			ps = append(ps, filepath.Join(p, y, d))
		}
		dtstart = dtstart.Add(Day)
	}
	return ps, nil
}

func walkFiles(paths []string, upi string, max int) <-chan *File {
	q := make(chan *File)
	go func() {
		defer close(q)

		var group errgroup.Group

		sema := make(chan struct{}, max)
		for _, a := range paths {
			dir := a
			sema <- struct{}{}
			group.Go(func() error {
				err := findFiles(dir, upi, q)
				<-sema
				return err
			})
		}
		group.Wait()
	}()
	return q
}

func findFiles(dir, upi string, queue chan<- *File) error {
	return filepath.Walk(dir, func(p string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() {
			return nil
		}
		if n := i.Name(); upi != "" && strings.Index(n, upi) < 0 {
			return nil
		}
		switch e := filepath.Ext(p); e {
		case ".xml":
			// ignore xml files
		case ".zip":
		case ".tar":
			fs, err := scanTar(p, upi)
			if err != nil {
				return err
			}
			for f := range fs {
				queue <- f
			}
		case ".lst":
			r, err := os.Open(p)
			if err != nil {
				return err
			}
			defer r.Close()

			s := bufio.NewScanner(r)
			s.Split(bufio.ScanLines)
			for i := 0; s.Scan(); i++ {
				p := s.Text()
				if len(p) == 0 || filepath.Ext(p) == ".xml" {
					continue
				}
				f, err := parseFilename(p, upi, 0)
				if err != nil {
					continue
				}
				if f != nil {
					queue <- f
				}
			}
			return s.Err()
		default:
			f, err := parseFilename(p, upi, i.Size())
			if err != nil {
				return err
			}
			if f != nil {
				queue <- f
			}
		}
		return nil
	})
}

func scanZip(p, upi string) (<-chan *File, error) {
	rc, err := zip.OpenReader(p)
	if err != nil {
		return nil, err
	}
	q := make(chan *File)
	go func() {
		defer func() {
			rc.Close()
			close(q)
		}()
		for _, f := range rc.File {
			if filepath.Ext(f.Name) == ".xml" {
				continue
			}
			f, err := parseFilename(f.Name, upi, int64(f.UncompressedSize64))
			if err != nil {
				break
			}
			if f != nil {
				q <- f
			}
		}
	}()
	return q, nil
}

func scanTar(p, upi string) (<-chan *File, error) {
	r, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	q := make(chan *File)
	go func() {
		defer func() {
			r.Close()
			close(q)
		}()
		t := tar.NewReader(r)
		for {
			h, err := t.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			if filepath.Ext(h.Name) == ".xml" {
				continue
			}
			f, err := parseFilename(h.Name, upi, h.Size)
			if err != nil {
				break
			}
			if f != nil {
				q <- f
			}
			if _, err := io.CopyN(ioutil.Discard, t, h.Size); err != nil {
				break
			}
		}
	}()
	return q, nil
}

func parseFilename(p, upi string, i int64) (*File, error) {
	// if !utf8.ValidString(p) {
	// 	return nil, nil
	// }
	if !Keep(filepath.Base(p)) {
		return nil, nil
	}
	ps := strings.Split(filepath.Base(p), "_")

	f := File{
		Path:   p,
		Source: strings.TrimLeft(ps[0], "0"),
		Size:   i,
	}
	if s, err := strconv.ParseInt(f.Source, 16, 8); err != nil {
		return nil, err
	} else {
		var origins []int
		switch ps[len(ps)-5] {
		case "1", "2":
			origins = OriImages
		case "3":
			origins = OriSciences
		default:
		}
		if !acceptOrigin(int(s), origins) {
			return nil, nil
		}
	}
	if len(upi) == 0 {
		f.Info = strings.Join(ps[1:len(ps)-5], "_")
	} else {
		f.Info = upi
	}
	if n, err := strconv.ParseUint(ps[len(ps)-4], 10, 32); err == nil {
		f.Sequence = uint32(n)
	} else {
		return nil, err
	}

	if t, err := time.Parse("20060102150405", ps[len(ps)-3]+ps[len(ps)-2]); err == nil {
		// var delta time.Duration
		// if !acqtime {
		// 	ps := strings.SplitN(ps[len(ps)-1], ".", 2)
		// 	d, _ := strconv.ParseInt(strings.TrimLeft(ps[0], "0"), 10, 64)
		// 	delta = time.Duration(d) * time.Minute
		// }
		// f.AcqTime = t.Add(delta)
		f.AcqTime = t
	} else {
		return nil, err
	}
	return &f, nil
}

var (
	OriImages   = []int{0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47}
	OriSciences = []int{0x39, 0x40, 0x41, 0x51}
)

func acceptOrigin(o int, origins []int) bool {
	if len(origins) == 0 {
		return false
	}
	ix := sort.SearchInts(origins, o)
	return ix < len(origins) && origins[ix] == o
}
