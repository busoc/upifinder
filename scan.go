package main

import (
	"archive/tar"
	"archive/zip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
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
		default:
			f, err := parseFilename(p, upi, i.Size())
			if err != nil {
				return err
			}
			queue <- f
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
			f, err := parseFilename(f.Name, upi, int64(f.UncompressedSize64))
			if err != nil {
				break
			}
			q <- f
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
			f, err := parseFilename(h.Name, upi, h.Size)
			if err != nil {
				break
			}
			q <- f
			if _, err := io.CopyN(ioutil.Discard, t, h.Size); err != nil {
				break
			}
		}
	}()
	return q, nil
}

func parseFilename(p, upi string, i int64) (*File, error) {
	ps := strings.Split(filepath.Base(p), "_")

	f := File{
		Path:   p,
		Source: strings.TrimLeft(ps[0], "0"),
		Size:   i,
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
