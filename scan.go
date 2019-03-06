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
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

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
			if n := i.Name(); upi != "" && strings.Index(n, upi) < 0 {
				return nil
			}
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
