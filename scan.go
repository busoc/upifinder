package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type File struct {
	Path     string    `json:"-" xml:"-"`
	Source   string    `json:"source" xml:"source"`
	Info     string    `json:"upi" xml:"upi"`
	Size     int64     `json:"size" xml:"size"`
	Sequence uint32    `json:"sequence" xml:"sequence"`
	AcqTime  time.Time `json:"dtstamp" xml:"dtstamp"`
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

func walkFiles(paths []string, upi string, max int, acqtime bool) <-chan *File {
	q := make(chan *File)
	go func() {
		defer close(q)

		var group errgroup.Group

		sema := make(chan struct{}, max)
		for _, a := range paths {
			dir := a
			sema <- struct{}{}
			group.Go(func() error {
				err := findFiles(dir, upi, acqtime, q)
				<-sema
				return err
			})
		}
		group.Wait()
	}()
	return q
}

func findFiles(dir, upi string, acqtime bool, queue chan<- *File) error {
	return filepath.Walk(dir, func(p string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() {
			return nil
		}
		if n := i.Name(); strings.HasSuffix(n, "xml") || (upi != "" && strings.Index(n, upi) < 0) {
			return nil
		}
		ps := strings.Split(filepath.Base(p), "_")

		f := File{
			Path:   p,
			Source: strings.TrimLeft(ps[0], "0"),
			Size:   i.Size(),
		}
		if len(upi) == 0 {
			f.Info = strings.Join(ps[1:len(ps)-5], "_")
		} else {
			f.Info = upi
		}
		if n, err := strconv.ParseUint(ps[len(ps)-4], 10, 32); err == nil {
			f.Sequence = uint32(n)
		} else {
			return err
		}

		if t, err := time.Parse("20060102150405", ps[len(ps)-3]+ps[len(ps)-2]); err == nil {
			var delta time.Duration
			if !acqtime {
				ps := strings.SplitN(ps[len(ps)-1], ".", 2)
				d, _ := strconv.ParseInt(strings.TrimLeft(ps[0], "0"), 10, 64)
				delta = time.Duration(d) * time.Minute
			}
			f.AcqTime = t.Add(delta)
		} else {
			return err
		}
		queue <- &f
		return nil
	})
}
