package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/midbel/cli"
	"golang.org/x/sync/errgroup"
)

var pushCommand = &cli.Command{
	Usage: "push [-d] <remote> <archive...>",
	Short: "push report(s) to remote server",
	Run:   runPush,
}

func runPush(cmd *cli.Command, args []string) error {
	period := cmd.Flag.Int("d", 0, "period")
	chunk := cmd.Flag.Int("c", 0, "send by chunk")
	parallel := cmd.Flag.Bool("p", false, "run in parallel")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	var s, e time.Time
	remote, rest := cmd.Flag.Arg(0), cmd.Flag.Args()
	u, err := url.Parse(remote)
	if err != nil {
		return err
	}
	paths, err := listPaths(rest[1:], *period, s, e)
	if err != nil {
		return err
	}
	if !*parallel {
		if err := pushCount(paths, *u, *chunk); err != nil {
			return err
		}
		if err := pushCheck(paths, *u, *chunk); err != nil {
			return err
		}
		return nil
	} else {
		var group errgroup.Group
		group.Go(func() error {
			return pushCount(paths, *u, *chunk)
		})
		group.Go(func() error {
			return pushCheck(paths, *u, *chunk)
		})
		return group.Wait()
	}
}

func pushCheck(paths []string, u url.URL, chunk int) error {
	//report gaps
	rs := checkFiles(walkFiles(paths, "", 1), 0, false, byUPI)
	if len(rs) == 0 {
		return nil
	}
	c := struct {
		When time.Time `json:"dtstamp"`
		Data []*Gap    `json:"report"`
	}{
		When: time.Now(),
		Data: rs,
	}
	u.Path = path.Join(u.Path, statusReport) + "/"

	var err error
	if chunk > 0 {
		for i := 0; i < len(c.Data); i += chunk {
			offset := chunk
			if n := len(c.Data[i:]); n < offset {
				offset = n
			}
			cs := struct {
				When time.Time `json:"dtstamp"`
				Data []*Gap    `json:"report"`
			}{
				When: c.When,
				Data: c.Data[i : i+offset],
			}
			if err = pushData(u.String(), cs); err != nil {
				break
			}
		}
	} else {
		err = pushData(u.String(), c)
	}
	return err
}

func pushCount(paths []string, u url.URL, chunk int) error {
	//count files and post reports
	rs := countFiles(walkFiles(paths, "", 8))
	if len(rs) == 0 {
		return nil
	}
	c := struct {
		When time.Time        `json:"dtstamp"`
		Data map[string]*Coze `json:"report"`
	}{
		When: time.Now(),
		Data: rs,
	}
	u.Path = path.Join(u.Path, filesReport) + "/"

	var err error
	if chunk > 0 {
		keys := make([]string, len(c.Data))
		var i int
		for n := range c.Data {
			keys[i] = n
			i++
		}
		for i := 0; i < len(keys); i += chunk {
			offset := chunk
			if n := len(keys[i:]); n < chunk {
				offset = n
			}
			cs := struct {
				When time.Time        `json:"dtstamp"`
				Data map[string]*Coze `json:"report"`
			}{
				When: c.When,
				Data: make(map[string]*Coze),
			}
			for j := i; j < i+offset; j++ {
				cs.Data[keys[j]] = c.Data[keys[j]]
			}
			if err = pushData(u.String(), cs); err != nil {
				break
			}
		}
	} else {
		err = pushData(u.String(), c)
	}
	return err
}

func pushData(remote string, data interface{}) error {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPost, remote, &buf)
	if err != nil {
		return err
	}
	r.Header.Set("content-encoding", "gzip")
	r.Header.Set("content-type", "application/json")
	// rs, err := http.Post(remote, "application/json", &w)
	// if err != nil {
	// 	return err
	// }
	rs, err := http.DefaultClient.Do(r)
	defer rs.Body.Close()
	if rs.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf(http.StatusText(rs.StatusCode))
	}
	return nil
}
