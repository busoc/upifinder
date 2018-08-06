package main

import (
	"bytes"
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
	var group errgroup.Group
	group.Go(func() error {
		//count files and post reports
		rs := countFiles(walkFiles(paths, "", 8, false), false)
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
		remote := *u
		remote.Path = path.Join(remote.Path, "files") + "/"
		return pushData(remote.String(), c)
	})
	group.Go(func() error {
		//report gaps
		rs := checkFiles(walkFiles(paths, "", 1, true), 0)
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
		remote := *u
		remote.Path = path.Join(remote.Path, "status") + "/"
		return pushData(remote.String(), c)
	})
	return group.Wait()
}

func pushData(remote string, data interface{}) error {
	var w bytes.Buffer
	if err := json.NewEncoder(&w).Encode(data); err != nil {
		return err
	}
	rs, err := http.Post(remote, "application/json", &w)
	if err != nil {
		return err
	}
	defer rs.Body.Close()
	if rs.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf(http.StatusText(rs.StatusCode))
	}
	return nil
}
