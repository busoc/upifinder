package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/midbel/cli"
)

var checkCommand = &cli.Command{
	Usage: "check [-a] [-d] [-s] [-e] [-u] [-i] [-f] <archive,...>",
	Short: "provide the number of missing files in the archive",
	Run:   runCheck,
	Desc: `"check" traverse the Hadock archive to find missing files.

If no UPI is given, "check" will collect the list of missing files for each UPI
found into the Hadock archive in the given period.

The period of time is selected by upifinder with the following rules (depending
of the value given to the command line):

* [s] + [e] : walk from START to END date
* [s] + [d] : walk from START to START + DAYS date
* [e] + [d] : walk from END - DAYS to END date
* [d]       : walk from TODAY - DAYS to TODAY
* default   : walk recursively on the given path(s)

Options:

-u UPI     only count files for the given UPI
-s START   only count files created after START
-e END     only count files created before END
-d DAYS    only count files created during a period of DAYS
-i TIME    only consider gap with at least TIME duration
-f FORMAT  print the results in the given format ("", csv, column, summary, json)
-a         print the ACQTIME instead of the VMU time`,
}

type Gap struct {
	UPI    string    `json:"upi" xml:"upi"`
	Before uint32    `json:"last" xml:"last"`
	After  uint32    `json:"first" xml:"first"`
	Starts time.Time `json:"dtstart" xml:"dtstart"`
	Ends   time.Time `json:"dtend" xml:"dtend"`
}

func (g *Gap) Count() uint32 {
	return g.After - g.Before
}

func (g *Gap) Duration() time.Duration {
	return g.Ends.Sub(g.Starts)
}

func runCheck(cmd *cli.Command, args []string) error {
	var start, end When
	cmd.Flag.Var(&start, "s", "start")
	cmd.Flag.Var(&end, "e", "end")
	acqtime := cmd.Flag.Bool("a", false, "acquisition time")
	upi := cmd.Flag.String("u", "", "upi")
	period := cmd.Flag.Int("d", 0, "period")
	interval := cmd.Flag.Duration("i", 0, "interval")
	format := cmd.Flag.String("f", "", "format")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}

	if cmd.Flag.NArg() == 0 {
		cmd.Help()
	}

	paths, err := listPaths(cmd.Flag.Args(), *period, start.Time, end.Time)
	if err != nil {
		return err
	}
	rs := checkFiles(walkFiles(paths, *upi, 1, *acqtime), *interval)
	if len(rs) == 0 {
		return nil
	}
	switch f := strings.ToLower(*format); f {
	case "", "column":
		count, delta := printCheckResults(os.Stdout, rs)

		log.Println()
		log.Printf("%d missing files (%s)", count, delta)
	case "summary":
		count, delta := printCheckResults(ioutil.Discard, rs)
		log.Printf("%d missing files (%s)", count, delta)
	case "csv":
		w := csv.NewWriter(os.Stdout)
		defer w.Flush()
		for _, g := range rs {
			row := []string{
				g.UPI,
				g.Starts.Format(time.RFC3339),
				g.Ends.Format(time.RFC3339),
				g.Duration().String(),
				strconv.FormatUint(uint64(g.Before), 10),
				strconv.FormatUint(uint64(g.After), 10),
				strconv.FormatUint(uint64(g.Count()), 10),
			}
			if err := w.Write(row); err != nil {
				return err
			}
		}
	case "json":
		c := struct {
			When    time.Time         `json:"dtstamp"`
			Paths   []string          `json:"dirs"`
			Count   int               `json:"count"`
			Gaps    map[string][]*Gap `json:"gaps"`
			Missing uint64            `json:"missing"`
			Elapsed time.Duration     `json:"duration"`
		}{
			When:  time.Now(),
			Paths: paths,
			Count: len(rs),
			Gaps:  make(map[string][]*Gap),
		}
		for _, g := range rs {
			c.Gaps[g.UPI] = append(c.Gaps[g.UPI], g)
			c.Elapsed += g.Duration()
			c.Missing += uint64(g.Count())
		}
		return json.NewEncoder(os.Stdout).Encode(c)
	// case "xml":
	default:
		return fmt.Errorf("unsupported format: %s", *format)
	}
	return nil
}

func checkFiles(files <-chan *File, interval time.Duration) []*Gap {
	rs := make([]*Gap, 0, 1000)
	cs := make(map[string]*File)
	for f := range files {
		n := f.String()
		if p, ok := cs[n]; ok && f.Sequence > p.Sequence+1 {
			g := Gap{
				UPI:    n,
				Starts: p.AcqTime,
				Ends:   f.AcqTime,
				Before: p.Sequence,
				After:  f.Sequence,
			}
			if interval == 0 || g.Duration() >= interval {
				rs = append(rs, &g)
			}
		}
		cs[n] = f
	}
	return rs
}

func printCheckResults(ws io.Writer, rs []*Gap) (uint64, time.Duration) {
	w := tabwriter.NewWriter(ws, 16, 2, 4, ' ', 0)
	defer w.Flush()

	logger := log.New(w, "", 0)
	logger.Println("UPI\tStarts\tEnds\tDuration\tBefore\tAfter")

	var (
		total   uint64
		elapsed time.Duration
	)
	for _, g := range rs {
		delta := g.Duration()
		count := g.Count()

		elapsed += delta
		total += uint64(count)
		logger.Printf("%-s\t%s\t%s\t%s\t%d\t%d\t%d", g.UPI, g.Starts, g.Ends, delta, g.Before, g.After, count)
	}
	return total, elapsed
}
