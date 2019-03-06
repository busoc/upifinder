package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/midbel/cli"
)

var checkCommand = &cli.Command{
	Usage: "check-upi [-b] [-d] [-s] [-e] [-u] [-i] [-f] [-g] [-k] <archive,...>",
	Alias: []string{"check"},
	Short: "provide the number of missing files in the archive by UPI",
	Run:   runCheck,
	Desc: `"check-upi" (check) traverse the Hadock archive to find gap(s) of files by UPI.

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

  -b BY      check gaps by upi or by source (default by upi)
  -u UPI     only count files for the given UPI
  -s START   only count files created after START
  -e END     only count files created before END
  -d DAYS    only count files created during a period of DAYS
  -i TIME    only consider gap with at least TIME duration
  -f FORMAT  print the results in the given format ("", csv, column)
  -a         keep all gaps even when a later playback/replay refill those
  -k         keep invalid files in the count of gaps
  -g         print the ACQTIME as seconds elapsed since GPS epoch`,
}

func runCheck(cmd *cli.Command, args []string) error {
	var start, end When
	cmd.Flag.Var(&start, "s", "start")
	cmd.Flag.Var(&end, "e", "end")
	by := cmd.Flag.String("b", "", "by")
	upi := cmd.Flag.String("u", "", "upi")
	period := cmd.Flag.Int("d", 0, "period")
	interval := cmd.Flag.Duration("i", 0, "interval")
	format := cmd.Flag.String("f", "", "format")
	toGPS := cmd.Flag.Bool("g", false, "convert time to GPS")
	keep := cmd.Flag.Bool("k", false, "keep invalid files")
	all := cmd.Flag.Bool("a", false, "keep all gaps even when refilled")

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
	var byf ByFunc
	switch strings.ToLower(*by) {
	case "upi", "":
		byf = byUPI
	case "source", "src":
		byf = bySource
	default:
		return fmt.Errorf("unsupported %s", *by)
	}
	rs := checkFiles(walkFiles(paths, *upi, 1), *interval, *keep, *all, byf)
	return reportCheckResults(rs, *format, *toGPS)
}

func checkFiles(files <-chan *File, interval time.Duration, keep, all bool, by ByFunc) []*Gap {
	rs := make(map[string][]*Gap)
	cs := make(map[string]*File)
	for f := range files {
		if !f.Valid() && !keep {
			continue
		}
		n := by(f)
		if p, ok := cs[n]; ok && f.Sequence > p.Sequence {
			var skip bool
			if !all {
				if gs, ok := rs[n]; ok && len(gs) > 0 {
					ix := sort.Search(len(gs), func(i int) bool {
						return gs[i].After >= f.Sequence
					})
					if ix < len(gs) && f.Sequence-gs[ix].Before == 1 {
						gs[ix].Before = f.Sequence
						if d := gs[ix].After - gs[ix].Before; d == 1 {
							if ix == len(gs)-1 {
								rs[n] = gs[:ix]
							} else {
								rs[n] = append(gs[:ix], gs[ix+1:]...)
							}
						}
						skip = true
					}
				}
			}
			if !skip {
				g := f.Compare(p)
				if (g != nil && g.Count() > 0) && (interval == 0 || g.Duration() >= interval) {
					rs[n] = append(rs[n], g)
				}
			}
		}
		cs[n] = f
	}
	var gs []*Gap
	for _, vs := range rs {
		gs = append(gs, vs...)
	}
	return gs
}

//
// func checkFiles(files <-chan *File, interval time.Duration, keep bool, by ByFunc) []*Gap {
// 	rs := make([]*Gap, 0, 1000)
// 	cs := make(map[string]*File)
// 	for f := range files {
// 		if !f.Valid() && !keep {
// 			continue
// 		}
// 		n := by(f)
// 		if p, ok := cs[n]; ok && f.Sequence > p.Sequence {
// 			g := f.Compare(p)
// 			if (g != nil && g.Count() > 0) && (interval == 0 || g.Duration() >= interval) {
// 				rs = append(rs, g)
// 			}
// 		}
// 		cs[n] = f
// 	}
// 	return rs
// }

func reportCheckResults(rs []*Gap, format string, toGPS bool) error {
	if len(rs) == 0 {
		return nil
	}
	switch f := strings.ToLower(format); f {
	case "column", "":
		count, delta := printCheckColumns(os.Stdout, rs, toGPS)

		log.Println()
		log.Printf("%d missing files (%s)", count, delta)
	case "csv":
		return printCheckValues(os.Stdout, rs, toGPS)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
	return nil
}

func printCheckValues(ws io.Writer, rs []*Gap, gps bool) error {
	w := csv.NewWriter(ws)
	defer w.Flush()
	for _, g := range rs {
		var starts, ends string
		if gps {
			starts, ends = timeToGPS(g.Starts), timeToGPS(g.Ends)
		} else {
			starts, ends = g.Starts.Format(time.RFC3339), g.Ends.Format(time.RFC3339)
		}
		row := []string{
			Transform(g.UPI),
			starts,
			ends,
			g.Duration().String(),
			strconv.FormatUint(uint64(g.Before), 10),
			strconv.FormatUint(uint64(g.After), 10),
			strconv.FormatUint(uint64(g.Count()), 10),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func printCheckColumns(ws io.Writer, rs []*Gap, gps bool) (uint64, time.Duration) {
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

		var starts, ends string
		if gps {
			starts, ends = timeToGPS(g.Starts), timeToGPS(g.Ends)
		} else {
			starts, ends = g.Starts.Format(time.RFC3339), g.Ends.Format(time.RFC3339)
		}
		logger.Printf("%-s\t%s\t%s\t%s\t%d\t%d\t%d", Transform(g.UPI), starts, ends, delta, g.Before, g.After, count)
	}
	return total, elapsed
}

func timeToGPS(t time.Time) string {
	left := t.Sub(UNIX).Seconds()
	right := GPS.Sub(UNIX).Seconds()

	return strconv.FormatFloat(left-right, 'f', 0, 64)
}
