package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/midbel/cli"
	"github.com/midbel/linewriter"
)

var checkCommand = &cli.Command{
	Usage: "check-upi [-b] [-d] [-s] [-e] [-u] [-i] [-c] [-g] [-k] <archive,...>",
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
  -c         print the results as csv
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
	csv := cmd.Flag.Bool("c", false, "csv")
	toGPS := cmd.Flag.Bool("g", false, "convert time to GPS")
	keep := cmd.Flag.Bool("k", false, "keep invalid files")

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
	case "source":
		byf = bySource
	default:
		return fmt.Errorf("unsupported %s", *by)
	}
	if rs := checkFiles(walkFiles(paths, *upi, 1), *interval, *keep, byf); len(rs) > 0 {
		reportCheckResults(rs, *csv, *toGPS)
	}
	return nil
}

func reportCheckResults(gs []*Gap, csv, gps bool) {
	line := Line(csv)
	for i := 0; i < len(gs); i++ {
		g := gs[i]

		line.AppendString(Transform(g.UPI), 16, linewriter.AlignRight)
		if gps {
			line.AppendUint(timeToGPS(g.Starts), 10, linewriter.AlignRight)
			line.AppendUint(timeToGPS(g.Ends), 10, linewriter.AlignRight)
		} else {
			line.AppendTime(g.Starts, time.RFC3339, linewriter.AlignRight)
			line.AppendTime(g.Ends, time.RFC3339, linewriter.AlignRight)
		}
		if elapsed := g.Duration(); csv {
			line.AppendUint(uint64(elapsed.Seconds()), 10, linewriter.AlignRight)
		} else {
			line.AppendDuration(elapsed, 10, linewriter.AlignRight)
		}
		line.AppendUint(uint64(g.Before), 10, linewriter.AlignRight)
		line.AppendUint(uint64(g.After), 10, linewriter.AlignRight)
		line.AppendUint(uint64(g.Count()), 10, linewriter.AlignRight)

		io.Copy(os.Stdout, line)
	}
}

func checkFiles(files <-chan *File, interval time.Duration, keep bool, by ByFunc) []*Gap {
	rs := make(map[string][]*Gap)
	cs := make(map[string]*File)
	qs := make(map[string][]*Range)
	for f := range files {
		if !f.Valid() && !keep {
			continue
		}
		n := by(f)
		if s, ok := inRanges(qs[n], f.Sequence); !ok {
			qs[n] = s
		} else {
			cs[n] = f
			continue
		}

		var skip bool
		if gs, ok := rs[n]; ok && len(gs) > 0 {
			ix := sort.Search(len(gs), func(i int) bool {
				return gs[i].After >= f.Sequence
			})
			if ix < len(gs) {
				if d := f.Sequence - gs[ix].Before; d <= 1 {
					gs[ix].Before, gs[ix].Starts = f.Sequence, f.AcqTime
					if gs[ix].Count() == 0 {
						rs[n] = append(gs[:ix], gs[ix+1:]...)
					}
					skip = true
				} else {
					if gs[ix].Before < f.Sequence {
						g := *gs[ix]
						g.Before, g.Starts = f.Sequence, f.AcqTime
						gs[ix].After, gs[ix].Ends = f.Sequence, f.AcqTime
						rs[n] = append(gs[:ix+1], append([]*Gap{&g}, gs[ix+1:]...)...)
						skip = true
					}
				}
			}
		}
		if p, ok := cs[n]; ok && f.Sequence > p.Sequence {
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

func timeToGPS(t time.Time) uint64 {
	left := t.Sub(UNIX).Seconds()
	right := GPS.Sub(UNIX).Seconds()

	return uint64(left - right)
}
