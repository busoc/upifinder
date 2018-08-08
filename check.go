package main

import (
	"encoding/csv"
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

var checkSourceCommand = &cli.Command{
	Usage: "check-src [-a] [-d] [-s] [-e] [-c] [-i] [-f] [-g] <archive,...>",
	Short: "provide the number of missing files in the archive by sources",
	Run:   runCheckSource,
}

var checkUPICommand = &cli.Command{
	Usage: "check [-a] [-d] [-s] [-e] [-u] [-i] [-f] [-g] <archive,...>",
	Short: "provide the number of missing files in the archive by UPI",
	Run:   runCheckUPI,
	Desc: `"check" traverse the Hadock archive to find missing files by UPI.

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
  -f FORMAT  print the results in the given format ("", csv, column)
  -a         print the ACQTIME instead of the VMU time
  -g         print the ACQTIME as seconds elapsed since GPS epoch (-a should be set)`,
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

func runCheckSource(cmd *cli.Command, args []string) error {
	var start, end When
	cmd.Flag.Var(&start, "s", "start")
	cmd.Flag.Var(&end, "e", "end")
	acqtime := cmd.Flag.Bool("a", false, "acquisition time")
	period := cmd.Flag.Int("d", 0, "period")
	interval := cmd.Flag.Duration("i", 0, "interval")
	format := cmd.Flag.String("f", "", "format")
	toGPS := cmd.Flag.Bool("g", false, "convert time to GPS")
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
	rs := checkFilesBySources(walkFiles(paths, "", 1, *acqtime), *interval)
	return reportCheckResults(rs, *format, *toGPS, *acqtime)
}

func runCheckUPI(cmd *cli.Command, args []string) error {
	var start, end When
	cmd.Flag.Var(&start, "s", "start")
	cmd.Flag.Var(&end, "e", "end")
	acqtime := cmd.Flag.Bool("a", false, "acquisition time")
	upi := cmd.Flag.String("u", "", "upi")
	period := cmd.Flag.Int("d", 0, "period")
	interval := cmd.Flag.Duration("i", 0, "interval")
	format := cmd.Flag.String("f", "", "format")
	toGPS := cmd.Flag.Bool("g", false, "convert time to GPS")
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
	rs := checkFilesByUPI(walkFiles(paths, *upi, 1, *acqtime), *interval)
	return reportCheckResults(rs, *format, *toGPS, *acqtime)
}

func reportCheckResults(rs []*Gap, format string, toGPS, acqtime bool) error {
	if len(rs) == 0 {
		return nil
	}
	switch f := strings.ToLower(format); f {
	case "column":
		count, delta := printCheckResults(os.Stdout, rs, toGPS && acqtime)

		log.Println()
		log.Printf("%d missing files (%s)", count, delta)
	case "":
		count, delta := printCheckResults(ioutil.Discard, rs, toGPS)
		log.Printf("%d missing files (%s)", count, delta)
	case "csv":
		w := csv.NewWriter(os.Stdout)
		defer w.Flush()
		for _, g := range rs {
			var starts, ends string
			if toGPS && acqtime {
				starts, ends = timeToGPS(g.Starts), timeToGPS(g.Ends)
			} else {
				starts, ends = g.Starts.Format(time.RFC3339), g.Ends.Format(time.RFC3339)
			}
			row := []string{
				g.UPI,
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
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
	return nil
}

func checkFilesBySources(files <-chan *File, interval time.Duration) []*Gap {
	rs := make([]*Gap, 0, 1000)
	cs := make(map[string]*File)
	for f := range files {
		if p, ok := cs[f.Source]; ok && f.Sequence > p.Sequence+1 {
			g := Gap{
				UPI:    f.Info,
				Starts: p.AcqTime,
				Ends:   f.AcqTime,
				Before: p.Sequence,
				After:  f.Sequence,
			}
			if interval == 0 || g.Duration() >= interval {
				rs = append(rs, &g)
			}
		}
		cs[f.Source] = f
	}
	return rs
}

func checkFilesByUPI(files <-chan *File, interval time.Duration) []*Gap {
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

func printCheckResults(ws io.Writer, rs []*Gap, gps bool) (uint64, time.Duration) {
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
		logger.Printf("%-s\t%s\t%s\t%s\t%d\t%d\t%d", g.UPI, starts, ends, delta, g.Before, g.After, count)
	}
	return total, elapsed
}

func timeToGPS(t time.Time) string {
	left := t.Sub(UNIX).Seconds()
	right := GPS.Sub(UNIX).Seconds()

	return strconv.FormatFloat(left-right, 'f', 0, 64)
}
