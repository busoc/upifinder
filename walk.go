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

var walkCommand = &cli.Command{
	Usage: "walk [-d] [-s] [-e] [-m] [-u] [-f] <archive,...>",
	Short: "provide the number of files available in the archive",
	Run:   runWalk,
	Desc: `"walk" traverse the Hadock archive and count the number of files
created during one run per sources.

If no UPI is given, "walk" will collect the count of files for each UPI found
into the Hadock archive in the given period.

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
  -f FORMAT  print the results in the given format ("", csv, column)
  -m         merge all count files of UPI

Examples:

count files for all UPI on the last seven days for all sources:
$ upifinder -d 7 /data/images/playback/*

count files between two dates for a specific UPI:
$ upifinder -u XYZ -s 2018-06-04 -e 2018-06-11 /data/images/realtime/*

count files at a date for a specific UPI:
$ upifinder -u XYZ -s 2018-06-04 -e 2018-06-11 /data/images/realtime/38/2018/175/10

count files between two dates for a specific UPI and a specific source:
$ upifinder -u XYZ -s 2018-06-04 -e 2018-06-11 /data/images/playback/38

Developed with %s by GC`,
}

type When struct {
	time.Time
}

func (w *When) Set(v string) error {
	t, err := time.Parse(TimeFormat, v)
	if err == nil {
		w.Time = t
	}
	return err
}

func (w *When) String() string {
	if !w.IsZero() {
		return w.Format(TimeFormat)
	}
	return time.Now().Format(TimeFormat)
}

type Coze struct {
	Count   uint64 `json:"total" xml:"total"`
	Size    uint64 `json:"size" xml:"size"`
	Invalid uint64 `json:"invalid" xml:"invalid"`
	Uniq    uint64 `json:"uniq" xml:"uniq"`
}

func (c Coze) Corrupted() float64 {
	if c.Count == 0 || c.Invalid == 0 {
		return 0
	}
	return 100 * (float64(c.Invalid) / float64(c.Count))
}

func runWalk(cmd *cli.Command, args []string) error {
	cmd.Desc = fmt.Sprintf(cmd.Desc, "\u2764")

	var start, end When
	cmd.Flag.Var(&start, "s", "start")
	cmd.Flag.Var(&end, "e", "end")
	upi := cmd.Flag.String("u", "", "upi")
	period := cmd.Flag.Int("d", 0, "period")
	merge := cmd.Flag.Bool("m", false, "merge")
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

	rs := countFiles(walkFiles(paths, *upi, 8, false), *merge)
	if len(rs) == 0 {
		return nil
	}
	switch f := strings.ToLower(*format); f {
	case "column":
		z := printWalkResults(os.Stdout, rs)

		log.Println()
		log.Printf("%d files found (%dMB) - uniq: %d - corrupted: %d (%3.2f%%)", z.Count, z.Size>>20, z.Uniq, z.Invalid, z.Corrupted())
	case "":
		z := printWalkResults(ioutil.Discard, rs)
		log.Printf("%d files found (%dMB) - uniq: %d - corrupted: %d (%3.2f%%)", z.Count, z.Size>>20, z.Uniq, z.Invalid, z.Corrupted())
	case "csv":
		w := csv.NewWriter(os.Stdout)
		defer w.Flush()
		for n, c := range rs {
			row := []string{
				n,
				strconv.FormatUint(c.Count, 10),
				strconv.FormatUint(c.Uniq, 10),
				strconv.FormatUint(c.Size>>20, 10),
				strconv.FormatUint(c.Invalid, 10),
				strconv.FormatFloat(c.Corrupted(), 'f', -1, 64),
			}
			if err := w.Write(row); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported format: %s", *format)
	}

	return nil
}

func printWalkResults(ws io.Writer, rs map[string]*Coze) *Coze {
	w := tabwriter.NewWriter(ws, 16, 2, 4, ' ', 0)
	defer w.Flush()

	logger := log.New(w, "", 0)
	logger.Println("UPI\tFiles\tUniq\tSize (MB)\tCorrupted")

	var z Coze
	for n, c := range rs {
		z.Count += c.Count
		z.Size += c.Size
		z.Invalid += c.Invalid
		z.Uniq += c.Uniq

		logger.Printf("%-s\t%d\t%d\t%d\t%d\t(%3.2f%%)", n, c.Count, c.Uniq, c.Size>>20, c.Invalid, c.Corrupted())
	}
	return &z
}

func countFiles(queue <-chan *File, merge bool) map[string]*Coze {
	rs := make(map[string]*Coze)
	fs := make(map[string]struct{})

	for f := range queue {
		var k string
		if merge {
			k = f.Info
		} else {
			k = f.String()
		}
		c, ok := rs[k]
		if !ok {
			c = &Coze{}
			rs[k] = c
		}
		c.Count++
		c.Size += uint64(f.Size)

		n := f.Name()
		if _, ok := fs[n]; !ok {
			c.Uniq++
			fs[n] = struct{}{}
		}
		if !f.Valid() {
			c.Invalid++
		}
	}
	return rs
}
