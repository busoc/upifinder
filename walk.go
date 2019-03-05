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

var walkCommand = &cli.Command{
	Usage: "walk [-d] [-s] [-e] [-u] [-f] <archive,...>",
	Short: "provide the number of files available in the archive",
	Alias: []string{"scan", "report"},
	Run:   runWalk,
	Desc: `"walk" (report, scan) traverse the Hadock archive and count the number of files
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

Unique files:

the uniq field only reports the number of unique files (correct) excluding bad files
from the count and the doubles.

Options:

  -u UPI     only count files for the given UPI
  -s START   only count files created after START
  -e END     only count files created before END
  -d DAYS    only count files created during a period of DAYS
  -f FORMAT  print the results in the given format ("", csv, column)

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

func runWalk(cmd *cli.Command, args []string) error {
	cmd.Desc = fmt.Sprintf(cmd.Desc, "\u2764")

	var start, end When
	cmd.Flag.Var(&start, "s", "start")
	cmd.Flag.Var(&end, "e", "end")
	upi := cmd.Flag.String("u", "", "upi")
	period := cmd.Flag.Int("d", 0, "period")
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

	rs := countFiles(walkFiles(paths, *upi, 8))
	if len(rs) == 0 {
		return nil
	}
	switch f := strings.ToLower(*format); f {
	case "column", "":
		z := printWalkResults(os.Stdout, rs)

		log.Println()
		s := strings.TrimSpace(prettySize(z.Size))
		log.Printf("%d files found (%s) - uniq: %d - corrupted: %d (%3.2f%%)", z.Count, s, z.Uniq, z.Invalid, z.Corrupted())
	case "csv":
		w := csv.NewWriter(os.Stdout)
		defer w.Flush()
		for n, c := range rs {
			row := []string{
				Transform(n),
				strconv.FormatUint(c.Count, 10),
				strconv.FormatUint(c.Uniq, 10),
				strconv.FormatUint(c.Size>>20, 10),
				strconv.FormatUint(c.Invalid, 10),
				strconv.FormatFloat(c.Corrupted(), 'f', -1, 64),
				strconv.FormatUint(c.Missing(), 10),
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
	const row = "%-s\t%d\t%d\t%s\t%d\t%3.2f%%\t%s\t%s\t%d\t%d\t%d"
	w := tabwriter.NewWriter(ws, 16, 2, 4, ' ', 0)
	defer w.Flush()

	logger := log.New(w, "", 0)
	logger.Println("UPI\tFiles\tUniq\tSize\tInvalid\tRatio\tStarts\tEnds\tFirst\tLast\tMissing")

	var (
		z  Coze
		vs []string
	)
	for n := range rs {
		vs = append(vs, n)
	}
	sort.Strings(vs)
	for _, n := range vs {
		c := rs[n]

		z.Count += c.Count
		z.Size += c.Size
		z.Invalid += c.Invalid
		z.Uniq += c.Uniq

		starts, ends := c.Starts.Format(time.RFC3339), c.Ends.Format(time.RFC3339)
		first, last := c.Range()
		logger.Printf(row,
			Transform(c.UPI),
			c.Count,
			c.Uniq,
			prettySize(c.Size),
			c.Invalid,
			c.Corrupted(),
			starts,
			ends,
			first, // before was c.First
			last, // before was c.Last
			c.Missing(),
		)
	}
	return &z
}

func countFiles(queue <-chan *File) map[string]*Coze {
	rs := make(map[string]*Coze)

	for f := range queue {
		k := f.String()
		c, ok := rs[k]
		if !ok {
			c = &Coze{
				UPI:    f.String(),
				First:  f.Sequence,
				Last:   f.Sequence,
				Starts: f.AcqTime,
				Ends:   f.AcqTime,
				// seen:   make(map[uint32]struct{}),
				// seen: 	[]uint32{f.Sequence},
			}
			rs[k] = c
		}
		c.Update(f)
		// c.Count++
		// c.Size += uint64(f.Size)
		// if c.Starts.IsZero() || c.Starts.After(f.AcqTime) {
		// 	c.Starts = f.AcqTime
		// 	c.First = f.Sequence
		// }
		// if c.Ends.IsZero() || c.Ends.Before(f.AcqTime) {
		// 	c.Ends = f.AcqTime
		// 	c.Last = f.Sequence
		// }
		//
		// if f.Valid() {
		// 	if !c.Seen(f.Sequence) {
		// 		c.Uniq++
		// 	}
		// } else {
		// 	c.Invalid++
		// }
	}
	return rs
}
