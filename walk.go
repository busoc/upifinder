package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/midbel/cli"
	"github.com/midbel/linewriter"
)

var walkCommand = &cli.Command{
	Usage: "walk [-d] [-s] [-e] [-u] [-c] <archive,...>",
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
  -c         print the results as csv
  -z         discard UPI that have no missing files

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
	csv := cmd.Flag.Bool("c", false, "csv")
	zero := cmd.Flag.Bool("z", false, "discard row with zero missing")
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
	if rs := countFiles(walkFiles(paths, *upi, 8)); len(rs) > 0 {
		reportWalkResults(rs, *csv, *zero)
	}
	return nil
}

func reportWalkResults(rs map[string]*Coze, csv, zero bool) {
	vs := make([]string, 0, len(rs))
	for n := range rs {
		vs = append(vs, n)
	}
	sort.Strings(vs)
	line := Line(csv)
	for _, n := range vs {
		c := rs[n]
		if zero && c.Missing() == 0 {
			continue
		}

		first, last := c.Range()

		line.AppendString(Transform(c.UPI), 16, linewriter.AlignRight)
		line.AppendUint(c.Count, 10, linewriter.AlignRight)
		line.AppendUint(c.Uniq, 10, linewriter.AlignRight)
		if csv {
			line.AppendUint(c.Size, 10, linewriter.AlignRight)
		} else {
			line.AppendSize(int64(c.Size), 10, linewriter.AlignRight)
		}
		line.AppendUint(c.Invalid, 10, linewriter.AlignRight)
		if ratio := c.Corrupted(); csv {
			line.AppendFloat(ratio, 10, 2, linewriter.AlignRight)
		} else {
			line.AppendPercent(ratio, 10, 2, linewriter.AlignRight)
		}
		line.AppendTime(c.Starts, time.RFC3339, linewriter.AlignRight)
		line.AppendTime(c.Ends, time.RFC3339, linewriter.AlignRight)
		line.AppendUint(uint64(first), 10, linewriter.AlignRight)
		line.AppendUint(uint64(last), 10, linewriter.AlignRight)
		line.AppendUint(c.Missing(), 10, linewriter.AlignRight)

		io.Copy(os.Stdout, line)
	}
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
			}
			rs[k] = c
		}
		c.Update(f)
	}
	return rs
}
