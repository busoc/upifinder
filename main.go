package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"golang.org/x/sync/errgroup"
)

const (
	TimeFormat    = "2006-01-02"
	Day           = time.Hour * 24
	DefaultPeriod = 7
)

const helpText = `{{.Name}} scan the Hadock archive and produces report about
its status such as:

  * total and uniq files
  * total corrupted files
  * status of missing files

Usage:

  {{.Name}} command [arguments]

The commands are:

{{range .Commands}}{{if .Runnable}}{{printf "  %-9s %s" .String .Short}}{{end}}
{{end}}

Use {{.Name}} [command] -h for more information about its usage.
`

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

type File struct {
	Path     string
	Source   string
	Info     string
	Size     int64
	Sequence uint32
	AcqTime  time.Time
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

var commands = []*cli.Command{
	{
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
  -f FORMAT  print the results in the given format ("", csv, column, summary, json)
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
	},
	{
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
	},
	{
		Usage: "server [-d] <config.toml>",
		Short: "",
		Run:   runServer,
	},
	{
		Usage: "push [-d] <remote> <archive...>",
		Short: "push report(s) to remote server",
		Run:   runPush,
	},
}

func init() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	cli.Version = "0.4.0"
	cli.BuildTime = "2018-08-03 12:06:00"
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Fatalf("unexpected error: %s", err)
		}
	}()
	usage := func() {
		data := struct {
			Name     string
			Commands []*cli.Command
		}{
			Name:     filepath.Base(os.Args[0]),
			Commands: commands,
		}
		t := template.Must(template.New("help").Parse(helpText))
		t.Execute(os.Stderr, data)

		os.Exit(2)
	}
	if err := cli.Run(commands, usage, nil); err != nil {
		log.Fatalln(err)
	}
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

func runServer(cmd *cli.Command, args []string) error {
	dev := cmd.Flag.Bool("d", false, "development mode")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	c := struct {
		Addr     string `toml:"address"`
		Database string `toml:"database"`
	}{}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer f.Close()
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	dir, _ := filepath.Split(c.Database)
	if err := os.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		return err
	}
	db, err := bolt.Open(c.Database, 0644, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	hist := &History{db}

	r := mux.NewRouter()
	r.Handle("/", negociateStructured(viewNodes(hist))).HeadersRegexp("Accept", "application/(xml|json)").Methods(http.MethodGet)
	r.Handle("/{instance}/{type}/{mode}/{report}/", negociateStructured(viewReports(hist))).HeadersRegexp("Accept", "application/(xml|json)").Methods(http.MethodGet)
	// r.Handle("/{instance}/{type}/{mode}/{report}/{UPI}", negociateStructured(viewReport(hist))).HeadersRegexp("Accept", "application/(xml|json)").Methods(http.MethodGet)
	r.Handle("/{instance}/{type}/{mode}/{report}/", negociateCSV(viewReports(hist))).Headers("Accept", "text/csv").Methods(http.MethodGet)
	// r.Handle("/{instance}/{type}/{mode}/{report}/{UPI}", negociateCSV(viewReport(hist))).Headers("Accept", "text/csv").Methods(http.MethodGet)
	r.Handle("/{instance}/{type}/{mode}/{report}/", negociateStructured(storeReports(hist))).Headers("Content-Type", "application/json").Methods(http.MethodPost)
	if *dev {
		h := handlers.LoggingHandler(os.Stderr, handlers.CompressHandler(r))
		return http.ListenAndServe(c.Addr, handlers.CORS()(h))
	}
	return http.ListenAndServe(c.Addr, r)
}

type ErrNotFound string

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("%s not found", string(e))
}

type History struct {
	*bolt.DB
}

type timegap struct {
	When time.Time `json:"dtstamp" xml:"dtstamp"`
	*Gap
}

type timecoze struct {
	When time.Time `json:"dtstamp" xml:"dtstamp"`
	*Coze
}

type Node struct {
	Name string `json:"name" xml:"name"`
	Values []string `json:"upis" xml:"upis"`
}

func (h *History) ViewNodes() []*Node {
	var vs []*Node
	err := h.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			n := Node{Name: string(name)}
			b.ForEach(func(k, _ []byte) error {
				n.Values = append(n.Values, string(k))
				return nil
			})
			vs = append(vs, &n)
			return nil
		})
	})
	if err != nil {
		return nil
	}
	return vs
}

func (h *History) ViewStatus(key string) ([]*timegap, error) {
	var ds []*timegap
	err := h.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(key))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			b := b.Bucket(k)
			c := b.Cursor()
			k, v = c.Last()
			if k == nil && v == nil {
				return nil
			}
			var (
				g Gap
				t time.Time
			)
			if err := t.UnmarshalText(k); err != nil {
				return err
			}
			if err := json.Unmarshal(v, &g); err != nil {
				return err
			}
			ds = append(ds, &timegap{When: t, Gap: &g})
			return nil
		})
	})
	return ds, err
}

func (h *History) ViewFiles(key string) (map[string]*timecoze, error) {
	ds := make(map[string]*timecoze)
	err := h.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(key))
		if b == nil {
			return nil
		}
		return b.ForEach(func(u, v []byte) error {
			b := b.Bucket(u)
			c := b.Cursor()
			k, v := c.Last()
			if k == nil && v == nil {
				return nil
			}
			var (
				z Coze
				t time.Time
			)
			if err := t.UnmarshalText(k); err != nil {
				return err
			}
			if err := json.Unmarshal(v, &z); err != nil {
				return err
			}
			ds[string(u)] = &timecoze{When: t, Coze: &z}
			return nil
		})
	})
	return ds, err
}

func (h *History) StoreStatus(key string, ds []*Gap, when time.Time) error {
	return h.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return err
		}
		mmt, err := when.MarshalText()
		if err != nil {
			return err
		}
		for _, d := range ds {
			b, err := b.CreateBucketIfNotExists([]byte(d.UPI))
			if err != nil {
				return err
			}
			var w bytes.Buffer
			if err := json.NewEncoder(&w).Encode(d); err != nil {
				return err
			}
			if err := b.Put(mmt, w.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (h *History) StoreFiles(key string, ds map[string]*Coze, when time.Time) error {
	return h.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return err
		}
		mmt, err := when.MarshalText()
		if err != nil {
			return err
		}
		for u, c := range ds {
			b, err := b.CreateBucketIfNotExists([]byte(u))
			if err != nil {
				return err
			}
			var w bytes.Buffer
			if err := json.NewEncoder(&w).Encode(c); err != nil {
				return err
			}
			if err := b.Put(mmt, w.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}

func viewNodes(hist *History) Handler {
	f := func(r *http.Request) (interface{}, error) {
		return hist.ViewNodes(), nil
	}
	return f
}

func viewReport(hist *History) Handler {
	f := func(r *http.Request) (interface{}, error) {
		return nil, nil
	}
	return f
}

func storeReports(hist *History) Handler {
	f := func(r *http.Request) (interface{}, error) {
		defer r.Body.Close()

		vars := mux.Vars(r)
		key := fmt.Sprintf("%s/%s/%s/%s", vars["instance"], vars["type"], vars["mode"], vars["report"])

		var err error
		switch report := vars["report"]; report {
		case "status":
			c := struct {
				When time.Time `json:"dtstamp"`
				Data []*Gap    `json:"report"`
			}{}
			if err = json.NewDecoder(r.Body).Decode(&c); err != nil {
				break
			}
			err = hist.StoreStatus(key, c.Data, c.When)
		case "files":
			c := struct {
				When time.Time        `json:"dtstamp"`
				Data map[string]*Coze `json:"report"`
			}{}
			if err = json.NewDecoder(r.Body).Decode(&c); err != nil {
				break
			}
			err = hist.StoreFiles(key, c.Data, c.When)
		default:
			return nil, ErrNotFound(report)
		}
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	return f
}

func viewReports(hist *History) Handler {
	f := func(r *http.Request) (interface{}, error) {
		vars := mux.Vars(r)
		key := fmt.Sprintf("%s/%s/%s/%s", vars["instance"], vars["type"], vars["mode"], vars["report"])

		var (
			data interface{}
			err  error
		)
		switch report := vars["report"]; report {
		case "files":
			data, err = hist.ViewFiles(key)
		case "status":
			data, err = hist.ViewStatus(key)
		default:
			return nil, ErrNotFound(report)
		}
		if err != nil {
			return nil, err
		}
		return data, nil
	}
	return f
}

type Handler func(*http.Request) (interface{}, error)

func negociateStructured(h Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		data, err := h(r)
		if err != nil {
			code := http.StatusInternalServerError
			switch err.(type) {
			case ErrNotFound:
				code = http.StatusNotFound
			default:
			}
			http.Error(w, err.Error(), code)
			return
		}
		if data == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		switch a := r.Header.Get("accept"); a {
		default:
			http.Error(w, fmt.Sprintf("can not export resources in %s", a), http.StatusNotImplemented)
		case "application/json":
			w.Header().Set("content-type", "application/json")
			json.NewEncoder(w).Encode(data)
		case "application/xml":
			w.Header().Set("content-type", "application/xml")
			c := struct {
				XMLName xml.Name    `xml:"reports"`
				Data    interface{} `xml:"report"`
			}{Data: data}
			xml.NewEncoder(w).Encode(c)
		}
	}
	return http.HandlerFunc(f)
}

func negociateCSV(h Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		data, err := h(r)
		if err != nil {
			code := http.StatusInternalServerError
			switch err.(type) {
			case ErrNotFound:
				code = http.StatusNotFound
			default:
			}
			http.Error(w, err.Error(), code)
			return
		}
		if data == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		wr := csv.NewWriter(w)
		defer wr.Flush()

		switch vs := data.(type) {
		default:
			http.Error(w, "can not export resources in txt/csv", http.StatusNotImplemented)
		case []*timegap:
			w.Header().Set("content-type", "text/csv")
			for _, g := range vs {
				row := []string{
					g.UPI,
					g.Starts.Format(time.RFC3339),
					g.Ends.Format(time.RFC3339),
					g.Duration().String(),
					strconv.FormatUint(uint64(g.Before), 10),
					strconv.FormatUint(uint64(g.After), 10),
					strconv.FormatUint(uint64(g.Count()), 10),
				}
				if err := wr.Write(row); err != nil {
					return
				}
			}
		case map[string]*timecoze:
			w.Header().Set("content-type", "text/csv")
			for u, c := range vs {
				row := []string{
					u,
					strconv.FormatUint(c.Count, 10),
					strconv.FormatUint(c.Uniq, 10),
					strconv.FormatUint(c.Size>>20, 10),
					strconv.FormatUint(c.Invalid, 10),
					strconv.FormatFloat(c.Corrupted(), 'f', -1, 64),
				}
				if err := wr.Write(row); err != nil {
					return
				}
			}
		}
	}
	return http.HandlerFunc(f)
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
	case "", "column":
		z := printWalkResults(os.Stdout, rs)

		log.Println()
		log.Printf("%d files found (%dMB) - uniq: %d - corrupted: %d (%3.2f%%)", z.Count, z.Size>>20, z.Uniq, z.Invalid, z.Corrupted())
	case "summary":
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
	case "json":
		c := struct {
			When     time.Time        `json:"dtstamp"`
			Paths    []string         `json:"dirs"`
			Counters map[string]*Coze `json:"status"`
		}{
			When:     time.Now(),
			Paths:    paths,
			Counters: rs,
		}
		return json.NewEncoder(os.Stdout).Encode(c)
	// case "xml":
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
