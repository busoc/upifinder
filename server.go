package main

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

type Handler func(*http.Request) (interface{}, error)

var srvCommand = &cli.Command{
	Usage: "server [-d] <config.toml>",
	Short: "",
	Run:   runServer,
}

func runServer(cmd *cli.Command, args []string) error {
	dev := cmd.Flag.Bool("d", false, "development mode")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	c := struct {
		Addr     string `toml:"address"`
		Database string `toml:"database"`
		Datadir  string `toml:"datadir"`
		Interval int    `toml:"interval"`
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

	h := setupHandlers(hist, c.Datadir, c.Interval)
	if *dev {
		h = handlers.LoggingHandler(os.Stderr, h)
	}
	return http.ListenAndServe(c.Addr, h)
}

const (
	reportURL = "/{instance}/{type}/{mode}/{report}/"
	filesURL  = "/{instance}/{type}/{mode}/archives/"
)

const (
	acceptCSV    = "text/csv"
	acceptJSON   = "application/json"
	acceptStruct = "application/(xml|json)"
)

const (
	statusReport = "status"
	filesReport  = "files"
)

const MaxBodySize = 16<<10

const DefaultInterval = time.Duration(24*30) * time.Hour

func setupHandlers(hist *History, datadir string, interval int) http.Handler {
	cs := []string{
		acceptCSV,
		acceptStruct,
	}
	delay := time.Duration(interval) * time.Hour
	r := mux.NewRouter()
	for _, c := range cs {
		var h http.Handler
		switch c {
		case acceptCSV:
			h = negociateCSV(viewReports(hist, delay))
		case acceptStruct:
			h = negociateStructured(viewReports(hist, delay))
		}
		if h == nil {
			continue
		}
		r.Handle(reportURL, h).HeadersRegexp("Accept", c).Methods(http.MethodGet)
	}
	s := negociateStructured(storeReports(hist))
	r.Handle(reportURL, s).Headers("Content-Type", acceptJSON).Methods(http.MethodPost, http.MethodOptions)

	f := negociateStructured(listFiles(datadir))
	r.Handle(filesURL, f).HeadersRegexp("Accept", acceptStruct).Methods(http.MethodGet, http.MethodOptions)

	rx := handlers.CompressHandler(r)
	return handlers.CORS()(rx)
}

func listFiles(datadir string) Handler {
	f := func(r *http.Request) (interface{}, error) {
		c := struct {
			Starts time.Time
			Ends   time.Time
		}{}
		var err error
		q := r.URL.Query()
		if c.Starts, err = time.Parse(time.RFC3339, q.Get("dtstart")); err != nil {
			return nil, err
		}
		if c.Ends, err = time.Parse(time.RFC3339, q.Get("dtend")); err != nil {
			return nil, err
		}

		vars := mux.Vars(r)
		base := filepath.Join(datadir, vars["instance"], vars["type"], vars["mode"])
		is, err := ioutil.ReadDir(base)
		if err != nil {
			return nil, err
		}
		dirs := make([]string, 0, len(is))
		for _, i := range is {
			if i.IsDir() {
				dirs = append(dirs, filepath.Join(base, i.Name()))
			}
		}
		paths, err := listPaths(dirs, 0, c.Starts, c.Ends)
		if err != nil {
			return nil, err
		}
		var ds []*File
		for f := range walkFiles(paths, q.Get("upi"), 8) {
			f.Path = filepath.Base(f.Path)
			ds = append(ds, f)
		}
		return ds, nil
	}
	return f
}

func viewNodes(hist *History) Handler {
	f := func(r *http.Request) (interface{}, error) {
		return hist.ViewNodes(), nil
	}
	return f
}

func storeReports(hist *History) Handler {
	f := func(r *http.Request) (interface{}, error) {
		defer r.Body.Close()

		vars := mux.Vars(r)
		key := fmt.Sprintf("%s/%s/%s/%s", vars["instance"], vars["type"], vars["mode"], vars["report"])

		var err error
		switch report, body := vars["report"], io.LimitReader(r.Body, MaxBodySize); report {
		case statusReport:
			c := struct {
				When time.Time `json:"dtstamp"`
				Data []*Gap    `json:"report"`
			}{}
			if err = json.NewDecoder(body).Decode(&c); err != nil {
				break
			}
			err = hist.StoreStatus(key, c.Data, c.When)
		case filesReport:
			c := struct {
				When time.Time        `json:"dtstamp"`
				Data map[string]*Coze `json:"report"`
			}{}
			if err = json.NewDecoder(body).Decode(&c); err != nil {
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

func viewReports(hist *History, interval time.Duration) Handler {
	if interval <= 0 {
		interval = DefaultInterval
	}
	f := func(r *http.Request) (interface{}, error) {
		vars := mux.Vars(r)
		key := fmt.Sprintf("%s/%s/%s/%s", vars["instance"], vars["type"], vars["mode"], vars["report"])

		q, err := parseQuery(r.URL.Query(), interval)
		if err != nil {
			return nil, err
		}

		var data interface{}
		switch report := vars["report"]; report {
		case filesReport:
			data, err = hist.ViewFiles(key, q)
		case statusReport:
			data, err = hist.ViewStatus(key, q)
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

type query struct {
	Starts time.Time
	Ends   time.Time
	UPI    []string
}

func (q query) Keep(u string, s, e time.Time) bool {
	if len(q.UPI) > 0 {
		ix := sort.SearchStrings(q.UPI, u)
		if ix >= len(q.UPI) || q.UPI[ix] != u {
			return false
		}
	}
	return q.Between(s) || q.Between(e)
}

func (q query) Between(t time.Time) bool {
	if q.Starts.IsZero() || q.Ends.IsZero() {
		return true
	}
	return t.Equal(q.Starts) || t.Equal(q.Ends) || (t.After(q.Starts) && t.Before(q.Ends))
}

func parseQuery(qs url.Values, interval time.Duration) (*query, error) {
	var (
		q   query
		err error
	)
	q.Starts, err = parseTime(qs.Get("dtstart"))
	if err != nil {
		return nil, err
	}
	q.Ends, err = parseTime(qs.Get("dtend"))
	if err != nil {
		return nil, err
	}
	if !q.Starts.IsZero() && !q.Ends.IsZero() {
		if q.Starts.Equal(q.Ends) || q.Starts.After(q.Ends) {
			return nil, fmt.Errorf("invalid starts/ends")
		}
		if q.Ends.Sub(q.Starts) > interval {
			return nil, fmt.Errorf("interval too large")
		}
	}
	q.UPI = qs["upi"]
	sort.Strings(q.UPI)

	return &q, nil
}

func parseTime(s string) (time.Time, error) {
	var (
		t   time.Time
		err error
	)
	if s == "" {
		return t, err
	}
	formats := []string{
		time.RFC3339,
		"2006-01-02",
		"2006-01-02 15:04:05",
	}
	for _, f := range formats {
		t, err = time.Parse(f, s)
		if err == nil {
			return t, nil
		}
	}
	return t, fmt.Errorf("no suitable format found for %q", s)
}

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
