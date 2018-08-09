package main

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
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
	r.Handle("/{instance}/{type}/{mode}/archives/", negociateStructured(listFiles(c.Datadir))).HeadersRegexp("Accept", "application/(xml|json)").Methods(http.MethodGet)
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
