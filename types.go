package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

type Gap struct {
	UPI    string    `json:"upi" xml:"upi"`
	Before uint32    `json:"last" xml:"last"`
	After  uint32    `json:"first" xml:"first"`
	Starts time.Time `json:"dtstart" xml:"dtstart"`
	Ends   time.Time `json:"dtend" xml:"dtend"`
}

func (g *Gap) Count() uint32 {
	return (g.After - g.Before) - 1
}

func (g *Gap) Duration() time.Duration {
	return g.Ends.Sub(g.Starts)
}

type Range struct {
	First uint32
	Last  uint32
}

func (r *Range) Total() uint32 {
	return r.Last - r.First
}

func (r *Range) Has(v uint32) bool {
	return r.First <= v && r.Last >= v
}

func (r *Range) String() string {
	return fmt.Sprintf("[%d, %d]", r.First, r.Last)
}

func single(v uint32) *Range {
	return &Range{v, v}
}

type Coze struct {
	UPI     string `json:"upi" xml:"upi"`
	Count   uint64 `json:"total" xml:"total"`
	Size    uint64 `json:"size" xml:"size"`
	Invalid uint64 `json:"invalid" xml:"invalid"`
	Uniq    uint64 `json:"uniq" xml:"uniq"`

	Starts time.Time `json:"dtstart" xml:"dtstart"`
	Ends   time.Time `json:"dtend" xml:"dtend"`

	First uint32 `json:"first" xml:"first"`
	Last  uint32 `json:"last" xml:"last"`

	seen []*Range
}

func (c *Coze) Update(f *File) {
	c.Count++
	c.Size += uint64(f.Size)
	if c.Starts.IsZero() || c.Starts.Equal(f.AcqTime) || c.Starts.After(f.AcqTime) {
		c.Starts = f.AcqTime
		c.First = f.Sequence
	}
	if c.Ends.IsZero() || c.Ends.Equal(f.AcqTime) || c.Ends.Before(f.AcqTime) {
		c.Ends = f.AcqTime
		c.Last = f.Sequence
	}

	if f.Valid() {
		if !c.Seen(f.Sequence) {
			c.Uniq++
		}
	} else {
		c.Invalid++
	}
}

func (c *Coze) Seen(v uint32) bool {
	s, ok := inRanges(c.seen, v)
	if !ok {
		c.seen = s
	}
	return ok
}

func (c Coze) Ranges() []*Range {
	return c.seen
}

func (c Coze) MissingRanges() []*Range {
	n := len(c.seen)
	if n == 0 {
		return nil
	}
	var rs []*Range
	for i := 1; i < n; i++ {
		r := Range{First: c.seen[i-1].Last, Last: c.seen[i].First}
		rs = append(rs, &r)
	}
	return rs
}

func (c Coze) Total() uint32 {
	var t uint32
	for _, r := range c.seen {
		t += r.Total()
	}
	return t + 1
}

func (c Coze) Range() (uint32, uint32) {
	n := len(c.seen)
	if n == 0 {
		return 0, 0
	}
	first, last := c.seen[0], c.seen[n-1]
	return first.First, last.Last
}

func (c Coze) Missing() uint64 {
	if len(c.seen) == 0 {
		return 0
	}
	var m uint64
	for i := 1; i < len(c.seen); i++ {
		d := c.seen[i].First - c.seen[i-1].Last
		m += uint64(d - 1)
	}
	return m
}

func (c Coze) Duration() time.Duration {
	return c.Ends.Sub(c.Starts)
}

func (c Coze) Corrupted() float64 {
	if c.Count == 0 || c.Invalid == 0 {
		return 0
	}
	return float64(c.Invalid) / float64(c.Count)
}

type ByFunc func(*File) string

func byUPI(f *File) string {
	return f.String()
}

func bySource(f *File) string {
	return f.Source
}

type File struct {
	Path     string    `json:"path" xml:"path"`
	Source   string    `json:"source" xml:"source"`
	Info     string    `json:"upi" xml:"upi"`
	Size     int64     `json:"size" xml:"size"`
	Sequence uint32    `json:"sequence" xml:"sequence"`
	AcqTime  time.Time `json:"dtstamp" xml:"dtstamp"`
	RecTime  time.Time `json:"-" xml:"-"`
}

func (f *File) Compare(p *File) *Gap {
	if p == nil || f.String() != p.String() || f.Sequence == p.Sequence+1 {
		return nil
	}
	if p.AcqTime.After(f.AcqTime) {
		return p.Compare(f)
	}
	g := Gap{
		UPI:    p.String(),
		Starts: p.AcqTime,
		Ends:   f.AcqTime,
		Before: p.Sequence,
		After:  f.Sequence,
	}
	return &g
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

func parseFilename(p, upi string, i int64) (*File, error) {
	// if !utf8.ValidString(p) {
	// 	return nil, nil
	// }
	if !Keep(filepath.Base(p)) {
		return nil, nil
	}
	ps := strings.Split(filepath.Base(p), "_")

	f := File{
		Path:   p,
		Source: strings.TrimLeft(ps[0], "0"),
		Size:   i,
	}
	if s, err := strconv.ParseInt(f.Source, 16, 8); err != nil {
		return nil, err
	} else {
		var origins []int
		switch ps[len(ps)-5] {
		case "1", "2":
			origins = OriImages
		case "3":
			origins = OriSciences
		default:
		}
		if !acceptOrigin(int(s), origins) {
			return nil, nil
		}
	}
	if len(upi) == 0 {
		f.Info = strings.Join(ps[1:len(ps)-5], "_")
	} else {
		f.Info = upi
	}
	if n, err := strconv.ParseUint(ps[len(ps)-4], 10, 32); err == nil {
		f.Sequence = uint32(n)
	} else {
		return nil, err
	}

	if t, err := time.Parse("20060102150405", ps[len(ps)-3]+ps[len(ps)-2]); err == nil {
		d, _ := strconv.ParseInt(strings.TrimLeft(ps[0], "0"), 10, 64)
		f.RecTime = t.Add(time.Duration(d) * time.Minute)
		f.AcqTime = t
	} else {
		return nil, err
	}
	return &f, nil
}

var (
	OriImages   = []int{0x33, 0x34, 0x37, 0x38, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47}
	OriSciences = []int{0x35, 0x36, 0x39, 0x40, 0x41, 0x51}
)

func acceptOrigin(o int, origins []int) bool {
	if len(origins) == 0 {
		return false
	}
	ix := sort.SearchInts(origins, o)
	return ix < len(origins) && origins[ix] == o
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

func inRanges(seen []*Range, v uint32) ([]*Range, bool) {
	n := len(seen)
	if n == 0 {
		seen = append(seen, single(v))
		return seen, false
	}
	ix := sort.Search(n, func(i int) bool {
		return seen[i].Last >= v
	})
	if ix >= n {
		if d := v - seen[n-1].Last; d == 1 {
			seen[n-1].Last = v
		} else {
			seen = append(seen, single(v))
		}
		return seen, false
	}
	if seen[ix].Has(v) {
		return seen, true
	}
	if d := seen[ix].First - v; d == 1 {
		seen[ix].First = v
		if ix > 0 {
			if d := v - seen[ix-1].Last; d == 1 {
				seen[ix].First = seen[ix-1].First
				seen = append(seen[:ix-1], seen[ix:]...)
			}
		}
		return seen, false
	}
	if ix == 0 {
		seen = append([]*Range{single(v)}, seen...)
		return seen, false
	} else {
		if d := v - seen[ix-1].Last; d == 1 {
			seen[ix-1].Last = v
		} else {
			seen = append(seen[:ix], append([]*Range{single(v)}, seen[ix:]...)...)
		}
		return seen, false
	}
	return seen, true
}
