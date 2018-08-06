package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
	"log"

	"github.com/boltdb/bolt"
)

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
	Name   string   `json:"name" xml:"name"`
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
			log.Println("marshalling time failed for status report:", err)
			return err
		}
		for _, d := range ds {
			if err := storeReport(b, []byte(d.UPI), mmt, d); err != nil {
				log.Printf("storing status report for %s failed: %s", d.UPI, err)
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
			log.Println("marshalling time failed for files report:", err)
			return err
		}
		for u, c := range ds {
			if err := storeReport(b, []byte(u), mmt, c); err != nil {
				log.Printf("storing files report for %s failed:", u, err)
				return err
			}
		}
		return nil
	})
}

func storeReport(b *bolt.Bucket, upi, mmt []byte, datum interface{}) error {
	b, err := b.CreateBucketIfNotExists(upi)
	if err != nil {
		return err
	}
	bs, err := json.Marshal(datum)
	if err != nil {
		return err
	}
	k, v := b.Cursor().Last()
	if bytes.Equal(v, bs) {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return b.Put(mmt, bs)
}
