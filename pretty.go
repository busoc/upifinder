package main

import (
	"fmt"
	"math"
	"strings"
	"unicode"
)

const (
	kilo = 1024
	mega = kilo * kilo
	giga = mega * kilo
	tera = giga * kilo
)

func prettySize(s uint64) string {
	f := float64(s)
	var (
		x, m float64
		u, p string
	)
	if x, m = f/tera, math.Mod(f, tera); x > 1.0 {
		u = "TB"
	} else if x, m = f/giga, math.Mod(f, giga); x > 1.0 {
		u = "GB"
	} else if x, m = f/mega, math.Mod(f, mega); x > 1.0 {
		u = "MB"
	} else if x, m = f/kilo, math.Mod(f, kilo); x > 1.0 {
		u = "KB"
	} else {
		x, u = f, "B"
	}
	if m > 0 {
		p = "%6.2f%s"
	} else {
		p = "%6.0f%s"
	}
	return fmt.Sprintf(p, x, u)
}

func Transform(upi string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '/' {
			return r
		} else {
			return '*'
		}
	}, upi)
}

func Keep(n string) bool {
	str := []byte(n)
	for i := 0; i < len(str); i++ {
		b := str[i]
		k := ('A' <= b && b <= 'Z') || ('a' <= b && b <= 'z') || ('0' <= b && b <= '9') || b == '-' || b == '_' || b == '.'
		if !k {
			return k
		}
	}
	return true
}
