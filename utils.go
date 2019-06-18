package main

import (
	"strings"
	"unicode"
)

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
