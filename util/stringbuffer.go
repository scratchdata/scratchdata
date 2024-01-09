package util

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	// sqlIdent matches a valid unquoted SQL identifier
	sqlIdent = regexp.MustCompile(`^[a-zA-Z]\w*$`)
)

// StringBuffer provide various helper methods for working with a strings.Builder
type StringBuffer struct {
	strings.Builder
}

// Pf is equivalent to `fmt.Fprintf(b, f, a...)`
func (b *StringBuffer) Pf(f string, a ...any) *StringBuffer {
	fmt.Fprintf(b, f, a...)
	return b
}

// Sp prints a space to the buffer, it's equivalent to `b.Pf(" ")`
func (b *StringBuffer) Sp() *StringBuffer {
	return b.Pf(" ")
}

// Pquote prints a quoted string to the buffer with quote's in the input replaced with escapedQuote
//
// It's is equivalent to:
// > s = Sprintf(f, a...)
// > s = RecplaceAll(s, quote, escapedQuote)
// > b.Pf(quote + s + quote)`
func (b *StringBuffer) Pquote(quote rune, escapedQuote string, f string, a ...any) *StringBuffer {
	s := fmt.Sprintf(f, a...)
	s = strings.ReplaceAll(s, string(quote), escapedQuote)
	b.WriteRune(quote)
	b.WriteString(s)
	b.WriteRune(quote)
	return b
}

// SQLString prints a quoted SQL string to the buffer
func (b *StringBuffer) SQLString(f string, a ...any) *StringBuffer {
	return b.Pquote('\'', `''`, f, a...)
}

// SQLIdent prints a (quoted, iff required) SQL identifier to the buffer
func (b *StringBuffer) SQLIdent(f string, a ...any) *StringBuffer {
	s := fmt.Sprintf(f, a...)
	if sqlIdent.MatchString(s) {
		b.WriteString(s)
		return b
	}
	return b.Pquote('"', `""`, s)
}

// Pif is equivalent to `if (ok) { b.Pf(f, a...) }`
func (b *StringBuffer) Pif(ok bool, f string, a ...any) *StringBuffer {
	if ok {
		b.Pf(f, a...)
	}
	return b
}

// Reset reset the buffer to be empty
func (b *StringBuffer) Reset() *StringBuffer {
	b.Builder.Reset()
	return b
}
