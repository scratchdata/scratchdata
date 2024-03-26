package util

import (
	"testing"
)

func TestStringBuffer(t *testing.T) {
	b := &StringBuffer{}

	b.Printf("hello")
	if s := b.String(); s != "hello" {
		t.Fatalf("Expected `hello`; Got %#q", s)
	}

	b.Printf(" %s", "world")
	if s := b.String(); s != "hello world" {
		t.Fatalf("Expected `hello hello`; Got %#q", s)
	}

	b.PrintfIf(false, "%s", "!")
	if s := b.String(); s != "hello world" {
		t.Fatalf("Expected `hello hello`; Got %#q", s)
	}

	b.PrintfIf(true, "%s", "!")
	if s := b.String(); s != "hello world!" {
		t.Fatalf("Expected `hello hello!`; Got %#q", s)
	}

	if s, r := `"a\"b"`, b.Reset().Quote('"', `\"`, `a"b`).String(); r != s {
		t.Fatalf("Expected %#q; Got %#q", r, s)
	}

	if s, r := `col`, b.Reset().SQLIdent(`col`).String(); r != s {
		t.Fatalf("Expected %#q; Got %#q", r, s)
	}
	if s, r := `"tbl.col"`, b.Reset().SQLIdent(`tbl.col`).String(); r != s {
		t.Fatalf("Expected %#q; Got %#q", r, s)
	}
	if s, r := `"c""l"`, b.Reset().SQLIdent(`c"l`).String(); r != s {
		t.Fatalf("Expected %#q; Got %#q", r, s)
	}

	if s, r := `'str'`, b.Reset().SQLString(`str`).String(); r != s {
		t.Fatalf("Expected %#q; Got %#q", r, s)
	}
	if s, r := `'s''r'`, b.Reset().SQLString(`s'r`).String(); r != s {
		t.Fatalf("Expected %#q; Got %#q", r, s)
	}
}
