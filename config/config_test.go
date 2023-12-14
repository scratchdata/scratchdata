package config

import (
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	fileNames, err := filepath.Glob(filepath.FromSlash("testdata/config/*.toml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range fileNames {
		_, err := Load(fn)
		if err != nil {
			t.Fatalf("%s: %s\n", fn, err)
		}
	}

	invalidFileNames, err := filepath.Glob(filepath.FromSlash("testdata/config/invalid/*.toml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range invalidFileNames {
		_, err := Load(fn)
		if err == nil {
			t.Fatalf("%s: loading should fail\n", fn)
		}
	}
}
