package memory

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestStorage(t *testing.T) {
	files := []struct {
		Path    string
		Content string
	}{
		{"/a", "hello"},
		{"/b", "world"},
	}
	ms := NewStorage()

	for _, f := range files {
		if err := ms.Upload(f.Path, strings.NewReader(f.Content)); err != nil {
			t.Fatalf("Upload(%s, %s): %s", f.Path, f.Content, err)
		}
	}

	for _, f := range files {
		t.Run("download: "+f.Path, func(t *testing.T) {
			tmp, err := os.CreateTemp(t.TempDir(), "")
			if err != nil {
				t.Fatalf("Cannot create temp file: %s", err)
			}
			defer tmp.Close()

			if err := ms.Download(f.Path, tmp); err != nil {
				t.Fatalf("Download(%s): %s", f.Path, err)
			}

			if _, err := tmp.Seek(0, 0); err != nil {
				t.Fatalf("Cannot seek temp file: %s", err)
			}

			content, err := io.ReadAll(tmp)
			if err != nil {
				t.Fatalf("Download(%s): read: %s", f.Path, err)
			}

			if string(content) != f.Content {
				t.Fatalf("Download(%s): Expected '%s; Got '%s'", f.Path, f.Content, content)
			}
		})
	}

	t.Run("download: missing file", func(t *testing.T) {
		tmp, err := os.CreateTemp(t.TempDir(), "")
		if err != nil {
			t.Fatalf("Cannot create temp file: %s", err)
		}
		defer tmp.Close()

		if err := ms.Download("/c", tmp); err == nil {
			t.Fatalf("Downloading missing file /c should fail")
		}
	})
}
