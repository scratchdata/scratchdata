package memory

import (
	"io"
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
		rc, err := ms.Download(f.Path)
		if err != nil {
			t.Fatalf("Download(%s): %s", f.Path, err)
		}
		content, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("Download(%s): read: %s", f.Path, err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("Download(%s): close: %s", f.Path, err)
		}
		if string(content) != f.Content {
			t.Fatalf("Download(%s): Expected '%s; Got '%s'", f.Path, f.Content, content)
		}
	}

	_, err := ms.Download("/c")
	if err == nil {
		t.Fatalf("Downloading missing file /c should fail")
	}
}
