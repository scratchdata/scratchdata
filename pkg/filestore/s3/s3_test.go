package s3

import (
	"context"
	"errors"
	"net/http/httptest"
	"scratchdata/config"
	"scratchdata/pkg/filestore"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func TestS3(t *testing.T) {
	faker := gofakes3.New(s3mem.New())
	s3Srv := httptest.NewServer(faker.Server())
	defer s3Srv.Close()

	files := []struct {
		Path    string
		Content string
	}{
		{"file-a", "hello"},
		{"file-b", "world"},
	}

	bucket := "test-bucket"
	store := NewStorage(config.S3{
		AccessKeyId:     "user",
		SecretAccessKey: "hunter2",
		S3Bucket:        bucket,
		Region:          "test-region",
		Endpoint:        s3Srv.URL,
	})

	_, err := store.client.CreateBucketWithContext(
		context.Background(),
		&s3.CreateBucketInput{Bucket: aws.String(bucket)},
	)
	if err != nil {
		t.Fatalf("Cannot create bucket: %s: %s", bucket, err)
	}

	for _, f := range files {
		t.Run("Upload:"+f.Path, func(t *testing.T) {
			if err := store.Upload(f.Path, strings.NewReader(f.Content)); err != nil {
				t.Fatal(err)
			}
		})
	}

	for _, f := range files {
		t.Run("Download:"+f.Path, func(t *testing.T) {
			w := aws.NewWriteAtBuffer(nil)
			if err := store.Download(f.Path, w); err != nil {
				t.Fatal(err)
			}
			res := string(w.Bytes())
			if res != f.Content {
				t.Fatalf("Expected '%s'; Got '%s'", f.Content, res)
			}
		})
	}

	t.Run("download: missing file", func(t *testing.T) {
		w := aws.NewWriteAtBuffer(nil)
		if err := store.Download("missig-file", w); !errors.Is(err, filestore.ErrNotFound) {
			t.Fatalf("Downloading missing-file should fail with error '%s': Got '%v'", filestore.ErrNotFound, err)
		}
	})
}
