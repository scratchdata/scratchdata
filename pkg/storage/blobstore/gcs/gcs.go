package gcs

import (
	"context"

	"io"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/util"
	"google.golang.org/api/option"
)

type Storage struct {
	Bucket                string `mapstructure:"bucket"`
	CredentialsJsonString string `mapstructure:"credentials_json"`
	Client                *storage.Client
}

func (s *Storage) Upload(path string, r io.Reader) error {
	ctx := context.TODO()
	wc := s.Client.Bucket(s.Bucket).Object(path).NewWriter(ctx)
	if _, err := io.Copy(wc, r); err != nil {
		log.Error().Err(err).Msg("error copying to gcs")
		wc.Close()
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Storage) Download(path string, w io.WriterAt) error {
	ctx := context.TODO()
	rc, err := s.Client.Bucket(s.Bucket).Object(path).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	objectSize := rc.Attrs.Size

	chunkSize := 1 * 1024 * 1024 // 1MB chunk

	var offset int64 = 0
	for {

		buf := make([]byte, chunkSize)

		n, err := rc.Read(buf)
		if err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			return err
		}

		_, err = w.WriteAt(buf[:n], offset)
		if err != nil {
			return err
		}

		offset += int64(n)

		if offset >= objectSize {
			break
		}
	}

	return nil
}

func (s *Storage) Delete(path string) error {
	ctx := context.TODO()
	if err := s.Client.Bucket(s.Bucket).Object(path).Delete(ctx); err != nil {
		return err
	}
	return nil
}

func NewStorage(c map[string]any) (*Storage, error) {
	q := util.ConfigToStruct[Storage](c)
	ctx := context.TODO()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(q.CredentialsJsonString)))
	if err != nil {
		return nil, err
	}

	return &Storage{
		Bucket: q.Bucket,
		Client: client,
	}, nil
}
