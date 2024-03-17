package gcs

import (
	"context"
	"os"

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

func (s *Storage) Download(path string, w *os.File) error {
	ctx := context.TODO()
	rc, err := s.Client.Bucket(s.Bucket).Object(path).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(w, rc)
	if err != nil {
		log.Error().Err(err).Msg("error reading from gcs")
		return err

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
