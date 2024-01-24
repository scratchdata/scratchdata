package bigquery

import (
	"cloud.google.com/go/bigquery"
	"github.com/oklog/ulid/v2"
)

type Record map[string]bigquery.Value

func (r Record) Save() (map[string]bigquery.Value, string, error) {
	return r, ulid.Make().String(), nil
}

var _ bigquery.ValueSaver = Record{}
