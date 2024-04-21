package api

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	queue_models "github.com/scratchdata/scratchdata/pkg/storage/queue/models"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var insertSize = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "insert_bytes",
	Help:    "Bytes inserted in single request",
	Buckets: prometheus.ExponentialBucketsRange(1000, 100_000_000, 5),
})

var insertArraySize = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "insert_array_length",
	Help:    "Items in single request",
	Buckets: prometheus.LinearBuckets(1, 50, 10),
})

func (a *ScratchDataAPIStruct) Copy(w http.ResponseWriter, r *http.Request) {
	message := queue_models.CopyDataMessage{}

	err := render.DecodeJSON(r.Body, &message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	message.SourceID = a.AuthGetDatabaseID(r.Context())

	teamId := a.AuthGetTeamID(r.Context())

	// Make sure the destination db is the same team as the source
	_, err = a.storageServices.Database.GetDestination(r.Context(), teamId, message.DestinationID)
	if err != nil {
		http.Error(w, "invalid destination", http.StatusBadRequest)
		return
	}

	// enqueue the copy job
	msg, err := a.storageServices.Database.Enqueue(models.CopyData, message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	render.JSON(w, r, render.M{"job_id": msg.ID})

}

func (a *ScratchDataAPIStruct) Select(w http.ResponseWriter, r *http.Request) {
	databaseID := a.AuthGetDatabaseID(r.Context())

	var query string
	query = r.URL.Query().Get("query")

	format := r.URL.Query().Get("format")

	if r.Method == "POST" {
		queryBytes, err := io.ReadAll(r.Body)
		if err != nil && len(queryBytes) > 0 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Unable to read query"))
			return
		}
		query = string(queryBytes)
	}

	if strings.TrimSpace(query) == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Query cannot be blank"))
		return
	}

	if err := a.executeQueryAndStreamData(r.Context(), w, query, databaseID, format); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *ScratchDataAPIStruct) executeQueryAndStreamData(ctx context.Context, w http.ResponseWriter, query string, databaseID uint, format string) error {
	dest, err := a.destinationManager.GetDestination(ctx, databaseID)
	if err != nil {
		return err
	}

	switch strings.ToLower(format) {
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		return dest.QueryCSV(query, w)
	default:
		w.Header().Set("Content-Type", "application/json")
		return dest.QueryJSON(query, w)
	}
}

func (a *ScratchDataAPIStruct) Insert(w http.ResponseWriter, r *http.Request) {
	databaseID := a.AuthGetDatabaseID(r.Context())
	table := chi.URLParam(r, "table")
	flatten := r.URL.Query().Get("flatten")

	var flattener Flattener
	if flatten == "vertical" {
		flattener = VerticalFlattener{}
	} else if flatten == "multitable" {
		flattener = NewMultiTableFlattener()
	} else {
		flattener = HorizontalFlattener{}
	}

	body, err := io.ReadAll(r.Body)
	insertSize.Observe(float64(len(body)))

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Unable to read data"))
		return
	}

	if !gjson.ValidBytes(body) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid JSON"))
		return
	}

	parsed := gjson.ParseBytes(body)

	parsed.IsArray()
	lines := parsed.Array()

	insertArraySize.Observe(float64(len(lines)))

	errorItems := map[int]bool{}
	for i, line := range lines {
		flatItems, err := flattener.Flatten(table, line.Raw)
		if err != nil {
			errorItems[i] = true
			log.Trace().Err(err).Str("json", line.Raw).Msg("Unable to flatten JSON")
			continue
		}

		for _, flatItem := range flatItems {
			var writeErr error
			var toWrite string

			toWrite = flatItem.JSON

			if !gjson.Get(flatItem.JSON, "__row_id").Exists() {
				snowID := a.snow.Generate()
				rowID := snowID.Int64()
				if toWrite, err = sjson.Set(flatItem.JSON, "__row_id", rowID); err != nil {
					log.Trace().Err(err).Str("json", flatItem.JSON).Msg("Unable to add __row_id")
				}
			}

			writeErr = a.dataSink.WriteData(int64(databaseID), flatItem.Table, []byte(toWrite))

			if writeErr != nil {
				errorItems[i] = true
				log.Trace().Err(writeErr).Str("json", flatItem.JSON).Msg("Unable to write JSON")
			}
		}
	}

	if len(errorItems) > 0 {
		if len(errorItems) == len(lines) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Unable to insert data"))
			return
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Partially inserted data"))
			return
		}
	}

	w.Write([]byte("ok"))
}
